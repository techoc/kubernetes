/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package services

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/kubernetes/test/e2e/framework"
)

var serverStartTimeout = flag.Duration("server-start-timeout", time.Second*120, "Time to wait for each server to become healthy.")

// server 管理一个通过命令启动和停止的独立 server 进程。

type server struct {
	// name 是 server 名称，仅用于日志。
	name string
	// startCommand 是启动 server 的命令
	startCommand *exec.Cmd
	// killCommand 是停止 server 的命令。非必需；
	// 未指定时用 `kill` 停止 server。
	killCommand *exec.Cmd
	// restartCommand 是重启 server 的命令。如果提供，重启时
	// 用它代替 startCommand。
	restartCommand *exec.Cmd
	// healthCheckUrls 是检查 server 是否就绪的 URL。
	healthCheckUrls []string
	// outFilename 是日志文件名。server 的 stdout 和 stderr
	// 会重定向到该文件。
	outFilename string
	// monitorParent 决定 server 是否监视其父进程，并在
	// 父进程退出时退出。
	monitorParent bool
	// restartOnExit 决定是否围绕 server 启动重启循环。
	restartOnExit bool
	// 向这个 channel 写入（如果非 nil）会停止重启循环。
	// 拆除 server 时，应检查该 channel，存在则写入。
	stopRestartingCh chan<- bool
	// 从这个 channel 读取可确认重启循环已停止。
	ackStopRestartingCh <-chan bool
	// 如果服务由 systemd 管理，这里是 systemd unit 名；否则为空。
	systemdUnitName string
}

// newServer 返回一个新的 server，带有给定的名称、命令、健康检查
// URL 等。
func newServer(name string, start, kill, restart *exec.Cmd, urls []string, outputFileName string, monitorParent, restartOnExit bool, systemdUnitName string) *server {
	return &server{
		name:            name,
		startCommand:    start,
		killCommand:     kill,
		restartCommand:  restart,
		healthCheckUrls: urls,
		outFilename:     outputFileName,
		monitorParent:   monitorParent,
		restartOnExit:   restartOnExit,
		systemdUnitName: systemdUnitName,
	}
}

// commandToString 把命令格式化成字符串。
func commandToString(c *exec.Cmd) string {
	if c == nil {
		return ""
	}
	return strings.Join(append([]string{c.Path}, c.Args[1:]...), " ")
}

func (s *server) String() string {
	return fmt.Sprintf("server %q start-command: `%s`, kill-command: `%s`, restart-command: `%s`, health-check: %v, output-file: %q", s.name,
		commandToString(s.startCommand), commandToString(s.killCommand), commandToString(s.restartCommand), s.healthCheckUrls, s.outFilename)
}

// start 通过运行 server 的命令启动它，用健康检查监控它，
// 并在配置需要时确保它被重启。
//
// 注意：restartOnExit == true 时需要 len(s.healthCheckUrls) > 0 才能正常工作。
func (s *server) start() error {
	klog.Infof("Starting server %q with command %q", s.name, commandToString(s.startCommand))
	errCh := make(chan error)

	// 如果 server 配置为退出时重启，则设置重启 channel。
	var stopRestartingCh, ackStopRestartingCh chan bool
	if s.restartOnExit {
		if len(s.healthCheckUrls) == 0 {
			return fmt.Errorf("tried to start %s which has s.restartOnExit == true, but no health check urls provided", s)
		}

		stopRestartingCh = make(chan bool)
		ackStopRestartingCh = make(chan bool)

		s.stopRestartingCh = stopRestartingCh
		s.ackStopRestartingCh = ackStopRestartingCh
	}

	// 这个 goroutine 实际运行 server 的启动命令。
	go func() {
		defer close(errCh)

		// 创建输出文件名
		outPath := path.Join(framework.TestContext.ReportDir, s.outFilename)
		outfile, err := os.Create(outPath)
		if err != nil {
			errCh <- fmt.Errorf("failed to create file %q for `%s` %v", outPath, s, err)
			return
		}
		klog.Infof("Output file for server %q: %v", s.name, outfile.Name())
		defer outfile.Close()
		defer outfile.Sync()

		// 设置命令把输出写入文件
		s.startCommand.Stdout = outfile
		s.startCommand.Stderr = outfile

		// 如果设置了 monitorParent，启动 server 时设置 Pdeathsig。
		if s.monitorParent {
			// 本测试进程死亡时，server 也应被杀掉。
			attrs := &syscall.SysProcAttr{}
			// Hack：不用 build tag 设置仅 Linux 可用的字段。
			deathSigField := reflect.ValueOf(attrs).Elem().FieldByName("Pdeathsig")
			if deathSigField.IsValid() {
				deathSigField.Set(reflect.ValueOf(syscall.SIGTERM))
			} else {
				errCh <- fmt.Errorf("failed to set Pdeathsig field (non-linux build)")
				return
			}
			s.startCommand.SysProcAttr = attrs
		}

		// 启动命令
		err = s.startCommand.Start()
		if err != nil {
			errCh <- fmt.Errorf("failed to run %s: %w", s, err)
			return
		}
		if !s.restartOnExit {
			klog.Infof("Waiting for server %q start command to complete", s.name)
			// 如果不打算重启，在这里 Wait() 释放资源即可。
			// 否则在重启循环中 Wait()。
			err = s.startCommand.Wait()
			if err != nil {
				errCh <- fmt.Errorf("failed to run start command for server %q: %w", s.name, err)
				return
			}
		} else {
			usedStartCmd := true
			for {
				klog.Infof("Running health check for service %q", s.name)
				// 等待初始健康检查通过，确认 server 已启动。
				err := readinessCheck(s.name, s.healthCheckUrls, nil)
				if err != nil {
					if usedStartCmd {
						klog.Infof("Waiting for server %q start command to complete after initial health check failed", s.name)
						s.startCommand.Wait() // 必要时释放资源。
					}
					// 这不应发生，立即停止 e2eService 进程。
					klog.Fatalf("Restart loop readinessCheck failed for %q", s.name)
				} else {
					klog.Infof("Initial health check passed for service %q", s.name)
				}

				// 初始健康检查通过，等待健康检查再次失败。
			stillAlive:
				for {
					select {
					case <-stopRestartingCh:
						ackStopRestartingCh <- true
						return
					case <-time.After(time.Second):
						for _, url := range s.healthCheckUrls {
							resp, err := http.Head(url)
							if err != nil || resp.StatusCode != http.StatusOK {
								break stillAlive
							}
						}
					}
				}

				if usedStartCmd {
					s.startCommand.Wait() // 释放上一次命令的资源
					usedStartCmd = false
				}
				if s.restartCommand != nil {
					// 每次运行前都要重新复制 restartCommand，
					// 可能需要重启多次
					s.restartCommand = &exec.Cmd{
						Path:        s.restartCommand.Path,
						Args:        s.restartCommand.Args,
						Env:         s.restartCommand.Env,
						Dir:         s.restartCommand.Dir,
						Stdin:       s.restartCommand.Stdin,
						Stdout:      s.restartCommand.Stdout,
						Stderr:      s.restartCommand.Stderr,
						ExtraFiles:  s.restartCommand.ExtraFiles,
						SysProcAttr: s.restartCommand.SysProcAttr,
					}
					// 运行并等待退出。假定这个命令执行时间
					// 很短，例如 systemctl restart
					klog.Infof("Restarting server %q with restart command", s.name)
					err = s.restartCommand.Run()
					if err != nil {
						// 这不应发生，立即停止 e2eService 进程。
						klog.Fatalf("Restarting server %s with restartCommand failed. Error: %v.", s, err)
					}
				} else {
					s.startCommand = &exec.Cmd{
						Path:        s.startCommand.Path,
						Args:        s.startCommand.Args,
						Env:         s.startCommand.Env,
						Dir:         s.startCommand.Dir,
						Stdin:       s.startCommand.Stdin,
						Stdout:      s.startCommand.Stdout,
						Stderr:      s.startCommand.Stderr,
						ExtraFiles:  s.startCommand.ExtraFiles,
						SysProcAttr: s.startCommand.SysProcAttr,
					}
					klog.Infof("Restarting server %q with start command", s.name)
					err = s.startCommand.Start()
					usedStartCmd = true
					if err != nil {
						// 这不应发生，立即停止 e2eService 进程。
						klog.Fatalf("Restarting server %s with startCommand failed. Error: %v.", s, err)
					}
				}
			}
		}
	}()

	return readinessCheck(s.name, s.healthCheckUrls, errCh)
}

// kill 运行 server 的 kill 命令。
func (s *server) kill() error {
	klog.Infof("Kill server %q", s.name)
	name := s.name
	cmd := s.startCommand

	// 如果 s 有重启循环，先关掉它。
	if s.restartOnExit {
		s.stopRestartingCh <- true
		<-s.ackStopRestartingCh
	}

	if s.killCommand != nil {
		return s.killCommand.Run()
	}

	if cmd == nil {
		return fmt.Errorf("could not kill %q because both `killCommand` and `startCommand` are nil", name)
	}

	if cmd.Process == nil {
		klog.V(2).Infof("%q not running", name)
		return nil
	}
	pid := cmd.Process.Pid
	if pid <= 1 {
		return fmt.Errorf("invalid PID %d for %q", pid, name)
	}

	// 强制关闭前，先尝试以友好方式关闭进程。
	waitChan := make(chan error)
	go func() {
		_, err := cmd.Process.Wait()
		waitChan <- err
		close(waitChan)
	}()

	const timeout = 10 * time.Second
	for _, signal := range []string{"-TERM", "-KILL"} {
		klog.V(2).Infof("Killing process %d (%s) with %s", pid, name, signal)
		cmd := exec.Command("kill", signal, strconv.Itoa(pid))
		_, err := cmd.Output()
		if err != nil {
			klog.Errorf("Error signaling process %d (%s) with %s: %v", pid, name, signal, err)
			continue
		}

		select {
		case err := <-waitChan:
			if err != nil {
				return fmt.Errorf("error stopping %q: %w", name, err)
			}
			// 成功！
			return nil
		case <-time.After(timeout):
			// 继续。
		}
	}

	return fmt.Errorf("unable to stop %q", name)
}

func (s *server) stopUnit() error {
	klog.Infof("Stopping systemd unit for server %q with unit name: %q", s.name, s.systemdUnitName)
	if s.systemdUnitName != "" {
		err := exec.Command("sudo", "systemctl", "stop", s.systemdUnitName).Run()
		if err != nil {
			return fmt.Errorf("Failed to stop systemd unit name: %q: %w", s.systemdUnitName, err)
		}
	}
	return nil
}
