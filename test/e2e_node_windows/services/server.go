//go:build windows

/*
Copyright The Kubernetes Authors.

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
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/test/e2e/framework"
)

var serverStartTimeout = flag.Duration("server-start-timeout", time.Second*120, "Time to wait for each server to become healthy.")

var (
	once      sync.Once
	jobHandle windows.Handle
)

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

		// 用 Windows 特有的 startProcess 函数启动 server
		err = startProcess(s.startCommand, s.monitorParent)

		if err != nil {
			errCh <- fmt.Errorf("failed to run %s: %w", s, err)
			return
		}
		if !s.restartOnExit {
			klog.Infof("Waiting for server %q start command to complete", s.name)
			err = s.startCommand.Wait()
			if err != nil {
				errCh <- fmt.Errorf("failed to run start command for server %q: %w", s.name, err)
				return
			}
		} else {
			usedStartCmd := true
			for {
				klog.Infof("Running health check for service %q", s.name)
				err := readinessCheck(s.name, s.healthCheckUrls, nil)
				if err != nil {
					if usedStartCmd {
						klog.Infof("Waiting for server %q start command to complete after initial health check failed", s.name)
						s.startCommand.Wait()
					}
					klog.Fatalf("Restart loop readinessCheck failed for %q", s.name)
				} else {
					klog.Infof("Initial health check passed for service %q", s.name)
				}

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
					s.startCommand.Wait()
					usedStartCmd = false
				}
				if s.restartCommand != nil {
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
					klog.Infof("Restarting server %q with restart command", s.name)
					err = s.restartCommand.Run()
					if err != nil {
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
			return nil
		case <-time.After(timeout):
			// 继续。
		}
	}

	return fmt.Errorf("unable to stop %q", name)
}

func (s *server) stopUnit() error {
	// Windows 上没有 systemd；这是空操作。
	return nil
}

// startProcess 用给定的命令和参数启动进程。
func startProcess(cmd *exec.Cmd, monitorParent bool) error {
	if monitorParent {
		jobHandle, err := getJobHandle()
		if err != nil {
			return err
		}

		go func() {
			defer windows.CloseHandle(jobHandle)
			waitForTerminationSignal()
		}()

		err = cmd.Start()
		if err != nil {
			return err
		}

		return assignProcessToJob(jobHandle, cmd.Process.Pid)
	}
	return cmd.Start()
}

// getJobHandle 返回 job handle 的单实例。
func getJobHandle() (windows.Handle, error) {
	var err error
	once.Do(func() {
		jobHandle, err = createJobObject()
	})
	return jobHandle, err
}

// createJobObject 创建一个 Windows job object。
func createJobObject() (windows.Handle, error) {
	jobHandle, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return 0, err
	}

	var info windows.JOBOBJECT_BASIC_LIMIT_INFORMATION
	info.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	var extendedInfo windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	extendedInfo.BasicLimitInformation = info
	_, err = windows.SetInformationJobObject(
		jobHandle,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&extendedInfo)),
		uint32(unsafe.Sizeof(extendedInfo)),
	)
	if err != nil {
		windows.CloseHandle(jobHandle)
		return 0, err
	}
	return jobHandle, nil
}

// assignProcessToJob 把进程分配到 job object。
func assignProcessToJob(jobHandle windows.Handle, pid int) error {
	processHandle, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		return err
	}
	defer windows.CloseHandle(processHandle)
	return windows.AssignProcessToJobObject(jobHandle, processHandle)
}
