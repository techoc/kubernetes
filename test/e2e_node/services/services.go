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
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"

	"k8s.io/klog/v2"

	"k8s.io/kubernetes/test/e2e/framework"
)

// E2EServices 在独立进程中启动和停止 e2e 服务。
// 测试用它来启动和停止所有 e2e 服务。
type E2EServices struct {
	// monitorParent 决定子进程是否应监视当前进程并随之退出。
	rmDirs        []string
	monitorParent bool
	services      *server
	kubelet       *server
	logs          logFiles
}

// NewE2EServices 返回一个新的 E2EServices 实例。
func NewE2EServices(monitorParent bool) *E2EServices {
	return &E2EServices{
		monitorParent: monitorParent,
		// 为便于进一步调试而需要收集的特殊日志文件。
		logs: getLogFiles(),
	}
}

// Start 通过回调测试二进制文件，在另一个进程中启动 e2e 服务。
// 当所有 e2e 服务就绪或发生错误时返回。
//
// 我们希望把 e2e 服务静态链接进测试二进制文件，但不希望它们的 glog 输出
// 污染测试结果。因此以 run-services-mode 运行该二进制文件，
// 在另一个进程中启动 e2e 服务。
// 该函数启动 2 个进程：
//   - 内部 e2e 服务：静态链接在测试二进制文件中的服务——apiserver、etcd 和 namespace controller。
//   - kubelet：kubelet 二进制文件在外部。（我们计划在有标准的 kubelet 启动器后，
//     将主要的 kubelet 启动逻辑移出。）
func (e *E2EServices) Start(ctx context.Context, featureGates map[string]bool) error {
	var err error
	if e.services, err = e.startInternalServices(); err != nil {
		return fmt.Errorf("failed to start internal services: %w", err)
	}
	klog.Infof("Node services started.")
	// 是否运行 kubelet 取决于当前是否在执行 conformance 测试套件。
	if framework.TestContext.NodeConformance {
		klog.Info("nothing to do in node-e2e-services, running conformance suite")
	} else {
		// 启动 kubelet。
		e.kubelet, err = e.startKubelet(ctx, featureGates)
		if err != nil {
			return fmt.Errorf("failed to start kubelet: %w", err)
		}
		klog.Infof("Kubelet started.")
	}
	return nil
}

// Stop 停止 e2e 服务。
func (e *E2EServices) Stop() {
	defer func() {
		if !framework.TestContext.NodeConformance {
			// 收集日志文件。
			e.collectLogFiles()
		}
	}()
	if e.services != nil {
		if err := e.services.kill(); err != nil {
			klog.Errorf("Failed to stop services: %v", err)
		}
	}
	if e.kubelet != nil {
		if err := e.kubelet.kill(); err != nil {
			klog.Errorf("Failed to kill kubelet: %v", err)
		}
		// 停止 kubelet systemd unit，这会删除 kubelet 的临时 unit。
		if err := e.kubelet.stopUnit(); err != nil {
			klog.Errorf("Failed to stop kubelet systemd unit: %v", err)
		}
	}
	for _, d := range e.rmDirs {
		err := os.RemoveAll(d)
		if err != nil {
			klog.Errorf("Failed to delete directory %s: %v", d, err)
		}
	}
}

// RunE2EServices 实际启动 e2e 服务。
// 该函数用于在当前进程中启动 e2e 服务，只在 run-services-mode 下使用。
func RunE2EServices(t *testing.T) {
	e := newE2EServices()
	if err := e.run(t); err != nil {
		klog.Fatalf("Failed to run e2e services: %v", err)
	}
}

const (
	// services.log 是所有服务日志的汇总。
	servicesLogFile = "services.log"
	// LogVerbosityLevel 与集群 e2e 测试中使用的日志级别保持一致。
	LogVerbosityLevel = "4"
)

// startInternalServices 在独立进程中启动内部服务。
func (e *E2EServices) startInternalServices() (*server, error) {
	testBin, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("can't get current binary: %w", err)
	}
	// 把所有 flag 传给子进程，使其看到相同的 flag 集合。
	startCmd := exec.Command(testBin,
		append(
			[]string{"--run-services-mode", fmt.Sprintf("--bearer-token=%s", framework.TestContext.BearerToken)},
			os.Args[1:]...,
		)...)
	server := newServer("services", startCmd, nil, nil, getServicesHealthCheckURLs(), servicesLogFile, e.monitorParent, false, "")
	return server, server.start()
}

// collectLogFiles 通过 journalctl 或创建符号链接收集需要的日志。
// 由于通过 scp 从远程目录复制文件，符号链接会被当作普通文件处理，
// 其内容会被复制过去。
func (e *E2EServices) collectLogFiles() {
	// 如果未指定报告目录，则无需处理。
	if framework.TestContext.ReportDir == "" {
		return
	}
	klog.Info("Fetching log files...")
	journaldFound := isJournaldAvailable()
	for targetFileName, log := range e.logs {
		targetLink := path.Join(framework.TestContext.ReportDir, targetFileName)
		if journaldFound {
			// 跳过在使用 journald 的系统上没有对应项的日志文件。
			if len(log.JournalctlCommand) == 0 {
				continue
			}
			klog.Infof("Get log file %q with journalctl command %v.", targetFileName, log.JournalctlCommand)
			out, err := exec.Command("journalctl", log.JournalctlCommand...).CombinedOutput()
			if err != nil {
				klog.Errorf("failed to get %q from journald: %v, %v", targetFileName, string(out), err)
			} else {
				if err = os.WriteFile(targetLink, out, 0644); err != nil {
					klog.Errorf("failed to write logs to %q: %v", targetLink, err)
				}
			}
			continue
		}
		for _, file := range log.Files {
			if _, err := os.Stat(file); err != nil {
				// 该发行版中没有此文件属于预期情况。
				continue
			}
			if err := copyLogFile(file, targetLink); err != nil {
				klog.Error(err)
			} else {
				break
			}
		}
	}
}

// isJournaldAvailable 返回执行测试的系统是否使用 journald。
func isJournaldAvailable() bool {
	_, err := exec.LookPath("journalctl")
	return err == nil
}

func copyLogFile(src, target string) error {
	// 对于非 journald 系统，直接复制文件。
	if out, err := exec.Command("cp", src, target).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to copy %q to %q: %v, %v", src, target, out, err)
	}
	if out, err := exec.Command("chmod", "a+r", target).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to make log file %q world readable: %v, %v", target, out, err)
	}
	return nil
}
