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
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"k8s.io/klog/v2"

	"k8s.io/kubernetes/test/e2e/framework"
)

// E2EServices 在独立进程中启动和停止 e2e 服务。
// 测试通过它来启动和停止所有 e2e 服务。
type E2EServices struct {
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
		logs:          getLogFiles(),
	}
}

// Start 通过回调测试二进制文件在另一个进程中启动 e2e 服务。
// 当所有 e2e 服务就绪或发生错误时返回。
func (e *E2EServices) Start(ctx context.Context, featureGates map[string]bool) error {
	var err error
	if e.services, err = e.startInternalServices(); err != nil {
		return fmt.Errorf("failed to start internal services: %w", err)
	}
	klog.Infof("Node services started.")
	if framework.TestContext.NodeConformance {
		klog.Info("nothing to do in node-e2e-services, running conformance suite")
	} else {
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
		if err := e.kubelet.stopUnit(); err != nil {
			klog.Errorf("Failed to stop kubelet unit: %v", err)
		}
	}
	for _, d := range e.rmDirs {
		err := os.RemoveAll(d)
		if err != nil {
			klog.Errorf("Failed to delete directory %s: %v", d, err)
		}
	}
}

// RunE2EServices 实际启动 e2e 服务。该函数用于在当前进程中启动 e2e 服务，
// 仅在 run-services-mode 下使用。
func RunE2EServices(t *testing.T) {
	e := newE2EServices()
	if err := e.run(t); err != nil {
		klog.Fatalf("Failed to run e2e services: %v", err)
	}
}

const (
	// servicesLogFile 是所有服务日志的汇总
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
	startCmd := exec.Command(testBin,
		append(
			[]string{"--run-services-mode", fmt.Sprintf("--bearer-token=%s", framework.TestContext.BearerToken)},
			os.Args[1:]...,
		)...)
	server := newServer("services", startCmd, nil, nil, getServicesHealthCheckURLs(), servicesLogFile, e.monitorParent, false, "")
	return server, server.start()
}

// collectLogFiles 在 Windows 上是空操作（没有 journald 或 Linux 日志路径）。
func (e *E2EServices) collectLogFiles() {
	klog.Info("Log collection is not supported on Windows.")
}
