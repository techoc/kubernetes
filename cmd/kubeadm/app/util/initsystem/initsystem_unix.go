//go:build !windows

/*
Copyright 2017 The Kubernetes Authors.

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

package initsystem

import (
	"fmt"
	"os/exec"
	"strings"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// OpenRCInitSystem 定义 OpenRC 实现。
type OpenRCInitSystem struct{}

// ServiceStart 尝试启动指定服务。
func (openrc OpenRCInitSystem) ServiceStart(service string) error {
	args := []string{service, "start"}
	return exec.Command("rc-service", args...).Run()
}

// ServiceStop 尝试停止指定服务。
func (openrc OpenRCInitSystem) ServiceStop(service string) error {
	args := []string{service, "stop"}
	return exec.Command("rc-service", args...).Run()
}

// ServiceRestart 尝试重新加载环境并重启指定服务。
func (openrc OpenRCInitSystem) ServiceRestart(service string) error {
	args := []string{service, "restart"}
	return exec.Command("rc-service", args...).Run()
}

// ServiceExists 确认服务已定义。OpenRC 在服务不存在或未启用时写入 stderr，
// 而 systemd 仅写入 stdout；因此使用 CombinedOutput 并忽略错误。
func (openrc OpenRCInitSystem) ServiceExists(service string) bool {
	args := []string{service, "status"}
	outBytes, _ := exec.Command("rc-service", args...).CombinedOutput()
	return !strings.Contains(string(outBytes), "does not exist")
}

// ServiceIsEnabled 确认服务已配置为每次启动时自动运行。
func (openrc OpenRCInitSystem) ServiceIsEnabled(service string) bool {
	args := []string{"show", "default"}
	outBytes, _ := exec.Command("rc-update", args...).Output()
	return strings.Contains(string(outBytes), service)
}

// ServiceIsActive 确认服务正在运行或尝试运行；kubelet 崩溃循环时也视为活动。
func (openrc OpenRCInitSystem) ServiceIsActive(service string) bool {
	args := []string{service, "status"}
	outBytes, _ := exec.Command("rc-service", args...).CombinedOutput()
	outStr := string(outBytes)
	return !strings.Contains(outStr, "stopped") && !strings.Contains(outStr, "does not exist")
}

// EnableCommand 返回描述如何启用服务的字符串。
func (openrc OpenRCInitSystem) EnableCommand(service string) string {
	return fmt.Sprintf("rc-update add %s default", service)
}

// SystemdInitSystem 定义 systemd 实现。
type SystemdInitSystem struct{}

// EnableCommand 返回描述如何启用服务的字符串。
func (sysd SystemdInitSystem) EnableCommand(service string) string {
	return fmt.Sprintf("systemctl enable %s.service", service)
}

// reloadSystemd 重新加载 systemd 守护进程。
func (sysd SystemdInitSystem) reloadSystemd() error {
	if err := exec.Command("systemctl", "daemon-reload").Run(); err != nil {
		return errors.Wrap(err, "failed to reload systemd")
	}
	return nil
}

// ServiceStart 尝试启动指定服务。
func (sysd SystemdInitSystem) ServiceStart(service string) error {
	// 启动服务前确认 systemd 已就绪。
	if err := sysd.reloadSystemd(); err != nil {
		return err
	}
	args := []string{"start", service}
	return exec.Command("systemctl", args...).Run()
}

// ServiceRestart 尝试重新加载环境并重启指定服务。
func (sysd SystemdInitSystem) ServiceRestart(service string) error {
	// 重启服务前确认 systemd 已就绪。
	if err := sysd.reloadSystemd(); err != nil {
		return err
	}
	args := []string{"restart", service}
	return exec.Command("systemctl", args...).Run()
}

// ServiceStop 尝试停止指定服务。
func (sysd SystemdInitSystem) ServiceStop(service string) error {
	args := []string{"stop", service}
	return exec.Command("systemctl", args...).Run()
}

// ServiceExists 确认服务已在此 init 系统中定义。
func (sysd SystemdInitSystem) ServiceExists(service string) bool {
	args := []string{"status", service}
	outBytes, _ := exec.Command("systemctl", args...).Output()
	output := string(outBytes)
	return !strings.Contains(output, "Loaded: not-found") && !strings.Contains(output, "could not be found")
}

// ServiceIsEnabled 确认服务已配置为每次启动时自动运行。
func (sysd SystemdInitSystem) ServiceIsEnabled(service string) bool {
	args := []string{"is-enabled", service}
	err := exec.Command("systemctl", args...).Run()
	return err == nil
}

// ServiceIsActive 检查服务是否为 "active"。kubelet 等服务崩溃循环时状态为
// "activating"，此实现也将其视为活动。
func (sysd SystemdInitSystem) ServiceIsActive(service string) bool {
	args := []string{"is-active", service}
	// 此处忽略错误；服务处于 "activating" 时命令返回非零退出码。
	outBytes, _ := exec.Command("systemctl", args...).Output()
	output := strings.TrimSpace(string(outBytes))
	if output == "active" || output == "activating" {
		return true
	}
	return false
}

// GetInitSystem 返回当前系统的 InitSystem；无法检测到受支持的系统时返回 nil。
// 这表示跳过 init 系统检查，并非错误。
func GetInitSystem() (InitSystem, error) {
	// PATH 中存在 systemctl 即假定系统使用 systemd。
	_, err := exec.LookPath("systemctl")
	if err == nil {
		return &SystemdInitSystem{}, nil
	}
	_, err = exec.LookPath("openrc")
	if err == nil {
		binaries := []string{"rc-service", "rc-update"}
		for _, binary := range binaries {
			_, err = exec.LookPath(binary)
			if err != nil {
				return nil, errors.Wrapf(err, "openrc detected, but missing required binary: %s", binary)
			}
		}
		return &OpenRCInitSystem{}, nil
	}

	return nil, errors.New("no supported init system detected, skipping checking for services")
}
