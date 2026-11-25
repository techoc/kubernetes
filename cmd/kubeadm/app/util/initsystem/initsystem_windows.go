//go:build windows

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
	"time"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// WindowsInitSystem 是 InitSystem 的 Windows 实现。
type WindowsInitSystem struct{}

// EnableCommand 返回描述如何启用服务的字符串。
func (sysd WindowsInitSystem) EnableCommand(service string) string {
	return fmt.Sprintf("Set-Service '%s' -StartupType Automatic", service)
}

// ServiceStart 尝试启动指定服务。
// 遵循 Windows 文档：https://docs.microsoft.com/en-us/windows/desktop/Services/starting-a-service
func (sysd WindowsInitSystem) ServiceStart(service string) error {
	m, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer m.Disconnect()

	s, err := m.OpenService(service)
	if err != nil {
		return errors.Wrapf(err, "could not access service %s", service)
	}
	defer s.Close()

	// 检查服务是否已启动。
	status, err := s.Query()
	if err != nil {
		return errors.Wrapf(err, "could not query service %s", service)
	}

	if status.State != svc.Stopped && status.State != svc.StopPending {
		return nil
	}

	timeout := time.Now().Add(10 * time.Second)
	for status.State != svc.Stopped {
		if timeout.Before(time.Now()) {
			return errors.Errorf("timeout waiting for %s service to stop", service)
		}
		time.Sleep(300 * time.Millisecond)
		status, err = s.Query()
		if err != nil {
			return errors.Wrapf(err, "could not retrieve %s service status", service)
		}
	}

	// 启动服务。
	err = s.Start("is", "manual-started")
	if err != nil {
		return errors.Wrapf(err, "could not start service %s", service)
	}

	// 检查启动是否成功。
	status, err = s.Query()
	if err != nil {
		return errors.Wrapf(err, "could not query service %s", service)
	}
	timeout = time.Now().Add(10 * time.Second)
	for status.State != svc.Running {
		if timeout.Before(time.Now()) {
			return errors.Errorf("timeout waiting for %s service to start", service)
		}
		time.Sleep(300 * time.Millisecond)
		status, err = s.Query()
		if err != nil {
			return errors.Wrapf(err, "could not retrieve %s service status", service)
		}
	}
	return nil
}

// ServiceRestart 尝试重新加载环境并重启指定服务。
func (sysd WindowsInitSystem) ServiceRestart(service string) error {
	if err := sysd.ServiceStop(service); err != nil {
		return errors.Wrapf(err, "couldn't stop service %s", service)
	}
	if err := sysd.ServiceStart(service); err != nil {
		return errors.Wrapf(err, "couldn't start service %s", service)
	}

	return nil
}

// ServiceStop 尝试停止指定服务。
// 遵循 Windows 文档：https://docs.microsoft.com/en-us/windows/desktop/Services/stopping-a-service
func (sysd WindowsInitSystem) ServiceStop(service string) error {
	m, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer m.Disconnect()

	s, err := m.OpenService(service)
	if err != nil {
		return errors.Wrapf(err, "could not access service %s", service)
	}
	defer s.Close()

	// 检查服务是否已停止。
	status, err := s.Query()
	if err != nil {
		return errors.Wrapf(err, "could not query service %s", service)
	}

	if status.State == svc.Stopped {
		return nil
	}

	// 若处于 StopPending，等待服务最终停止。
	if status.State == svc.StopPending {
		timeout := time.Now().Add(10 * time.Second)
		for status.State != svc.Stopped {
			if timeout.Before(time.Now()) {
				return errors.Errorf("timeout waiting for %s service to stop", service)
			}
			time.Sleep(300 * time.Millisecond)
			status, err = s.Query()
			if err != nil {
				return errors.Wrapf(err, "could not retrieve %s service status", service)
			}
		}
		return nil
	}

	// 停止服务。
	status, err = s.Control(svc.Stop)
	if err != nil {
		return errors.Wrapf(err, "could not stop service %s", service)
	}

	// 检查停止是否成功。
	status, err = s.Query()
	if err != nil {
		return errors.Wrapf(err, "could not query service %s", service)
	}
	timeout := time.Now().Add(10 * time.Second)
	for status.State != svc.Stopped {
		if timeout.Before(time.Now()) {
			return errors.Errorf("timeout waiting for %s service to stop", service)
		}
		time.Sleep(300 * time.Millisecond)
		status, err = s.Query()
		if err != nil {
			return errors.Wrapf(err, "could not retrieve %s service status", service)
		}
	}
	return nil
}

// ServiceExists 确认服务已在此 init 系统中定义。
func (sysd WindowsInitSystem) ServiceExists(service string) bool {
	m, err := mgr.Connect()
	if err != nil {
		return false
	}
	defer m.Disconnect()
	s, err := m.OpenService(service)
	if err != nil {
		return false
	}
	defer s.Close()

	return true
}

// ServiceIsEnabled 确认服务已配置为每次启动时自动运行。
func (sysd WindowsInitSystem) ServiceIsEnabled(service string) bool {
	m, err := mgr.Connect()
	if err != nil {
		return false
	}
	defer m.Disconnect()

	s, err := m.OpenService(service)
	if err != nil {
		return false
	}
	defer s.Close()

	c, err := s.Config()
	if err != nil {
		return false
	}

	return c.StartType != mgr.StartDisabled
}

// ServiceIsActive 确认服务正在运行或尝试运行；kubelet 崩溃循环时也视为活动。
func (sysd WindowsInitSystem) ServiceIsActive(service string) bool {
	m, err := mgr.Connect()
	if err != nil {
		return false
	}
	defer m.Disconnect()
	s, err := m.OpenService(service)
	if err != nil {
		return false
	}
	defer s.Close()

	status, err := s.Query()
	if err != nil {
		return false
	}
	return status.State == svc.Running
}

// GetInitSystem 返回当前系统的 InitSystem；无法检测到受支持的系统时返回 nil。
// 这表示跳过 init 系统检查，并非错误。
func GetInitSystem() (InitSystem, error) {
	m, err := mgr.Connect()
	if err != nil {
		return nil, errors.Wrap(err, "no supported init system detected")
	}
	defer m.Disconnect()
	return &WindowsInitSystem{}, nil
}
