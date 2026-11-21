//go:build linux

/*
Copyright 2020 The Kubernetes Authors.

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

package systemd

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/godbus/dbus/v5"
	"k8s.io/klog/v2"
)

const (
	// logind 的 D-Bus 服务名、对象路径与接口名。
	logindService   = "org.freedesktop.login1"
	logindObject    = dbus.ObjectPath("/org/freedesktop/login1")
	logindInterface = "org.freedesktop.login1.Manager"
)

// dBusConnector 抽象 D-Bus 连接所需的方法，便于测试替换。
type dBusConnector interface {
	// Object 返回指定服务与路径的 D-Bus 对象。
	Object(dest string, path dbus.ObjectPath) dbus.BusObject
	// AddMatchSignal 注册要接收的信号匹配规则。
	AddMatchSignal(options ...dbus.MatchOption) error
	// Signal 注册接收信号的通道。
	Signal(ch chan<- *dbus.Signal)
	// Close 关闭 D-Bus 连接。
	Close() error
}

// DBusCon 提供通过 D-Bus 与 systemd、logind 交互的方法。
type DBusCon struct {
	SystemBus dBusConnector
}

// Close 关闭底层 D-Bus 连接。
func (bus *DBusCon) Close() error {
	return bus.SystemBus.Close()
}

// NewDBusCon 连接系统 D-Bus 并返回 DBusCon。
func NewDBusCon() (*DBusCon, error) {
	conn, err := dbus.ConnectSystemBus()
	if err != nil {
		return nil, err
	}

	return &DBusCon{
		SystemBus: conn,
	}, nil
}

// InhibitLock 是调用 InhibitShutdown() 创建 systemd inhibitor 后得到的锁。
type InhibitLock uint32

// CurrentInhibitDelay 返回 logind.conf(5) 中配置的延迟 inhibitor 超时值。
// 详见 https://www.freedesktop.org/software/systemd/man/logind.conf.html。
func (bus *DBusCon) CurrentInhibitDelay() (time.Duration, error) {
	obj := bus.SystemBus.Object(logindService, logindObject)
	res, err := obj.GetProperty(logindInterface + ".InhibitDelayMaxUSec")
	if err != nil {
		return 0, fmt.Errorf("failed reading InhibitDelayMaxUSec property from logind: %w", err)
	}

	delay, ok := res.Value().(uint64)
	// 通道关闭说明 D-Bus 连接已断开，随之关闭输出通道。
	if !ok {
		return 0, fmt.Errorf("InhibitDelayMaxUSec from logind is not a uint64 as expected")
	}

	// InhibitDelayMaxUSec is in microseconds
	// logind 以微秒为单位暴露 InhibitDelayMaxUSec。
	duration := time.Duration(delay) * time.Microsecond
	return duration, nil
}

// InhibitShutdown 调用 logind 的 Inhibit() 创建 systemd inhibitor 并返回锁。
// 详见 https://www.freedesktop.org/wiki/Software/systemd/inhibit/。
func (bus *DBusCon) InhibitShutdown() (InhibitLock, error) {
	obj := bus.SystemBus.Object(logindService, logindObject)
	what := "shutdown"
	who := "kubelet"
	why := "Kubelet needs time to handle node shutdown"
	mode := "delay"

	// 以 delay 模式创建 inhibitor：kubelet 处理关机期间阻止关机开始。
	call := obj.Call("org.freedesktop.login1.Manager.Inhibit", 0, what, who, why, mode)
	if call.Err != nil {
		return InhibitLock(0), fmt.Errorf("failed creating systemd inhibitor: %w", call.Err)
	}

	// 返回的文件描述符即锁；关闭它就会释放 inhibitor。
	var fd uint32
	err := call.Store(&fd)
	if err != nil {
		return InhibitLock(0), fmt.Errorf("failed storing inhibit lock file descriptor: %w", err)
	}

	return InhibitLock(fd), nil
}

// ReleaseInhibitLock 释放底层 inhibitor 锁，随后关机流程会开始。
func (bus *DBusCon) ReleaseInhibitLock(lock InhibitLock) error {
	err := syscall.Close(int(lock))

	if err != nil {
		return fmt.Errorf("unable to close systemd inhibitor lock: %w", err)
	}

	return nil
}

// ReloadLogindConf 通过 D-Bus 向 systemd-logind 发送 SIGHUP，使其重新加载配置。
func (bus *DBusCon) ReloadLogindConf() error {
	systemdService := "org.freedesktop.systemd1"
	systemdObject := "/org/freedesktop/systemd1"
	systemdInterface := "org.freedesktop.systemd1.Manager"

	obj := bus.SystemBus.Object(systemdService, dbus.ObjectPath(systemdObject))
	unit := "systemd-logind.service"
	who := "all"
	var signal int32 = 1 // SIGHUP

	// 向 systemd-logind 发送 SIGHUP 使其重新加载配置。
	call := obj.Call(systemdInterface+".KillUnit", 0, unit, who, signal)
	if call.Err != nil {
		return fmt.Errorf("unable to reload logind conf: %w", call.Err)
	}

	return nil
}

// MonitorShutdown 通过监听 logind 的 "PrepareForShutdown" 事件检测节点关机。
// 详见 https://www.freedesktop.org/wiki/Software/systemd/inhibit/。
func (bus *DBusCon) MonitorShutdown(logger klog.Logger) (<-chan bool, error) {
	// 订阅 logind 的 PrepareForShutdown 信号。
	err := bus.SystemBus.AddMatchSignal(dbus.WithMatchInterface(logindInterface), dbus.WithMatchMember("PrepareForShutdown"), dbus.WithMatchObjectPath("/org/freedesktop/login1"))

	if err != nil {
		return nil, err
	}

	busChan := make(chan *dbus.Signal, 1)
	bus.SystemBus.Signal(busChan)

	shutdownChan := make(chan bool, 1)

	// 在后台把 D-Bus 信号转换为布尔通道：true 表示即将关机。
	go func() {
		for {
			event, ok := <-busChan
			if !ok {
				close(shutdownChan)
				return
			}
			if event == nil || len(event.Body) == 0 {
				logger.Error(nil, "Failed obtaining shutdown event, PrepareForShutdown event was empty")
				continue
			}
			shutdownActive, ok := event.Body[0].(bool)
			if !ok {
				logger.Error(nil, "Failed obtaining shutdown event, PrepareForShutdown event was not bool type as expected")
				continue
			}
			shutdownChan <- shutdownActive
		}
	}()

	return shutdownChan, nil
}

const (
	// logind 配置目录与 kubelet 覆盖配置文件名。
	logindConfigDirectory = "/etc/systemd/logind.conf.d/"
	kubeletLogindConf     = "99-kubelet.conf"
)

// OverrideInhibitDelay 写入 logind 配置文件，把 InhibitDelayMaxSec 覆盖为期望值。
func (bus *DBusCon) OverrideInhibitDelay(inhibitDelayMax time.Duration) error {
	err := os.MkdirAll(logindConfigDirectory, 0755)
	if err != nil {
		return fmt.Errorf("failed creating %v directory: %w", logindConfigDirectory, err)
	}

	// 这里设置 logind 的 `InhibitDelayMaxUSec` D-Bus 属性（以微秒计的最大 inhibitor 延迟）。
	// 对应的配置文件属性名为 `InhibitDelayMaxSec`，以秒为单位，通过 logind.conf 设置。
	// 详见 https://www.freedesktop.org/software/systemd/man/logind.conf.html。

	// 配置文件以秒为单位，格式化时保留整数秒。
	inhibitOverride := fmt.Sprintf(`# Kubelet logind override
[Login]
InhibitDelayMaxSec=%.0f
`, inhibitDelayMax.Seconds())

	logindOverridePath := filepath.Join(logindConfigDirectory, kubeletLogindConf)
	if err := os.WriteFile(logindOverridePath, []byte(inhibitOverride), 0644); err != nil {
		return fmt.Errorf("failed writing logind shutdown inhibit override file %v: %w", logindOverridePath, err)
	}

	return nil
}
