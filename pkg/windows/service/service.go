//go:build windows

/*
Copyright 2018 The Kubernetes Authors.

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

// Package service 提供将 Kubernetes 组件作为 Windows 服务运行的功能。
// 在 Windows 平台上，kube-apiserver、kube-proxy 等组件可以注册为 Windows 服务，
// 由 SCM（Service Control Manager）管理生命周期。支持预关闭事件（PreShutdown），
// 让组件在节点关闭前执行优雅退出逻辑（如驱逐 Pod）。
package service

import (
	"os"
	"syscall"
	"time"
	"unsafe"

	"k8s.io/apiserver/pkg/server"
	"k8s.io/klog/v2"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
)

// handler 实现 svc.Handler 接口，处理 Windows 服务的生命周期事件。
// 通过 channels 与主线程通信，将 Windows 服务控制请求转换为 Go 信号。
type handler struct {
	tosvc              chan bool          // 用于终止服务主循环的信号
	fromsvc            chan error         // 从 svc.Run 返回错误，通知 InitService 启动结果
	acceptPreshutdown  bool               // 是否接受预关闭事件（PreShutdown）
	preshutdownHandler PreshutdownHandler // 预关闭事件的处理回调
}

// PreshutdownHandler 在收到 Windows 服务的 PreShutdown 事件时调用。
// 用于在节点关闭前执行优雅退出逻辑，如驱逐 Pod 或停止接收新请求。
type PreshutdownHandler interface {
	ProcessShutdownEvent() error
}

// SERVICE_PRESHUTDOWN_INFO 是 Windows API 中查询/设置预关闭超时时间的结构体。
// 预关闭超时是指 SCM 发出 PreShutdown 事件到强制关闭之间的时间窗口。
type SERVICE_PRESHUTDOWN_INFO struct {
	PreshutdownTimeout uint32 // 预关闭超时时间（毫秒），SCM 等待服务完成优雅关闭的最大时间
}

// QueryPreShutdownInfo 查询 Windows 服务的预关闭超时配置。
// 使用 QueryServiceConfig2 API，自动处理缓冲区大小不足的情况（ERROR_INSUFFICIENT_BUFFER）。
func QueryPreShutdownInfo(h windows.Handle) (*SERVICE_PRESHUTDOWN_INFO, error) {
	n := uint32(1024)
	b := make([]byte, n)
	for {
		err := windows.QueryServiceConfig2(h, windows.SERVICE_CONFIG_PRESHUTDOWN_INFO, &b[0], n, &n)
		if err == nil {
			break
		}
		// 缓冲区不足时，API 会返回所需大小，重新分配后重试
		if err.(syscall.Errno) != syscall.ERROR_INSUFFICIENT_BUFFER {
			return nil, err
		}
		if n <= uint32(len(b)) {
			return nil, err
		}

		b = make([]byte, n)
	}

	info := (*SERVICE_PRESHUTDOWN_INFO)(unsafe.Pointer(&b[0]))

	return info, nil
}

// UpdatePreShutdownInfo 更新 Windows 服务的预关闭超时配置。
// 设置后，SCM 会在关闭服务前先发出 PreShutdown 事件，等待指定时间后再强制关闭。
func UpdatePreShutdownInfo(h windows.Handle, timeoutMilliSeconds uint32) error {
	preshutdownInfo := SERVICE_PRESHUTDOWN_INFO{
		PreshutdownTimeout: timeoutMilliSeconds,
	}

	err := windows.ChangeServiceConfig2(h, windows.SERVICE_CONFIG_PRESHUTDOWN_INFO, (*byte)(unsafe.Pointer(&preshutdownInfo)))
	if err != nil {
		return err
	}

	return nil
}

// thehandler 是全局唯一的 handler 实例。
// Windows 服务模型要求每个进程只能注册一个服务，因此使用全局变量。
var thehandler *handler

// InitService 将当前进程注册为 Windows 服务（不接受预关闭事件）。
// 作为 Windows 服务的入口点，调用后进程由 SCM 管理。
func InitService(serviceName string) error {
	return initService(serviceName, false)
}

// InitServiceWithShutdown 将当前进程注册为 Windows 服务（接受预关闭事件）。
// 与 InitService 的区别是启用了 PreShutdown 事件，允许在节点关闭前执行优雅退出逻辑。
func InitServiceWithShutdown(serviceName string) error {
	return initService(serviceName, true)
}

// initService 将当前进程注册为 Windows 服务。
// acceptPreshutdown 决定是否接受 PreShutdown 事件（用于优雅关闭）。
// 在 goroutine 中调用 svc.Run 启动服务主循环，通过 channel 等待首次信号确认启动成功。
func initService(serviceName string, acceptPreshutdown bool) error {
	var err error
	h := &handler{
		tosvc:              make(chan bool),
		fromsvc:            make(chan error),
		acceptPreshutdown:  acceptPreshutdown,
		preshutdownHandler: nil,
	}

	thehandler = h

	// 在 goroutine 中运行服务主循环，阻塞直到收到停止信号
	go func() {
		err = svc.Run(serviceName, h)
		h.fromsvc <- err
	}()

	// 等待服务处理器的第一个信号，确认服务已成功启动或返回错误
	err = <-h.fromsvc
	if err != nil {
		klog.Errorf("Running %s as a Windows has error %v!", serviceName, err)
		return err
	}
	klog.Infof("Running %s as a Windows service!", serviceName)
	return nil
}

// SetPreShutdownHandler 设置预关闭事件的回调处理函数。
// 当 SCM 发出 PreShutdown 事件时，调用此处理函数执行优雅退出逻辑。
func SetPreShutdownHandler(preshutdownhandler PreshutdownHandler) {
	thehandler.preshutdownHandler = preshutdownhandler
}

// IsServiceInitialized 检查是否已初始化为 Windows 服务。
// 用于条件性地执行服务特定的逻辑（如注册预关闭处理器）。
func IsServiceInitialized() bool {
	return thehandler != nil
}

// Execute 实现 svc.Handler 接口，是 Windows 服务的主事件循环。
// 处理 SCM 发出的控制命令：Stop、Shutdown、PreShutdown、ParamChange 等。
// 将 Windows 服务控制请求转换为 Go 信号（server.RequestShutdown），
// 让 Kubernetes 组件的统一信号处理机制（apiserver/pkg/server/signal.go）处理关闭逻辑。
func (h *handler) Execute(_ []string, r <-chan svc.ChangeRequest, s chan<- svc.Status) (bool, uint32) {
	s <- svc.Status{State: svc.StartPending, Accepts: 0}
	// 通知 initService 服务已启动，解除阻塞
	h.fromsvc <- nil

	// 根据是否接受预关闭事件，设置不同的 Accepts 标志
	if h.acceptPreshutdown {
		s <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptPreShutdown | svc.Accepted(windows.SERVICE_ACCEPT_PARAMCHANGE)}
		klog.Infof("Accept preshutdown")
	} else {
		s <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown | svc.Accepted(windows.SERVICE_ACCEPT_PARAMCHANGE)}
	}

	klog.Infof("Service running")
Loop:
	for {
		select {
		case <-h.tosvc:
			break Loop
		case c := <-r:
			switch c.Cmd {
			case svc.Cmd(windows.SERVICE_CONTROL_PARAMCHANGE):
				// 服务参数变更通知，仅返回当前状态
				s <- c.CurrentStatus
			case svc.Interrogate:
				// SCM 查询当前状态
				s <- c.CurrentStatus
			case svc.Stop, svc.Shutdown:
				klog.Infof("Service stopping")

				s <- svc.Status{State: svc.StopPending}
				// 将 Windows 服务停止请求转换为 Go 信号，由统一的信号处理器处理。
				// Windows 服务没有控制台，无法生成 CTRL_BREAK_EVENT，因此需要专门的通知机制。
				graceful := server.RequestShutdown()

				// 继续报告 StopPending 状态，让 SCM 知道服务正在优雅关闭。
				// 如果超时，SCM 会强制终止进程。
				s <- svc.Status{State: svc.StopPending}

				// 如果无法优雅退出（如 kube-proxy 不使用信号机制），直接退出进程。
				// 延迟 1 秒确保 SCM 已收到状态更新，避免 SCM 认为服务崩溃。
				if !graceful {
					go func() {
						time.Sleep(1 * time.Second)
						os.Exit(0)
					}()
				}
				break Loop
			case svc.PreShutdown:
				// 节点预关闭事件：SCM 在关闭节点前发出，让服务有时间执行清理操作。
				// 通常用于驱逐 Pod 或转移领导权，避免数据丢失。
				klog.Infof("Node pre-shutdown")
				s <- svc.Status{State: svc.StopPending}

				if h.preshutdownHandler != nil {
					h.preshutdownHandler.ProcessShutdownEvent()
				}

				break Loop
			}
		}
	}

	return false, 0
}
