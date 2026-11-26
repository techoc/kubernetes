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

package server

import (
	"context"
	"os"
	"os/signal"
)

// onlyOneSignalHandler 通过 close 实现“只能初始化一次”的断言：
// 重复调用会 panic，提示调用方流程有误。
var onlyOneSignalHandler = make(chan struct{})
var shutdownHandler chan os.Signal

// SetupSignalHandler 注册 SIGTERM 与 SIGINT。返回的 stop channel
// 在收到任一信号时关闭。若收到第二次信号，程序
// 以退出码 1 终止。
// SetupSignalContext 与 SetupSignalHandler 只能调用其中一个，
// 且只能调用一次。
func SetupSignalHandler() <-chan struct{} {
	return SetupSignalContext().Done()
}

// SetupSignalContext 与 SetupSignalHandler 相同，但返回 context.Context。
// SetupSignalContext 与 SetupSignalHandler 只能调用其中一个，
// 且只能调用一次。
//
// 第一次信号触发优雅关闭（cancel context），第二次信号直接退出，
// 保证关闭卡住时运维仍能强制终止进程。
func SetupSignalContext() context.Context {
	close(onlyOneSignalHandler) // panics when called twice

	// 缓冲 2 以容纳“第二次信号”，避免 goroutine 尚未处理时被阻塞。
	shutdownHandler = make(chan os.Signal, 2)

	ctx, cancel := context.WithCancel(context.Background())
	signal.Notify(shutdownHandler, shutdownSignals...)
	go func() {
		<-shutdownHandler
		cancel()
		<-shutdownHandler
		os.Exit(1) // second signal. Exit directly.
	}()

	return ctx
}

// RequestShutdown 模拟收到被视为关闭信号（SIGTERM/SIGINT）的事件
// 返回是否有 handler 收到了通知
//
// 用于测试或内部触发关闭；非阻塞发送，无人监听时返回 false。
func RequestShutdown() bool {
	if shutdownHandler != nil {
		select {
		case shutdownHandler <- shutdownSignals[0]:
			return true
		default:
		}
	}

	return false
}
