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

package signals

import (
	"context"
	"os"
	"os/signal"
)

// 用一个无缓冲 channel 当作一次性开关：close 只能成功一次，
// 第二次调用 SetupSignalHandler 会直接 panic，避免多个 goroutine 争抢信号处理权。
var onlyOneSignalHandler = make(chan struct{})

// SetupSignalHandler registered for SIGTERM and SIGINT. A context is returned
// which is cancelled on one of these signals. If a second signal is caught,
// the program is terminated with exit code 1.
//
// 把信号转成 ctx 取消，是为了让所有长驻组件共用一条退出路径；
// 第二次信号直接退出，是给第一次优雅关闭卡住时留的强制刹车。
func SetupSignalHandler() context.Context {
	close(onlyOneSignalHandler) // panics when called twice

	// 缓冲为 2：优雅关闭期间若又来一个信号，必须能被接住并触发强制退出。
	c := make(chan os.Signal, 2)
	ctx, cancel := context.WithCancel(context.Background())
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		// 第一次信号只通知，不直接退出，给各组件时间完成清理。
		cancel()
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return ctx
}
