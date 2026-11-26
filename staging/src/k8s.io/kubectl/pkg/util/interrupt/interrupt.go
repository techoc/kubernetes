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

package interrupt

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// terminationSignals 是各支持平台（Linux、Darwin、Windows）
// 上会导致程序退出的信号。
var terminationSignals = []os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}

// Handler 保证在临界区（传给 Run 的函数）结束后通知函数
// 必定执行，即使进程被终止；且每个通知只执行一次。
type Handler struct {
	notify []func()
	final  func(os.Signal)
	once   sync.Once
}

// Chain 创建新 Handler，在临界区结束时执行自身通知，
// 再执行传入 Handler 的通知，从而支持临界区嵌套且每个通知仍只执行一次。
// 通知函数可执行任意清理，但不应退出进程，
// 退出由父 Handler 负责。
func Chain(handler *Handler, notify ...func()) *Handler {
	if handler == nil {
		return New(nil, notify...)
	}
	return New(handler.Signal, append(notify, handler.Close)...)
}

// New 创建新 Handler，保证临界区结束（或被 OS 中断）后执行所有通知，
// 再调用最终处理函数；未指定时默认为 `os.Exit(1)`。
// 一个 Handler 只能用于一个临界区。
func New(final func(os.Signal), notify ...func()) *Handler {
	return &Handler{
		final:  final,
		notify: notify,
	}
}

// 尚未执行时，Close 会执行所有通知函数。
func (h *Handler) Close() {
	h.once.Do(func() {
		for _, fn := range h.notify {
			fn()
		}
	})
}

// 收到 os.Signal 时调用，保证先执行所有通知，
// 再执行最终处理函数；每个 Handler 实例只能调用一次。
func (h *Handler) Signal(s os.Signal) {
	h.once.Do(func() {
		for _, fn := range h.notify {
			fn()
		}
		if h.final == nil {
			os.Exit(1)
		}
		h.final(s)
	})
}

// 保证 fn 退出后（即使被 OS 终止信号中断）执行所有通知；
// 每个 Handler 实例的通知只执行一次，
// 多次调用 Run 不会得到预期行为。
func (h *Handler) Run(fn func() error) error {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, terminationSignals...)
	defer func() {
		signal.Stop(ch)
		close(ch)
	}()
	go func() {
		sig, ok := <-ch
		if !ok {
			return
		}
		h.Signal(sig)
	}()
	defer h.Close()
	return fn()
}
