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

// terminationSignals 是在支持的平台（Linux、Darwin、Windows）上会导致程序退出的信号集合。
var terminationSignals = []os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}

// Handler 保证在临界区（传给 Run 的函数）结束后执行通知回调，
// 即使进程收到终止信号也不例外；每个通知回调保证只执行一次。
type Handler struct {
	notify []func()
	final  func(os.Signal)
	once   sync.Once
}

// New 创建一个 Handler，保证临界区结束（或被 OS 中断）后执行所有 notify 回调，
// 随后调用 final 回调。未指定 final 时默认执行 os.Exit(1)。
// 一个 Handler 只能用于一个临界区。
func New(final func(os.Signal), notify ...func()) *Handler {
	return &Handler{
		final:  final,
		notify: notify,
	}
}

// Close 在通知回调尚未执行过时执行全部回调。
func (h *Handler) Close() {
	h.once.Do(func() {
		for _, fn := range h.notify {
			fn()
		}
	})
}

// Signal 在收到 os.Signal 时调用，保证先执行所有通知回调，再执行 final 回调。
// 每个 Handler 实例只应调用一次。
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

// Run 保证在 fn 退出后（即使进程被 OS 终止信号中断）执行所有通知回调。
// 通知每个 Handler 实例只执行一次，因此不要对同一实例多次调用 Run。
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
