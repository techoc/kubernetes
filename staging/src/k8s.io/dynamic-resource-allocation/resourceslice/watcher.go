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

package resourceslice

import (
	"context"

	"k8s.io/apimachinery/pkg/watch"
)

// newWrapWatcher 创建一个包装 watcher：后台从底层 watch 接收事件，仅转发通过 match 的事件。
func newWrapWatcher(ctx context.Context, w watch.Interface, match func(event watch.Event) bool) *wrapWatcher {
	// 派生可取消的上下文，用于停止接收 goroutine。
	ctx, cancel := context.WithCancel(ctx)

	// 初始化包装 watcher 的事件通道与完成信号。
	watcher := &wrapWatcher{
		watcher: w,
		match:   match,
		ctx:     ctx,
		cancel:  cancel,
		result:  make(chan watch.Event),
		done:    make(chan struct{}),
	}
	// 启动后台接收 goroutine。
	go watcher.receive(ctx)

	return watcher
}

// wrapWatcher 对底层 watch.Interface 做事件过滤，并支持通过上下文优雅停止。
type wrapWatcher struct {
	// watcher 是底层 watch 对象；match 判断事件是否转发，nil 表示全部转发。
	watcher watch.Interface
	match   func(event watch.Event) bool

	// ctx 控制接收 goroutine 的生命周期，cancel 用于取消；result 输出过滤后的事件，done 在退出时关闭供 Stop 等待。
	ctx    context.Context
	cancel context.CancelFunc
	result chan watch.Event
	done   chan struct{}
}

// receive 在后台读取底层事件，过滤后写入 result；上下文取消或底层通道关闭时退出。
func (w *wrapWatcher) receive(ctx context.Context) {
	// 退出时关闭结果通道与完成信号。
	defer close(w.result)
	defer close(w.done)
	// 获取底层事件通道。
	resultChan := w.watcher.ResultChan()
	for {
		// 循环等待上下文取消或新事件。
		select {
		// 上下文取消则退出。
		case <-ctx.Done():
			return
		// 从底层通道读取事件。
		case event, ok := <-resultChan:
			// 底层通道关闭则退出。
			if !ok {
				return
			}
			// 无匹配函数或通过匹配时转发。
			if w.match == nil || w.match(event) {
				select {
				// 转发时同样尊重上下文取消。
				case <-ctx.Done():
					return
				// 将事件写入结果通道。
				case w.result <- event:
				}
			}
		}
	}
}

// ResultChan 返回过滤后事件的输出通道。
func (w *wrapWatcher) ResultChan() <-chan watch.Event {
	return w.result
}

// Stop 停止底层 watch 并等待接收 goroutine 退出，可安全重复调用。
func (w *wrapWatcher) Stop() {
	// 若上下文已取消则说明已停止。
	select {
	case <-w.ctx.Done():
	default:
		// 停止底层 watch 并取消接收 goroutine。
		w.watcher.Stop()
		w.cancel()
	}
	// 等待接收 goroutine 真正退出。
	<-w.done
}
