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

package registry

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// decoratedWatcher 在底层 watch 与消费者之间插入一个装饰器，对事件对象
// 做额外处理（例如填充 status），同时负责关闭时的级联取消。
type decoratedWatcher struct {
	w         watch.Interface
	decorator func(runtime.Object)
	cancel    context.CancelFunc
	resultCh  chan watch.Event
}

// newDecoratedWatcher 启动一个后台 goroutine 转发事件；调用方必须最终
// 调用 Stop 或取消 ctx，否则会泄漏 goroutine。
func newDecoratedWatcher(ctx context.Context, w watch.Interface, decorator func(runtime.Object)) *decoratedWatcher {
	ctx, cancel := context.WithCancel(ctx)
	d := &decoratedWatcher{
		w:         w,
		decorator: decorator,
		cancel:    cancel,
		resultCh:  make(chan watch.Event),
	}
	go d.run(ctx)
	return d
}

// run decorates watch events from the underlying watcher until its result channel
// is closed or the passed in context is done.
// When run() returns, decoratedWatcher#resultCh is closed.
func (d *decoratedWatcher) run(ctx context.Context) {
	var recv, send watch.Event
	var ok bool
	defer close(d.resultCh)
	for {
		select {
		case recv, ok = <-d.w.ResultChan():
			if !ok {
				// The underlying channel was closed, cancel our context
				d.cancel()
				return
			}
			switch recv.Type {
			case watch.Added, watch.Modified, watch.Deleted, watch.Bookmark:
				// 只对携带完整对象的常规事件做装饰；Bookmark 也带对象，
				// 需要同样处理以保持消费者一致。
				d.decorator(recv.Object)
				send = recv
			case watch.Error:
				// 错误事件的对象是 Status，不应交给装饰器。
				send = recv
			}
			// send 是零值时（未知事件类型）仍会向下转发，保持与底层
			// watch 的行为一致，避免静默丢弃。
			select {
			case d.resultCh <- send:
				// propagated event successfully
			case <-ctx.Done():
				// context timed out or was cancelled, stop the underlying watcher
				d.w.Stop()
				return
			}
		case <-ctx.Done():
			// context timed out or was cancelled, stop the underlying watcher
			d.w.Stop()
			return
		}
	}
}

// Stop 先停止底层 watcher，再取消上下文；顺序保证 run 不会在停止后
// 继续从 ResultChan 读取。
func (d *decoratedWatcher) Stop() {
	// stop the underlying watcher
	d.w.Stop()
	// cancel our context
	d.cancel()
}

// ResultChan 返回只读的事件通道；通道由 run 在退出时关闭。
func (d *decoratedWatcher) ResultChan() <-chan watch.Event {
	return d.resultCh
}
