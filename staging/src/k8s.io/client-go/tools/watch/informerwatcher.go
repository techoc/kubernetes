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

package watch

import (
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// newEventProcessor 创建事件缓冲器；out 由调用方提供，done 用于通知停止。
func newEventProcessor(out chan<- watch.Event) *eventProcessor {
	return &eventProcessor{
		out:  out,
		cond: sync.NewCond(&sync.Mutex{}),
		done: make(chan struct{}),
	}
}

// eventProcessor 缓冲事件，并在有读者等待时写入 out channel。
// 由于需要缓冲事件，输入侧用条件变量同步，输出侧用 channel 同步；
// 它既要在等待输入条件时能yield，也要在阻塞写输出 channel 时能yield。
//
// 输入侧用 cond 唤醒（事件到达），输出侧用 channel + done 选择（可被停止打断），
// 这样即使消费者暂时不读取也不会阻塞 Informer 的事件处理协程太久。
type eventProcessor struct {
	out chan<- watch.Event

	cond *sync.Cond
	buff []watch.Event

	done chan struct{}
}

// run 循环取批并写出，直到收到停止信号。
func (e *eventProcessor) run() {
	for {
		batch := e.takeBatch()
		e.writeBatch(batch)
		if e.stopped() {
			return
		}
	}
}

// takeBatch 阻塞等待缓冲区非空或停止；取走当前全部事件以减少锁竞争。
func (e *eventProcessor) takeBatch() []watch.Event {
	e.cond.L.Lock()
	defer e.cond.L.Unlock()

	for len(e.buff) == 0 && !e.stopped() {
		e.cond.Wait()
	}

	batch := e.buff
	e.buff = nil
	return batch
}

// writeBatch 逐条写出；done 关闭时丢弃剩余事件并返回。
func (e *eventProcessor) writeBatch(events []watch.Event) {
	for _, event := range events {
		select {
		case e.out <- event:
		case <-e.done:
			return
		}
	}
}

// push 把事件追加到缓冲区并唤醒 run。
func (e *eventProcessor) push(event watch.Event) {
	e.cond.L.Lock()
	defer e.cond.L.Unlock()
	defer e.cond.Signal()
	e.buff = append(e.buff, event)
}

// stopped 非阻塞地检查 done 是否已关闭。
func (e *eventProcessor) stopped() bool {
	select {
	case <-e.done:
		return true
	default:
		return false
	}
}

// stop 关闭 done 并唤醒可能阻塞在 cond.Wait 的 run。
func (e *eventProcessor) stop() {
	close(e.done)
	e.cond.Signal()
}

// NewIndexerInformerWatcher 会创建 IndexerInformer 并包装成 watch.Interface，
// 可以在任何使用 Watch 方法返回的普通 Watcher 的地方使用；
// 同时返回一个 channel，用于等待 informer 完全停止。
//
// 上下文日志：支持上下文日志的代码应使用 NewIndexerInformerWatcherWithLogger 而非 NewIndexerInformerWatcher。
func NewIndexerInformerWatcher(lw cache.ListerWatcher, objType runtime.Object) (cache.Indexer, cache.Controller, watch.Interface, <-chan struct{}) {
	return NewIndexerInformerWatcherWithLogger(klog.Background(), lw, objType)
}

// NewIndexerInformerWatcherWithLogger 会创建 IndexerInformer 并包装成 watch.Interface，
// 可以在任何使用 Watch 方法返回的普通 Watcher 的地方使用；
// 同时返回一个 channel，用于等待 informer 完全停止。
//
// 返回的 watch.Interface 在首次 List 完成前不会产生事件；对调用方而言，
// 它等价于一个从“当前状态”开始的 watch。doneCh 在 Informer 退出后关闭。
func NewIndexerInformerWatcherWithLogger(logger klog.Logger, lw cache.ListerWatcher, objType runtime.Object) (cache.Indexer, cache.Controller, watch.Interface, <-chan struct{}) {
	ch := make(chan watch.Event)
	w := watch.NewProxyWatcher(ch)
	e := newEventProcessor(ch)

	indexer, informer := cache.NewIndexerInformer(lw, objType, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			e.push(watch.Event{
				Type:   watch.Added,
				Object: obj.(runtime.Object),
			})
		},
		UpdateFunc: func(old, new interface{}) {
			e.push(watch.Event{
				Type:   watch.Modified,
				Object: new.(runtime.Object),
			})
		},
		DeleteFunc: func(obj interface{}) {
			staleObj, stale := obj.(cache.DeletedFinalStateUnknown)
			if stale {
				// 基于 watch.Event 的 watch API 无法把这些额外信息传递下去，
				// 但调用方可以检查 metadata.deletionTimestamp 是否已设置
				// 来过滤这类对象
				// 用最后一次已知状态代替；调用方可通过 deletionTimestamp 判断是否为删除墓碑。
				obj = staleObj.Obj
			}

			e.push(watch.Event{
				Type:   watch.Deleted,
				Object: obj.(runtime.Object),
			})
		},
	}, cache.Indexers{})

	// 该协程会被停止，但不等待其退出。
	go e.run()

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		defer e.stop()
		// 等待 w.StopChan() 是传统行为，这里予以保留，
		// 并额外加入 logger 以支持上下文日志。
		ctx := wait.ContextForChannel(w.StopChan())
		ctx = klog.NewContext(ctx, logger)
		informer.RunWithContext(ctx)
	}()

	return indexer, informer, w, doneCh
}
