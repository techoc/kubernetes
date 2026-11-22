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

// eventProcessor buffers events and writes them to an out chan when a reader
// is waiting. Because of the requirement to buffer events, it synchronizes
// input with a condition, and synchronizes output with a channels. It needs to
// be able to yield while both waiting on an input condition and while blocked
// on writing to the output channel.
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

// NewIndexerInformerWatcher will create an IndexerInformer and wrap it into watch.Interface
// so you can use it anywhere where you'd have used a regular Watcher returned from Watch method.
// it also returns a channel you can use to wait for the informers to fully shutdown.
//
// Contextual logging: NewIndexerInformerWatcherWithLogger should be used instead of NewIndexerInformerWatcher in code which supports contextual logging.
func NewIndexerInformerWatcher(lw cache.ListerWatcher, objType runtime.Object) (cache.Indexer, cache.Controller, watch.Interface, <-chan struct{}) {
	return NewIndexerInformerWatcherWithLogger(klog.Background(), lw, objType)
}

// NewIndexerInformerWatcherWithLogger will create an IndexerInformer and wrap it into watch.Interface
// so you can use it anywhere where you'd have used a regular Watcher returned from Watch method.
// it also returns a channel you can use to wait for the informers to fully shutdown.
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
				// We have no means of passing the additional information down using
				// watch API based on watch.Event but the caller can filter such
				// objects by checking if metadata.deletionTimestamp is set
				// 用最后一次已知状态代替；调用方可通过 deletionTimestamp 判断是否为删除墓碑。
				obj = staleObj.Obj
			}

			e.push(watch.Event{
				Type:   watch.Deleted,
				Object: obj.(runtime.Object),
			})
		},
	}, cache.Indexers{})

	// This will get stopped, but without waiting for it.
	go e.run()

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		defer e.stop()
		// Waiting for w.StopChan() is the traditional behavior which gets
		// preserved here, with the logger added to support contextual logging.
		ctx := wait.ContextForChannel(w.StopChan())
		ctx = klog.NewContext(ctx, logger)
		informer.RunWithContext(ctx)
	}()

	return indexer, informer, w, doneCh
}
