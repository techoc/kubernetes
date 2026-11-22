/*
Copyright 2014 The Kubernetes Authors.

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
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// FullChannelBehavior controls how the Broadcaster reacts if a watcher's watch
// channel is full.
//
// FullChannelBehavior 控制某个 watcher 的 watch 通道已满时 Broadcaster 的行为。
type FullChannelBehavior int

const (
	// WaitIfChannelFull 表示等待该 watcher 的通道腾出空间，可能拖慢所有 watcher。
	WaitIfChannelFull FullChannelBehavior = iota
	// DropIfChannelFull 表示直接丢弃发往该 watcher 的事件，不阻塞其他 watcher。
	DropIfChannelFull
)

// Buffer the incoming queue a little bit even though it should rarely ever accumulate
// anything, just in case a few events are received in such a short window that
// Broadcaster can't move them onto the watchers' queues fast enough.
//
// 给 incoming 队列留一点缓冲：虽然通常不会积压，但短时间内集中到达的事件
// 可能让 Broadcaster 来不及搬到各 watcher 的队列。
const incomingQueueLength = 25

// Broadcaster distributes event notifications among any number of watchers. Every event
// is delivered to every watcher.
//
// Broadcaster 在任意数量的 watcher 之间分发事件通知：每个事件都会送达每个 watcher。
type Broadcaster struct {
	watchers     map[int64]*broadcasterWatcher
	nextWatcher  int64
	distributing sync.WaitGroup

	// incomingBlock allows us to ensure we don't race and end up sending events
	// to a closed channel following a broadcaster shutdown.
	//
	// incomingBlock 确保在 Broadcaster 关闭后不会发生竞态、向已关闭的通道发送事件。
	incomingBlock sync.Mutex
	incoming      chan Event
	stopped       chan struct{}

	// How large to make watcher's channel.
	//
	// watchQueueLength 是每个 watcher 通道的容量。
	watchQueueLength int
	// If one of the watch channels is full, don't wait for it to become empty.
	// Instead just deliver it to the watchers that do have space in their
	// channels and move on to the next event.
	// It's more fair to do this on a per-watcher basis than to do it on the
	// "incoming" channel, which would allow one slow watcher to prevent all
	// other watchers from getting new events.
	//
	// 当某个 watch 通道已满时，不等待它变空，而是只投递给还有空间的 watcher，
	// 然后处理下一个事件。按 watcher 而非按 "incoming" 通道处理更公平：
	// 后者会让一个慢 watcher 阻塞所有其他 watcher 接收新事件。
	fullChannelBehavior FullChannelBehavior
}

// NewBroadcaster creates a new Broadcaster. queueLength is the maximum number of events to queue per watcher.
// It is guaranteed that events will be distributed in the order in which they occur,
// but the order in which a single event is distributed among all of the watchers is unspecified.
//
// NewBroadcaster 创建 Broadcaster；queueLength 是每个 watcher 的最大排队事件数。
// 事件保证按发生顺序分发，但单个事件在各 watcher 之间的投递顺序未定义。
func NewBroadcaster(queueLength int, fullChannelBehavior FullChannelBehavior) *Broadcaster {
	m := &Broadcaster{
		watchers:            map[int64]*broadcasterWatcher{},
		incoming:            make(chan Event, incomingQueueLength),
		stopped:             make(chan struct{}),
		watchQueueLength:    queueLength,
		fullChannelBehavior: fullChannelBehavior,
	}
	m.distributing.Add(1)
	go m.loop()
	return m
}

// NewLongQueueBroadcaster functions nearly identically to NewBroadcaster,
// except that the incoming queue is the same size as the outgoing queues
// (specified by queueLength).
//
// NewLongQueueBroadcaster 与 NewBroadcaster 几乎相同，
// 区别是 incoming 队列容量与各 watcher 的出站队列相同（由 queueLength 指定）。
func NewLongQueueBroadcaster(queueLength int, fullChannelBehavior FullChannelBehavior) *Broadcaster {
	m := &Broadcaster{
		watchers:            map[int64]*broadcasterWatcher{},
		incoming:            make(chan Event, queueLength),
		stopped:             make(chan struct{}),
		watchQueueLength:    queueLength,
		fullChannelBehavior: fullChannelBehavior,
	}
	m.distributing.Add(1)
	go m.loop()
	return m
}

// internalRunFunctionMarker 是内部专用的事件类型，用于把函数塞进 incoming 队列执行，
// 与真实 watch 事件区分。
const internalRunFunctionMarker = "internal-do-function"

// a function type we can shoehorn into the queue.
//
// functionFakeRuntimeObject 使函数能作为 runtime.Object 塞进事件队列。
type functionFakeRuntimeObject func()

func (obj functionFakeRuntimeObject) GetObjectKind() schema.ObjectKind {
	return schema.EmptyObjectKind
}
func (obj functionFakeRuntimeObject) DeepCopyObject() runtime.Object {
	if obj == nil {
		return nil
	}
	// 函数不可变，直接返回原函数即可。
	return obj
}

// Execute f, blocking the incoming queue (and waiting for it to drain first).
// The purpose of this terrible hack is so that watchers added after an event
// won't ever see that event, and will always see any event after they are
// added.
//
// blockQueue 阻塞 incoming 队列（先等其排空）后执行 f。
// 这样做的目的是：在某个事件之后添加的 watcher 绝不会看到该事件，
// 而一定能看到添加之后的事件。
func (m *Broadcaster) blockQueue(f func()) {
	m.incomingBlock.Lock()
	defer m.incomingBlock.Unlock()
	select {
	case <-m.stopped:
		return
	default:
	}
	var wg sync.WaitGroup
	wg.Add(1)
	m.incoming <- Event{
		Type: internalRunFunctionMarker,
		Object: functionFakeRuntimeObject(func() {
			defer wg.Done()
			f()
		}),
	}
	wg.Wait()
}

// Watch adds a new watcher to the list and returns an Interface for it.
// Note: new watchers will only receive new events. They won't get an entire history
// of previous events. It will block until the watcher is actually added to the
// broadcaster.
//
// Watch 向列表添加新 watcher 并返回其 Interface。
// 注意：新 watcher 只接收新事件，不会获得历史事件；函数会阻塞到 watcher 真正加入为止。
func (m *Broadcaster) Watch() (Interface, error) {
	var w *broadcasterWatcher
	m.blockQueue(func() {
		id := m.nextWatcher
		m.nextWatcher++
		w = &broadcasterWatcher{
			result:  make(chan Event, m.watchQueueLength),
			stopped: make(chan struct{}),
			id:      id,
			m:       m,
		}
		m.watchers[id] = w
	})
	if w == nil {
		return nil, fmt.Errorf("broadcaster already stopped")
	}
	return w, nil
}

// WatchWithPrefix adds a new watcher to the list and returns an Interface for it. It sends
// queuedEvents down the new watch before beginning to send ordinary events from Broadcaster.
// The returned watch will have a queue length that is at least large enough to accommodate
// all of the items in queuedEvents. It will block until the watcher is actually added to
// the broadcaster.
//
// WatchWithPrefix 添加新 watcher 并返回其 Interface：先向新 watch 发送 queuedEvents，
// 再开始发送 Broadcaster 的普通事件。返回的 watch 队列容量至少能容纳 queuedEvents 全部条目；
// 函数会阻塞到 watcher 真正加入为止。
func (m *Broadcaster) WatchWithPrefix(queuedEvents []Event) (Interface, error) {
	var w *broadcasterWatcher
	m.blockQueue(func() {
		id := m.nextWatcher
		m.nextWatcher++
		length := m.watchQueueLength
		// 队列至少要能放下预置事件，另留 1 个空位避免立即满。
		if n := len(queuedEvents) + 1; n > length {
			length = n
		}
		w = &broadcasterWatcher{
			result:  make(chan Event, length),
			stopped: make(chan struct{}),
			id:      id,
			m:       m,
		}
		m.watchers[id] = w
		for _, e := range queuedEvents {
			w.result <- e
		}
	})
	if w == nil {
		return nil, fmt.Errorf("broadcaster already stopped")
	}
	return w, nil
}

// stopWatching stops the given watcher and removes it from the list.
//
// stopWatching 停止指定 watcher 并从列表中移除。
func (m *Broadcaster) stopWatching(id int64) {
	m.blockQueue(func() {
		w, ok := m.watchers[id]
		if !ok {
			// 已从列表移除，无需处理。
			return
		}
		delete(m.watchers, id)
		close(w.result)
	})
}

// closeAll disconnects all watchers (presumably in response to a Shutdown call).
//
// closeAll 断开所有 watcher（通常由 Shutdown 调用触发）。
func (m *Broadcaster) closeAll() {
	for _, w := range m.watchers {
		close(w.result)
	}
	// 清空 map：stopWatching 依赖 map 中是否存在该 id 来避免重复 close 通道。
	m.watchers = map[int64]*broadcasterWatcher{}
}

// Action distributes the given event among all watchers.
//
// Action 把给定事件分发给所有 watcher；Broadcaster 已关闭时返回错误。
func (m *Broadcaster) Action(action EventType, obj runtime.Object) error {
	m.incomingBlock.Lock()
	defer m.incomingBlock.Unlock()
	select {
	case <-m.stopped:
		return fmt.Errorf("broadcaster already stopped")
	default:
	}

	m.incoming <- Event{action, obj}
	return nil
}

// Action distributes the given event among all watchers, or drops it on the floor
// if too many incoming actions are queued up.  Returns true if the action was sent,
// false if dropped.
//
// ActionOrDrop 把事件分发给所有 watcher；incoming 队列已满时直接丢弃。
// 发送成功返回 true，丢弃返回 false；Broadcaster 已关闭时返回错误。
func (m *Broadcaster) ActionOrDrop(action EventType, obj runtime.Object) (bool, error) {
	m.incomingBlock.Lock()
	defer m.incomingBlock.Unlock()

	// 确保 Broadcaster 已停止时不再发送事件。
	select {
	case <-m.stopped:
		return false, fmt.Errorf("broadcaster already stopped")
	default:
	}

	select {
	case m.incoming <- Event{action, obj}:
		return true, nil
	default:
		return false, nil
	}
}

// Shutdown disconnects all watchers (but any queued events will still be distributed).
// You must not call Action or Watch* after calling Shutdown. This call blocks
// until all events have been distributed through the outbound channels. Note
// that since they can be buffered, this means that the watchers might not
// have received the data yet as it can remain sitting in the buffered
// channel. It will block until the broadcaster stop request is actually executed
//
// Shutdown 断开所有 watcher（已排队的事件仍会被分发）。调用后不得再调用 Action 或 Watch*。
// 该调用会阻塞到所有事件经出站通道分发完毕；由于通道有缓冲，watcher 可能尚未真正读到数据
// （数据仍停留在缓冲通道中）。函数会阻塞到停止请求真正执行。
func (m *Broadcaster) Shutdown() {
	m.blockQueue(func() {
		close(m.stopped)
		close(m.incoming)
	})
	m.distributing.Wait()
}

// loop receives from m.incoming and distributes to all watchers.
//
// loop 从 m.incoming 接收事件并分发给所有 watcher。
func (m *Broadcaster) loop() {
	// 故意不捕获 panic：watch.Broadcaster 有 bug 时应让进程崩溃以便暴露问题。
	for event := range m.incoming {
		if event.Type == internalRunFunctionMarker {
			event.Object.(functionFakeRuntimeObject)()
			continue
		}
		m.distribute(event)
	}
	m.closeAll()
	m.distributing.Done()
}

// distribute sends event to all watchers. Blocking.
//
// distribute 把事件发送给所有 watcher；是否阻塞取决于 fullChannelBehavior。
func (m *Broadcaster) distribute(event Event) {
	if m.fullChannelBehavior == DropIfChannelFull {
		for _, w := range m.watchers {
			select {
			case w.result <- event:
			case <-w.stopped:
			default: // 无法入队时不要阻塞。
			}
		}
	} else {
		for _, w := range m.watchers {
			select {
			case w.result <- event:
			case <-w.stopped:
			}
		}
	}
}

// broadcasterWatcher handles a single watcher of a broadcaster
//
// broadcasterWatcher 是 Broadcaster 中单个 watcher 的实现。
type broadcasterWatcher struct {
	result  chan Event
	stopped chan struct{}
	stop    sync.Once
	id      int64
	m       *Broadcaster
}

// ResultChan returns a channel to use for waiting on events.
//
// ResultChan 返回用于等待事件的通道。
func (mw *broadcasterWatcher) ResultChan() <-chan Event {
	return mw.result
}

// Stop stops watching and removes mw from its list.
// It will block until the watcher stop request is actually executed
//
// Stop 停止 watch 并从列表移除 mw；会阻塞到停止请求真正执行。
// 借助 sync.Once 保证重复调用安全。
func (mw *broadcasterWatcher) Stop() {
	mw.stop.Do(func() {
		close(mw.stopped)
		mw.m.stopWatching(mw.id)
	})
}
