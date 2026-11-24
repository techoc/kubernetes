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

// FullChannelBehavior 控制 Broadcaster 在某个 watcher 的事件通道已满时如何处理。
//
// FullChannelBehavior 控制某个 watcher 的 watch 通道已满时 Broadcaster 的行为。
type FullChannelBehavior int

const (
	// WaitIfChannelFull 表示等待该 watcher 的通道腾出空间，可能拖慢所有 watcher。
	WaitIfChannelFull FullChannelBehavior = iota
	// DropIfChannelFull 表示直接丢弃发往该 watcher 的事件，不阻塞其他 watcher。
	DropIfChannelFull
)

// incoming 队列保留少量缓冲。正常情况下事件很少积压，但短时间内集中到达时，
// Broadcaster 可能来不及将事件转移到各 watcher 的队列中。
//
// 给 incoming 队列留一点缓冲：虽然通常不会积压，但短时间内集中到达的事件
// 可能让 Broadcaster 来不及搬到各 watcher 的队列。
const incomingQueueLength = 25

// Broadcaster 在任意数量的 watcher 之间分发事件通知，每个事件都会发送给所有 watcher。
//
// Broadcaster 在任意数量的 watcher 之间分发事件通知：每个事件都会送达每个 watcher。
type Broadcaster struct {
	watchers     map[int64]*broadcasterWatcher
	nextWatcher  int64
	distributing sync.WaitGroup

	// incomingBlock 用于串行化发送与关闭操作，避免 Broadcaster 关闭后仍向已关闭的通道发送事件。
	//
	// incomingBlock 确保在 Broadcaster 关闭后不会发生竞态、向已关闭的通道发送事件。
	incomingBlock sync.Mutex
	incoming      chan Event
	stopped       chan struct{}

	// 每个 watcher 的事件通道容量。
	//
	// watchQueueLength 是每个 watcher 通道的容量。
	watchQueueLength int
	// 某个 watcher 的通道已满时，不等待它腾出空间，而是继续投递给其他仍有空间的 watcher，
	// 随后处理下一个事件。按 watcher 单独处理更公平；如果在 incoming 队列层面阻塞，
	// 一个消费缓慢的 watcher 就可能阻止所有其他 watcher 收到新事件。
	//
	// 当某个 watch 通道已满时，不等待它变空，而是只投递给还有空间的 watcher，
	// 然后处理下一个事件。按 watcher 而非按 "incoming" 通道处理更公平：
	// 后者会让一个慢 watcher 阻塞所有其他 watcher 接收新事件。
	fullChannelBehavior FullChannelBehavior
}

// NewBroadcaster 创建 Broadcaster。queueLength 是每个 watcher 可排队的最大事件数。
// 事件按发生顺序分发，但同一个事件在不同 watcher 之间的具体投递顺序不作保证。
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

// NewLongQueueBroadcaster 与 NewBroadcaster 基本相同，区别在于 incoming 队列容量
// 也设置为 queueLength，与每个 watcher 的出站队列容量一致。
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

// functionFakeRuntimeObject 将函数包装为可放入事件队列的 runtime.Object。
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

// blockQueue 先阻塞 incoming 队列并等待队列中的既有事件处理完成，再执行 f。
// 这样可以保证新加入的 watcher 不会收到加入之前发生的事件，但一定能收到加入之后的事件。
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

// Watch 将新 watcher 加入列表并返回对应的 Interface。新 watcher 只接收加入之后的事件，
// 不会获得此前的历史事件；该方法会阻塞，直到 watcher 真正加入 Broadcaster。
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

// WatchWithPrefix 将新 watcher 加入列表并返回对应的 Interface。在发送 Broadcaster 的普通事件前，
// 先向新 watch 发送 queuedEvents。返回的 watch 队列容量至少能够容纳 queuedEvents 中的全部事件；
// 该方法会阻塞，直到 watcher 真正加入 Broadcaster。
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

// stopWatching 停止指定 watcher，并将其从 Broadcaster 的 watcher 列表中移除。
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

// closeAll 断开所有 watcher，通常由 Shutdown 调用触发。
//
// closeAll 断开所有 watcher（通常由 Shutdown 调用触发）。
func (m *Broadcaster) closeAll() {
	for _, w := range m.watchers {
		close(w.result)
	}
	// 清空 map：stopWatching 依赖 map 中是否存在该 id 来避免重复 close 通道。
	m.watchers = map[int64]*broadcasterWatcher{}
}

// Action 将指定事件加入 incoming 队列，等待分发给所有 watcher。
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

// ActionOrDrop 将指定事件加入 incoming 队列；如果队列已满则直接丢弃。
// 成功入队返回 true，因队列已满而丢弃返回 false。
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

// Shutdown 断开所有 watcher，但队列中尚未处理的事件仍会继续分发。调用后不得再调用 Action 或 Watch*。
// 该方法会阻塞，直到所有事件都已写入各 watcher 的出站通道。由于出站通道可能带缓冲，
// 这并不意味着 watcher 已经读取事件，数据仍可能留在通道缓冲区中。
// 方法会一直等待，直到 Broadcaster 真正执行停止请求。
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

// loop 从 m.incoming 接收事件，并将其分发给所有 watcher。
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

// distribute 将事件发送给所有 watcher；具体是否阻塞由 fullChannelBehavior 决定。
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

// broadcasterWatcher 表示 Broadcaster 中的一个 watcher。
//
// broadcasterWatcher 是 Broadcaster 中单个 watcher 的实现。
type broadcasterWatcher struct {
	result  chan Event
	stopped chan struct{}
	stop    sync.Once
	id      int64
	m       *Broadcaster
}

// ResultChan 返回用于接收 watch 事件的通道。
//
// ResultChan 返回用于等待事件的通道。
func (mw *broadcasterWatcher) ResultChan() <-chan Event {
	return mw.result
}

// Stop 停止接收事件并将 mw 从 Broadcaster 中移除；会阻塞到停止请求真正执行。
//
// Stop 停止 watch 并从列表移除 mw；会阻塞到停止请求真正执行。
// 借助 sync.Once 保证重复调用安全。
func (mw *broadcasterWatcher) Stop() {
	mw.stop.Do(func() {
		close(mw.stopped)
		mw.m.stopWatching(mw.id)
	})
}
