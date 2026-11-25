/*
Copyright 2024 The Kubernetes Authors.

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

// Package workqueue 提供 k8s.io/dynamic-resource-allocation 内部使用的工具。
// 本文件给出一个线程安全的工作队列模拟实现，便于测试从队列取任务并处理的函数。
package workqueue

import (
	"maps"
	"slices"
	"sync"
	"time"

	"k8s.io/client-go/util/workqueue"
)

// TODO（pohly）：将此代码移至 k8s.io/client-go/util/workqueue/mockqueue.go。
// TODO（pohly）：若证明具有通用价值，就把它移到 client-go 的 workqueue 包中；
// 这里的文档注释已经按迁移后的位置来写了。

// Mock 是 [TypedRateLimitingInterface] 的一个实现，用于测试“从队列中取出工作项并处理”的函数。
// 它是线程安全的。
// 零值实例即可直接使用。典型用法如下：调用 SyncOne 触发同步，再用 State 检查队列状态。
//
//	var m workqueue.Mock[string]
//	m.SyncOne("some-item", func(queue workqueue.TypedRateLimitingInterface[string]) { ... })
//	if diff := cmp.Diff(workqueue.MockState[string]{}, m.State()); diff != "" {
//	    t.Errorf("unexpected state of mock work queue after sync (-want, +got):\n%s", diff)
//	}
//
// 延迟队列的行为参考 https://github.com/kubernetes/client-go/blob/270e5ab1714527c455865953da8ceba2810dbb50/util/workqueue/delaying_queue.go#L340-L349。
// 所有切片在变空时都会重置为 nil，因此不会出现 nil 与空切片导致的无意义差异。
// Mock 内部字段：mutex 保护 state 的并发访问；state 记录队列的当前快照。
type Mock[T comparable] struct {
	mutex sync.Mutex
	state MockState[T]
}

// MockState 是 Mock 队列状态的可导出快照，供测试断言使用。
type MockState[T comparable] struct {
	// Ready 保存已就绪、等待被处理的工作项。
	Ready []T

	// InFlight 保存正在处理中的工作项，即已调用 Get 但尚未调用 Done 的项。
	InFlight []T

	// MismatchedDone 保存没有对应 Get 却被调用了 Done 的工作项。
	MismatchedDone []T

	// Later 保存需要延迟入队的工作项，即调用过 AddAfter 的项，顺序与 AddAfter 调用顺序一致。
	Later []MockDelayedItem[T]

	// Failures 保存处理失败的工作项及其重试次数，即调用过 AddRateLimited 但尚未 Forget 的项；重试次数始终大于零。
	Failures map[T]int

	// ShutDownCalled 记录 ShutDown 被调用的次数。
	ShutDownCalled int

	// ShutDownWithDrainCalled 记录 ShutDownWithDrain 被调用的次数。
	ShutDownWithDrainCalled int
}

// DeepCopy 对所有切片和 map 做一次浅拷贝快照；切片中的元素本身不会被深拷贝，
// 但这些元素通常是不可变的键。
func (m MockState[T]) DeepCopy() *MockState[T] {
	// 逐一克隆各个切片与 map，避免快照与原始状态共享底层存储。
	m.Ready = slices.Clone(m.Ready)
	m.InFlight = slices.Clone(m.InFlight)
	m.MismatchedDone = slices.Clone(m.MismatchedDone)
	m.Later = slices.Clone(m.Later)
	m.Failures = maps.Clone(m.Failures)
	return &m
}

// MockDelayedItem 表示一个被安排延迟处理的工作项。
// 其中 Item 是工作项本身，Duration 是要求的延迟时长。
type MockDelayedItem[T comparable] struct {
	Item     T
	Duration time.Duration
}

// SyncOne 把 item 加入队列并调用 sync，用于驱动一次同步处理。
// sync 可以不断从队列取任务直到队列为空；随后队列会表现为正在关闭，促使其返回。
// 测试随后可通过 State 取得队列状态并检查结果。
func (m *Mock[T]) SyncOne(item T, sync func(workqueue.TypedRateLimitingInterface[T])) {
	// 用 defer 在解锁之后调用 sync，因为 sync 内部会访问队列，持锁调用会死锁。
	defer sync(m)
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 把待处理的工作项放入就绪队列。
	m.state.Ready = append(m.state.Ready, item)
}

// State 返回队列当前状态的副本，供测试断言使用。
func (m *Mock[T]) State() MockState[T] {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 返回深拷贝快照，避免调用方持有内部可变状态。
	return *m.state.DeepCopy()
}

// Add 把工作项加入就绪队列；已存在时不会重复添加。
func (m *Mock[T]) Add(item T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 先去重，避免同一个工作项重复入队。
	if !slices.Contains(m.state.Ready, item) {
		m.state.Ready = append(m.state.Ready, item)
	}
}

// Len 返回当前就绪队列中的工作项数量。
func (m *Mock[T]) Len() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return len(m.state.Ready)
}

// Get 取出就绪队列中的首个（最旧的）工作项。
// 队列为空时返回 shutdown=true，以此通知调用方停止处理循环。
func (m *Mock[T]) Get() (item T, shutdown bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 没有就绪项时视作队列关闭，返回零值与 shutdown=true。
	if len(m.state.Ready) == 0 {
		shutdown = true
		return
	}
	// 取出队首元素并前移切片。
	item = m.state.Ready[0]
	m.state.Ready = m.state.Ready[1:]
	// 队列清空后把切片置为 nil，避免 nil 与空切片造成状态差异。
	if len(m.state.Ready) == 0 {
		m.state.Ready = nil
	}
	// 取出后进入处理中状态，等待 Done。
	m.state.InFlight = append(m.state.InFlight, item)
	return item, false
}

// Done 标记一个工作项处理完毕，将其从处理中集合移除。
// 若找不到对应项，则记录为一次不匹配的 Done。
func (m *Mock[T]) Done(item T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	index := slices.Index(m.state.InFlight, item)
	// 没有匹配的 Get，记录到 MismatchedDone 中。
	if index < 0 {
		m.state.MismatchedDone = append(m.state.MismatchedDone, item)
	}
	// 从处理中集合中删除该项。
	m.state.InFlight = slices.Delete(m.state.InFlight, index, index+1)
	// 清空后重置为 nil，保持状态表示一致。
	if len(m.state.InFlight) == 0 {
		m.state.InFlight = nil
	}
}

// ShutDown 记录一次关闭调用，而不实际停止任何 goroutine。
func (m *Mock[T]) ShutDown() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 仅累加调用次数供测试断言。
	m.state.ShutDownCalled++
}

// ShutDownWithDrain 记录一次“排空后关闭”的调用。
func (m *Mock[T]) ShutDownWithDrain() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 仅累加调用次数供测试断言。
	m.state.ShutDownWithDrainCalled++
}

// ShuttingDown 报告队列是否已被要求关闭（两种方式任一被调用过即为 true）。
func (m *Mock[T]) ShuttingDown() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.state.ShutDownCalled > 0 || m.state.ShutDownWithDrainCalled > 0
}

// AddAfter 把工作项安排为延迟入队。
// 延迟为 0 时直接入队；已存在的延迟项只会被缩短延迟，不会被延长。
func (m *Mock[T]) AddAfter(item T, duration time.Duration) {
	// 零延迟等价于普通 Add。
	if duration == 0 {
		m.Add(item)
		return
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 查找是否已有同一工作项的延迟记录。
	for i := range m.state.Later {
		if m.state.Later[i].Item == item {
			// 与 client-go 行为一致：对已存在项只缩短延迟，不延长。
			if m.state.Later[i].Duration > duration {
				m.state.Later[i].Duration = duration
			}
			return
		}
	}

	// 新工作项：按调用顺序追加到延迟列表末尾。
	m.state.Later = append(m.state.Later, MockDelayedItem[T]{Item: item, Duration: duration})
}

// CancelAfter 是对 TypedDelayingInterface 的扩展，允许测试移除某个可能通过 AddAfter 加入、
// 也可能从未加入过的延迟工作项。
func (m *Mock[T]) CancelAfter(item T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 从延迟列表中删除所有匹配该项的记录。
	m.state.Later = slices.DeleteFunc(m.state.Later, func(later MockDelayedItem[T]) bool {
		return later.Item == item
	})
}

// AddRateLimited 记录一次限速重试：把工作项的重试次数加一。
func (m *Mock[T]) AddRateLimited(item T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 第一次失败时按需初始化 map。
	if m.state.Failures == nil {
		m.state.Failures = make(map[T]int)
	}
	m.state.Failures[item]++
}

// Forget 清除工作项的失败记录，表示其已被成功处理或不再重试。
func (m *Mock[T]) Forget(item T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 尚无失败记录时无需操作。
	if m.state.Failures == nil {
		return
	}
	delete(m.state.Failures, item)
}

// NumRequeues 返回某个工作项当前的重试次数，没有记录时为 0。
func (m *Mock[T]) NumRequeues(item T) int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.state.Failures[item]
}
