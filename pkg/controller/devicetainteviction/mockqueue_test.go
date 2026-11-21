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

package devicetainteviction

import (
	"maps"
	"slices"
	"sync"
	"time"

	"k8s.io/client-go/util/workqueue"
)

// TODO (pohly): 若证明该实现具有通用价值，将其移至 k8s.io/client-go/util/workqueue/mockqueue.go。
// 当前文档注释已按目标位置的代码风格撰写。

// Mock 是 [TypedRateLimitingInterface] 的测试实现，可用于测试从队列取出
// 工作项并处理的函数。它是线程安全的。
//
// 零值实例即可直接使用。典型用法如下：
//
//	var m workqueue.Mock[string]
//	m.SyncOne("some-item", func(queue workqueue.TypedRateLimitingInterface[string]) { ... } )
//	if diff := cmp.Diff(workqueue.MockState[string]{}, m.State()); diff != "" {
//	    t.Errorf("unexpected state of mock work queue after sync (-want, +got):\n%s", diff)
//	}
//
// 所有切片在变空时都会重置为 nil，避免 nil 与空切片差异带来的伪断言失败。
type Mock[T comparable] struct {
	mutex sync.Mutex
	state MockState[T]
}

// MockState 记录模拟工作队列在某一时刻的完整状态，供测试断言使用。
type MockState[T comparable] struct {
	// Ready 是已就绪、等待处理的工作项。
	Ready []T

	// InFlight 是正在处理的工作项（已调用 Get，尚未调用 Done）。
	InFlight []T

	// MismatchedDone 记录没有对应 Get 就被调用 Done 的工作项，用于暴露配对错误。
	MismatchedDone []T

	// Later 是延迟入队的工作项（调用了 AddAfter），按 AddAfter 的调用顺序排列。
	Later []MockDelayedItem[T]

	// Failures 记录处理失败的工作项及其重试次数（至少调用过一次 AddRateLimited，
	// 且尚未调用 Forget）。重试计数始终大于零。
	Failures map[T]int

	// ShutDownCalled 记录 ShutDown 被调用的次数。
	ShutDownCalled int

	// ShutDownWithDrainCalled 记录 ShutDownWithDrain 被调用的次数。
	ShutDownWithDrainCalled int
}

// DeepCopy 对所有切片做快照。它无法深拷贝切片中的工作项本身，
// 但这些键通常是不可变的。
func (m MockState[T]) DeepCopy() *MockState[T] {
	m.Ready = slices.Clone(m.Ready)
	m.InFlight = slices.Clone(m.InFlight)
	m.MismatchedDone = slices.Clone(m.MismatchedDone)
	m.Later = slices.Clone(m.Later)
	m.Failures = maps.Clone(m.Failures)
	return &m
}

// MockDelayedItem 表示一个被排入延迟处理队列的工作项及其延迟时长。
type MockDelayedItem[T comparable] struct {
	Item     T
	Duration time.Duration
}

// SyncOne 将工作项加入队列并调用 sync 函数。sync 可以反复从队列取出
// 工作项直到队列为空，随后队列会表现为关闭状态，促使 sync 返回。
//
// 测试随后可通过 State 获取队列状态并检查结果。
func (m *Mock[T]) SyncOne(item T, sync func(workqueue.TypedRateLimitingInterface[T])) {
	// sync 必须在未持有互斥锁时执行，否则其内部对队列的方法调用会死锁。
	defer sync(m)
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.state.Ready = append(m.state.Ready, item)
}

// State 返回队列当前状态的快照。
func (m *Mock[T]) State() MockState[T] {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return *m.state.DeepCopy()
}

// Add 实现 [TypedInterface]。
func (m *Mock[T]) Add(item T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 工作队列语义要求同一项在 Ready 中不重复。
	if !slices.Contains(m.state.Ready, item) {
		m.state.Ready = append(m.state.Ready, item)
	}
}

// Len 实现 [TypedInterface]。
func (m *Mock[T]) Len() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return len(m.state.Ready)
}

// Get 实现 [TypedInterface]。
func (m *Mock[T]) Get() (item T, shutdown bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if len(m.state.Ready) == 0 {
		// 没有就绪项时向调用方报告关闭，令处理循环退出。
		shutdown = true
		return
	}
	item = m.state.Ready[0]
	m.state.Ready = m.state.Ready[1:]
	if len(m.state.Ready) == 0 {
		m.state.Ready = nil
	}
	m.state.InFlight = append(m.state.InFlight, item)
	return item, false
}

// Done 实现 [TypedInterface]。
func (m *Mock[T]) Done(item T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	index := slices.Index(m.state.InFlight, item)
	// 未找到对应 Get 的 Done 单独记录，便于测试发现错误的调用配对。
	if index < 0 {
		m.state.MismatchedDone = append(m.state.MismatchedDone, item)
	}
	m.state.InFlight = slices.Delete(m.state.InFlight, index, index+1)
	if len(m.state.InFlight) == 0 {
		m.state.InFlight = nil
	}
}

// ShutDown 实现 [TypedInterface]。
func (m *Mock[T]) ShutDown() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.state.ShutDownCalled++
}

// ShutDownWithDrain 实现 [TypedInterface]。
func (m *Mock[T]) ShutDownWithDrain() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.state.ShutDownWithDrainCalled++
}

// ShuttingDown 实现 [TypedInterface]。
func (m *Mock[T]) ShuttingDown() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.state.ShutDownCalled > 0 || m.state.ShutDownWithDrainCalled > 0
}

// AddAfter 实现 [TypedDelayingInterface.AddAfter]。
func (m *Mock[T]) AddAfter(item T, duration time.Duration) {
	if duration == 0 {
		m.Add(item)
		return
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	for i := range m.state.Later {
		if m.state.Later[i].Item == item {
			// https://github.com/kubernetes/client-go/blob/270e5ab1714527c455865953da8ceba2810dbb50/util/workqueue/delaying_queue.go#L340-L349
			// 与真实延迟队列一致：对已存在的项只能缩短延迟，不能延长。
			if m.state.Later[i].Duration > duration {
				m.state.Later[i].Duration = duration
			}
			return
		}
	}

	m.state.Later = append(m.state.Later, MockDelayedItem[T]{Item: item, Duration: duration})
}

// CancelAfter 是对 TypedDelayingInterface 的测试扩展：允许移除一个此前可能通过 AddAfter 加入、也可能未加入的工作项。
func (m *Mock[T]) CancelAfter(item T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.state.Later = slices.DeleteFunc(m.state.Later, func(later MockDelayedItem[T]) bool {
		return later.Item == item
	})
}

// AddRateLimited 实现 [TypedRateLimitingInterface.AddRateLimited]。
func (m *Mock[T]) AddRateLimited(item T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.state.Failures == nil {
		m.state.Failures = make(map[T]int)
	}
	m.state.Failures[item]++
}

// Forget 实现 [TypedRateLimitingInterface.Forget]。
func (m *Mock[T]) Forget(item T) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.state.Failures == nil {
		return
	}
	delete(m.state.Failures, item)
}

// NumRequeues 实现 [TypedRateLimitingInterface.NumRequeues]。
func (m *Mock[T]) NumRequeues(item T) int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.state.Failures[item]
}
