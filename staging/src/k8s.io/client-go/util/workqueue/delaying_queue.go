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

package workqueue

import (
	"container/heap"
	"sync"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// DelayingInterface 是可延迟入队的 Interface，便于在失败后重新入队而不会陷入
// 忙循环。
//
// Deprecated: use TypedDelayingInterface instead.
type DelayingInterface TypedDelayingInterface[any]

// TypedDelayingInterface 是可延迟入队的 Interface，便于在失败后重新入队而不会
// 陷入忙循环。
type TypedDelayingInterface[T comparable] interface {
	TypedInterface[T]
	// AddAfter 在给定时长之后把条目加入工作队列。
	//
	// duration <= 0 时立即入队；队列关闭后调用是安全的空操作。
	AddAfter(item T, duration time.Duration)
}

// DelayingQueueConfig 指定自定义 DelayingInterface 的可选配置。
//
// Deprecated: use TypedDelayingQueueConfig instead.
type DelayingQueueConfig = TypedDelayingQueueConfig[any]

// TypedDelayingQueueConfig 指定自定义 DelayingInterface 的可选配置。
type TypedDelayingQueueConfig[T comparable] struct {
	// 可选 logger。队列名不会自动加入其中，若需要请调用方自行添加。
	Logger *klog.Logger

	// 队列名称；未命名时不会注册 metrics。
	Name string

	// MetricsProvider 可选，用于为该队列指定 metrics provider，而非使用全局 provider。
	MetricsProvider MetricsProvider

	// Clock 可注入真实或假的时钟，便于测试。
	//
	// 延迟队列完全依赖注入的时钟，测试用假时钟即可精确控制触发时机。
	Clock clock.WithTicker

	// Queue 可注入自定义底层队列，替代默认实现。
	Queue TypedInterface[T]
}

// NewDelayingQueue 创建支持延迟入队的工作队列，不产生 metrics。若需使用
// MetricsProvider，请改用 NewDelayingQueueWithConfig 并指定名字。
//
// Deprecated: use NewTypedDelayingQueue instead.
func NewDelayingQueue() DelayingInterface {
	return NewDelayingQueueWithConfig(DelayingQueueConfig{})
}

// NewTypedDelayingQueue 创建支持延迟入队的工作队列，不产生 metrics。若需使用
// MetricsProvider，请改用 NewTypedDelayingQueueWithConfig 并指定名字。
func NewTypedDelayingQueue[T comparable]() TypedDelayingInterface[T] {
	return NewTypedDelayingQueueWithConfig(TypedDelayingQueueConfig[T]{})
}

// NewDelayingQueueWithConfig 创建可自定义各项属性的工作队列。
//
// Deprecated: use NewTypedDelayingQueueWithConfig instead.
func NewDelayingQueueWithConfig(config DelayingQueueConfig) DelayingInterface {
	return NewTypedDelayingQueueWithConfig[any](config)
}

// TypedNewDelayingQueue 仅为向后兼容而保留。
//
// Deprecated: use NewTypedDelayingQueueWithConfig instead.
func TypedNewDelayingQueue[T comparable]() TypedDelayingInterface[T] {
	return NewTypedDelayingQueue[T]()
}

// NewTypedDelayingQueueWithConfig 创建可自定义各项属性的工作队列。
//
// 会启动一个后台 waitingLoop 协程，负责在到期时把条目搬进底层队列；
// 队列关闭时该协程退出。
func NewTypedDelayingQueueWithConfig[T comparable](config TypedDelayingQueueConfig[T]) TypedDelayingInterface[T] {
	logger := klog.Background()
	if config.Logger != nil {
		logger = *config.Logger
	}
	if config.Clock == nil {
		config.Clock = clock.RealClock{}
	}

	if config.Queue == nil {
		config.Queue = NewTypedWithConfig[T](TypedQueueConfig[T]{
			Name:            config.Name,
			MetricsProvider: config.MetricsProvider,
			Clock:           config.Clock,
		})
	}

	return newDelayingQueue(logger, config.Clock, config.Queue, config.Name, config.MetricsProvider)
}

// NewDelayingQueueWithCustomQueue 创建可注入自定义底层队列的工作队列。
// Deprecated: Use NewDelayingQueueWithConfig instead.
func NewDelayingQueueWithCustomQueue(q Interface, name string) DelayingInterface {
	return NewDelayingQueueWithConfig(DelayingQueueConfig{
		Name:  name,
		Queue: q,
	})
}

// NewNamedDelayingQueue 创建支持延迟入队的带名字工作队列。
// Deprecated: Use NewDelayingQueueWithConfig instead.
func NewNamedDelayingQueue(name string) DelayingInterface {
	return NewDelayingQueueWithConfig(DelayingQueueConfig{Name: name})
}

// NewDelayingQueueWithCustomClock 创建带名字的工作队列，可注入真实或假的时钟
// 以便测试。
// Deprecated: Use NewDelayingQueueWithConfig instead.
func NewDelayingQueueWithCustomClock(clock clock.WithTicker, name string) DelayingInterface {
	return NewDelayingQueueWithConfig(DelayingQueueConfig{
		Name:  name,
		Clock: clock,
	})
}

// newDelayingQueue 构造延迟队列并启动 waitingLoop；heartbeat 为兜底的
// 最长等待上限。
func newDelayingQueue[T comparable](logger klog.Logger, clock clock.WithTicker, q TypedInterface[T], name string, provider MetricsProvider) *delayingType[T] {
	ret := &delayingType[T]{
		TypedInterface:  q,
		clock:           clock,
		heartbeat:       clock.NewTicker(maxWait),
		stopCh:          make(chan struct{}),
		waitingForAddCh: make(chan *waitFor[T], 1000),
		metrics:         newRetryMetrics(name, provider),
	}

	go ret.waitingLoop(logger)
	return ret
}

// delayingType 包装 Interface，提供延迟重新入队能力。
type delayingType[T comparable] struct {
	TypedInterface[T]

	// clock 用于跟踪延迟触发的时间。
	clock clock.Clock

	// stopCh 用于向 waitingLoop 发出关闭信号。
	stopCh chan struct{}
	// stopOnce 保证只发出一次关闭信号。
	//
	// 关闭已关闭的 channel 会 panic，因此 ShutDown 必须幂等。
	stopOnce sync.Once

	// heartbeat 保证最长等待 maxWait 后就触发检查。
	heartbeat clock.Ticker

	// waitingForAddCh 是缓冲 channel，用于向 waitingForAdd 输送待入队条目。
	//
	// 缓冲 1000 是为了吸收突发延迟入队，避免调用方在等待循环忙时被阻塞。
	waitingForAddCh chan *waitFor[T]

	// metrics 统计重试次数。
	metrics retryMetrics
}

// waitFor 保存待入队条目及其应入队的时间。
type waitFor[T any] struct {
	data    T
	readyAt time.Time
	// 在优先队列（heap）中的下标。
	// index 由 container/heap 维护，用于 insert 时就地调整已有条目。
	index int
}

// waitForPriorityQueue 为 waitFor 条目实现优先队列。
//
// waitForPriorityQueue 实现 heap.Interface：readyAt 最小（即最早触发）的条目位于
// 根（下标 0）。Peek 返回下标 0 的最小条目；Pop 返回容器/heap 移除并放到
// Len()-1 位置的最小条目；Push 在 Len() 处加入条目，由容器/heap 调整到正确位置。
type waitForPriorityQueue[T any] []*waitFor[T]

func (pq waitForPriorityQueue[T]) Len() int {
	return len(pq)
}
func (pq waitForPriorityQueue[T]) Less(i, j int) bool {
	return pq[i].readyAt.Before(pq[j].readyAt)
}
func (pq waitForPriorityQueue[T]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push 向队列添加条目；不应直接调用，应使用 `heap.Push`。
func (pq *waitForPriorityQueue[T]) Push(x interface{}) {
	n := len(*pq)
	item := x.(*waitFor[T])
	item.index = n
	*pq = append(*pq, item)
}

// Pop 从队列移除条目；不应直接调用，应使用 `heap.Pop`。
func (pq *waitForPriorityQueue[T]) Pop() interface{} {
	n := len(*pq)
	item := (*pq)[n-1]
	item.index = -1
	*pq = (*pq)[0:(n - 1)]
	return item
}

// Peek 返回队列首个条目但不移除，也不修改队列；可直接安全调用。
func (pq waitForPriorityQueue[T]) Peek() interface{} {
	return pq[0]
}

// ShutDown 停止队列。队列清空后，Get() 返回的 shutdown bool 为 true。
// 该方法可多次调用。
func (q *delayingType[T]) ShutDown() {
	q.stopOnce.Do(func() {
		q.TypedInterface.ShutDown()
		close(q.stopCh)
		q.heartbeat.Stop()
	})
}

// AddAfter 在给定延迟之后把条目加入工作队列。
func (q *delayingType[T]) AddAfter(item T, duration time.Duration) {
	// 已在关闭中则不再添加。
	if q.ShuttingDown() {
		return
	}

	q.metrics.retry()

	// 无延迟的条目立即入队。
	if duration <= 0 {
		q.Add(item)
		return
	}

	select {
	case <-q.stopCh:
		// ShutDown 时解除阻塞。
	case q.waitingForAddCh <- &waitFor[T]{data: item, readyAt: q.clock.Now().Add(duration)}:
	}
}

// maxWait 限制最长等待时间，纯属防止意外情况：每 10 秒检查一次队列开销不大，
// 且可保证过期条目滞留不超过 10 秒。
const maxWait = 10 * time.Second

// waitingLoop 一直运行到队列关闭，持续检查待入队条目列表。
//
// 三个唤醒来源：停止信号、定时器（队首到期）、heartbeat（兜底扫描）。
// 收到新条目时会一次性排空 waitingForAddCh，减少循环轮次。
func (q *delayingType[T]) waitingLoop(logger klog.Logger) {
	defer utilruntime.HandleCrashWithLogger(logger)

	// 列表为空时使用的占位 channel。
	never := make(<-chan time.Time)

	// 创建一个在队首条目就绪时到期的定时器。
	var nextReadyAtTimer clock.Timer

	waitingForQueue := &waitForPriorityQueue[T]{}
	heap.Init(waitingForQueue)

	waitingEntryByData := map[T]*waitFor[T]{}

	for {
		if q.TypedInterface.ShuttingDown() {
			return
		}

		now := q.clock.Now()

		// 添加已就绪的条目。
		for waitingForQueue.Len() > 0 {
			entry := waitingForQueue.Peek().(*waitFor[T])
			if entry.readyAt.After(now) {
				break
			}

			entry = heap.Pop(waitingForQueue).(*waitFor[T])
			q.Add(entry.data)
			delete(waitingEntryByData, entry.data)
		}

		// 为首个条目的 readyAt 设置等待（若存在）。
		nextReadyAt := never
		if waitingForQueue.Len() > 0 {
			if nextReadyAtTimer != nil {
				nextReadyAtTimer.Stop()
			}
			entry := waitingForQueue.Peek().(*waitFor[T])
			nextReadyAtTimer = q.clock.NewTimer(entry.readyAt.Sub(now))
			nextReadyAt = nextReadyAtTimer.C()
		}

		select {
		case <-q.stopCh:
			return

		case <-q.heartbeat.C():
			// 继续循环，将添加就绪条目。

		case <-nextReadyAt:
			// 继续循环，将添加就绪条目。

		case waitEntry := <-q.waitingForAddCh:
			if waitEntry.readyAt.After(q.clock.Now()) {
				insert(waitingForQueue, waitingEntryByData, waitEntry)
			} else {
				q.Add(waitEntry.data)
			}

			drained := false
			for !drained {
				select {
				case waitEntry := <-q.waitingForAddCh:
					if waitEntry.readyAt.After(q.clock.Now()) {
						insert(waitingForQueue, waitingEntryByData, waitEntry)
					} else {
						q.Add(waitEntry.data)
					}
				default:
					drained = true
				}
			}
		}
	}
}

// insert 把条目加入优先队列；若已存在，则按需更新 readyAt。
//
// 同一对象多次延迟入队时只保留最早的时间：更晚的调度不能推迟已经承诺的
// 重试，否则会无限拖延。
func insert[T comparable](q *waitForPriorityQueue[T], knownEntries map[T]*waitFor[T], entry *waitFor[T]) {
	// 仅当新时间能让条目更早入队时才更新已有条目的时间。
	existing, exists := knownEntries[entry.data]
	if exists {
		if existing.readyAt.After(entry.readyAt) {
			existing.readyAt = entry.readyAt
			heap.Fix(q, existing.index)
		}

		return
	}

	heap.Push(q, entry)
	knownEntries[entry.data] = entry
}
