/*
Copyright 2015 The Kubernetes Authors.

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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
)

// Deprecated: Interface is deprecated, use TypedInterface instead.
//
// 已弃用：Interface 是 TypedInterface[any] 的别名，新代码应直接使用泛型版本。
type Interface TypedInterface[any]

// TypedInterface 是工作队列的公开契约：同一 item 在进入处理前可被多次 Add，
// 但只会被处理一次；处理完成后若期间又被 Add，则会重新入队。
type TypedInterface[T comparable] interface {
	Add(item T)
	Len() int
	Get() (item T, shutdown bool)
	Done(item T)
	ShutDown()
	ShutDownWithDrain()
	ShuttingDown() bool
}

// Queue 是条目的底层存储。下面这些方法总是由同一个 goroutine 调用。
//
// 由于调用方（Typed）已持有自己的锁，这里无需再做同步；
// 自定义实现只需保证 FIFO 语义与 Len/Pop 的一致性。
type Queue[T comparable] interface {
	// 已有条目再次 Add 时可调用 Touch 钩子，供实现按需调整该条目的优先级。
	Touch(item T)
	// Push 添加新条目。
	Push(item T)
	// Len 返回条目总数。
	Len() int
	// Pop 取出一个条目。
	Pop() (item T)
}

// DefaultQueue 是基于切片的 FIFO 队列。
func DefaultQueue[T comparable]() Queue[T] {
	return new(queue[T])
}

// queue 是基于切片实现的 Queue。
type queue[T comparable] []T

func (q *queue[T]) Touch(item T) {}

func (q *queue[T]) Push(item T) {
	*q = append(*q, item)
}

func (q *queue[T]) Len() int {
	return len(*q)
}

func (q *queue[T]) Pop() (item T) {
	item = (*q)[0]

	// 底层数组仍持有该对象的引用，因此它不会被 GC 回收。
	//
	// 清空已弹出位置，避免底层数组一直持有对象引用导致内存无法回收。
	(*q)[0] = *new(T)
	*q = (*q)[1:]

	return item
}

// QueueConfig 指定自定义 Interface 的可选配置。
// Deprecated: use TypedQueueConfig instead.
//
// 已弃用：请使用 TypedQueueConfig；本别名为保持 API 兼容而保留。
type QueueConfig = TypedQueueConfig[any]

// TypedQueueConfig 是队列的可选配置；零值即表示使用默认实现。
type TypedQueueConfig[T comparable] struct {
	// 队列名称；未命名时不会注册 metrics。
	Name string

	// MetricsProvider 可选，用于为该队列指定 metrics provider，而非使用全局 provider。
	MetricsProvider MetricsProvider

	// Clock 可注入真实或假的时钟，便于测试。
	Clock clock.WithTicker

	// Queue 可注入底层队列，默认是基于切片的 FIFO。
	Queue Queue[T]
}

// New 创建新的工作队列（见包注释）。
//
// Deprecated: use NewTyped instead.
//
// 已弃用：请使用 NewTyped，以便获得类型安全的接口。
func New() *Type {
	return NewWithConfig(QueueConfig{
		Name: "",
	})
}

// NewTyped 创建新的工作队列（见包注释）。
func NewTyped[T comparable]() *Typed[T] {
	return NewTypedWithConfig(TypedQueueConfig[T]{
		Name: "",
	})
}

// NewWithConfig 创建可自定义各项属性的工作队列。
//
// Deprecated: use NewTypedWithConfig instead.
//
// 已弃用：请使用 NewTypedWithConfig。
func NewWithConfig(config QueueConfig) *Type {
	return NewTypedWithConfig(config)
}

// NewTypedWithConfig 创建可自定义各项属性的工作队列。
func NewTypedWithConfig[T comparable](config TypedQueueConfig[T]) *Typed[T] {
	return newQueueWithConfig(config, defaultUnfinishedWorkUpdatePeriod)
}

// NewNamed 创建带名字的队列。
// Deprecated: Use NewWithConfig instead.
//
// 已弃用：请使用 NewWithConfig；命名只影响 metrics 注册。
func NewNamed(name string) *Type {
	return NewWithConfig(QueueConfig{
		Name: name,
	})
}

// newQueueWithConfig 构造带名字的工作队列，可自定义属性以便测试。
func newQueueWithConfig[T comparable](config TypedQueueConfig[T], updatePeriod time.Duration) *Typed[T] {
	metricsProvider := globalMetricsProvider
	if config.MetricsProvider != nil {
		metricsProvider = config.MetricsProvider
	}

	// Clock/Queue 都可以留空，这里统一补默认值，避免调用方处理 nil。
	if config.Clock == nil {
		config.Clock = clock.RealClock{}
	}

	if config.Queue == nil {
		config.Queue = DefaultQueue[T]()
	}

	return newQueue(
		config.Clock,
		config.Queue,
		newQueueMetrics[T](metricsProvider, config.Name, config.Clock),
		updatePeriod,
	)
}

func newQueue[T comparable](c clock.WithTicker, queue Queue[T], metrics queueMetrics[T], updatePeriod time.Duration) *Typed[T] {
	t := &Typed[T]{
		clock:                      c,
		queue:                      queue,
		dirty:                      sets.Set[T]{},
		processing:                 sets.Set[T]{},
		cond:                       sync.NewCond(&sync.Mutex{}),
		metrics:                    metrics,
		unfinishedWorkUpdatePeriod: updatePeriod,
		stopCh:                     make(chan struct{}),
	}

	// noMetrics 类型不启动 goroutine，避免无谓消耗资源。
	//
	// noMetrics 表示 metrics 关闭，此时没必要为“未完成工作量”统计启动 ticker goroutine。
	if _, ok := metrics.(noMetrics[T]); !ok {
		t.wg.Go(t.updateUnfinishedWorkLoop)
	}

	return t
}

const defaultUnfinishedWorkUpdatePeriod = 500 * time.Millisecond

// Type 是工作队列（见包注释）。
// Deprecated: Use Typed instead.
//
// 已弃用：Type 是 Typed[any] 的别名，新代码应使用 Typed。
type Type = Typed[any]

type Typed[t comparable] struct {
	// queue 定义处理条目的顺序。queue 中每个元素都应位于 dirty 集合且不在
	// processing 集合中。
	queue Queue[t]

	// dirty 集合包含所有需要处理的条目。
	dirty sets.Set[t]

	// 正在处理的条目位于 processing 集合，它们也可能同时出现在 dirty 集合。处理完后
	// 从 processing 移除时，若它仍在 dirty 中，就把它重新放回 queue。
	processing sets.Set[t]

	cond *sync.Cond

	shuttingDown bool
	drain        bool

	metrics queueMetrics[t]

	unfinishedWorkUpdatePeriod time.Duration
	clock                      clock.WithTicker

	// wg 管理队列启动的 goroutine，使 ShutDown 能优雅等待它们退出。
	wg sync.WaitGroup

	stopCh chan struct{}
	// stopOnce 保证只发出一次关闭信号。
	stopOnce sync.Once
}

// Add 标记条目需要处理。队列关闭后新条目会被静默忽略，不入队也不标脏。
func (q *Typed[T]) Add(item T) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}
	if q.dirty.Has(item) {
		// 若同一条目在处理前再次 Add，且实现关心此事（如需要重置优先级），则调用 Touch。
		if !q.processing.Has(item) {
			q.queue.Touch(item)
		}
		return
	}

	q.metrics.add(item)

	q.dirty.Insert(item)
	if q.processing.Has(item) {
		// 正在处理中：只标记 dirty，等 Done 时再重新入队，
		// 避免同一时刻有两个 worker 处理同一个 item。
		return
	}

	q.queue.Push(item)
	q.cond.Signal()
}

// Len 返回当前队列长度，仅用于参考；不要基于 Len 的具体值决定 Add/Get 的调用，
// 因为两者之间无法保持同步。
func (q *Typed[T]) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.queue.Len()
}

// Get 阻塞直到能返回待处理条目。若 shutdown=true，调用方应结束其 goroutine。
// 处理完条目后必须调用 Done。
func (q *Typed[T]) Get() (item T, shutdown bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for q.queue.Len() == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if q.queue.Len() == 0 {
		// 此时必然正在关闭队列。
		return *new(T), true
	}

	item = q.queue.Pop()

	q.metrics.get(item)

	// 进入 processing 并移出 dirty：处理期间再次 Add 会重新标记 dirty。
	q.processing.Insert(item)
	q.dirty.Delete(item)

	return item, false
}

// Done 标记条目处理完成；若处理期间它又被标脏，则重新入队等待再次处理。
func (q *Typed[T]) Done(item T) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.metrics.done(item)

	q.processing.Delete(item)
	if q.dirty.Has(item) {
		// 处理期间被再次 Add：立即重新入队，保证不丢失这次更新。
		q.queue.Push(item)
		q.cond.Signal()
	} else if q.processing.Len() == 0 {
		// 没有积压时唤醒可能在 ShutDownWithDrain 中等待的调用方。
		q.cond.Signal()
	}
}

// ShutDown 使 q 忽略此后 Add 的所有条目。worker 会继续处理队列中已有条目，
// 直到清空后收到关闭信号。
func (q *Typed[T]) ShutDown() {
	defer q.wg.Wait()
	q.stopOnce.Do(func() {
		defer close(q.stopCh)
	})

	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.drain = false
	q.shuttingDown = true
	q.cond.Broadcast()
}

// ShutDownWithDrain 类似 ShutDown，但会等待队列中所有条目处理完毕。
// 可在 ShutDownWithDrain 之后调用 ShutDown 强制其停止等待。
// worker 必须在处理完条目后调用 Done，否则 ShutDownWithDrain 会永久阻塞。
func (q *Typed[T]) ShutDownWithDrain() {
	defer q.wg.Wait()
	q.stopOnce.Do(func() {
		defer close(q.stopCh)
	})
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.drain = true
	q.shuttingDown = true
	q.cond.Broadcast()

	// 等待正在处理的 item 全部 Done；ShutDown 会把 drain 置 false 从而提前解除等待。
	for q.processing.Len() != 0 && q.drain {
		q.cond.Wait()
	}
}

func (q *Typed[T]) ShuttingDown() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	return q.shuttingDown
}

func (q *Typed[T]) updateUnfinishedWork() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if !q.shuttingDown {
		q.metrics.updateUnfinishedWork()
	}
}

func (q *Typed[T]) updateUnfinishedWorkLoop() {
	t := q.clock.NewTicker(q.unfinishedWorkUpdatePeriod)
	defer t.Stop()
	for {
		select {
		case <-t.C():
			q.updateUnfinishedWork()
		case <-q.stopCh:
			return
		}
	}
}
