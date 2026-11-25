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

package cache

import (
	"errors"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
)

// PopProcessFunc 传给 Queue 接口的 Pop() 方法，用于处理从队列弹出的累积对象。
// isInInitialList 为 true 表示该项来自首次 Replace 的快照，
// 供处理器区分初始同步与增量事件。
type PopProcessFunc func(obj interface{}, isInInitialList bool) error

// ErrFIFOClosed 在 FIFO 已关闭时返回。
// 关闭后仍阻塞在 Pop 的消费者会收到此错误，从而退出循环。
var ErrFIFOClosed = errors.New("DeltaFIFO: manipulating with closed queue")

// Queue 在 ReflectorStore 之上维护一组待“处理”的 Store key。
// 每次 Add、Update 或 Delete 都可能把对象的 key 放入该集合。
// Queue 能从累积对象推导出对应的 key，可被多个 goroutine 并发访问，
// 并且可以被“关闭”：关闭之后 Pop 操作返回错误。
type Queue interface {
	ReflectorStore

	// Pop 会阻塞，直到至少有一个待处理的 key，或者 Queue 被关闭；
	// 后者使 Pop 返回错误。有 key 可处理时，Pop 原子地选取一个 key，
	// 从 Store 中移除该 (key, 累积对象) 关联并处理累积对象，返回被处理的
	// 累积对象及处理结果。PopProcessFunc 可以返回 ErrRequeue{inner}，
	// 此时 Pop 会（a）在原子处理中把该 (key, 累积对象) 关联放回 Queue，
	// （b）返回其中的内部错误。期望 Pop 的调用方是单线程消费者，
	// 否则可能出现多个 PopProcessFunc 同时运行。
	Pop(PopProcessFunc) (interface{}, error)

	// HasSynced 在首批 key 全部被弹出后返回 true。若第一次 Replace 发生在任何
	// Add、AddIfNotPresent、Update、Delete 之前，首批 key 即该次 Replace 的 key；
	// 否则首批为空。
	HasSynced() bool

	// HasSyncedChecker 返回的 DoneChecker 在首批 key 全部被弹出后完成。首批 key
	// 的定义与 HasSynced 相同。
	HasSyncedChecker() DoneChecker

	// Close 关闭队列。
	Close()
}

// QueueWithBatch 在 Queue 接口之上增加批量处理能力。
//
// 除了标准的单条 Pop 方法外，QueueWithBatch 还提供 PopBatch，可以把多个条目
// 作为一批一起弹出并处理。当一次操作处理多个排队 key 或累积对象能提升效率时，
// 可以借此改善处理吞吐。
// TODO: feature gate GA 之后考虑把该接口合并回 Queue
type QueueWithBatch interface {
	Queue

	// PopBatch 的行为与 Queue#Pop 类似，但以批次方式处理多个 key。
	// 分批策略（如每批包含的 key 数量）由具体实现决定。批次就绪时调用
	// ProcessBatchFunc，单条条目就绪时调用 PopProcessFunc。两者必须执行
	// 相同的处理逻辑以保证行为一致。期望 PopBatch 的调用方是单线程消费者，
	// 否则可能出现多个 ProcessBatchFunc/PopProcessFunc 同时运行。
	PopBatch(processBatch ProcessBatchFunc, processSingle PopProcessFunc) error
}

// FIFO 是这样一种 Queue：（a）每个累积对象就是最近一次提供的对象本身；
// （b）待处理 key 的集合是一个 FIFO。累积对象初始为空，删除对象会使其累积
// 对象重新变空。Resync 是空操作。
//
// 因此：若某个对象的 key 在队列中等待处理期间发生了多次 add/update，
// 该对象只会被处理一次，且处理的是最新版本——这是 channel 做不到的。
//
// FIFO 适合以下场景：
//   - 每个对象恰好处理一次。
//   - 处理时希望拿到对象的最新版本。
//   - 不想处理已删除的对象，它们应从队列中移除。
//   - 不想周期性地重新处理对象。
//
// 其他场景请参考 DeltaFIFO。
//
// 这里的“FIFO”只保证 key 的处理顺序，不保留事件历史；
// 因此不能用于需要看到每一次变更（edge）的场景，那应使用 DeltaFIFO/RealFIFO。
type FIFO struct {
	lock sync.RWMutex
	cond sync.Cond
	// 依赖不变式：`items` 中的每个 key 也都在 `queue` 中。
	// 该不变式让 Pop 可以安全地跳过已被删除的 key。
	items map[string]interface{}
	queue []string

	// synced 初始是打开的 channel。一旦初始同步完成，checkSynced 会将其
	// 关闭（且仅关闭一次！）。
	synced       chan struct{}
	syncedClosed bool

	// 若 Replace() 插入的首批条目已填充完毕，或先调用了 Delete/Add/Update，
	// 则 populated 为 true。
	populated bool
	// initialPopulationCount 是第一次 Replace() 调用插入的条目数量
	initialPopulationCount int

	// keyFunc 生成入队/检索使用的 key，必须是确定性的。
	keyFunc KeyFunc

	// 表示队列已关闭，便于控制循环在队列为空时退出。
	// 目前并不用它来拦截任何 CRUD 操作。
	closed bool
}

var (
	_ = Queue(&FIFO{})       // FIFO 是一个 Queue
	_ = DoneChecker(&FIFO{}) // ……同时实现了 DoneChecker。
)

// Close 关闭队列。
// 关闭后唤醒所有等待者；之后 Pop 返回 ErrFIFOClosed，但其他 CRUD 仍会执行。
func (f *FIFO) Close() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.closed = true
	f.cond.Broadcast()
}

// HasSynced 在以下情况返回 true：先调用了 Add/Update/Delete/AddIfNotPresent，
// 或 Replace() 插入的首批条目已全部弹出。
func (f *FIFO) HasSynced() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.hasSynced_locked()
}

// HasSyncedChecker 返回的 DoneChecker 在以下情况视为完成：先调用了
// Add/Update/Delete/AddIfNotPresent，或 Replace() 插入的首批条目已全部弹出。
// 返回自身以便配合 WaitForNamedCacheSync 等待 channel 关闭。
func (f *FIFO) HasSyncedChecker() DoneChecker {
	return f
}

// Name 实现 [DoneChecker.Name]。
func (f *FIFO) Name() string {
	return "FIFO" // FIFO 除了少数测试外几乎没人用，为它改 NewFIFO API 增加名称参数并不划算。
}

// Done 实现 [DoneChecker.Done]。
func (f *FIFO) Done() <-chan struct{} {
	return f.synced
}

// hasSynced_locked 返回此前 checkSynced 调用的结果，调用方必须已持有 f.lock。
func (f *FIFO) hasSynced_locked() bool {
	return f.syncedClosed
}

// checkSynced 检查初始同步是否完成。只要 populated 或 initialPopulationCount
// 在持锁状态下发生变化，就必须调用它。
//
// synced channel 只能关闭一次，syncedClosed 用于避免重复 close 导致 panic。
func (f *FIFO) checkSynced() {
	synced := f.populated && f.initialPopulationCount == 0
	if synced && !f.syncedClosed {
		f.syncedClosed = true
		close(f.synced)
	}
}

// Add 插入条目并放入队列；仅当条目不存在时才会入队。
func (f *FIFO) Add(obj interface{}) error {
	id, err := f.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	f.checkSynced()
	// 已存在的 key 不重复入队；稍后处理时会拿到 items 中最新的对象。
	if _, exists := f.items[id]; !exists {
		f.queue = append(f.queue, id)
	}
	f.items[id] = obj
	f.cond.Broadcast()
	return nil
}

// 在该实现中 Update 与 Add 相同。
func (f *FIFO) Update(obj interface{}) error {
	return f.Add(obj)
}

// Delete 移除条目且不会将其加入队列，因为该实现假定消费者只关心对象本身，
// 不关心它们被创建/添加的顺序。
func (f *FIFO) Delete(obj interface{}) error {
	id, err := f.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	f.checkSynced()
	// 不清理 queue 中的 id：Pop 时发现 items 缺失会自动跳过。
	delete(f.items, id)
	return err
}

// IsClosed 检查队列是否已关闭
func (f *FIFO) IsClosed() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.closed
}

// Pop 等待直到有条目就绪并处理它。若多个条目同时就绪，按添加/更新的先后顺序返回。
// 条目在处理前会从队列（及存储）中移除，因此若处理失败，应通过
// AddIfNotPresent() 重新放回。process 函数在持锁状态下调用，其中更新与队列
// 需要同步的数据结构是安全的。
func (f *FIFO) Pop(process PopProcessFunc) (interface{}, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	for {
		for len(f.queue) == 0 {
			// 队列为空时，Pop() 会阻塞直到有新条目入队。
			// 调用 Close() 后 f.closed 被置位且条件被广播，
			// 使该循环继续执行并从 Pop() 返回。
			if f.closed {
				return nil, ErrFIFOClosed
			}

			f.cond.Wait()
		}
		// 初次 Replace 尚未全部处理完时，弹出的项标记为 initial list。
		isInInitialList := !f.hasSynced_locked()
		id := f.queue[0]
		f.queue = f.queue[1:]
		if f.initialPopulationCount > 0 {
			f.initialPopulationCount--
			// 必须在 process 完成*之后*执行。
			// 用 defer 确保处理函数返回（或 panic 展开）后才计入已同步。
			defer f.checkSynced()
		}
		item, ok := f.items[id]
		if !ok {
			// 条目可能随后已被删除。
			continue
		}
		delete(f.items, id)
		err := process(item, isInInitialList)
		return item, err
	}
}

// Replace 会删除 'f' 的内容，改用给定的 map。'f' 接管该 map 的所有权，
// 调用此函数后不应再引用它。f 的队列也会被重置；返回时其中包含 map 里的
// 全部条目，顺序不确定。
func (f *FIFO) Replace(list []interface{}, resourceVersion string) error {
	// 先完整构建新 map，再在锁内一次性替换，避免消费者看到半成品状态。
	items := make(map[string]interface{}, len(list))
	for _, item := range list {
		key, err := f.keyFunc(item)
		if err != nil {
			return KeyError{item, err}
		}
		items[key] = item
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	if !f.populated {
		f.populated = true
		f.initialPopulationCount = len(items)
		f.checkSynced()
	}

	f.items = items
	f.queue = f.queue[:0]
	for id := range items {
		f.queue = append(f.queue, id)
	}
	if len(f.queue) > 0 {
		f.cond.Broadcast()
	}
	return nil
}

// Resync 确保 Store 中每个对象的 key 都在队列里。这本应是空操作，
// 因为所有操作都维持了该性质。
//
// 保留该实现是为了满足 Queue 接口；若不变式被破坏，它会补入缺失的 key。
func (f *FIFO) Resync() error {
	f.lock.Lock()
	defer f.lock.Unlock()

	inQueue := sets.NewString()
	for _, id := range f.queue {
		inQueue.Insert(id)
	}
	for id := range f.items {
		if !inQueue.Has(id) {
			f.queue = append(f.queue, id)
		}
	}
	if len(f.queue) > 0 {
		f.cond.Broadcast()
	}
	return nil
}

// NewFIFO 返回一个可用于排队待处理条目的 Store。
func NewFIFO(keyFunc KeyFunc) *FIFO {
	f := &FIFO{
		synced:  make(chan struct{}),
		items:   map[string]interface{}{},
		queue:   []string{},
		keyFunc: keyFunc,
	}
	f.cond.L = &f.lock
	return f
}
