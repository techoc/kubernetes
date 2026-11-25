/*
Copyright 2025 The Kubernetes Authors.

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
	"fmt"
	"sync"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	utiltrace "k8s.io/utils/trace"
)

// RealFIFOOptions 是 RealFIFO 的配置参数。
//
// 部分选项互斥：AtomicEvents 与 KnownObjects 不能同时设置，
// UnlockWhileProcessing 和 EmitDeltaTypeBookmark 只能在 AtomicEvents 下启用。
type RealFIFOOptions struct {
	// 若设置，日志将输出到该 logger 而非 klog.Background()。
	// fifo 的名称会自动附加。
	Logger *klog.Logger

	// Name 可用于覆盖新实例默认的 "RealFIFO" 名称。
	// 可选。仅当 Identifier.Name 返回空字符串时生效。
	Name string

	// KeyFunction 用于确定对象应有的 key。（它通过返回的 RealFIFO 的 keyOf()
	// 方法暴露，并对已删除对象和队列状态做了额外处理。）
	// 可选，默认为 MetaNamespaceKeyFunc。
	KeyFunction KeyFunc

	// KnownObjects 应返回该队列的消费者“已知”的 key 列表。Replace() 被调用时，
	// 用它来判断哪些条目缺失；缺失的条目会产生 'Deleted' delta。
	// 若 AtomicEvents 为 false，则必须提供 KnownObjects，因为它用于在 Replace
	// 和 Resync 处理时查询内部存储的状态。
	//
	// 它通常指向 informer 的 indexer；与 AtomicEvents 互斥是为了避免在
	// 释放锁处理事件时读到与队列不一致的存储状态。
	KnownObjects KeyListerGetter

	// 若设置，会在对象入队前对其调用。详情请参见 TransformFunc 的注释。
	Transformer TransformFunc

	// AtomicEvents 指定 RealFIFO 是否以原子方式发出事件。若设置，Replace 和
	// Resync 操作将原子地发出单个事件。
	// 若 AtomicEvents 为 true，KnownObjects 必须为 nil。
	AtomicEvents bool

	// UnlockWhileProcessing 指定 RealFIFO 能否在处理事件时释放锁。若设置，
	// 处理事件期间会释放锁，以允许其他 goroutine 向队列添加条目。
	// 若 UnlockWhileProcessing 为 true，AtomicEvents 也必须为 true。
	UnlockWhileProcessing bool

	// Identifier 用于在指标和日志中标识本 FIFO。
	// 可选。若为零值，将不发布指标，且 trace 日志不包含 Name 或 Resource 字段。
	Identifier InformerNameAndResource

	// MetricsProvider 用于为该 FIFO 创建指标。
	MetricsProvider InformerMetricsProvider

	// EmitDeltaTypeBookmark 指定 RealFIFO 是否发出 bookmark delta。
	// 仅当 AtomicEvents 为 true 时可设置。
	//
	// 在 AtomicEvents 下发出 bookmark 才能保证 Replace/Resync 期间事件不脱离 RV 顺序。
	EmitDeltaTypeBookmark bool
}

const (
	defaultBatchSize = 1000
)

var _ QueueWithBatch = &RealFIFO{}

// RealFIFO 是一个 Queue，Reflector 的每条通知都按顺序经 Pop 传递给队列消费者。
// 这意味着它：
// 1. 会投递已删除条目的通知；
// 2. 每个条目可能投递多条通知，而不是仅投递最新值。
//
// 与只保留最新状态的缓存不同，RealFIFO 保序投递每一条 Delta，
// 因此消费者必须自己处理重复事件和删除事件。
type RealFIFO struct {
	// logger 是每个实例独立的 logger。构造实例时确定，默认为 klog.Background()。
	logger klog.Logger

	// name 是 fifo 的名称，会包含在 logger 中。
	name string

	lock sync.RWMutex
	// cond 的 L 在构造时指向 &lock；Close 通过 Broadcast 唤醒等待中的 Pop。
	cond sync.Cond

	// items 是待处理 Delta 的 FIFO；出队后会把槽位置零以避免引用泄漏。
	items []Delta

	// synced 初始为未关闭的 channel。一旦初始同步被认为完成，
	// checkSynced_locked 会将其关闭（仅一次！）。
	synced       chan struct{}
	syncedClosed bool

	// 若 Replace() 插入的第一批条目已全部填充，或首先调用了 Delete/Add/Update，
	// 则 populated 为 true。
	populated bool
	// initialPopulationCount 是首次调用 Replace() 插入的条目数量。
	initialPopulationCount int

	// keyFunc 用于生成队列条目插入和检索所用的 key，必须具有确定性。
	keyFunc KeyFunc

	// knownObjects 列出“已知”的 key——影响 Delete()、Replace() 和 Resync()。
	// 若 emitAtomicEvents 为 true，则为 nil。
	knownObjects KeyListerGetter

	// 标识队列已关闭。
	// 用于在队列为空时让控制循环得以退出。
	// 目前不用于限制任何 CRUD 操作。
	closed bool

	// 若非 nil，会对每个对象调用。
	transformer TransformFunc

	// batchSize 决定一个批次最多可合并的对象数量。
	batchSize int

	// emitAtomicEvents 决定 Replace、Resync 等事件是以原子方式发出，还是作为
	// 一系列事件发出。设置后，对 FIFO 的任何调用都只发出单个事件。
	// 若设置：
	// * 发出单个 ReplacedAll 事件，而不是多个 Replace 事件
	// * 发出单个 SyncAll 事件，而不是多个 Sync 事件
	emitAtomicEvents bool

	// unlockWhileProcessing 决定处理事件时能否释放锁。
	// 仅当 emitAtomicEvents 为 true 时可设置。若 unlockWhileProcessing 为 true，
	// Pop 和 PopBatch 必须由单线程消费者调用。
	unlockWhileProcessing bool

	// identifier 用于在指标和日志中标识本 FIFO。
	identifier InformerNameAndResource

	// metrics 保存本 FIFO 的全部指标。
	metrics *fifoMetrics

	// emitDeltaTypeBookmark 决定是否发出 bookmark delta。
	// 仅当 emitAtomicEvents 为 true 时可设置，以避免 Replace 和 Resync 期间
	// 事件脱离 RV 顺序传播。
	emitDeltaTypeBookmark bool
}

// ReplacedAllInfo 是与 type=ReplacedAll 的 Delta 关联的对象。
// 表示 Replace 产生的一次原子事件，Objects 已完成 transformer 处理。
type ReplacedAllInfo struct {
	// ResourceVersion 是创建该 Delta 的 Replace() 调用传入的 resource version。
	ResourceVersion string
	// Objects 是创建该 Delta 的 Replace() 调用传入的对象列表，
	// 其中已应用配置好的 transformation。
	Objects []interface{}
}

// BookmarkInfo 是与 type=Bookmark 的 Delta 关联的对象。
type BookmarkInfo struct {
	// ResourceVersion 是创建该 Delta 的 Bookmark() 调用传入的 resource version。
	ResourceVersion string
}

// SyncAllInfo 是与 type=SyncAll 的 Delta 关联的对象，
// 用于触发整个队列的 resync。
type SyncAllInfo struct{}

var (
	_ = Queue(&RealFIFO{})             // RealFIFO 是一个 Queue。
	_ = TransformingStore(&RealFIFO{}) // RealFIFO 实现 TransformingStore，以支持内存优化。
	_ = DoneChecker(&RealFIFO{})       // RealFIFO 实现 DoneChecker。
)

// Close 关闭队列。
// 关闭后 Pop/PopBatch 在队列取空时返回 ErrFIFOClosed；已入队的条目仍会被处理。
func (f *RealFIFO) Close() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.closed = true
	f.cond.Broadcast()
}

// keyOf 暴露 f 的 keyFunc，同时也能识别 Deltas 对象或 DeletedFinalStateUnknown
// 对象的 key。
func (f *RealFIFO) keyOf(obj interface{}) (string, error) {
	// Deltas 取最新一条 Delta 的对象；空的 Deltas 属于非法输入。
	if d, ok := obj.(Deltas); ok {
		if len(d) == 0 {
			return "", KeyError{obj, ErrZeroLengthDeltasObject}
		}
		obj = d.Newest().Object
	}
	if d, ok := obj.(Delta); ok {
		obj = d.Object
	}
	if d, ok := obj.(DeletedFinalStateUnknown); ok {
		// tombstone 可能包裹一个已无法取到的对象，其 Key 字段仍可信。
		return d.Key, nil
	}
	return f.keyFunc(obj)
}

// 若首先调用了 Add/Update/Delete，或 Replace() 插入的第一批条目已被 Pop 完，
// HasSynced 返回 true。
//
// 注意是“已处理”而非“已入队”：Replace 的首批对象全部 Pop 完成后才置为已同步。
func (f *RealFIFO) HasSynced() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.hasSynced_locked()
}

// 若首先调用了 Add/Update/Delete/AddIfNotPresent，或 Replace() 插入的第一批
// 条目已被 Pop 完，HasSyncedChecker 即为 done。
func (f *RealFIFO) HasSyncedChecker() DoneChecker {
	return f
}

// Name 实现 [DoneChecker.Name]。
func (f *RealFIFO) Name() string {
	return f.name
}

// Done 实现 [DoneChecker.Done]。
// 返回的 channel 在首次同步完成时关闭，供 WaitForCacheSync 等待。
func (f *RealFIFO) Done() <-chan struct{} {
	return f.synced
}

// hasSynced_locked 返回此前 checkSynced_locked 调用的结果。
func (f *RealFIFO) hasSynced_locked() bool {
	return f.syncedClosed
}

// checkSynced_locked 检查初始批次（经 Replace 设置）是否已投递完毕，并按需
// 关闭 synced channel。在修改 f.populated 和/或 f.initialPopulationCount 之后、
// 仍持有互斥锁时必须调用。
func (f *RealFIFO) checkSynced_locked() {
	// 同步条件：已经历过首次填充，且首批条目已全部出队处理。
	synced := f.populated && f.initialPopulationCount == 0
	if synced && !f.syncedClosed {
		// 初始同步完成。
		f.syncedClosed = true
		close(f.synced)
	}
}

// addToItems_locked 向 delta 列表追加内容。
// 调用方必须持有 f.lock；出错时不会追加任何条目。
func (f *RealFIFO) addToItems_locked(deltaActionType DeltaType, skipTransform bool, obj interface{}) error {
	// 必须能读取 key，才能判断 knownObjects 与本 FIFO 中的条目是否重叠
	_, err := f.keyOf(obj)
	if err != nil {
		return KeyError{obj, err}
	}

	// 每个对象只经过这条代码路径一次，所以这里是调用 transform func 的合适位置。
	//
	// 若 obj 是 DeletedFinalStateUnknown 墓碑，或动作是 Sync，
	// 则该对象已经经过 transformer 处理。
	//
	// 若把缓存中已存在的对象传入 Replace()，transformer 必须幂等以避免重复
	// 修改它们，或者与所有从缓存读取的一方协调以避免数据竞态。
	// 默认 informer 不会把已存在的对象传给 Replace。
	if f.transformer != nil {
		_, isTombstone := obj.(DeletedFinalStateUnknown)
		if !isTombstone && !skipTransform {
			var err error
			obj, err = f.transformer(obj)
			if err != nil {
				return err
			}
		}
	}

	f.items = append(f.items, Delta{
		Type:   deltaActionType,
		Object: obj,
	})
	f.cond.Broadcast()
	f.metrics.numberOfQueuedItem.Set(float64(len(f.items)))

	return nil
}

// addReplaceToItemsLocked 向 delta 列表追加内容。
// 在原子模式下，Replace 只入队一条 ReplacedAll，消费者一次性拿到整批对象。
func (f *RealFIFO) addReplaceToItemsLocked(objs []interface{}, resourceVersion string) error {
	// 被替换的条目必须在加入队列前完成转换。这些对象都应当是尚未转换过的。
	if f.transformer != nil {
		transformedObjs := make([]interface{}, len(objs))
		for i, obj := range objs {
			transformedObj, err := f.transformer(obj)
			if err != nil {
				return err
			}
			transformedObjs[i] = transformedObj
		}
		objs = transformedObjs
	}

	info := ReplacedAllInfo{
		ResourceVersion: resourceVersion,
		Objects:         objs,
	}
	f.items = append(f.items, Delta{
		Type:   ReplacedAll,
		Object: info,
	})
	f.cond.Broadcast()
	f.metrics.numberOfQueuedItem.Set(float64(len(f.items)))

	return nil
}

// addResyncToItemsLocked 在原子模式下入队一条 SyncAll，表示需要重放整个缓存。
func (f *RealFIFO) addResyncToItemsLocked() error {
	f.items = append(f.items, Delta{
		Type:   SyncAll,
		Object: SyncAllInfo{},
	})
	f.cond.Broadcast()
	f.metrics.numberOfQueuedItem.Set(float64(len(f.items)))

	return nil
}

// Add 插入一个条目并放入队列。仅当该条目尚不存在时才入队。
//
// 注意：上面“仅在不存在时入队”的描述不适用于本实现，
// RealFIFO 会保留每一条通知，不做去重。
func (f *RealFIFO) Add(obj interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.populated = true
	f.checkSynced_locked()
	retErr := f.addToItems_locked(Added, false, obj)

	return retErr
}

// 在本实现中 Update 与 Add 相同。
// 只有 Delta 的 Type 不同，用于让消费者区分新增与更新。
func (f *RealFIFO) Update(obj interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.populated = true
	f.checkSynced_locked()
	retErr := f.addToItems_locked(Updated, false, obj)

	return retErr
}

// Delete 移除一个条目。它不会把条目加入队列，因为本实现假定消费者只关心
// 对象本身，而不关心它们被创建/添加的顺序。
//
// 注意：RealFIFO 仍会把 Deleted Delta 入队，消费者会收到删除通知，
// 因此上面“不加入队列”的描述与实际实现不一致。
func (f *RealFIFO) Delete(obj interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.populated = true
	f.checkSynced_locked()
	retErr := f.addToItems_locked(Deleted, false, obj)

	return retErr
}

// IsClosed 检查队列是否已关闭。
func (f *RealFIFO) IsClosed() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.closed
}

// Pop 等待直到有条目就绪并处理它。若有多个条目就绪，则按它们被添加/更新的
// 顺序返回。条目在处理前会从队列（和存储）中移除。仅当 UnlockWhileProcessing
// 为 false 时，才保证 process 函数在锁内被调用。若 process 函数要更新需要与
// 队列保持同步的数据结构，请确保 UnlockWhileProcessing 为 false。Pop 的调用方
// 应为单线程消费者，否则可能出现多个 PopProcessFunc 同时运行。
func (f *RealFIFO) Pop(process PopProcessFunc) (interface{}, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	// 队列为空时阻塞等待；Close 设置的 closed 标志配合 Broadcast 解除等待。
	for len(f.items) == 0 {
		// 队列为空时，Pop() 会阻塞直到新条目入队。
		// 调用 Close() 后，f.closed 被置位并广播条件变量，
		// 使本循环继续并从 Pop() 返回。
		if f.closed {
			return nil, ErrFIFOClosed
		}

		f.cond.Wait()
	}

	isInInitialList := !f.hasSynced_locked()
	// 先取出队首；随后的 defer 会在处理结束后递减首批计数，避免 hasSynced 提前为真。
	item := f.items[0]
	// 底层数组仍存在并引用该对象，因此若不置零引用，该对象不会被 GC。
	f.items[0] = Delta{}
	f.items = f.items[1:]
	// 按需递减 initialPopulationCount。
	// 放在 defer 中是为了只在处理完成*之后*递减，
	// 从而避免处理尚未结束时并发的 hasSynced 调用错误地返回 true。
	defer func() {
		if f.initialPopulationCount > 0 {
			f.initialPopulationCount--
			f.checkSynced_locked()
		}
	}()

	// 仅当队列深度大于 10 且处理一个条目耗时超过 100 毫秒时才记录 trace。
	// 队列深度不会很高，因为处理条目时会锁住队列，处理完成前无法添加新条目。
	// https://github.com/kubernetes/kubernetes/issues/103789
	if len(f.items) > 10 {
		id, _ := f.keyOf(item)
		fields := []utiltrace.Field{
			{Key: "ID", Value: id},
			{Key: "Depth", Value: len(f.items)},
			{Key: "Reason", Value: "slow event handlers blocking the queue"},
		}
		if name := f.identifier.Name(); len(name) > 0 {
			fields = append(fields, utiltrace.Field{Key: "Name", Value: name})
		}
		if gvr := f.identifier.GroupVersionResource(); !gvr.Empty() {
			fields = append(fields, utiltrace.Field{Key: "Resource", Value: gvr})
		}
		trace := utiltrace.New("RealFIFO Pop Process", fields...)
		defer trace.LogIfLong(100 * time.Millisecond)
	}
	f.metrics.numberOfQueuedItem.Set(float64(len(f.items)))

	// 处理该条目；这一步可能释放锁，允许其他 goroutine 向队列添加条目。
	err := f.whileProcessing_locked(func() error {
		// 这里包装成 Deltas 以兼容此前的 Pop 函数及解析返回值的调用方。
		return process(Deltas{item}, isInInitialList)
	})
	return Deltas{item}, err
}

// whileProcessing_locked 调用 `process` 函数并记录处理时延。
// 调用 `whileProcessing_locked` 前必须持有锁，返回时仍持有锁。
// 若 f.unlockWhileProcessing 为 true 且 f.items 队列不太长，whileProcessing_locked
// 会在调用 `process` 期间释放锁。
func (f *RealFIFO) whileProcessing_locked(process func() error) error {
	// 在调用 `process` 前解锁，使处理期间可以入队新条目。
	// 仅当队列中的条目少于 2 个满批次时才这样做，
	// 以防止队列无限增长。
	if f.unlockWhileProcessing && len(f.items) < f.batchSize*2 {
		f.lock.Unlock()
		defer f.lock.Lock()
	}
	startTime := time.Now()
	err := process()
	f.metrics.processingLatency.Observe(time.Since(startTime).Seconds())
	return err
}

// batchable 记录可以批量处理的 delta 类型。
var batchable = map[DeltaType]bool{
	Sync:     true,
	Replaced: true,
	Added:    true,
	Updated:  true,
	Deleted:  true,
}

// PopBatch 尽量多地弹出条目，用 processBatch 作为一批处理；若无法批量多个条目，
// 则用 processSingle 处理单个条目。
//
// 仅当 UnlockWhileProcessing 为 false 时，才保证 processBatch 和 processSingle
// 在锁内被调用。若处理函数要更新需要与队列保持同步的数据结构，请确保
// UnlockWhileProcessing 为 false。PopBatch 的调用方应为单线程消费者，
// 否则可能出现多个 ProcessBatchFunc 或 PopProcessFunc 同时运行。
//
// 批量仅限同一 key 不重复的可批处理 Delta 类型；遇到不可批处理类型或重复 key
// 时立即收束批次，保证同一对象的相对顺序。
func (f *RealFIFO) PopBatch(processBatch ProcessBatchFunc, processSingle PopProcessFunc) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	for len(f.items) == 0 {
		// 队列为空时，Pop() 会阻塞直到新条目入队。
		// 调用 Close() 后，f.closed 被置位并广播条件变量，
		// 使本循环继续并从 Pop() 返回。
		if f.closed {
			return ErrFIFOClosed
		}

		f.cond.Wait()
	}

	isInInitialList := !f.hasSynced_locked()
	unique := sets.NewString()
	deltas := make([]Delta, 0, min(len(f.items), f.batchSize))
	moveDeltaToProcessList := func(i int) {
		deltas = append(deltas, f.items[i])
		// 底层数组仍存在并引用该对象，因此若不置零引用，该对象不会被 GC。
		f.items[i] = Delta{}
	}
	// 仅将不重复的条目打包成一个批次
	for i := 0; i < f.batchSize && i < len(f.items); i++ {
		if f.initialPopulationCount > 0 && i >= f.initialPopulationCount {
			break
		}
		item := f.items[i]
		if !batchable[item.Type] {
			if len(deltas) == 0 {
				// 若不可批处理的 delta 位于列表首位，则单独处理它
				moveDeltaToProcessList(i)
			}
			// 遇到不可批处理的 delta 时收束批次
			break
		}
		id, err := f.keyOf(item)
		if err != nil {
			// 出错时在此收束批次。
			// TODO：当 RealFIFOOptions 像已弃用的 DeltaFIFO 一样支持传入 klog 实例后，记录该错误。
			// 仍然把这个损坏的条目弹出队列以兼容非批量行为；当首个元素损坏时应该是安全的，
			// 但对第 N 个损坏元素，存在这样一种风险：损坏条目仍可能被处理，
			// 意外破坏批次的唯一性。
			moveDeltaToProcessList(i)
			break
		}
		if unique.Has(id) {
			// 遇到重复条目时收束批次
			break
		}
		unique.Insert(id)
		moveDeltaToProcessList(i)
	}
	f.items = f.items[len(deltas):]
	// 按需递减 initialPopulationCount。
	// 放在 defer 中是为了只在处理完成*之后*递减，
	// 从而避免处理尚未结束时并发的 hasSynced 调用错误地返回 true。
	defer func() {
		if f.initialPopulationCount > 0 {
			f.initialPopulationCount -= len(deltas)
			f.checkSynced_locked()
		}
	}()

	// 仅当队列深度大于 10 且处理一个条目耗时超过 100 毫秒时才记录 trace
	// （整个批次最多 1 秒）。
	// 队列深度不会很高，因为处理条目时会锁住队列，处理完成前无法添加新条目。
	// https://github.com/kubernetes/kubernetes/issues/103789
	if len(f.items) > 10 {
		id, _ := f.keyOf(deltas[0])
		fields := []utiltrace.Field{
			{Key: "ID", Value: id},
			{Key: "Depth", Value: len(f.items)},
			{Key: "Reason", Value: "slow event handlers blocking the queue"},
			{Key: "BatchSize", Value: len(deltas)},
		}
		if name := f.identifier.Name(); len(name) > 0 {
			fields = append(fields, utiltrace.Field{Key: "Name", Value: name})
		}
		if gvr := f.identifier.GroupVersionResource(); !gvr.Empty() {
			fields = append(fields, utiltrace.Field{Key: "Resource", Value: gvr})
		}
		trace := utiltrace.New("RealFIFO PopBatch Process", fields...)
		defer trace.LogIfLong(min(100*time.Millisecond*time.Duration(len(deltas)), time.Second))
	}
	f.metrics.numberOfQueuedItem.Set(float64(len(f.items)))

	if len(deltas) == 1 {
		return f.whileProcessing_locked(func() error {
			return processSingle(Deltas{deltas[0]}, isInInitialList)
		})
	}
	return f.whileProcessing_locked(func() error {
		return processBatch(deltas, isInInitialList)
	})
}

// Bookmark 在允许时入队一个 bookmark Delta，用于推进已见的 resourceVersion。
// 未启用 EmitDeltaTypeBookmark 时直接忽略，避免消费者看到意外类型。
func (f *RealFIFO) Bookmark(resourceVersion string) error {
	if !f.emitDeltaTypeBookmark {
		return nil
	}
	f.lock.Lock()
	defer f.lock.Unlock()

	f.items = append(f.items, Delta{
		Type:   Bookmark,
		Object: BookmarkInfo{ResourceVersion: resourceVersion},
	})
	f.cond.Broadcast()
	return nil
}

// Replace
// 1. 找出 f.items 中不在 newItems 里的条目，为它们构造合成删除事件
// 2. 找出 knownObjects 中不在 newItems 里的条目，为它们构造合成删除事件
// 3. 将 newItems 加入队列
func (f *RealFIFO) Replace(newItems []interface{}, resourceVersion string) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	var err error
	// 原子模式只产生一条 ReplacedAll，否则逐个核对队列与 knownObjects 的差异。
	if f.emitAtomicEvents {
		err = f.addReplaceToItemsLocked(newItems, resourceVersion)
	} else {
		err = reconcileReplacement(f.logger, f.items, f.knownObjects, newItems, f.keyOf,
			func(obj DeletedFinalStateUnknown) error {
				return f.addToItems_locked(Deleted, true, obj)
			},
			func(obj interface{}) error {
				return f.addToItems_locked(Replaced, false, obj)
			})
	}
	if err != nil {
		return err
	}

	if !f.populated {
		f.populated = true
		// 只有首次 Replace 才计入初始同步；后续 Replace 是重连后的全量刷新。
		f.initialPopulationCount = len(f.items)
		f.checkSynced_locked()
	}

	return nil
}

// reconcileReplacement 结合队列中已有的条目与新一批条目，依据队列条目和已知
// 对象的状态，按条目是被删除还是被替换/新增，分别调用 onDelete 和 onReplace。
func reconcileReplacement(
	logger klog.Logger,
	queuedItems []Delta,
	knownObjects KeyListerGetter,
	newItems []interface{},
	keyOf func(obj interface{}) (string, error),
	onDelete func(obj DeletedFinalStateUnknown) error,
	onReplace func(obj interface{}) error,
) error {
	// 先确定所有待添加条目的 key。在为不存在于 newItems 中的条目创建完合成删除
	// 事件之前，不能把这些条目加入队列
	newKeys := sets.Set[string]{}
	for _, obj := range newItems {
		key, err := keyOf(obj)
		if err != nil {
			return KeyError{obj, err}
		}
		newKeys.Insert(key)
	}

	queuedKeys := []string{}
	lastQueuedItemForKey := map[string]Delta{}
	for _, queuedItem := range queuedItems {
		queuedKey, err := keyOf(queuedItem.Object)
		if err != nil {
			return KeyError{queuedItem.Object, err}
		}

		if _, seen := lastQueuedItemForKey[queuedKey]; !seen {
			queuedKeys = append(queuedKeys, queuedKey)
		}
		lastQueuedItemForKey[queuedKey] = queuedItem
	}

	// 队列中已有的删除都很重要，有两种情况：
	// 1. queuedItems 中有 key/X 的 delete 且 newItems 中有 key/X 的 replace。
	//    说明排队的 UID 已被删除，又创建了一个新的。
	// 2. queuedItems 中有 key/X 的 delete 且 newItems 中没有 key/X。
	//    说明排队的条目已被删除。
	// 针对队列中的对象做删除探测。
	for _, queuedKey := range queuedKeys {
		if newKeys.Has(queuedKey) {
			continue
		}

		// 删除已存在但不在新列表中的条目。
		// 若与 apiserver 断连期间错过了 watch 删除事件，就可能出现这种情况。
		lastQueuedItem := lastQueuedItemForKey[queuedKey]
		// 若条目已标记为删除，无需再加一个删除事件
		if lastQueuedItem.Type == Deleted {
			continue
		}

		// 走到这里说明排队条目的最后一条记录*不是*删除，需要我们补一个删除事件
		deletedObj := lastQueuedItem.Object

		retErr := onDelete(DeletedFinalStateUnknown{
			Key: queuedKey,
			Obj: deletedObj,
		})
		if retErr != nil {
			return fmt.Errorf("couldn't enqueue object: %w", retErr)
		}
	}

	// 探测不在队列中、但存在于 KnownObjects 中的对象的删除
	knownKeys := knownObjects.ListKeys()
	for _, knownKey := range knownKeys {
		if newKeys.Has(knownKey) { // 仍然存在
			continue
		}
		if _, inQueuedItems := lastQueuedItemForKey[knownKey]; inQueuedItems { // 这些已添加过删除
			continue
		}

		deletedObj, exists, err := knownObjects.GetByKey(knownKey)
		if err != nil {
			deletedObj = nil
			utilruntime.HandleErrorWithLogger(logger, err, "Error during lookup, placing DeleteFinalStateUnknown marker without object", "key", knownKey)
		} else if !exists {
			deletedObj = nil
			utilruntime.HandleErrorWithLogger(logger, nil, "Key does not exist in known objects store, placing DeleteFinalStateUnknown marker without object", "key", knownKey)
		}
		retErr := onDelete(DeletedFinalStateUnknown{
			Key: knownKey,
			Obj: deletedObj,
		})
		if retErr != nil {
			return fmt.Errorf("couldn't enqueue object: %w", retErr)
		}
	}

	// 条目所需的删除事件已就绪，现在可以把 newItems 加入 items 队列
	for _, obj := range newItems {
		if err := onReplace(obj); err != nil {
			return fmt.Errorf("couldn't enqueue object: %w", err)
		}
	}

	return nil
}

// Resync 会确保 Store 中的每个对象的 key 都在队列里。这本应是空操作，
// 因为所有操作都维持着这一性质。
func (f *RealFIFO) Resync() error {
	// TODO 从逻辑上讲这无法由 FIFO 完成，只能由 indexer 完成
	f.lock.Lock()
	defer f.lock.Unlock()

	// 原子模式下仅入队一条 SyncAll，由消费者重放整个 store。
	if f.emitAtomicEvents {
		return f.addResyncToItemsLocked()
	}

	if f.knownObjects == nil {
		return nil
	}

	keysInQueue := sets.Set[string]{}
	for _, item := range f.items {
		key, err := f.keyOf(item.Object)
		if err != nil {
			return KeyError{item, err}
		}
		keysInQueue.Insert(key)
	}

	knownKeys := f.knownObjects.ListKeys()
	for _, knownKey := range knownKeys {
		// 若在做 Resync() 且该对象已有事件排队，则忽略对它的 Resync。
		// 这是为了避免这样一种竞态：resync 携带的是对象的旧值（因为为该对象排队
		// 事件并不会触发底层存储 <knownObjects> 的改变）。
		if keysInQueue.Has(knownKey) {
			continue
		}

		knownObj, exists, err := f.knownObjects.GetByKey(knownKey)
		if err != nil {
			utilruntime.HandleErrorWithLogger(f.logger, err, "Unable to queue object for sync", "key", knownKey)
			continue
		} else if !exists {
			utilruntime.HandleErrorWithLogger(f.logger, nil, "Key does not exist in known objects store, unable to queue object for sync", "key", knownKey)
			continue
		}

		retErr := f.addToItems_locked(Sync, true, knownObj)
		if retErr != nil {
			return fmt.Errorf("couldn't queue object: %w", err)
		}
	}

	return nil
}

// Transformer 实现 TransformingStore 接口。
func (f *RealFIFO) Transformer() TransformFunc {
	return f.transformer
}

// NewRealFIFO 返回一个可用于对待处理条目排队的 Store。
//
// Deprecated: 请改用 NewRealFIFOWithOptions。
// 旧构造函数只能设置三个字段；新代码应使用 RealFIFOOptions 以启用原子事件与指标。
func NewRealFIFO(keyFunc KeyFunc, knownObjects KeyListerGetter, transformer TransformFunc) *RealFIFO {
	return NewRealFIFOWithOptions(RealFIFOOptions{
		KeyFunction:  keyFunc,
		KnownObjects: knownObjects,
		Transformer:  transformer,
	})
}

// NewRealFIFOWithOptions 返回一个可用于处理条目变更的 Queue。
// 另见 RealFIFO 的注释。
func NewRealFIFOWithOptions(opts RealFIFOOptions) *RealFIFO {
	// 未指定 key 函数时默认使用 namespace/name。
	if opts.KeyFunction == nil {
		opts.KeyFunction = MetaNamespaceKeyFunc
	}

	if opts.AtomicEvents {
		// 若以原子方式发出事件，就不能依赖 known objects 存储，
		// 因为原子事件要求在处理事件时可以释放锁。
		if opts.KnownObjects != nil {
			panic("coding error: knownObjects must not be provided when AtomicEvents is true")
		}
	} else {
		if opts.UnlockWhileProcessing {
			panic("coding error: UnlockWhileProcessing must be false when AtomicEvents is false")
		}
		if opts.KnownObjects == nil {
			panic("coding error: knownObjects must be provided when AtomicEvents is false")
		}
		// 若不以原子方式发出事件，就不能发出 bookmark delta。
		if opts.EmitDeltaTypeBookmark {
			panic("coding error: EmitDeltaTypeBookmark must be false when AtomicEvents is false")
		}
	}

	// 批量大小固定为 defaultBatchSize；批量越大，单次处理耗时越长。
	f := &RealFIFO{
		logger:                klog.Background(),
		name:                  "RealFIFO",
		items:                 make([]Delta, 0, 10),
		synced:                make(chan struct{}),
		keyFunc:               opts.KeyFunction,
		knownObjects:          opts.KnownObjects,
		transformer:           opts.Transformer,
		batchSize:             defaultBatchSize,
		emitAtomicEvents:      opts.AtomicEvents,
		emitDeltaTypeBookmark: opts.EmitDeltaTypeBookmark,
		unlockWhileProcessing: opts.UnlockWhileProcessing,
		identifier:            opts.Identifier,
		metrics:               newFIFOMetrics(opts.Identifier, opts.MetricsProvider),
	}
	if opts.Logger != nil {
		f.logger = *opts.Logger
	}
	if name := opts.Name; name != "" {
		f.name = name
	}
	if name := opts.Identifier.Name(); name != "" {
		f.name = name
	}
	f.logger = klog.LoggerWithName(f.logger, f.name)
	f.cond.L = &f.lock
	return f
}
