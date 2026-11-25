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
	"fmt"
	"sync"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	"k8s.io/klog/v2"
	utiltrace "k8s.io/utils/trace"
)

// DeltaFIFOOptions 是 DeltaFIFO 的配置参数，全部可选。
//
// 除 Name/Logger 外，其余字段决定队列的键计算、删除探测与向后兼容行为；
// 默认值由 NewDeltaFIFOWithOptions 填充。
type DeltaFIFOOptions struct {
	// 若设置，日志将输出到该 logger 而非 klog.Background()。
	// fifo 的名称会自动附加。
	Logger *klog.Logger

	// Name 可用于覆盖新实例默认的 "DeltaFIFO" 名称。
	Name string

	// KeyFunction 用于确定对象应有的 key。（它通过返回的 DeltaFIFO 的 KeyOf()
	// 方法暴露，并对已删除对象和队列状态做了额外处理。）
	// 可选，默认为 MetaNamespaceKeyFunc。
	KeyFunction KeyFunc

	// KnownObjects 应返回该队列的消费者“已知”的 key 列表。Replace() 被调用时，
	// 用它来判断哪些条目缺失；缺失的条目会产生 'Deleted' delta。
	// 如果可以容忍 Replace() 时丢失删除事件，KnownObjects 可以为 nil。
	//
	// 通常就是 informer 的本地 Indexer：通过与 re-list 结果对比来发现已删除对象。
	KnownObjects KeyListerGetter

	// EmitDeltaTypeReplaced 表示队列消费者理解 Replaced DeltaType。在加入
	// `Replaced` 事件类型之前，Replace() 调用与 Sync() 处理方式相同。出于向后
	// 兼容考虑，默认为 false。为 true 时，传给 Replace() 的条目会发送 `Replaced`
	// 事件；为 false 时则改为发送 `Sync` 事件。
	EmitDeltaTypeReplaced bool

	// 若设置，会在对象入队前对其调用。详情请参见 TransformFunc 的注释。
	Transformer TransformFunc
}

// DeltaFIFO 与 FIFO 类似，但有两点不同。其一，与某个对象 key 关联的累积器
// 不是该对象本身，而是一个 Deltas——即该对象的 Delta 值切片。把对象应用到
// Deltas 意味着追加一个 Delta，除非待追加的 Delta 是 Deleted 且 Deltas 已经
// 以 Deleted 结尾；此时 Deltas 不会增长，但如果较旧的 Deleted 的对象是
// DeletedFinalStateUnknown，末尾的 Deleted 会被新的 Deleted 替换。
//
// 第二点不同是 DeltaFIFO 提供了两种额外的对象应用到累积器的方式：Replaced
// 和 Sync。若 EmitDeltaTypeReplaced 未设为 true，为向后兼容，replace 事件会
// 使用 Sync。周期性 resync 事件也使用 Sync。
//
// DeltaFIFO 是生产者-消费者队列，Reflector 充当生产者，消费者则是调用 Pop()
// 方法的一方。
//
// DeltaFIFO 适用于以下场景：
//   - 每个对象变更（delta）至多处理一次。
//   - 处理对象时，希望看到自上次处理以来发生的所有变化。
//   - 需要处理其中一些对象的删除。
//   - 可能希望周期性地重新处理对象。
//
// DeltaFIFO 的 Pop()、Get() 和 GetByKey() 方法返回 interface{} 以满足
// Store/Queue 接口，但返回的始终是 Deltas 类型的对象。List() 返回 FIFO 中
// 每个累积器里最新的对象。
//
// DeltaFIFO 的 knownObjects（KeyListerGetter）提供按 Store key 列出与获取
// 对象的能力。这些对象称为“已知对象”，该集合会以不同的方式影响 Delete、
// Replace 和 Resync 方法的行为。
//
// 关于线程的说明：如果在多个线程中并行调用 Pop()，可能出现多个线程在处理
// 同一对象略微不同的版本。
type DeltaFIFO struct {
	// logger 是每个实例独立的 logger。构造实例时确定，默认为 klog.Background()。
	logger klog.Logger

	// name 是 fifo 的名称，会包含在 logger 中。
	name string

	// lock/cond 保护对 'items' 和 'queue' 的访问。
	// cond 的 L 指向 &lock，等待/唤醒必须与同一把锁配合使用。
	lock sync.RWMutex
	cond sync.Cond

	// `items` 将 key 映射到 Deltas。每个 Deltas 至少包含一个 Delta。
	items map[string]Deltas

	// `queue` 维护供 Pop() 消费的 key 的 FIFO 顺序。
	// `queue` 中没有重复项。
	// 一个 key 在 `queue` 中当且仅当它在 `items` 中。
	//
	// 两个不变量必须同时成立：队列中键不重复，且与 items 的键集合一致；
	// Pop 删除 items 时也会同步从 queue 头部移除。
	queue []string

	// synced 初始为未关闭的 channel。一旦初始同步被认为完成，
	// checkSynced_locked 会将其关闭（仅一次！）。
	synced       chan struct{}
	syncedClosed bool

	// 若 Replace() 插入的第一批条目已全部填充，或首先调用了
	// Delete/Add/Update/AddIfNotPresent，则 populated 为 true。
	populated bool
	// initialPopulationCount 是首次调用 Replace() 插入的条目数量。
	//
	// 初始 Replace 产生的条目每被 Pop 消费一个就递减，降到 0 且 populated 为真时才算同步完成。
	initialPopulationCount int

	// keyFunc 用于生成队列条目插入和检索所用的 key，必须具有确定性。
	keyFunc KeyFunc

	// knownObjects 列出“已知”的 key——影响 Delete()、Replace() 和 Resync()。
	knownObjects KeyListerGetter

	// 用于标识队列已关闭，使控制循环能在队列为空时退出。
	// 目前不用于限制任何 CRUD 操作。
	closed bool

	// emitDeltaTypeReplaced 决定调用 Replace() 时发出 Replaced 还是 Sync
	// DeltaType（用于保持向后兼容）。
	emitDeltaTypeReplaced bool

	// 若非 nil，会对每个对象调用。
	transformer TransformFunc
}

// TransformFunc 允许在对象被处理前对其进行转换。
//
// 最常见的用法是：当某个组件不关心对象的某些部分时，清理这些部分以降低
// 组件的内存占用。
//
// v1.27 新增：TransformFunc 在所有其他角色之前看到对象，因此现在可以安全地
// 原地修改对象而不必做拷贝。
//
// 建议 TransformFunc 具备幂等性。若缓存中已存在的对象被传入 Replace()，
// 则必须幂等，以避免重复修改它们。不过默认 informer 不会把已存在的对象
// 传给 Replace。
//
// 注意：TransformFunc 在对象插入通知队列时被调用，对性能极其敏感；
// 请勿在其中执行耗时操作。
//
// 另外，它运行在持有 DeltaFIFO 锁的路径上，任何阻塞都会拖慢整个 informer 的事件分发。
type TransformFunc func(interface{}) (interface{}, error)

// DeltaType 是变更的类型（新增、删除等）。
type DeltaType string

// 变更类型定义。
//
// 这些字符串会出现在事件处理器的回调里，修改取值属于破坏性变更，必须保持稳定。
const (
	Added   DeltaType = "Added"
	Updated DeltaType = "Updated"
	Deleted DeltaType = "Deleted"
	// 当遇到 watch 错误而必须重新 list，或首次列出对象时发出 Replaced。
	// 我们无法得知被替换的对象是否发生了变化。
	//
	// 注意：早期版本的 DeltaFIFO 对 Replace 事件也使用 Sync。因此仅当选项
	// EmitDeltaTypeReplaced 为 true 时才会发出 Replaced。
	Replaced DeltaType = "Replaced"
	// 当遇到 watch 错误而必须重新 list，或首次列出对象时发出 ReplacedAll。
	// 原因与 Replaced 相同，但在 FIFO 支持原子替换时改为发出此事件。
	// 该事件返回被替换条目的完整列表，而非单个对象。
	ReplacedAll DeltaType = "ReplacedAll"
	// Sync 用于周期性 resync 期间产生的合成事件。
	Sync DeltaType = "Sync"
	// SyncAll 表示所有已知对象都应被重新处理。
	// 该事件包含 SyncAllInfo 类型的对象。
	SyncAll DeltaType = "SyncAll"
	// Bookmark 在 Bookmark 调用和 Replace 调用时发出，用于向消费者传递
	// resource version 信息。
	Bookmark DeltaType = "Bookmark"
)

// Delta 是 Deltas（Delta 对象列表）的成员，而 Deltas 正是 DeltaFIFO 存储的
// 类型。它告诉你发生了什么变化，以及该变化之后*对象的状态。
//
// [*] 除非变化是删除——此时得到的是对象被删除前的最终状态。
//
// Type 描述变化种类，Object 是该变化之后的对象状态；
// 删除场景下 Object 可能是 DeletedFinalStateUnknown 墓碑，表示最终状态未知。
type Delta struct {
	Type   DeltaType
	Object interface{}
}

// Deltas 是单个对象的一个或多个 'Delta' 组成的列表。
// 最旧的 delta 在下标 0，最新的 delta 在末尾。
type Deltas []Delta

// NewDeltaFIFO 返回一个可用于处理条目变更的 Queue。
//
// keyFunc 用于确定对象应有的 key。（它通过返回的 DeltaFIFO 的 KeyOf() 方法
// 暴露，并对已删除对象和队列状态做了额外处理。）
//
// 可以传入 'knownObjects' 以修改 Delete、Replace 和 Resync 的行为。
// 若不需要这些修改，它可以为 nil。
//
// TODO：考虑把 keyLister 与本对象合并，在 Pop() 被调用时跟踪“已知” key 的
// 列表。需要思考这对错误重试的影响。
//
//	注意：误用本设计且使用外部已知对象源时可能引发竞态。
//	是否存在潜在竞态取决于消费者如何修改 knownObjects。在 Pop() 中，process
//	函数是在锁内调用的，因此在其中更新需要与队列保持同步的数据结构
//	（如 knownObjects）是安全的。
//
//	示例：
//	当消费者是 sharedIndexInformer 时
//	(https://github.com/kubernetes/kubernetes/blob/0cdd940f/staging/src/k8s.io/client-go/tools/cache/shared_informer.go#L192)，
//	不存在竞态，因为 knownObjects（s.indexer）是在 DeltaFIFO 的锁保护下被安全
//	修改的。唯一的例外是 GetStore() 和 GetIndexer() 方法，它们暴露了修改底层
//	存储的途径。目前这两个方法仅用于创建 Lister 和内部测试。
//
// 另见 DeltaFIFO 的注释。
//
// 警告：本函数构造的 DeltaFIFO 不区分由 Replace 调用引发的事件（例如来自
// relist，其中可能包含对象更新）与由周期性 resync 产生的合成事件（仅重新
// 发出已有对象）。详见 https://issue.k8s.io/86015。
//
// 若要按事件类型区分并收到 `Replaced` 事件，请改用
// `NewDeltaFIFOWithOptions(DeltaFIFOOptions{..., EmitDeltaTypeReplaced: true})`。
//
// Deprecated: 等价于 NewDeltaFIFOWithOptions(DeltaFIFOOptions{KeyFunction: keyFunc, KnownObjects: knownObjects})。
// 已弃用：请改用 NewDeltaFIFOWithOptions，以便显式选择是否区分 Replaced 事件。
func NewDeltaFIFO(keyFunc KeyFunc, knownObjects KeyListerGetter) *DeltaFIFO {
	return NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction:  keyFunc,
		KnownObjects: knownObjects,
	})
}

// NewDeltaFIFOWithOptions 返回一个可用于处理条目变更的 Queue。
// 另见 DeltaFIFO 的注释。
func NewDeltaFIFOWithOptions(opts DeltaFIFOOptions) *DeltaFIFO {
	if opts.KeyFunction == nil {
		opts.KeyFunction = MetaNamespaceKeyFunc
	}

	f := &DeltaFIFO{
		logger:       klog.Background(),
		name:         "DeltaFIFO",
		synced:       make(chan struct{}),
		items:        map[string]Deltas{},
		queue:        []string{},
		keyFunc:      opts.KeyFunction,
		knownObjects: opts.KnownObjects,

		emitDeltaTypeReplaced: opts.EmitDeltaTypeReplaced,
		transformer:           opts.Transformer,
	}
	// 先落默认值，再覆盖可选项：Name/Logger 都允许为空。
	if opts.Logger != nil {
		f.logger = *opts.Logger
	}
	if name := opts.Name; name != "" {
		f.name = name
	}
	f.logger = klog.LoggerWithName(f.logger, f.name)
	// Cond 必须绑定到实际的锁变量地址，拷贝结构体会破坏该绑定。
	f.cond.L = &f.lock
	return f
}

var (
	_ = Queue(&DeltaFIFO{})             // DeltaFIFO 是一个 Queue。
	_ = TransformingStore(&DeltaFIFO{}) // DeltaFIFO 实现 TransformingStore，以支持内存优化。
	_ = DoneChecker(&DeltaFIFO{})       // DeltaFIFO 实现 DoneChecker。
)

var (
	// ErrZeroLengthDeltasObject：若遇到长度为零的 Deltas 对象（理论上不可能，
	// 但为完备起见保留），会在 KeyError 中返回。
	ErrZeroLengthDeltasObject = errors.New("0 length Deltas object; can't get key")
)

// Close 关闭队列。
//
// 只标记关闭并唤醒所有等待者，不清理已有条目：
// 消费者仍需把队列中剩余条目处理完（或由调用方直接丢弃）。
func (f *DeltaFIFO) Close() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.closed = true
	f.cond.Broadcast()
}

// KeyOf 暴露 f 的 keyFunc，同时也能识别 Deltas 对象或 DeletedFinalStateUnknown
// 对象的 key。
//
// 对 Deltas 取最新一条的对象来计算键；对墓碑则直接返回其保存的 Key，
// 因为墓碑内的对象可能为 nil 或不完整。
func (f *DeltaFIFO) KeyOf(obj interface{}) (string, error) {
	if d, ok := obj.(Deltas); ok {
		if len(d) == 0 {
			return "", KeyError{obj, ErrZeroLengthDeltasObject}
		}
		obj = d.Newest().Object
	}
	if d, ok := obj.(DeletedFinalStateUnknown); ok {
		return d.Key, nil
	}
	return f.keyFunc(obj)
}

// Transformer 实现 TransformingStore 接口。
func (f *DeltaFIFO) Transformer() TransformFunc {
	return f.transformer
}

// 若首先调用了 Add/Update/Delete/AddIfNotPresent，或 Replace() 插入的第一批
// 条目已被 Pop 完，HasSynced 返回 true。
func (f *DeltaFIFO) HasSynced() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.hasSynced_locked()
}

// 若首先调用了 Add/Update/Delete/AddIfNotPresent，或 Replace() 插入的第一批
// 条目已被 Pop 完，HasSyncedChecker 即为 done。
func (f *DeltaFIFO) HasSyncedChecker() DoneChecker {
	return f
}

// Name 实现 [DoneChecker.Name]。
func (f *DeltaFIFO) Name() string {
	return f.name
}

// Done 实现 [DoneChecker.Done]。
func (f *DeltaFIFO) Done() <-chan struct{} {
	return f.synced
}

// hasSynced_locked 返回此前 checkSynced_locked 调用的结果。
func (f *DeltaFIFO) hasSynced_locked() bool {
	return f.syncedClosed
}

// checkSynced_locked 检查初始同步是否完成。每当 populated 或
// initialPopulationCount 变化时都必须调用。
//
// 调用方必须已持有 f.lock；close 只执行一次，避免重复关闭 channel 导致 panic。
func (f *DeltaFIFO) checkSynced_locked() {
	synced := f.populated && f.initialPopulationCount == 0
	if synced && !f.syncedClosed {
		// 初始同步完成。
		f.syncedClosed = true
		close(f.synced)
	}
}

// Add 插入一个条目并放入队列。仅当该条目尚不存在时才入队。
func (f *DeltaFIFO) Add(obj interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	// 任何 Add/Update/Delete 都说明队列已被外部事件填充，无需等待 Replace。
	f.populated = true
	f.checkSynced_locked()
	return f.queueActionLocked(Added, obj)
}

// Update 与 Add 相同，但产生的是 Updated Delta。
func (f *DeltaFIFO) Update(obj interface{}) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	f.checkSynced_locked()
	return f.queueActionLocked(Updated, obj)
}

// Delete 与 Add 相同，但产生 Deleted Delta。若给定对象并不存在，则被忽略
// （例如，它可能已被某次 Replace（re-list）删除）。在本方法中，
// `f.knownObjects`（若非 nil）通过 GetByKey 提供*额外*的、被认为已存在的对象。
func (f *DeltaFIFO) Delete(obj interface{}) error {
	id, err := f.KeyOf(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	f.checkSynced_locked()
	if f.knownObjects == nil {
		if _, exists := f.items[id]; !exists {
			// 大概是在某次 relist 时被删除了。
			// 不要对同一次删除做第二次报告。
			return nil
		}
	} else {
		// 仅当对象在 knownObjects 中不存在、且 items 中也没有对应条目时，
		// 才跳过这次“删除”动作。注意：即使 items 中已有“删除”动作也可以忽略，
		// 因为它会在 "queueActionLocked" 中自动去重。
		//
		// 两处都不存在才认为是重复的删除事件；只要任一处存在，仍需入队，
		// 由 queueActionLocked 负责后续去重。
		_, exists, err := f.knownObjects.GetByKey(id)
		_, itemsExist := f.items[id]
		if err == nil && !exists && !itemsExist {
			// 大概是在某次 relist 时被删除了。
			// 不要对同一次删除做第二次报告。
			return nil
		}
	}

	// 存在于 items 和/或 KnownObjects 中
	return f.queueActionLocked(Deleted, obj)
}

// re-list 和 watch 可能以任意顺序多次投递相同的更新。若最近两个 delta 相同，
// 本函数会将它们合并。
//
// 只比较末尾两条即可：每次入队都调用一次，连续的重复会在下一次调用时继续合并。
func dedupDeltas(deltas Deltas) Deltas {
	n := len(deltas)
	if n < 2 {
		return deltas
	}
	a := &deltas[n-1]
	b := &deltas[n-2]
	if out := isDup(a, b); out != nil {
		deltas[n-2] = *out
		return deltas[:n-1]
	}
	return deltas
}

// 若 a 与 b 表示同一事件，返回应当保留的 delta；否则返回 nil。
// TODO：除了删除之外，还有别的需要去重吗？
func isDup(a, b *Delta) *Delta {
	if out := isDeletionDup(a, b); out != nil {
		return out
	}
	// TODO：检测其他重复情况？存在吗？
	return nil
}

// 若两者都是删除，保留信息更多的一个。
//
// 两个删除事件相邻时，保留对象信息更完整的那一个：
// DeletedFinalStateUnknown 墓碑信息更少，因此被真实对象对应的 Delta 取代。
func isDeletionDup(a, b *Delta) *Delta {
	if b.Type != Deleted || a.Type != Deleted {
		return nil
	}
	// 需要更精细的判断吗，还是这样就够了？
	if _, ok := b.Object.(DeletedFinalStateUnknown); ok {
		return a
	}
	return b
}

// queueActionLocked 向对象的 delta 列表追加内容。调用方必须先加锁。
func (f *DeltaFIFO) queueActionLocked(actionType DeltaType, obj interface{}) error {
	return f.queueActionInternalLocked(actionType, actionType, obj)
}

// queueActionInternalLocked 向对象的 delta 列表追加内容。
// actionType 会被发出，必须遵循 emitDeltaTypeReplaced。
// internalActionType 仅在本函数内部使用，必须忽略 emitDeltaTypeReplaced。
// 调用方必须先加锁。
func (f *DeltaFIFO) queueActionInternalLocked(actionType, internalActionType DeltaType, obj interface{}) error {
	id, err := f.KeyOf(obj)
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
		// 墓碑与 Sync 事件的对象早已经过 transformer，重复调用既浪费也可能破坏幂等性。
		if !isTombstone && internalActionType != Sync {
			var err error
			obj, err = f.transformer(obj)
			if err != nil {
				return err
			}
		}
	}

	oldDeltas := f.items[id]
	newDeltas := append(oldDeltas, Delta{actionType, obj})
	newDeltas = dedupDeltas(newDeltas)

	if len(newDeltas) > 0 {
		// 仅当该键首次出现时才追加到 queue，保证 queue 中无重复。
		if _, exists := f.items[id]; !exists {
			f.queue = append(f.queue, id)
		}
		f.items[id] = newDeltas
		f.cond.Broadcast()
	} else {
		// 这永远不会发生，因为 dedupDeltas 在非空输入（如此处）时
		// 从不返回空列表。
		// 万一真的发生了，也要处理并报错。
		if oldDeltas == nil {
			utilruntime.HandleErrorWithLogger(f.logger, nil, "Impossible dedupDeltas, ignoring", "id", id, "oldDeltas", oldDeltas, "obj", obj)
			return nil
		}
		utilruntime.HandleErrorWithLogger(f.logger, nil, "Impossible dedupDeltas, breaking invariant by storing empty Deltas", "id", id, "oldDeltas", oldDeltas, "obj", obj)
		f.items[id] = newDeltas
		return fmt.Errorf("Impossible dedupDeltas for id=%q: oldDeltas=%#+v, obj=%#+v; broke DeltaFIFO invariant by storing empty Deltas", id, oldDeltas, obj)
	}
	return nil
}

// IsClosed 检查队列是否已关闭。
func (f *DeltaFIFO) IsClosed() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.closed
}

// Pop 会阻塞直到队列中有条目，然后返回一个。若有多个条目就绪，则按它们被
// 添加/更新的顺序返回。条目在返回前会从队列（和存储）中移除，因此若未能成功
// 处理，需要用 AddIfNotPresent() 把它加回去。
// process 函数在锁内调用，所以在其中更新需要与队列保持同步的数据结构
// （如 knownKeys）是安全的。
// process 应避免昂贵的 I/O 操作，以免其他队列操作（即 Add() 和 Get()）被
// 阻塞太久。
//
// Pop 返回一个 'Deltas'，其中完整列出了该对象在队列中停留期间发生的所有事情
// （delta）。
func (f *DeltaFIFO) Pop(process PopProcessFunc) (interface{}, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	for {
		for len(f.queue) == 0 {
			// 队列为空时，Pop() 会阻塞直到新条目入队。
			// 调用 Close() 后，f.closed 被置位并广播条件变量，
			// 使本循环继续并从 Pop() 返回。
			if f.closed {
				return nil, ErrFIFOClosed
			}

			f.cond.Wait()
		}
		// 在消费初始批次之前记录该状态，供 process 判断这是否是“首次列表”事件。
		isInInitialList := !f.hasSynced_locked()
		id := f.queue[0]
		f.queue = f.queue[1:]
		depth := len(f.queue)
		if f.initialPopulationCount > 0 {
			f.initialPopulationCount--
			f.checkSynced_locked()
		}
		item, ok := f.items[id]
		if !ok {
			// 这不应该发生
			utilruntime.HandleErrorWithLogger(f.logger, nil, "Inconceivable! Item was in f.queue but not f.items; ignoring", "id", id)
			continue
		}
		// 先删除再处理：process 在锁内执行时，即使再次 Add 同一个键也会重新入队，
		// 从而避免丢失处理期间到达的更新。
		delete(f.items, id)
		// 仅当队列深度大于 10 且处理一个条目耗时超过 100 毫秒时才记录 trace。
		// 队列深度不会很高，因为处理条目时会锁住队列，处理完成前无法添加新条目。
		// https://github.com/kubernetes/kubernetes/issues/103789
		if depth > 10 {
			trace := utiltrace.New("DeltaFIFO Pop Process",
				utiltrace.Field{Key: "ID", Value: id},
				utiltrace.Field{Key: "Depth", Value: depth},
				utiltrace.Field{Key: "Reason", Value: "slow event handlers blocking the queue"})
			defer trace.LogIfLong(100 * time.Millisecond)
		}
		err := process(item, isInInitialList)
		// 这里无需 copyDeltas，因为所有权已转移给调用方。
		return item, err
	}
}

// Replace 原子地完成两件事：(1) 用 Sync 或 Replaced DeltaType 添加给定对象，
// 然后 (2) 执行一些删除。具体而言：对每个已存在、但其 key 不在 `list` 中任何
// 对象里的 key K，等效于 `Delete(DeletedFinalStateUnknown{K, O})`，其中 O 是
// K 最新的已知对象。已存在的 key 是 `f.items` 与 `f.knownObjects`（若非 nil）
// 中 key 的并集。key K 的最后已知对象是 `f.items` 中最后一个 delta 里的对象；
// 若 `f.items` 中没有 K 的 delta，则是 `f.knownObjects` 中的对象。
func (f *DeltaFIFO) Replace(list []interface{}, _ string) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	keys := make(sets.Set[string], len(list))

	// 为旧客户端保持向后兼容。
	// 默认仍发出 Sync，只有显式打开 EmitDeltaTypeReplaced 才使用 Replaced 类型。
	action := Sync
	if f.emitDeltaTypeReplaced {
		action = Replaced
	}

	// 为每个新条目添加 Sync/Replaced 动作。
	//
	// internalActionType 固定为 Replaced：transformer 判定需要按“非首次事件”处理，
	// 但又不受 emitDeltaTypeReplaced 的兼容性开关影响。
	for _, item := range list {
		key, err := f.KeyOf(item)
		if err != nil {
			return KeyError{item, err}
		}
		keys.Insert(key)
		if err := f.queueActionInternalLocked(action, Replaced, item); err != nil {
			return fmt.Errorf("couldn't enqueue object: %v", err)
		}
	}

	// 针对队列中的对象做删除探测
	queuedDeletions := 0
	for k, oldItem := range f.items {
		if keys.Has(k) {
			continue
		}
		// 删除已存在但不在新列表中的条目。
		// 若与 apiserver 断连期间错过了 watch 删除事件，就可能出现这种情况。
		var deletedObj interface{}
		if n := oldItem.Newest(); n != nil {
			deletedObj = n.Object

			// 若上一个对象是 DeletedFinalStateUnknown，需提取真正的 Object
			if d, ok := deletedObj.(DeletedFinalStateUnknown); ok {
				deletedObj = d.Obj
			}
		}
		queuedDeletions++
		if err := f.queueActionLocked(Deleted, DeletedFinalStateUnknown{k, deletedObj}); err != nil {
			return err
		}
	}

	if f.knownObjects != nil {
		// 探测不在队列中、但存在于 KnownObjects 中的对象的删除
		//
		// 这部分覆盖“队列里已经没有、但缓存中还残留”的对象：
		// 例如上次处理失败或事件被合并后，仍需补发一次删除通知。
		knownKeys := f.knownObjects.ListKeys()
		for _, k := range knownKeys {
			if keys.Has(k) {
				continue
			}
			if len(f.items[k]) > 0 {
				continue
			}

			deletedObj, exists, err := f.knownObjects.GetByKey(k)
			if err != nil {
				deletedObj = nil
				utilruntime.HandleErrorWithLogger(f.logger, err, "Unexpected error during lookup, placing DeleteFinalStateUnknown marker without object", "key", k)
			} else if !exists {
				deletedObj = nil
				f.logger.Info("Key does not exist in known objects store, placing DeleteFinalStateUnknown marker without object", "key", k)
			}
			queuedDeletions++
			if err := f.queueActionLocked(Deleted, DeletedFinalStateUnknown{k, deletedObj}); err != nil {
				return err
			}
		}
	}

	if !f.populated {
		f.populated = true
		// 初始批次可能同时包含新增与删除，两者都要被消费才算完成第一次同步。
		f.initialPopulationCount = keys.Len() + queuedDeletions
		f.checkSynced_locked()
	}

	return nil
}

// Resync 会为 `f.knownObjects` 列出的、其 key 尚未排队待处理的每个对象添加
// 一个 Sync 类型的 Delta。若 `f.knownObjects` 为 `nil`，Resync 不做任何事。
//
// 这是周期性 resync 的入口，不会访问 apiserver，只把缓存中已有对象重新投递给消费者。
func (f *DeltaFIFO) Resync() error {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.knownObjects == nil {
		return nil
	}

	keys := f.knownObjects.ListKeys()
	for _, k := range keys {
		if err := f.syncKeyLocked(k); err != nil {
			return err
		}
	}
	return nil
}

func (f *DeltaFIFO) syncKeyLocked(key string) error {
	obj, exists, err := f.knownObjects.GetByKey(key)
	if err != nil {
		utilruntime.HandleErrorWithLogger(f.logger, err, "Unexpected error during lookup, unable to queue object for sync", "key", key)
		return nil
	} else if !exists {
		f.logger.Info("Key does not exist in known objects store, unable to queue object for sync", "key", key)
		return nil
	}

	// 若在做 Resync() 且该对象已有事件排队，则忽略对它的 Resync。
	// 这是为了避免这样一种竞态：resync 携带的是对象的旧值（因为为该对象排队
	// 事件并不会触发底层存储 <knownObjects> 的改变）。
	//
	// 已有待处理事件的键跳过 resync：否则会把旧版本对象再次塞进队列，
	// 导致处理器先处理后到的新版本、再处理旧版本。
	id, err := f.KeyOf(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	if len(f.items[id]) > 0 {
		return nil
	}

	if err := f.queueActionLocked(Sync, obj); err != nil {
		return fmt.Errorf("couldn't queue object: %v", err)
	}
	return nil
}

// KeyListerGetter 是任何知道如何列出其 key 并按 key 查找的对象。
//
// 通常由 Indexer 实现；DeltaFIFO 用它做删除探测与 resync。
type KeyListerGetter interface {
	KeyLister
	KeyGetter
}

// KeyLister 是任何知道如何列出其 key 的对象。
type KeyLister interface {
	ListKeys() []string
}

// KeyGetter 是任何知道如何获取给定 key 下所存值的对象。
type KeyGetter interface {
	// GetByKey 返回与该 key 关联的值；不存在时 exists 为 false。
	GetByKey(key string) (value interface{}, exists bool, err error)
}

// Oldest 是便捷函数，返回最旧的 delta；若无 delta 则返回 nil。
func (d Deltas) Oldest() *Delta {
	if len(d) > 0 {
		return &d[0]
	}
	return nil
}

// Newest 是便捷函数，返回最新的 delta；若无 delta 则返回 nil。
func (d Deltas) Newest() *Delta {
	if n := len(d); n > 0 {
		return &d[n-1]
	}
	return nil
}

// copyDeltas 返回 d 的浅拷贝：即只拷贝切片，不拷贝切片中的对象。
// 这使得 Get/List 返回的对象可以确定不会被后续修改覆盖。
func copyDeltas(d Deltas) Deltas {
	d2 := make(Deltas, len(d))
	copy(d2, d)
	return d2
}

// 当对象已被删除、但与 apiserver 断连期间错过了 watch 删除事件时，
// DeletedFinalStateUnknown 会被放入 DeltaFIFO。此时我们无法知道对象最终的
// “静止”状态，因此其中包含的 `Obj` 有可能是过期的。
//
// Key 始终可信；Obj 可能为 nil 或过期，消费者应优先依赖 Key。
type DeletedFinalStateUnknown struct {
	Key string
	Obj interface{}
}
