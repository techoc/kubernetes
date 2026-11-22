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

package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgofeaturegate "k8s.io/client-go/features"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// This file implements a low-level controller that is used in
// sharedIndexInformer, which is an implementation of
// SharedIndexInformer.  Such informers, in turn, are key components
// in the high level controllers that form the backbone of the
// Kubernetes control plane.  Look at those for examples, or the
// example in
// https://github.com/kubernetes/client-go/tree/master/examples/workqueue
// .

// Config contains all the settings for one of these low-level controllers.
//
// 控制器只做两件事：驱动 Reflector 把事件写入 Queue，以及循环 Pop 出来交给
// Process/ProcessBatch。它不关心业务语义，因此 handler 的耗时直接决定队列
// 积压程度。
type Config struct {
	// The queue for your objects - has to be a DeltaFIFO due to
	// assumptions in the implementation. Your Process() function
	// should accept the output of this Queue's Pop() method.
	//
	// 必须是有序 FIFO；实现依赖它保证同一对象的同一时刻只有一次处理。
	Queue

	// Something that can list and watch your objects.
	ListerWatcher

	// Process can process a popped Deltas.
	Process ProcessFunc

	// ProcessBatch can process a batch of popped Deltas, which should return `TransactionError` if not all items
	// in the batch were successfully processed.
	//
	// For batch processing to be used:
	// * ProcessBatch must be non-nil
	// * Queue must implement QueueWithBatch
	// * The client InOrderInformersBatchProcess feature gate must be enabled
	//
	// If any of those are false, Process is used and no batch processing is done.
	//
	// 批量模式的前提还包括 InOrderInformers：事件必须已按序到达，否则
	// 批量应用 Delta 会破坏对象最终状态。
	ProcessBatch ProcessBatchFunc

	// ObjectType is an example object of the type this controller is
	// expected to handle.
	ObjectType runtime.Object

	// ObjectDescription is the description to use when logging type-specific information about this controller.
	ObjectDescription string

	// FullResyncPeriod is the period at which ShouldResync is considered.
	//
	// 只决定何时询问 ShouldResync，本身不会强制重放全部对象。
	FullResyncPeriod time.Duration

	// MinWatchTimeout, if set, will define the minimum timeout for watch requests send
	// to kube-apiserver. However, values lower than 5m will not be honored to avoid
	// negative performance impact on controlplane.
	// Optional - if unset a default value of 5m will be used.
	MinWatchTimeout time.Duration

	// ShouldResync is periodically used by the reflector to determine
	// whether to Resync the Queue. If ShouldResync is `nil` or
	// returns true, it means the reflector should proceed with the
	// resync.
	ShouldResync ShouldResyncFunc

	// Called whenever the ListAndWatch drops the connection with an error.
	//
	// Contextual logging: WatchErrorHandlerWithContext should be used instead of WatchErrorHandler in code which supports contextual logging.
	WatchErrorHandler WatchErrorHandler

	// Called whenever the ListAndWatch drops the connection with an error
	// and WatchErrorHandler is not set.
	WatchErrorHandlerWithContext WatchErrorHandlerWithContext

	// WatchListPageSize is the requested chunk size of initial and relist watch lists.
	//
	// 设为 0 表示使用服务端默认值；分块过小会增加往返次数。
	WatchListPageSize int64
}

// ShouldResyncFunc is a type of function that indicates if a reflector should perform a
// resync or not. It can be used by a shared informer to support multiple event handlers with custom
// resync periods.
//
// 共享 informer 的多个 handler 可能要求不同周期，因此返回 bool 而非固定周期。
type ShouldResyncFunc func() bool

// ProcessFunc processes a single object.
//
// obj 通常是 Deltas（同一个键的多次变更合并），isInInitialList 表示该对象是否
// 来自 informer 的首次全量列表。
type ProcessFunc func(obj interface{}, isInInitialList bool) error

// ProcessBatchFunc processes multiple objects in batch.
// The deltas must not contain multiple entries for the same object.
//
// 同一批次内同一对象只出现一次，是保证批量事务语义的前提。
type ProcessBatchFunc func(deltas []Delta, isInInitialList bool) error

// controller 是 Controller 的默认实现，持有 Reflector 供 LastSyncResourceVersion 使用。
//
// reflector 在 RunWithContext 中创建，故用读写锁保护，允许其他 goroutine
// 并发读取其同步进度。
//
// `*controller` implements Controller
type controller struct {
	config         Config
	reflector      *Reflector
	reflectorMutex sync.RWMutex
	clock          clock.Clock
}

// Controller is a low-level controller that is parameterized by a
// Config and used in sharedIndexInformer.
type Controller interface {
	// RunWithContext does two things.  One is to construct and run a Reflector
	// to pump objects/notifications from the Config's ListerWatcher
	// to the Config's Queue and possibly invoke the occasional Resync
	// on that Queue.  The other is to repeatedly Pop from the Queue
	// and process with the Config's ProcessFunc.  Both of these
	// continue until the context is canceled.
	//
	// It's an error to call RunWithContext more than once.
	// RunWithContext blocks; call via go.
	RunWithContext(ctx context.Context)

	// Run does the same as RunWithContext with a stop channel instead of
	// a context.
	//
	// Contextual logging: RunWithcontext should be used instead of Run in code which supports contextual logging.
	Run(stopCh <-chan struct{})

	// HasSynced delegates to the Config's Queue
	HasSynced() bool

	// HasSyncedChecker enables waiting for syncing without polling.
	// The returned DoneChecker can be passed to WaitFor.
	// It delegates to the Config's Queue.
	HasSyncedChecker() DoneChecker

	// LastSyncResourceVersion delegates to the Reflector when there
	// is one, otherwise returns the empty string
	//
	// Reflector 尚未创建（Run 之前）时返回空串，调用方不能把它当作有效的
	// resourceVersion 使用。
	LastSyncResourceVersion() string
}

// New makes a new Controller from the given Config.
func New(c *Config) Controller {
	// 复制 Config 值，避免调用方在 Run 之前修改配置导致数据竞争。
	ctlr := &controller{
		config: *c,
		clock:  &clock.RealClock{},
	}
	return ctlr
}

// Run implements [Controller.Run].
func (c *controller) Run(stopCh <-chan struct{}) {
	c.RunWithContext(wait.ContextForChannel(stopCh))
}

// RunWithContext implements [Controller.RunWithContext].
func (c *controller) RunWithContext(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)
	// 上下文结束时关闭 Queue，让阻塞在 Pop 上的消费者退出；
	// 关闭后 Pop 返回 ErrFIFOClosed，processLoop 据此结束。
	go func() {
		<-ctx.Done()
		c.config.Queue.Close()
	}()
	logger := klog.FromContext(ctx)
	r := NewReflectorWithOptions(
		c.config.ListerWatcher,
		c.config.ObjectType,
		c.config.Queue,
		ReflectorOptions{
			Logger:          &logger,
			ResyncPeriod:    c.config.FullResyncPeriod,
			MinWatchTimeout: c.config.MinWatchTimeout,
			TypeDescription: c.config.ObjectDescription,
			Clock:           c.clock,
		},
	)
	r.ShouldResync = c.config.ShouldResync
	r.WatchListPageSize = c.config.WatchListPageSize
	// 优先使用旧的无上下文 handler；两者都设置时新 handler 被忽略，
	// 这是为保持既有配置行为不变。
	if c.config.WatchErrorHandler != nil {
		r.watchErrorHandler = func(_ context.Context, r *Reflector, err error) {
			c.config.WatchErrorHandler(r, err)
		}
	} else if c.config.WatchErrorHandlerWithContext != nil {
		r.watchErrorHandler = c.config.WatchErrorHandlerWithContext
	}

	// 先发布 reflector 再启动，保证 LastSyncResourceVersion 能观察到它。
	c.reflectorMutex.Lock()
	c.reflector = r
	c.reflectorMutex.Unlock()

	var wg wait.Group

	wg.StartWithContext(ctx, r.RunWithContext)

	// processLoop 内部自行处理上下文取消；外层 Until 以 1 秒间隔重入，
	// 用于在异常返回后继续消费队列。
	wait.UntilWithContext(ctx, c.processLoop, time.Second)
	wg.Wait()
}

// Returns true once this controller has completed an initial resource listing
func (c *controller) HasSynced() bool {
	return c.config.Queue.HasSynced()
}

// HasSyncedChecker enables waiting for syncing without polling.
// The returned DoneChecker can be passed to [WaitFor].
// It delegates to the Config's Queue.
func (c *controller) HasSyncedChecker() DoneChecker {
	return c.config.Queue.HasSyncedChecker()
}

// LastSyncResourceVersion 返回 Reflector 最近一次成功同步的 resourceVersion。
func (c *controller) LastSyncResourceVersion() string {
	c.reflectorMutex.RLock()
	defer c.reflectorMutex.RUnlock()
	if c.reflector == nil {
		return ""
	}
	return c.reflector.LastSyncResourceVersion()
}

// processLoop drains the work queue.
// TODO: Consider doing the processing in parallel. This will require a little thought
// to make sure that we don't end up processing the same object multiple times
// concurrently.
func (c *controller) processLoop(ctx context.Context) {
	// 批量能力在进入循环前判定一次，避免每次 Pop 都做接口断言和门控查询；
	// 三个条件缺一不可，否则退回逐条处理。
	useBatchProcess := false
	batchQueue, ok := c.config.Queue.(QueueWithBatch)
	if ok && c.config.ProcessBatch != nil && clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.InOrderInformersBatchProcess) {
		useBatchProcess = true
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var err error
			if useBatchProcess {
				// PopBatch 在内部循环取队首连续批次，并保证同对象顺序。
				err = batchQueue.PopBatch(c.config.ProcessBatch, PopProcessFunc(c.config.Process))
			} else {
				// otherwise fallback to non-batch process behavior
				_, err = c.config.Pop(PopProcessFunc(c.config.Process))
			}
			if err != nil {
				// 队列关闭是正常退出路径，其他错误交给上层 Until 重试。
				if errors.Is(err, ErrFIFOClosed) {
					return
				}
			}
		}
	}
}

// ResourceEventHandler can handle notifications for events that
// happen to a resource. The events are informational only, so you
// can't return an error.  The handlers MUST NOT modify the objects
// received; this concerns not only the top level of structure but all
// the data structures reachable from it.
//   - OnAdd is called when an object is added.
//   - OnUpdate is called when an object is modified. Note that oldObj is the
//     last known state of the object-- it is possible that several changes
//     were combined together, so you can't use this to see every single
//     change. OnUpdate is also called when a re-list happens, and it will
//     get called even if nothing changed. This is useful for periodically
//     evaluating or syncing something.
//   - OnDelete will get the final state of the item if it is known, otherwise
//     it will get an object of type DeletedFinalStateUnknown. This can
//     happen if the watch is closed and misses the delete event and we don't
//     notice the deletion until the subsequent re-list.
//
// handler 在 informer 自己的协程中串行调用；任何阻塞都会拖慢整个缓存同步，
// 耗时操作应转交工作队列。isInInitialList 仅对 OnAdd 有意义。
type ResourceEventHandler interface {
	OnAdd(obj interface{}, isInInitialList bool)
	OnUpdate(oldObj, newObj interface{})
	OnDelete(obj interface{})
}

// Object is the subset of metav1.Object that is needed to determine the
// name and, if applicable, namespace of an object. All standard Kubernetes
// API types satisfy this interface through their embedded ObjectMeta.
//
// comparable is required so that a zero T can be compared against
// OptionalObj to detect a missing object (e.g. a nil pointer).
type Object interface {
	comparable

	GetName() string
	GetNamespace() string
}

// TypedResourceEventHandler is a type-safe variant of ResourceEventHandler.
//
// T 必须是可比较的具名对象指针（见 Object），这样编译器能阻止把错误类型
// 传入 handler，同时保留与未类型化版本相同的事件顺序语义。
type TypedResourceEventHandler[T Object] interface {
	OnAdd(obj T, isInInitialList bool)
	OnUpdate(oldObj, newObj T)
	OnDelete(obj DeletedObject[T])
}

// DeletedObject provides information about a deleted object.
//
// It implements GetObjectName and GetKey, which replace
// [DeletionHandlingObjectToName] and [DeletionHandlingMetaNamespaceKeyFunc]
//
// It implements the GetName and GetNamespace methods for use
// with klog.KObj: an OnDelete event handler can log
// it's parameter directly and is guaranteed to produce the same
// log output as with klog.KObj applied to the actual object.
//
// All of these methods work regardless whether DeletedObject actually has
// the deleted object.
type DeletedObject[T Object] struct {
	// OptionalObj is the deleted object.
	//
	// It can be:
	// - stale: it is some *older* revision, not the one which got deleted
	// - nil: the informer noticed a deletion without finding *any* copy of the deleted object
	//
	// The only guaranteed information is the key under which the deleted
	// object was stored in the informer cache. Use [DeletedObject.GetObjectName]
	// and [DeletedObject.GetKey] to extract the namespace and name resp.
	// the key under which the deleted object was stored. They work in
	// all cases.
	OptionalObj T

	// FinalStateUnknown is nil if the deleted object is available and up-to-date.
	// Otherwise FinalStateUnknown is non-nil to provide the key.
	// The untyped object inside it matches OptionalObj.
	FinalStateUnknown *DeletedFinalStateUnknown
}

// GetObjectName returns the object name (= namespace if available and name)
// for the deleted object. Note that this is slightly different from
// GetName which returns just that name without the namespace.
//
// This is a type-safe variant of [DeletionHandlingObjectToName] which
// cannot fail because T is guaranteed to have a name and namespace and
// the key of FinalStateUnknown, if invalid, is simply treated as a
// name without a namespace.
//
// 优先信任 FinalStateUnknown.Key：它来自缓存索引，即使对象内容已过期也
// 仍能唯一标识被删除的对象。
func (d DeletedObject[T]) GetObjectName() ObjectName {
	if d.FinalStateUnknown != nil {
		// Same splitting as SplitMetaNamespaceKey and ParseObjectName,
		// except that a key with more than one "/" is not treated as an error:
		// the extra "/" become part of the name.
		namespace, name, found := strings.Cut(d.FinalStateUnknown.Key, "/")
		if !found {
			return ObjectName{Name: namespace}
		}
		return ObjectName{Namespace: namespace, Name: name}
	}
	var null T
	if d.OptionalObj == null {
		// Avoid calling GetName/GetNamespace on a zero OptionalObj (e.g. a
		// nil pointer), which could panic. A nil pointer boxed behind a
		// further layer of indirection is not detected here and is not
		// handled.
		return ObjectName{}
	}
	// Same logic as MetaObjectToName, adapted to use T's own
	// GetNamespace and GetName methods instead of going through
	// meta.Accessor as ObjectToName does.
	if namespace := d.OptionalObj.GetNamespace(); len(namespace) > 0 {
		return ObjectName{Namespace: namespace, Name: d.OptionalObj.GetName()}
	}
	return ObjectName{Name: d.OptionalObj.GetName()}
}

// GetName returns the name without the namespace. Note that this is slightly
// different from GetObjectName which returns the combination of namespace and name in a
// struct.
func (d DeletedObject[T]) GetName() string {
	return d.GetObjectName().Name
}

// GetNamespace returns the namespace if there is one, otherwise the empty string.
func (d DeletedObject[T]) GetNamespace() string {
	return d.GetObjectName().Namespace
}

// GetKey returns the key under which the deleted object was stored.
//
// This is a type-safe variant of [DeletionHandlingMetaNamespaceKeyFunc]
// which cannot fail because T is guaranteed to have a name and namespace.
//
// 编码规则与 MetaNamespaceKeyFunc 一致：有命名空间时为 namespace/name，
// 否则仅为 name。
func (d DeletedObject[T]) GetKey() string {
	if d.FinalStateUnknown != nil {
		return d.FinalStateUnknown.Key
	}
	var null T
	if d.OptionalObj == null {
		return ""
	}
	// Same encoding as MetaNamespaceKeyFunc via ObjectName.String,
	// adapted to use T's own GetNamespace and GetName methods.
	if namespace := d.OptionalObj.GetNamespace(); len(namespace) > 0 {
		return namespace + "/" + d.OptionalObj.GetName()
	}
	return d.OptionalObj.GetName()
}

// ResourceEventHandlerFuncs is an adaptor to let you easily specify as many or
// as few of the notification functions as you want while still implementing
// ResourceEventHandler.  This adapter does not remove the prohibition against
// modifying the objects.
//
// See ResourceEventHandlerDetailedFuncs if your use needs to propagate
// HasSynced.
type ResourceEventHandlerFuncs struct {
	AddFunc    func(obj interface{})
	UpdateFunc func(oldObj, newObj interface{})
	DeleteFunc func(obj interface{})
}

// OnAdd calls AddFunc if it's not nil.
func (r ResourceEventHandlerFuncs) OnAdd(obj interface{}, isInInitialList bool) {
	if r.AddFunc != nil {
		r.AddFunc(obj)
	}
}

// OnUpdate calls UpdateFunc if it's not nil.
func (r ResourceEventHandlerFuncs) OnUpdate(oldObj, newObj interface{}) {
	if r.UpdateFunc != nil {
		r.UpdateFunc(oldObj, newObj)
	}
}

// OnDelete calls DeleteFunc if it's not nil.
func (r ResourceEventHandlerFuncs) OnDelete(obj interface{}) {
	if r.DeleteFunc != nil {
		r.DeleteFunc(obj)
	}
}

// TypedResourceEventHandlerFuncs 是 TypedResourceEventHandler 的函数适配器。
//
// 与未类型化版本相同：未设置的函数会被跳过，因此可以只关心部分事件。
type TypedResourceEventHandlerFuncs[T Object] struct {
	AddFunc    func(obj T)
	UpdateFunc func(oldObj, newObj T)
	DeleteFunc func(obj DeletedObject[T])
}

var _ TypedResourceEventHandler[*metav1.ObjectMeta] = TypedResourceEventHandlerFuncs[*metav1.ObjectMeta]{}

func (r TypedResourceEventHandlerFuncs[T]) OnAdd(obj T, isInInitialList bool) {
	if r.AddFunc != nil {
		r.AddFunc(obj)
	}
}

func (r TypedResourceEventHandlerFuncs[T]) OnUpdate(oldObj, newObj T) {
	if r.UpdateFunc != nil {
		r.UpdateFunc(oldObj, newObj)
	}
}

func (r TypedResourceEventHandlerFuncs[T]) OnDelete(obj DeletedObject[T]) {
	if r.DeleteFunc != nil {
		r.DeleteFunc(obj)
	}
}

// ResourceEventHandlerDetailedFuncs is exactly like ResourceEventHandlerFuncs
// except its AddFunc accepts the isInInitialList parameter, for propagating
// HasSynced.
type ResourceEventHandlerDetailedFuncs struct {
	AddFunc    func(obj interface{}, isInInitialList bool)
	UpdateFunc func(oldObj, newObj interface{})
	DeleteFunc func(obj interface{})
}

// OnAdd calls AddFunc if it's not nil.
func (r ResourceEventHandlerDetailedFuncs) OnAdd(obj interface{}, isInInitialList bool) {
	if r.AddFunc != nil {
		r.AddFunc(obj, isInInitialList)
	}
}

// OnUpdate calls UpdateFunc if it's not nil.
func (r ResourceEventHandlerDetailedFuncs) OnUpdate(oldObj, newObj interface{}) {
	if r.UpdateFunc != nil {
		r.UpdateFunc(oldObj, newObj)
	}
}

// OnDelete calls DeleteFunc if it's not nil.
func (r ResourceEventHandlerDetailedFuncs) OnDelete(obj interface{}) {
	if r.DeleteFunc != nil {
		r.DeleteFunc(obj)
	}
}

// TypedResourceEventHandlerDetailedFuncs 是 TypedResourceEventHandler 的函数适配器，
// 且 AddFunc 带 isInInitialList，用于向上游传播 HasSynced。
type TypedResourceEventHandlerDetailedFuncs[T Object] struct {
	AddFunc    func(obj T, isInInitialList bool)
	UpdateFunc func(oldObj, newObj T)
	DeleteFunc func(obj DeletedObject[T])
}

var _ TypedResourceEventHandler[*metav1.ObjectMeta] = TypedResourceEventHandlerDetailedFuncs[*metav1.ObjectMeta]{}

func (r TypedResourceEventHandlerDetailedFuncs[T]) OnAdd(obj T, isInInitialList bool) {
	if r.AddFunc != nil {
		r.AddFunc(obj, isInInitialList)
	}
}

func (r TypedResourceEventHandlerDetailedFuncs[T]) OnUpdate(oldObj, newObj T) {
	if r.UpdateFunc != nil {
		r.UpdateFunc(oldObj, newObj)
	}
}

func (r TypedResourceEventHandlerDetailedFuncs[T]) OnDelete(obj DeletedObject[T]) {
	if r.DeleteFunc != nil {
		r.DeleteFunc(obj)
	}
}

// FilteringResourceEventHandler applies the provided filter to all events coming
// in, ensuring the appropriate nested handler method is invoked. An object
// that starts passing the filter after an update is considered an add, and an
// object that stops passing the filter after an update is considered a delete.
// Like the handlers, the filter MUST NOT modify the objects it is given.
//
// 这种“过滤状态迁移”设计让下游 handler 只需处理自己关心的对象，
// 不必自行维护成员集合。
type FilteringResourceEventHandler struct {
	FilterFunc func(obj interface{}) bool
	Handler    ResourceEventHandler
}

// OnAdd calls the nested handler only if the filter succeeds
func (r FilteringResourceEventHandler) OnAdd(obj interface{}, isInInitialList bool) {
	if !r.FilterFunc(obj) {
		return
	}
	r.Handler.OnAdd(obj, isInInitialList)
}

// OnUpdate ensures the proper handler is called depending on whether the filter matches
func (r FilteringResourceEventHandler) OnUpdate(oldObj, newObj interface{}) {
	// 分别计算新旧对象的过滤结果，以识别进入/离开过滤集合的边界。
	newer := r.FilterFunc(newObj)
	older := r.FilterFunc(oldObj)
	switch {
	case newer && older:
		r.Handler.OnUpdate(oldObj, newObj)
	case newer && !older:
		r.Handler.OnAdd(newObj, false)
	case !newer && older:
		r.Handler.OnDelete(oldObj)
	default:
		// do nothing
	}
}

// OnDelete calls the nested handler only if the filter succeeds
func (r FilteringResourceEventHandler) OnDelete(obj interface{}) {
	if !r.FilterFunc(obj) {
		return
	}
	r.Handler.OnDelete(obj)
}

// TypedFilteringResourceEventHandler is a type-safe variant of
// [FilteringResourceEventHandler].
type TypedFilteringResourceEventHandler[T Object] struct {
	FilterFunc func(obj T) bool
	Handler    TypedResourceEventHandler[T]
}

// OnAdd calls the nested handler only if the filter succeeds
func (r TypedFilteringResourceEventHandler[T]) OnAdd(obj T, isInInitialList bool) {
	if !r.FilterFunc(obj) {
		return
	}
	r.Handler.OnAdd(obj, isInInitialList)
}

// OnUpdate ensures the proper handler is called depending on whether the filter matches
func (r TypedFilteringResourceEventHandler[T]) OnUpdate(oldObj, newObj T) {
	// 与未类型化版本一致，用新旧对象各自是否通过过滤器来决定事件类型。
	newer := r.FilterFunc(newObj)
	older := r.FilterFunc(oldObj)
	switch {
	case newer && older:
		r.Handler.OnUpdate(oldObj, newObj)
	case newer && !older:
		r.Handler.OnAdd(newObj, false)
	case !newer && older:
		r.Handler.OnDelete(DeletedObject[T]{OptionalObj: oldObj})
	default:
		// do nothing
	}
}

// OnDelete calls the nested handler only if the filter succeeds. The filter
// is applied to [DeletedObject.OptionalObj], which may be the zero value of T
// if the deleted object could not be recovered; a FilterFunc that relies on T
// having a valid state must handle that case itself.
func (r TypedFilteringResourceEventHandler[T]) OnDelete(obj DeletedObject[T]) {
	if !r.FilterFunc(obj.OptionalObj) {
		return
	}
	r.Handler.OnDelete(obj)
}

// DeletionHandlingMetaNamespaceKeyFunc checks for
// DeletedFinalStateUnknown objects before calling
// MetaNamespaceKeyFunc.
//
// DeletedFinalStateUnknown 携带的 Key 已经是计算好的索引键，直接复用可避免
// 对可能已失效的对象再次取元数据。
func DeletionHandlingMetaNamespaceKeyFunc(obj interface{}) (string, error) {
	if d, ok := obj.(DeletedFinalStateUnknown); ok {
		return d.Key, nil
	}
	return MetaNamespaceKeyFunc(obj)
}

// DeletionHandlingObjectToName checks for
// DeletedFinalStateUnknown objects before calling
// ObjectToName.
func DeletionHandlingObjectToName(obj interface{}) (ObjectName, error) {
	if d, ok := obj.(DeletedFinalStateUnknown); ok {
		return ParseObjectName(d.Key)
	}
	return ObjectToName(obj)
}

// InformerOptions configure a Reflector.
//
// 这些选项最终被翻译成 Queue/Reflector 的配置；字符串化的 Identifier 用于
// 指标标签，因此生产代码应尽量设置。
type InformerOptions struct {
	// Logger, if not nil, is used instead of klog.Background() for logging.
	Logger *klog.Logger

	// ListerWatcher implements List and Watch functions for the source of the resource
	// the informer will be informing about.
	ListerWatcher ListerWatcher

	// ObjectType is an object of the type that informer is expected to receive.
	ObjectType runtime.Object

	// Handler defines functions that should called on object mutations.
	Handler ResourceEventHandler

	// ResyncPeriod is the underlying Reflector's resync period. If non-zero, the store
	// is re-synced with that frequency - Modify events are delivered even if objects
	// didn't change.
	// This is useful for synchronizing objects that configure external resources
	// (e.g. configure cloud provider functionalities).
	// Optional - if unset, store resyncing is not happening periodically.
	//
	// 注意这是“本地重放”而非重新 List：事件中的对象来自缓存，resourceVersion
	// 未必变化，handler 必须幂等。
	ResyncPeriod time.Duration

	// MinWatchTimeout, if set, will define the minimum timeout for watch requests send
	// to kube-apiserver. However, values lower than 5m will not be honored to avoid
	// negative performance impact on controlplane.
	// Optional - if unset a default value of 5m will be used.
	MinWatchTimeout time.Duration

	// Indexers, if set, are the indexers for the received objects to optimize
	// certain queries.
	// Optional - if unset no indexes are maintained.
	Indexers Indexers

	// Transform function, if set, will be called on all objects before they will be
	// put into the Store and corresponding Add/Modify/Delete handlers will be invoked
	// for them.
	// Optional - if unset no additional transforming is happening.
	Transform TransformFunc

	// Identifier is used to identify the FIFO for metrics and logging purposes.
	// If not set, metrics will not be published.
	Identifier InformerNameAndResource

	// InformerMetricsProvider is the metrics provider for the informer.
	// If not set, metrics will be no-ops.
	InformerMetricsProvider InformerMetricsProvider
}

// NewInformerWithOptions returns a Store and a controller for populating the store
// while also providing event notifications. You should only used the returned
// Store for Get/List operations; Add/Modify/Deletes will cause the event
// notifications to be faulty.
//
// 返回的 Store 与 controller 共享同一个底层缓存；若调用方直接写入 Store，
// 会绕过 Queue 的事件合并逻辑，导致 handler 收到不一致的通知。
func NewInformerWithOptions(options InformerOptions) (Store, Controller) {
	var clientState Store
	// 只有需要按字段/标签查询时才构建 Indexer，普通 Store 更轻量。
	if options.Indexers == nil {
		clientState = NewStore(DeletionHandlingMetaNamespaceKeyFunc, WithStoreMetrics(options.Identifier, options.InformerMetricsProvider))
	} else {
		clientState = NewIndexer(DeletionHandlingMetaNamespaceKeyFunc, options.Indexers, WithStoreMetrics(options.Identifier, options.InformerMetricsProvider))
	}
	return clientState, newInformer(clientState, options, DeletionHandlingMetaNamespaceKeyFunc)
}

// NewInformer returns a Store and a controller for populating the store
// while also providing event notifications. You should only used the returned
// Store for Get/List operations; Add/Modify/Deletes will cause the event
// notifications to be faulty.
//
// Parameters:
//   - lw is list and watch functions for the source of the resource you want to
//     be informed of.
//   - objType is an object of the type that you expect to receive.
//   - resyncPeriod: if non-zero, will re-list this often (you will get OnUpdate
//     calls, even if nothing changed). Otherwise, re-list will be delayed as
//     long as possible (until the upstream source closes the watch or times out,
//     or you stop the controller).
//   - h is the object you want notifications sent to.
//
// Deprecated: Use NewInformerWithOptions instead.
func NewInformer(
	lw ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
) (Store, Controller) {
	// This will hold the client state, as we know it.
	clientState := NewStore(DeletionHandlingMetaNamespaceKeyFunc)

	options := InformerOptions{
		ListerWatcher: lw,
		ObjectType:    objType,
		Handler:       h,
		ResyncPeriod:  resyncPeriod,
	}
	return clientState, newInformer(clientState, options, DeletionHandlingMetaNamespaceKeyFunc)
}

// NewIndexerInformer returns an Indexer and a Controller for populating the index
// while also providing event notifications. You should only used the returned
// Index for Get/List operations; Add/Modify/Deletes will cause the event
// notifications to be faulty.
//
// Parameters:
//   - lw is list and watch functions for the source of the resource you want to
//     be informed of.
//   - objType is an object of the type that you expect to receive.
//   - resyncPeriod: if non-zero, will re-list this often (you will get OnUpdate
//     calls, even if nothing changed). Otherwise, re-list will be delayed as
//     long as possible (until the upstream source closes the watch or times out,
//     or you stop the controller).
//   - h is the object you want notifications sent to.
//   - indexers is the indexer for the received object type.
//
// Deprecated: Use NewInformerWithOptions instead.
func NewIndexerInformer(
	lw ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
	indexers Indexers,
) (Indexer, Controller) {
	// This will hold the client state, as we know it.
	clientState := NewIndexer(DeletionHandlingMetaNamespaceKeyFunc, indexers)

	options := InformerOptions{
		ListerWatcher: lw,
		ObjectType:    objType,
		Handler:       h,
		ResyncPeriod:  resyncPeriod,
		Indexers:      indexers,
	}
	return clientState, newInformer(clientState, options, DeletionHandlingMetaNamespaceKeyFunc)
}

// NewTransformingInformer returns a Store and a controller for populating
// the store while also providing event notifications. You should only used
// the returned Store for Get/List operations; Add/Modify/Deletes will cause
// the event notifications to be faulty.
// The given transform function will be called on all objects before they will
// put into the Store and corresponding Add/Modify/Delete handlers will
// be invoked for them.
//
// Deprecated: Use NewInformerWithOptions instead.
func NewTransformingInformer(
	lw ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
	transformer TransformFunc,
) (Store, Controller) {
	// This will hold the client state, as we know it.
	clientState := NewStore(DeletionHandlingMetaNamespaceKeyFunc)

	options := InformerOptions{
		ListerWatcher: lw,
		ObjectType:    objType,
		Handler:       h,
		ResyncPeriod:  resyncPeriod,
		Transform:     transformer,
	}
	return clientState, newInformer(clientState, options, DeletionHandlingMetaNamespaceKeyFunc)
}

// NewTransformingIndexerInformer returns an Indexer and a controller for
// populating the index while also providing event notifications. You should
// only used the returned Index for Get/List operations; Add/Modify/Deletes
// will cause the event notifications to be faulty.
// The given transform function will be called on all objects before they will
// be put into the Index and corresponding Add/Modify/Delete handlers will
// be invoked for them.
//
// Deprecated: Use NewInformerWithOptions instead.
func NewTransformingIndexerInformer(
	lw ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
	indexers Indexers,
	transformer TransformFunc,
) (Indexer, Controller) {
	// This will hold the client state, as we know it.
	clientState := NewIndexer(DeletionHandlingMetaNamespaceKeyFunc, indexers)

	options := InformerOptions{
		ListerWatcher: lw,
		ObjectType:    objType,
		Handler:       h,
		ResyncPeriod:  resyncPeriod,
		Indexers:      indexers,
		Transform:     transformer,
	}
	return clientState, newInformer(clientState, options, DeletionHandlingMetaNamespaceKeyFunc)
}

// Multiplexes updates in the form of a list of Deltas into a Store, and informs
// a given handler of events OnUpdate, OnAdd, OnDelete
//
// Delta 中的对象可能来自本地重放，因此“更新”不保证内容变化；handler 需
// 自行按 resourceVersion 或语义去重。
func processDeltas(
	logger klog.Logger,
	// Object which receives event notifications from the given deltas
	handler ResourceEventHandler,
	clientState Store,
	deltas Deltas,
	isInInitialList bool,
	keyFunc KeyFunc,
) error {
	// from oldest to newest
	for _, d := range deltas {
		obj := d.Object

		switch d.Type {
		case ReplacedAll:
			// 原子替换：所有变更被封装在一个事件中，先整体落盘再回调，
			// 避免 handler 看到中间状态。
			info, ok := obj.(ReplacedAllInfo)
			if !ok {
				return fmt.Errorf("ReplacedAll did not contain ReplacedAllInfo: %T", obj)
			}
			if err := processReplacedAllInfo(logger, handler, info, clientState, isInInitialList, keyFunc); err != nil {
				return err
			}
		case SyncAll:
			// SyncAll 不改变 Store，只是把所有现存对象当作一次更新重放。
			_, ok := obj.(SyncAllInfo)
			if !ok {
				return fmt.Errorf("SyncAll did not contain SyncAllInfo: %T", obj)
			}
			objs := clientState.List()
			for _, obj := range objs {
				handler.OnUpdate(obj, obj)
			}
			return nil
		case Sync, Replaced, Added, Updated:
			// 以 Store 中是否已有该键来区分新增与更新：首次 List 得到的所有
			// 对象都会走 OnAdd，并携带 isInInitialList。
			if old, exists, err := clientState.Get(obj); err == nil && exists {
				if err := clientState.Update(obj); err != nil {
					return err
				}
				handler.OnUpdate(old, obj)
			} else {
				if err := clientState.Add(obj); err != nil {
					return err
				}
				handler.OnAdd(obj, isInInitialList)
			}
		case Deleted:
			// 先删缓存再通知，保证 OnDelete 中查缓存看不到已删除对象。
			if err := clientState.Delete(obj); err != nil {
				return err
			}
			handler.OnDelete(obj)
		case Bookmark:
			// Bookmark 只推进 resourceVersion，不产生用户可见事件。
			info, ok := obj.(BookmarkInfo)
			if !ok {
				return fmt.Errorf("bookmark delta did not contain BookmarkInfo: %T", obj)
			}
			clientState.Bookmark(info.ResourceVersion)
		}
	}
	return nil
}

// processDeltasInBatch applies a batch of Delta objects to the given Store and
// notifies the ResourceEventHandler of add, update, or delete events.
//
// If the Store supports transactions (TransactionStore), all Deltas are applied
// atomically in a single transaction and corresponding handler callbacks are
// executed afterward. Otherwise, each Delta is processed individually.
//
// Returns an error if any Delta or transaction fails. For TransactionError,
// only successful operations trigger callbacks.
func processDeltasInBatch(
	logger klog.Logger,
	handler ResourceEventHandler,
	clientState Store,
	deltas []Delta,
	isInInitialList bool,
	keyFunc KeyFunc,
) error {
	// from oldest to newest
	txns := make([]Transaction, 0)
	callbacks := make([]func(), 0)
	txnStore, txnSupported := clientState.(TransactionStore)
	if !txnSupported {
		// Store 不支持事务时退化为逐条处理，牺牲原子性但保持功能可用；
		// 错误聚合后一次性返回，避免静默丢事件。
		var errs []error
		for _, delta := range deltas {
			if err := processDeltas(logger, handler, clientState, Deltas{delta}, isInInitialList, keyFunc); err != nil {
				errs = append(errs, err)
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("unexpected error when handling deltas: %v", errs)
		}
		return nil
	}
	// deltasList is a list of unique objects
	for _, d := range deltas {
		obj := d.Object
		switch d.Type {
		case Sync, Replaced, Added, Updated:
			// 事务内查询 Store 仍能看到提交前状态，因此 old 是该批次的“前像”；
			// 回调延迟到事务提交后执行，避免 handler 看到未提交数据。
			// it will only return one old object for each because items are unique
			if old, exists, err := clientState.Get(obj); err == nil && exists {
				txn := Transaction{
					Type:   TransactionTypeUpdate,
					Object: obj,
				}
				txns = append(txns, txn)
				callbacks = append(callbacks, func() {
					handler.OnUpdate(old, obj)
				})
			} else {
				txn := Transaction{
					Type:   TransactionTypeAdd,
					Object: obj,
				}
				txns = append(txns, txn)
				callbacks = append(callbacks, func() {
					handler.OnAdd(obj, isInInitialList)
				})
			}
		case Deleted:
			txn := Transaction{
				Type:   TransactionTypeDelete,
				Object: obj,
			}
			txns = append(txns, txn)
			callbacks = append(callbacks, func() {
				handler.OnDelete(obj)
			})
		default:
			return fmt.Errorf("Delta type %s is not supported in batch processing", d.Type)
		}
	}

	err := txnStore.Transaction(txns...)
	if err != nil {
		// 部分成功时只对已提交的条目触发回调，保证回调与缓存状态一致。
		// if txn had error, only execute the callbacks for the successful ones
		for _, i := range err.SuccessfulIndices {
			if i < len(callbacks) {
				callbacks[i]()
			}
		}
		// formatting the error so txns doesn't escape and keeps allocated in the stack.
		return fmt.Errorf("not all items in the batch successfully processed: %s", err.Error())
	}
	for _, callback := range callbacks {
		callback()
	}
	return nil
}

// processReplacedAllInfo 处理一次 ReplacedAll：先比对旧集合算出删除与新增/更新，
// 再整体替换 Store，最后按“先删除、后增改”的顺序回调。
//
// 这个顺序与 RealFIFO#Replace 保持一致，下游 handler 可以依赖相同语义。
func processReplacedAllInfo(logger klog.Logger, handler ResourceEventHandler, info ReplacedAllInfo, clientState Store, isInInitialList bool, keyFunc KeyFunc) error {
	var deletions []DeletedFinalStateUnknown
	type replacement struct {
		oldObj interface{}
		newObj interface{}
	}
	replacements := make([]replacement, 0, len(info.Objects))

	err := reconcileReplacement(logger, nil, clientState, info.Objects, keyFunc,
		func(obj DeletedFinalStateUnknown) error {
			deletions = append(deletions, obj)
			return nil
		},
		func(obj interface{}) error {
			// This behavior matches processDeltas handling of Replace deltas
			if old, exists, err := clientState.Get(obj); err == nil && exists {
				replacements = append(replacements, replacement{newObj: obj, oldObj: old})
			} else {
				replacements = append(replacements, replacement{newObj: obj})
			}
			return nil
		},
	)
	if err != nil {
		return err
	}

	// Replace the client state first so the store reflects the events handlers are given
	if err := clientState.Replace(info.Objects, info.ResourceVersion); err != nil {
		return err
	}
	// Processing all deletions first matches behavior of RealFIFO#Replace
	for _, objToDelete := range deletions {
		handler.OnDelete(objToDelete)
	}
	// Processing adds/updates in order observed by reconcileReplacement matches behavior of RealFIFO#Replace
	for _, r := range replacements {
		if r.oldObj != nil {
			handler.OnUpdate(r.oldObj, r.newObj)
		} else {
			handler.OnAdd(r.newObj, isInInitialList)
		}
	}
	return nil
}

// newInformer returns a controller for populating the store while also
// providing event notifications.
//
// Parameters
//   - clientState is the store you want to populate
//   - options contain the options to configure the controller
func newInformer(clientState Store, options InformerOptions, keyFunc KeyFunc) Controller {
	// This will hold incoming changes. Note how we pass clientState in as a
	// KeyLister, that way resync operations will result in the correct set
	// of update/delete deltas.

	logger := klog.Background()
	if options.Logger != nil {
		logger = *options.Logger
	}
	// FIFO 会为日志附加自身名称，因此这里接收返回的 logger 供 Process 使用。
	logger, fifo := newQueueFIFO(logger, options.ObjectType, clientState, options.Transform, options.Identifier, options.InformerMetricsProvider)

	cfg := &Config{
		Queue:            fifo,
		ListerWatcher:    options.ListerWatcher,
		ObjectType:       options.ObjectType,
		FullResyncPeriod: options.ResyncPeriod,
		MinWatchTimeout:  options.MinWatchTimeout,

		Process: func(obj interface{}, isInInitialList bool) error {
			if deltas, ok := obj.(Deltas); ok {
				// This must be the logger *of the fifo*.
				return processDeltas(logger, options.Handler, clientState, deltas, isInInitialList, keyFunc)
			}
			return errors.New("object given as Process argument is not Deltas")
		},
		ProcessBatch: func(deltaList []Delta, isInInitialList bool) error {
			// Same here.
			return processDeltasInBatch(logger, options.Handler, clientState, deltaList, isInInitialList, keyFunc)
		},
	}
	return New(cfg)
}

// newQueueFIFO constructs a new FIFO, choosing between real and delta FIFO
// depending on the InOrderInformers feature gate.
//
// It returns the FIFO and the logger used by the FIFO.
// That logger includes the name used for the FIFO,
// in contrast to the logger which was passed in.
//
// 新实现（RealFIFO）按序交付事件；旧 DeltaFIFO 为兼容保留，关闭门控时仍会
// 使用，因此两条路径必须保持可观察行为一致。
func newQueueFIFO(logger klog.Logger, objectType any, clientState Store, transform TransformFunc, identifier InformerNameAndResource, metricsProvider InformerMetricsProvider) (klog.Logger, Queue) {
	if clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.InOrderInformers) {
		options := RealFIFOOptions{
			Logger:          &logger,
			Name:            fmt.Sprintf("RealFIFO %T", objectType),
			KeyFunction:     MetaNamespaceKeyFunc,
			Transformer:     transform,
			Identifier:      identifier,
			MetricsProvider: metricsProvider,
		}
		// If atomic events are enabled, unset clientState in the case of atomic events as we cannot pass a
		// store to an atomic fifo.
		if clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.AtomicFIFO) {
			options.AtomicEvents = true
			options.UnlockWhileProcessing = clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.UnlockWhileProcessingFIFO)
			options.EmitDeltaTypeBookmark = true
		} else {
			// 非原子模式下 FIFO 需要 KnownObjects 来计算 Resync/Replace 的增量。
			options.KnownObjects = clientState
		}
		f := NewRealFIFOWithOptions(options)
		return f.logger, f
	} else {
		f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
			Logger:                &logger,
			Name:                  fmt.Sprintf("DeltaFIFO %T", objectType),
			KnownObjects:          clientState,
			EmitDeltaTypeReplaced: true,
			Transformer:           transform,
		})
		return f.logger, f
	}
}
