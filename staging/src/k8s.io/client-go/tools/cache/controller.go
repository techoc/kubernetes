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

// 本文件实现一个低层控制器，供 sharedIndexInformer（SharedIndexInformer 的
// 实现）使用。而这类 informer 又是构成 Kubernetes 控制平面主干的高层控制器
// 的关键组件。可参考那些控制器中的示例，或
// https://github.com/kubernetes/client-go/tree/master/examples/workqueue
// 中的示例。

// Config 包含这类低层控制器的全部设置。
//
// 控制器只做两件事：驱动 Reflector 把事件写入 Queue，以及循环 Pop 出来交给
// Process/ProcessBatch。它不关心业务语义，因此 handler 的耗时直接决定队列
// 积压程度。
type Config struct {
	// 存放你的对象的队列——由于实现中的假设，必须是 DeltaFIFO。
	// 你的 Process() 函数应接受该 Queue 的 Pop() 方法的输出。
	//
	// 必须是有序 FIFO；实现依赖它保证同一对象的同一时刻只有一次处理。
	Queue

	// 能够对你的对象执行 list 和 watch 的组件。
	ListerWatcher

	// Process 处理一个被 Pop 出的 Deltas。
	Process ProcessFunc

	// ProcessBatch 批量处理多个被 Pop 出的 Deltas；若批次中并非所有条目都
	// 处理成功，应返回 `TransactionError`。
	//
	// 启用批量处理需满足：
	// * ProcessBatch 非 nil
	// * Queue 实现 QueueWithBatch
	// * 客户端 InOrderInformersBatchProcess 特性门控已开启
	//
	// 任一条件不满足则使用 Process，不做批量处理。
	//
	// 批量模式的前提还包括 InOrderInformers：事件必须已按序到达，否则
	// 批量应用 Delta 会破坏对象最终状态。
	ProcessBatch ProcessBatchFunc

	// ObjectType 是本控制器预期处理的类型的一个示例对象。
	ObjectType runtime.Object

	// ObjectDescription 是在记录本控制器类型相关信息时使用的描述。
	ObjectDescription string

	// FullResyncPeriod 是考虑调用 ShouldResync 的周期。
	//
	// 只决定何时询问 ShouldResync，本身不会强制重放全部对象。
	FullResyncPeriod time.Duration

	// MinWatchTimeout 若设置，则定义发往 kube-apiserver 的 watch 请求的最小
	// 超时时间。低于 5m 的值不会被采纳，以免对控制平面造成负面影响。
	// 可选——未设置时使用默认值 5m。
	MinWatchTimeout time.Duration

	// ShouldResync 被 reflector 周期性地用于判断是否要 Resync 队列。
	// 若 ShouldResync 为 `nil` 或返回 true，表示 reflector 应当执行 resync。
	ShouldResync ShouldResyncFunc

	// 当 ListAndWatch 因错误断开连接时被调用。
	//
	// 上下文日志：支持上下文日志的代码应使用 WatchErrorHandlerWithContext
	// 而非 WatchErrorHandler。
	WatchErrorHandler WatchErrorHandler

	// 当 ListAndWatch 因错误断开连接、且未设置 WatchErrorHandler 时被调用。
	WatchErrorHandlerWithContext WatchErrorHandlerWithContext

	// WatchListPageSize 是初始列表和 relist 列表请求的分块大小。
	//
	// 设为 0 表示使用服务端默认值；分块过小会增加往返次数。
	WatchListPageSize int64
}

// ShouldResyncFunc 用于指示 reflector 是否应执行 resync。共享 informer 可用它
// 支持带自定义周期的多个事件处理器。
//
// 共享 informer 的多个 handler 可能要求不同周期，因此返回 bool 而非固定周期。
type ShouldResyncFunc func() bool

// ProcessFunc 处理单个对象。
//
// obj 通常是 Deltas（同一个键的多次变更合并），isInInitialList 表示该对象是否
// 来自 informer 的首次全量列表。
type ProcessFunc func(obj interface{}, isInInitialList bool) error

// ProcessBatchFunc 批量处理多个对象。同一对象不得在 deltas 中出现多条。
// 同一批次内同一对象只出现一次，是保证批量事务语义的前提。
type ProcessBatchFunc func(deltas []Delta, isInInitialList bool) error

// controller 是 Controller 的默认实现，持有 Reflector 供 LastSyncResourceVersion 使用。
//
// reflector 在 RunWithContext 中创建，故用读写锁保护，允许其他 goroutine
// 并发读取其同步进度。
//
// `*controller` 实现 Controller。
type controller struct {
	config         Config
	reflector      *Reflector
	reflectorMutex sync.RWMutex
	clock          clock.Clock
}

// Controller 是由 Config 参数化的低层控制器，用于 sharedIndexInformer。
type Controller interface {
	// RunWithContext 做两件事。其一是构造并运行一个 Reflector，把对象/通知
	// 从 Config 的 ListerWatcher 泵入 Config 的 Queue，并可能偶尔对该 Queue
	// 执行 Resync。其二是反复从 Queue 中 Pop，并用 Config 的 ProcessFunc
	// 处理。两者都持续到上下文被取消。
	//
	// 多次调用 RunWithContext 是错误。
	// RunWithContext 会阻塞；请用 go 调用。
	RunWithContext(ctx context.Context)

	// Run 与 RunWithContext 相同，只是用停止 channel 代替上下文。
	//
	// 上下文日志：支持上下文日志的代码应使用 RunWithContext 而非 Run。
	Run(stopCh <-chan struct{})

	// HasSynced 委托给 Config 的 Queue。
	HasSynced() bool

	// HasSyncedChecker 支持无需轮询地等待同步。返回的 DoneChecker 可传给
	// WaitFor。它委托给 Config 的 Queue。
	HasSyncedChecker() DoneChecker

	// LastSyncResourceVersion 在 Reflector 存在时委托给它，否则返回空字符串。
	//
	// Reflector 尚未创建（Run 之前）时返回空串，调用方不能把它当作有效的
	// resourceVersion 使用。
	LastSyncResourceVersion() string
}

// New 根据给定的 Config 创建一个新 Controller。
func New(c *Config) Controller {
	// 复制 Config 值，避免调用方在 Run 之前修改配置导致数据竞争。
	ctlr := &controller{
		config: *c,
		clock:  &clock.RealClock{},
	}
	return ctlr
}

// Run 实现 [Controller.Run]。
func (c *controller) Run(stopCh <-chan struct{}) {
	c.RunWithContext(wait.ContextForChannel(stopCh))
}

// RunWithContext 实现 [Controller.RunWithContext]。
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

// 当本控制器完成初始资源列表后返回 true。
func (c *controller) HasSynced() bool {
	return c.config.Queue.HasSynced()
}

// HasSyncedChecker 支持无需轮询地等待同步。返回的 DoneChecker 可传给 [WaitFor]。
// 它委托给 Config 的 Queue。
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

// processLoop 消费工作队列。
// TODO：考虑并行处理。这需要一些设计，以确保同一对象不会被并发地多次处理。
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
				// 否则回退到非批量处理行为
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

// ResourceEventHandler 可以处理发生在资源上的事件通知。事件仅具参考性，
// 因此无法返回错误。处理器绝不可以修改收到的对象；这不仅指顶层结构，
// 还包括从其可达的所有数据结构。
//   - OnAdd 在对象被添加时调用。
//   - OnUpdate 在对象被修改时调用。注意 oldObj 是该对象最后已知的状态——
//     多个变更可能被合并，所以不能借此看到每一个单独的变化。re-list 发生时
//     OnUpdate 也会被调用，即使什么都没变也会调用。这对周期性地评估或同步
//     某些东西很有用。
//   - OnDelete 在已知条目最终状态时会拿到它，否则会得到一个
//     DeletedFinalStateUnknown 类型的对象。当 watch 关闭错过了删除事件、
//     直到后续 re-list 才注意到删除时就可能出现这种情况。
//
// handler 在 informer 自己的协程中串行调用；任何阻塞都会拖慢整个缓存同步，
// 耗时操作应转交工作队列。isInInitialList 仅对 OnAdd 有意义。
type ResourceEventHandler interface {
	OnAdd(obj interface{}, isInInitialList bool)
	OnUpdate(oldObj, newObj interface{})
	OnDelete(obj interface{})
}

// Object 是确定对象名称及（如有）命名空间所需的 metav1.Object 子集。
// 所有标准 Kubernetes API 类型都通过内嵌的 ObjectMeta 满足该接口。
//
// 要求 comparable 是为了能把零值 T 与 OptionalObj 比较，
// 以检测对象缺失（如 nil 指针）。
type Object interface {
	comparable

	GetName() string
	GetNamespace() string
}

// TypedResourceEventHandler 是 ResourceEventHandler 的类型安全变体。
//
// T 必须是可比较的具名对象指针（见 Object），这样编译器能阻止把错误类型
// 传入 handler，同时保留与未类型化版本相同的事件顺序语义。
type TypedResourceEventHandler[T Object] interface {
	OnAdd(obj T, isInInitialList bool)
	OnUpdate(oldObj, newObj T)
	OnDelete(obj DeletedObject[T])
}

// DeletedObject 提供被删除对象的信息。
//
// 它实现了 GetObjectName 和 GetKey，用于替代
// [DeletionHandlingObjectToName] 和 [DeletionHandlingMetaNamespaceKeyFunc]。
//
// 它实现了 GetName 和 GetNamespace 方法以便与 klog.KObj 配合使用：
// OnDelete 事件处理器可以直接记录其参数，并保证产生与对真实对象应用
// klog.KObj 相同的日志输出。
//
// 无论 DeletedObject 是否真的持有被删除对象，上述方法都可用。
type DeletedObject[T Object] struct {
	// OptionalObj 是被删除的对象。
	//
	// 它可能是：
	// - 过期的：某个*更旧*的版本，而不是被删除的那个版本
	// - nil：informer 注意到删除，但没有找到被删除对象的*任何*副本
	//
	// 唯一有保证的信息，是被删除对象在 informer 缓存中存储时使用的 key。
	// 使用 [DeletedObject.GetObjectName] 和 [DeletedObject.GetKey] 分别提取
	// 命名空间与名称、以及被删除对象存储时使用的 key。它们在各种情况下都可用。
	OptionalObj T

	// 若被删除对象可用且是最新的，FinalStateUnknown 为 nil。
	// 否则 FinalStateUnknown 非 nil，用于提供 key。
	// 其中未类型化的对象与 OptionalObj 一致。
	FinalStateUnknown *DeletedFinalStateUnknown
}

// GetObjectName 返回被删除对象的对象名（= 如有则含命名空间，加上名称）。
// 注意这与 GetName 略有不同——后者只返回名称，不含命名空间。
//
// 这是 [DeletionHandlingObjectToName] 的类型安全变体，不会失败：T 保证有
// 名称和命名空间，而 FinalStateUnknown 的 key 若非法，只是被当作无命名空间
// 的名称处理。
//
// 优先信任 FinalStateUnknown.Key：它来自缓存索引，即使对象内容已过期也
// 仍能唯一标识被删除的对象。
func (d DeletedObject[T]) GetObjectName() ObjectName {
	if d.FinalStateUnknown != nil {
		// 切分方式与 SplitMetaNamespaceKey 和 ParseObjectName 相同，
		// 只是不将含多个 "/" 的 key 视为错误：
		// 多余的 "/" 成为名称的一部分。
		namespace, name, found := strings.Cut(d.FinalStateUnknown.Key, "/")
		if !found {
			return ObjectName{Name: namespace}
		}
		return ObjectName{Namespace: namespace, Name: name}
	}
	var null T
	if d.OptionalObj == null {
		// 避免在零值 OptionalObj（如 nil 指针）上调用 GetName/GetNamespace，
		// 那可能导致 panic。多一层间接包装的 nil 指针在此不会被检测到，
		// 也不做处理。
		return ObjectName{}
	}
	// 逻辑与 MetaObjectToName 相同，只是改用 T 自身的 GetNamespace 和
	// GetName 方法，而不像 ObjectToName 那样经由 meta.Accessor。
	if namespace := d.OptionalObj.GetNamespace(); len(namespace) > 0 {
		return ObjectName{Namespace: namespace, Name: d.OptionalObj.GetName()}
	}
	return ObjectName{Name: d.OptionalObj.GetName()}
}

// GetName 返回不含命名空间的名称。注意这与 GetObjectName 略有不同——
// 后者在结构体中返回命名空间与名称的组合。
func (d DeletedObject[T]) GetName() string {
	return d.GetObjectName().Name
}

// GetNamespace 返回命名空间（如有），否则返回空字符串。
func (d DeletedObject[T]) GetNamespace() string {
	return d.GetObjectName().Namespace
}

// GetKey 返回被删除对象存储时使用的 key。
//
// 这是 [DeletionHandlingMetaNamespaceKeyFunc] 的类型安全变体，不会失败：
// T 保证有名称和命名空间。
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
	// 经由 ObjectName.String 采用与 MetaNamespaceKeyFunc 相同的编码，
	// 改用 T 自身的 GetNamespace 和 GetName 方法。
	if namespace := d.OptionalObj.GetNamespace(); len(namespace) > 0 {
		return namespace + "/" + d.OptionalObj.GetName()
	}
	return d.OptionalObj.GetName()
}

// ResourceEventHandlerFuncs 是一个适配器，让你轻松选择实现
// ResourceEventHandler 时 desired 提供多少个通知函数。
// 该适配器并不解除“禁止修改对象”的约束。
//
// 若你的用法需要传播 HasSynced，请使用 ResourceEventHandlerDetailedFuncs。
type ResourceEventHandlerFuncs struct {
	AddFunc    func(obj interface{})
	UpdateFunc func(oldObj, newObj interface{})
	DeleteFunc func(obj interface{})
}

// OnAdd 在 AddFunc 非 nil 时调用它。
func (r ResourceEventHandlerFuncs) OnAdd(obj interface{}, isInInitialList bool) {
	if r.AddFunc != nil {
		r.AddFunc(obj)
	}
}

// OnUpdate 在 UpdateFunc 非 nil 时调用它。
func (r ResourceEventHandlerFuncs) OnUpdate(oldObj, newObj interface{}) {
	if r.UpdateFunc != nil {
		r.UpdateFunc(oldObj, newObj)
	}
}

// OnDelete 在 DeleteFunc 非 nil 时调用它。
func (r ResourceEventHandlerFuncs) OnDelete(obj interface{}) {
	if r.DeleteFunc != nil {
		r.DeleteFunc(obj)
	}
}

// TypedResourceEventHandlerFuncs 是 TypedResourceEventHandler 的函数适配器。
//
// 与未类型化版本相同：未设置的函数会被跳过，因此可以只关心部分事件。
// 该适配器同样禁止修改收到的对象。
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

// ResourceEventHandlerDetailedFuncs 与 ResourceEventHandlerFuncs 完全相同，
// 只是其 AddFunc 接受 isInInitialList 参数，用于传播 HasSynced。
type ResourceEventHandlerDetailedFuncs struct {
	AddFunc    func(obj interface{}, isInInitialList bool)
	UpdateFunc func(oldObj, newObj interface{})
	DeleteFunc func(obj interface{})
}

// OnAdd 在 AddFunc 非 nil 时调用它。
func (r ResourceEventHandlerDetailedFuncs) OnAdd(obj interface{}, isInInitialList bool) {
	if r.AddFunc != nil {
		r.AddFunc(obj, isInInitialList)
	}
}

// OnUpdate 在 UpdateFunc 非 nil 时调用它。
func (r ResourceEventHandlerDetailedFuncs) OnUpdate(oldObj, newObj interface{}) {
	if r.UpdateFunc != nil {
		r.UpdateFunc(oldObj, newObj)
	}
}

// OnDelete 在 DeleteFunc 非 nil 时调用它。
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

// FilteringResourceEventHandler 对进来的所有事件应用给定的过滤器，确保调用
// 合适的嵌套 handler 方法。更新后开始通过过滤器的对象视为添加，更新后不再
// 通过过滤器的对象视为删除。与 handler 一样，过滤器绝不可以修改收到的对象。
//
// 这种“过滤状态迁移”设计让下游 handler 只需处理自己关心的对象，
// 不必自行维护成员集合。
type FilteringResourceEventHandler struct {
	FilterFunc func(obj interface{}) bool
	Handler    ResourceEventHandler
}

// OnAdd 仅在过滤器通过时调用嵌套 handler。
func (r FilteringResourceEventHandler) OnAdd(obj interface{}, isInInitialList bool) {
	if !r.FilterFunc(obj) {
		return
	}
	r.Handler.OnAdd(obj, isInInitialList)
}

// OnUpdate 根据过滤器是否匹配，确保调用正确的 handler。
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
		// 什么都不做
	}
}

// OnDelete 仅在过滤器通过时调用嵌套 handler。
func (r FilteringResourceEventHandler) OnDelete(obj interface{}) {
	if !r.FilterFunc(obj) {
		return
	}
	r.Handler.OnDelete(obj)
}

// TypedFilteringResourceEventHandler 是 [FilteringResourceEventHandler]
// 的类型安全变体。
type TypedFilteringResourceEventHandler[T Object] struct {
	FilterFunc func(obj T) bool
	Handler    TypedResourceEventHandler[T]
}

// OnAdd 仅在过滤器通过时调用嵌套 handler。
func (r TypedFilteringResourceEventHandler[T]) OnAdd(obj T, isInInitialList bool) {
	if !r.FilterFunc(obj) {
		return
	}
	r.Handler.OnAdd(obj, isInInitialList)
}

// OnUpdate 根据过滤器是否匹配，确保调用正确的 handler。
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
		// 什么都不做
	}
}

// OnDelete 仅在过滤器通过时调用嵌套 handler。过滤器作用于
// [DeletedObject.OptionalObj]：若无法恢复被删除对象，它可能是 T 的零值；
// 依赖 T 处于有效状态的 FilterFunc 必须自行处理该情况。
func (r TypedFilteringResourceEventHandler[T]) OnDelete(obj DeletedObject[T]) {
	if !r.FilterFunc(obj.OptionalObj) {
		return
	}
	r.Handler.OnDelete(obj)
}

// DeletionHandlingMetaNamespaceKeyFunc 在调用 MetaNamespaceKeyFunc 之前先检查
// DeletedFinalStateUnknown 对象。
//
// DeletedFinalStateUnknown 携带的 Key 已经是计算好的索引键，直接复用可避免
// 对可能已失效的对象再次取元数据。
func DeletionHandlingMetaNamespaceKeyFunc(obj interface{}) (string, error) {
	if d, ok := obj.(DeletedFinalStateUnknown); ok {
		return d.Key, nil
	}
	return MetaNamespaceKeyFunc(obj)
}

// DeletionHandlingObjectToName 在调用 ObjectToName 之前先检查
// DeletedFinalStateUnknown 对象。
func DeletionHandlingObjectToName(obj interface{}) (ObjectName, error) {
	if d, ok := obj.(DeletedFinalStateUnknown); ok {
		return ParseObjectName(d.Key)
	}
	return ObjectToName(obj)
}

// InformerOptions 用于配置 Reflector。
//
// 这些选项最终被翻译成 Queue/Reflector 的配置；字符串化的 Identifier 用于
// 指标标签，因此生产代码应尽量设置。
type InformerOptions struct {
	// Logger 若非 nil，则用于日志输出，取代 klog.Background()。
	Logger *klog.Logger

	// ListerWatcher 为 informer 所通知的资源来源实现 List 和 Watch 函数。
	ListerWatcher ListerWatcher

	// ObjectType 是 informer 预期接收类型的一个对象。
	ObjectType runtime.Object

	// Handler 定义对象变更时应调用的函数。
	Handler ResourceEventHandler

	// ResyncPeriod 是底层 Reflector 的 resync 周期。若非零，将以该频率重新
	// 同步 store——即使对象没有变化也会投递 Modify 事件。
	// 这对同步那些配置外部资源的对象很有用（例如配置云服务商功能）。
	// 可选——未设置时 store 不会周期性重同步。
	//
	// 注意这是“本地重放”而非重新 List：事件中的对象来自缓存，resourceVersion
	// 未必变化，handler 必须幂等。
	ResyncPeriod time.Duration

	// MinWatchTimeout 若设置，则定义发往 kube-apiserver 的 watch 请求的最小
	// 超时时间。低于 5m 的值不会被采纳，以免对控制平面造成负面影响。
	// 可选——未设置时使用默认值 5m。
	MinWatchTimeout time.Duration

	// Indexers 若设置，则作为接收对象的索引器以优化某些查询。
	// 可选——未设置时不维护任何索引。
	Indexers Indexers

	// Transform 函数若设置，将在所有对象放入 Store 并触发相应
	// Add/Modify/Delete handler 之前被调用。
	// 可选——未设置时不做额外转换。
	Transform TransformFunc

	// Identifier 用于在指标和日志中标识该 FIFO。未设置时不发布指标。
	Identifier InformerNameAndResource

	// InformerMetricsProvider 是该 informer 的指标提供者。未设置时指标为 no-op。
	InformerMetricsProvider InformerMetricsProvider
}

// NewInformerWithOptions 返回一个 Store 和一个用于填充该 store 的控制器，
// 同时提供事件通知。你只应对返回的 Store 执行 Get/List 操作；
// Add/Modify/Delete 会导致事件通知失真。
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

// NewInformer 返回一个 Store 和一个用于填充该 store 的控制器，同时提供事件
// 通知。你只应对返回的 Store 执行 Get/List 操作；Add/Modify/Delete 会导致
// 事件通知失真。
//
// 参数：
//   - lw：你所关心的资源来源的 list 和 watch 函数。
//   - objType：你预期接收类型的一个对象。
//   - resyncPeriod：若非零，将以该频率重新 list（即使没有变化也会收到 OnUpdate
//     调用）。否则，re-list 会尽可能推迟（直到上游来源关闭 watch 或超时，
//     或你停止控制器）。
//   - h：接收通知的目标对象。
//
// Deprecated: 请改用 NewInformerWithOptions。
func NewInformer(
	lw ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
) (Store, Controller) {
	// 用于保存我们所知的客户端状态。
	clientState := NewStore(DeletionHandlingMetaNamespaceKeyFunc)

	options := InformerOptions{
		ListerWatcher: lw,
		ObjectType:    objType,
		Handler:       h,
		ResyncPeriod:  resyncPeriod,
	}
	return clientState, newInformer(clientState, options, DeletionHandlingMetaNamespaceKeyFunc)
}

// NewIndexerInformer 返回一个 Indexer 和一个用于填充索引的 Controller，
// 同时提供事件通知。你只应对返回的 Index 执行 Get/List 操作；
// Add/Modify/Delete 会导致事件通知失真。
//
// 参数：
//   - lw：你所关心的资源来源的 list 和 watch 函数。
//   - objType：你预期接收类型的一个对象。
//   - resyncPeriod：若非零，将以该频率重新 list（即使没有变化也会收到 OnUpdate
//     调用）。否则，re-list 会尽可能推迟（直到上游来源关闭 watch 或超时，
//     或你停止控制器）。
//   - h：接收通知的目标对象。
//   - indexers：针对接收对象类型的索引器。
//
// Deprecated: 请改用 NewInformerWithOptions。
func NewIndexerInformer(
	lw ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
	indexers Indexers,
) (Indexer, Controller) {
	// 用于保存我们所知的客户端状态。
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

// NewTransformingInformer 返回一个 Store 和一个用于填充该 store 的控制器，
// 同时提供事件通知。你只应对返回的 Store 执行 Get/List 操作；
// Add/Modify/Delete 会导致事件通知失真。
// 给定的 transform 函数会在所有对象放入 Store 并触发相应 Add/Modify/Delete
// handler 之前被调用。
//
// Deprecated: 请改用 NewInformerWithOptions。
func NewTransformingInformer(
	lw ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
	transformer TransformFunc,
) (Store, Controller) {
	// 用于保存我们所知的客户端状态。
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

// NewTransformingIndexerInformer 返回一个 Indexer 和一个用于填充索引的
// 控制器，同时提供事件通知。你只应对返回的 Index 执行 Get/List 操作；
// Add/Modify/Delete 会导致事件通知失真。
// 给定的 transform 函数会在所有对象放入索引并触发相应 Add/Modify/Delete
// handler 之前被调用。
//
// Deprecated: 请改用 NewInformerWithOptions。
func NewTransformingIndexerInformer(
	lw ListerWatcher,
	objType runtime.Object,
	resyncPeriod time.Duration,
	h ResourceEventHandler,
	indexers Indexers,
	transformer TransformFunc,
) (Indexer, Controller) {
	// 用于保存我们所知的客户端状态。
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

// 将以 Deltas 列表形式到来的更新多路复用到 Store 中，并把 OnUpdate、OnAdd、
// OnDelete 事件通知给给定的 handler。
//
// Delta 中的对象可能来自本地重放，因此“更新”不保证内容变化；handler 需
// 自行按 resourceVersion 或语义去重。
func processDeltas(
	logger klog.Logger,
	// 接收来自给定 deltas 的事件通知的对象
	handler ResourceEventHandler,
	clientState Store,
	deltas Deltas,
	isInInitialList bool,
	keyFunc KeyFunc,
) error {
	// 从最旧到最新
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

// processDeltasInBatch 将一批 Delta 对象应用到给定 Store，并把新增、更新或
// 删除事件通知给 ResourceEventHandler。
//
// 若 Store 支持事务（TransactionStore），所有 Delta 会在单个事务中原子应用，
// 随后再执行对应的 handler 回调。否则逐个处理每个 Delta。
//
// 任何 Delta 或事务失败都会返回错误。对 TransactionError，只有成功操作会
// 触发回调。
func processDeltasInBatch(
	logger klog.Logger,
	handler ResourceEventHandler,
	clientState Store,
	deltas []Delta,
	isInInitialList bool,
	keyFunc KeyFunc,
) error {
	// 从最旧到最新
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
	// deltas 是去重对象列表
	for _, d := range deltas {
		obj := d.Object
		switch d.Type {
		case Sync, Replaced, Added, Updated:
			// 事务内查询 Store 仍能看到提交前状态，因此 old 是该批次的“前像”；
			// 回调延迟到事务提交后执行，避免 handler 看到未提交数据。
			// 由于条目唯一，每个对象只会返回一个旧对象
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
		// 若事务出错，只执行成功条目对应的回调
		for _, i := range err.SuccessfulIndices {
			if i < len(callbacks) {
				callbacks[i]()
			}
		}
		// 对错误做格式化，避免 txns 逃逸、保持在栈上分配。
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
			// 该行为与 processDeltas 处理 Replace delta 的方式一致
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

	// 先替换客户端状态，使 store 反映 handler 将收到的事件
	if err := clientState.Replace(info.Objects, info.ResourceVersion); err != nil {
		return err
	}
	// 先处理所有删除，与 RealFIFO#Replace 的行为一致
	for _, objToDelete := range deletions {
		handler.OnDelete(objToDelete)
	}
	// 按 reconcileReplacement 观察到的顺序处理新增/更新，与 RealFIFO#Replace 的行为一致
	for _, r := range replacements {
		if r.oldObj != nil {
			handler.OnUpdate(r.oldObj, r.newObj)
		} else {
			handler.OnAdd(r.newObj, isInInitialList)
		}
	}
	return nil
}

// newInformer 返回一个用于填充 store 的控制器，同时提供事件通知。
//
// 参数
//   - clientState：要填充的 store
//   - options：配置该控制器的选项
func newInformer(clientState Store, options InformerOptions, keyFunc KeyFunc) Controller {
	// 用于保存到来的变更。注意我们把 clientState 作为 KeyLister 传入，
	// 这样 resync 操作会产生正确的更新/删除 delta 集合。

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
				// 这里必须使用 *fifo 自己的* logger。
				return processDeltas(logger, options.Handler, clientState, deltas, isInInitialList, keyFunc)
			}
			return errors.New("object given as Process argument is not Deltas")
		},
		ProcessBatch: func(deltaList []Delta, isInInitialList bool) error {
			// 同理。
			return processDeltasInBatch(logger, options.Handler, clientState, deltaList, isInInitialList, keyFunc)
		},
	}
	return New(cfg)
}

// newQueueFIFO 构造新的 FIFO，依据 InOrderInformers 特性门控在 real 与 delta
// FIFO 之间选择。
//
// 返回 FIFO 以及 FIFO 使用的 logger。该 logger 包含 FIFO 的名称，
// 与传入时的 logger 不同。
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
		// 若启用了原子事件，则不设置 clientState：
		// 无法把 store 传给原子 fifo。
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
