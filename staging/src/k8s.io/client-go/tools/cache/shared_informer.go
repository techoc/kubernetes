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
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache/synctrack"
	"k8s.io/utils/buffer"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	utiltrace "k8s.io/utils/trace"

	"k8s.io/klog/v2"

	clientgofeaturegate "k8s.io/client-go/features"
)

// SharedInformer 为其客户端提供与某组对象权威状态之间的最终一致性联动。
// 对象由 API 组、kind/resource、命名空间（如有）和 name 标识；就本契约而言，
// `ObjectMeta.UID` 不是对象 ID 的一部分。一个 SharedInformer 只联动某个特定
// API 组和 kind/resource 的对象；其关联的对象集合还可以进一步限定为单个
// 命名空间（如适用），和/或通过标签选择器、字段选择器限定。
//
// 对象的权威状态以 apiserver 提供的访问为准，对象会经过严格有序的状态序列。
// 对象状态要么是（1）存在，带有 ResourceVersion 及其他相应内容，要么是
// （2）"absent"（不存在）。
//
// SharedInformer 为每个相关对象维护一份本地缓存——通过 GetStore() 暴露，
// 带索引的 informer 还通过 GetIndexer() 暴露，创建/访问该 informer 的相关
// 机制也可能暴露它——并与权威状态保持最终一致。也就是说，除非被持续性通信
// 故障阻断，一旦某个对象 ID X 在权威侧与状态 S 关联，那么对于集合中包含
// (X, S) 的每个 SharedInformer I，最终必然出现以下情况之一：（1）I 的缓存
// 将 X 关联到 S 或 X 的更晚状态；（2）I 已停止；（3）X 的权威状态服务终止。
// 为形式完整性起见，我们规定"不存在"状态满足任何标签选择器或字段选择器的
// 约束。
//
// 对于给定的 informer 和相关对象 ID X，informer 缓存中出现的状态序列是 X
// 在权威侧状态序列的子序列。也就是说，某些状态可能永远不会出现在缓存中，
// 但出现的状态之间顺序正确。注意：不同对象之间看到的状态没有任何顺序保证。
//
// 本地缓存初始为空，在 `Run()` 期间被填充和更新。
//
// 简单示例一：若对象集合此后不再变化，创建联动该集合的 SharedInformer 并
// `Run()`，则该 informer 的缓存最终会持有该集合的精确副本（除非过早停止、
// 权威状态服务终止，或两侧通信故障持续阻碍达成）。
//
// 简单示例二：若本地缓存曾经为某对象 ID 持有非"不存在"状态，而该对象最终
// 从权威状态中被删除，则该对象最终也会从本地缓存中被删除（除非
// SharedInformer 过早停止、权威状态服务终止，或通信故障持续阻碍期望结果）。
//
// Store 中的 key：命名空间对象为 namespace/name 形式，非命名空间对象就是
// name。客户端可以用 `MetaNamespaceKeyFunc(obj)` 提取给定对象的 key，用
// `SplitMetaNamespaceKey(key)` 将 key 拆分为组成部分。
//
// 对本地缓存的每次查询都完全基于缓存状态的单个快照作答。因此 `List` 调用的
// 结果不会包含两条 namespace 和 name 都相同的记录。
//
// 这里的客户端由 ResourceEventHandler 标识。对于 SharedInformer 本地缓存的
// 每次更新、以及在 `Run()` 之前添加的每个客户端，最终要么 SharedInformer
// 停止，要么客户端收到该更新的通知。在 `Run()` 开始之后添加的客户端会收到
// 一个启动批次通知，内容是添加时刻缓存中已存在对象的 Add 事件；此后对该
// 客户端添加之后发生的每次缓存更新，最终要么 SharedInformer 停止，要么该
// 客户端收到通知。客户端通知发生在对应的缓存更新之后；对 SharedIndexInformer
// 而言还发生在对应的索引更新之后。在这些规定通知之前，可能发生额外的缓存与
// 索引更新。对给定的 SharedInformer 和客户端，通知按序投递；对给定的
// SharedInformer、客户端和对象 ID，通知按顺序投递。由于 `ObjectMeta.UID`
// 不参与对象标识，可能出现：（1）本地缓存中带 ID（如 namespace 和 name）X、
// `ObjectMeta.UID` U1 的对象 O1 被删除，随后（2）另一个带 ID X、
// ObjectMeta.UID U2 的对象 O2 被创建，此时 informer 的客户端不会分别收到
// (1) 和 (2) 的通知，而是只收到一条从 O1 到 O2 的更新通知。需要检测这种
// 情况的客户端可以在处理更新通知的代码（即 ResourceEventHandler 的
// `OnUpdate` 方法）中比较新旧对象的 `ObjectMeta.UID` 字段。
//
// 客户端必须及时处理每条通知；SharedInformer 并不擅长处理大量积压的待投递
// 通知。耗时的处理应转交给其他机制，例如 `client-go/util/workqueue`。
//
// 删除通知暴露的是最后一次本地已知的非"不存在"状态，但其 ResourceVersion
// 会被替换为一个对象实际不存在时的 ResourceVersion。
//
// 概括：SharedInformer 把 ListAndWatch 得到的权威状态同步到本地缓存，
// 再把变化分发给注册的事件处理器。缓存最终一致、同一对象的通知有序，
// 但不同对象之间没有顺序保证；处理器必须快速返回，耗时逻辑应交给 workqueue。
type SharedInformer interface {
	// AddEventHandler 使用共享 informer 的 resync 周期添加一个事件处理器。
	// 单个处理器的事件按序投递，但不同处理器之间没有协调。
	// 返回处理器的注册句柄，可用于再次移除该处理器，或判断处理器是否已同步
	// （即已看到初始列表中的每一项）。
	//
	// 上下文日志：支持上下文日志的代码应改用 AddEventHandlerWithOptions，并在 options 中传入 logger。
	AddEventHandler(handler ResourceEventHandler) (ResourceEventHandlerRegistration, error)
	// AddEventHandlerWithResyncPeriod 向共享 informer 添加事件处理器并请求指定的
	// resync 周期；零值表示该处理器不关心 resync。resync 操作是指为 informer
	// 本地缓存中的每个对象向处理器投递一次更新通知；它不会与权威存储产生任何
	// 交互。有些 informer 完全不做 resync，即使处理器请求了非零 resyncPeriod。
	// 对于会做 resync 的 informer，以及每个请求 resync 的处理器，该 informer
	// 会确定一个名义 resync 周期：不短于请求的周期，但可能更长。由于实现本身
	// 需要时间做工作，且可能存在负载竞争与调度噪声，任意两次实际 resync 之间
	// 的时间可能超过名义周期。
	// 返回可用于再次移除该处理器的注册句柄；若处理器无法添加则返回错误。
	//
	// 上下文日志：支持上下文日志的代码应改用 AddEventHandlerWithOptions，并在 options 中传入 logger。
	AddEventHandlerWithResyncPeriod(handler ResourceEventHandler, resyncPeriod time.Duration) (ResourceEventHandlerRegistration, error)
	// AddEventHandlerWithOptions 是 AddEventHandlerWithResyncPeriod 的变体，
	// 所有可选参数通过结构体传入。
	AddEventHandlerWithOptions(handler ResourceEventHandler, options HandlerOptions) (ResourceEventHandlerRegistration, error)
	// RemoveEventHandler 根据注册句柄移除此前添加的事件处理器。
	// 本函数保证幂等且线程安全。
	//
	// 注意：RemoveEventHandler 是异步的。它会停止排队新事件，但不会等待已排队
	// 的事件执行完毕。本函数返回后，处理剩余事件的 goroutine 可能仍在运行并
	// 调用回调。
	//
	// 如果调用方需要等待所有处理器执行完毕（例如为了安全地关闭 channel 或释放
	// 处理器占用的资源），应使用 [ShutDownEventHandler]。
	RemoveEventHandler(handle ResourceEventHandlerRegistration) error
	// GetStore 以 Store 形式返回 informer 的本地缓存。
	GetStore() Store
	// GetController 已废弃，没有任何用处。
	GetController() Controller
	// Run 启动并运行共享 informer，停止后返回。
	// stopCh 关闭时 informer 会停止。
	//
	// 上下文日志：使用上下文日志的代码应改用 RunWithContext。
	Run(stopCh <-chan struct{})
	// RunWithContext 启动并运行共享 informer，停止后返回。
	// context 被取消时 informer 会停止。
	RunWithContext(ctx context.Context)
	// HasSynced 在共享 informer 的 store 至少被一次完整的对象集合权威状态
	// LIST 告知后返回 true。这与 "resync" 无关。
	//
	// 注意：这不能告诉你单个处理器是否已同步！要判断这一点，请调用
	// AddEventHandler 返回的句柄上的 HasSynced。
	HasSynced() bool
	// HasSyncedChecker 在共享 informer 的 store 至少被一次完整的对象集合权威
	// 状态 LIST 告知后视为完成。这与 "resync" 无关。
	//
	// 注意：这不能告诉你单个处理器是否已同步！要判断这一点，请使用
	// AddEventHandler 返回的句柄上的 HasSyncedChecker。
	HasSyncedChecker() DoneChecker
	// LastSyncResourceVersion 是上次与底层 store 同步时观察到的 resource version。
	// 返回的值不与底层 store 的访问同步，不是线程安全的。
	LastSyncResourceVersion() string

	// WatchErrorHandler 在 ListAndWatch 因错误断开连接时被调用。调用该处理器后，
	// informer 会退避并重试。
	//
	// 默认实现会查看错误类型，并尝试以合适的级别记录错误消息。
	//
	// 只有一个处理器，因此多次调用时最后一次生效；在 informer 已启动后调用会
	// 返回错误。
	//
	// 该处理器用于提升可见性，而不是（例如）暂停消费者。处理器应快速返回——
	// 任何开销大的处理都应转交出去。
	//
	// 上下文日志：支持上下文日志的代码应改用 SetWatchErrorHandlerWithContext。
	SetWatchErrorHandler(handler WatchErrorHandler) error

	// SetWatchErrorHandlerWithContext 是 SetWatchErrorHandler 的变体，
	// 处理器会额外接收一个 context 参数。
	SetWatchErrorHandlerWithContext(handler WatchErrorHandlerWithContext) error

	// TransformFunc 会在每个对象即将被存储前被调用。
	//
	// 这个函数的用途是让你有机会移除、转换或规范化字段。一个用例是从对象中
	// 剥离用不到的 metadata 字段以节省内存开销。
	//
	// 必须在 informer 启动前设置。
	//
	// 更多细节请参考 TransformFunc 的注释。
	SetTransform(handler TransformFunc) error

	// IsStopped 报告 informer 是否已经停止。不能向已停止的 informer 添加事件
	// 处理器。已停止的 informer 不会再被启动。
	IsStopped() bool
}

// ResourceEventHandlerRegistration 是 ResourceEventHandler 在某个
// SharedInformer 中注册状态的不透明接口。必须把它传回同一个 SharedInformer
// 的 `RemoveEventHandler` 才能注销处理器。
//
// 也用于判断处理器是否已同步（初始列表中的所有条目都已投递完毕）。
type ResourceEventHandlerRegistration interface {
	// HasSynced 报告父级 informer 是否已同步、且同步前的所有事件是否都已投递完毕。
	HasSynced() bool

	// HasSyncedChecker 报告父级 informer 是否已同步、且同步前的所有事件是否都已投递完毕。
	HasSyncedChecker() DoneChecker
}

// HandlerOptions 是 [SharedInformer.AddEventHandlerWithOptions] 的可选配置。
// 可以留空。
type HandlerOptions struct {
	// Logger 覆盖默认的 klog.Background() logger。
	//
	// 处理器内部的日志应使用该 logger，才能带上是哪个 informer 的上下文。
	Logger *klog.Logger

	// ResyncPeriod 向 informer 请求某个 resync 周期。零值表示处理器不关心
	// resync。并非所有 informer 都会 resync，即使被请求。详见
	// [SharedInformer.AddEventHandlerWithResyncPeriod]。
	//
	// 为 nil 时使用共享 informer 的默认 resync 周期。
	ResyncPeriod *time.Duration
}

// SharedIndexInformer 在 SharedInformer 基础上提供添加和获取 Indexer 的能力。
type SharedIndexInformer interface {
	SharedInformer
	AddIndexers(indexers Indexers) error
	GetIndexer() Indexer
}

// TypedSharedIndexInformer 为 SharedIndexInformer 中非类型安全的方法提供
// 类型安全的变体，使用这些变体时无需类型转换。
//
// 类型参数 T 由调用方保证与 informer 实际处理的对象类型一致；
// 包装层只在回调边界做类型断言。
type TypedSharedIndexInformer[T Object] interface {
	SharedIndexInformer

	// AddTypedEventHandler 是 SharedIndexInformer.AddEventHandlerWithOptions
	// 和 SharedIndexInformer.AddEventHandler 的类型安全替代。
	//
	// 不传 options 时，默认行为与 AddEventHandler 或传入空 HandlerOptions 的
	// AddEventHandlerWithOptions 相同。传一个实例等价于 AddEventHandlerWithOptions。
	// 传多于一个是非法调用，会返回错误。
	AddTypedEventHandler(handler TypedResourceEventHandler[T], options ...HandlerOptions) (ResourceEventHandlerRegistration, error)
	AddTypedIndexers(indexers TypedIndexers[T]) error
	GetTypedIndexer() TypedIndexer[T]
}

// NewTypedSharedIndexInformer 把普通 informer 包装为类型安全视图。
//
// 不复制底层 informer，所有类型安全方法最终都委托给同一个缓存与分发器。
func NewTypedSharedIndexInformer[T Object](informer SharedIndexInformer) TypedSharedIndexInformer[T] {
	return &typedSharedIndexInformer[T]{SharedIndexInformer: informer}
}

type typedSharedIndexInformer[T Object] struct {
	SharedIndexInformer
}

func (s typedSharedIndexInformer[T]) AddTypedEventHandler(handler TypedResourceEventHandler[T], options ...HandlerOptions) (ResourceEventHandlerRegistration, error) {
	var o HandlerOptions
	// 可变参数只为兼容“零个或一个 options”，多于一个属于调用错误。
	switch len(options) {
	case 0:
	case 1:
		o = options[0]
	default:
		return nil, fmt.Errorf("at most one HandlerOptions may be passed, got %d", len(options))
	}
	return s.AddEventHandlerWithOptions(&typedResourceEventHandler[T]{handler}, o)
}

func (s typedSharedIndexInformer[T]) AddTypedIndexers(indexers TypedIndexers[T]) error {
	return s.AddIndexers(TypedIndexersToIndexers(indexers))
}

func (s typedSharedIndexInformer[T]) GetTypedIndexer() TypedIndexer[T] {
	return &typedIndexer[T]{s.GetIndexer()}
}

// typedResourceEventHandler 通过调用 TypedResourceEventHandler 实例的方法，
// 实现非类型安全的 ResourceEventHandler 接口。
type typedResourceEventHandler[T Object] struct {
	handler TypedResourceEventHandler[T]
}

var _ ResourceEventHandler = &typedResourceEventHandler[*metav1.ObjectMeta]{}

func (h *typedResourceEventHandler[T]) OnAdd(obj any, isInitialList bool) {
	h.handler.OnAdd(obj.(T), isInitialList)
}

func (h *typedResourceEventHandler[T]) OnUpdate(oldObj, newObj any) {
	h.handler.OnUpdate(oldObj.(T), newObj.(T))
}

func (h *typedResourceEventHandler[T]) OnDelete(obj any) {
	if tomb, ok := obj.(DeletedFinalStateUnknown); ok {
		if tomb.Obj == nil {
			h.handler.OnDelete(DeletedObject[T]{FinalStateUnknown: &tomb})
			return
		}
		h.handler.OnDelete(DeletedObject[T]{OptionalObj: tomb.Obj.(T), FinalStateUnknown: &tomb})
		return
	}
	h.handler.OnDelete(DeletedObject[T]{OptionalObj: obj.(T)})
}

type typedIndexer[T any] struct {
	Indexer
}

func (i typedIndexer[T]) TypedIndex(indexName string, obj T) ([]T, error) {
	untyped, err := i.Index(indexName, obj)
	if err != nil {
		return nil, err
	}
	typed := make([]T, len(untyped))
	for i, obj := range untyped {
		typed[i] = obj.(T)
	}
	return typed, nil
}

func (i typedIndexer[T]) ByTypedIndex(indexName, indexedValue string) ([]T, error) {
	untyped, err := i.ByIndex(indexName, indexedValue)
	if err != nil {
		return nil, err
	}
	typed := make([]T, len(untyped))
	for i, obj := range untyped {
		typed[i] = obj.(T)
	}
	return typed, nil
}

func (i typedIndexer[T]) AddTypedIndexers(newIndexers TypedIndexers[T]) error {
	untyped := make(Indexers, len(newIndexers))
	for i, indexer := range newIndexers {
		untyped[i] = func(obj any) ([]string, error) {
			return indexer(obj.(T))
		}
	}
	return i.AddIndexers(untyped)
}

// NewSharedInformer 为 ListerWatcher 创建新实例。完整细节见 NewSharedIndexInformerWithOptions。
func NewSharedInformer(lw ListerWatcher, exampleObject runtime.Object, defaultEventHandlerResyncPeriod time.Duration) SharedInformer {
	return NewSharedIndexInformer(lw, exampleObject, defaultEventHandlerResyncPeriod, Indexers{})
}

// NewSharedIndexInformer 为 ListerWatcher 和指定的 Indexers 创建新实例。完整
// 细节见 NewSharedIndexInformerWithOptions。
func NewSharedIndexInformer(lw ListerWatcher, exampleObject runtime.Object, defaultEventHandlerResyncPeriod time.Duration, indexers Indexers) SharedIndexInformer {
	return NewSharedIndexInformerWithOptions(
		lw,
		exampleObject,
		SharedIndexInformerOptions{
			ResyncPeriod: defaultEventHandlerResyncPeriod,
			Indexers:     indexers,
		},
	)
}

// NewSharedIndexInformerWithOptions 为 ListerWatcher 创建新实例。
// 若 options.ResyncPeriod 为零，创建的 informer 不做 resync。否则：对每个
// 请求了非零 resync 周期的处理器（无论在 informer 启动前还是启动后添加），
// 名义 resync 周期是请求的周期向上取整到 informer resync 检查周期的整数倍。
// 该 informer 的 resync 检查周期在 informer 开始运行时确定，取以下两者的
// 最大值：（a）informer 启动前请求的各 resync 周期与这里的 options.ResyncPeriod
// 的最小值；（b）本文件中定义的常量 `minimumResyncPeriod`。
func NewSharedIndexInformerWithOptions(lw ListerWatcher, exampleObject runtime.Object, options SharedIndexInformerOptions) SharedIndexInformer {
	realClock := &clock.RealClock{}

	processor := &sharedProcessor{clock: realClock}
	// listenersRCond 使用 RLock 作为等待语义：distribute 持读锁等待“监听器已启动”，
	// run() 持写锁设置 started 后 Signal，避免通知在监听器就绪前丢失。
	processor.listenersRCond = sync.NewCond(processor.listenersLock.RLocker())

	return &sharedIndexInformer{
		indexer:                         NewIndexer(DeletionHandlingMetaNamespaceKeyFunc, options.Indexers, WithStoreMetrics(options.Identifier, options.InformerMetricsProvider)),
		processor:                       processor,
		synced:                          make(chan struct{}),
		listerWatcher:                   lw,
		objectType:                      exampleObject,
		objectDescription:               options.ObjectDescription,
		resyncCheckPeriod:               options.ResyncPeriod,
		defaultEventHandlerResyncPeriod: options.ResyncPeriod,
		clock:                           realClock,
		cacheMutationDetector:           NewCacheMutationDetector(fmt.Sprintf("%T", exampleObject)),
		identifier:                      options.Identifier,
		informerMetricsProvider:         options.InformerMetricsProvider,
		keyFunc:                         DeletionHandlingMetaNamespaceKeyFunc,
	}
}

// SharedIndexInformerOptions 配置 sharedIndexInformer。
type SharedIndexInformerOptions struct {
	// ResyncPeriod 是默认的事件处理器 resync 周期和 resync 检查周期。
	// 未设置/未指定时默认为 0（不 resync）。
	ResyncPeriod time.Duration

	// Indexers 是 sharedIndexInformer 的索引器。未设置/未指定时不配置任何索引器。
	Indexers Indexers

	// ObjectDescription 是 sharedIndexInformer 所处理对象的描述，会透传给底层
	// Reflector 的类型描述。
	ObjectDescription string

	// Identifier 用于在指标和日志中标识该 FIFO。未设置时不发布指标。
	Identifier InformerNameAndResource

	// InformerMetricsProvider 是 FIFO 队列的指标提供者。未设置时指标为空操作。
	InformerMetricsProvider InformerMetricsProvider
}

// InformerSynced 是用于判断 informer 是否已同步的函数。它对判断缓存是否已同步很有用。
type InformerSynced func() bool

const (
	// syncedPollPeriod 控制检查 sync 函数状态的频率
	syncedPollPeriod = 100 * time.Millisecond

	// initialBufferSize 是可缓冲的事件通知的初始数量。
	initialBufferSize = 1024
)

// WaitForNamedCacheSync 是 WaitForCacheSync 的包装，会生成日志消息，表明由
// name 标识的调用方正在等待同步，随后记录同步成功或失败。
//
// 上下文日志：支持上下文日志的代码应改用 WaitForNamedCacheSyncWithContext。
func WaitForNamedCacheSync(controllerName string, stopCh <-chan struct{}, cacheSyncs ...InformerSynced) bool {
	klog.Background().Info("Waiting for caches to sync", "controller", controllerName)

	if !WaitForCacheSync(stopCh, cacheSyncs...) {
		utilruntime.HandleErrorWithContext(context.Background(), nil, "Unable to sync caches", "controller", controllerName)
		return false
	}

	klog.Background().Info("Caches are synced", "controller", controllerName)
	return true
}

// WaitForNamedCacheSyncWithContext 是 WaitForCacheSyncWithContext 的包装，
// 会生成日志消息表明调用方正在等待同步，随后记录同步成功或失败。
//
// 可以用上下文日志在这些日志消息中标识调用方。日志级别为零，与
// [WaitForNamedCacheSync] 相同。如果这太啰嗦，可以在 context 中存放一个
// 提高了阈值的 logger：
//
//	WaitForNamedCacheSyncWithContext(klog.NewContext(ctx, logger.V(5)), ...)
func WaitForNamedCacheSyncWithContext(ctx context.Context, cacheSyncs ...InformerSynced) bool {
	logger := klog.FromContext(ctx)
	logger.Info("Waiting for caches to sync")

	if !WaitForCacheSync(ctx.Done(), cacheSyncs...) {
		utilruntime.HandleErrorWithContext(ctx, nil, "Unable to sync caches")
		return false
	}

	logger.Info("Caches are synced")
	return true
}

// WaitForCacheSync 等待缓存完成填充。成功返回 true；控制器应关闭时返回 false。
// 调用方应优先使用 WaitForNamedCacheSync()
//
// 采用轮询而非事件通知，实现简单但存在最多 syncedPollPeriod 的延迟；
// 新代码可考虑基于 DoneChecker 的 WaitFor。
func WaitForCacheSync(stopCh <-chan struct{}, cacheSyncs ...InformerSynced) bool {
	err := wait.PollImmediateUntil(syncedPollPeriod,
		func() (bool, error) {
			for _, syncFunc := range cacheSyncs {
				if !syncFunc() {
					return false, nil
				}
			}
			return true, nil
		},
		stopCh)
	if err != nil {
		return false
	}

	return true
}

// WaitFor 等待一组活动完成，例如缓存同步。
// 全部活动完成返回 true；在此之前 context 被取消则返回 false。
//
// 若提供了非空的 "what"，等待期间会记录进度信息（"Waiting",  for="<what>"）。
// 日志级别为 V(0)。调用方可以用下面的方式降低详细程度：
//
//	WaitFor(klog.NewContext(ctx, klog.FromContext(ctx).V(2)), ...)
//
// 与其他 WaitForCacheSync 替代方案不同，这里直接等待 DoneChecker 的 channel，
// 不需要轮询，因而能立即响应；在 synctest 单元测试中等待完成不会随机推进
// 时间，使测试更可预测。
func WaitFor(ctx context.Context, what string, checkers ...DoneChecker) bool {
	logger := klog.FromContext(ctx)
	if what != "" {
		helper, l := logger.WithCallStackHelper()
		logger = l
		helper()
		logger.Info("Waiting", "for", what)
	}

	// 并行检查，确保每个 checker 完成后能尽快记录 "Done waiting"，
	// 这个时间点是有用信息。不能在 goroutine 内部记日志——调用栈展开
	// 会失效——所以每个 goroutine 只在自己的 checker 完成时通知父
	// goroutine，由主 goroutine 记录日志。
	//
	// 每个 checker 一个 goroutine，结果经无缓冲 channel 回传；
	// 主 goroutine 收满 len(checkers) 个结果即返回，不会被单个慢 checker 阻塞。
	var wg sync.WaitGroup
	type result struct {
		checker DoneChecker
		done    bool
	}
	doneChecker := make(chan result)
	for _, checker := range checkers {
		wg.Go(func() {
			select {
			case <-checker.Done():
				doneChecker <- result{checker, true}
			case <-ctx.Done():
				// 可能即使 checker 已经完成也会走到这里，因为 select 不是
				// 确定性的。在彻底放弃前再做一次非阻塞检查。
				select {
				case <-checker.Done():
					doneChecker <- result{checker, true}
				default:
					doneChecker <- result{checker, false}
				}
			}
		})
	}
	instances := make([]string, 0, len(checkers))
	for range len(checkers) {
		// 保证每个 goroutine 恰好返回一个结果，所以这里不会永久阻塞。
		result := <-doneChecker
		if result.done {
			if what != "" {
				logger.Info("Done waiting", "for", what, "instance", result.checker.Name())
			}
		} else {
			// 除非我们要 a) 记日志或 b) 交互式调试，否则不需要这些信息。
			instances = append(instances, result.checker.Name())
		}
	}
	wg.Wait()

	if what != "" && len(instances) > 0 {
		slices.Sort(instances)
		logger.Info("Timed out waiting", "for", what, "cause", context.Cause(ctx), "instances", instances)
	}

	done := len(instances) == 0
	return done
}

// DoneChecker 与 [InformerSynced] 不同，支持在不轮询的情况下等待某个活动
// 结束，并且有一个描述自身的名称。
//
// 如需非阻塞地检查是否完成，请使用 [IsDone]。
type DoneChecker interface {
	// Name 返回描述被等待对象的字符串。
	//
	// 注意：名称可能是计算得到的，调用方应只在非热点代码路径中获取名称。
	Name() string

	// Done 返回在活动完成时被关闭的 channel。
	Done() <-chan struct{}
}

// IsDone 在活动完成时返回 true，否则返回 false。
func IsDone(checker DoneChecker) bool {
	select {
	case <-checker.Done():
		return true
	default:
		return false
	}
}

// SyncResult 是共享 informer 工厂的 WaitForCacheSyncWithContext 的结果。
// 这类工厂底层使用 [WaitFor] 等待所有已实例化的 informer，然后给出这份
// 已同步情况的摘要。
//
// 注意：informer 可能先于注册到它们的所有事件处理器完成同步。若代码要确保
// 自身状态全部是最新的，应自行用 informer 的 HasSyncedChecker() *和* 注册
// 句柄的 HasSyncChecker() 结果做 WaitFor。
type SyncResult struct {
	// Err 在所有 informer 缓存都同步时为 nil；否则是停止等待缓存同步的原因
	// （即 context.Cause(ctx)）。
	Err error

	// Synced 将 SharedInformerFactory 中每个已注册的 informer 映射到其
	// 是否已同步。
	Synced map[reflect.Type]bool
}

// AsError 在并非所有缓存都同步时把 SyncResult 转成错误，否则返回 nil。
// 错误会包装 context.Cause(ctx)，并包含未同步 informer 的信息。
func (c SyncResult) AsError() error {
	if c.Err == nil {
		return nil
	}

	unsynced := make([]string, 0, len(c.Synced))
	for t, synced := range c.Synced {
		if !synced {
			unsynced = append(unsynced, t.String())
		}
	}
	slices.Sort(unsynced)

	return fmt.Errorf("failed to sync all caches: %s: %w", strings.Join(unsynced, ", "), c.Err)
}

// `*sharedIndexInformer` 实现 SharedIndexInformer，有三个主要组件。其一是
// 带索引的本地缓存 `indexer Indexer`。其二是 Controller：通过 ListerWatcher
// 拉取对象/通知并推入 DeltaFIFO——其 knownObjects 就是 informer 的本地
// 缓存——同时并发地从该 fifo 弹出 Delta，并用 `sharedIndexInformer::HandleDeltas`
// 处理。每次 HandleDeltas 调用（在持有 fifo 锁的情况下进行）会依次处理每个
// Delta：对每个 Delta，既更新本地缓存，也把相应通知塞给 sharedProcessor。
// 其三是 sharedProcessor，负责把这些通知转发给 informer 的各个客户端。
type sharedIndexInformer struct {
	// indexer 是本地缓存兼索引，DeltaFIFO 的 knownObjects 也指向它。
	indexer    Indexer
	controller Controller

	// synced 在创建 sharedIndexInformer 时创建，当 Run 检测到 processor
	// 已创建后被关闭。
	synced chan struct{}

	processor             *sharedProcessor
	cacheMutationDetector MutationDetector

	listerWatcher ListerWatcher

	// objectType 是本 informer 预期处理的类型的示例对象。若设置，对象类型不匹配的
	// 事件会被丢弃而不会投递给监听器。
	//
	// 同时用于类型化 API 的 T 断言、日志中的类型描述以及缓存变更检测器的命名。
	objectType runtime.Object
	// objectDescription 是本 informer 所处理对象的描述，通常会给出默认值。
	objectDescription string

	// resyncCheckPeriod 是希望 reflector 的 resync 定时器触发的频率，
	// 以便调用 shouldResync 检查是否有监听器需要 resync。
	resyncCheckPeriod time.Duration
	// defaultEventHandlerResyncPeriod 是通过 AddEventHandler 添加的处理器使用的
	// 默认 resync 周期（即调用方不指定、直接使用共享 informer 默认值的情况）。
	defaultEventHandlerResyncPeriod time.Duration
	// clock 便于测试
	clock clock.Clock

	started, stopped bool
	startedLock      sync.Mutex

	// blockDeltas 提供了一种停止所有事件分发的方式，使迟到的事件处理器能安全地
	// 加入共享 informer。
	//
	// 添加/移除处理器时必须持有它：先挡住分发，再把缓存快照作为初始 Add 注入新处理器，
	// 避免处理器在“已同步”和“收到初始列表”之间看到不一致状态。
	blockDeltas sync.Mutex

	// 每当 ListAndWatch 因错误断开连接时被调用。
	watchErrorHandler WatchErrorHandlerWithContext

	transform TransformFunc

	// identifier 用于在指标和日志中标识本 informer。
	identifier InformerNameAndResource

	// informerMetricsProvider 是 FIFO 队列的指标提供者。
	informerMetricsProvider InformerMetricsProvider

	// keyFunc 由底层处理函数在处理 delta 时被调用。
	keyFunc KeyFunc
}

// dummyController 掩盖了 SharedInformer 与专用 informer 的差异：后者调用方
// 可以直接 `Run`。这里 Run 方法是空实现，因为更上层的逻辑决定何时启动
// SharedInformer 及其关联的 controller。由于返回信息始终是异步的，
// 旧调用方不会察觉行为变化。
type dummyController struct {
	informer *sharedIndexInformer
}

func (v *dummyController) RunWithContext(context.Context) {
}

func (v *dummyController) Run(stopCh <-chan struct{}) {
}

func (v *dummyController) HasSynced() bool {
	return v.informer.HasSynced()
}

func (v *dummyController) HasSyncedChecker() DoneChecker {
	return v.informer.HasSyncedChecker()
}

func (v *dummyController) LastSyncResourceVersion() string {
	if clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.InformerResourceVersion) {
		return v.informer.LastSyncResourceVersion()
	}

	return ""
}

type updateNotification struct {
	oldObj interface{}
	newObj interface{}
}

type addNotification struct {
	newObj          interface{}
	isInInitialList bool
}

type deleteNotification struct {
	oldObj interface{}
}

func (s *sharedIndexInformer) SetWatchErrorHandler(handler WatchErrorHandler) error {
	return s.SetWatchErrorHandlerWithContext(func(_ context.Context, r *Reflector, err error) {
		handler(r, err)
	})
}

func (s *sharedIndexInformer) SetWatchErrorHandlerWithContext(handler WatchErrorHandlerWithContext) error {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.started {
		return fmt.Errorf("informer has already started")
	}

	s.watchErrorHandler = handler
	return nil
}

func (s *sharedIndexInformer) SetTransform(handler TransformFunc) error {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.started {
		return fmt.Errorf("informer has already started")
	}

	s.transform = handler
	return nil
}

func (s *sharedIndexInformer) Run(stopCh <-chan struct{}) {
	s.RunWithContext(wait.ContextForChannel(stopCh))
}

func (s *sharedIndexInformer) RunWithContext(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)
	logger := klog.FromContext(ctx)

	// informer 只能运行一次：重复 Run 会创建多个 controller 与消费者，
	// 导致同一事件被处理多次，因此这里直接忽略第二次调用。
	if s.HasStarted() {
		logger.Info("Warning: the sharedIndexInformer has started, run more than once is not allowed")
		return
	}

	func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()

		// 在持锁状态下构建 FIFO 与 controller，保证 started 标志与它们同时可见。
		logger, fifo := newQueueFIFO(logger, s.objectType, s.indexer, s.transform, s.identifier, s.informerMetricsProvider)

		cfg := &Config{
			Queue:             fifo,
			ListerWatcher:     s.listerWatcher,
			ObjectType:        s.objectType,
			ObjectDescription: s.objectDescription,
			FullResyncPeriod:  s.resyncCheckPeriod,
			ShouldResync:      s.processor.shouldResync,

			Process: func(obj interface{}, isInInitialList bool) error {
				return s.handleDeltas(logger, obj, isInInitialList)
			},
			ProcessBatch: func(deltas []Delta, isInInitialList bool) error {
				return s.handleBatchDeltas(logger, deltas, isInInitialList)
			},
			WatchErrorHandlerWithContext: s.watchErrorHandler,
		}

		s.controller = New(cfg)
		s.controller.(*controller).clock = s.clock
		s.started = true
	}()

	// 使用独立的 stop context，因为 Processor 必须严格晚于 controller 停止。
	// 父 context 的取消会被忽略，但所有值都会传递下去，包括但不限于 logger。
	//
	// 独立 context 使延迟调用的顺序可控：controller 返回后先通知 processor 停止，
	// 再等待 processor 退出，避免控制器还在分发时处理器已被拆掉。
	processorStopCtx, stopProcessor := context.WithCancelCause(context.WithoutCancel(ctx))
	var wg wait.Group
	defer wg.Wait()                                         // 等待 Processor 停止
	defer stopProcessor(errors.New("informer is stopping")) // 通知 Processor 停止
	// TODO: 扩展 MutationDetector 接口，使其可选地提供可供此处使用的
	// RunWithContext 方法。
	wg.StartWithChannel(processorStopCtx.Done(), s.cacheMutationDetector.Run)
	wg.StartWithContext(processorStopCtx, s.processor.run)
	wg.Start(func() {
		select {
		case <-ctx.Done():
			// 在未同步完成时就被停止了。
		case <-s.controller.HasSyncedChecker().Done():
			// Controller 已同步，因此我们也已同步。
			//
			// 只有 controller 完成首次同步才关闭 synced，缓存此时才可被安全读取。
			close(s.synced)
		}
	})

	defer func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()
		s.stopped = true // 不再接受新的监听器
	}()
	s.controller.RunWithContext(ctx)
}

func (s *sharedIndexInformer) HasStarted() bool {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()
	return s.started
}

func (s *sharedIndexInformer) HasSynced() bool {
	select {
	case <-s.synced:
		return true
	default:
		return false
	}
}

func (s *sharedIndexInformer) HasSyncedChecker() DoneChecker {
	return &sharedIndexInformerDone{
		s: s,
	}
}

// sharedIndexInformerDone 为 [sharedIndexInformer] 实现 [NamedCacheSync]。
type sharedIndexInformerDone struct {
	s *sharedIndexInformer
}

func (sd *sharedIndexInformerDone) Name() string {
	return fmt.Sprintf("SharedIndexInformer %T", sd.s.objectType)
}

func (sd *sharedIndexInformerDone) Done() <-chan struct{} {
	return sd.s.synced
}

func (s *sharedIndexInformer) LastSyncResourceVersion() string {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.controller == nil {
		return ""
	}
	return s.controller.LastSyncResourceVersion()
}

func (s *sharedIndexInformer) GetStore() Store {
	return s.indexer
}

func (s *sharedIndexInformer) GetIndexer() Indexer {
	return s.indexer
}

func (s *sharedIndexInformer) AddIndexers(indexers Indexers) error {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.stopped {
		return fmt.Errorf("indexer was not added because it has stopped already")
	}

	return s.indexer.AddIndexers(indexers)
}

func (s *sharedIndexInformer) GetController() Controller {
	return &dummyController{informer: s}
}

func (s *sharedIndexInformer) AddEventHandler(handler ResourceEventHandler) (ResourceEventHandlerRegistration, error) {
	return s.AddEventHandlerWithOptions(handler, HandlerOptions{})
}

func determineResyncPeriod(logger klog.Logger, desired, check time.Duration) time.Duration {
	if desired == 0 {
		return desired
	}
	if check == 0 {
		logger.Info("Warning: the specified resyncPeriod is invalid because this shared informer doesn't support resyncing", "desired", desired)
		return 0
	}
	if desired < check {
		logger.Info("Warning: the specified resyncPeriod is being increased to the minimum resyncCheckPeriod", "desired", desired, "resyncCheckPeriod", check)
		return check
	}
	return desired
}

const minimumResyncPeriod = 1 * time.Second

func (s *sharedIndexInformer) AddEventHandlerWithResyncPeriod(handler ResourceEventHandler, resyncPeriod time.Duration) (ResourceEventHandlerRegistration, error) {
	return s.AddEventHandlerWithOptions(handler, HandlerOptions{ResyncPeriod: &resyncPeriod})
}

func (s *sharedIndexInformer) AddEventHandlerWithOptions(handler ResourceEventHandler, options HandlerOptions) (ResourceEventHandlerRegistration, error) {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.stopped {
		return nil, fmt.Errorf("handler %v was not added to shared informer because it has stopped already", handler)
	}

	logger := ptr.Deref(options.Logger, klog.Background())
	resyncPeriod := ptr.Deref(options.ResyncPeriod, s.defaultEventHandlerResyncPeriod)
	if resyncPeriod > 0 {
		if resyncPeriod < minimumResyncPeriod {
			logger.Info("Warning: resync period is too small. Changing it to the minimum allowed value", "resyncPeriod", resyncPeriod, "minimumResyncPeriod", minimumResyncPeriod)
			resyncPeriod = minimumResyncPeriod
		}

		if resyncPeriod < s.resyncCheckPeriod {
			if s.started {
				logger.Info("Warning: resync period is smaller than resync check period and the informer has already started. Changing it to the resync check period", "resyncPeriod", resyncPeriod, "resyncCheckPeriod", s.resyncCheckPeriod)

				resyncPeriod = s.resyncCheckPeriod
			} else {
				// 若事件处理器的 resyncPeriod 小于当前的 resyncCheckPeriod，则把
				// resyncCheckPeriod 更新为与 resyncPeriod 一致，并相应调整所有监听器
				// 的 resync 周期
				s.resyncCheckPeriod = resyncPeriod
				s.processor.resyncCheckPeriodChanged(logger, resyncPeriod)
			}
		}
	}

	listener := newProcessListener(logger, handler, resyncPeriod, determineResyncPeriod(logger, resyncPeriod, s.resyncCheckPeriod), s.clock.Now(), initialBufferSize, s.HasSyncedChecker())

	if !s.started {
		handle, _ := s.processor.addListener(listener)
		return handle, nil
	}

	// 为了安全接入，必须：
	// 1. 停止发送 add/update/delete 通知
	// 2. 对 store 做一次 list
	// 3. 向新处理器发送合成的 "Add" 事件
	// 4. 解除阻塞
	//
	// blockDeltas 保证第 1~4 步之间没有并发分发，否则处理器可能先收到增量事件、
	// 再收到初始列表，导致状态错乱。
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()

	handle, started := s.processor.addListener(listener)
	for _, item := range s.indexer.List() {
		// 注意：我们在持有锁时入队这些通知，且在返回句柄之前完成。因此不存在
		// 任何时机让调用方的 HasSynced 方法错误地返回 true（即共享 informer
		// 已同步、但句柄尚未观察到 isInInitialList 为 true 的 Add，或处理通知
		// 的线程比本线程添加通知更快、计数暂时为零的情况）。
		listener.add(addNotification{newObj: item, isInInitialList: true})
	}

	// 初始列表已添加，现在可以允许监听器感知"上游已同步"。
	if started {
		s.processor.wg.Start(listener.watchSynced)
	}

	return handle, nil
}

func (s *sharedIndexInformer) handleDeltas(logger klog.Logger, obj interface{}, isInInitialList bool) error {
	// 与添加处理器互斥：确保处理某个对象的 delta 时，不会有监听器正在做初始列表快照。
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()

	if deltas, ok := obj.(Deltas); ok {
		return processDeltas(logger, s, s.indexer, deltas, isInInitialList, s.keyFunc)
	}
	return errors.New("object given as Process argument is not Deltas")
}

func (s *sharedIndexInformer) handleBatchDeltas(logger klog.Logger, deltas []Delta, isInInitialList bool) error {
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()
	return processDeltasInBatch(logger, s, s.indexer, deltas, isInInitialList, s.keyFunc)
}

// OnAdd 符合 ResourceEventHandler 接口。
func (s *sharedIndexInformer) OnAdd(obj interface{}, isInInitialList bool) {
	// 本函数的调用在 s.blockDeltas 锁保护下进行，因此分发通知是安全的。
	s.cacheMutationDetector.AddObject(obj)
	s.processor.distribute(addNotification{newObj: obj, isInInitialList: isInInitialList}, false)
}

// OnUpdate 符合 ResourceEventHandler 接口。
func (s *sharedIndexInformer) OnUpdate(old, new interface{}) {
	isSync := false

	// 如果是 Sync 事件，isSync 应为 true。
	// 如果是 Replaced 事件，当 resource version 未变化时 isSync 为 true。
	// RV 未变化：这是 Sync/Replaced 事件，因此 isSync 为 true。

	if accessor, err := meta.Accessor(new); err == nil {
		if oldAccessor, err := meta.Accessor(old); err == nil {
			// 未改变 resourceVersion 的事件被视为 resync 事件，
			// 只传播给请求了 resync 的监听器
			isSync = accessor.GetResourceVersion() == oldAccessor.GetResourceVersion()
		}
	}

	// 本函数的调用在 s.blockDeltas 锁保护下进行，因此分发通知是安全的。
	s.cacheMutationDetector.AddObject(new)
	s.processor.distribute(updateNotification{oldObj: old, newObj: new}, isSync)
}

// OnDelete 符合 ResourceEventHandler 接口。
func (s *sharedIndexInformer) OnDelete(old interface{}) {
	// 本函数的调用在 s.blockDeltas 锁保护下进行，因此分发通知是安全的。
	s.processor.distribute(deleteNotification{oldObj: old}, false)
}

// IsStopped 报告 informer 是否已经停止。
func (s *sharedIndexInformer) IsStopped() bool {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()
	return s.stopped
}

func (s *sharedIndexInformer) RemoveEventHandler(handle ResourceEventHandlerRegistration) error {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	// 为了安全移除，必须：
	// 1. 停止发送 add/update/delete 通知
	// 2. 移除并停止监听器
	// 3. 解除阻塞
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()
	return s.processor.removeListener(handle)
}

// ShutDownEventHandler 移除事件处理器并阻塞，直到它完全停止处理事件。
//
// 与 RemoveEventHandler 一样，它是幂等且线程安全的。但是，绝不能从事件
// 处理器自身的回调内部调用它，否则会导致死锁。
func ShutDownEventHandler(informer SharedInformer, handle ResourceEventHandlerRegistration) error {
	if err := informer.RemoveEventHandler(handle); err != nil {
		return err
	}
	if s, ok := handle.(interface{ ShutdownChan() <-chan struct{} }); ok {
		<-s.ShutdownChan()
	} else {
		return fmt.Errorf("handle does not support ShutdownChan()")
	}
	return nil
}

// sharedProcessor 持有一组 processorListener，可以把通知对象分发给它的
// 监听器。分发操作有两种：同步分发只发给一部分监听器——这个子集（a）在
// 不定期的 shouldResync 调用中重新计算，（b）每个监听器最初都在其中；
// 非同步分发给每个监听器。
//
// listeners map 的 value 表示该监听器本轮是否需要 resync，
// 由 shouldResync 周期性重算；distribute 据此过滤同步事件。
type sharedProcessor struct {
	listenersStarted bool
	listenersLock    sync.RWMutex
	listenersRCond   *sync.Cond // 调用 Wait 前必须持有 listenersLock 的读锁。
	// 监听器到其当前是否在同步的映射
	listeners map[*processorListener]bool
	clock     clock.Clock
	wg        wait.Group
}

func (p *sharedProcessor) getListener(registration ResourceEventHandlerRegistration) *processorListener {
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()

	if p.listeners == nil {
		return nil
	}

	if result, ok := registration.(*processorListener); ok {
		if _, exists := p.listeners[result]; exists {
			return result
		}
	}

	return nil
}

func (p *sharedProcessor) addListener(listener *processorListener) (ResourceEventHandlerRegistration, bool) {
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	if p.listeners == nil {
		p.listeners = make(map[*processorListener]bool)
	}

	p.listeners[listener] = true

	if p.listenersStarted {
		// 不会启动 listener.watchSynced！调用方必须先添加初始列表，再启动它。
		p.wg.Start(listener.run)
		p.wg.Start(listener.pop)
	}

	return listener, p.listenersStarted
}

func (p *sharedProcessor) removeListener(handle ResourceEventHandlerRegistration) error {
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	listener, ok := handle.(*processorListener)
	if !ok {
		return fmt.Errorf("invalid key type %t", handle)
	} else if p.listeners == nil {
		// 没有注册任何监听器，什么都不做
		return nil
	} else if _, exists := p.listeners[listener]; !exists {
		// 监听器未注册，什么都不做
		return nil
	}

	delete(p.listeners, listener)

	if p.listenersStarted {
		close(listener.addCh)
	} else {
		close(listener.runFinished)
	}

	return nil
}

func (p *sharedProcessor) distribute(obj interface{}, sync bool) {
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()

	// 在阻塞式写入各监听器的 channel 之前，确保它们都已启动。如果 processor
	// 已停止，p.listeners 会被清空，此时同样继续往下走并直接返回、什么都不做。
	//
	// 用读锁 + Cond 等待，避免在监听器 goroutine 尚未启动时向其 channel 写入而永久阻塞。
	for !p.listenersStarted && len(p.listeners) > 0 {
		p.listenersRCond.Wait()
	}

	for listener, isSyncing := range p.listeners {
		switch {
		case !sync:
			// 非同步消息会投递给每个监听器
			listener.add(obj)
		case isSyncing:
			// 同步消息投递给每个正在同步的监听器
			listener.add(obj)
		default:
			// 跳过非同步监听器的同步对象
		}
	}
}

// sharedProcessorRunHook 可在测试中用于在 sharedProcessor.run 开始时执行额外代码。
var sharedProcessorRunHook atomic.Pointer[func()]

func (p *sharedProcessor) run(ctx context.Context) {
	func() {
		hook := sharedProcessorRunHook.Load()
		if hook != nil {
			(*hook)()
		}
		// 修改 listenersStarted 需要写锁。
		p.listenersLock.Lock()
		defer p.listenersLock.Unlock()
		for listener := range p.listeners {
			p.wg.Start(listener.watchSynced)
			p.wg.Start(listener.run)
			p.wg.Start(listener.pop)
		}
		p.listenersStarted = true
		p.listenersRCond.Signal()
	}()
	<-ctx.Done()

	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()
	for listener := range p.listeners {
		close(listener.addCh) // 通知 .pop() 停止；.pop() 会再通知 .run() 停止
	}

	// 清空监听器列表，因为它们都已关闭
	// （processorListener 不能复用）
	p.listeners = nil

	// 重置为 false，因为没有监听器在运行
	p.listenersStarted = false

	// 唤醒 sharedProcessor.distribute。
	p.listenersRCond.Signal()

	p.wg.Wait() // 等待所有 .pop() 和 .run() 停止
}

// shouldResync 询问每个监听器是否需要 resync，依据是每个监听器的 resyncPeriod。
func (p *sharedProcessor) shouldResync() bool {
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	resyncNeeded := false
	now := p.clock.Now()
	for listener := range p.listeners {
		// 需要遍历所有监听器看它们是否需要 resync，这样才能为将要 resync 的
		// 监听器做好准备。
		shouldResync := listener.shouldResync(now)
		p.listeners[listener] = shouldResync

		if shouldResync {
			resyncNeeded = true
			listener.determineNextResync(now)
		}
	}
	return resyncNeeded
}

func (p *sharedProcessor) resyncCheckPeriodChanged(logger klog.Logger, resyncCheckPeriod time.Duration) {
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()

	for listener := range p.listeners {
		resyncPeriod := determineResyncPeriod(
			logger, listener.requestedResyncPeriod, resyncCheckPeriod)
		listener.setResyncPeriod(resyncPeriod)
	}
}

// processorListener 把通知从 sharedProcessor 转发给一个 ResourceEventHandler
// ——使用三个 goroutine、两个无缓冲 channel 和一个无界环形缓冲区。
// `add(notification)` 函数把给定通知发送到 `addCh`。一个 goroutine 运行
// `pop()`，在 `nextCh` 跟不上时利用环形缓冲区的存储把通知从 `addCh` 抽到
// `nextCh`。另一个 goroutine 运行 `run()`，从 `nextCh` 接收通知并同步调用
// 相应的处理器方法。
//
// 第三个 goroutine 监视上游"已同步"的 channel，并通知 SingleFileTracker
// 实例。该实例结合上游状态和 processorListener 的状态，实现整体的
// "事件处理器已同步"。
//
// processorListener 还负责跟踪监听器调整后的请求 resync 周期。
//
// 三个 goroutine 的分工保证了“向 addCh 写入永不阻塞分发方”，
// 代价是慢处理器会让 ring buffer 持续增长（注释中明确接受这一取舍）。
type processorListener struct {
	logger      klog.Logger
	nextCh      chan interface{}
	addCh       chan interface{}
	done        chan struct{}
	runFinished chan struct{}

	handler     ResourceEventHandler
	handlerName string

	syncTracker       *synctrack.SingleFileTracker
	upstreamHasSynced DoneChecker

	// pendingNotifications 是无界环形缓冲区，存放所有尚未分发的通知。
	// 每个监听器一个，但失败/停滞的监听器会被无限追加通知直到 OOM。
	// TODO: 这并不比以前更糟，因为 reflector 背后本来就是无界的 DeltaFIFO，
	// 但我们应该尝试做得更好。
	pendingNotifications buffer.RingGrowing
	// pendingNotificationsLength 跟踪 pendingNotifications 的大小，只被 pop() 修改。
	// run() 读取它来决定何时启用开销较大的时间追踪。
	pendingNotificationsLength atomic.Int64

	// requestedResyncPeriod 是监听器希望从共享 informer 获得的全量 resync
	// 频率，但经过两处调整。一是设置下限 `minimumResyncPeriod`；二是另一个
	// 下限，即 sharedIndexInformer 的 `resyncCheckPeriod`，它只在（a）于
	// sharedIndexInformer 启动之后发起的 AddEventHandlerWithResyncPeriod
	// 调用中、且（b）informer 确实会做 resync 时才施加。
	requestedResyncPeriod time.Duration
	// resyncPeriod 是该监听器逻辑中实际使用的阈值。仅当 sharedIndexInformer
	// 不做 resync 时，该值才与 requestedResyncPeriod 不同，此时这里的值为零。
	// 实际两次 resync 的时间间隔取决于 sharedProcessor 的 `shouldResync`
	// 函数何时被调用，以及 sharedIndexInformer 何时处理 `Sync` 类型的 Delta 对象。
	resyncPeriod time.Duration
	// nextResync 是监听器应当获得全量 resync 的最早时间
	nextResync time.Time
	// resyncLock 保护对 resyncPeriod 和 nextResync 的访问
	resyncLock sync.Mutex
}

// HasSynced 在源 informer 已同步、且所有相应事件都已投递时返回 true。
func (p *processorListener) HasSynced() bool {
	return p.syncTracker.HasSynced()
}

// HasNamedSync 在源 informer 已同步、且所有相应事件都已投递时视为完成。
func (p *processorListener) HasSyncedChecker() DoneChecker {
	return p.syncTracker
}

func (p *processorListener) ShutdownChan() <-chan struct{} {
	return p.runFinished
}

func newProcessListener(logger klog.Logger, handler ResourceEventHandler, requestedResyncPeriod, resyncPeriod time.Duration, now time.Time, bufferSize int, hasSynced DoneChecker) *processorListener {
	handlerName := nameForHandler(handler)
	ret := &processorListener{
		logger:                logger,
		nextCh:                make(chan interface{}),
		addCh:                 make(chan interface{}),
		done:                  make(chan struct{}),
		runFinished:           make(chan struct{}),
		upstreamHasSynced:     hasSynced,
		handler:               handler,
		handlerName:           handlerName,
		syncTracker:           synctrack.NewSingleFileTracker(fmt.Sprintf("%s + event handler %s", hasSynced.Name(), handlerName)),
		pendingNotifications:  *buffer.NewRingGrowing(bufferSize),
		requestedResyncPeriod: requestedResyncPeriod,
		resyncPeriod:          resyncPeriod,
	}

	ret.determineNextResync(now)

	return ret
}

func (p *processorListener) add(notification interface{}) {
	if a, ok := notification.(addNotification); ok && a.isInInitialList {
		p.syncTracker.Start()
	}
	p.addCh <- notification
}

func (p *processorListener) pop() {
	defer utilruntime.HandleCrashWithLogger(p.logger)
	defer close(p.nextCh) // Tell .run() to stop
	defer close(p.done)   // Tell .watchSynced() to stop

	// nextCh 初始为 nil 时该 select 分支被禁用，直到有通知可发送。
	var nextCh chan<- interface{}
	var notification interface{}
	for {
		select {
		case nextCh <- notification:
			// 通知已分发
			var ok bool
			notification, ok = p.pendingNotifications.ReadOne()
			if ok {
				p.pendingNotificationsLength.Add(-1)
			} else { // 没有可弹出的了
				nextCh = nil // 禁用该 select 分支
			}
		case notificationToAdd, ok := <-p.addCh:
			if !ok {
				return
			}
			// 没有待发送通知时直接交接，省去一次 ring buffer 读写。
			if notification == nil { // 没有待弹出的通知（且 pendingNotifications 为空）
				// 优化该情形——跳过写入 pendingNotifications
				notification = notificationToAdd
				nextCh = p.nextCh
			} else { // 已有一条通知在等待分发
				p.pendingNotifications.WriteOne(notificationToAdd)
				p.pendingNotificationsLength.Add(1)
			}
		}
	}
}

func (p *processorListener) run() {
	defer close(p.runFinished)
	// 该调用会阻塞直到 channel 关闭。若通知过程中发生 panic，我们会捕获它，
	// **出问题的那条通知会被跳过！**，短暂延迟（一秒）后尝试下一条通知。
	// 这通常好于永远不再投递。
	//
	// 仅在 utilruntime 被配置为不 panic 时适用，这不是默认行为。
	//
	// 若处理器 panic 且 runtime 未选择崩溃，跳过当前通知并退避一秒，
	// 防止同一个坏对象把整个监听器卡死。
	sleepAfterCrash := false
	for next := range p.nextCh {
		if sleepAfterCrash {
			// 在处理下一项之前休眠。
			time.Sleep(time.Second)
		}
		func() {
			// 下面会被重置，但只有走到那一步才会。
			sleepAfterCrash = true
			defer utilruntime.HandleCrashWithLogger(p.logger)
			pendingNotifications := p.pendingNotificationsLength.Load()
			if pendingNotifications > initialBufferSize {
				trace := utiltrace.New("processorListener handler",
					utiltrace.Field{Key: "handler", Value: p.handlerName},
					utiltrace.Field{Key: "pendingNotifications", Value: pendingNotifications},
				)
				defer trace.LogIfLong(100 * time.Millisecond)
			}

			switch notification := next.(type) {
			case updateNotification:
				p.handler.OnUpdate(notification.oldObj, notification.newObj)
			case addNotification:
				p.handler.OnAdd(notification.newObj, notification.isInInitialList)
				if notification.isInInitialList {
					p.syncTracker.Finished()
				}
			case deleteNotification:
				p.handler.OnDelete(notification.oldObj)
			default:
				utilruntime.HandleErrorWithLogger(p.logger, nil, "unrecognized notification", "notificationType", fmt.Sprintf("%T", next))
			}
			sleepAfterCrash = false
		}()
	}
}

func (p *processorListener) watchSynced() {
	select {
	case <-p.upstreamHasSynced.Done():
		// 通知 tracker：上游已同步。
		p.syncTracker.UpstreamHasSynced()
	case <-p.done:
		// 放弃等待同步。
	}
}

// shouldResync 判断监听器是否需要 resync。若监听器的 resyncPeriod 为 0，
// 总是返回 false。
func (p *processorListener) shouldResync(now time.Time) bool {
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	if p.resyncPeriod == 0 {
		return false
	}

	return now.After(p.nextResync) || now.Equal(p.nextResync)
}

func (p *processorListener) determineNextResync(now time.Time) {
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	p.nextResync = now.Add(p.resyncPeriod)
}

func (p *processorListener) setResyncPeriod(resyncPeriod time.Duration) {
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	p.resyncPeriod = resyncPeriod
}
