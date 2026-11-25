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
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/naming"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/client-go/tools/pager"
	"k8s.io/client-go/util/watchlist"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	"k8s.io/utils/trace"
)

const defaultExpectedTypeName = "<unspecified>"

var (
	// 通过为 watch 请求设置超时来分摊 apiserver 的负载——超时值在
	// [minWatchTimeout, 2*minWatchTimeout] 内随机。
	//
	// watch 超时在 [min, max] 内随机，避免大量 reflector 同时到期造成惊群。
	defaultMinWatchTimeout = 5 * time.Minute
	defaultMaxWatchTimeout = 2 * defaultMinWatchTimeout
	// 过去每秒调用一次（1 QPS），这里的目标是在 API server 不健康时减少约 98% 的流量。
	// 使用这些参数，退避会停在 [30,60) 秒区间，即 0.22 QPS。
	//
	// 指数退避参数：起始 800ms、上限 30s，用于 apiserver 不健康时大幅降低重试流量。
	defaultBackoffInit = 800 * time.Millisecond
	defaultBackoffMax  = 30 * time.Second
	// 若 2 分钟内没有触发退避，认为 API server 健康，重置退避。
	defaultBackoffReset  = 2 * time.Minute
	defaultBackoffFactor = 2.0
	defaultBackoffJitter = 1.0
)

// ReflectorStore 是 reflector 用到的 cache.Store 子集。
//
// ReflectorStore 是 reflector 对 store 的最小接口要求；DeltaFIFO、Indexer 等均可满足。
// 使用最小接口便于在测试中用简单实现替换。
type ReflectorStore interface {
	// Add 把给定对象添加到与该对象 key 关联的累积器中。
	Add(obj interface{}) error

	// Update 用给定对象更新其 key 关联累积器中的记录。
	Update(obj interface{}) error

	// Delete 从给定对象 key 关联的累积器中删除该对象。
	Delete(obj interface{}) error

	// Replace 会删除 store 的内容，改用给定的列表。Store 接管该列表的所有权，
	// 调用本函数后不应再引用它。
	Replace([]interface{}, string) error

	// Resync 在此处的语义中没有意义，但在某些有额外复杂行为的实现
	// （如 DeltaFIFO）中有含义。
	Resync() error
}

// ReflectorBookmarkStore 是一个可选接口，允许 store 获知 reflector 收到的
// bookmark 事件。
//
// ReflectorBookmarkStore 是可选接口：若 store 实现它，reflector 会把 bookmark 的
// resourceVersion 转发给 store，使 store 能在无实际变更时也推进 RV。
type ReflectorBookmarkStore interface {
	Bookmark(resourceVersion string) error
}

// TransformingStore 是所提供的 store 可以实现的可选接口。
// 若所提供的 store 实现了它，reflector 会在其内部 store 中使用相同的 transformer。
//
// TransformingStore 允许 store 声明其 TransformFunc，reflector 在内部临时 store
// （如 watch-list 的临时存储）中复用同一转换，保证处理语义一致。
type TransformingStore interface {
	ReflectorStore
	Transformer() TransformFunc
}

// Reflector 监听指定资源，并使其所有变更都反映到给定的 store 中。
//
// Reflector 是客户端缓存的“拉取引擎”：先 List 全量填充 store，再 Watch 增量更新，
// 断线后按 resourceVersion 续watch，遇到 RV 过期则重新 List。
type Reflector struct {
	logger klog.Logger
	// name 标识本 reflector。默认情况下尽可能取 file:line。
	name string
	// 预期放入 store 的类型名称。若提供了 expectedGVK 则用其字符串形式，
	// 否则用 expectedType 的字符串形式。仅用于展示，
	// 不应被用于解析或比较。
	typeDescription string
	// 预期放入 store 的类型的示例对象。只要求类型正确；唯一的例外是当类型为
	// `unstructured.Unstructured` 时，对象的 `"apiVersion"` 和 `"kind"`
	// 也必须正确。
	expectedType reflect.Type
	// 若为 unstructured，预期放入 store 的对象的 GVK。
	expectedGVK *schema.GroupVersionKind
	// 与 watch 来源同步的目标 store。
	store ReflectorStore
	// listerWatcher 用于执行 list 和 watch。
	listerWatcher ListerWatcherWithContext
	// resyncPeriod 返回重试的下一次退避间隔。
	resyncPeriod time.Duration
	// delayHandler 提供带重置语义的退避延迟函数（成功一次即重置）。
	delayHandler wait.DelayFunc
	// minWatchTimeout 定义 watch 请求的最小超时。
	minWatchTimeout time.Duration
	// maxWatchTimeout 定义 watch 请求的最大超时。
	// 实际超时在 [minWatchTimeout, maxWatchTimeout] 内随机。
	maxWatchTimeout time.Duration
	// clock 便于测试中操控时间。
	clock clock.Clock
	// paginatedResult 决定 list 调用是否强制分页。
	// 依据首次 list 调用的结果设置。
	paginatedResult bool
	// lastSyncResourceVersion 是与底层 store 同步时最近一次观察到的
	// resource version token。它是线程安全的，
	// 但与底层 store 并非同步一致。
	//
	// RV 与 store 的写入并非原子一致：读取时可能比 store 内容略新或略旧，
	// 这一宽松性由 watch 的重放语义兜底。
	lastSyncResourceVersion string
	// 若上一次使用 lastSyncResourceVersion 的 list 或 watch 请求因
	// "expired" 或 "too large resource version" 错误而失败，
	// isLastSyncResourceVersionUnavailable 为 true。
	isLastSyncResourceVersionUnavailable bool
	// lastSyncResourceVersionMutex 保护对 lastSyncResourceVersion 的读写。
	lastSyncResourceVersionMutex sync.RWMutex
	// 当 ListAndWatch 因错误断开连接时被调用。
	watchErrorHandler WatchErrorHandlerWithContext
	// WatchListPageSize 是初始与 resync watch 列表请求的分块大小。
	// 未设置时：一致性读（RV=""）或允许任意旧数据的读（RV="0"）
	// 默认使用 pager.PageSize；其余情况（RV != "" && RV != "0"）
	// 会关闭分页，以便从 watch cache 提供服务。
	// 注意：应谨慎使用，因为分页列表总是直接由 etcd 提供服务，
	// 效率明显更低，可能引发严重的性能与扩展性问题。
	WatchListPageSize int64
	// ShouldResync 会被周期调用，每次返回 `true` 时调用 Store 的 Resync 操作。
	ShouldResync func() bool
	// MaxInternalErrorRetryDuration 定义对 watch 返回的内部错误应重试多久。
	MaxInternalErrorRetryDuration time.Duration
	// useWatchList 开启后，reflector 会打开一个流从 API server 拉取数据。
	// 流式的主要优势是消耗更少的服务端资源。
	//
	// 旧行为是发起 LIST 请求、分块获取数据。
	// 分页 list 效率较低，且取决于对象实际大小，可能增加 APIServer 的内存消耗。
	//
	// 参见 https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/3157-watch-list#design-details
	//
	// useWatchList 对应 WatchListClient 特性：用 SendInitialEvents 的流式 watch 代替分页 List，
	// 以降低 apiserver 内存与 etcd 压力；服务端不支持时回退到 List。
	useWatchList bool
}

// Name 返回 reflector 的标识名（默认取自调用栈的 file:line）。
func (r *Reflector) Name() string {
	return r.name
}

// TypeDescription 返回用于日志展示的类型描述，仅用于显示。
func (r *Reflector) TypeDescription() string {
	return r.typeDescription
}

// ResourceVersionUpdater 允许 store 实现跟踪 reflector 当前的 resource version。
// 在启用存储 bookmark 时尤其重要。
type ResourceVersionUpdater interface {
	// UpdateResourceVersion 在 reflector 当前 resource version 每次更新时被调用。
	UpdateResourceVersion(resourceVersion string)
}

// WatchErrorHandler 在 ListAndWatch 因错误断开连接时被调用。
// 调用该 handler 后，informer 会退避并重试。
//
// 默认实现会根据错误类型尝试以合适的级别记录错误信息。
//
// 该 handler 的实现可以用其他方式展示错误信息。实现应尽快返回——
// 任何昂贵的处理都应卸载到别处。
type WatchErrorHandler func(r *Reflector, err error)

// WatchErrorHandlerWithContext 在 ListAndWatch 因错误断开连接时被调用。
// 调用该 handler 后，informer 会退避并重试。
//
// 默认实现会根据错误类型尝试以合适的级别记录错误信息。
//
// 该 handler 的实现可以用其他方式展示错误信息。实现应尽快返回——
// 任何昂贵的处理都应卸载到别处。
//
// 与 WatchErrorHandler 相同，但携带 ctx 以支持结构化日志；新代码应优先使用带 ctx 的版本。
type WatchErrorHandlerWithContext func(ctx context.Context, r *Reflector, err error)

// DefaultWatchErrorHandler 是 WatchErrorHandlerWithContext 的默认实现。
//
// 默认实现按错误类型选择日志级别：EOF 视为正常结束，过期错误降为 V(4)，
// 其余错误交给全局错误处理器（通常记录并上报）。
func DefaultWatchErrorHandler(ctx context.Context, r *Reflector, err error) {
	switch {
	case isExpiredError(err):
		// 不要设置 LastSyncResourceVersionUnavailable——ResourceVersion=RV 的
		// LIST 调用本身语义就是返回至少与该 RV 一样新的数据。
		// 所以先尝试以最近观察对象的 resource version 作为 RV 进行 LIST。
		klog.FromContext(ctx).V(4).Info("Watch closed", "reflector", r.name, "type", r.typeDescription, "err", err)
	case err == io.EOF:
		// watch 正常关闭
	case err == io.ErrUnexpectedEOF:
		klog.FromContext(ctx).V(1).Info("Watch closed with unexpected EOF", "reflector", r.name, "type", r.typeDescription, "err", err)
	default:
		utilruntime.HandleErrorWithContext(ctx, err, "Failed to watch", "reflector", r.name, "type", r.typeDescription)
	}
}

// NewNamespaceKeyedIndexerAndReflector 创建一个 Indexer 和一个 Reflector。
// Indexer 被配置为按 namespace 建立索引。
func NewNamespaceKeyedIndexerAndReflector(lw ListerWatcher, expectedType interface{}, resyncPeriod time.Duration) (indexer Indexer, reflector *Reflector) {
	indexer = NewIndexer(MetaNamespaceKeyFunc, Indexers{NamespaceIndex: MetaNamespaceIndexFunc})
	reflector = NewReflector(lw, expectedType, indexer, resyncPeriod)
	return indexer, reflector
}

// NewReflector 创建一个新 Reflector，其名称默认取调用栈中本包之外最近的
// source_file.go:line。更多信息参见 NewReflectorWithOptions。
func NewReflector(lw ListerWatcher, expectedType interface{}, store ReflectorStore, resyncPeriod time.Duration) *Reflector {
	return NewReflectorWithOptions(lw, expectedType, store, ReflectorOptions{ResyncPeriod: resyncPeriod})
}

// NewNamedReflector 创建一个使用指定名称的新 Reflector。更多信息参见
// NewReflectorWithOptions。
func NewNamedReflector(name string, lw ListerWatcher, expectedType interface{}, store ReflectorStore, resyncPeriod time.Duration) *Reflector {
	return NewReflectorWithOptions(lw, expectedType, store, ReflectorOptions{Name: name, ResyncPeriod: resyncPeriod})
}

// ReflectorOptions 用于配置 Reflector。
type ReflectorOptions struct {
	// Logger 若非 nil，则用于日志输出，取代 klog.Background()。
	// reflector 的名称会自动附加。
	Logger *klog.Logger

	// Name 是 Reflector 的名称。未设置时，默认取调用栈中本包之外最近的
	// source_file.go:line。
	Name string

	// TypeDescription 是 Reflector 的类型描述。未设置时按以下规则取默认值：
	// 若传给 NewReflectorWithOptions 的 expectedType 为 nil，类型为 "<unspecified>"；
	// 若 expectedType 是 *unstructured.Unstructured 实例且其 apiVersion 和 kind
	// 字段已设置，则类型描述为二者的字符串编码；否则类型描述为 expectedType 的
	// Go 类型。
	TypeDescription string

	// ResyncPeriod 是 Reflector 的 resync 周期。未设置时默认为 0（不 resync）。
	ResyncPeriod time.Duration

	// MinWatchTimeout 若非零，定义发往 kube-apiserver 的 watch 请求的最小超时。
	// 低于 5m 的值不会被采纳，以免对控制平面造成负面影响。
	MinWatchTimeout time.Duration

	// Clock 便于测试中控制时间。未设置时默认为 clock.RealClock{}。
	Clock clock.Clock

	// Backoff 是可选的自定义退避配置。
	// 若设置，将取代默认指数退避。
	// 将在其上调用 DelayWithReset(clock, resetDuration) 以生成延迟函数。
	// TODO(#136943)：通过 SharedInformerFactory 暴露该配置。
	Backoff *wait.Backoff
}

// NewReflectorWithOptions 创建一个新的 Reflector 对象，它会让给定的 store
// 与服务端给定资源的内容保持同步。除非 expectedType 为 nil，Reflector 保证
// 只把 expectedType 类型的对象放入 store。若 resyncPeriod 非零，reflector
// 会周期性咨询其 ShouldResync 函数以决定是否调用 Store 的 Resync 操作；
// `ShouldResync==nil` 表示总是“是”。这让你既可以用 reflector 周期性地处理
// 所有对象，也可以增量处理发生变化的对象。
func NewReflectorWithOptions(lw ListerWatcher, expectedType interface{}, store ReflectorStore, options ReflectorOptions) *Reflector {
	reflectorClock := options.Clock
	if reflectorClock == nil {
		reflectorClock = clock.RealClock{}
	}

	// 低于默认下限的 MinWatchTimeout 不被采纳，避免对控制面造成过大压力。
	minWatchTimeout := defaultMinWatchTimeout
	maxWatchTimeout := defaultMaxWatchTimeout
	if options.MinWatchTimeout > defaultMinWatchTimeout {
		minWatchTimeout = options.MinWatchTimeout
		maxWatchTimeout = 2 * minWatchTimeout
	}
	if maxWatchTimeout < minWatchTimeout {
		klog.TODO().V(3).Info(
			"maxWatchTimeout was less than minWatchTimeout, overriding to minWatchTimeout. Watch timeout randomization is disabled.",
			"minWatchTimeout", minWatchTimeout,
			"maxWatchTimeout", maxWatchTimeout,
		)
		maxWatchTimeout = minWatchTimeout
	}

	// 未提供自定义退避时，构造默认指数退避；Steps 使总时长约等于 Cap。
	backoff := options.Backoff
	if backoff == nil {
		backoff = &wait.Backoff{
			Duration: defaultBackoffInit,
			Cap:      defaultBackoffMax,
			Steps:    int(math.Ceil(float64(defaultBackoffMax) / float64(defaultBackoffInit))),
			Factor:   defaultBackoffFactor,
			Jitter:   defaultBackoffJitter,
		}
	}

	r := &Reflector{
		name:              options.Name,
		resyncPeriod:      options.ResyncPeriod,
		minWatchTimeout:   minWatchTimeout,
		maxWatchTimeout:   maxWatchTimeout,
		typeDescription:   options.TypeDescription,
		listerWatcher:     ToListerWatcherWithContext(lw),
		store:             store,
		delayHandler:      backoff.DelayWithReset(reflectorClock, defaultBackoffReset),
		clock:             reflectorClock,
		watchErrorHandler: WatchErrorHandlerWithContext(DefaultWatchErrorHandler),
		expectedType:      reflect.TypeOf(expectedType),
	}

	// 默认名称取自调用栈，便于在日志中定位是哪个 informer。
	if r.name == "" {
		r.name = naming.GetNameFromCallsite(internalPackages...)
	}

	logger := klog.Background()
	if options.Logger != nil {
		logger = *options.Logger
	}
	logger = klog.LoggerWithName(logger, r.name)
	r.logger = logger

	if r.typeDescription == "" {
		r.typeDescription = getTypeDescriptionFromObject(expectedType)
	}

	if r.expectedGVK == nil {
		r.expectedGVK = getExpectedGVKFromObject(expectedType)
	}

	// WatchListClient 特性开启且客户端支持其语义时，才走流式 watch-list；
	// 在单元测试或旧客户端下自动禁用并提示。
	r.useWatchList = clientfeatures.FeatureGates().Enabled(clientfeatures.WatchListClient)
	if r.useWatchList && watchlist.DoesClientNotSupportWatchListSemantics(lw) {
		r.logger.V(2).Info(
			"The client used to build this informer/reflector doesn't support WatchList semantics. The feature will be disabled. This is expected in unit tests but not in production. For details, see the documentation of watchlist.DoesClientNotSupportWatchListSemantics().",
			"feature", clientfeatures.WatchListClient,
		)
		r.useWatchList = false
	}

	return r
}

// getTypeDescriptionFromObject 推导类型描述：unstructured 对象优先用其 GVK，
// 否则用 Go 类型名；nil 返回固定占位符。
func getTypeDescriptionFromObject(expectedType interface{}) string {
	if expectedType == nil {
		return defaultExpectedTypeName
	}

	reflectDescription := reflect.TypeOf(expectedType).String()

	obj, ok := expectedType.(*unstructured.Unstructured)
	if !ok {
		return reflectDescription
	}

	gvk := obj.GroupVersionKind()
	if gvk.Empty() {
		return reflectDescription
	}

	return gvk.String()
}

// getExpectedGVKFromObject 仅对已设置 GVK 的 unstructured 返回 GVK，
// 用于校验 watch 事件的对象类型；其余情况返回 nil 表示不校验。
func getExpectedGVKFromObject(expectedType interface{}) *schema.GroupVersionKind {
	obj, ok := expectedType.(*unstructured.Unstructured)
	if !ok {
		return nil
	}

	gvk := obj.GroupVersionKind()
	if gvk.Empty() {
		return nil
	}

	return &gvk
}

// internalPackages 是生成默认 reflector 名称时忽略的包。这些包位于到
// NewReflector 的常见调用链上，因此会产生熵很低的 reflector 名称。
var internalPackages = []string{"client-go/tools/cache/"}

// Run 反复使用 reflector 的 ListAndWatch 获取所有对象及后续增量。
// stopCh 关闭时 Run 退出。
//
// 上下文日志：支持上下文日志的代码应使用 RunWithContext 而非 Run。
func (r *Reflector) Run(stopCh <-chan struct{}) {
	r.RunWithContext(wait.ContextForChannel(stopCh))
}

// RunWithContext 反复使用 reflector 的 ListAndWatch 获取所有对象及后续增量。
// 上下文被取消时退出。
func (r *Reflector) RunWithContext(ctx context.Context) {
	logger := klog.FromContext(ctx)
	logger.V(3).Info("Starting reflector", "type", r.typeDescription, "resyncPeriod", r.resyncPeriod, "reflector", r.name)
	// Until 会立即执行循环（immediate=true），并在每轮成功后重置退避计时器
	// （sliding=true）。泛化的 QPS 目标（约 0.22 QPS）见文件开头的退避常量。
	_ = r.delayHandler.Until(ctx, true, true, func(ctx context.Context) (bool, error) {
		if err := r.ListAndWatchWithContext(ctx); err != nil {
			r.watchErrorHandler(ctx, r, err)
		}
		return false, nil
	})
	logger.V(3).Info("Stopping reflector", "type", r.typeDescription, "resyncPeriod", r.resyncPeriod, "reflector", r.name)
}

var (
	// 用于表示 watch 因 reflector 客户端传入的停止 channel 的信号而停止。
	errorStopRequested = errors.New("stop requested")
)

// resyncChan 返回一个在需要 resync 时会收到数据的 channel，以及一个清理函数。
//
// resyncPeriod 为 0 时返回 nil 通道，永不触发。
func (r *Reflector) resyncChan() (<-chan time.Time, func() bool) {
	if r.resyncPeriod == 0 {
		// 该 channel 永远不会收到任何数据
		return nil, func() bool { return false }
	}
	// 需要清理函数：设想 watch 总是失败、不得不频繁 list 的场景。此时若不
	// 手动停止定时器，可能同时有大量定时器处于活动状态。
	t := r.clock.NewTimer(r.resyncPeriod)
	return t.C(), t.Stop
}

// ListAndWatch 先 list 全部条目并获取调用时刻的 resource version，
// 然后用该 resource version 进行 watch。
// 若 ListAndWatch 甚至未能尝试初始化 watch，则返回错误。
//
// 上下文日志：支持上下文日志的代码应使用 ListAndWatchWithContext 而非 ListAndWatch。
func (r *Reflector) ListAndWatch(stopCh <-chan struct{}) error {
	return r.ListAndWatchWithContext(wait.ContextForChannel(stopCh))
}

// ListAndWatchWithContext 先 list 全部条目并获取调用时刻的 resource version，
// 然后用该 resource version 进行 watch。
// 若 ListAndWatchWithContext 甚至未能尝试初始化 watch，则返回错误。
func (r *Reflector) ListAndWatchWithContext(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	logger.V(3).Info("Listing and watching", "type", r.typeDescription, "reflector", r.name)
	var err error
	var w watch.Interface
	fallbackToList := !r.useWatchList

	defer func() {
		if w != nil {
			w.Stop()
		}
	}()

	if r.useWatchList {
		w, err = r.watchList(ctx)
		if w == nil && err == nil {
			// stopCh 已关闭
			return nil
		}
		if err != nil {
			logger.V(4).Info(
				"Data couldn't be fetched in watchlist mode. Falling back to regular list. This is expected if watchlist is not supported or disabled in kube-apiserver.",
				"err", err,
			)
			fallbackToList = true
			// 确保不会意外把垃圾数据传给 watch。
			w = nil
		}
	}

	// watch-list 失败或未启用时退化为传统 List，保证最终能建立初始状态。
	if fallbackToList {
		err = r.list(ctx)
		if err != nil {
			return err
		}
	}

	logger.V(2).Info("Caches populated", "type", r.typeDescription, "reflector", r.name)
	return r.watchWithResync(ctx, w)
}

// startResync 周期调用 r.store.Resync() 方法。
// 注意本方法会阻塞，应当在独立的 goroutine 中调用。
func (r *Reflector) startResync(ctx context.Context, resyncerrc chan error) {
	logger := klog.FromContext(ctx)
	resyncCh, cleanup := r.resyncChan()
	defer func() {
		cleanup() // 调用最后写入的 cleanup
	}()
	for {
		select {
		case <-resyncCh:
		case <-ctx.Done():
			return
		}
		// ShouldResync 为 nil 表示“总是需要 resync”。
		if r.ShouldResync == nil || r.ShouldResync() {
			logger.V(4).Info("Forcing resync", "reflector", r.name)
			if err := r.store.Resync(); err != nil {
				resyncerrc <- err
				return
			}
		}
		// 每轮重新创建定时器，保证下一次从当前时刻开始计间隔。
		cleanup()
		resyncCh, cleanup = r.resyncChan()
	}
}

// watchWithResync 运行 watch，并在后台运行 startResync。
func (r *Reflector) watchWithResync(ctx context.Context, w watch.Interface) error {
	resyncerrc := make(chan error, 1)
	cancelCtx, cancel := context.WithCancel(ctx)
	// 等待 goroutine 结束对 race detector 有意义。
	// 否则“本函数返回+等待它的代码”与“goroutine 正在做事”之间存在竞态。
	var wg wait.Group
	defer func() {
		cancel()
		wg.Wait()
	}()
	wg.Start(func() {
		r.startResync(cancelCtx, resyncerrc)
	})
	return r.watch(ctx, w, resyncerrc)
}

// watch 向服务端发起 watch 请求、消费 watch 事件，并在达到退出场景前不断重启 watch。
//
// 若已提供 watch 则直接使用，否则会另起一个。
// 只要 watcher 已启动，返回前一定会将其停止。
func (r *Reflector) watch(ctx context.Context, w watch.Interface, resyncerrc chan error) error {
	stopCh := ctx.Done()
	logger := klog.FromContext(ctx)
	var err error
	retry := NewRetryWithDeadline(r.MaxInternalErrorRetryDuration, time.Minute, apierrors.IsInternalError, r.clock)
	defer func() {
		if w != nil {
			w.Stop()
		}
	}()

	for {
		// 给 stopCh 一个停止循环的机会，即便下方错误处理中有 continue 语句
		select {
		case <-stopCh:
			// 只有当 watchlist 或 list 请求成功后 stopCh
			// 被关闭时才会走到这里
			return nil
		default:
		}

		// 在发送请求前启动计时，因为某些代理在首个 watch 事件发出前不会 flush header
		start := r.clock.Now()

		// 若 w 已初始化，则必然已越过任何乱序的合成 Added 事件
		propagateRVFromStart := true
		if w == nil {
			// 超时随机化可避免大量客户端在同一时刻重建 watch。
			timeoutSeconds := int64(r.minWatchTimeout.Seconds() + rand.Float64()*(r.maxWatchTimeout.Seconds()-r.minWatchTimeout.Seconds()))
			options := metav1.ListOptions{
				ResourceVersion: r.LastSyncResourceVersion(),
				// 要避免 watcher 悬挂的情况：超时窗口内收不到任何事件的 watcher 会被停止。
				TimeoutSeconds: &timeoutSeconds,
				// 为减少 watch 重启对 kube-apiserver 的压力，可以启用 watch bookmark。
				// Reflector 不假设有 bookmark 返回（若服务端不支持 watch bookmark，
				// 会忽略该字段）。
				AllowWatchBookmarks: true,
			}
			if options.ResourceVersion == "" || options.ResourceVersion == "0" {
				// 若 watch 起始的 resource version 会带来非 RV 顺序的合成 ADDED 事件，
				// 则等越过该组事件后再传播 RV
				propagateRVFromStart = false
			}

			w, err = r.listerWatcher.WatchWithContext(ctx, options)
			if err != nil {
				// 连接被拒或 429 时只需退避后重连，不必重新 List。
				if canRetry := isWatchErrorRetriable(err); canRetry {
					logger.V(4).Info("Watch failed - backing off", "reflector", r.name, "type", r.typeDescription, "err", err)
					select {
					case <-stopCh:
						return nil
					case <-r.clock.After(r.delayHandler()):
						continue
					}
				}
				return err
			}
		}

		err = handleWatch(ctx, start, w, r.store, r.expectedType, r.expectedGVK, r.name, r.typeDescription,
			func(rv string, eventReceivedBesidesAdded bool) {
				// 只有收到至少一个非 Added 事件，或 RV 此前已被设置时，才更新 store 中的
				// resource version。因为可能遇到两种场景：
				// 1. watch 从 LastSyncResourceVersion 字段指定的 resource version 启动。
				//    此时可以放心更新 store 中的 RV 而不必担心乱序，因为我们不会收到
				//    可能乱序的资源的合成 added 事件。
				// 2. watch 在 LastSyncResourceVersion 为空时启动。此时在收到至少一个
				//    非 added 事件之前不更新 LastSyncResourceVersion，因为这是确保
				//    watch 已越过初始列表阶段的唯一办法。
				//
				// 只有在确认已越过“合成 Added 事件”阶段后才推进 RV，
				// 否则可能把乱序的合成事件对应的 RV 当作已同步点。
				if propagateRVFromStart || eventReceivedBesidesAdded {
					r.setLastSyncResourceVersion(rv)
					if rvu, ok := r.store.(ResourceVersionUpdater); ok {
						rvu.UpdateResourceVersion(rv)
					}
				}
			},
			r.clock, resyncerrc)
		// handleWatch 总会停止 watcher，所以这里无需再停。
		// 置为 nil 以便下一轮循环触发重试。
		w = nil
		retry.After(err)
		if err != nil {
			if !errors.Is(err, errorStopRequested) {
				switch {
				case isExpiredError(err):
					// 不要设置 LastSyncResourceVersionUnavailable——ResourceVersion=RV 的
					// LIST 调用本身语义就是返回至少与该 RV 一样新的数据。
					// 所以先尝试以最近观察对象的 resource version 作为 RV 进行 LIST。
					logger.V(4).Info("Watch closed", "reflector", r.name, "type", r.typeDescription, "err", err)
				case apierrors.IsTooManyRequests(err):
					logger.V(2).Info("Watch returned 429 - backing off", "reflector", r.name, "type", r.typeDescription)
					select {
					case <-stopCh:
						return nil
					case <-r.clock.After(r.delayHandler()):
						continue
					}
				case apierrors.IsInternalError(err) && retry.ShouldRetry():
					logger.V(2).Info("Retrying watch after internal error", "reflector", r.name, "type", r.typeDescription, "err", err)
					continue
				default:
					logger.Info("Warning: watch ended with error", "reflector", r.name, "type", r.typeDescription, "err", err)
				}
			}
			return nil
		}
	}
}

// list 列出全部条目，并记录调用时从服务端获取的 resource version。
// 该 resource version 可用于后续推进通知（即 watch）。
func (r *Reflector) list(ctx context.Context) error {
	var resourceVersion string
	options := metav1.ListOptions{ResourceVersion: r.relistResourceVersion()}

	initTrace := trace.New("Reflector ListAndWatch", trace.Field{Key: "name", Value: r.name})
	defer initTrace.LogIfLong(10 * time.Second)
	var list runtime.Object
	var paginatedResult bool
	var err error
	listCh := make(chan struct{}, 1)
	panicCh := make(chan interface{}, 1)
	// List 在独立 goroutine 中执行，使调用方能通过 ctx 取消；
	// panic 通过 panicCh 转交，避免被 goroutine 边界吞掉。
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
		}()
		// 若 listerWatcher 支持，尝试分块收集列表；否则首次 list 请求
		// 就会返回完整响应。
		pager := pager.New(pager.SimplePageFunc(func(opts metav1.ListOptions) (runtime.Object, error) {
			return r.listerWatcher.ListWithContext(ctx, opts)
		}))
		// 分页策略：显式大小 > 首次已分页 > 有具体 RV 时禁用分页以便走 watch cache。
		switch {
		case r.WatchListPageSize != 0:
			pager.PageSize = r.WatchListPageSize
		case r.paginatedResult:
			// 首次就得到了分页结果。假定该资源和服务端尊重分页请求
			// （即 watch cache 很可能被禁用），保持默认分页大小。
		case options.ResourceVersion != "" && options.ResourceVersion != "0":
			// 用户没有显式请求分页。
			//
			// 当 ResourceVersion != "" 时，有机会从 watch cache list，
			// 但（对 ResourceVersion != "0"）只有未设置 Limit 时才会如此。
			// 为避免 etcd 上的惊群（例如 master 升级时），我们显式关闭分页，
			// 强制从 watch cache（若启用）list。
			// 按 RV 的既有语义（结果至少与提供的 RV 一样新），
			// 这样做是正确的，不会导致时间回退。
			//
			// 对 ResourceVersion="0" 同样不关闭分页，因为该场景下 watch cache
			// 本来就会忽略 Limit，且若 watch cache 未启用也不会引入回归。
			pager.PageSize = 0
		}

		list, paginatedResult, err = pager.ListWithAlloc(context.Background(), options)
		if isExpiredError(err) || isTooLargeResourceVersionError(err) {
			r.setIsLastSyncResourceVersionUnavailable(true)
			// 若 list 使用的 resource version 不可用，立即重试。
			// 若分页 list 的后续页因 "Expired" 错误失败，pager 已会回退到全量 list，
			// 但 pager 可能未启用、全量 list 也可能因其所用 RV 过期而失败，
			// 或缓存尚未同步到所提供的 resource version。因此我们总是回退到
			// resourceVersion=""，以恢复并确保 reflector 能继续前进。
			//
			// RV 过期时用空 RV 重新 List（由 etcd 做一致性读），确保能够前进。
			list, paginatedResult, err = pager.ListWithAlloc(context.Background(), metav1.ListOptions{ResourceVersion: r.relistResourceVersion()})
		}
		if err == nil {
			// Table 格式不是 reflector 支持的列表形态，直接报错而非尝试解析。
			if unsupportedList, unsupportedListGVK := isUnsupportedTableListObject(list); unsupportedList {
				err = fmt.Errorf("unsupported list gvk: %v, type: %v", unsupportedListGVK, r.typeDescription)
			}
		}
		close(listCh)
	}()
	select {
	case <-ctx.Done():
		return nil
	case r := <-panicCh:
		panic(r)
	case <-listCh:
	}

	initTrace.Step("Objects listed", trace.Field{Key: "error", Value: err}, trace.Field{Key: "count", Value: meta.LenList(list)})
	if err != nil {
		return fmt.Errorf("failed to list %v: %w", r.typeDescription, err)
	}

	// 检查 list 结果是否分页，并据此设置 paginatedResult。
	// 但只想对初始 list 这么做（这是唯一会设置 ResourceVersion="0" 的场景）。
	// 原因是之后某些情况下我们可能通过设置 ResourceVersion="" 强制直接从 etcd
	// list，即使 watch cache 已启用也会返回分页结果；但那种情况下我们仍希望
	// 尽可能把请求发给 watch cache。
	//
	// ResourceVersion="0" 的请求返回分页结果，意味着 watch cache 被禁用且该类型
	// 对象数量很多。此时没有优先从 watch cache list 的必要。
	if options.ResourceVersion == "0" && paginatedResult {
		r.paginatedResult = true
	}

	r.setIsLastSyncResourceVersionUnavailable(false) // list was successful
	listMetaInterface, err := meta.ListAccessor(list)
	if err != nil {
		return fmt.Errorf("unable to understand list result %#v: %v", list, err)
	}
	resourceVersion = listMetaInterface.GetResourceVersion()
	initTrace.Step("Resource version extracted")
	items, err := meta.ExtractListWithAlloc(list)
	if err != nil {
		return fmt.Errorf("unable to understand list result %#v (%v)", list, err)
	}
	initTrace.Step("Objects extracted")
	// Replace 是原子整体替换：保证 store 不会长时间处于半新半旧状态。
	if err := r.syncWith(items, resourceVersion); err != nil {
		return fmt.Errorf("unable to sync list result: %v", err)
	}
	initTrace.Step("SyncWith done")
	r.setLastSyncResourceVersion(resourceVersion)
	initTrace.Step("Resource version updated")
	return nil
}

// watchList 建立一个流，按
// https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/3157-watch-list#proposal
// 所述从服务端获取一致的数据快照。
//
// 场景 1：从 Most Recent 开始（RV=""，ResourceVersionMatch=ResourceVersionMatchNotOlderThan）
// 与服务端建立一致的流。返回的数据是一致的，如同通过法定人数读直接由 etcd 提供。
// 流以到最新 ResourceVersion 为止所有资源的合成 "Added" 事件开始，
// 以包含最新 ResourceVersion 的合成 "Bookmark" 事件结束。
// 收到 "Bookmark" 事件后，reflector 被认为已同步。
// 它用收集到的条目替换其内部 store，并复用当前 watch 请求获取后续事件。
//
// 场景 2：从 Exact 开始（RV>"0"，ResourceVersionMatch=ResourceVersionMatchNotOlderThan）
// 与服务端在所提供的 resource version 上建立流。
// 为建立初始状态，服务端以合成 "Added" 事件开始。
// 流以包含所提供或更新 resource version 的合成 "Bookmark" 事件结束。
// 收到 "Bookmark" 事件后，reflector 被认为已同步。
// 它用收集到的条目替换其内部 store，并复用当前 watch 请求获取后续事件。
//
// watchList 以 watch-list（SendInitialEvents）方式获取初始全量快照：服务端先推送合成的 Added
// 事件，再以带 initial-events-end 注解的 Bookmark 表示结束；收到该 Bookmark 后整体 Replace store，
// 并复用同一条 watch 流继续接收增量事件。
func (r *Reflector) watchList(ctx context.Context) (watch.Interface, error) {
	stopCh := ctx.Done()
	logger := klog.FromContext(ctx)
	var w watch.Interface
	var err error
	var temporaryStore Store
	var resourceVersion string
	// TODO(#115478)：看看本函数能否转为方法，
	// 以及错误处理能否与 r.watch 方法统一
	isErrorRetriableWithSideEffectsFn := func(err error) bool {
		if canRetry := isWatchErrorRetriable(err); canRetry {
			logger.V(2).Info("watch-list failed - backing off", "reflector", r.name, "type", r.typeDescription, "err", err)
			<-r.clock.After(r.delayHandler())
			return true
		}
		if isExpiredError(err) || isTooLargeResourceVersionError(err) {
			// 我们尝试重建 watch 请求，但提供的 RV 要么已过期，
			// 要么大于服务端已知的值。此时重置 RV，
			// 尝试从 watch cache 获取一致快照（场景 1）
			//
			// RV 不可用时清空，重试会走 case 1（从最新 RV 获取一致性快照）。
			r.setIsLastSyncResourceVersionUnavailable(true)
			return true
		}
		return false
	}

	// 若外部 store 声明了 transformer，临时 store 也使用同一转换，保证后续 Replace 语义一致。
	var transformer TransformFunc
	storeOpts := []StoreOption{}
	if tr, ok := r.store.(TransformingStore); ok && tr.Transformer() != nil {
		transformer = tr.Transformer()
		storeOpts = append(storeOpts, WithTransformer(transformer))
	}

	initTrace := trace.New("Reflector WatchList", trace.Field{Key: "name", Value: r.name})
	defer initTrace.LogIfLong(10 * time.Second)
	for {
		select {
		case <-stopCh:
			return nil, nil
		default:
		}

		resourceVersion = ""
		lastKnownRV := r.rewatchResourceVersion()
		temporaryStore = NewStore(DeletionHandlingMetaNamespaceKeyFunc, storeOpts...)
		// TODO(#115478)：大 "list"、慢客户端、慢网络、优先级与公平性（P&F）
		//  都可能拖慢流式传输甚至最终失败。
		//  这种情况下是否应该增大超时后重试？
		timeoutSeconds := int64(r.minWatchTimeout.Seconds() + rand.Float64()*(r.maxWatchTimeout.Seconds()-r.minWatchTimeout.Seconds()))
		options := metav1.ListOptions{
			ResourceVersion:      lastKnownRV,
			AllowWatchBookmarks:  true,
			SendInitialEvents:    ptr.To(true),
			ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
			TimeoutSeconds:       &timeoutSeconds,
		}
		start := r.clock.Now()

		w, err = r.listerWatcher.WatchWithContext(ctx, options)
		if err != nil {
			if isErrorRetriableWithSideEffectsFn(err) {
				continue
			}
			return nil, err
		}
		watchListBookmarkReceived, err := handleListWatch(ctx, start, w, temporaryStore, r.expectedType, r.expectedGVK, r.name, r.typeDescription,
			func(rv string, eventReceivedBesidesAdded bool) {
				// 仅记录“非 Added”事件之后看到的 RV，避免采用合成事件的乱序 RV。
				if eventReceivedBesidesAdded {
					resourceVersion = rv
				}
			},
			r.clock, make(chan error))
		if err != nil {
			w.Stop() // stop and retry with clean state
			if errors.Is(err, errorStopRequested) {
				return nil, nil
			}
			if isErrorRetriableWithSideEffectsFn(err) {
				continue
			}
			return nil, err
		}
		// 收到结束 Bookmark 才算拿到完整初始状态；否则重来。
		if watchListBookmarkReceived {
			break
		}
	}
	// 我们已通过 "k8s.io/initial-events-end" bookmark 确认，
	// 成功从 watch-list 获取了初始状态。
	initTrace.Step("Objects streamed", trace.Field{Key: "count", Value: len(temporaryStore.List())})
	r.setIsLastSyncResourceVersionUnavailable(false)

	// 使用 temporaryStore 以确保与当前 store 实现解耦。
	// 目前 store 以实现为队列，上层组件在完成内容替换后会尽快将其排空。
	checkWatchListDataConsistencyIfRequested(ctx, r.name, resourceVersion, r.listerWatcher.ListWithContext, transformer, temporaryStore.List)

	// 一次性 Replace，避免外部 store 看到中间态。
	if err := r.store.Replace(temporaryStore.List(), resourceVersion); err != nil {
		return nil, fmt.Errorf("unable to sync watch-list result: %w", err)
	}
	initTrace.Step("SyncWith done")
	r.setLastSyncResourceVersion(resourceVersion)

	return w, nil
}

// syncWith 用给定列表替换 store 中的条目。
func (r *Reflector) syncWith(items []runtime.Object, resourceVersion string) error {
	found := make([]interface{}, 0, len(items))
	for _, item := range items {
		found = append(found, item)
	}
	return r.store.Replace(found, resourceVersion)
}

// handleListWatch 消费 w 的事件、更新 Store，并记录最后看到的
// ResourceVersion，以便重试时从该 ResourceVersion 继续。成功时，接收完初始
// 对象集后 watcher 会保持打开，以便继续监听后续事件。
func handleListWatch(
	ctx context.Context,
	start time.Time,
	w watch.Interface,
	store Store,
	expectedType reflect.Type,
	expectedGVK *schema.GroupVersionKind,
	name string,
	expectedTypeName string,
	setLastSyncResourceVersion func(string, bool),
	clock clock.Clock,
	errCh chan error,
) (bool, error) {
	exitOnWatchListBookmarkReceived := true
	return handleAnyWatch(ctx, start, w, store, expectedType, expectedGVK, name, expectedTypeName,
		setLastSyncResourceVersion, exitOnWatchListBookmarkReceived, clock, errCh)
}

// handleWatch 消费 w 的事件、更新 Store，并记录最后看到的
// ResourceVersion，以便重试时从该 ResourceVersion 继续。退出时 watcher
// 总会被停止。
func handleWatch(
	ctx context.Context,
	start time.Time,
	w watch.Interface,
	store ReflectorStore,
	expectedType reflect.Type,
	expectedGVK *schema.GroupVersionKind,
	name string,
	expectedTypeName string,
	setLastSyncResourceVersion func(string, bool),
	clock clock.Clock,
	errCh chan error,
) error {
	exitOnWatchListBookmarkReceived := false
	_, err := handleAnyWatch(ctx, start, w, store, expectedType, expectedGVK, name, expectedTypeName,
		setLastSyncResourceVersion, exitOnWatchListBookmarkReceived, clock, errCh)
	return err
}

// handleAnyWatch 消费 w 的事件、更新 Store，并记录最后看到的 ResourceVersion，
// 以便重试时从该 RV 继续。若 exitOnWatchListBookmarkReceived 为 true，则持续消费
// watch 事件，直到收到带有 WatchList 注解的 bookmark 事件。
// 只要收到了 WatchList bookmark，即使 exitOnWatchListBookmarkReceived 为 false，
// 也返回 true（watchListBookmarkReceived）。
// watcher 总会被停止，除非 exitOnWatchListBookmarkReceived 和
// watchListBookmarkReceived 同时为 true。这允许调用方复用同一 watch 流
// 继续监听新事件。
//
// handleAnyWatch 是事件消费的核心循环：校验对象类型/GVK、按事件类型更新 store、
// 维护 RV，并可按需在 watch-list 结束 bookmark 处退出且保留流供后续复用。
func handleAnyWatch(
	ctx context.Context,
	start time.Time,
	w watch.Interface,
	store ReflectorStore,
	expectedType reflect.Type,
	expectedGVK *schema.GroupVersionKind,
	name string,
	expectedTypeName string,
	setLastSyncResourceVersion func(string, bool),
	exitOnWatchListBookmarkReceived bool,
	clock clock.Clock,
	errCh chan error,
) (bool, error) {
	watchListBookmarkReceived := false
	eventReceivedBesidesAdded := false
	eventCount := 0
	logger := klog.FromContext(ctx)
	initialEventsEndBookmarkWarningTicker := newInitialEventsEndBookmarkTicker(logger, name, clock, start, exitOnWatchListBookmarkReceived)
	defer initialEventsEndBookmarkWarningTicker.Stop()
	stopWatcher := true
	defer func() {
		if stopWatcher {
			w.Stop()
		}
	}()

loop:
	for {
		select {
		case <-ctx.Done():
			return watchListBookmarkReceived, errorStopRequested
		case err := <-errCh:
			return watchListBookmarkReceived, err
		case event, ok := <-w.ResultChan():
			if !ok {
				break loop
			}
			if event.Type == watch.Error {
				return watchListBookmarkReceived, apierrors.FromObject(event.Object)
			}
			// 类型/GVK 不符的事件通常意味着配置错误或服务端协议问题，记录后跳过而非中断。
			if expectedType != nil {
				if e, a := expectedType, reflect.TypeOf(event.Object); e != a {
					utilruntime.HandleErrorWithContext(ctx, nil, "Unexpected watch event object type", "reflector", name, "expectedType", e, "actualType", a)
					continue
				}
			}
			if expectedGVK != nil {
				if e, a := *expectedGVK, event.Object.GetObjectKind().GroupVersionKind(); e != a {
					utilruntime.HandleErrorWithContext(ctx, nil, "Unexpected watch event object gvk", "reflector", name, "expectedGVK", e, "actualGVK", a)
					continue
				}
			}
			// 不支持接收 Table 格式的资源，
			// 更多信息见 #132926
			if unsupportedGVK := isUnsupportedTableObject(event.Object); unsupportedGVK {
				utilruntime.HandleErrorWithContext(ctx, nil, "Unsupported watch event object gvk", "reflector", name, "actualGVK", event.Object.GetObjectKind().GroupVersionKind())
				continue
			}
			meta, err := meta.Accessor(event.Object)
			if err != nil {
				utilruntime.HandleErrorWithContext(ctx, err, "Unable to understand watch event", "reflector", name, "event", event)
				continue
			}
			resourceVersion := meta.GetResourceVersion()
			switch event.Type {
			case watch.Added:
				err := store.Add(event.Object)
				if err != nil {
					utilruntime.HandleErrorWithContext(ctx, err, "Unable to add watch event object to store", "reflector", name, "object", event.Object)
				}
			case watch.Modified:
				eventReceivedBesidesAdded = true
				err := store.Update(event.Object)
				if err != nil {
					utilruntime.HandleErrorWithContext(ctx, err, "Unable to update watch event object to store", "reflector", name, "object", event.Object)
				}
			case watch.Deleted:
				// TODO：是否有消费者需要访问“最后已知状态”
				// （即 event.Object 传入的对象）？若有，可能需要调整这里。
				eventReceivedBesidesAdded = true
				err := store.Delete(event.Object)
				if err != nil {
					utilruntime.HandleErrorWithContext(ctx, err, "Unable to delete watch event object from store", "reflector", name, "object", event.Object)
				}
			case watch.Bookmark:
				// `Bookmark` 表示 watch 已同步到此，只需更新 resourceVersion
				eventReceivedBesidesAdded = true
				// initial-events-end 注解标记 watch-list 初始全量推送结束。
				if meta.GetAnnotations()[metav1.InitialEventsAnnotationKey] == "true" {
					watchListBookmarkReceived = true
				}
				// 把 bookmark 事件中的 resource version 传播给声明需要它的 store
				if bookmarkStore, ok := store.(ReflectorBookmarkStore); ok {
					err := bookmarkStore.Bookmark(resourceVersion)
					if err != nil {
						utilruntime.HandleErrorWithContext(ctx, err, "Unable to send bookmark event to store", "reflector", name, "object", event.Object)
					}
				}
			default:
				utilruntime.HandleErrorWithContext(ctx, err, "Unknown watch event", "reflector", name, "event", event)
			}
			// eventReceivedBesidesAdded 为 true 表明肯定已越过初始的合成 Added 事件
			setLastSyncResourceVersion(resourceVersion, eventReceivedBesidesAdded)
			eventCount++
			if exitOnWatchListBookmarkReceived && watchListBookmarkReceived {
				// 保留 watcher 不停止，交由调用方继续复用这条流。
				stopWatcher = false
				watchDuration := clock.Since(start)
				klog.FromContext(ctx).V(4).Info("Exiting watch because received the bookmark that marks the end of initial events stream", "reflector", name, "totalItems", eventCount, "duration", watchDuration)
				return watchListBookmarkReceived, nil
			}
			initialEventsEndBookmarkWarningTicker.observeLastEventTimeStamp(clock.Now())
		case <-initialEventsEndBookmarkWarningTicker.C():
			initialEventsEndBookmarkWarningTicker.warnIfExpired()
		}
	}

	watchDuration := clock.Since(start)
	// 一秒内无事件的关闭通常意味着代理或网络异常，单独报错以便上层退避。
	if watchDuration < 1*time.Second && eventCount == 0 {
		return watchListBookmarkReceived, &VeryShortWatchError{Name: name}
	}
	klog.FromContext(ctx).V(4).Info("Watch close", "reflector", name, "type", expectedTypeName, "totalItems", eventCount)
	return watchListBookmarkReceived, nil
}

// LastSyncResourceVersion 是最近一次与底层 store 同步时观察到的 resource version。
// 返回值与底层 store 的访问并不同步，且不是线程安全的。
func (r *Reflector) LastSyncResourceVersion() string {
	r.lastSyncResourceVersionMutex.RLock()
	defer r.lastSyncResourceVersionMutex.RUnlock()
	return r.lastSyncResourceVersion
}

func (r *Reflector) setLastSyncResourceVersion(v string) {
	r.lastSyncResourceVersionMutex.Lock()
	defer r.lastSyncResourceVersionMutex.Unlock()
	r.lastSyncResourceVersion = v
}

// relistResourceVersion 决定 reflector 应 list 或 relist 的起始 resource version。
// 返回 lastSyncResourceVersion，从而 relist 的资源版本不早于已在 relist 结果或
// watch 事件中观察到的版本；或当上一次 relist 返回 HTTP 410 (Gone) 时返回 ""，
// 使 relist 通过法定人数读使用 etcd 中最新的 resource version。
func (r *Reflector) relistResourceVersion() string {
	r.lastSyncResourceVersionMutex.RLock()
	defer r.lastSyncResourceVersionMutex.RUnlock()

	if r.isLastSyncResourceVersionUnavailable {
		// 由于本 reflector 发出的是分页 list 请求，而当 lastSyncResourceVersion
		// 不可用时所有分页 list 请求都会跳过 watch cache，因此设置
		// ResourceVersion="" 重新 list，通过 etcd 一致性读把 reflector
		// 重建到最新可用的 ResourceVersion。
		return ""
	}
	if r.lastSyncResourceVersion == "" {
		// 出于性能考虑，reflector 的初始 list 使用 "0" 作为 resource version，
		// 以便在 watch cache 启用时由其提供服务。
		return "0"
	}
	return r.lastSyncResourceVersion
}

// rewatchResourceVersion 决定 reflector 应开始流式接收的 resource version。
func (r *Reflector) rewatchResourceVersion() string {
	r.lastSyncResourceVersionMutex.RLock()
	defer r.lastSyncResourceVersionMutex.RUnlock()
	if r.isLastSyncResourceVersionUnavailable {
		// 初始流应返回最新 resource version 的数据。
		// 返回的数据必须是一致的，即如同通过法定人数读由 etcd 提供
		return ""
	}
	return r.lastSyncResourceVersion
}

// setIsLastSyncResourceVersionUnavailable 设置上一次使用 lastSyncResourceVersion
// 的 list 或 watch 请求是否返回了 "expired" 或 "too large resource version" 错误。
func (r *Reflector) setIsLastSyncResourceVersionUnavailable(isUnavailable bool) {
	r.lastSyncResourceVersionMutex.Lock()
	defer r.lastSyncResourceVersionMutex.Unlock()
	r.isLastSyncResourceVersionUnavailable = isUnavailable
}

// isExpiredError 识别 RV 过期/已删除（HTTP 410）两类错误。
func isExpiredError(err error) bool {
	// 在 Kubernetes 1.17 及更早版本中，api server 对 HTTP 410 (Gone) 响应会同时返回
	// apierrors.StatusReasonExpired 和 apierrors.StatusReasonGone。1.18 起 kube server
	// 更一致，总是返回 apierrors.StatusReasonExpired。出于向后兼容，只有当 reflector
	// 完全放弃支持 Kubernetes 1.17 服务端时才能移除 apierrors.IsGone 检查。
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err)
}

// isTooLargeResourceVersionError 识别“请求的 RV 大于服务端已知值”的错误，
// 同时兼容旧版本服务端不设置 cause 而只写消息文本的情况。
func isTooLargeResourceVersionError(err error) bool {
	if apierrors.HasStatusCause(err, metav1.CauseTypeResourceVersionTooLarge) {
		return true
	}
	// 在 Kubernetes 1.17.0-1.18.5 中，api server 不会把错误状态的 cause 设置为
	// metav1.CauseTypeResourceVersionTooLarge 来表示请求的最小 resource version
	// 大于当前可用的最大 resource version。为与这些服务端版本保持向后兼容，
	// 我们还需要根据错误消息字段的内容来识别该错误。
	if !apierrors.IsTimeout(err) {
		return false
	}
	apierr, ok := err.(apierrors.APIStatus)
	if !ok || apierr == nil || apierr.Status().Details == nil {
		return false
	}
	for _, cause := range apierr.Status().Details.Causes {
		// 匹配 api server 1.17.0-1.18.5 在该错误情况下返回的消息
		if cause.Message == "Too large resource version" {
			return true
		}
	}

	// 匹配 1.17.0 之前 api server 返回的消息
	if strings.Contains(apierr.Status().Message, "Too large resource version") {
		return true
	}

	return false
}

// isWatchErrorRetriable 判断从服务端收到的 watch 错误是否可以安全重试。
func isWatchErrorRetriable(err error) bool {
	// 若是 "connection refused" 错误，多半说明 apiserver 无响应。
	// 重新 list 所有对象没有必要，因为我们很可能可以从上次结束的位置重启 watch。
	// 这种情况下应开始指数退避并重发 watch 请求。
	// "429" 错误同理。
	if utilnet.IsConnectionRefused(err) || apierrors.IsTooManyRequests(err) {
		return true
	}
	return false
}

// initialEventsEndBookmarkTicker 是一个定时器：若在定义的 tick 间隔内没有收到
// 标记 watch 流结束的 bookmark 事件，就产生警告。
//
// 注意：
// 本类型暴露的方法不是线程安全的。
type initialEventsEndBookmarkTicker struct {
	clock.Ticker
	clock  clock.Clock
	name   string
	logger klog.Logger

	watchStart           time.Time
	tickInterval         time.Duration
	lastEventObserveTime time.Time
}

// 若 exitOnInitialEventsEndBookmarkRequested 为 false，newInitialEventsEndBookmarkTicker
// 返回一个 noop ticker。否则返回一个 ticker，其方法会在定义的 tick 间隔内未收到
// 标记 watch 流结束的 bookmark 事件时产生警告。
//
// 注意由调用方决定是否调用 t.C() 和 t.Stop()。
//
// 实践中，reflector 一旦收到 bookmark 事件就退出 watchHandler 并调用 t.C() 方法。
func newInitialEventsEndBookmarkTicker(logger klog.Logger, name string, c clock.Clock, watchStart time.Time, exitOnWatchListBookmarkReceived bool) *initialEventsEndBookmarkTicker {
	return newInitialEventsEndBookmarkTickerInternal(logger, name, c, watchStart, 10*time.Second, exitOnWatchListBookmarkReceived)
}

func newInitialEventsEndBookmarkTickerInternal(logger klog.Logger, name string, c clock.Clock, watchStart time.Time, tickInterval time.Duration, exitOnWatchListBookmarkReceived bool) *initialEventsEndBookmarkTicker {
	// 仅当需要等待 watch-list 结束 bookmark 且时钟支持 Ticker 时才创建真实 ticker，
	// 否则返回 noop，避免无意义的定时开销。
	clockWithTicker, ok := c.(clock.WithTicker)
	if !ok || !exitOnWatchListBookmarkReceived {
		if exitOnWatchListBookmarkReceived {
			logger.Info("Warning: clock does not support WithTicker interface but exitOnInitialEventsEndBookmark was requested")
		}
		return &initialEventsEndBookmarkTicker{
			Ticker: &noopTicker{},
		}
	}

	return &initialEventsEndBookmarkTicker{
		Ticker:       clockWithTicker.NewTicker(tickInterval),
		clock:        c,
		name:         name,
		logger:       logger,
		watchStart:   watchStart,
		tickInterval: tickInterval,
	}
}

func (t *initialEventsEndBookmarkTicker) observeLastEventTimeStamp(lastEventObserveTime time.Time) {
	t.lastEventObserveTime = lastEventObserveTime
}

func (t *initialEventsEndBookmarkTicker) warnIfExpired() {
	if err := t.produceWarningIfExpired(); err != nil {
		t.logger.Info("Warning: event bookmark expired", "err", err)
	}
}

// produceWarningIfExpired 在距上次收到事件的经过时间超过 tickInterval 时，
// 返回一个表示警告的错误。
//
// 应在 t.C() 产出值时调用本方法。
func (t *initialEventsEndBookmarkTicker) produceWarningIfExpired() error {
	if _, ok := t.Ticker.(*noopTicker); ok {
		return nil /*noop ticker，无需警告*/
	}
	// 尚未收到任何事件时以 watch 起点计算，便于尽早暴露“服务端不发送事件”的问题。
	if t.lastEventObserveTime.IsZero() {
		return fmt.Errorf("%s: awaiting required bookmark event for initial events stream, no events received for %v", t.name, t.clock.Since(t.watchStart))
	}
	elapsedTime := t.clock.Now().Sub(t.lastEventObserveTime)
	hasBookmarkTimerExpired := elapsedTime >= t.tickInterval

	if !hasBookmarkTimerExpired {
		return nil
	}
	return fmt.Errorf("%s: hasn't received required bookmark event marking the end of initial events stream, received last event %v ago", t.name, elapsedTime)
}

var _ clock.Ticker = &noopTicker{}

// TODO(#115478)：移动到 k8s/utils 仓库
// noopTicker 是永不触发的 Ticker，用于禁用超时告警。
type noopTicker struct{}

func (t *noopTicker) C() <-chan time.Time { return nil }

func (t *noopTicker) Stop() {}

// 当 watch 结果 channel 在一秒内关闭且未发送任何事件时，返回 VeryShortWatchError。
type VeryShortWatchError struct {
	// Reflector 的名称
	Name string
}

// Error 实现 error 接口。
func (e *VeryShortWatchError) Error() string {
	return fmt.Sprintf("very short watch: %s: Unexpected watch close - "+
		"watch lasted less than a second and no items received", e.Name)
}

// unsupportedTableGVK 列出已知不支持的 Table 资源 GVK。
var unsupportedTableGVK = map[schema.GroupVersionKind]bool{
	metav1beta1.SchemeGroupVersion.WithKind("Table"): true,
	metav1.SchemeGroupVersion.WithKind("Table"):      true,
}

// isUnsupportedTableObject 检查给定 runtime.Object 是否属于一组已知不支持的
// GroupVersionKind 的 "Table" 对象。
func isUnsupportedTableObject(rawObject runtime.Object) bool {
	unstructuredObj, ok := rawObject.(*unstructured.Unstructured)
	if !ok {
		return false
	}
	if unstructuredObj.GetKind() != "Table" {
		return false
	}

	return unsupportedTableGVK[rawObject.GetObjectKind().GroupVersionKind()]
}

// isUnsupportedTableListObject 判断列表对象是否为不支持的 Table 列表，
// 并返回其 GVK 以便记录清晰的错误信息。
func isUnsupportedTableListObject(rawObject runtime.Object) (bool, schema.GroupVersionKind) {
	unstructuredObj, ok := rawObject.(*unstructured.UnstructuredList)
	if !ok {
		return false, schema.GroupVersionKind{}
	}

	return unsupportedTableGVK[unstructuredObj.GetObjectKind().GroupVersionKind()], unstructuredObj.GetObjectKind().GroupVersionKind()
}
