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

package cacher

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/metadata"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher/delegator"
	"k8s.io/apiserver/pkg/storage/cacher/key"
	"k8s.io/apiserver/pkg/storage/cacher/metrics"
	"k8s.io/apiserver/pkg/storage/cacher/progress"
	"k8s.io/apiserver/pkg/storage/cacher/store"
	etcdfeature "k8s.io/apiserver/pkg/storage/feature"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

var (
	// emptyFunc 是 newCacheWatcher 的占位 forget 函数：真正的 forget 需要持锁计算。
	emptyFunc = func(bool) {}
	// coreNamespaceResource 用于识别 namespaces 这一集群级特例。
	coreNamespaceResource = schema.GroupResource{Group: "", Resource: "namespaces"}
)

const (
	// storageWatchListPageSize 是 cacher 向存储发起初始 watch 列表和重新同步
	// watch 列表时的请求分块大小。
	//
	// 分页拉取可避免一次性从 etcd 读取海量对象造成内存与延迟尖峰。
	storageWatchListPageSize = int64(10000)

	// DefaultEventFreshDuration 是我们希望保留事件的默认时间窗口。
	// 该值设为 defaultBookmarkFrequency 加上一段 epsilon，以最大化
	// 「最后一个 bookmark 落在保留历史内」的可能性，同时尽量减少内存占用。
	DefaultEventFreshDuration = defaultBookmarkFrequency + 15*time.Second

	// defaultBookmarkFrequency 定义除在 watch 截止前立即发送之外，
	// watch bookmark 应当发送的频率。
	defaultBookmarkFrequency = time.Minute
)

// Config 包含给定 Cache 的配置。
type Config struct {
	// 底层的 storage.Interface。
	Storage storage.Interface

	// 底层的 storage.Versioner。
	Versioner storage.Versioner

	// cacher 正在缓存的 GroupResource。用于在日志和指标中区分
	// *unstructured.Unstructured（CRD）。
	GroupResource schema.GroupResource

	// EventsHistoryWindow 指定存储保留的最小历史时长。
	// 若低于 DefaultEventFreshDuration，缓存创建将会失败。
	//
	// 该值必须覆盖 bookmark 周期，否则 watch 可能因历史被裁剪而中断。
	EventsHistoryWindow time.Duration

	// Cache 将缓存给定 Type 的对象，并假定它们全部存储在底层数据库的
	// ResourcePrefix 目录下。
	ResourcePrefix string

	// KeyFunc 用于获取给定对象在底层存储中的键。
	KeyFunc func(runtime.Object) (string, error)

	// GetAttrsFunc 用于获取对象的 labels 和 fields。
	GetAttrsFunc func(runtime.Object) (label labels.Set, field fields.Set, err error)

	// IndexerFuncs 用于优化需要处理某个 incoming event 的
	// watcher 数量。
	IndexerFuncs storage.IndexerFuncs

	// Indexers 用于加速 list 操作；若未找到 indexer，则回退到普通 list
	// 操作。
	Indexers *cache.Indexers

	// NewFunc 是一个创建存储 Type 类型对象的空新对象的函数。
	NewFunc func() runtime.Object

	// NewList 是一个创建存储 Type 类型对象列表的空新对象的函数。
	NewListFunc func() runtime.Object

	Codec runtime.Codec

	Clock clock.WithTicker
}

// watchersMap 保存某作用域下的 watcher，键为 Cacher.watcherIdx 分配的序号。
type watchersMap map[int]*cacheWatcher

// addWatcher 注册 watcher。
func (wm watchersMap) addWatcher(w *cacheWatcher, number int) {
	wm[number] = w
}

// deleteWatcher 移除 watcher；不存在时是空操作，因此可重复调用。
func (wm watchersMap) deleteWatcher(number int) {
	delete(wm, number)
}

// terminateAll 清空并逐个回调 done；遍历中删除是安全的（Go map 允许）。
func (wm watchersMap) terminateAll(done func(*cacheWatcher)) {
	for key, watcher := range wm {
		delete(wm, key)
		done(watcher)
	}
}

// indexedWatchers 是 watcher 的两级索引：按 namespace/name 作用域，以及按索引字段值。
//
// 两级索引把“每个事件遍历所有 watcher”降为“只遍历可能命中的 watcher”，
// 是 cacher 能支撑大量 watch 连接的关键。
type indexedWatchers struct {
	allWatchers   map[namespacedName]watchersMap
	valueWatchers map[string]watchersMap
}

// addWatcher 根据是否支持索引字段值，把 watcher 放入 valueWatchers 或 allWatchers。
func (i *indexedWatchers) addWatcher(w *cacheWatcher, number int, scope namespacedName, value string, supported bool) {
	if supported {
		if _, ok := i.valueWatchers[value]; !ok {
			i.valueWatchers[value] = watchersMap{}
		}
		i.valueWatchers[value].addWatcher(w, number)
	} else {
		scopedWatchers, ok := i.allWatchers[scope]
		if !ok {
			scopedWatchers = watchersMap{}
			i.allWatchers[scope] = scopedWatchers
		}
		scopedWatchers.addWatcher(w, number)
	}
}

// deleteWatcher 从对应索引中移除；索引变空时删除该键，避免 map 无限增长。
func (i *indexedWatchers) deleteWatcher(number int, scope namespacedName, value string, supported bool) {
	if supported {
		i.valueWatchers[value].deleteWatcher(number)
		if len(i.valueWatchers[value]) == 0 {
			delete(i.valueWatchers, value)
		}
	} else {
		i.allWatchers[scope].deleteWatcher(number)
		if len(i.allWatchers[scope]) == 0 {
			delete(i.allWatchers, scope)
		}
	}
}

// terminateAll 终止全部 watcher 并重置索引。
//
// 出现 watcher 属于异常情况（通常意味着缓存重建），因此打 warning 便于发现。
func (i *indexedWatchers) terminateAll(groupResource schema.GroupResource, done func(*cacheWatcher)) {
	// 注意我们不必调用 watcher 的 setDrainInputBufferLocked 方法，
	// 因为我们直接利用其默认值——立即停止；
	// 另外，已经设置过 draining 策略的 watcher
	// 不再存在于表中（它们已从 allWatchers 和 valueWatchers 映射中移除）
	if len(i.allWatchers) > 0 || len(i.valueWatchers) > 0 {
		klog.Warningf("Terminating all watchers from cacher %v", groupResource)
	}
	for _, watchers := range i.allWatchers {
		watchers.terminateAll(done)
	}
	for _, watchers := range i.valueWatchers {
		watchers.terminateAll(done)
	}
	i.allWatchers = map[namespacedName]watchersMap{}
	i.valueWatchers = map[string]watchersMap{}
}

// 由于这里不需要高精度，我们把超时在同一秒内的所有 watcher
// 放进同一个桶，并在超时时一次性弹出它们。更具体地说，
// 若设置的触发时间为 X，你可以在 (X-1,X+1) 区间内收到 bookmark。
//
// 按秒分桶把“为每个 watcher 维护定时器”降为“每秒处理一个桶”，
// 在数万 watcher 的规模下显著降低定时器开销。
type watcherBookmarkTimeBuckets struct {
	// watchersBuckets 的键是「距 createTime 的秒数」
	watchersBuckets   map[int64][]*cacheWatcher
	createTime        time.Time
	startBucketID     int64
	clock             clock.Clock
	bookmarkFrequency time.Duration
}

// newTimeBucketWatchers 创建以 clock.Now() 为起点的分桶表。
func newTimeBucketWatchers(clock clock.Clock, bookmarkFrequency time.Duration) *watcherBookmarkTimeBuckets {
	return &watcherBookmarkTimeBuckets{
		watchersBuckets:   make(map[int64][]*cacheWatcher),
		createTime:        clock.Now(),
		startBucketID:     0,
		clock:             clock,
		bookmarkFrequency: bookmarkFrequency,
	}
}

// 向桶中添加一个 watcher；若截止时间早于起始时间，
// 则会被加入第一个桶。
//
// 返回 false 表示该 watcher 已无需要发送的 bookmark（例如已过期）。
func (t *watcherBookmarkTimeBuckets) addWatcherThreadUnsafe(w *cacheWatcher) bool {
	// 注意返回的时间可能早于 t.createTime，尤其是当 nextBookmarkTime
	// 方法给出 Time 类型的零值时，
	// 因此 bucketID 可能为负值
	nextTime, ok := w.nextBookmarkTime(t.clock.Now(), t.bookmarkFrequency)
	if !ok {
		return false
	}
	bucketID := int64(nextTime.Sub(t.createTime) / time.Second)
	if bucketID < t.startBucketID {
		bucketID = t.startBucketID
	}
	watchers := t.watchersBuckets[bucketID]
	t.watchersBuckets[bucketID] = append(watchers, w)
	return true
}

// popExpiredWatchersThreadUnsafe 弹出所有已到期桶中的 watcher，并推进 startBucketID。
func (t *watcherBookmarkTimeBuckets) popExpiredWatchersThreadUnsafe() [][]*cacheWatcher {
	currentBucketID := int64(t.clock.Since(t.createTime) / time.Second)
	// 绝大多数情况下应只有一个或两个元素
	expiredWatchers := make([][]*cacheWatcher, 0, 2)
	for ; t.startBucketID <= currentBucketID; t.startBucketID++ {
		if watchers, ok := t.watchersBuckets[t.startBucketID]; ok {
			delete(t.watchersBuckets, t.startBucketID)
			expiredWatchers = append(expiredWatchers, watchers)
		}
	}
	return expiredWatchers
}

// filterWithAttrsFunc 判断事件对象是否应投递给某个 watcher。
type filterWithAttrsFunc func(key string, l labels.Set, f fields.Set, obj runtime.Object) bool

// indexedTriggerFunc 描述用于 watcher 索引的单个字段及其取值函数。
type indexedTriggerFunc struct {
	indexName   string
	indexerFunc storage.IndexerFunc
}

// Cacher 负责从内部缓存为给定资源提供 WATCH 和 LIST 请求服务，
// 并根据底层存储内容在后台更新其缓存。
// Cacher 实现了 storage.Interface（尽管大多数调用只是
// 委托给底层存储）。
//
// 读写路径不同：写操作直接委托给底层存储，读操作（Get/GetList/Watch）优先由
// watchCache 服务，从而把 etcd 的读压力与 apiserver 的读并发解耦。
type Cacher struct {
	// 用于性能调试的 HighWaterMarks。
	// 重要：由于 HighWaterMark 使用 sync/atomic，因 32 位平台上的一个 bug，
	// 它必须位于结构体顶部。
	// 参见：https://golang.org/pkg/sync/atomic/ 获取更多信息
	incomingHWM storage.HighWaterMark
	// 应当被分发给 watchers 的 incoming 事件。
	incoming chan watchCacheEvent

	resourcePrefix string

	sync.RWMutex

	// 访问 cacher 的缓存之前，必须等待 ready 就绪。
	// 这是为了防止用户访问尚未初始化或正在重新填充的结构。
	// 当 cacher 被暂停或停止时，ready 需要被置为 false。
	// 当 cacher 初始化完成、可以使用后，ready 需要被置为 true。
	ready *ready

	// 底层的 storage.Interface。
	storage storage.Interface

	// 底层缓存中对象的预期类型。
	objectType reflect.Type
	// 用于日志输出，以区分 *unstructured.Unstructured（CRD）
	groupResource schema.GroupResource

	// 对象近期变更与当前状态的“滑动窗口”。
	watchCache *watchCache
	reflector  *cache.Reflector

	// Versioner 用于处理 resource version。
	versioner storage.Versioner

	// newFunc 是一个创建存储 Type 类型对象的空新对象的函数。
	newFunc func() runtime.Object

	// newListFunc 是一个创建用于存储 Type 类型对象的空新列表的函数。
	newListFunc func() runtime.Object

	// indexedTrigger 用于优化需要处理 incoming event 的
	// watcher 数量。
	indexedTrigger *indexedTriggerFunc
	// watchers 是从 watcher 感兴趣的 trigger 函数取值
	// 到对应 watcher 集合的映射
	watcherIdx int
	watchers   indexedWatchers

	// 定义在分发事件时，可以花在等待未就绪 watcher 上的
	// 时间预算，超过后将其关闭。
	dispatchTimeoutBudget timeBudget

	// 处理优雅终止。
	stopLock sync.RWMutex
	stopped  bool
	stopCh   chan struct{}
	stopWg   sync.WaitGroup

	clock clock.Clock
	// timer 用于避免底层 watcher 中不必要的内存分配。
	timer *time.Timer

	// dispatching 表示当前是否有某个事件
	// 正在分发中。
	dispatching bool
	// watchersBuffer 是可能关心当前正在分发事件的
	// watcher 列表。
	watchersBuffer []*cacheWatcher
	// blockedWatchers 是当前缓冲区已满的 watcher 列表。
	blockedWatchers []*cacheWatcher
	// watchersToStop 是本来应当在当前分发过程中停止、
	// 但为避免与 watcher 中关闭通道的竞态而推迟到
	// 该事件分发结束时再停止的 watcher 列表。
	watchersToStop []*cacheWatcher
	// 维护一个超时队列，以便在 watcher 超时之前发送 bookmark 事件。
	// 注意：访问该字段时必须持有 Cacher.lock 保护。
	bookmarkWatchers *watcherBookmarkTimeBuckets
	// expiredBookmarkWatchers 是已过期、需要为下一次 bookmark 事件重新调度的 watcher 列表
	expiredBookmarkWatchers []*cacheWatcher
	compactor               *compactor
	watcherMetrics          *metrics.WatcherMetricsObservers
}

// NewCacherFromConfig 根据给定配置创建一个新的 Cacher，负责从内部缓存
// 提供 WATCH 和 LIST 请求服务，并在后台更新其缓存。
//
// 构造阶段完成校验与装配，真正的缓存填充在后台 goroutine 中通过 reflector 完成；
// 在 ready 变为 true 之前，读请求会收到 429 而不是读到不完整数据。
func NewCacherFromConfig(config Config) (*Cacher, error) {
	stopCh := make(chan struct{})
	obj := config.NewFunc()
	// 在构造阶段就报出这个错误，而不是等到获取第一个
	// watch 项时再报，因为这样更容易定位问题。
	if err := runtime.CheckCodec(config.Codec, obj); err != nil {
		return nil, fmt.Errorf("storage codec doesn't seem to match given type: %v", err)
	}

	var indexedTrigger *indexedTriggerFunc
	if config.IndexerFuncs != nil {
		// 目前我们不支持为给定资源定义多个 trigger 函数。
		if len(config.IndexerFuncs) > 1 {
			return nil, fmt.Errorf("cacher %s doesn't support more than one IndexerFunc: ", reflect.TypeOf(obj).String())
		}
		for key, value := range config.IndexerFuncs {
			if value != nil {
				indexedTrigger = &indexedTriggerFunc{
					indexName:   key,
					indexerFunc: value,
				}
			}
		}
	}

	if config.Clock == nil {
		config.Clock = clock.RealClock{}
	}
	objType := reflect.TypeOf(obj)
	resourcePrefix := config.ResourcePrefix
	if resourcePrefix == "" {
		return nil, fmt.Errorf("resourcePrefix cannot be empty")
	}
	if resourcePrefix == "/" {
		return nil, fmt.Errorf("resourcePrefix cannot be /")
	}
	if !strings.HasPrefix(resourcePrefix, "/") {
		return nil, fmt.Errorf("resourcePrefix needs to start from /")
	}
	cacher := &Cacher{
		resourcePrefix: resourcePrefix,
		ready:          newReady(config.Clock),
		storage:        config.Storage,
		objectType:     objType,
		groupResource:  config.GroupResource,
		versioner:      config.Versioner,
		newFunc:        config.NewFunc,
		newListFunc:    config.NewListFunc,
		indexedTrigger: indexedTrigger,
		watcherIdx:     0,
		watchers: indexedWatchers{
			allWatchers:   make(map[namespacedName]watchersMap),
			valueWatchers: make(map[string]watchersMap),
		},
		// TODO: 需要确定缓冲区大小的正确取值。
		incoming:              make(chan watchCacheEvent, 100),
		dispatchTimeoutBudget: newTimeBudget(),
		// 我们可能需要停止两者：
		// - wait.Until goroutine
		// - reflector.ListAndWatch
		// 且无法保证它们停止的顺序。
		// 因此我们只需关闭通道，并通过 WaitGroup 同步等待。
		stopCh:           stopCh,
		clock:            config.Clock,
		timer:            time.NewTimer(time.Duration(0)),
		bookmarkWatchers: newTimeBucketWatchers(config.Clock, defaultBookmarkFrequency),
		watcherMetrics:   metrics.NewWatcherMetricsObservers(config.GroupResource),
	}

	// 确保 timer 已停止。
	if !cacher.timer.Stop() {
		// 消费已触发（但尚未被接收）的 timer 事件，
		// 以免将来复用时出现虚假超时。
		<-cacher.timer.C
	}
	var contextMetadata metadata.MD
	if utilfeature.DefaultFeatureGate.Enabled(features.SeparateCacheWatchRPC) {
		// 为 cacher 发起的 watch 和 progress notify 请求添加 grpc 上下文元数据，以：
		// * 防止 cacher 打开的 watch 被饿死——将其移到与绕过 cacher 的 watch 请求分开的 Watch RPC 上。
		// * 确保 progress notification 请求与其 watch 在同一 Watch RPC 上执行，这是其工作所要求的。
		contextMetadata = metadata.New(map[string]string{"source": "cache"})
	}

	eventFreshDuration := config.EventsHistoryWindow
	if eventFreshDuration < DefaultEventFreshDuration {
		return nil, fmt.Errorf("config.EventsHistoryWindow (%v) must not be below %v", eventFreshDuration, DefaultEventFreshDuration)
	}

	progressRequester := progress.NewConditionalProgressRequester(config.Storage.RequestWatchProgress, config.Clock, contextMetadata)
	watchCache := newWatchCache(
		config.KeyFunc, cacher.processEvent, config.GetAttrsFunc, config.Versioner, config.Indexers,
		config.Clock, eventFreshDuration, config.GroupResource, progressRequester, config.Storage.GetCurrentResourceVersion)
	listerWatcher := NewListerWatcher(config.Storage, resourcePrefix, config.NewListFunc, contextMetadata)
	reflectorName := "storage/cacher.go:" + resourcePrefix

	reflector := cache.NewNamedReflector(reflectorName, listerWatcher, nil, watchCache, 0)
	// 为 reflector 的 pager 配置合适的分页块大小，用于从存储拉取数据。
	// 若分页列表调用因 "Expired" 错误失败，pager 会回退到全量列表。
	reflector.WatchListPageSize = storageWatchListPageSize
	// 当 etcd 连续 3 个周期丢失 leader 时，会返回 "no leader" 错误。
	// 我们不想因此终止所有 watcher，因为重建所有 watcher 会给 api-server 带来高负载。
	// 大多数情况下，leader 会在几个周期内重新选出。
	//
	// 容忍 30s 的内部错误可避免 etcd 短暂选主时引发 watch 大面积重连风暴。
	reflector.MaxInternalErrorRetryDuration = time.Second * 30

	cacher.watchCache = watchCache
	cacher.reflector = reflector

	if utilfeature.DefaultFeatureGate.Enabled(features.SizeBasedListCostEstimate) {
		err := config.Storage.EnableResourceSizeEstimation(cacher.getKeys)
		if err != nil {
			return nil, fmt.Errorf("failed to enable resource size estimation: %w", err)
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
		cacher.compactor = newCompactor(config.Storage, watchCache, config.Clock)
		go cacher.compactor.Run(stopCh)
	}

	go cacher.dispatchEvents()
	go progressRequester.Run(stopCh)

	cacher.stopWg.Add(1)
	go func() {
		defer cacher.stopWg.Done()
		defer cacher.terminateAllWatchers()
		wait.Until(
			func() {
				if !cacher.isStopped() {
					cacher.startCaching(stopCh)
				}
			}, time.Second, stopCh,
		)
	}()
	return cacher, nil
}

// startCaching 启动 reflector 的 ListAndWatch，并在首次 Replace 完成时把缓存标记为 ready。
//
// ListAndWatch 返回错误时只记录并重试（外层 wait.Until），因为临时错误不应
// 让整个 cacher 退出；在此期间读请求会持续得到 429。
func (c *Cacher) startCaching(stopChannel <-chan struct{}) {
	startTime := time.Now()
	c.watchCache.SetOnReplace(func() {
		c.ready.setReady()
		duration := time.Since(startTime)
		klog.V(1).InfoS("cacher initialized", "group", c.groupResource.Group, "resource", c.groupResource.Resource, "duration", duration)
		metrics.WatchCacheInitializations.WithLabelValues(c.groupResource.Group, c.groupResource.Resource).Inc()
		metrics.WatchCacheInitializationDuration.WithLabelValues(c.groupResource.Group, c.groupResource.Resource).Observe(duration.Seconds())
	})
	var err error
	defer func() {
		c.ready.setError(err)
	}()

	c.terminateAllWatchers()
	err = c.reflector.ListAndWatch(stopChannel)
	if err != nil {
		klog.Errorf("cacher (%v): unexpected ListAndWatch error: %v; reinitializing...", c.groupResource.String(), err)
		metrics.WatchCacheInitializationErrors.WithLabelValues(c.groupResource.Group, c.groupResource.Resource).Inc()
	}
}

// namespacedName 是 watcher 作用域键；空字段表示“不限制”。
type namespacedName struct {
	namespace string
	name      string
}

// Watch 为请求建立一个基于缓存的 watch，返回的事件流从 requiredResourceVersion 开始。
//
// 关键不变量：只有在拿到 watchCache 读锁并确定事件区间后，才把 watcher 注册进
// 索引，且注册时要再次校验 ready 代际，防止缓存重建期间把 watcher 挂到即将失效的
// 缓存上。
func (c *Cacher) Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error) {
	ctx, span := tracing.Start(ctx, "cacher.Watch",
		attribute.String("audit-id", audit.GetAuditIDTruncated(ctx)),
		attribute.Stringer("type", c.groupResource))
	defer span.End(500 * time.Millisecond)
	key, err := c.prepareKey(key, opts.Recursive)
	if err != nil {
		return nil, err
	}
	pred := opts.Predicate
	requestedWatchRV, err := c.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}

	readyGeneration, downtime, err := c.ready.checkAndReadGeneration()
	if err != nil {
		return nil, errors.NewTooManyRequests(err.Error(), calculateRetryAfterForUnreadyCache(downtime))
	}

	// 确定 watch 的 namespace 和 name 作用域：先取自请求，其次取自 field selector
	//
	// 作用域既可能来自 URL 路径（request context），也可能来自 fieldSelector 的等值匹配，
	// 两者都考虑是为了让索引查找覆盖所有等价的请求形式。
	scope := namespacedName{}
	if requestNamespace, ok := request.NamespaceFrom(ctx); ok && len(requestNamespace) > 0 {
		scope.namespace = requestNamespace
	} else if selectorNamespace, ok := pred.Field.RequiresExactMatch("metadata.namespace"); ok {
		scope.namespace = selectorNamespace
	}
	if requestInfo, ok := request.RequestInfoFrom(ctx); ok && requestInfo != nil && len(requestInfo.Name) > 0 {
		scope.name = requestInfo.Name
	} else if selectorName, ok := pred.Field.RequiresExactMatch("metadata.name"); ok {
		scope.name = selectorName
	}

	// 对于形如 '/api/v1/watch/namespaces/*' 的请求，将 scope.namespace 置为空。
	// namespaces 对象不会在 ObjFields 中填充 metadata.namespace。
	if c.groupResource == coreNamespaceResource && len(scope.namespace) > 0 && scope.namespace == scope.name {
		scope.namespace = ""
	}

	triggerValue, triggerSupported := "", false
	if c.indexedTrigger != nil {
		for _, field := range pred.IndexFields {
			if field == c.indexedTrigger.indexName {
				if value, ok := pred.Field.RequiresExactMatch(field); ok {
					triggerValue, triggerSupported = value, true
					break
				}
			}
		}
	}

	// 这本质上是在两者之间权衡：
	// - 尽可能小以减少内存占用
	// - 足够大，以确保需要处理
	//   大量变更的 watcher 有足够的缓冲，
	//   避免因某个 watcher 处理卡顿而阻塞其他 watcher
	//
	// 缓冲区按 watcher 数量动态建议，避免每个连接都分配大缓冲。
	chanSize := c.watchCache.suggestedWatchChannelSize(c.indexedTrigger != nil, triggerSupported)

	// 对于 watch-list 请求，client-go 会在收到任何错误时
	// 回退到标准 LIST
	//
	// watch-list 依赖 RequestWatchProgress 能力；不支持时返回 errWatcher 让客户端回退到 LIST，
	// 而不是静默降级成普通 watch（那会破坏 SendInitialEvents 语义）。
	if isListWatchRequest(opts) && !etcdfeature.DefaultFeatureSupportChecker.Supports(storage.RequestWatchProgress) {
		return newErrWatcher(fmt.Errorf("a watch stream was requested by the client but the required storage feature %s is disabled", storage.RequestWatchProgress)), nil
	}

	// 确定 watch 缓存必须同步到的 ResourceVersion
	requiredResourceVersion, err := c.getWatchCacheResourceVersion(ctx, requestedWatchRV, opts)
	if err != nil {
		return newErrWatcher(err), nil
	}

	// 确定用于计算 bookmarkAfterResourceVersion 的函数
	bookmarkAfterResourceVersionFn, err := c.getBookmarkAfterResourceVersionLockedFunc(requestedWatchRV, requiredResourceVersion, opts)
	if err != nil {
		return newErrWatcher(err), nil
	}

	// 确定 watch 超时时间（'0' 表示未设置截止时间，跳过检查）
	deadline, _ := ctx.Deadline()

	identifier := fmt.Sprintf("key: %q, labels: %q, fields: %q", key, pred.Label, pred.Field)

	// 在此处创建 watcher，以减少锁内的内存分配，
	// 因为内存分配可能触发 GC 并阻塞线程。
	// 另请注意，emptyFunc 只是占位符，
	// 直到我们能够计算 watcher.forget 函数（该计算必须在持锁时进行）。
	watcher := newCacheWatcher(
		chanSize,
		filterWithAttrsAndPrefixFunction(key, pred, c.groupResource),
		emptyFunc,
		c.versioner,
		deadline,
		pred.AllowWatchBookmarks,
		c.groupResource,
		c.watcherMetrics,
		c.clock,
		identifier,
	)

	// 注意：调用 c.waitUntilWatchCacheFreshAndForceAllEvents 时不能持有
	// c.watchCache.RLock，否则存在死锁风险，
	// 主要是因为 c.watchCache.processEvent 方法将无法推进
	//
	// 此外，尽管 c.waitUntilWatchCacheFreshAndForceAllEvents 会获取锁，
	// 但在该方法结束后释放锁是安全的，因为我们不要求
	// 该调用与后续真正获取事件的调用之间具有任何原子性。
	err = c.waitUntilWatchCacheFreshAndForceAllEvents(ctx, requiredResourceVersion, opts)
	if err != nil {
		return newErrWatcher(err), nil
	}

	// 我们显式使用非线程安全版本并自行加锁，以确保
	// 在此期间不会有新事件被处理。从本函数返回时
	// watchCache 将被解锁。
	// 注意我们不能在 Cacher 锁下进行此操作以避免死锁，因为
	// 底层 watchCache 会在其自身的锁下调用 processEvent。
	c.watchCache.RLock()
	defer c.watchCache.RUnlock()

	var cacheInterval *watchCacheInterval
	cacheInterval, err = c.watchCache.getAllEventsSinceLocked(requiredResourceVersion, key, opts)
	if err != nil {
		// 为与未缓存 watch 的实现保持一致，一旦通过了 authn/authz/admission
		// 并成功解析了 resource version，其他错误必须以类型为 ERROR 的
		// watch 事件失败，而不是直接返回错误。
		return newErrWatcher(err), nil
	}

	c.setInitialEventsEndBookmarkIfRequested(cacheInterval, opts, c.watchCache.resourceVersion)

	addedWatcher := false
	func() {
		c.Lock()
		defer c.Unlock()

		// 注册前再次检查 ready 代际：等待期间缓存可能已重建，此时旧区间与
		// 新缓存不连续，必须放弃注册。
		if generation, _, err := c.ready.checkAndReadGeneration(); generation != readyGeneration || err != nil {
			// 缓存变为未就绪或代际已经不同。
			// 避免注册并启动该 watch，因为它反正
			// 必须立即被终止。
			return
		}

		// 在可以计算时更新 watcher.forget 函数。
		watcher.forget = forgetWatcher(c, watcher, c.watcherIdx, scope, triggerValue, triggerSupported)
		// 更新 bookMarkAfterResourceVersion
		watcher.setBookmarkAfterResourceVersion(bookmarkAfterResourceVersionFn())
		c.watchers.addWatcher(watcher, c.watcherIdx, scope, triggerValue, triggerSupported)
		addedWatcher = true

		// 仅当客户端支持 watch bookmark 时才将其加入队列。
		if watcher.allowWatchBookmarks {
			c.bookmarkWatchers.addWatcherThreadUnsafe(watcher)
		}
		c.watcherIdx++
	}()

	if !addedWatcher {
		// 此时 watcher 尚未真正启动，因此直接丢弃它是安全的。
		//
		// 我们模拟立即终止 watch 的行为，其实质
		// 就是简单地关闭该 watcher。
		return newImmediateCloseWatcher(), nil
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.ShardedListAndWatch) && pred.ShardSelector != nil && !pred.ShardSelector.Empty() {
		metrics.RecordShardedWatchStarted(c.groupResource)
		originalForget := watcher.forget
		watcher.forget = func(drainWatcher bool) {
			metrics.RecordShardedWatchStopped(c.groupResource)
			originalForget(drainWatcher)
		}
	}

	go watcher.processInterval(ctx, cacheInterval, requiredResourceVersion)
	return watcher, nil
}

// Get 从缓存读取单个对象；缓存未同步到请求的 RV 时会等待（有超时）。
func (c *Cacher) Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error {
	ctx, span := tracing.Start(ctx, "cacher.Get",
		attribute.String("audit-id", audit.GetAuditIDTruncated(ctx)),
		attribute.String("key", key),
		attribute.String("resource-version", opts.ResourceVersion))
	key, err := c.prepareKey(key, false)
	if err != nil {
		return err
	}
	getRV, err := c.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return err
	}

	objVal, err := conversion.EnforcePtr(objPtr)
	if err != nil {
		return err
	}

	obj, exists, readResourceVersion, err := c.watchCache.WaitUntilFreshAndGet(ctx, getRV, key)
	if err != nil {
		return err
	}
	// Get 处理耗时超过 500ms 才算慢，而等待缓存新鲜的超时是 3s，
	// 因此要避免 trace 里只体现出等待时间。
	defer span.End(500 * time.Millisecond)

	if exists {
		elem, ok := obj.(*store.Element)
		if !ok {
			return fmt.Errorf("non *store.Element returned from storage: %v", obj)
		}
		objVal.Set(reflect.ValueOf(elem.Object).Elem())
	} else {
		objVal.Set(reflect.Zero(objVal.Type()))
		if !opts.IgnoreNotFound {
			return storage.NewKeyNotFoundError(key, int64(readResourceVersion))
		}
	}
	return nil
}

// computeListLimit 判断 cacher 是否应当对 incoming LIST 请求
// 应用 limit，并返回其取值。
//
// 注意该函数不检查 RV 也不检查 Continuation token，
// 这些参数由 shouldDelegateList 函数负责校验。
//
// 截至目前，对于设置 RV == 0 的请求，limit 会被忽略
func computeListLimit(opts storage.ListOptions) int64 {
	if opts.Predicate.Limit <= 0 || opts.ResourceVersion == "0" {
		return 0
	}
	return opts.Predicate.Limit
}

// listResp 是缓存返回的 list 结果：对象指针 + 该结果对应的 RV。
type listResp struct {
	Items           []interface{}
	ResourceVersion uint64
}

// GetList 实现了 storage.Interface
//
// 过滤器为空时走零拷贝快路径；否则先筛选对象指针、再按需构造结果切片，
// 避免为被过滤掉的对象分配内存。
func (c *Cacher) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	preparedKey, err := c.prepareKey(key, opts.Recursive)
	if err != nil {
		return err
	}
	_, err = c.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return err
	}

	ctx, span := tracing.Start(ctx, "cacher.GetList",
		attribute.String("audit-id", audit.GetAuditIDTruncated(ctx)),
		attribute.Stringer("type", c.groupResource))
	defer span.End(500 * time.Millisecond)

	if downtime, err := c.ready.check(); err != nil {
		// 若 Cacher 尚未初始化，按 https://kep.k8s.io/4568
		// 所述拒绝 List 请求
		return errors.NewTooManyRequests(err.Error(), calculateRetryAfterForUnreadyCache(downtime))
	}
	span.AddEvent("Ready")

	// 从缓存中列出 RV 至少为 'listRV' 的元素。
	listPtr, err := meta.GetItemsPtr(listObj)
	if err != nil {
		return err
	}
	listVal, err := conversion.EnforcePtr(listPtr)
	if err != nil {
		return err
	}
	if listVal.Kind() != reflect.Slice {
		return fmt.Errorf("need a pointer to slice, got %v", listVal.Kind())
	}

	resp, indexUsed, err := c.watchCache.WaitUntilFreshAndGetList(ctx, preparedKey, opts)
	if err != nil {
		return err
	}
	span.AddEvent("Listed items from cache", attribute.Int("count", len(resp.Items)))
	var lastSelectedObjectKey string
	var hasMoreListItems bool
	limit := computeListLimit(opts)
	if opts.Predicate.Empty() {
		// 每个项都匹配，因此结果大小可以预先确定，
		// 项可以直接拷入结果列表而无需中间切片。
		//
		// 快路径：直接按 limit 截断并写入结果，省去中间切片与二次拷贝。
		count := len(resp.Items)
		if limit > 0 && int64(count) > limit {
			count = int(limit)
			hasMoreListItems = true
		}
		listVal.Set(reflect.MakeSlice(listVal.Type(), count, count))
		for i, obj := range resp.Items[:count] {
			elem, ok := obj.(*store.Element)
			if !ok {
				return fmt.Errorf("non *store.Element returned from storage: %v", obj)
			}
			listVal.Index(i).Set(reflect.ValueOf(elem.Object).Elem())
			lastSelectedObjectKey = elem.Key
		}
	} else {
		// 存符合条件对象的指针，
		// 为什么不直接把对象放进 listObj 的 items 里？
		//   ListObject 中的元素是 Struct 类型，构造切片会带来过多的内存消耗。
		//   因此我们尽可能推迟这一操作
		var selectedObjects []runtime.Object
		for i, obj := range resp.Items {
			elem, ok := obj.(*store.Element)
			if !ok {
				return fmt.Errorf("non *store.Element returned from storage: %v", obj)
			}
			shardMatch := true
			if utilfeature.DefaultFeatureGate.Enabled(features.ShardedListAndWatch) {
				var err error
				shardMatch, err = opts.Predicate.MatchesSharding(elem.Object)
				if err != nil {
					return fmt.Errorf("shard matching failed: %w", err)
				}
			}
			if shardMatch && opts.Predicate.MatchesObjectAttributes(elem.Labels, elem.Fields) {
				selectedObjects = append(selectedObjects, elem.Object)
				lastSelectedObjectKey = elem.Key
			}
			if limit > 0 && int64(len(selectedObjects)) >= limit {
				hasMoreListItems = i < len(resp.Items)-1
				break
			}
		}
		if len(selectedObjects) == 0 {
			// 为保持一致性，确保结果中永不返回 nil 的 Items 指针。
			listVal.Set(reflect.MakeSlice(listVal.Type(), 0, 0))
		} else {
			// 由于结果集大小已知，按合适的大小重建切片
			listVal.Set(reflect.MakeSlice(listVal.Type(), len(selectedObjects), len(selectedObjects)))
			span.AddEvent("Resized result")
			for i, o := range selectedObjects {
				listVal.Index(i).Set(reflect.ValueOf(o).Elem())
			}
		}
	}
	span.AddEvent("Filtered items", attribute.Int("count", listVal.Len()))
	if c.versioner != nil {
		// continue token 的 RV 取本次结果快照的 RV，而不是最新 RV，
		// 否则分页过程中数据变化会导致翻页结果不一致。
		continueValue, remainingItemCount, err := storage.PrepareContinueToken(lastSelectedObjectKey, key, int64(resp.ResourceVersion), int64(len(resp.Items)), hasMoreListItems, opts)
		if err != nil {
			return err
		}

		if err = c.versioner.UpdateList(listObj, resp.ResourceVersion, continueValue, remainingItemCount); err != nil {
			return err
		}
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.ShardedListAndWatch) {
		opts.Predicate.SetShardInfoOnList(listObj)
	}
	metrics.RecordListCacheMetrics(c.groupResource, indexUsed, len(resp.Items), listVal.Len())
	return nil
}

// baseObjectThreadUnsafe 对 cachingObject 省略加锁。
//
// 直接取内层对象，避免为了读索引字段而触发一次深拷贝；调用方必须保证无并发写。
func baseObjectThreadUnsafe(object runtime.Object) runtime.Object {
	if co, ok := object.(*cachingObject); ok {
		return co.object
	}
	return object
}

// triggerValuesThreadUnsafe 返回事件对应的索引值。
//
// Update 事件会返回新旧两个值：旧值对应的 watcher 也需要收到事件（对象不再匹配其选择器时
// 必须发送 Deleted），否则 watcher 会永远保留一个已失效的对象。
func (c *Cacher) triggerValuesThreadUnsafe(event *watchCacheEvent) ([]string, bool) {
	if c.indexedTrigger == nil {
		return nil, false
	}

	result := make([]string, 0, 2)
	result = append(result, c.indexedTrigger.indexerFunc(baseObjectThreadUnsafe(event.Object)))
	if event.PrevObject == nil {
		return result, true
	}
	prevTriggerValue := c.indexedTrigger.indexerFunc(baseObjectThreadUnsafe(event.PrevObject))
	if result[0] != prevTriggerValue {
		result = append(result, prevTriggerValue)
	}
	return result, true
}

// processEvent 是 watchCache 的事件回调：把事件放入 incoming 队列，由 dispatchEvents 异步分发。
//
// 这里必须保持非阻塞语义之外的最小工作，因为它在 watchCache 锁内被调用；
// 队列积压通过 incomingHWM 观测。
func (c *Cacher) processEvent(event *watchCacheEvent) {
	if curLen := int64(len(c.incoming)); c.incomingHWM.Update(curLen) {
		// 监控 incoming 队列是否积压以及积压程度。
		klog.V(1).Infof("cacher (%v): %v objects queued in incoming channel.", c.groupResource.String(), curLen)
	}
	c.incoming <- *event
}

// dispatchEvents 是单 goroutine 的事件分发循环：串行处理事件与周期性 bookmark。
//
// 单 goroutine 保证事件顺序，避免 watcher 看到乱序的 RV。
func (c *Cacher) dispatchEvents() {
	// 加入抖动，帮助拉平任何聚合负载。
	bookmarkTimer := c.clock.NewTimer(wait.Jitter(time.Second, 0.25))
	defer bookmarkTimer.Stop()

	// 内部 informer 一旦与底层 store 完成
	// 首次成功同步，就会填充 RV。
	// 缓存必须等这次首次同步完成后才能被视为就绪。
	// 由于 lastProcessedResourceVersion 为 0 时无法发送 bookmark，
	// 我们在进入分发循环之前以激进的方式轮询首个列表 RV。
	lastProcessedResourceVersion := uint64(0)
	if err := wait.PollUntilContextCancel(wait.ContextForChannel(c.stopCh), 10*time.Millisecond, true, func(_ context.Context) (bool, error) {
		if rv := c.watchCache.getListResourceVersion(); rv != 0 {
			lastProcessedResourceVersion = rv
			return true, nil
		}
		return false, nil
	}); err != nil {
		// 鉴于上面的函数从不返回错误，
		// 非空错误意味着 stopCh 已关闭
		return
	}
	for {
		select {
		case event, ok := <-c.incoming:
			if !ok {
				return
			}
			// 不要分发来自存储层的 bookmark。
			// 为了在 kube-apiserver 重启时支持高效的 watch 恢复，
			// 它们可能非常频繁（甚至达到亚秒级），
			// 若把它们向下传播可能会使整个系统过载。
			//
			// TODO: 如果将来我们认定其性能和可扩展性
			// 开销可以接受，这里就是把它们接入的位置。
			// 但届时需要通过检查此处的 resourceVersion
			// 是否变化，来判断本调用是由 bookmark 事件
			// 还是由普通的 Add/Update/Delete 操作触发的。
			//
			// 存储层 bookmark 频率极高，直接下发给客户端会造成放大效应；
			// cacher 用自己的 1 秒节拍重新生成 bookmark。
			if event.Type != watch.Bookmark {
				c.dispatchEvent(&event)
			}
			lastProcessedResourceVersion = event.ResourceVersion
			metrics.EventsCounter.WithLabelValues(c.groupResource.Group, c.groupResource.Resource).Inc()
		case <-bookmarkTimer.C():
			bookmarkTimer.Reset(wait.Jitter(time.Second, 0.25))
			bookmarkEvent := &watchCacheEvent{
				Type:            watch.Bookmark,
				Object:          c.newFunc(),
				ResourceVersion: lastProcessedResourceVersion,
			}
			if err := c.versioner.UpdateObject(bookmarkEvent.Object, bookmarkEvent.ResourceVersion); err != nil {
				klog.Errorf("failure to set resourceVersion to %d on bookmark event %+v", bookmarkEvent.ResourceVersion, bookmarkEvent.Object)
				continue
			}
			c.dispatchEvent(bookmarkEvent)
		case <-c.stopCh:
			return
		}
	}
}

// setCachingObjects 为事件中的对象套上 cachingObject，使同一事件被多个 watcher 序列化时只编码一次。
//
// Delete 事件只包装 PrevObject，因为只有它会被序列化下发。
func setCachingObjects(event *watchCacheEvent, versioner storage.Versioner) {
	switch event.Type {
	case watch.Added, watch.Modified:
		if object, err := newCachingObject(event.Object); err == nil {
			event.Object = object
		} else {
			klog.Errorf("couldn't create cachingObject from: %#v", event.Object)
		}
		// 不包装 update 事件的 PrevObject（create 事件中它为 nil）。
		// 我们只在需要投递 DELETE watch 事件时编码它们，因此当
		// event.Object 非 nil 时，它只可能被这类 watcher 使用：
		// 其选择器对对象的旧版本满足、对当前版本
		// 不再满足。
		// 这种情况足够罕见，不值得每次都为此对象
		// 做深拷贝（由 newCachingObject 完成）。
	case watch.Deleted:
		// 不包装 delete 事件的 Object——这些事件不用于投递
		// 任何事件。只包装 PrevObject。
		if object, err := newCachingObject(event.PrevObject); err == nil {
			// 更新对象的 resource version。
			// event.PrevObject 用于投递 DELETE watch 事件，
			// 对于这类事件，我们把 resourceVersion 设为 <当前值>，而不是
			// 对象最后一次修改时的 resourceVersion。
			updateResourceVersion(object, versioner, event.ResourceVersion)
			event.PrevObject = object
		} else {
			klog.Errorf("couldn't create cachingObject from: %#v", event.Object)
		}
	}
}

// dispatchEvent 把事件投递给所有相关 watcher。
//
// 先用非阻塞方式投递，快的 watcher 不会被慢的拖累；只有缓冲区满的 watcher
// 才在时间预算内等待，超时后直接关闭该 watcher（慢客户端不应拖垮整个 cacher）。
func (c *Cacher) dispatchEvent(event *watchCacheEvent) {
	c.startDispatching(event)
	defer c.finishDispatching()
	// startDispatching 之后被停止的 watcher 会推迟到 finishDispatching 处理。

	// 由于 add() 可能阻塞，我们在 cacher 未持锁时显式执行投递。
	// 先以非阻塞方式分发事件，使快的 watcher
	// 不会被慢的 watcher 阻塞。
	if event.Type == watch.Bookmark {
		for _, watcher := range c.watchersBuffer {
			watcher.nonblockingAdd(event)
		}
	} else {
		// 仅为分发此事件而启用对象序列化的缓存。
		//
		// 把序列化结果常驻内存会增加内存占用，
		// 但它有助于为从旧版本发起的 watch 缓存编码结果。
		// 不过我们仍然没有令人信服的数据表明其收益
		// 能抵得上内存占用的增加，所以目前我们在分发完此事件后
		// 就丢弃缓存的序列化结果。
		//
		// 鉴于 CachingObject 只是包装对象而并不执行
		// 深拷贝（直到某个字段被显式修改），我们无条件
		// 创建它，以保证安全性并减少深拷贝。
		//
		// 做一次浅拷贝以允许覆写 Object 和 PrevObject。
		wcEvent := *event
		setCachingObjects(&wcEvent, c.versioner)
		event = &wcEvent

		c.blockedWatchers = c.blockedWatchers[:0]
		for _, watcher := range c.watchersBuffer {
			if !watcher.nonblockingAdd(event) {
				c.blockedWatchers = append(c.blockedWatchers, watcher)
			}
		}

		if len(c.blockedWatchers) > 0 {
			// dispatchEvent 被非常频繁地调用，因此安排
			// 复用 timer 而不是不断分配。
			startTime := time.Now()
			timeout := c.dispatchTimeoutBudget.takeAvailable()
			c.timer.Reset(timeout)

			// 向所有被阻塞的 watcher 发送事件。只要 timer 还在运行，
			// `add` 就会等待 watcher 解除阻塞。超时后，
			// `add` 不再等待，而是立即关闭仍未解除阻塞的 watcher。
			// 因此，每个 watcher 都有机会在 timer 运行期间
			// 自行解除阻塞，而不仅仅是列表靠前的那些。
			timer := c.timer
			for _, watcher := range c.blockedWatchers {
				if !watcher.add(event, timer) {
					// timer 已触发，将其置为 nil 以清理。
					timer = nil
				}
			}

			// 若 timer 未触发则停止它
			if timer != nil && !timer.Stop() {
				// 消费已触发（但尚未被接收）的 timer 事件，
				// 以免将来复用时出现虚假超时。
				<-timer.C
			}

			c.dispatchTimeoutBudget.returnUnused(timeout - time.Since(startTime))
		}
	}
}

// startDispatchingBookmarkEventsLocked 把到期的 bookmark watcher 加入本轮的投递列表。
func (c *Cacher) startDispatchingBookmarkEventsLocked() {
	// 弹出已到期的 watcher，但显式跳过已停止的，
	// 因为 watcher 停止时我们不会把它从 bookmarkWatchers 中删除。
	for _, watchers := range c.bookmarkWatchers.popExpiredWatchersThreadUnsafe() {
		for _, watcher := range watchers {
			// 此处持有 c.Lock()。
			// watcher.stopThreadUnsafe() 由 c.Lock() 保护
			if watcher.stopped {
				continue
			}
			c.watchersBuffer = append(c.watchersBuffer, watcher)
			c.expiredBookmarkWatchers = append(c.expiredBookmarkWatchers, watcher)
		}
	}
}

// startDispatching 选出可能关心给定事件的 watcher，
// 并把 dispatching 标记为 true。
//
// 通过 namespace/name 与索引值两套索引快速收敛候选 watcher，避免 O(全部 watcher) 遍历。
func (c *Cacher) startDispatching(event *watchCacheEvent) {
	// 在这里调用 triggerValuesThreadUnsafe 是安全的，因为此时
	// 只有本线程能访问该事件（我们每次分发都会
	// 创建独立的 watchCacheEvent）。
	triggerValues, supported := c.triggerValuesThreadUnsafe(event)

	c.Lock()
	defer c.Unlock()

	c.dispatching = true
	// 我们复用该切片，以避免在每次
	// dispatchEvent() 调用中重新分配内存。这可能使 Go GC 无法释放
	// 之前阶段遗留在当前切片长度之后的
	// 元素，但这样的元素数量有限，
	// 而避免内存分配带来的收益要大得多。
	c.watchersBuffer = c.watchersBuffer[:0]

	if event.Type == watch.Bookmark {
		c.startDispatchingBookmarkEventsLocked()
		// 在此返回，以减少后续代码的缩进和 diff
		return
	}

	// 对每个适用的 namespace/name 组合遍历 watcher
	namespace := event.ObjFields["metadata.namespace"]
	name := event.ObjFields["metadata.name"]
	if len(namespace) > 0 {
		if len(name) > 0 {
			// 按 name 限定作用域的 namespaced watcher
			for _, watcher := range c.watchers.allWatchers[namespacedName{namespace: namespace, name: name}] {
				c.watchersBuffer = append(c.watchersBuffer, watcher)
			}
		}
		// 未按 name 限定作用域的 namespaced watcher
		for _, watcher := range c.watchers.allWatchers[namespacedName{namespace: namespace}] {
			c.watchersBuffer = append(c.watchersBuffer, watcher)
		}
	}
	if len(name) > 0 {
		// 按 name 限定作用域的 cluster-wide watcher
		for _, watcher := range c.watchers.allWatchers[namespacedName{name: name}] {
			c.watchersBuffer = append(c.watchersBuffer, watcher)
		}
	}
	// 未按 name 限定作用域的 cluster-wide watcher
	for _, watcher := range c.watchers.allWatchers[namespacedName{}] {
		c.watchersBuffer = append(c.watchersBuffer, watcher)
	}

	if supported {
		// 遍历关心 trigger 给定取值的 watcher。
		for _, triggerValue := range triggerValues {
			for _, watcher := range c.watchers.valueWatchers[triggerValue] {
				c.watchersBuffer = append(c.watchersBuffer, watcher)
			}
		}
	} else {
		// supported 为 false 通常意味着 trigger 函数
		// 未定义（或不感知任何索引）。这种情况下，
		// watcher 过滤器通常也不应生成任何
		// trigger 取值，但某些错误配置仍可能引发问题，
		// 因此出于谨慎我们保留这个分支。

		// 遍历对所有精确取值感兴趣的 watcher。
		for _, watchers := range c.watchers.valueWatchers {
			for _, watcher := range watchers {
				c.watchersBuffer = append(c.watchersBuffer, watcher)
			}
		}
	}
}

// finishDispatching 停止所有在此期间本应停止、但被推迟的
// watcher——推迟是为了避免关闭 watcher 的输入通道，
// 因为 add() 可能仍在向其写入。
// 它同时把 dispatching 标记为 false。
//
// 延迟停止 watcher 是为了避免与 add() 并发关闭 channel（会 panic）。
func (c *Cacher) finishDispatching() {
	c.Lock()
	defer c.Unlock()
	c.dispatching = false
	for _, watcher := range c.watchersToStop {
		watcher.stopLocked()
	}
	c.watchersToStop = c.watchersToStop[:0]

	for _, watcher := range c.expiredBookmarkWatchers {
		if watcher.stopped {
			continue
		}
		// 如有需要，把 watcher 重新入队以等待下一次 bookmark。
		c.bookmarkWatchers.addWatcherThreadUnsafe(watcher)
	}
	c.expiredBookmarkWatchers = c.expiredBookmarkWatchers[:0]
}

// terminateAllWatchers 终止所有 watcher；缓存重建与 Stop 时调用。
func (c *Cacher) terminateAllWatchers() {
	c.Lock()
	defer c.Unlock()
	c.watchers.terminateAll(c.groupResource, c.stopWatcherLocked)
}

// stopWatcherLocked 在分发过程中延迟停止，避免与正在进行的 add 冲突。
func (c *Cacher) stopWatcherLocked(watcher *cacheWatcher) {
	if c.dispatching {
		c.watchersToStop = append(c.watchersToStop, watcher)
	} else {
		watcher.stopLocked()
	}
}

// isStopped 返回 cacher 是否已停止。
func (c *Cacher) isStopped() bool {
	c.stopLock.RLock()
	defer c.stopLock.RUnlock()
	return c.stopped
}

// Compact 把 etcd 压缩请求透传给底层存储。
func (c *Cacher) Compact(resourceVersion string) error {
	rv, err := c.versioner.ParseResourceVersion(resourceVersion)
	if err != nil {
		return err
	}
	c.watchCache.storage.Compact(rv)
	return nil
}

// MarkConsistent 透传一致性标记给底层存储。
func (c *Cacher) MarkConsistent(consistent bool) {
	c.watchCache.storage.MarkConsistent(consistent)
}

// Stop 实现优雅终止。
//
// 幂等：cacher 会与子资源共享，重复 Stop 是正常情况，直接返回。
func (c *Cacher) Stop() {
	c.stopLock.Lock()
	if c.stopped {
		// 避免停止两次（注意：cacher 会与子资源共享）
		c.stopLock.Unlock()
		return
	}
	c.stopped = true
	c.ready.stop()
	c.stopLock.Unlock()
	close(c.stopCh)
	c.stopWg.Wait()
}

// prepareKey 为 key 加上资源前缀；recursive 表示是否按前缀匹配子资源。
func (c *Cacher) prepareKey(key string, recursive bool) (string, error) {
	return storage.PrepareKey(c.resourcePrefix, key, recursive)
}

// forgetWatcher 返回 watcher 的清理函数：从索引移除并停止。
//
// 可能被多次调用（Stop 与 terminateAllWatchers 竞争），因此底层操作必须幂等。
func forgetWatcher(c *Cacher, w *cacheWatcher, index int, scope namespacedName, triggerValue string, triggerSupported bool) func(bool) {
	return func(drainWatcher bool) {
		c.Lock()
		defer c.Unlock()

		w.setDrainInputBufferLocked(drainWatcher)

		// 该 watcher 有可能已经不在结构中（例如 Stop() 与
		// terminateAllWatchers() 同时发生的情形），但对同一个
		// watcher 多次调用 stopLocked() 是安全的。
		c.watchers.deleteWatcher(index, scope, triggerValue, triggerSupported)
		c.stopWatcherLocked(w)
	}
}

// filterWithAttrsAndPrefixFunction 组合三层过滤：key 前缀、分片选择器、标签/字段选择器。
//
// 分片匹配出错时按“不匹配”处理并记录错误：分片配置问题不应导致事件被错误下发。
func filterWithAttrsAndPrefixFunction(prefix string, p storage.SelectionPredicate, groupResource schema.GroupResource) filterWithAttrsFunc {
	isSharded := utilfeature.DefaultFeatureGate.Enabled(features.ShardedListAndWatch) && p.ShardSelector != nil && !p.ShardSelector.Empty()
	filterFunc := func(objKey string, label labels.Set, field fields.Set, obj runtime.Object) bool {
		if !key.HasPathPrefix(objKey, prefix) {
			return false
		}
		if isSharded {
			matches, err := p.MatchesSharding(obj)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("shard matching failed for %v: %w", groupResource, err))
				return false
			}
			if !matches {
				metrics.RecordWatchFilteredEvent(groupResource)
				return false
			}
		}
		return p.MatchesObjectAttributes(label, field)
	}
	return filterFunc
}

// LastSyncResourceVersion 返回底层缓存已同步到的 resource version。
//
// 缓存未就绪时返回 503，调用方（如健康检查）据此判断是否可用。
func (c *Cacher) LastSyncResourceVersion() (uint64, error) {
	if err := c.ready.wait(context.Background()); err != nil {
		return 0, errors.NewServiceUnavailable(err.Error())
	}

	resourceVersion := c.reflector.LastSyncResourceVersion()
	return c.versioner.ParseResourceVersion(resourceVersion)
}

// getBookmarkAfterResourceVersionLockedFunc 返回一个函数，
// 该函数给出其后将投递 bookmark 事件的 ResourceVersion。
//
// 返回的函数必须在持有 watchCache 锁时调用。
//
// 返回闭包而不是值，是因为其中的 watchCache.resourceVersion 必须在真正使用时
// （持锁状态下）读取，才能保证与事件区间一致。
func (c *Cacher) getBookmarkAfterResourceVersionLockedFunc(parsedResourceVersion, requiredResourceVersion uint64, opts storage.ListOptions) (func() uint64, error) {
	if !isListWatchRequest(opts) {
		return func() uint64 { return 0 }, nil
	}

	switch {
	case len(opts.ResourceVersion) == 0:
		return func() uint64 { return requiredResourceVersion }, nil
	case parsedResourceVersion == 0:
		// 此处假定已经持有 watchCache 锁
		return func() uint64 { return c.watchCache.resourceVersion }, nil
	default:
		return func() uint64 { return parsedResourceVersion }, nil
	}
}

// isListWatchRequest 在 staging/src/k8s.io/apiserver/pkg/endpoints/handlers/get.go 中有对应实现
//
// 只有同时要求初始事件与 bookmark 的请求才是 watch-list 语义。
func isListWatchRequest(opts storage.ListOptions) bool {
	return opts.SendInitialEvents != nil && *opts.SendInitialEvents && opts.Predicate.AllowWatchBookmarks
}

// getWatchCacheResourceVersion 返回 watch 缓存必须同步到的 ResourceVersion
//
// 取决于输入参数，返回的 ResourceVersion 的语义为：
//   - 必须等于 Exact RV（当 parsedWatchResourceVersion > 0 时）
//   - 可以是 Any RV（当 parsedWatchResourceVersion = 0 时）
//   - 必须等于 Most Recent RV（从 etcd 返回一个 RV）
//
// 注意：上述语义由 API 校验（定义在其他位置）强制保证：
//
//	if SendInitiaEvents != nil => ResourceVersionMatch = NotOlderThan
//	if ResourceVersionmatch != nil => ResourceVersionMatch = NotOlderThan & SendInitialEvents != nil
func (c *Cacher) getWatchCacheResourceVersion(ctx context.Context, parsedWatchResourceVersion uint64, opts storage.ListOptions) (uint64, error) {
	if len(opts.ResourceVersion) != 0 {
		return parsedWatchResourceVersion, nil
	}
	// 遗留（legacy）场景
	if opts.SendInitialEvents == nil && opts.ResourceVersion == "" {
		return 0, nil
	}
	rv, err := c.storage.GetCurrentResourceVersion(ctx)
	return rv, err
}

// waitUntilWatchCacheFreshAndForceAllEvents 在请求了 sendInitialEvents 时，
// 等待缓存至少与给定的 requestedWatchRV 一样新；
// 否则允许直接建立连接，因为客户端
// 无需不必要的阻塞即可等待事件。
//
// 仅在需要回放全量初始事件时才等待缓存追平；普通 watch 允许立即建立连接。
func (c *Cacher) waitUntilWatchCacheFreshAndForceAllEvents(ctx context.Context, requestedWatchRV uint64, opts storage.ListOptions) error {
	if opts.SendInitialEvents != nil && *opts.SendInitialEvents {
		// 前方有坑（here be dragons）：
		// 由于 etcd 特性检查器需要检查所有成员
		// 才能确定某特性是否受支持，
		// 即使该特性不受支持，我们也可能收到肯定响应。
		//
		// 在这种极罕见的场景下，最坏情况是该
		// 请求会等待 3 秒后才失败。
		span := tracing.SpanFromContext(ctx)
		consistentReadSupported := delegator.ConsistentReadSupported()
		c.watchCache.RLock()
		span.AddEvent("watchCache locked acquired")
		defer c.watchCache.RUnlock()
		err := c.watchCache.waitUntilFreshLocked(ctx, consistentReadSupported, requestedWatchRV)
		if err != nil {
			return err
		}
		span.AddEvent("watchCache fresh enough")
		return nil
	}
	return nil
}

// Wait 阻塞直到 cacher 就绪（Ready）或已停止（Stopped），若已停止则返回错误。
func (c *Cacher) Wait(ctx context.Context) error {
	return c.ready.wait(ctx)
}

// setInitialEventsEndBookmarkIfRequested 为 watchlist 请求在 watchCacheInterval 中设置 initialEventsEndBookmark 字段
//
// bookmark 的 RV 必须是“初始事件区间结束时”的 RV，客户端据此知道可以从哪里继续 watch。
func (c *Cacher) setInitialEventsEndBookmarkIfRequested(cacheInterval *watchCacheInterval, opts storage.ListOptions, currentResourceVersion uint64) {
	if opts.SendInitialEvents != nil && *opts.SendInitialEvents && opts.Predicate.AllowWatchBookmarks {
		// 无需为该 bookmark 事件设置 InitialEventsAnnotation，
		// 因为它会在 cacheWatcher.convertToWatchEvent 方法中
		// 转换事件时被自动设置
		initialEventsEndBookmark := &watchCacheEvent{
			Type:            watch.Bookmark,
			Object:          c.newFunc(),
			ResourceVersion: currentResourceVersion,
		}

		if err := c.versioner.UpdateObject(initialEventsEndBookmark.Object, initialEventsEndBookmark.ResourceVersion); err != nil {
			klog.Errorf("failure to set resourceVersion to %d on initialEventsEndBookmark event %+v for watchlist request and wait for bookmark trigger to send", initialEventsEndBookmark.ResourceVersion, initialEventsEndBookmark.Object)
			initialEventsEndBookmark = nil
		}

		cacheInterval.initialEventsEndBookmark = initialEventsEndBookmark
	}
}

// getKeys 返回缓存中的全部 key，供底层存储做大小估算。
//
// 先取当前 RV 再等缓存追平到该 RV，保证 key 集合与 RV 一致。
func (c *Cacher) getKeys(ctx context.Context) ([]string, error) {
	ctx, span := tracing.Start(ctx, "cacher.getKeys",
		attribute.String("audit-id", audit.GetAuditIDTruncated(ctx)))
	defer span.End(500 * time.Millisecond)
	rev, err := c.storage.GetCurrentResourceVersion(ctx)
	if err != nil {
		return nil, err
	}
	span.AddEvent("GetCurrentResourceVersion succeed", attribute.Int64("resource-version", int64(rev)))
	return c.watchCache.WaitUntilFreshAndGetKeys(ctx, rev)
}

// Ready 返回缓存是否已就绪。
func (c *Cacher) Ready() bool {
	_, err := c.ready.check()
	return err == nil
}

// errWatcher 以实现 watch.Interface 的方式返回单个错误
//
// 用于“watch 已建立但立即失败”的场景：客户端先收到 200，再在事件流中收到 Error 事件。
type errWatcher struct {
	result chan watch.Event
}

// newErrWatcher 把错误包装成 watch.Error 事件；若错误本身是 runtime.Object 或
// StatusError 则保留其结构，否则构造通用 500 Status。
func newErrWatcher(err error) *errWatcher {
	// 创建一个错误事件
	errEvent := watch.Event{Type: watch.Error}
	switch err := err.(type) {
	case runtime.Object:
		errEvent.Object = err
	case *errors.StatusError:
		errEvent.Object = &err.ErrStatus
	default:
		errEvent.Object = &metav1.Status{
			Status:  metav1.StatusFailure,
			Message: err.Error(),
			Reason:  metav1.StatusReasonInternalError,
			Code:    http.StatusInternalServerError,
		}
	}

	// 创建一个容量为单个事件的 watcher，填入事件，然后关闭通道
	watcher := &errWatcher{result: make(chan watch.Event, 1)}
	watcher.result <- errEvent
	close(watcher.result)

	return watcher
}

// ShouldDelegateExactRV 判断指定 RV 的请求是否应交给底层存储处理。
//
// 只有递归查询且底层启用了快照时，缓存才有可能服务精确 RV；否则一律委托。
func (c *Cacher) ShouldDelegateExactRV(resourceVersion string, recursive bool) (delegator.Result, error) {
	// 不支持 Non-Recursive，直到 WaitUntilFreshAndGet 实现了精确 RV 为止。
	if !recursive || !c.watchCache.storage.SnapshottingEnabled() {
		return delegator.Result{ShouldDelegate: true}, nil
	}
	listRV, err := c.versioner.ParseResourceVersion(resourceVersion)
	if err != nil {
		return delegator.Result{}, err
	}
	return c.shouldDelegateExactRV(listRV)
}

// ShouldDelegateContinue 判断带 continue token 的翻页请求是否应委托给底层存储。
func (c *Cacher) ShouldDelegateContinue(continueToken string, recursive bool) (delegator.Result, error) {
	// 不支持 Non-Recursive，直到 WaitUntilFreshAndGet 实现了精确 RV 为止。
	if !recursive || !c.watchCache.storage.SnapshottingEnabled() {
		return delegator.Result{ShouldDelegate: true}, nil
	}
	_, continueRV, err := storage.DecodeContinue(continueToken, c.resourcePrefix)
	if err != nil {
		return delegator.Result{}, err
	}
	if continueRV > 0 {
		return c.shouldDelegateExactRV(uint64(continueRV))
	} else {
		// 携带负数 RV 的 Continue 属于一致读。
		return c.ShouldDelegateConsistentRead()
	}
}

// shouldDelegateExactRV 判断缓存能否服务精确 RV；缓存未追平未来 RV 时，
// 只有底层支持一致读才可能委托。
func (c *Cacher) shouldDelegateExactRV(rv uint64) (delegator.Result, error) {
	// 对未来的 revision 发起 Exact 请求需要一致读支持，但其本身并不是一致读。
	if c.watchCache.notFresh(rv) {
		return delegator.Result{
			ShouldDelegate: !delegator.ConsistentReadSupported(),
		}, nil
	}
	canServe := c.watchCache.storage.CanServeExactRV(rv)
	return delegator.Result{
		ShouldDelegate: !canServe,
	}, nil
}

// ShouldDelegateConsistentRead 一致读必须由底层存储提供；仅当底层不支持时标记委托。
func (c *Cacher) ShouldDelegateConsistentRead() (delegator.Result, error) {
	return delegator.Result{
		ConsistentRead: true,
		ShouldDelegate: !delegator.ConsistentReadSupported(),
	}, nil
}

// 实现了 watch.Interface。
func (c *errWatcher) ResultChan() <-chan watch.Event {
	return c.result
}

// 实现了 watch.Interface。
func (c *errWatcher) Stop() {
	// 空操作（no-op）
}

// immediateCloseWatcher 实现了一个立即关闭的 watch.Interface
type immediateCloseWatcher struct {
	result chan watch.Event
}

// newImmediateCloseWatcher 返回一个已关闭的 watcher，用于“注册失败但请求本身合法”的场景。
func newImmediateCloseWatcher() *immediateCloseWatcher {
	watcher := &immediateCloseWatcher{result: make(chan watch.Event)}
	close(watcher.result)
	return watcher
}

// 实现了 watch.Interface。
func (c *immediateCloseWatcher) ResultChan() <-chan watch.Event {
	return c.result
}

// 实现了 watch.Interface。
func (c *immediateCloseWatcher) Stop() {
	// 空操作（no-op）
}
