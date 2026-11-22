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
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher/delegator"
	"k8s.io/apiserver/pkg/storage/cacher/metrics"
	"k8s.io/apiserver/pkg/storage/cacher/progress"
	"k8s.io/apiserver/pkg/storage/cacher/store"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	// blockTimeout determines how long we're willing to block the request
	// to wait for a given resource version to be propagated to cache,
	// before terminating request and returning Timeout error with retry
	// after suggestion.
	// 读取一致性由缓存追平 revision 保证；超过该时间仍未追平则返回
	// TooLargeResourceVersion，让客户端带 Retry-After 重试。
	blockTimeout = 3 * time.Second

	// resourceVersionTooHighRetrySeconds is the seconds before a operation should be retried by the client
	// after receiving a 'too high resource version' error.
	resourceVersionTooHighRetrySeconds = 1
)

// watchCacheEvent is a single "watch event" that is send to users of
// watchCache. Additionally to a typical "watch.Event" it contains
// the previous value of the object to enable proper filtering in the
// upper layers.
// 保留前后两个版本的标签/字段，是为了让上层能在不反序列化对象的情况下
// 判断事件是否因选择器变化而需要转成 Added/Deleted（例如标签被移除）。
type watchCacheEvent struct {
	Type            watch.EventType
	Object          runtime.Object
	ObjLabels       labels.Set
	ObjFields       fields.Set
	PrevObject      runtime.Object
	PrevObjLabels   labels.Set
	PrevObjFields   fields.Set
	Key             string
	ResourceVersion uint64
	RecordTime      time.Time
	// CacheReceived is the time the event was received by the cacher.
	CacheReceived time.Time
}

// watchCache implements a Store interface.
// However, it depends on the elements implementing runtime.Object interface.
//
// watchCache is a "sliding window" (with a limited capacity) of objects
// observed from a watch.
// watchCache 同时维护“当前对象索引”和“最近的 revision 历史事件窗口”：
// list/get 走前者，watch 从后者按 revision 起始回放，窗口越界则要求
// 调用方回退到 etcd。
type watchCache struct {
	sync.RWMutex

	// Condition on which lists are waiting for the fresh enough
	// resource version.
	// 等待者持有读锁并 cond.Wait；事件写入方拿写锁后 Broadcast。
	cond *sync.Cond

	// ResourceVersion up to which the watchCache is propagated.
	resourceVersion uint64

	// This handler is run at the end of every successful Replace() method.
	onReplace func()

	history *watchCacheHistory
	storage *store.WatchCacheStorage

	config *ImmutableWatchCacheConfig
}

// ImmutableWatchCacheConfig 是构造后不再修改的配置，集中承载函数指针与
// 依赖对象，使 watchCache 本体只保留可变状态。
type ImmutableWatchCacheConfig struct {
	// keyFunc is used to get a key in the underlying storage for a given object.
	keyFunc func(runtime.Object) (string, error)

	// getAttrsFunc is used to get labels and fields of an object.
	getAttrsFunc func(runtime.Object) (labels.Set, fields.Set, error)

	// This handler is run at the end of every Add/Update/Delete method
	// and additionally gets the previous value of the object.
	eventHandler func(*watchCacheEvent)

	// for testing timeouts.
	clock clock.Clock

	// An underlying storage.Versioner.
	versioner storage.Versioner

	// cacher's group resource
	groupResource schema.GroupResource

	// For testing cache interval invalidation.
	indexValidator indexValidator

	// Requests progress notification if there are requests waiting for watch
	// to be fresh
	waitingUntilFresh *progress.ConditionalProgressRequester

	getCurrentRV func(context.Context) (uint64, error)
}

// newWatchCache 组装缓存：cond 绑定的是 RWMutex 的读锁，因此等待者之间
// 可以并发阻塞，而写入方独占更新。indexValidator 依赖 history，故在
// 构造完 history 后再赋值。
func newWatchCache(
	keyFunc func(runtime.Object) (string, error),
	eventHandler func(*watchCacheEvent),
	getAttrsFunc func(runtime.Object) (labels.Set, fields.Set, error),
	versioner storage.Versioner,
	indexers *cache.Indexers,
	clock clock.WithTicker,
	eventFreshDuration time.Duration,
	groupResource schema.GroupResource,
	progressRequester *progress.ConditionalProgressRequester,
	getCurrentRV func(context.Context) (uint64, error),
) *watchCache {
	config := &ImmutableWatchCacheConfig{
		keyFunc:           keyFunc,
		getAttrsFunc:      getAttrsFunc,
		eventHandler:      eventHandler,
		clock:             clock,
		versioner:         versioner,
		groupResource:     groupResource,
		waitingUntilFresh: progressRequester,
		getCurrentRV:      getCurrentRV,
	}

	wc := &watchCache{
		resourceVersion: 0,
		config:          config,
		history:         newWatchCacheHistory(config, eventFreshDuration),
		storage:         store.NewWatchCacheStorage(config.keyFunc, indexers),
	}
	wc.cond = sync.NewCond(wc.RLocker())
	wc.config.indexValidator = wc.history.isIndexValidLocked

	return wc
}

// Add takes runtime.Object as an argument.
// 把 reflector 的 Added 通知转换为缓存事件；resourceVersion 从对象元数据中取，
// 保证缓存 revision 与事件一致。
func (w *watchCache) Add(obj interface{}) error {
	object, resourceVersion, err := w.objectToVersionedRuntimeObject(obj)
	if err != nil {
		return err
	}
	event := watch.Event{Type: watch.Added, Object: object}

	return w.processEvent(event, resourceVersion)
}

// Update takes runtime.Object as an argument.
func (w *watchCache) Update(obj interface{}) error {
	object, resourceVersion, err := w.objectToVersionedRuntimeObject(obj)
	if err != nil {
		return err
	}
	event := watch.Event{Type: watch.Modified, Object: object}

	return w.processEvent(event, resourceVersion)
}

// Delete takes runtime.Object as an argument.
func (w *watchCache) Delete(obj interface{}) error {
	object, resourceVersion, err := w.objectToVersionedRuntimeObject(obj)
	if err != nil {
		return err
	}
	event := watch.Event{Type: watch.Deleted, Object: object}

	return w.processEvent(event, resourceVersion)
}

// objectToVersionedRuntimeObject 校验类型并解析 resourceVersion；任何失败都
// 直接返回错误而不是跳过事件，否则缓存会与 etcd 静默分叉。
func (w *watchCache) objectToVersionedRuntimeObject(obj interface{}) (runtime.Object, uint64, error) {
	object, ok := obj.(runtime.Object)
	if !ok {
		return nil, 0, fmt.Errorf("obj does not implement runtime.Object interface: %v", obj)
	}
	resourceVersion, err := w.config.versioner.ObjectResourceVersion(object)
	if err != nil {
		return nil, 0, err
	}
	return object, resourceVersion, nil
}

// processEvent is safe as long as there is at most one call to it in flight
// at any point in time.
// 这个串行前提来自 reflector：它从单一 goroutine 同步回调 Add/Update/Delete，
// 因此更新 history/storage/revision 不需要额外的每事件锁顺序协议。
func (w *watchCache) processEvent(event watch.Event, resourceVersion uint64) error {
	// recordTime 由事件来源提供（可能来自 etcd），cacheReceived 是本进程
	// 收到事件的时刻；两者用于区分“事件发生时间”与“缓存观测时间”。
	cacheReceived := w.config.clock.Now()
	recordTime := cacheReceived
	if withRecordTime, ok := event.Object.(storage.WatchEventWithRecordTime); ok {
		recordTime = withRecordTime.RecordTime()
		event.Object = withRecordTime.Unwrap()
	}

	metrics.EventsReceivedCounter.WithLabelValues(w.config.groupResource.Group, w.config.groupResource.Resource).Inc()

	key, err := w.config.keyFunc(event.Object)
	if err != nil {
		return fmt.Errorf("couldn't compute key: %v", err)
	}
	elem := &store.Element{Key: key, Object: event.Object}
	elem.Labels, elem.Fields, err = w.config.getAttrsFunc(event.Object)
	if err != nil {
		return err
	}

	wcEvent := &watchCacheEvent{
		Type:            event.Type,
		Object:          elem.Object,
		ObjLabels:       elem.Labels,
		ObjFields:       elem.Fields,
		Key:             key,
		ResourceVersion: resourceVersion,
		RecordTime:      recordTime,
		CacheReceived:   cacheReceived,
	}

	// We can call w.storage.Get() outside of a critical section,
	// because the w.storage itself is thread-safe and the only
	// place where it is modified is below (via UpdateStoreLocked)
	// and these calls are serialized because reflector is processing
	// events one-by-one.
	previous, exists, err := w.storage.Get(event.Object)
	if err != nil {
		return err
	}
	if exists {
		previousElem := previous.(*store.Element)
		wcEvent.PrevObject = previousElem.Object
		wcEvent.PrevObjLabels = previousElem.Labels
		wcEvent.PrevObjFields = previousElem.Fields
	}

	// 写锁内一次性完成历史追加、revision 推进与对象索引更新，避免读者
	// 看到“revision 已前进但历史尚未落盘”的中间态。
	if err := func() error {
		w.Lock()
		defer w.Unlock()

		w.history.updateCache(wcEvent)
		w.resourceVersion = resourceVersion
		defer w.cond.Broadcast()

		// 历史窗口满时压缩过期快照，限制内存并让基于旧 revision 的
		// list/watch 明确地失败而不是读到不完整历史。
		if w.history.isCacheFullLocked() {
			oldestRV := w.history.OldestResourceVersionLocked()
			w.storage.CompactSnapshotsLocked(oldestRV)
		}
		if err := w.storage.UpdateStoreLocked(event.Type, elem, resourceVersion); err != nil {
			return err
		}
		return nil
	}(); err != nil {
		return err
	}

	// Avoid calling event handler under lock.
	// This is safe as long as there is at most one call to Add/Update/Delete and
	// UpdateResourceVersion in flight at any point in time, which is true now,
	// because reflector calls them synchronously from its main thread.
	// 处理器在锁外调用，避免下游回调（可能较慢）阻塞所有读请求。
	if w.config.eventHandler != nil {
		w.config.eventHandler(wcEvent)
	}
	metrics.RecordResourceVersion(w.config.groupResource, resourceVersion)
	return nil
}

// UpdateResourceVersion 只在 bookmark/进度通知时推进 revision，不产生对象
// 变更事件；同样以 Bookmark 事件通知下游，使其能更新 lastObservedRV。
func (w *watchCache) UpdateResourceVersion(resourceVersion string) {
	rv, err := w.config.versioner.ParseResourceVersion(resourceVersion)
	if err != nil {
		klog.Errorf("Couldn't parse resourceVersion: %v", err)
		return
	}

	func() {
		w.Lock()
		defer w.Unlock()
		w.resourceVersion = rv
		w.cond.Broadcast()
	}()

	// Avoid calling event handler under lock.
	// This is safe as long as there is at most one call to Add/Update/Delete and
	// UpdateResourceVersion in flight at any point in time, which is true now,
	// because reflector calls them synchronously from its main thread.
	if w.config.eventHandler != nil {
		wcEvent := &watchCacheEvent{
			Type:            watch.Bookmark,
			ResourceVersion: rv,
		}
		w.config.eventHandler(wcEvent)
	}
	metrics.RecordResourceVersion(w.config.groupResource, rv)
}

// waitUntilFreshLocked waits until cache is at least as fresh as given resourceVersion.
// 调用方必须持有读锁。resourceVersion 为 0 表示接受任意（可能过期的）结果，
// 因此直接返回；否则在 cond 上等待，超时后返回 TooLargeResourceVersion。
func (w *watchCache) waitUntilFreshLocked(ctx context.Context, consistentReadSupported bool, resourceVersion uint64) error {
	if resourceVersion == 0 || resourceVersion <= w.resourceVersion {
		return nil
	}
	// 只有支持一致性读时才登记等待者：进度请求器会据此向 etcd 索取进度，
	// 否则没有意义且会增加 etcd 压力。
	if consistentReadSupported {
		w.config.waitingUntilFresh.Add()
		defer w.config.waitingUntilFresh.Remove()
	}
	startTime := w.config.clock.Now()
	defer func() {
		if resourceVersion > 0 {
			metrics.WatchCacheReadWait.WithContext(ctx).WithLabelValues(w.config.groupResource.Group, w.config.groupResource.Resource).Observe(w.config.clock.Since(startTime).Seconds())
		}
	}()

	// In case resourceVersion is 0, we accept arbitrarily stale result.
	// As a result, the condition in the below for loop will never be
	// satisfied (w.resourceVersion is never negative), this call will
	// never hit the w.cond.Wait().
	// As a result - we can optimize the code by not firing the wakeup
	// function (and avoid starting a gorotuine), especially given that
	// resourceVersion=0 is the most common case.
	// 仅在需要等待时才启动超时唤醒 goroutine：它会在 blockTimeout 后
	// Broadcast，让 Wait 循环有机会检查并返回超时错误。
	if resourceVersion > 0 {
		go func() {
			// Wake us up when the time limit has expired.  The docs
			// promise that time.After (well, NewTimer, which it calls)
			// will wait *at least* the duration given. Since this go
			// routine starts sometime after we record the start time, and
			// it will wake up the loop below sometime after the broadcast,
			// we don't need to worry about waking it up before the time
			// has expired accidentally.
			<-w.config.clock.After(blockTimeout)
			w.cond.Broadcast()
		}()
	}

	for w.resourceVersion < resourceVersion {
		if w.config.clock.Since(startTime) >= blockTimeout {
			// Request that the client retry after 'resourceVersionTooHighRetrySeconds' seconds.
			return storage.NewTooLargeResourceVersionError(resourceVersion, w.resourceVersion, resourceVersionTooHighRetrySeconds)
		}
		// Wait 会原子地释放读锁并阻塞，被 Broadcast 唤醒后重新获取读锁。
		w.cond.Wait()
	}
	return nil
}

// WaitUntilFreshAndGetList 根据 Recursive 选择“列表”还是“单键”读取路径；
// 两者都先等待缓存追平目标 revision。
func (c *watchCache) WaitUntilFreshAndGetList(ctx context.Context, key string, opts storage.ListOptions) (listResp, string, error) {
	if opts.Recursive {
		return c.waitUntilFreshAndList(ctx, key, opts)
	}
	return c.waitUntilFreshAndGetList(ctx, key, opts)
}

// waitUntilFreshAndGetList 读取单键（或前缀）结果。未显式指定 revision 时
// 向 etcd 请求当前 revision 以实现一致性读，而不是使用可能落后的缓存 revision。
func (c *watchCache) waitUntilFreshAndGetList(ctx context.Context, key string, opts storage.ListOptions) (listResp, string, error) {
	var listRV uint64
	var err error
	if opts.ResourceVersionMatch == "" && opts.ResourceVersion == "" {
		// Consistent read
		listRV, err = c.config.getCurrentRV(ctx)
		if err != nil {
			return listResp{}, "", err
		}
	} else {
		listRV, err = c.config.versioner.ParseResourceVersion(opts.ResourceVersion)
		if err != nil {
			return listResp{}, "", err
		}
	}
	obj, exists, readResourceVersion, err := c.WaitUntilFreshAndGet(ctx, listRV, key)
	if err != nil {
		return listResp{}, "", err
	}
	if exists {
		return listResp{Items: []interface{}{obj}, ResourceVersion: readResourceVersion}, "", nil
	}
	// 对象不存在也是成功结果，仍需返回读到的 revision 供客户端继续 watch。
	return listResp{ResourceVersion: readResourceVersion}, "", nil
}

// WaitUntilFreshAndList returns list of pointers to `storeElement` objects along
// with their ResourceVersion and the name of the index, if any, that was used.
// 原注释沿用了旧函数名（历史遗留）；本函数实际只返回键，供上层按需取对象，
// 减少一次性拷贝。
func (w *watchCache) WaitUntilFreshAndGetKeys(ctx context.Context, resourceVersion uint64) ([]string, error) {
	span := tracing.SpanFromContext(ctx)
	consistentReadSupported := delegator.ConsistentReadSupported()
	w.RLock()
	span.AddEvent("watchCache locked acquired")
	defer w.RUnlock()
	err := w.waitUntilFreshLocked(ctx, consistentReadSupported, resourceVersion)
	if err != nil {
		return nil, err
	}
	span.AddEvent("watchCache fresh enough")
	keys := w.storage.ListKeys()
	span.AddEvent("ListKeys success")
	return keys, nil
}

// NOTICE: Structure follows the shouldDelegateList function in
// staging/src/k8s.io/apiserver/pkg/storage/cacher/delegator.go
// 分支顺序与委派判定保持一致，避免出现“委派说能服务、缓存却拒绝”的分叉；
// 修改任一处都需同步另一处。
func (w *watchCache) waitUntilFreshAndList(ctx context.Context, key string, opts storage.ListOptions) (resp listResp, index string, err error) {
	listRV, err := w.config.versioner.ParseResourceVersion(opts.ResourceVersion)
	if err != nil {
		return listResp{}, "", err
	}
	switch opts.ResourceVersionMatch {
	case metav1.ResourceVersionMatchExact:
		return w.waitAndListExactRV(ctx, key, "", listRV)
	case metav1.ResourceVersionMatchNotOlderThan:
	case "":
		// Continue
		// 分页 continue token 内含起始键与 revision；revision 为 0 时无法
		// 精确定位，退化为一致性读。
		if len(opts.Predicate.Continue) > 0 {
			continueKey, continueRV, err := storage.DecodeContinue(opts.Predicate.Continue, key)
			if err != nil {
				return listResp{}, "", errors.NewBadRequest(fmt.Sprintf("invalid continue token: %v", err))
			}
			if continueRV > 0 {
				return w.waitAndListExactRV(ctx, key, continueKey, uint64(continueRV))
			} else {
				// Don't pass matchValues as they don't support continueKey
				return w.waitAndListConsistent(ctx, key, continueKey, nil)
			}
		}
		// Legacy exact match
		// 带 limit 且指定了非零 revision 的历史行为按精确快照处理。
		if opts.Predicate.Limit > 0 && len(opts.ResourceVersion) > 0 && opts.ResourceVersion != "0" {
			return w.waitAndListExactRV(ctx, key, "", listRV)
		}
		if opts.ResourceVersion == "" {
			return w.waitAndListConsistent(ctx, key, "", opts.Predicate.MatcherIndex(ctx))
		}
	}
	return w.waitAndListLatestRV(ctx, listRV, key, "", opts.Predicate.MatcherIndex(ctx))
}

// waitAndListExactRV 读取指定 revision 的不可变快照并列出前缀；快照已过期
// 时会返回错误，促使上层回退到 etcd。
func (w *watchCache) waitAndListExactRV(ctx context.Context, key, continueKey string, resourceVersion uint64) (resp listResp, index string, err error) {
	store, err := w.waitAndGetExactSnapshot(ctx, resourceVersion)
	if err != nil {
		return listResp{}, "", err
	}
	items, err := store.OrderedListPrefix(key, continueKey)
	return listResp{
		Items:           items,
		ResourceVersion: resourceVersion,
	}, "", err
}

// waitAndGetExactSnapshot 等待缓存追平 revision 后取该 revision 的快照；
// 期间持有读锁，保证快照与 revision 对应。
func (w *watchCache) waitAndGetExactSnapshot(ctx context.Context, resourceVersion uint64) (store.Snapshot, error) {
	span := tracing.SpanFromContext(ctx)
	consistentReadSupported := delegator.ConsistentReadSupported()
	w.RLock()
	span.AddEvent("watchCache locked acquired")
	defer w.RUnlock()
	err := w.waitUntilFreshLocked(ctx, consistentReadSupported, resourceVersion)
	if err != nil {
		return nil, err
	}
	span.AddEvent("watchCache fresh enough")

	store, err := w.storage.GetExactSnapshotLocked(resourceVersion)
	if err != nil {
		span.AddEvent("GetExactSnapshotLocked failed", attribute.String("error", err.Error()))
		return nil, err
	}
	span.AddEvent("GetExactSnapshotLocked success")
	return store, nil
}

// waitAndListConsistent 先取当前 revision 再走 latest 路径，等价于
// “读一致”语义：结果至少包含调用开始时的所有写入。
func (w *watchCache) waitAndListConsistent(ctx context.Context, key, continueKey string, matchValues []storage.MatchValue) (resp listResp, index string, err error) {
	span := tracing.SpanFromContext(ctx)
	resourceVersion, err := w.config.getCurrentRV(ctx)
	if err != nil {
		return listResp{}, "", err
	}
	span.AddEvent("getCurrentRV success")
	return w.waitAndListLatestRV(ctx, resourceVersion, key, continueKey, matchValues)
}

// waitAndListLatestRV 取得最新快照并列出结果；返回的 revision 可能与请求的
// minResourceVersion 不同（缓存更新），但一定不旧于它。
func (w *watchCache) waitAndListLatestRV(ctx context.Context, minResourceVersion uint64, key, continueKey string, matchValues []storage.MatchValue) (resp listResp, index string, err error) {
	snap, resourceVersion, index, err := w.waitAndGetLatestSnapshot(ctx, minResourceVersion, key, continueKey, matchValues)
	if err != nil {
		return listResp{}, "", err
	}
	items, err := snap.OrderedListPrefix(key, continueKey)
	if err != nil {
		return listResp{}, "", err
	}
	return listResp{
		Items:           items,
		ResourceVersion: resourceVersion,
	}, index, nil
}

// waitAndGetLatestSnapshot 优先用谓词给出的索引减少候选集；索引不可用时
// 回退到全量快照。索引只是预过滤，结果随后仍会经过完整谓词过滤。
func (w *watchCache) waitAndGetLatestSnapshot(ctx context.Context, minResourceVersion uint64, key, continueKey string, matchValues []storage.MatchValue) (snap store.Snapshot, resourceVersion uint64, index string, err error) {
	consistentReadSupported := delegator.ConsistentReadSupported()
	span := tracing.SpanFromContext(ctx)
	w.RLock()
	span.AddEvent("watchCache locked acquired")
	defer w.RUnlock()
	err = w.waitUntilFreshLocked(ctx, consistentReadSupported, minResourceVersion)
	if err != nil {
		return nil, 0, "", err
	}
	span.AddEvent("watchCache fresh enough")
	// This isn't the place where we do "final filtering" - only some "prefiltering" is happening here. So the only
	// requirement here is to NOT miss anything that should be returned. We can return as many non-matching items as we
	// want - they will be filtered out later. The fact that we return less things is only further performance improvement.
	// TODO: if multiple indexes match, return the one with the fewest items, so as to do as much filtering as possible.
	for _, matchValue := range matchValues {
		snap, err := w.storage.GetByIndexSnapshot(matchValue.IndexName, matchValue.Value)
		if err == nil {
			span.AddEvent("GetByIndexSnapshot success", attribute.String("index", matchValue.IndexName))
			return snap, w.resourceVersion, matchValue.IndexName, nil
		}
		span.AddEvent("GetByIndexSnapshot fail", attribute.String("index", matchValue.IndexName), attribute.String("error", err.Error()))
	}
	snap, err = w.storage.GetLatestSnapshotOrBuildLocked(key, continueKey)
	if err != nil {
		span.AddEvent("GetLatestSnapshotOrBuildLocked failed", attribute.String("error", err.Error()))
		return nil, 0, "", err
	}
	span.AddEvent("GetLatestSnapshotOrBuildLocked success")
	return snap, w.resourceVersion, "", nil
}

// notFresh 判断给定 revision 是否仍高于缓存已追平的位置。
func (w *watchCache) notFresh(resourceVersion uint64) bool {
	w.RLock()
	defer w.RUnlock()
	return resourceVersion > w.resourceVersion
}

// WaitUntilFreshAndGet returns a pointers to <storeElement> object.
// 返回的 revision 是本次读取实际观察到的缓存 revision，供调用方在响应中回写。
func (w *watchCache) WaitUntilFreshAndGet(ctx context.Context, resourceVersion uint64, key string) (interface{}, bool, uint64, error) {
	span := tracing.SpanFromContext(ctx)
	consistentReadSupported := delegator.ConsistentReadSupported()
	w.RLock()
	span.AddEvent("watchCache locked acquired")
	defer w.RUnlock()
	err := w.waitUntilFreshLocked(ctx, consistentReadSupported, resourceVersion)
	if err != nil {
		return nil, false, 0, err
	}
	span.AddEvent("watchCache fresh enough")
	value, exists, err := w.storage.GetByKey(key)
	if err != nil {
		span.AddEvent("GetByKey failed", attribute.String("error", err.Error()))
		return nil, false, 0, err
	}
	span.AddEvent("GetByKey success")
	return value, exists, w.resourceVersion, err
}

// Replace takes slice of runtime.Object as a parameter.
// 用一次全量 list 的结果替换缓存（初始同步或 re-list）。先在锁外完成所有
// key/属性计算，任何对象失败即整体放弃，保证缓存不会处于半替换状态。
func (w *watchCache) Replace(objs []interface{}, resourceVersion string) error {
	version, err := w.config.versioner.ParseResourceVersion(resourceVersion)
	if err != nil {
		return err
	}

	toReplace := make([]interface{}, 0, len(objs))
	for _, obj := range objs {
		object, ok := obj.(runtime.Object)
		if !ok {
			return fmt.Errorf("didn't get runtime.Object for replace: %#v", obj)
		}
		key, err := w.config.keyFunc(object)
		if err != nil {
			return fmt.Errorf("couldn't compute key: %v", err)
		}
		objLabels, objFields, err := w.config.getAttrsFunc(object)
		if err != nil {
			return err
		}
		toReplace = append(toReplace, &store.Element{
			Key:    key,
			Object: object,
			Labels: objLabels,
			Fields: objFields,
		})
	}

	w.Lock()
	defer w.Unlock()

	// Ensure startIndex never decreases, so that existing watchCacheInterval
	// instances get "invalid" errors if the try to download from the buffer
	// using their own start/end indexes calculated from previous buffer
	// content.

	// Empty the cyclic buffer, ensuring startIndex doesn't decrease.
	// Reset 而不是清零复用索引：递增的 startIndex 能让旧 interval 立即
	// 失效，避免它们把新缓冲区的内容当作旧 revision 读出。
	w.history.ResetLocked()

	if err := w.storage.ReplaceLocked(toReplace, resourceVersion, version); err != nil {
		return err
	}
	w.resourceVersion = version
	if w.onReplace != nil {
		w.onReplace()
	}
	// 唤醒所有等待该 revision 的读请求。
	w.cond.Broadcast()

	metrics.RecordResourceVersion(w.config.groupResource, version)
	klog.V(3).Infof("Replaced watchCache (rev: %v) ", resourceVersion)
	return nil
}

// SetOnReplace 注册替换完成后的回调；加锁保证与 Replace 的可见性一致。
func (w *watchCache) SetOnReplace(onReplace func()) {
	w.Lock()
	defer w.Unlock()
	w.onReplace = onReplace
}

// Resync 是 Store 接口要求的方法；watchCache 由事件持续维护，无需周期同步。
func (w *watchCache) Resync() error {
	// Nothing to do
	return nil
}

// getListResourceVersion 返回当前存储的 list revision，供 watch 起点校验。
func (w *watchCache) getListResourceVersion() uint64 {
	w.RLock()
	defer w.RUnlock()
	return w.storage.ListResourceVersion()
}

// suggestedWatchChannelSize 基于历史窗口大小推荐每个 watcher 的缓冲容量，
// 太小会频繁阻塞/丢事件，太大则浪费内存。
func (w *watchCache) suggestedWatchChannelSize(indexExists, triggerUsed bool) int {
	w.RLock()
	defer w.RUnlock()
	return w.history.suggestedWatchChannelSize(indexExists, triggerUsed)
}

// getAllEventsSinceLocked returns a watchCacheInterval that can be used to
// retrieve events since a certain resourceVersion. This function assumes to
// be called under the watchCache lock.
// getIntervalFromStoreLocked 在两种情况下使用：sendInitialEvents 需要完整
// 初始状态；resourceVersion=0 且未显式禁用初始事件时保持旧行为（先返回当前
// 状态再从此处 watch）。
func (w *watchCache) getAllEventsSinceLocked(resourceVersion uint64, key string, opts storage.ListOptions) (*watchCacheInterval, error) {
	_, matchesSingle := opts.Predicate.MatchesSingle()
	// 递归 list 不能按单键处理，否则会只返回一个对象。
	matchesSingle = matchesSingle && !opts.Recursive
	if opts.SendInitialEvents != nil && *opts.SendInitialEvents {
		return w.getIntervalFromStoreLocked(key, matchesSingle)
	}

	if resourceVersion == 0 {
		if opts.SendInitialEvents == nil {
			// resourceVersion = 0 means that we don't require any specific starting point
			// and we would like to start watching from ~now.
			// However, to keep backward compatibility, we additionally need to return the
			// current state and only then start watching from that point.
			//
			// TODO: In v2 api, we should stop returning the current state - #13969.
			return w.getIntervalFromStoreLocked(key, matchesSingle)
		}
		// SendInitialEvents = false and resourceVersion = 0
		// means that the request would like to start watching
		// from Any resourceVersion
		// 从当前 revision 起 watch，不回放历史。
		resourceVersion = w.resourceVersion
	}

	return w.history.GetIntervalLocked(resourceVersion, w.storage.ListResourceVersion(), w.RWMutex.RLocker())
}

// getIntervalFromStoreLocked returns a watchCacheInterval
// that covers the entire storage state.
// This function assumes to be called under the watchCache lock.
func (w *watchCache) getIntervalFromStoreLocked(key string, matchesSingle bool) (*watchCacheInterval, error) {
	// When not matching a single key, an immutable snapshot lets us
	// defer the O(N) interval build off the watchCache lock.
	// 单键查询数据量小，直接同步构建即可；非单键必须懒构建以免长时间持锁。
	if !matchesSingle {
		if snapshot, ok := w.storage.LatestSnapshotLocked(); ok {
			return newCacheIntervalFromLazySnapshot(w.resourceVersion, snapshot), nil
		}
	}
	return newCacheIntervalFromStore(w.resourceVersion, w.storage.StoreLocked(), key, matchesSingle)
}
