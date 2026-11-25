/*
Copyright 2023 The Kubernetes Authors.

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

	"k8s.io/utils/clock"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher/metrics"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"

	"k8s.io/klog/v2"
)

// cacheWatcher 可能的状态取值。
// 状态机的用途：客户端可以通过 resourceVersionMatch=NotOlderThan + 初始 RV
// 要求“先收到一个 RV 不早于该值的 bookmark，再开始接收事件”。
// 在收到该 bookmark 前，不能向客户端发送普通事件，否则会破坏 watch 契约。
const (
	// cacheWatcherWaitingForBookmark indicates the cacher
	// is waiting for a bookmark event with a specific RV set
	cacheWatcherWaitingForBookmark = iota

	// cacheWatcherBookmarkReceived indicates that the cacher
	// has received a bookmark event with required RV
	cacheWatcherBookmarkReceived

	// cacheWatcherBookmarkSent indicates that the cacher
	// has already sent a bookmark event to a client
	cacheWatcherBookmarkSent
)

// cacheWatcher 实现了 watch.Interface，把 watchCache 中的事件按过滤器转换为
// 客户端可见的 watch.Event。它是非线程安全的：除 state 由 stateMutex 保护外，
// 其余字段均由 Cacher 的锁或单一 goroutine 持有，调用方必须遵守既有的锁约定。
type cacheWatcher struct {
	input     chan *watchCacheEvent
	result    chan watch.Event
	done      chan struct{}
	filter    filterWithAttrsFunc
	stopped   bool
	forget    func(bool)
	versioner storage.Versioner
	// 服务端会在 deadline 之后关闭该 watcher，这里保存 deadline 以便在此之前
	// 主动发送 bookmark 事件，避免客户端在超时前无法确认进度。
	deadline            time.Time
	allowWatchBookmarks bool
	groupResource       schema.GroupResource
	watcherMetrics      *metrics.WatcherMetricsObservers
	clock               clock.Clock

	// human readable identifier that helps assigning cacheWatcher
	// instance with request
	identifier string

	// drainInputBuffer 表示是否应推迟关闭该 watcher，先把输入缓冲区中的事件全部
	// 发送出去。用于“已收到目标 bookmark 但尚未发送”的场景：先把缓冲区排空，
	// 客户端才能拿到它等待的 RV 并重建 watch。
	drainInputBuffer bool

	// bookmarkAfterResourceVersion holds an RV that indicates
	// when we should start delivering bookmark events.
	// If this field holds the value of 0 that means
	// we don't have any special preferences toward delivering bookmark events.
	// Note that this field is used in conjunction with the state field.
	// It should not be changed once the watcher has been started.
	bookmarkAfterResourceVersion uint64

	// stateMutex protects state
	stateMutex sync.Mutex

	// state holds a numeric value indicating the current state of the watcher
	state int
}

// newCacheWatcher 创建 watcher；缓冲区大小与过滤器由调用方决定。
// clk 为 nil 时使用真实时钟，便于生产与测试共用同一构造路径。
func newCacheWatcher(
	chanSize int,
	filter filterWithAttrsFunc,
	forget func(bool),
	versioner storage.Versioner,
	deadline time.Time,
	allowWatchBookmarks bool,
	groupResource schema.GroupResource,
	watcherMetrics *metrics.WatcherMetricsObservers,
	clk clock.Clock,
	identifier string,
) *cacheWatcher {
	if clk == nil {
		clk = clock.RealClock{}
	}
	return &cacheWatcher{
		input:               make(chan *watchCacheEvent, chanSize),
		result:              make(chan watch.Event, chanSize),
		done:                make(chan struct{}),
		filter:              filter,
		stopped:             false,
		forget:              forget,
		versioner:           versioner,
		deadline:            deadline,
		allowWatchBookmarks: allowWatchBookmarks,
		groupResource:       groupResource,
		watcherMetrics:      watcherMetrics,
		clock:               clk,
		identifier:          identifier,
	}
}

// ResultChan 返回客户端读取事件的通道。
// Implements watch.Interface.
func (c *cacheWatcher) ResultChan() <-chan watch.Event {
	return c.result
}

// Stop 停止 watcher；graceful=false 表示无需排空输入缓冲。
// Implements watch.Interface.
func (c *cacheWatcher) Stop() {
	c.forget(false)
}

// we rely on the fact that stopLocked is actually protected by Cacher.Lock()
func (c *cacheWatcher) stopLocked() {
	if !c.stopped {
		c.stopped = true
		// stop without draining the input channel was requested.
		if !c.drainInputBuffer {
			close(c.done)
		}
		close(c.input)
	}

	// 即使 watcher 之前已经停止，只要它此前处于 draining 模式而现在不再需要排空，
	// 我们就必须在此处关闭 done 通道。否则处理 goroutine 可能泄漏：它会继续尝试把
	// 对象放入 result 通道，而该通道已满、接收端已经没有人处理事件了。
	// 这段处理针对“先 graceful 停止、后强制停止”的转换，防止处理 goroutine
	// 阻塞在满的 result 通道上永久泄漏。
	if !c.drainInputBuffer && !c.isDoneChannelClosedLocked() {
		close(c.done)
	}
}

// nonblockingAdd 尝试不阻塞地投递事件。
// 返回 false 表示缓冲区已满，或该 bookmark 早于客户端要求的 RV（丢弃以避免污染）。
func (c *cacheWatcher) nonblockingAdd(event *watchCacheEvent) bool {
	// if the bookmarkAfterResourceVersion hasn't been seen
	// we will try to deliver a bookmark event every second.
	// the following check will discard a bookmark event
	// if it is < than the bookmarkAfterResourceVersion
	// so that we don't pollute the input channel
	if event.Type == watch.Bookmark && event.ResourceVersion < c.bookmarkAfterResourceVersion {
		return false
	}
	select {
	case c.input <- event:
		c.markBookmarkAfterRvAsReceived(event)
		return true
	default:
		return false
	}
}

// Nil timer means that add will not block (if it can't send event immediately, it will break the watcher)
//
// Note that bookmark events are never added via the add method only via the nonblockingAdd.
// Changing this behaviour will require moving the markBookmarkAfterRvAsReceived method
//
// add 尝试投递事件，必要时最多阻塞到 timer 到期。
// 超时后强制关闭 watcher：宁可断开，也不让一个慢客户端拖住整个 watchCache。
func (c *cacheWatcher) add(event *watchCacheEvent, timer *time.Timer) bool {
	// Try to send the event immediately, without blocking.
	if c.nonblockingAdd(event) {
		return true
	}

	closeFunc := func() {
		// This means that we couldn't send event to that watcher.
		// Since we don't want to block on it infinitely,
		// we simply terminate it.
		metrics.TerminatedWatchersCounter.WithLabelValues(c.groupResource.Group, c.groupResource.Resource).Inc()
		// This means that we couldn't send event to that watcher.
		// Since we don't want to block on it infinitely, we simply terminate it.

		// we are graceful = false, when:
		//
		// (a) The bookmarkAfterResourceVersionReceived hasn't been received,
		//     we can safely terminate the watcher. Because the client is waiting
		//     for this specific bookmark, and we even haven't received one.
		// (b) We have seen the bookmarkAfterResourceVersion, and it was sent already to the client.
		//     We can simply terminate the watcher.

		// we are graceful = true, when:
		//
		// (a) We have seen a bookmark, but it hasn't been sent to the client yet.
		//     That means we should drain the input buffer which contains
		//     the bookmarkAfterResourceVersion we want. We do that to make progress
		//     as clients can re-establish a new watch with the given RV and receive
		//     further notifications.
		graceful := func() bool {
			c.stateMutex.Lock()
			defer c.stateMutex.Unlock()
			return c.state == cacheWatcherBookmarkReceived
		}()
		klog.V(1).Infof("Forcing %v watcher close due to unresponsiveness: %v. len(c.input) = %v, len(c.result) = %v, graceful = %v", c.groupResource.String(), c.identifier, len(c.input), len(c.result), graceful)
		c.forget(graceful)
	}

	if timer == nil {
		closeFunc()
		return false
	}

	// OK, block sending, but only until timer fires.
	select {
	case c.input <- event:
		return true
	case <-timer.C:
		closeFunc()
		return false
	}
}

// nextBookmarkTime 计算下一次应发送 bookmark 的时间。
// 返回的 bool 为 false 表示已无可发送的 bookmark（例如 deadline 已过），
// 调用方应停止调度。
func (c *cacheWatcher) nextBookmarkTime(now time.Time, bookmarkFrequency time.Duration) (time.Time, bool) {
	// We try to send bookmarks:
	//
	// (a) right before the watcher timeout - for now we simply set it 2s before
	//     the deadline
	//
	// (b) roughly every minute
	//
	// (c) immediately when the bookmarkAfterResourceVersion wasn't confirmed
	//     in this scenario the client have already seen (or is in the process of sending)
	//     all initial data and is interested in seeing
	//     a specific RV value (aka. the bookmarkAfterResourceVersion)
	//     since we don't know when the cacher will see the RV we increase frequency
	//
	// (b) gives us periodicity if the watch breaks due to unexpected
	// conditions, (a) ensures that on timeout the watcher is as close to
	// now as possible - this covers 99% of cases.

	// 尚未确认目标 RV 时立即发送，尽快让客户端完成“同步点”握手。
	if !c.wasBookmarkAfterRvReceived() {
		return time.Time{}, true // schedule immediately
	}

	heartbeatTime := now.Add(bookmarkFrequency)
	if c.deadline.IsZero() {
		// Timeout is set by our client libraries (e.g. reflector) as well as defaulted by
		// apiserver if properly configured. So this shoudln't happen in practice.
		return heartbeatTime, true
	}
	// 优先在 deadline 前 2 秒发送，让 bookmark 在连接被服务端关闭前送达。
	if pretimeoutTime := c.deadline.Add(-2 * time.Second); pretimeoutTime.Before(heartbeatTime) {
		heartbeatTime = pretimeoutTime
	}

	if heartbeatTime.Before(now) {
		return time.Time{}, false
	}
	return heartbeatTime, true
}

// wasBookmarkAfterRvReceived same as wasBookmarkAfterRvReceivedLocked just acquires a lock
func (c *cacheWatcher) wasBookmarkAfterRvReceived() bool {
	c.stateMutex.Lock()
	defer c.stateMutex.Unlock()
	return c.wasBookmarkAfterRvReceivedLocked()
}

// wasBookmarkAfterRvReceivedLocked 检查该 cacheWatcher 是否已看到
// RV >= bookmarkAfterResourceVersion 的 bookmark 事件。调用方需持有 stateMutex。
func (c *cacheWatcher) wasBookmarkAfterRvReceivedLocked() bool {
	return c.state != cacheWatcherWaitingForBookmark
}

// markBookmarkAfterRvAsReceived indicates that the given cacheWatcher
// have seen a bookmark event >= bookmarkAfterResourceVersion
func (c *cacheWatcher) markBookmarkAfterRvAsReceived(event *watchCacheEvent) {
	if event.Type == watch.Bookmark {
		c.stateMutex.Lock()
		defer c.stateMutex.Unlock()
		if c.wasBookmarkAfterRvReceivedLocked() {
			return
		}
		// bookmark events are scheduled by startDispatchingBookmarkEvents method
		// since we received a bookmark event that means we have
		// converged towards the expected RV and it is okay to update the state so that
		// this cacher can be scheduler for a regular bookmark events
		c.state = cacheWatcherBookmarkReceived
	}
}

// wasBookmarkAfterRvSentLocked 检查该 watcher 是否已发送过
// RV >= bookmarkAfterResourceVersion 的 bookmark 事件。调用方需持有 stateMutex。
func (c *cacheWatcher) wasBookmarkAfterRvSentLocked() bool {
	return c.state == cacheWatcherBookmarkSent
}

// wasBookmarkAfterRvSent same as wasBookmarkAfterRvSentLocked just acquires a lock
func (c *cacheWatcher) wasBookmarkAfterRvSent() bool {
	c.stateMutex.Lock()
	defer c.stateMutex.Unlock()
	return c.wasBookmarkAfterRvSentLocked()
}

// markBookmarkAfterRvSent indicates that the given cacheWatcher
// have sent a bookmark event with an RV >= the bookmarkAfterResourceVersion
//
// this function relies on the fact that the nonblockingAdd method
// won't admit a bookmark event with an RV < the bookmarkAfterResourceVersion
// so the first received bookmark event is considered to match the bookmarkAfterResourceVersion
func (c *cacheWatcher) markBookmarkAfterRvSent(event *watchCacheEvent) {
	// note that bookmark events are not so common so will acquire a lock every ~60 second or so
	if event.Type == watch.Bookmark {
		c.stateMutex.Lock()
		defer c.stateMutex.Unlock()
		if !c.wasBookmarkAfterRvSentLocked() {
			c.state = cacheWatcherBookmarkSent
		}
	}
}

// setBookmarkAfterResourceVersion 设置 bookmarkAfterResourceVersion 及其关联状态。
// 仅应在 watcher 启动前调用（字段注释已说明启动后不得修改）。
func (c *cacheWatcher) setBookmarkAfterResourceVersion(bookmarkAfterResourceVersion uint64) {
	state := cacheWatcherWaitingForBookmark
	// 客户端未要求特定 RV 时无需等待，直接把状态置为已发送（no-op）。
	if bookmarkAfterResourceVersion == 0 {
		state = cacheWatcherBookmarkSent // if no specific RV was requested we assume no-op
	}
	c.state = state
	c.bookmarkAfterResourceVersion = bookmarkAfterResourceVersion
}

// setDrainInputBufferLocked if set to true indicates that we should delay closing this watcher
// until we send all events residing in the input buffer.
func (c *cacheWatcher) setDrainInputBufferLocked(drain bool) {
	c.drainInputBuffer = drain
}

// isDoneChannelClosed checks if c.done channel is closed
func (c *cacheWatcher) isDoneChannelClosedLocked() bool {
	select {
	case <-c.done:
		return true
	default:
	}
	return false
}

// getMutableObject 返回可安全改写 RV 的对象。
// cachingObject 会惰性深拷贝，因此无需在这里复制；其他对象必须深拷贝，
// 否则修改 resourceVersion 会污染缓存中共享的对象。
func getMutableObject(object runtime.Object) runtime.Object {
	if _, ok := object.(*cachingObject); ok {
		// It is safe to return without deep-copy, because the underlying
		// object will lazily perform deep-copy on the first try to change
		// any of its fields.
		return object
	}
	return object.DeepCopyObject()
}

// updateResourceVersion 更新对象的 RV；失败仅记录错误，不影响事件投递。
func updateResourceVersion(object runtime.Object, versioner storage.Versioner, resourceVersion uint64) {
	if err := versioner.UpdateObject(object, resourceVersion); err != nil {
		utilruntime.HandleError(fmt.Errorf("failure to version api object (%d) %#v: %v", resourceVersion, object, err))
	}
}

// convertToWatchEvent 把内部事件转换为客户端事件；不匹配过滤器的对象返回 nil。
// 过滤器对旧对象的匹配结果决定 ShouldDelete 的语义：只有对象“离开过滤器视角”时
// 才发送 Deleted，且使用旧对象内容 + 事件 RV。
func (c *cacheWatcher) convertToWatchEvent(event *watchCacheEvent) *watch.Event {
	if event.Type == watch.Bookmark {
		e := &watch.Event{Type: watch.Bookmark, Object: event.Object.DeepCopyObject()}
		// 首个满足条件的 bookmark 被标记为“初始事件结束”，
		// 客户端据此判断初次同步已完成。
		if !c.wasBookmarkAfterRvSent() {
			if err := storage.AnnotateInitialEventsEndBookmark(e.Object); err != nil {
				utilruntime.HandleError(fmt.Errorf("error while accessing object's metadata gr: %v, identifier: %v, obj: %#v, err: %v", c.groupResource, c.identifier, e.Object, err))
				return nil
			}
		}
		return e
	}

	curObjPasses := event.Type != watch.Deleted && c.filter(event.Key, event.ObjLabels, event.ObjFields, event.Object)
	oldObjPasses := false
	if event.PrevObject != nil {
		oldObjPasses = c.filter(event.Key, event.PrevObjLabels, event.PrevObjFields, event.PrevObject)
	}
	if !curObjPasses && !oldObjPasses {
		// Watcher is not interested in that object.
		return nil
	}

	switch {
	case curObjPasses && !oldObjPasses:
		return &watch.Event{Type: watch.Added, Object: getMutableObject(event.Object)}
	case curObjPasses && oldObjPasses:
		return &watch.Event{Type: watch.Modified, Object: getMutableObject(event.Object)}
	case !curObjPasses && oldObjPasses:
		// return a delete event with the previous object content, but with the event's resource version
		oldObj := getMutableObject(event.PrevObject)
		// We know that if oldObj is cachingObject (which can only be set via
		// setCachingObjects), its resourceVersion is already set correctly and
		// we don't need to update it. However, since cachingObject efficiently
		// handles noop updates, we avoid this microoptimization here.
		updateResourceVersion(oldObj, c.versioner, event.ResourceVersion)
		return &watch.Event{Type: watch.Deleted, Object: oldObj}
	}

	return nil
}

// 注意：sendWatchCacheEvent 被约定为不会修改 <event>！！！
// 它把事件写入 result 通道，返回构建与发送时刻供指标使用。
// 关键不变量：关闭 done 后不能向 result 写入新事件，但必须先把已进入选择前的
// 事件按序发送完，因此这里先做一次 done 的非阻塞检查，再在 select 中发送。
func (c *cacheWatcher) sendWatchCacheEvent(event *watchCacheEvent) (builtAt, sentAt time.Time) {
	watchEvent := c.convertToWatchEvent(event)
	if watchEvent == nil {
		// Watcher is not interested in that object.
		return time.Time{}, time.Time{}
	}
	builtAt = c.clock.Now()

	// We need to ensure that if we put event X to the c.result, all
	// previous events were already put into it before, no matter whether
	// c.done is close or not.
	// Thus we cannot simply select from c.done and c.result and this
	// would give us non-determinism.
	// At the same time, we don't want to block infinitely on putting
	// to c.result, when c.done is already closed.
	//
	// This ensures that with c.done already close, we at most once go
	// into the next select after this. With that, no matter which
	// statement we choose there, we will deliver only consecutive
	// events.
	select {
	case <-c.done:
		return time.Time{}, time.Time{}
	default:
	}

	select {
	case c.result <- *watchEvent:
		c.markBookmarkAfterRvSent(event)
		sentAt = c.clock.Now()
	case <-c.done:
	}
	return builtAt, sentAt
}

// processInterval 先把历史区间内的事件全部发给客户端，再转入实时事件处理。
// 返回时关闭 result 并停止 watcher。
func (c *cacheWatcher) processInterval(ctx context.Context, cacheInterval *watchCacheInterval, resourceVersion uint64) {
	defer utilruntime.HandleCrashWithContext(ctx)
	defer close(c.result)
	defer c.Stop()

	// Check how long we are processing initEvents.
	// As long as these are not processed, we are not processing
	// any incoming events, so if it takes long, we may actually
	// block all watchers for some time.
	// TODO: From the logs it seems that there happens processing
	// times even up to 1s which is very long. However, this doesn't
	// depend that much on the number of initEvents. E.g. from the
	// 2000-node Kubemark run we have logs like this, e.g.:
	// ... processing 13862 initEvents took 66.808689ms
	// ... processing 14040 initEvents took 993.532539ms
	// We should understand what is blocking us in those cases (e.g.
	// is it lack of CPU, network, or sth else) and potentially
	// consider increase size of result buffer in those cases.
	const initProcessThreshold = 500 * time.Millisecond
	startTime := time.Now()

	// cacheInterval may be created from a version being more fresh than requested
	// (e.g. for NotOlderThan semantic). In such a case, we need to prevent watch event
	// with lower resourceVersion from being delivered to ensure watch contract.
	if cacheInterval.resourceVersion > resourceVersion {
		resourceVersion = cacheInterval.resourceVersion
	}

	initEventCount := 0
	for {
		event, err := cacheInterval.Next()
		if err != nil {
			// An error indicates that the cache interval
			// has been invalidated and can no longer serve
			// events.
			//
			// Initially we considered sending an "out-of-history"
			// Error event in this case, but because historically
			// such events weren't sent out of the watchCache, we
			// decided not to. This is still ok, because on watch
			// closure, the watcher will try to re-instantiate the
			// watch and then will get an explicit "out-of-history"
			// window. There is potential for optimization, but for
			// now, in order to be on the safe side and not break
			// custom clients, the cost of it is something that we
			// are fully accepting.
			klog.Warningf("couldn't retrieve watch event to serve: %#v", err)
			return
		}
		if event == nil {
			break
		}
		c.sendWatchCacheEvent(event)

		// With some events already sent, update resourceVersion so that
		// events that were buffered and not yet processed won't be delivered
		// to this watcher second time causing going back in time.
		//
		// There is one case where events are not necessary ordered by
		// resourceVersion, being a case of watching from resourceVersion=0,
		// which at the beginning returns the state of each objects.
		// For the purpose of it, we need to max it with the resource version
		// that we have so far.
		if event.ResourceVersion > resourceVersion {
			resourceVersion = event.ResourceVersion
		}
		initEventCount++
	}

	if initEventCount > 0 {
		metrics.InitCounter.WithLabelValues(c.groupResource.Group, c.groupResource.Resource).Add(float64(initEventCount))
	}
	processingTime := time.Since(startTime)
	if processingTime > initProcessThreshold {
		klog.V(2).Infof("processing %d initEvents of %s (%s) took %v", initEventCount, c.groupResource, c.identifier, processingTime)
	}

	// 对 watchlist 请求，在发送完 cacheInterval 中的全部事件之后再发送 bookmark：
	// watchlist（SendInitialEvents）必须用 bookmark 标记初始事件结束，
	// 客户端才能确认自己已拿到完整快照。
	if cacheInterval.initialEventsEndBookmark != nil {
		c.sendWatchCacheEvent(cacheInterval.initialEventsEndBookmark)
	}
	c.process(ctx, resourceVersion)
}

// process 处理来自 watchCache 的实时事件，直到输入通道关闭或 ctx 取消。
func (c *cacheWatcher) process(ctx context.Context, resourceVersion uint64) {
	// 此时我们已经能处理传入的 watch 事件，但初始事件可能仍在处理中：它们的序列化
	// 以及发送给客户端都是异步完成的。这里通知 APF：该 watch 已就绪，
	// 可以开始计费/调度后续请求。
	// TODO: As describe in the KEP, we would like to estimate that by delaying
	//   the initialization signal proportionally to the number of events to
	//   process, but we're leaving this to the tuning phase.
	utilflowcontrol.WatchInitialized(ctx)

	for {
		select {
		case event, ok := <-c.input:
			if !ok {
				return
			}
			// 只发送比 resourceVersion 更新的事件；或者当 RV 恰等于 resourceVersion
			// 且我们尚未向客户端发送过 bookmark 时，发送该 bookmark 事件。
			// 过滤掉不新于起始 RV 的事件，保证不“回退时间”；
			// 例外是恰好等于起始 RV 且尚未发送过 bookmark 的情况。
			if event.ResourceVersion > resourceVersion || (event.Type == watch.Bookmark && event.ResourceVersion == resourceVersion && !c.wasBookmarkAfterRvSent()) {
				builtAt, sentAt := c.sendWatchCacheEvent(event)
				c.observeDispatchMetrics(event, builtAt, sentAt)
			}
		case <-ctx.Done():
			return
		}
	}
}

// observeDispatchMetrics 记录事件在各阶段的延迟；bookmark 与未发送的事件跳过。
func (c *cacheWatcher) observeDispatchMetrics(event *watchCacheEvent, builtAt, sentAt time.Time) {
	if event.Type == watch.Bookmark || sentAt.IsZero() || event.RecordTime.IsZero() {
		return
	}
	if !event.CacheReceived.IsZero() {
		c.watcherMetrics.ObserveStage(metrics.StageStorageToCache, event.CacheReceived.Sub(event.RecordTime))
	}
	c.watcherMetrics.ObserveStage(metrics.StageCacheToWatcher, sentAt.Sub(builtAt))
	c.watcherMetrics.ObserveStage(metrics.StageTotal, sentAt.Sub(event.RecordTime))
}
