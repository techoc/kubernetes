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
	// cacheWatcherWaitingForBookmark 表示 cacher 正在等待
	// 一个带有指定 RV 的 bookmark 事件
	cacheWatcherWaitingForBookmark = iota

	// cacheWatcherBookmarkReceived 表示 cacher 已收到
	// 带有所需 RV 的 bookmark 事件
	cacheWatcherBookmarkReceived

	// cacheWatcherBookmarkSent 表示 cacher 已向客户端
	// 发送过 bookmark 事件
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

	// 便于把 cacheWatcher 实例与请求对应起来的可读标识符
	identifier string

	// drainInputBuffer 表示是否应推迟关闭该 watcher，先把输入缓冲区中的事件全部
	// 发送出去。用于“已收到目标 bookmark 但尚未发送”的场景：先把缓冲区排空，
	// 客户端才能拿到它等待的 RV 并重建 watch。
	drainInputBuffer bool

	// bookmarkAfterResourceVersion 保存一个 RV，指示我们应当从何时开始
	// 投递 bookmark 事件。若该字段值为 0，表示我们对投递 bookmark 事件
	// 没有任何特殊偏好。注意该字段与 state 字段配合使用，
	// watcher 启动后不应再修改它。
	bookmarkAfterResourceVersion uint64

	// stateMutex 保护 state
	stateMutex sync.Mutex

	// state 保存一个数值，表示 watcher 的当前状态
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
// 实现 watch.Interface 接口。
func (c *cacheWatcher) ResultChan() <-chan watch.Event {
	return c.result
}

// Stop 停止 watcher；graceful=false 表示无需排空输入缓冲。
// 实现 watch.Interface 接口。
func (c *cacheWatcher) Stop() {
	c.forget(false)
}

// 这里依赖一个事实：stopLocked 实际上由 Cacher.Lock() 保护
func (c *cacheWatcher) stopLocked() {
	if !c.stopped {
		c.stopped = true
		// 此时请求的是不排空输入通道的停止方式。
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
	// 若 bookmarkAfterResourceVersion 尚未出现，我们每秒都会尝试投递
	// 一个 bookmark 事件。下面的检查会丢弃 RV 小于
	// bookmarkAfterResourceVersion 的 bookmark 事件，以免污染输入通道。
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

// timer 为 nil 表示 add 不会阻塞（若无法立即发送事件，就会终止该 watcher）。
//
// 注意：bookmark 事件从不经由 add 方法投递，只能通过 nonblockingAdd 投递。
// 若要改变这一行为，需要移动 markBookmarkAfterRvAsReceived 方法的位置。
//
// add 尝试投递事件，必要时最多阻塞到 timer 到期。
// 超时后强制关闭 watcher：宁可断开，也不让一个慢客户端拖住整个 watchCache。
func (c *cacheWatcher) add(event *watchCacheEvent, timer *time.Timer) bool {
	// 先尝试立即发送事件，不阻塞。
	if c.nonblockingAdd(event) {
		return true
	}

	closeFunc := func() {
		// 这意味着我们无法把事件发送给该 watcher。
		// 由于不想无限阻塞在它上面，我们直接把它终止掉。
		metrics.TerminatedWatchersCounter.WithLabelValues(c.groupResource.Group, c.groupResource.Resource).Inc()
		// 这意味着我们无法把事件发送给该 watcher。
		// 由于不想无限阻塞在它上面，我们直接把它终止掉。

		// graceful = false（非优雅关闭）的情形：
		//
		// (a) 尚未收到 bookmarkAfterResourceVersion：可以安全地终止 watcher。
		//     因为客户端正在等待这个特定的 bookmark，而我们甚至还没收到过任何一个。
		// (b) 已看到 bookmarkAfterResourceVersion，并且它已经发送给客户端：
		//     可以直接终止 watcher。

		// graceful = true（优雅关闭）的情形：
		//
		// (a) 已看到 bookmark，但尚未发送给客户端。
		//     这意味着应当排空输入缓冲区，其中包含我们想要的
		//     bookmarkAfterResourceVersion。这样做是为了推进进度：
		//     客户端可以用该 RV 重建一个新的 watch 并继续接收后续通知。
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

	// 好，现在阻塞发送，但最多持续到 timer 触发为止。
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
	// 我们会在以下时机尝试发送 bookmark：
	//
	// (a) 紧贴 watcher 超时之前——目前我们把它设在 deadline 前 2 秒
	//
	// (b) 大约每分钟一次
	//
	// (c) 当 bookmarkAfterResourceVersion 尚未被确认时立即发送：
	//     在这种场景下客户端已经看完（或正在发送过程中看完）全部初始数据，
	//     并希望看到一个特定的 RV 值（即 bookmarkAfterResourceVersion）。
	//     由于我们不知道 cacher 何时会看到该 RV，因此提高发送频率。
	//
	// (b) 在 watch 因意外情况断开时提供周期性兜底；(a) 保证超时时刻
	// bookmark 尽可能贴近当前时间——这覆盖了 99% 的场景。

	// 尚未确认目标 RV 时立即发送，尽快让客户端完成“同步点”握手。
	if !c.wasBookmarkAfterRvReceived() {
		return time.Time{}, true // 立即安排发送
	}

	heartbeatTime := now.Add(bookmarkFrequency)
	if c.deadline.IsZero() {
		// 超时时间由我们的客户端库（例如 reflector）设置，apiserver 配置正确时
		// 也会给出默认值。所以实践中不应出现 deadline 为零的情况。
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

// wasBookmarkAfterRvReceived 与 wasBookmarkAfterRvReceivedLocked 相同，只是会先获取锁
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

// markBookmarkAfterRvAsReceived 表示该 cacheWatcher 已看到
// RV >= bookmarkAfterResourceVersion 的 bookmark 事件
func (c *cacheWatcher) markBookmarkAfterRvAsReceived(event *watchCacheEvent) {
	if event.Type == watch.Bookmark {
		c.stateMutex.Lock()
		defer c.stateMutex.Unlock()
		if c.wasBookmarkAfterRvReceivedLocked() {
			return
		}
		// bookmark 事件由 startDispatchingBookmarkEvents 方法调度；
		// 既然已收到一个 bookmark 事件，说明我们已向期望的 RV 收敛，
		// 可以更新状态，让该 cacher 被调度去发送常规的 bookmark 事件
		c.state = cacheWatcherBookmarkReceived
	}
}

// wasBookmarkAfterRvSentLocked 检查该 watcher 是否已发送过
// RV >= bookmarkAfterResourceVersion 的 bookmark 事件。调用方需持有 stateMutex。
func (c *cacheWatcher) wasBookmarkAfterRvSentLocked() bool {
	return c.state == cacheWatcherBookmarkSent
}

// wasBookmarkAfterRvSent 与 wasBookmarkAfterRvSentLocked 相同，只是会先获取锁
func (c *cacheWatcher) wasBookmarkAfterRvSent() bool {
	c.stateMutex.Lock()
	defer c.stateMutex.Unlock()
	return c.wasBookmarkAfterRvSentLocked()
}

// markBookmarkAfterRvSent 表示该 cacheWatcher 已发送
// RV >= bookmarkAfterResourceVersion 的 bookmark 事件
//
// 本函数依赖这样一个事实：nonblockingAdd 方法不会接纳
// RV < bookmarkAfterResourceVersion 的 bookmark 事件，
// 因此第一个收到的 bookmark 事件即被视为与 bookmarkAfterResourceVersion 匹配
func (c *cacheWatcher) markBookmarkAfterRvSent(event *watchCacheEvent) {
	// 注意：bookmark 事件并不频繁，因此大约每 60 秒才会获取一次锁
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
		state = cacheWatcherBookmarkSent // 未请求特定 RV 时视为 no-op
	}
	c.state = state
	c.bookmarkAfterResourceVersion = bookmarkAfterResourceVersion
}

// setDrainInputBufferLocked 若设为 true，表示应推迟关闭本 watcher，
// 直到把输入缓冲区中驻留的所有事件全部发送出去。
func (c *cacheWatcher) setDrainInputBufferLocked(drain bool) {
	c.drainInputBuffer = drain
}

// isDoneChannelClosed 检查 c.done 通道是否已关闭
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
		// 不做深拷贝直接返回是安全的，因为底层对象在首次尝试修改
		// 其任一字段时会惰性执行深拷贝。
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
		// 该 watcher 对这个对象不感兴趣。
		return nil
	}

	switch {
	case curObjPasses && !oldObjPasses:
		return &watch.Event{Type: watch.Added, Object: getMutableObject(event.Object)}
	case curObjPasses && oldObjPasses:
		return &watch.Event{Type: watch.Modified, Object: getMutableObject(event.Object)}
	case !curObjPasses && oldObjPasses:
		// 返回一个内容为前一个对象、但携带本事件 resourceVersion 的删除事件
		oldObj := getMutableObject(event.PrevObject)
		// 我们清楚：若 oldObj 是 cachingObject（只能通过 setCachingObjects 设置），
		// 它的 resourceVersion 已正确设置，无需再更新。但由于 cachingObject
		// 能高效处理无操作更新，这里放弃了这一微优化。
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
		// 该 watcher 对这个对象不感兴趣。
		return time.Time{}, time.Time{}
	}
	builtAt = c.clock.Now()

	// 我们需要确保：只要把事件 X 放入 c.result，之前所有事件都已先行放入，
	// 无论 c.done 是否已关闭。
	// 因此不能简单地在 c.done 与 c.result 之间 select，那样会带来不确定性。
	// 同时，当 c.done 已关闭时，我们也不想在向 c.result 放入事件时无限阻塞。
	//
	// 下面的做法保证：在 c.done 已关闭的情况下，我们最多只会进入一次
	// 该检查之后的那个 select。这样一来，无论选中哪个分支，
	// 我们投递的都只是连续不断、不跳跃的事件序列。
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

	// 检查处理 initEvents 花了多长时间。在这些事件处理完之前，我们不会处理
	// 任何新到达的事件，因此如果耗时很长，实际上可能长时间阻塞所有 watcher。
	// TODO: 从日志看，处理时长甚至可达 1 秒，这非常长。然而这并不太取决于
	// initEvents 的数量。例如 2000 节点 Kubemark 运行中有这样的日志：
	// ... processing 13862 initEvents took 66.808689ms
	// ... processing 14040 initEvents took 993.532539ms
	// 我们应当弄清楚这些场景下是什么在阻塞我们（例如是 CPU、网络不足
	// 还是别的原因），并可以考虑在这些场景下增大 result 缓冲区。
	const initProcessThreshold = 500 * time.Millisecond
	startTime := time.Now()

	// cacheInterval 可能是基于比请求更新的版本创建的（例如 NotOlderThan 语义）。
	// 这种情况下，必须阻止更低 resourceVersion 的 watch 事件被投递给客户端，
	// 以保证 watch 契约。
	if cacheInterval.resourceVersion > resourceVersion {
		resourceVersion = cacheInterval.resourceVersion
	}

	initEventCount := 0
	for {
		event, err := cacheInterval.Next()
		if err != nil {
			// 出现错误说明该缓存区间已被判定失效，
			// 无法继续提供事件。
			//
			// 我们最初考虑在这种情况下发送一个
			// “超出历史范围”（out-of-history）的错误事件，但由于历史上
			// 这类事件并不会从 watchCache 发出，我们决定不这样做。
			// 这仍然是可以的：watch 关闭时，watcher 会尝试重建
			// watch，届时会显式地得到“超出历史范围”的窗口。
			// 存在可优化的空间，但为了稳妥起见、避免破坏
			// 自定义客户端，我们完全接受这一代价。
			klog.Warningf("couldn't retrieve watch event to serve: %#v", err)
			return
		}
		if event == nil {
			break
		}
		c.sendWatchCacheEvent(event)

		// 在已发送部分事件之后，更新 resourceVersion，避免那些已缓冲、
		// 尚未处理的事件被第二次投递给该 watcher，导致时间倒退。
		//
		// 有一种情况下事件不一定按 resourceVersion 有序：即从
		// resourceVersion=0 开始 watch 时，开头会返回每个对象各自的当前状态。
		// 为此需要用事件的 RV 与我们已有的 RV 取最大值。
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
	// TODO: 如 KEP 所述，我们希望按待处理事件的数量成比例地推迟初始化信号
	//   来估算这一耗时，但留到调优阶段再做。
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
