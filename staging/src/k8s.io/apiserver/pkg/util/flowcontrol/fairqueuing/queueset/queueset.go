/*
Copyright 2019 The Kubernetes Authors.

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

package queueset

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/util/flowcontrol/debug"
	fq "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing"
	"k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/eventclock"
	"k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/promise"
	"k8s.io/apiserver/pkg/util/flowcontrol/metrics"
	fqrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	"k8s.io/apiserver/pkg/util/shufflesharding"
	"k8s.io/klog/v2"
)

// nsTimeFmt 是日志中时间戳的格式，精确到纳秒，便于对齐虚拟时间推进。
const nsTimeFmt = "2006-01-02 15:04:05.000000000"

// queueSetFactory 实现 QueueSetFactory 接口
// queueSetFactory 用于创建 QueueSet 对象。
type queueSetFactory struct {
	clock                 eventclock.Interface
	promiseFactoryFactory promiseFactoryFactory
}

// promiseFactory 返回一个 WriteOnce
// —— 其 Set 方法在 queueSet 持锁时被调用，
// 其 Get 方法在 queueSet 未持锁时被调用。
// 参数与 `promise.NewWriteOnce` 相同。
//
// 这条锁约定很关键：Set 在持锁状态下调用，Get 在释放锁后调用，
// 从而避免等待者持有锁阻塞调度器。
type promiseFactory func(initial interface{}, doneCtx context.Context, doneVal interface{}) promise.WriteOnce

// promiseFactoryFactory 返回给定 queueSet 应当使用的 promiseFactory
type promiseFactoryFactory func(*queueSet) promiseFactory

// `*queueSetCompleter` 实现 `QueueSetCompleter`；`factory` 与 `theSet` 两个字段恰好只有一个非 nil。
// factory 非 nil 表示首次构造，theSet 非 nil 表示在既有队列集上重配置。
type queueSetCompleter struct {
	factory              *queueSetFactory
	reqsGaugePair        metrics.RatioedGaugePair
	execSeatsGauge       metrics.RatioedGauge
	seatDemandIntegrator metrics.Gauge
	theSet               *queueSet
	qCfg                 fq.QueuingConfig
	dealer               *shufflesharding.Dealer
}

// queueSet 实现本包文档中描述的「面向服务器请求的
// 公平排队」（Fair Queuing for Server Requests）技术，
// 其指针实现 QueueSet 接口。锁之前列出的字段
// 不应改动；锁之后列出的字段
// 必须在持锁状态下访问。
//
// 本类型的方法遵循这样的命名约定：后缀
// "Locked" 表示调用方必须持锁；不以 "Locked"
// 结尾的方法要么自己获取锁，
// 要么与加锁无关。
//
// 本类型的方法还遵循这样的约定：后缀
// "ToBoundLocked" 表示调用方可能必须随后补一次
// `boundNextDispatchLocked` 调用。之所以如此，是因为这类方法
// 会改变队列中最老的请求是哪一个，而这一变化意味着
// `boundNextDispatchLocked` 中的防绕组（anti-windup）技巧需要
// 相对于修订后的队列最老请求重新应用。
//
// 不变量：totRequestsWaiting/totSeatsWaiting/totSeatsInUse 等计数器必须与
// 各 queue 中的实际请求保持一致，任何修改都应在持锁状态下同时更新。
type queueSet struct {
	clock                    eventclock.Interface
	estimatedServiceDuration time.Duration

	reqsGaugePair metrics.RatioedGaugePair // .RequestsExecuting 只覆盖常规阶段

	execSeatsGauge metrics.RatioedGauge // 覆盖执行的所有阶段

	seatDemandIntegrator metrics.Gauge

	promiseFactory promiseFactory

	lock sync.Mutex

	// qCfg 持有当前的排队配置。其
	// DesiredNumQueues 可能小于当前的队列数量。
	// 若其 DesiredNumQueues 为零，则其其余排队
	// 参数保留 DesiredNumQueues 最后一次
	// 非零时（如果曾有）的取值。
	qCfg fq.QueuingConfig

	// 当前的派发配置。
	dCfg fq.DispatchingConfig

	// 若 `qCfg.DesiredNumQueues` 非零，则 dealer 非 nil
	// 且与 `qCfg` 匹配。
	dealer *shufflesharding.Dealer

	// queues 可能比期望的数量更长，因为多余的
	// 队列还在排空之中。
	queues []*queue

	// currentR 是进程启动以来每个队列所分配的席位秒数（seat-seconds）。
	// 这是我们对最初公平排队工作中名为 R 的进度计的推广。
	currentR fqrequest.SeatSeconds

	// lastRealTime 是 `virtualTime` 最近一次更新时 `clock.Now()` 返回的值
	lastRealTime time.Time

	// robinIndex 是最近一次被派发的队列的下标
	robinIndex int

	// totRequestsWaiting 是对所有队列求和的
	// 该队列中等待请求的数量
	totRequestsWaiting int

	// totRequestsExecuting 是本 queueSet 中当前正在执行的
	// 请求总数。它等于对所有队列求和的、
	// 从该队列执行中的请求数量之和。
	totRequestsExecuting int

	// requestsExecutingSet 是在没有队列的情况下，现实世界中正在执行的请求集合；
	// 否则请求由各队列自行跟踪。
	requestsExecutingSet sets.Set[*request]

	// totSeatsInUse 是本 queueset 中当前正在执行的所有
	// 请求占用的「席位（seats）」总数。
	totSeatsInUse int

	// totSeatsWaiting 是对所有等待请求求和的、它们的
	// 最大宽度（max width）。
	totSeatsWaiting int

	// enqueues 是曾经入过队的请求数量
	enqueues int

	// totRequestsDispatched 是本 queueSet 中
	// 已被派发（处理）过的请求总数。
	totRequestsDispatched int

	// totRequestsRejected 是本 queueSet 中
	// 已被拒绝的请求总数。
	totRequestsRejected int

	// totRequestsTimedout 是本 queueSet 中
	// 已超时的请求总数。
	totRequestsTimedout int

	// totRequestsCancelled 是本 queueSet 中
	// 已被取消的请求总数。
	totRequestsCancelled int
}

// NewQueueSetFactory 创建一个新的 QueueSetFactory 对象
func NewQueueSetFactory(c eventclock.Interface) fq.QueueSetFactory {
	return newTestableQueueSetFactory(c, ordinaryPromiseFactoryFactory)
}

// newTestableQueueSetFactory 用给定的 promiseFactoryFactory 创建新的 QueueSetFactory；可替换的 promiseFactory 让测试能够精确控制请求被派发/取消的时机。
func newTestableQueueSetFactory(c eventclock.Interface, promiseFactoryFactory promiseFactoryFactory) fq.QueueSetFactory {
	return &queueSetFactory{
		clock:                 c,
		promiseFactoryFactory: promiseFactoryFactory,
	}
}

// BeginConstruction 校验排队配置并返回构造器；此时尚未创建 queueSet。
func (qsf *queueSetFactory) BeginConstruction(qCfg fq.QueuingConfig, reqsGaugePair metrics.RatioedGaugePair, execSeatsGauge metrics.RatioedGauge, seatDemandIntegrator metrics.Gauge) (fq.QueueSetCompleter, error) {
	dealer, err := checkConfig(qCfg)
	if err != nil {
		return nil, err
	}
	return &queueSetCompleter{
		factory:              qsf,
		reqsGaugePair:        reqsGaugePair,
		execSeatsGauge:       execSeatsGauge,
		seatDemandIntegrator: seatDemandIntegrator,
		qCfg:                 qCfg,
		dealer:               dealer}, nil
}

// checkConfig 在配置合法且需要 dealer 时返回非 nil 的 Dealer，在配置非法时返回非 nil 的 error。
// DesiredNumQueues <= 0 表示不排队（仅并发限制），此时不需要 dealer。
func checkConfig(qCfg fq.QueuingConfig) (*shufflesharding.Dealer, error) {
	if qCfg.DesiredNumQueues <= 0 {
		return nil, nil
	}
	dealer, err := shufflesharding.NewDealer(qCfg.DesiredNumQueues, qCfg.HandSize)
	if err != nil {
		err = fmt.Errorf("the QueueSetConfig implies an invalid shuffle sharding config (DesiredNumQueues is deckSize): %w", err)
	}
	return dealer, err
}

// Complete 完成 queueSet 的构造或重配置，并应用派发配置。
func (qsc *queueSetCompleter) Complete(dCfg fq.DispatchingConfig) fq.QueueSet {
	qs := qsc.theSet
	if qs == nil {
		qs = &queueSet{
			clock: qsc.factory.clock,
			// 初始估计服务时长为 3ms，后续由实际观测修正；用于 anti-windup 估算。
			estimatedServiceDuration: 3 * time.Millisecond,
			reqsGaugePair:            qsc.reqsGaugePair,
			execSeatsGauge:           qsc.execSeatsGauge,
			seatDemandIntegrator:     qsc.seatDemandIntegrator,
			qCfg:                     qsc.qCfg,
			currentR:                 0,
			lastRealTime:             qsc.factory.clock.Now(),
			requestsExecutingSet:     sets.New[*request](),
		}
		qs.promiseFactory = qsc.factory.promiseFactoryFactory(qs)
	}
	qs.setConfiguration(context.Background(), qsc.qCfg, qsc.dealer, dCfg)
	return qs
}

// createQueues 是初始化 n 个队列数组的辅助方法；baseIndex 保证新增队列的 index 连续，避免与既有队列冲突。
func createQueues(n, baseIndex int) []*queue {
	fqqueues := make([]*queue, n)
	for i := 0; i < n; i++ {
		fqqueues[i] = &queue{index: baseIndex + i, requestsWaiting: newRequestFIFO(), requestsExecuting: sets.New[*request]()}
	}
	return fqqueues
}

// BeginConfigChange 在已有 queueSet 上准备一次配置变更；实际变更在 Complete 时生效。
func (qs *queueSet) BeginConfigChange(qCfg fq.QueuingConfig) (fq.QueueSetCompleter, error) {
	dealer, err := checkConfig(qCfg)
	if err != nil {
		return nil, err
	}
	return &queueSetCompleter{
		theSet: qs,
		qCfg:   qCfg,
		dealer: dealer}, nil
}

// setConfiguration 用于设置 queueSet 的配置。
// 字段更新时的更新处理也在这里完成 ——
// 例如：若 DesiredNum 增大，setConfiguration 会通过
// 添加更多队列来做协调。
func (qs *queueSet) setConfiguration(ctx context.Context, qCfg fq.QueuingConfig, dealer *shufflesharding.Dealer, dCfg fq.DispatchingConfig) {
	qs.lockAndSyncTime(ctx)
	defer qs.lock.Unlock()

	if qCfg.DesiredNumQueues > 0 {
		// 添加队列是唯一需要立即行动的事
		// 删除队列则靠自然消耗：当某个队列变空
		// 且队列太多时把它删掉。
		numQueues := len(qs.queues)
		if qCfg.DesiredNumQueues > numQueues {
			qs.queues = append(qs.queues,
				createQueues(qCfg.DesiredNumQueues-numQueues, len(qs.queues))...)
		}
	} else {
		// 关闭排队时保留旧的 QueueLengthLimit/HandSize，以便再次启用时沿用。
		qCfg.QueueLengthLimit = qs.qCfg.QueueLengthLimit
		qCfg.HandSize = qs.qCfg.HandSize
	}

	qs.qCfg = qCfg
	qs.dCfg = dCfg
	qs.dealer = dealer
	// 指标分母随配置更新；qll 至少为 1，避免除零。
	qll := qCfg.QueueLengthLimit
	if qll < 1 {
		qll = 1
	}
	if qCfg.DesiredNumQueues > 0 {
		qll *= qCfg.DesiredNumQueues
	}
	qs.reqsGaugePair.RequestsWaiting.SetDenominator(float64(qll))
	qs.reqsGaugePair.RequestsExecuting.SetDenominator(float64(dCfg.ConcurrencyDenominator))
	qs.execSeatsGauge.SetDenominator(float64(dCfg.ConcurrencyDenominator))

	// 配置放宽后立即尝试派发，避免等待下一次事件才释放积压请求。
	qs.dispatchAsMuchAsPossibleLocked()
}

// 关于某个请求的决策
type requestDecision int

// 经由请求的 decision 传递的取值
const (
	// 放行这个请求
	decisionExecute requestDecision = iota

	// 这个请求的 context 超时/被取消
	decisionCancel
)

// StartRequest 开始处理一个请求。我们采用的做法是：
// 每当「排队中」与「执行中」的请求总数发生变化时
// 就更新相应的指标，
// 因为只有指标 —— 而且仅指标 —— 是按
// FlowSchema 跟踪该数量的。
// 若返回的 Request 非 nil，则调用 queueSet 的 promiseFactory 恰好一次；
// 若 Request 为 nil，则不调用。
func (qs *queueSet) StartRequest(ctx context.Context, workEstimate *fqrequest.WorkEstimate, hashValue uint64, flowDistinguisher, fsName string, descr1, descr2 interface{}, queueNoteFn fq.QueueNoteFn) (fq.Request, bool) {
	qs.lockAndSyncTime(ctx)
	defer qs.lock.Unlock()
	var req *request

	// ========================================================================
	// 步骤 0:
	// 不排队模式下只做并发限制：容量不足立即拒绝，否则直接开始执行。
	if qs.qCfg.DesiredNumQueues < 1 {
		if !qs.canAccommodateSeatsLocked(workEstimate.MaxSeats()) {
			klog.V(5).Infof("QS(%s): rejecting request %q %#+v %#+v because %d seats are asked for, %d seats are in use (%d are executing) and the limit is %d",
				qs.qCfg.Name, fsName, descr1, descr2, workEstimate, qs.totSeatsInUse, qs.totRequestsExecuting, qs.dCfg.ConcurrencyLimit)
			qs.totRequestsRejected++
			metrics.AddReject(ctx, qs.qCfg.Name, fsName, "concurrency-limit")
			return nil, qs.isIdleLocked()
		}
		req = qs.dispatchSansQueueLocked(ctx, workEstimate, flowDistinguisher, fsName, descr1, descr2)
		return req, false
	}

	// ========================================================================
	// 步骤 1:
	// 1) 先做 shuffle sharding，选出一个队列。
	// 2) 若并发份额不足且队列长度已达上限，
	// 则拒绝当前请求
	// 3) 若未被拒绝，创建请求并入队
	req = qs.shuffleShardAndRejectOrEnqueueLocked(ctx, workEstimate, hashValue, flowDistinguisher, fsName, descr1, descr2, queueNoteFn)
	// req == nil 表示请求被拒绝了 —— 既没有剩余的
	// 并发份额，且队列长度已达上限
	if req == nil {
		klog.V(5).Infof("QS(%s): rejecting request %q %#+v %#+v due to queue full", qs.qCfg.Name, fsName, descr1, descr2)
		qs.totRequestsRejected++
		metrics.AddReject(ctx, qs.qCfg.Name, fsName, "queue-full")
		return nil, qs.isIdleLocked()
	}

	// ========================================================================
	// 步骤 2:
	// 下一步是调用尽可能多派发的方法。
	// 该方法运行一个循环，只要存在非空队列
	// 且当前执行中的请求数小于
	// 保证的并发值，循环就继续。循环体用
	// 公平排队技术挑选一个队列，并从该队列
	// 派发一个请求。
	qs.dispatchAsMuchAsPossibleLocked()

	return req, false
}

// ordinaryPromiseFactoryFactory 是 queueSetFactory
// 通常都会使用的 promiseFactoryFactory。
// 测试代码可能使用别的东西。
func ordinaryPromiseFactoryFactory(qs *queueSet) promiseFactory {
	return promise.NewWriteOnce
}

// MaxSeats 返回该请求所需占用的最大席位数，即
// WorkEstimate.InitialSeats 与 WorkEstimate.FinalSeats 两者的较大值。
func (req *request) MaxSeats() int {
	return req.workEstimate.MaxSeats()
}

// InitialSeats 返回请求初始占用的席位，用于计算服务时间对虚拟时钟的修正。
func (req *request) InitialSeats() int {
	return int(req.workEstimate.InitialSeats)
}

// NoteQueued 通知调用方请求进入/离开队列，用于其内部指标或状态跟踪。
func (req *request) NoteQueued(inQueue bool) {
	if req.queueNoteFn != nil {
		req.queueNoteFn(inQueue)
	}
}

// Finish 等待派发决定，执行 execFn，并保证无论 execFn 是否 panic 都会释放席位。
// 返回 QueueSet 是否因此变为空闲。
func (req *request) Finish(execFn func()) bool {
	exec, idle := req.wait()
	if !exec {
		return idle
	}
	func() {
		// defer 放在内层函数中，确保 execFn panic 时仍能走完成路径。
		defer func() {
			idle = req.qs.finishRequestAndDispatchAsMuchAsPossible(req)
		}()

		execFn()
	}()

	return idle
}

// wait 阻塞等待决策，并在取消/超时时从队列中移除请求。
func (req *request) wait() (bool, bool) {
	qs := req.qs

	// ========================================================================
	// 步骤 3:
	// 最后一步是等待来自
	// 某处的决策，然后据此行动。
	decisionAny := req.decision.Get()
	qs.lockAndSyncTime(req.ctx)
	defer qs.lock.Unlock()
	if req.waitStarted {
		// 这不可能发生，因为禁止客户端
		// 对同一个请求调用两次 Wait
		klog.Errorf("Duplicate call to the Wait method!  Immediately returning execute=false.  QueueSet=%s, startTime=%s, descr1=%#+v, descr2=%#+v", req.qs.qCfg.Name, req.startTime, req.descr1, req.descr2)
		return false, qs.isIdleLocked()
	}
	req.waitStarted = true
	switch decisionAny {
	case decisionCancel: // 在本 switch 之后的代码中处理
	case decisionExecute:
		klog.V(5).Infof("QS(%s): Dispatching request %#+v %#+v from its queue", qs.qCfg.Name, req.descr1, req.descr2)
		return true, false
	default:
		// 这不可能发生，所有可能的取值都已在上面处理
		klog.Errorf("QS(%s): Impossible decision (type %T, value %#+v) for request %#+v %#+v!  Treating as cancel", qs.qCfg.Name, decisionAny, decisionAny, req.descr1, req.descr2)
	}
	// TODO(aaron-prindle) 为这种情况添加指标
	klog.V(5).Infof("QS(%s): Ejecting request %#+v %#+v from its queue", qs.qCfg.Name, req.descr1, req.descr2)
	// 排队等待时长已超限，将请求从队列中移除；只有确实仍在队列中时才需要回滚计数，已派发的请求取消属于正常执行路径。
	queue := req.queue
	if req.removeFromQueueLocked() != nil {
		// 队首变化后需要重新应用 anti-windup，故延迟 boundNextDispatchLocked。
		defer qs.boundNextDispatchLocked(queue)
		qs.totRequestsWaiting--
		qs.totSeatsWaiting -= req.MaxSeats()
		qs.totRequestsRejected++
		qs.totRequestsCancelled++
		metrics.AddReject(req.ctx, qs.qCfg.Name, req.fsName, "time-out")
		metrics.AddRequestsInQueues(req.ctx, qs.qCfg.Name, req.fsName, -1)
		metrics.AddSeatsInQueues(req.ctx, qs.qCfg.Name, req.fsName, -req.MaxSeats())
		req.NoteQueued(false)
		qs.reqsGaugePair.RequestsWaiting.Add(-1)
		qs.seatDemandIntegrator.Set(float64(qs.totSeatsInUse + qs.totSeatsWaiting))
	}
	return false, qs.isIdleLocked()
}

// IsIdle 返回当前 queueSet 是否没有等待或执行中的请求。
func (qs *queueSet) IsIdle() bool {
	qs.lock.Lock()
	defer qs.lock.Unlock()
	return qs.isIdleLocked()
}

// isIdleLocked 的调用方必须已持有锁。
func (qs *queueSet) isIdleLocked() bool {
	return qs.totRequestsWaiting == 0 && qs.totRequestsExecuting == 0
}

// lockAndSyncTime 获取锁并更新虚拟时间。
// 把两者放在一起做，可以避免在调用 syncTimeLocked 之前
// 就修改了某些队列状态这种错误。
func (qs *queueSet) lockAndSyncTime(ctx context.Context) {
	qs.lock.Lock()
	qs.syncTimeLocked(ctx)
}

// syncTimeLocked 基于如下假设更新虚拟时间：
// 队列的当前状态自 `qs.lastRealTime` 起
// 一直有效。因此它应当在获取锁之后、
// 修改任何队列状态之前被调用。
func (qs *queueSet) syncTimeLocked(ctx context.Context) {
	realNow := qs.clock.Now()
	timeSinceLast := realNow.Sub(qs.lastRealTime)
	qs.lastRealTime = realNow
	prevR := qs.currentR
	incrR := fqrequest.SeatsTimesDuration(qs.getVirtualTimeRatioLocked(), timeSinceLast)
	qs.currentR = prevR + incrR
	switch {
	case prevR > qs.currentR:
		// 无符号量回绕意味着溢出，记录错误但不回退状态。
		klog.ErrorS(errors.New("queueset::currentR overflow"), "Overflow", "QS", qs.qCfg.Name, "when", realNow.Format(nsTimeFmt), "prevR", prevR, "incrR", incrR, "currentR", qs.currentR)
	case qs.currentR >= highR:
		qs.advanceEpoch(ctx, realNow, incrR)
	}
	metrics.SetCurrentR(qs.qCfg.Name, qs.currentR.ToFloat())
}

// rDecrement 是为避免溢出而需要将进度计 R 往回拨的量。
const rDecrement = fqrequest.MaxSeatSeconds / 2

// highR 是触发纪元（epoch）推进的阈值。
// 也就是说，把全局进度计 R 减去 rDecrement。
const highR = rDecrement + rDecrement/2

// advanceEpoch 从全局进度计 R 以及所有从该计
// 读出的读数中减去 rDecrement。
// now 和 incrR 参数仅用于往日志消息中附加信息。
//
// 所有 R 值同时平移，因此队列之间的相对顺序不变；这是避免计数器溢出的
// 唯一安全做法（只改全局 R 会破坏公平性）。
func (qs *queueSet) advanceEpoch(ctx context.Context, now time.Time, incrR fqrequest.SeatSeconds) {
	oldR := qs.currentR
	qs.currentR -= rDecrement
	klog.InfoS("Advancing epoch", "QS", qs.qCfg.Name, "when", now.Format(nsTimeFmt), "oldR", oldR, "newR", qs.currentR, "incrR", incrR)
	success := true
	for qIdx, queue := range qs.queues {
		if queue.requestsWaiting.Length() == 0 && queue.requestsExecuting.Len() == 0 {
			// 不只是做减法，因为这个值可能相当陈旧。
			// 在这种情况下重置为零是安全的，因为下一个请求
			// 会用 `qs.currentR` 覆盖掉这个零值。
			queue.nextDispatchR = 0
			continue
		}
		oldNextDispatchR := queue.nextDispatchR
		queue.nextDispatchR -= rDecrement
		// 无符号量下溢会回绕成极大值，说明状态已损坏，记录后继续（不中断服务）。
		if queue.nextDispatchR > oldNextDispatchR {
			klog.ErrorS(errors.New("queue::nextDispatchR underflow"), "Underflow", "QS", qs.qCfg.Name, "queue", qIdx, "oldNextDispatchR", oldNextDispatchR, "newNextDispatchR", queue.nextDispatchR, "incrR", incrR)
			success = false
		}
		queue.requestsWaiting.Walk(func(req *request) bool {
			oldArrivalR := req.arrivalR
			req.arrivalR -= rDecrement
			if req.arrivalR > oldArrivalR {
				klog.ErrorS(errors.New("request::arrivalR underflow"), "Underflow", "QS", qs.qCfg.Name, "queue", qIdx, "request", *req, "oldArrivalR", oldArrivalR, "incrR", incrR)
				success = false
			}
			return true
		})
	}
	metrics.AddEpochAdvance(ctx, qs.qCfg.Name, success)
}

// getVirtualTimeRatio 按照 `doc.go` 中的逻辑，
// 计算虚拟时间一直在推进的速率。
//
// 返回“每单位真实时间推进多少虚拟时间”：活跃队列越多，虚拟时间推进越慢，
// 从而在队列之间分摊并发额度。
func (qs *queueSet) getVirtualTimeRatioLocked() float64 {
	activeQueues := 0
	seatsRequested := 0
	for _, queue := range qs.queues {
		// 这里要的是本队列中请求的最大宽度（max width）之和，因为我们的
		// 目标是找出该队列可能达到的最大工作速率。
		seatsRequested += (queue.seatsInUse + queue.requestsWaiting.QueueSum().MaxSeatsSum)
		if queue.requestsWaiting.Length() > 0 || queue.requestsExecuting.Len() > 0 {
			activeQueues++
		}
	}
	if activeQueues == 0 {
		return 0
	}
	return math.Min(float64(seatsRequested), float64(qs.dCfg.ConcurrencyLimit)) / float64(activeQueues)
}

// shuffleShardAndRejectOrEnqueueLocked 封装了为 queueSet/QueueSet
// 校验并入队一个请求所需的逻辑：
// 1) 先做 shuffle sharding，选出一个队列。
// 2) 若并发份额不足且队列长度已达上限，
// 则拒绝当前请求
// 3) 若未被拒绝，创建请求并入队
// 成功入队时返回被入队的请求
// 在没有可用并发份额或
// 已达 QueueLengthLimit 时返回 nil
func (qs *queueSet) shuffleShardAndRejectOrEnqueueLocked(ctx context.Context, workEstimate *fqrequest.WorkEstimate, hashValue uint64, flowDistinguisher, fsName string, descr1, descr2 interface{}, queueNoteFn fq.QueueNoteFn) *request {
	// 先做 shuffle sharding，选出一个队列。
	queueIdx := qs.shuffleShardLocked(hashValue, descr1, descr2)
	queue := qs.queues[queueIdx]

	// 入队/出队都可能改变队首，统一在返回前修正 virtualStart。
	defer qs.boundNextDispatchLocked(queue)

	// 创建请求并入队
	req := &request{
		qs:                qs,
		fsName:            fsName,
		flowDistinguisher: flowDistinguisher,
		ctx:               ctx,
		// 默认决策为 cancel：若请求在派发前 ctx 结束，promise 会以 cancel 完成。
		decision:     qs.promiseFactory(nil, ctx, decisionCancel),
		arrivalTime:  qs.clock.Now(),
		arrivalR:     qs.currentR,
		queue:        queue,
		descr1:       descr1,
		descr2:       descr2,
		queueNoteFn:  queueNoteFn,
		workEstimate: qs.completeWorkEstimate(workEstimate),
	}
	if ok := qs.rejectOrEnqueueToBoundLocked(req); !ok {
		return nil
	}
	metrics.ObserveQueueLength(ctx, qs.qCfg.Name, fsName, queue.requestsWaiting.Length())
	return req
}

// shuffleShardLocked 用给定的 hashValue 和 queueSet 的
// shuffle sharding 参数，选出队列下标。
//
// 在“手牌”中选择当前排队工作量最小的队列；offset 轮换保证手牌重叠的流不会
// 总是偏向同一队列，从而消除系统性偏差。
func (qs *queueSet) shuffleShardLocked(hashValue uint64, descr1, descr2 interface{}) int {
	var backHand [8]int
	// 把牌发到数据结构里，这样下面的访问顺序不必与发牌顺序一致。
	// 这消除了手牌重叠的流之间的偏差。
	hand := qs.dealer.DealIntoHand(hashValue, backHand[:])
	handSize := len(hand)
	offset := qs.enqueues % handSize
	qs.enqueues++
	bestQueueIdx := -1
	minQueueSeatSeconds := fqrequest.MaxSeatSeconds
	for i := 0; i < handSize; i++ {
		queueIdx := hand[(offset+i)%handSize]
		queue := qs.queues[queueIdx]
		queueSum := queue.requestsWaiting.QueueSum()

		// 这是该队列中等待请求的总工作量（以席位秒计），
		// 我们将选择工作量最小的队列。
		thisQueueSeatSeconds := queueSum.TotalWorkSum
		klog.V(7).Infof("QS(%s): For request %#+v %#+v considering queue %d with sum: %#v and %d seats in use, nextDispatchR=%v", qs.qCfg.Name, descr1, descr2, queueIdx, queueSum, queue.seatsInUse, queue.nextDispatchR)
		if thisQueueSeatSeconds < minQueueSeatSeconds {
			minQueueSeatSeconds = thisQueueSeatSeconds
			bestQueueIdx = queueIdx
		}
	}
	if klogV := klog.V(6); klogV.Enabled() {
		chosenQueue := qs.queues[bestQueueIdx]
		klogV.Infof("QS(%s) at t=%s R=%v: For request %#+v %#+v chose queue %d, with sum: %#v & %d seats in use & nextDispatchR=%v", qs.qCfg.Name, qs.clock.Now().Format(nsTimeFmt), qs.currentR, descr1, descr2, bestQueueIdx, chosenQueue.requestsWaiting.QueueSum(), chosenQueue.seatsInUse, chosenQueue.nextDispatchR)
	}
	return bestQueueIdx
}

// rejectOrEnqueueToBoundLocked 拒绝或入队新到达的、
// 已分配到某个队列的请求。若同时撞上队列长度上限和
// 并发上限则返回 false。
// 否则入队并返回 true。
func (qs *queueSet) rejectOrEnqueueToBoundLocked(request *request) bool {
	queue := request.queue
	curQueueLength := queue.requestsWaiting.Length()
	// 资源条件不满足时拒绝新到达的请求；两个条件必须同时满足才拒绝：仍有并发余量时即便队列满也应先派发。
	if qs.totSeatsInUse >= qs.dCfg.ConcurrencyLimit &&
		curQueueLength >= qs.qCfg.QueueLengthLimit {
		return false
	}

	qs.enqueueToBoundLocked(request)
	return true
}

// 把一个请求入队到它所属的队列。
func (qs *queueSet) enqueueToBoundLocked(request *request) {
	queue := request.queue
	now := qs.clock.Now()
	if queue.requestsWaiting.Length() == 0 && queue.requestsExecuting.Len() == 0 {
		// 空队列的起始 R 被设为当前虚拟时间，避免继承历史积压造成的过早 virtualStart。
		queue.nextDispatchR = qs.currentR
		klogV := klog.V(6)
		if klogV.Enabled() {
			klogV.Infof("QS(%s) at t=%s R=%v: initialized queue %d start R due to request %#+v %#+v", qs.qCfg.Name, now.Format(nsTimeFmt), queue.nextDispatchR, queue.index, request.descr1, request.descr2)
		}
	}
	// Enqueue 返回一个删除函数，取消时用它从队列中摘除请求。
	request.removeFromQueueLocked = queue.requestsWaiting.Enqueue(request)
	qs.totRequestsWaiting++
	qs.totSeatsWaiting += request.MaxSeats()
	metrics.AddRequestsInQueues(request.ctx, qs.qCfg.Name, request.fsName, 1)
	metrics.AddSeatsInQueues(request.ctx, qs.qCfg.Name, request.fsName, request.MaxSeats())
	request.NoteQueued(true)
	qs.reqsGaugePair.RequestsWaiting.Add(1)
	qs.seatDemandIntegrator.Set(float64(qs.totSeatsInUse + qs.totSeatsWaiting))
}

// dispatchAsMuchAsPossibleLocked 尽可能多地执行当前可以完成的派发。
func (qs *queueSet) dispatchAsMuchAsPossibleLocked() {
	for qs.totRequestsWaiting != 0 && qs.totSeatsInUse < qs.dCfg.ConcurrencyLimit && qs.dispatchLocked() {
	}
}

// dispatchSansQueueLocked 在“不排队”模式下直接让请求开始执行，仅受并发限制约束。
func (qs *queueSet) dispatchSansQueueLocked(ctx context.Context, workEstimate *fqrequest.WorkEstimate, flowDistinguisher, fsName string, descr1, descr2 interface{}) *request {
	// 不调用 metrics.SetDispatchMetrics，因为不存在排队，也就没有有趣的虚拟世界
	now := qs.clock.Now()
	req := &request{
		qs:                qs,
		fsName:            fsName,
		flowDistinguisher: flowDistinguisher,
		ctx:               ctx,
		startTime:         now,
		// 无需等待：决策直接置为 execute。
		decision:     qs.promiseFactory(decisionExecute, ctx, decisionCancel),
		arrivalTime:  now,
		arrivalR:     qs.currentR,
		descr1:       descr1,
		descr2:       descr2,
		workEstimate: qs.completeWorkEstimate(workEstimate),
	}
	qs.totRequestsExecuting++
	qs.totSeatsInUse += req.MaxSeats()
	qs.requestsExecutingSet = qs.requestsExecutingSet.Insert(req)
	metrics.AddRequestsExecuting(ctx, qs.qCfg.Name, fsName, 1)
	metrics.AddSeatConcurrencyInUse(qs.qCfg.Name, fsName, req.MaxSeats())
	qs.reqsGaugePair.RequestsExecuting.Add(1)
	qs.execSeatsGauge.Add(float64(req.MaxSeats()))
	qs.seatDemandIntegrator.Set(float64(qs.totSeatsInUse + qs.totSeatsWaiting))
	klogV := klog.V(5)
	if klogV.Enabled() {
		klogV.Infof("QS(%s) at t=%s R=%v: immediate dispatch of request %q %#+v %#+v, qs will have %d executing", qs.qCfg.Name, now.Format(nsTimeFmt), qs.currentR, fsName, descr1, descr2, qs.totRequestsExecuting)
	}
	return req
}

// dispatchLocked 使用「面向服务器请求的公平排队」方法
// 挑选一个队列并派发该队列中最老的请求。返回值表示
// 是否有一个请求被出队；当所有队列都为空，
// 或下一个队列的队首请求无法派发时，返回 false。
func (qs *queueSet) dispatchLocked() bool {
	queue, request := qs.findDispatchQueueToBoundLocked()
	if queue == nil {
		return false
	}
	if request == nil { // 这不应该发生。但万一发生了……
		return false
	}
	qs.totRequestsWaiting--
	qs.totSeatsWaiting -= request.MaxSeats()
	metrics.AddRequestsInQueues(request.ctx, qs.qCfg.Name, request.fsName, -1)
	metrics.AddSeatsInQueues(request.ctx, qs.qCfg.Name, request.fsName, -request.MaxSeats())
	request.NoteQueued(false)
	qs.reqsGaugePair.RequestsWaiting.Add(-1)
	// 派发后队首变化，需要修正 virtualStart。
	defer qs.boundNextDispatchLocked(queue)
	// 若请求在派发前已被取消，Set 返回 false；此时不能再执行它。
	if !request.decision.Set(decisionExecute) {
		qs.seatDemandIntegrator.Set(float64(qs.totSeatsInUse + qs.totSeatsWaiting))
		return true
	}
	request.startTime = qs.clock.Now()
	// 就在此刻，请求离开其队列并开始执行。
	// 我们不承认「排队中」与「执行中」之间存在
	// 任何中间状态。虽然这意味着「执行中」
	// 包含了本包带来的一点开销，但这不是
	// 问题，因为其他开销也同样被计入。
	qs.totRequestsExecuting++
	qs.totSeatsInUse += request.MaxSeats()
	queue.requestsExecuting = queue.requestsExecuting.Insert(request)
	queue.seatsInUse += request.MaxSeats()
	metrics.AddRequestsExecuting(request.ctx, qs.qCfg.Name, request.fsName, 1)
	metrics.AddSeatConcurrencyInUse(qs.qCfg.Name, request.fsName, request.MaxSeats())
	qs.reqsGaugePair.RequestsExecuting.Add(1)
	qs.execSeatsGauge.Add(float64(request.MaxSeats()))
	qs.seatDemandIntegrator.Set(float64(qs.totSeatsInUse + qs.totSeatsWaiting))
	klogV := klog.V(6)
	if klogV.Enabled() {
		klogV.Infof("QS(%s) at t=%s R=%v: dispatching request %#+v %#+v work %v from queue %d with start R %v, queue will have %d waiting & %d requests occupying %d seats, set will have %d seats occupied",
			qs.qCfg.Name, request.startTime.Format(nsTimeFmt), qs.currentR, request.descr1, request.descr2,
			request.workEstimate, queue.index, queue.nextDispatchR, queue.requestsWaiting.Length(), queue.requestsExecuting.Len(), queue.seatsInUse, qs.totSeatsInUse)
	}
	// 请求出队获得服务时：qs.virtualStart += G * width
	// 单个请求的工作量若异常巨大，说明估算或配置有误，仅记录日志而不阻断。
	if request.totalWork() > rDecrement/100 { // 单次增量绝不应如此之大
		klog.Errorf("QS(%s) at t=%s R=%v: dispatching request %#+v %#+v with implausibly high work %v from queue %d with start R %v",
			qs.qCfg.Name, request.startTime.Format(nsTimeFmt), qs.currentR, request.descr1, request.descr2,
			request.workEstimate, queue.index, queue.nextDispatchR)
	}
	queue.nextDispatchR += request.totalWork()
	return true
}

// canAccommodateSeatsLocked 在本 queueSet 有足够的席位
// 可以容纳具有指定位数席位的请求时返回 true，
// 否则返回 false。
func (qs *queueSet) canAccommodateSeatsLocked(seats int) bool {
	switch {
	case qs.qCfg.DesiredNumQueues < 0:
		// 负值表示豁免流（exempt），完全不受并发限制。
		return true
	case seats > qs.dCfg.ConcurrencyLimit:
		// 我们已经挑出了虚拟完成时间最小的队列，但
		// 该请求申请的席位数超过了并发限制。
		// TODO: 这只是一个临时的快速修复，等有借用机制就位后就不再需要
		if qs.totRequestsExecuting == 0 {
			// 单个请求超过总并发额度时，允许其在系统空闲时独占执行，避免永久饥饿。
			// TODO: 为该请求施加额外的延迟，如 KEP 所述
			return true
		}
		// 要等本 queueSet 中「当前」正在执行的所有请求
		// 结束之后，才能执行这个请求。
		return false
	case qs.totSeatsInUse+seats > qs.dCfg.ConcurrencyLimit:
		return false
	}

	return true
}

// findDispatchQueueToBoundLocked 按轮询顺序检查各队列，
// 返回其中「最老等待请求的虚拟完成时间」最小的第一个队列，
// 并同时返回那个请求。
// 若所选队列的队首此刻无法派发，则返回两个 nil，
// 此时调用方无需随后补一次 `qs.boundNextDispatchLocked`。
func (qs *queueSet) findDispatchQueueToBoundLocked() (*queue, *request) {
	minVirtualFinish := fqrequest.MaxSeatSeconds
	sMin := fqrequest.MaxSeatSeconds
	dsMin := fqrequest.MaxSeatSeconds
	sMax := fqrequest.MinSeatSeconds
	dsMax := fqrequest.MinSeatSeconds
	var minQueue *queue
	var minIndex int
	nq := len(qs.queues)
	for range qs.queues {
		// 轮询起点每次前移一位，使虚拟完成时间相同的队列轮流获胜。
		qs.robinIndex = (qs.robinIndex + 1) % nq
		queue := qs.queues[qs.robinIndex]
		oldestWaiting, _ := queue.requestsWaiting.Peek()
		if oldestWaiting != nil {
			sMin = min(sMin, queue.nextDispatchR)
			sMax = max(sMax, queue.nextDispatchR)
			estimatedWorkInProgress := fqrequest.SeatsTimesDuration(float64(queue.seatsInUse), qs.estimatedServiceDuration)
			dsMin = min(dsMin, queue.nextDispatchR-estimatedWorkInProgress)
			dsMax = max(dsMax, queue.nextDispatchR-estimatedWorkInProgress)
			currentVirtualFinish := queue.nextDispatchR + oldestWaiting.totalWork()
			klog.V(11).InfoS("Considering queue to dispatch", "queueSet", qs.qCfg.Name, "queue", qs.robinIndex, "finishR", currentVirtualFinish)
			if currentVirtualFinish < minVirtualFinish {
				minVirtualFinish = currentVirtualFinish
				minQueue = queue
				minIndex = qs.robinIndex
			}
		}
	}

	oldestReqFromMinQueue, _ := minQueue.requestsWaiting.Peek()
	if oldestReqFromMinQueue == nil {
		// 这不可能发生
		klog.ErrorS(errors.New("selected queue is empty"), "Impossible", "queueSet", qs.qCfg.Name)
		return nil, nil
	}
	if !qs.canAccommodateSeatsLocked(oldestReqFromMinQueue.MaxSeats()) {
		// 由于我们并没有选出虚拟完成时间最小的
		// 队列，这里不推进轮询下标。
		klogV := klog.V(4)
		if klogV.Enabled() {
			klogV.Infof("QS(%s): request %v %v seats %d cannot be dispatched from queue %d, waiting for currently executing requests to complete, %d requests are occupying %d seats and the limit is %d",
				qs.qCfg.Name, oldestReqFromMinQueue.descr1, oldestReqFromMinQueue.descr2, oldestReqFromMinQueue.MaxSeats(), minQueue.index, qs.totRequestsExecuting, qs.totSeatsInUse, qs.dCfg.ConcurrencyLimit)
		}
		metrics.AddDispatchWithNoAccommodation(qs.qCfg.Name, oldestReqFromMinQueue.fsName)
		return nil, nil
	}
	oldestReqFromMinQueue.removeFromQueueLocked()

	// 若请求的最终席位超过该队列容量，则将其降为当前容量并调整附加延迟，以保持总工作量守恒；
	// 这是“超出并发上限的请求”的补偿：减少席位、增加延迟，保持总工作量守恒。
	if oldestReqFromMinQueue.workEstimate.FinalSeats > uint64(qs.dCfg.ConcurrencyLimit) {
		finalSeats := uint64(qs.dCfg.ConcurrencyLimit)
		additionalLatency := oldestReqFromMinQueue.workEstimate.finalWork.DurationPerSeat(float64(finalSeats))
		oldestReqFromMinQueue.workEstimate.FinalSeats = finalSeats
		oldestReqFromMinQueue.workEstimate.AdditionalLatency = additionalLatency
	}

	// 我们把下一轮的轮询起始下标设为
	// 本轮选中的队列。这样在虚拟完成时间相同的情况下，
	// 未被选中的队列会胜出
	qs.robinIndex = minIndex

	// 派发点早于到达点说明虚拟时间被错误推进，记录但继续运行。
	if minQueue.nextDispatchR < oldestReqFromMinQueue.arrivalR {
		klog.ErrorS(errors.New("dispatch before arrival"), "Inconceivable!", "QS", qs.qCfg.Name, "queue", minQueue.index, "dispatchR", minQueue.nextDispatchR, "request", oldestReqFromMinQueue)
	}
	metrics.SetDispatchMetrics(qs.qCfg.Name, qs.currentR.ToFloat(), minQueue.nextDispatchR.ToFloat(), sMin.ToFloat(), sMax.ToFloat(), dsMin.ToFloat(), dsMax.ToFloat())
	return minQueue, oldestReqFromMinQueue
}

// finishRequestAndDispatchAsMuchAsPossible 是一个便捷方法，
// 它对给定请求调用 finishRequest，然后尽可能多地
// 派发请求。这是一个请求执行完毕或被取消后
// 需要做的全部事情。返回的 bool 表示
// QueueSet 此刻是否空闲。
func (qs *queueSet) finishRequestAndDispatchAsMuchAsPossible(req *request) bool {
	qs.lockAndSyncTime(req.ctx)
	defer qs.lock.Unlock()

	qs.finishRequestLocked(req)
	qs.dispatchAsMuchAsPossibleLocked()
	return qs.isIdleLocked()
}

// finishRequestLocked 是此前被派发的请求完成其服务时
// 应当使用的回调。该回调会更新 queueSet 中的重要状态
func (qs *queueSet) finishRequestLocked(r *request) {
	now := qs.clock.Now()
	qs.totRequestsExecuting--
	metrics.AddRequestsExecuting(r.ctx, qs.qCfg.Name, r.fsName, -1)
	qs.reqsGaugePair.RequestsExecuting.Add(-1)

	actualServiceDuration := now.Sub(r.startTime)

	// TODO: 目前把逻辑放在局部，是为了更容易看清
	//  queueSet 与 queue 的计数器如何跟踪；将来
	//  可以重构把这个函数移走。
	// releaseSeatsLocked 幂等性由调用方保证：只在延迟结束或立即释放时调用一次。
	releaseSeatsLocked := func() {
		defer qs.removeQueueIfEmptyLocked(r)

		qs.totSeatsInUse -= r.MaxSeats()
		metrics.AddSeatConcurrencyInUse(qs.qCfg.Name, r.fsName, -r.MaxSeats())
		qs.execSeatsGauge.Add(-float64(r.MaxSeats()))
		qs.seatDemandIntegrator.Set(float64(qs.totSeatsInUse + qs.totSeatsWaiting))
		if r.queue != nil {
			r.queue.seatsInUse -= r.MaxSeats()
		}
	}

	defer func() {
		klogV := klog.V(6)
		if r.workEstimate.AdditionalLatency <= 0 {
			// 立即释放分配给该请求的席位
			releaseSeatsLocked()
			if !klogV.Enabled() {
			} else if r.queue != nil {
				klogV.Infof("QS(%s) at t=%s R=%v: request %#+v %#+v finished all use of %d seats, adjusted queue %d start R to %v due to service time %.9fs, queue will have %d requests with %#v waiting & %d requests occupying %d seats",
					qs.qCfg.Name, now.Format(nsTimeFmt), qs.currentR, r.descr1, r.descr2, r.workEstimate.MaxSeats(), r.queue.index,
					r.queue.nextDispatchR, actualServiceDuration.Seconds(), r.queue.requestsWaiting.Length(), r.queue.requestsWaiting.QueueSum(), r.queue.requestsExecuting.Len(), r.queue.seatsInUse)
			} else {
				klogV.Infof("QS(%s) at t=%s R=%v: request %#+v %#+v finished all use of %d seats, qs will have %d requests occupying %d seats", qs.qCfg.Name, now.Format(nsTimeFmt), qs.currentR, r.descr1, r.descr2, r.workEstimate.InitialSeats, qs.totRequestsExecuting, qs.totSeatsInUse)
			}
			return
		}

		additionalLatency := r.workEstimate.AdditionalLatency
		if !klogV.Enabled() {
		} else if r.queue != nil {
			klogV.Infof("QS(%s) at t=%s R=%v: request %#+v %#+v finished main use of %d seats but lingering on %d seats for %v seconds, adjusted queue %d start R to %v due to service time %.9fs, queue will have %d requests with %#v waiting & %d requests occupying %d seats",
				qs.qCfg.Name, now.Format(nsTimeFmt), qs.currentR, r.descr1, r.descr2, r.workEstimate.InitialSeats, r.workEstimate.FinalSeats, additionalLatency.Seconds(), r.queue.index,
				r.queue.nextDispatchR, actualServiceDuration.Seconds(), r.queue.requestsWaiting.Length(), r.queue.requestsWaiting.QueueSum(), r.queue.requestsExecuting.Len(), r.queue.seatsInUse)
		} else {
			klogV.Infof("QS(%s) at t=%s R=%v: request %#+v %#+v finished main use of %d seats but lingering on %d seats for %v seconds, qs will have %d requests occupying %d seats", qs.qCfg.Name, now.Format(nsTimeFmt), qs.currentR, r.descr1, r.descr2, r.workEstimate.InitialSeats, r.workEstimate.FinalSeats, additionalLatency.Seconds(), qs.totRequestsExecuting, qs.totSeatsInUse)
		}
		// EventAfterDuration 会在新 goroutine 中执行事件函数，因此分配给该请求的席位要等 AdditionalLatency 过后才释放，从而确保附加延迟不影响用户体验。
		// 延迟释放期间席位仍计入 totSeatsInUse，从而真正降低并发而不是仅做统计。
		qs.clock.EventAfterDuration(func(_ time.Time) {
			qs.lockAndSyncTime(r.ctx)
			defer qs.lock.Unlock()
			now := qs.clock.Now()
			releaseSeatsLocked()
			if !klogV.Enabled() {
			} else if r.queue != nil {
				klogV.Infof("QS(%s) at t=%s R=%v: request %#+v %#+v finished lingering on %d seats, queue %d will have %d requests with %#v waiting & %d requests occupying %d seats",
					qs.qCfg.Name, now.Format(nsTimeFmt), qs.currentR, r.descr1, r.descr2, r.workEstimate.FinalSeats, r.queue.index,
					r.queue.requestsWaiting.Length(), r.queue.requestsWaiting.QueueSum(), r.queue.requestsExecuting.Len(), r.queue.seatsInUse)
			} else {
				klogV.Infof("QS(%s) at t=%s R=%v: request %#+v %#+v finished lingering on %d seats, qs will have %d requests occupying %d seats", qs.qCfg.Name, now.Format(nsTimeFmt), qs.currentR, r.descr1, r.descr2, r.workEstimate.FinalSeats, qs.totRequestsExecuting, qs.totSeatsInUse)
			}
			qs.dispatchAsMuchAsPossibleLocked()
		}, additionalLatency)
	}()

	if r.queue != nil {
		// 请求已结束，从 requests executing 中移除
		r.queue.requestsExecuting = r.queue.requestsExecuting.Delete(r)

		// 请求服务结束、实际服务时间为 S 时，队列的起始 R 按 (G - S)*width 递减：
		// 实际服务时间短于估计时把多算的虚拟时间退还，长于估计则提前推进。
		r.queue.nextDispatchR -= fqrequest.SeatsTimesDuration(float64(r.InitialSeats()), qs.estimatedServiceDuration-actualServiceDuration)
		qs.boundNextDispatchLocked(r.queue)
	} else {
		qs.requestsExecutingSet = qs.requestsExecutingSet.Delete(r)
	}
}

// boundNextDispatchLocked 应用防绕组（anti-windup）技巧。
// 之所以需要技巧，是因为所有非空队列都被分配相同数量的
// 席位。一个用不满这些席位、又不会变空的队列，
// 其 `virtualStart` 会积累得越来越提前，
// 与那些实际用量超过分配量的队列相比就不公平。
// 下面的技巧针对这种不公平的前一半，
// 办法是坚持虚拟世界中的派发不得早于到达时刻。
func (qs *queueSet) boundNextDispatchLocked(queue *queue) {
	oldestReqFromMinQueue, _ := queue.requestsWaiting.Peek()
	if oldestReqFromMinQueue == nil {
		return
	}
	var virtualStartBound = oldestReqFromMinQueue.arrivalR
	if queue.nextDispatchR < virtualStartBound {
		if klogV := klog.V(4); klogV.Enabled() {
			klogV.InfoS("AntiWindup tweaked queue", "QS", qs.qCfg.Name, "queue", queue.index, "time", qs.clock.Now().Format(nsTimeFmt), "requestDescr1", oldestReqFromMinQueue.descr1, "requestDescr2", oldestReqFromMinQueue.descr2, "newVirtualStart", virtualStartBound, "deltaVirtualStart", (virtualStartBound - queue.nextDispatchR))
		}
		queue.nextDispatchR = virtualStartBound
	}
}

// removeQueueIfEmptyLocked 在队列数超过期望值且队列已空时按“自然淘汰”移除队列，
// 避免配置缩容时立即丢弃仍在排队的请求。
func (qs *queueSet) removeQueueIfEmptyLocked(r *request) {
	if r.queue == nil {
		return
	}

	// 若队列数量超过期望值，且该队列已没有请求，
	// 就把它移除
	if len(qs.queues) > qs.qCfg.DesiredNumQueues &&
		r.queue.requestsWaiting.Length() == 0 &&
		r.queue.requestsExecuting.Len() == 0 {
		qs.queues = removeQueueAndUpdateIndexes(qs.queues, r.queue.index)

		// 这里做递减以维持不变量：(qs.robinIndex+1) % numQueues
		// 是「下一个待派发队列」的下标，即最近被派发队列的后继
		if qs.robinIndex >= r.queue.index {
			qs.robinIndex--
		}
	}
}

// removeQueueAndUpdateIndexes 用重切（reslicing）从切片中移除某个下标的队列，
// 然后更新各队列的 'index' 字段使其正确
func removeQueueAndUpdateIndexes(queues []*queue, index int) []*queue {
	keptQueues := append(queues[:index], queues[index+1:]...)
	// 删除后所有后继队列的 index 需要前移，以维持 index == 切片下标的约定。
	for i := index; i < len(keptQueues); i++ {
		keptQueues[i].index--
	}
	return keptQueues
}

// Dump 返回队列集的可观测快照，供调试端点使用；includeRequestDetails
// 决定是否包含请求级细节（可能较敏感/昂贵）。
func (qs *queueSet) Dump(includeRequestDetails bool) debug.QueueSetDump {
	qs.lock.Lock()
	defer qs.lock.Unlock()
	d := debug.QueueSetDump{
		Queues:                     make([]debug.QueueDump, len(qs.queues)),
		QueuelessExecutingRequests: SetMapReduce(dumpRequest(includeRequestDetails), append1[debug.RequestDump])(qs.requestsExecutingSet),
		Waiting:                    qs.totRequestsWaiting,
		Executing:                  qs.totRequestsExecuting,
		SeatsInUse:                 qs.totSeatsInUse,
		SeatsWaiting:               qs.totSeatsWaiting,
		Dispatched:                 qs.totRequestsDispatched,
		Rejected:                   qs.totRequestsRejected,
		Timedout:                   qs.totRequestsTimedout,
		Cancelled:                  qs.totRequestsCancelled,
	}
	for i, q := range qs.queues {
		d.Queues[i] = q.dumpLocked(includeRequestDetails)
	}
	return d
}

// OnRequestDispatched 记录一次成功派发；由上层在请求真正开始处理后调用。
// 类型断言失败时静默返回，使该函数可安全地用于任意 fq.Request 实现。
func OnRequestDispatched(r fq.Request) {
	req, ok := r.(*request)
	if !ok {
		return
	}

	qs := req.qs
	if qs != nil {
		qs.lock.Lock()
		defer qs.lock.Unlock()
		qs.totRequestsDispatched++
	}
}
