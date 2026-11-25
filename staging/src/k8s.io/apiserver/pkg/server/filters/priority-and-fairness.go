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

package filters

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	epmetrics "k8s.io/apiserver/pkg/endpoints/metrics"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/httplog"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	fcmetrics "k8s.io/apiserver/pkg/util/flowcontrol/metrics"
	flowcontrolrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	"k8s.io/klog/v2"
	utilsclock "k8s.io/utils/clock"
)

// PriorityAndFairnessClassification 标识 API 优先级与公平性（APF）
// 对请求进行分类后的结果。
//
// 该结果用于回写响应头，让客户端/代理知道请求被哪个 FlowSchema 与
// PriorityLevelConfiguration 处理，但不暴露其可读名称（见 setResponseHeaders）。
type PriorityAndFairnessClassification struct {
	FlowSchemaName    string
	FlowSchemaUID     apitypes.UID
	PriorityLevelName string
	PriorityLevelUID  apitypes.UID
}

// waitingMark 跟踪处于等待（排队）状态而非正在执行的请求数。
var waitingMark = &requestWatermark{
	phase: epmetrics.WaitingPhase,
}

// 执行/等待中的请求数用原子变量按读写分开统计，避免热点锁；
// watermark 在后台周期性记录指标。
var atomicMutatingExecuting, atomicReadOnlyExecuting atomic.Int32
var atomicMutatingWaiting, atomicReadOnlyWaiting atomic.Int32

// newInitializationSignal 仅为测试目的而定义。
var newInitializationSignal = utilflowcontrol.NewInitializationSignal

// truncateLogField 截断日志字段，避免过长的 FlowSchema/PL 名污染访问日志。
func truncateLogField(s string) string {
	const maxFieldLogLength = 64

	if len(s) > maxFieldLogLength {
		s = s[0:maxFieldLogLength]
	}
	return s
}

// initAPFOnce 保证水位标记的指标观察器只接线一次。
var initAPFOnce sync.Once

// priorityAndFairnessHandler 把 APF 的排队/调度逻辑接入 HTTP 处理链。
type priorityAndFairnessHandler struct {
	handler                 http.Handler
	longRunningRequestCheck apirequest.LongRunningRequestCheck
	fcIfc                   utilflowcontrol.Interface
	workEstimator           flowcontrolrequest.WorkEstimatorFunc

	// droppedRequests 记录各优先级的请求丢弃历史，
	// 用于在系统过载时计算 Retry-After 响应头。
	droppedRequests utilflowcontrol.DroppedRequestsTracker

	// newReqWaitCtxFn 创建一个派生 context，其 deadline
	// 表示该请求允许在自己的队列中等待多久。
	newReqWaitCtxFn func(context.Context) (context.Context, context.CancelFunc)
}

// Handle 是 APF 过滤器的主体：分类、排队、执行或拒绝。
func (h *priorityAndFairnessHandler) Handle(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	requestInfo, ok := apirequest.RequestInfoFrom(ctx)
	if !ok {
		handleError(w, r, fmt.Errorf("no RequestInfo found in context"))
		return
	}
	user, ok := apirequest.UserFrom(ctx)
	if !ok {
		handleError(w, r, fmt.Errorf("no User found in context"))
		return
	}

	isWatchRequest := watchVerbs.Has(requestInfo.Verb)

	// 跳过对长时非 watch 请求的跟踪：长连接（exec/port-forward 等）可能持续很久，占用 APF 并发额度会
	// 导致正常请求饥饿；watch 例外，因为它需要按初始化时间计费。
	if h.longRunningRequestCheck != nil && h.longRunningRequestCheck(r, requestInfo) && !isWatchRequest {
		klog.V(6).Infof("Serving RequestInfo=%#+v, user.Info=%#+v as longrunning\n", requestInfo, user)
		h.handler.ServeHTTP(w, r)
		return
	}

	// classification 由 noteFn 在 APF 完成分类后填充；
	// estimateWork 与 setResponseHeaders 依赖它。
	var classification *PriorityAndFairnessClassification
	noteFn := func(fs *flowcontrol.FlowSchema, pl *flowcontrol.PriorityLevelConfiguration, flowDistinguisher string) {
		classification = &PriorityAndFairnessClassification{
			FlowSchemaName:    fs.Name,
			FlowSchemaUID:     fs.UID,
			PriorityLevelName: pl.Name,
			PriorityLevelUID:  pl.UID,
		}

		httplog.AddKeyValue(ctx, "apf_pl", truncateLogField(pl.Name))
		httplog.AddKeyValue(ctx, "apf_fs", truncateLogField(fs.Name))
	}
	// estimateWork 如果被调用，一定发生在 noteFn 之后
	estimateWork := func() flowcontrolrequest.WorkEstimate {
		if classification == nil {
			// workEstimator 在请求分类完成之前就被调用了，
			// 正常情况下绝不应该走到这里。
			klog.ErrorS(fmt.Errorf("workEstimator is being invoked before classification of the request has completed"),
				"Using empty FlowSchema and PriorityLevelConfiguration name", "verb", r.Method, "URI", r.RequestURI)
			return h.workEstimator(r, "", "")
		}

		workEstimate := h.workEstimator(r, classification.FlowSchemaName, classification.PriorityLevelName)

		fcmetrics.ObserveWorkEstimatedSeats(classification.PriorityLevelName, classification.FlowSchemaName, workEstimate.MaxSeats())
		httplog.AddKeyValue(ctx, "apf_iseats", workEstimate.InitialSeats)
		httplog.AddKeyValue(ctx, "apf_fseats", workEstimate.FinalSeats)
		httplog.AddKeyValue(ctx, "apf_additionalLatency", workEstimate.AdditionalLatency)

		return workEstimate
	}

	// served 标记请求是否真正进入 handler；为 false 说明被 APF 拒绝。
	var served bool
	isMutatingRequest := !nonMutatingRequestVerbs.Has(requestInfo.Verb)
	noteExecutingDelta := func(delta int32) {
		if isMutatingRequest {
			watermark.recordMutating(int(atomicMutatingExecuting.Add(delta)))
		} else {
			watermark.recordReadOnly(int(atomicReadOnlyExecuting.Add(delta)))
		}
	}
	noteWaitingDelta := func(delta int32) {
		if isMutatingRequest {
			waitingMark.recordMutating(int(atomicMutatingWaiting.Add(delta)))
		} else {
			waitingMark.recordReadOnly(int(atomicReadOnlyWaiting.Add(delta)))
		}
	}
	queueNote := func(inQueue bool) {
		if inQueue {
			noteWaitingDelta(1)
		} else {
			noteWaitingDelta(-1)
		}
	}

	digest := utilflowcontrol.RequestDigest{
		RequestInfo: requestInfo,
		User:        user,
	}

	if isWatchRequest {
		// 该 channel 会阻塞 handler.ServeHTTP() 的调用直到被关闭，
		// 而关闭动作发生在 execute() 内部。若 APF 拒绝了请求，则它永远不会被关闭。
		//
		// watch 的关键点：APF 只把“初始化”视为一次请求的生命周期，
		// 因此需要一个信号把初始化完成与真正的长连接处理衔接起来。
		shouldStartWatchCh := make(chan struct{})

		watchInitializationSignal := newInitializationSignal()
		// 这会对传给 handler.ServeHTTP() 的请求做包装，
		// 在其 context 中把 watchInitializationSignal 打通到存储层。
		var watchReq *http.Request
		// 该变量在 execute() 内部、关闭 shouldStartWatchCh 之前被赋值。
		// 若请求被 APF 拒绝，它保持为 nil。
		var forgetWatch utilflowcontrol.ForgetWatchFunc

		defer func() {
			// 防止请求未能到达存储层、从而不会发出初始化信号的情况。
			if watchInitializationSignal != nil {
				watchInitializationSignal.Signal()
			}
			// 如果注册过 watcher，就把它忘掉。
			//
			// 这里不存在竞态，因为执行到此刻时，下面两种情况之一已经发生：
			// case <-shouldStartWatchCh: execute() 已完成对 forgetWatch 的赋值
			// case <-resultCh: Handle() 已返回，而 Handle() 在 execute()
			//   运行期间不会返回
			if forgetWatch != nil {
				forgetWatch()
			}
		}()

		execute := func() {
			startedAt := time.Now()
			defer func() {
				httplog.AddKeyValue(ctx, "apf_init_latency", time.Since(startedAt))
			}()
			noteExecutingDelta(1)
			defer noteExecutingDelta(-1)
			served = true
			setResponseHeaders(classification, w)

			forgetWatch = h.fcIfc.RegisterWatch(r)

			// 通知主线程：可以开始建立 watch 了。
			close(shouldStartWatchCh)

			// 从 APF 的视角等待请求真正结束——对 watch 而言，
			// 请求的生命周期在其初始化完成时即告结束。
			watchInitializationSignal.Wait()
		}

		// 保证可以异步向 resultCh 投递一个结果：
		// 缓冲 1 保证负责 APF 的 goroutine 总能投递结果而不泄漏。
		resultCh := make(chan interface{}, 1)

		// 在一个单独的 goroutine 中调用 Handle。
		// 原因是：从 APF 的视角看，watch 一旦完成初始化，请求处理就结束了
		// （这通常比 watch 请求本身快若干个数量级）。也就是说 Handle() 会很快返回，
		// 出于性能考虑我们希望减少运行中的 goroutine 数量——
		// 因此把较短的 Handle() 放进专门的 goroutine，
		// 把真正的 watch 处理留在主 goroutine 里。
		go func() {
			defer func() {
				err := recover()
				// 不要包装哨兵值 http.ErrAbortHandler 这个 panic
				if err != nil && err != http.ErrAbortHandler {
					// 与标准库 http server 的代码一致。手动分配栈
					// 缓冲区大小，避免日志过大
					const size = 64 << 10
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					err = fmt.Sprintf("%v\n%s", err, buf)
				}

				// 无论是否发生 panic，都必须把结果放入 resultCh。
				resultCh <- err
			}()

			// 我们用调整过 deadline 的 handleCtx 来调用，有两个原因。
			// 其一是限制请求在开始执行之前可以等待的时间。
			// 另一个原因是：底层的 Handle() 可能会启动额外的 goroutine，
			// 而它正阻塞在 context 取消上。但从 APF 的视角看，我们不想等到
			// 整个 watch 请求处理完毕（那才是它的 context 真正被取消的时刻）——
			// 请求一旦从 APF 视角处理完成，我们就希望立刻解除该 goroutine 的阻塞。
			//
			// 注意：我们特意不使用该 context 去调用真正的 handler，
			// 以免过早取消请求。
			handleCtx, handleCtxCancel := h.newReqWaitCtxFn(ctx)
			defer handleCtxCancel()

			// 注意：无论请求最终被执行还是被拒绝，Handle 都会返回。
			// 在后一种（被拒绝）情况下，该函数会直接返回，
			// 不会调用传入的 `execute` 函数。
			h.fcIfc.Handle(handleCtx, digest, noteFn, estimateWork, queueNote, execute)
		}()

		select {
		case <-shouldStartWatchCh:
			func() {
				// TODO: 如果两个 goroutine 都发生 panic，需要把两者的堆栈跟踪
				// 都传递出去，才能正确记录日志：
				defer func() {
					// 防止请求未能到达存储层、从而不会发出初始化信号的情况。
					// 这一步必须在下面等待 resultCh 之前完成。
					watchInitializationSignal.Signal()
					// TODO: 考虑在 Handle 调用 panic 时立刻结束该请求。
					if err := <-resultCh; err != nil {
						panic(err)
					}
				}()
				watchCtx := utilflowcontrol.WithInitializationSignal(ctx, watchInitializationSignal)
				watchReq = r.WithContext(watchCtx)
				h.handler.ServeHTTP(w, watchReq)
			}()
		case err := <-resultCh:
			if err != nil {
				panic(err)
			}
		}
	} else {
		execute := func() {
			noteExecutingDelta(1)
			defer noteExecutingDelta(-1)
			served = true
			setResponseHeaders(classification, w)

			h.handler.ServeHTTP(w, r)
		}

		func() {
			handleCtx, cancelFn := h.newReqWaitCtxFn(ctx)
			defer cancelFn()
			h.fcIfc.Handle(handleCtx, digest, noteFn, estimateWork, queueNote, execute)
		}()
	}

	// 未被 APF 放行即视为拒绝：返回 429，并按该优先级的历史丢弃速率
	// 计算 Retry-After，避免客户端立即重试造成雪崩。
	if !served {
		setResponseHeaders(classification, w)

		epmetrics.RecordDroppedRequest(r, requestInfo, epmetrics.APIServerComponent, isMutatingRequest)
		epmetrics.RecordRequestTermination(r, requestInfo, epmetrics.APIServerComponent, http.StatusTooManyRequests)
		h.droppedRequests.RecordDroppedRequest(classification.PriorityLevelName)

		// TODO(wojtek-t): 来自 deads2k 的想法：可以考虑加入一些抖动；
		//  若结果不是整数，直接返回截断后的值，余下部分由服务端 sleep。
		tooManyRequests(r, w, strconv.Itoa(int(h.droppedRequests.GetRetryAfter(classification.PriorityLevelName))))
	}
}

// WithPriorityAndFairness 以一种细粒度的方式限制在途（in-flight）
// 请求的数量。
func WithPriorityAndFairness(
	handler http.Handler,
	longRunningRequestCheck apirequest.LongRunningRequestCheck,
	fcIfc utilflowcontrol.Interface,
	workEstimator flowcontrolrequest.WorkEstimatorFunc,
	defaultRequestWaitLimit time.Duration,
) http.Handler {
	if fcIfc == nil {
		klog.Warningf("priority and fairness support not found, skipping")
		return handler
	}
	initAPFOnce.Do(func() {
		initMaxInFlight(0, 0)
		// 推迟获取这些 gauge，直到其底层指标注册完成之后，
		// 这样它们才能绑定到那个高效的实现上。
		waitingMark.readOnlyObserver = fcmetrics.GetWaitingReadonlyConcurrency()
		waitingMark.mutatingObserver = fcmetrics.GetWaitingMutatingConcurrency()
	})

	clock := &utilsclock.RealClock{}
	newReqWaitCtxFn := func(ctx context.Context) (context.Context, context.CancelFunc) {
		return getRequestWaitContext(ctx, defaultRequestWaitLimit, clock)
	}

	priorityAndFairnessHandler := &priorityAndFairnessHandler{
		handler:                 handler,
		longRunningRequestCheck: longRunningRequestCheck,
		fcIfc:                   fcIfc,
		workEstimator:           workEstimator,
		droppedRequests:         utilflowcontrol.NewDroppedRequestsTracker(),
		newReqWaitCtxFn:         newReqWaitCtxFn,
	}
	return http.HandlerFunc(priorityAndFairnessHandler.Handle)
}

// StartPriorityAndFairnessWatermarkMaintenance 启动用于观测并维护
// priority-and-fairness 请求水位标记的 goroutine。
func StartPriorityAndFairnessWatermarkMaintenance(stopCh <-chan struct{}) {
	startWatermarkMaintenance(watermark, stopCh)
	startWatermarkMaintenance(waitingMark, stopCh)
}

// setResponseHeaders 只暴露 UID 而非名字：名字可能透露优先级分类策略，
// 且集群管理员为分类起的名称不应被普通客户端看到。
func setResponseHeaders(classification *PriorityAndFairnessClassification, w http.ResponseWriter) {
	if classification == nil {
		return
	}

	// 这里刻意设置 flow-schema 与 priority-level 的 UID 而不是名称。
	// 这样做是为了不暴露 cluster-admin 为分类和优先级级别所起的名字；
	// 另外这些名字可能会让用户一眼看出：自己之所以被拒绝，
	// 是因为被归入了低优先级分类。
	w.Header().Set(flowcontrol.ResponseHeaderMatchedPriorityLevelConfigurationUID, string(classification.PriorityLevelUID))
	w.Header().Set(flowcontrol.ResponseHeaderMatchedFlowSchemaUID, string(classification.FlowSchemaUID))
}

// tooManyRequests 返回 429 与 Retry-After 头。
func tooManyRequests(req *http.Request, w http.ResponseWriter, retryAfter string) {
	// 返回 429 状态码，表示 "Too Many Requests"
	w.Header().Set("Retry-After", retryAfter)
	http.Error(w, "Too many requests, please try again later.", http.StatusTooManyRequests)
}

// getRequestWaitContext 返回一个新的 context，其 deadline 表示
// 请求在被从其队列中移除并拒绝之前，最多允许等待多久。
// 返回的 context.CancelFunc 绝不能为 nil，调用方有责任
// 调用该 CancelFunc 以完成清理。
//   - ctx: 与该请求关联的 context（它可能带有 deadline，
//     也可能没有）。
//   - defaultRequestWaitLimit: 当请求 context 没有任何 deadline 时
//     所使用的默认等待时长。适用于
//     (a) watch 的初始化，或
//     (b) context 没有 deadline 的请求
//
// clock 便于在测试中验证该函数
//
// 排队等待时间取请求截止时间的四分之一：既给排队留出空间，
// 又保证被放行后仍有大部分时间用于真正处理。
func getRequestWaitContext(ctx context.Context, defaultRequestWaitLimit time.Duration, clock utilsclock.PassiveClock) (context.Context, context.CancelFunc) {
	if ctx.Err() != nil {
		return ctx, func() {}
	}

	reqArrivedAt := clock.Now()
	// 用接收时间而非当前时间计算剩余预算，避免在队列外耗时被忽略。
	if reqReceivedTimestamp, ok := apirequest.ReceivedTimestampFrom(ctx); ok {
		reqArrivedAt = reqReceivedTimestamp
	}

	// a) 我们允许该请求在其队列中等待的时间，
	// 为其 deadline 总时长的四分之一。
	// b) 如果请求 context 没有任何 deadline，
	// 则回退到默认值 'defaultRequestWaitLimit'。
	// 无论哪种情况，任何请求的等待上限都不得
	// 超过 1m 这个硬性限制
	//
	// 请求有 deadline 时：
	//   wait-limit = min(remaining deadline / 4, 1m)
	// 请求没有 deadline 时：
	//   wait-limit = min(defaultRequestWaitLimit, 1m)
	thisReqWaitLimit := defaultRequestWaitLimit
	if deadline, ok := ctx.Deadline(); ok {
		thisReqWaitLimit = deadline.Sub(reqArrivedAt) / 4
	}
	if thisReqWaitLimit > time.Minute {
		thisReqWaitLimit = time.Minute
	}

	return context.WithDeadline(ctx, reqArrivedAt.Add(thisReqWaitLimit))
}
