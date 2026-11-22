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

// PriorityAndFairnessClassification identifies the results of
// classification for API Priority and Fairness
//
// 该结果用于回写响应头，让客户端/代理知道请求被哪个 FlowSchema 与
// PriorityLevelConfiguration 处理，但不暴露其可读名称（见 setResponseHeaders）。
type PriorityAndFairnessClassification struct {
	FlowSchemaName    string
	FlowSchemaUID     apitypes.UID
	PriorityLevelName string
	PriorityLevelUID  apitypes.UID
}

// waitingMark tracks requests waiting rather than being executed
var waitingMark = &requestWatermark{
	phase: epmetrics.WaitingPhase,
}

// 执行/等待中的请求数用原子变量按读写分开统计，避免热点锁；
// watermark 在后台周期性记录指标。
var atomicMutatingExecuting, atomicReadOnlyExecuting atomic.Int32
var atomicMutatingWaiting, atomicReadOnlyWaiting atomic.Int32

// newInitializationSignal is defined for testing purposes.
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

	// droppedRequests tracks the history of dropped requests for
	// the purpose of computing RetryAfter header to avoid system
	// overload.
	droppedRequests utilflowcontrol.DroppedRequestsTracker

	// newReqWaitCtxFn creates a derived context with a deadline
	// of how long a given request can wait in its queue.
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

	// Skip tracking long running non-watch requests.
	// 长连接（exec/port-forward 等）可能持续很久，占用 APF 并发额度会
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
	// estimateWork is called, if at all, after noteFn
	estimateWork := func() flowcontrolrequest.WorkEstimate {
		if classification == nil {
			// workEstimator is being invoked before classification of
			// the request has completed, we should never be here though.
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
		// This channel blocks calling handler.ServeHTTP() until closed, and is closed inside execute().
		// If APF rejects the request, it is never closed.
		//
		// watch 的关键点：APF 只把“初始化”视为一次请求的生命周期，
		// 因此需要一个信号把初始化完成与真正的长连接处理衔接起来。
		shouldStartWatchCh := make(chan struct{})

		watchInitializationSignal := newInitializationSignal()
		// This wraps the request passed to handler.ServeHTTP(),
		// setting a context that plumbs watchInitializationSignal to storage
		var watchReq *http.Request
		// This is set inside execute(), prior to closing shouldStartWatchCh.
		// If the request is rejected by APF it is left nil.
		var forgetWatch utilflowcontrol.ForgetWatchFunc

		defer func() {
			// Protect from the situation when request will not reach storage layer
			// and the initialization signal will not be send.
			if watchInitializationSignal != nil {
				watchInitializationSignal.Signal()
			}
			// Forget the watcher if it was registered.
			//
			// This is race-free because by this point, one of the following occurred:
			// case <-shouldStartWatchCh: execute() completed the assignment to forgetWatch
			// case <-resultCh: Handle() completed, and Handle() does not return
			//   while execute() is running
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

			// Notify the main thread that we're ready to start the watch.
			close(shouldStartWatchCh)

			// Wait until the request is finished from the APF point of view
			// (which is when its initialization is done).
			watchInitializationSignal.Wait()
		}

		// Ensure that an item can be put to resultCh asynchronously.
		// 缓冲 1 保证负责 APF 的 goroutine 总能投递结果而不泄漏。
		resultCh := make(chan interface{}, 1)

		// Call Handle in a separate goroutine.
		// The reason for it is that from APF point of view, the request processing
		// finishes as soon as watch is initialized (which is generally orders of
		// magnitude faster then the watch request itself). This means that Handle()
		// call finishes much faster and for performance reasons we want to reduce
		// the number of running goroutines - so we run the shorter thing in a
		// dedicated goroutine and the actual watch handler in the main one.
		go func() {
			defer func() {
				err := recover()
				// do not wrap the sentinel ErrAbortHandler panic value
				if err != nil && err != http.ErrAbortHandler {
					// Same as stdlib http server code. Manually allocate stack
					// trace buffer size to prevent excessively large logs
					const size = 64 << 10
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					err = fmt.Sprintf("%v\n%s", err, buf)
				}

				// Ensure that the result is put into resultCh independently of the panic.
				resultCh <- err
			}()

			// We create handleCtx with an adjusted deadline, for two reasons.
			// One is to limit the time the request waits before its execution starts.
			// The other reason for it is that Handle() underneath may start additional goroutine
			// that is blocked on context cancellation. However, from APF point of view,
			// we don't want to wait until the whole watch request is processed (which is
			// when it context is actually cancelled) - we want to unblock the goroutine as
			// soon as the request is processed from the APF point of view.
			//
			// Note that we explicitly do NOT call the actuall handler using that context
			// to avoid cancelling request too early.
			handleCtx, handleCtxCancel := h.newReqWaitCtxFn(ctx)
			defer handleCtxCancel()

			// Note that Handle will return irrespective of whether the request
			// executes or is rejected. In the latter case, the function will return
			// without calling the passed `execute` function.
			h.fcIfc.Handle(handleCtx, digest, noteFn, estimateWork, queueNote, execute)
		}()

		select {
		case <-shouldStartWatchCh:
			func() {
				// TODO: if both goroutines panic, propagate the stack traces from both
				// goroutines so they are logged properly:
				defer func() {
					// Protect from the situation when request will not reach storage layer
					// and the initialization signal will not be send.
					// It has to happen before waiting on the resultCh below.
					watchInitializationSignal.Signal()
					// TODO: Consider finishing the request as soon as Handle call panics.
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

		// TODO(wojtek-t): Idea from deads2k: we can consider some jittering and in case of non-int
		//  number, just return the truncated result and sleep the remainder server-side.
		tooManyRequests(r, w, strconv.Itoa(int(h.droppedRequests.GetRetryAfter(classification.PriorityLevelName))))
	}
}

// WithPriorityAndFairness limits the number of in-flight
// requests in a fine-grained way.
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
		// Fetching these gauges is delayed until after their underlying metric has been registered
		// so that this latches onto the efficient implementation.
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

// StartPriorityAndFairnessWatermarkMaintenance starts the goroutines to observe and maintain watermarks for
// priority-and-fairness requests.
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

	// We intentionally set the UID of the flow-schema and priority-level instead of name. This is so that
	// the names that cluster-admins choose for categorization and priority levels are not exposed, also
	// the names might make it obvious to the users that they are rejected due to classification with low priority.
	w.Header().Set(flowcontrol.ResponseHeaderMatchedPriorityLevelConfigurationUID, string(classification.PriorityLevelUID))
	w.Header().Set(flowcontrol.ResponseHeaderMatchedFlowSchemaUID, string(classification.FlowSchemaUID))
}

// tooManyRequests 返回 429 与 Retry-After 头。
func tooManyRequests(req *http.Request, w http.ResponseWriter, retryAfter string) {
	// Return a 429 status indicating "Too Many Requests"
	w.Header().Set("Retry-After", retryAfter)
	http.Error(w, "Too many requests, please try again later.", http.StatusTooManyRequests)
}

// getRequestWaitContext returns a new context with a deadline of how
// long the request is allowed to wait before it is removed from its
// queue and rejected.
// The context.CancelFunc returned must never be nil and the caller is
// responsible for calling the CancelFunc function for cleanup.
//   - ctx: the context associated with the request (it may or may
//     not have a deadline).
//   - defaultRequestWaitLimit: the default wait duration that is used
//     if the request context does not have any deadline.
//     (a) initialization of a watch or
//     (b) a request whose context has no deadline
//
// clock comes in handy for testing the function
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

	// a) we will allow the request to wait in the queue for one
	// fourth of the time of its allotted deadline.
	// b) if the request context does not have any deadline
	// then we default to 'defaultRequestWaitLimit'
	// in any case, the wait limit for any request must not
	// exceed the hard limit of 1m
	//
	// request has deadline:
	//   wait-limit = min(remaining deadline / 4, 1m)
	// request has no deadline:
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
