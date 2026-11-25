/*
Copyright 2016 The Kubernetes Authors.

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
	"fmt"
	"net/http"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	fcmetrics "k8s.io/apiserver/pkg/util/flowcontrol/metrics"

	"k8s.io/klog/v2"
)

const (
	// 单位秒；429 响应提示客户端至少等待 1s 再重试。
	retryAfter = "1"

	// inflight 使用量指标的更新周期；由于指标统计的是周期内的最大值，周期越长，指标值越大。
	// 周期越长，聚合窗口内的峰值越大，因此保持 1s 以贴近真实并发。
	inflightUsageMetricUpdatePeriod = time.Second
)

var (
	// 只有只读 verb 走 nonMutating 额度；其余（含未知 verb）按变更请求限流，
	// 这是保守选择：无法确认只读时不应占用只读配额。
	nonMutatingRequestVerbs = sets.NewString("get", "list", "watch")
	watchVerbs              = sets.NewString("watch")
)

// handleError 处理请求上下文中缺失 RequestInfo 的编程错误：这是过滤器链
// 装配错误，返回 500 并记录日志，不能继续限流逻辑。
func handleError(w http.ResponseWriter, r *http.Request, err error) {
	errorMsg := fmt.Sprintf("Internal Server Error: %#v", r.RequestURI)
	http.Error(w, errorMsg, http.StatusInternalServerError)
	klog.Error(err.Error())
}

// requestWatermark 记录观测窗口内的并发峰值，并周期性重置；峰值而非瞬时值
// 才能反映限流上限是否逼近。
type requestWatermark struct {
	phase                                string
	readOnlyObserver, mutatingObserver   fcmetrics.RatioedGauge
	lock                                 sync.Mutex
	readOnlyWatermark, mutatingWatermark int
}

// recordMutating 更新实时观测值并抬高窗口峰值。先 Set 指标再拿锁，避免
// 指标更新阻塞峰值统计。
func (w *requestWatermark) recordMutating(mutatingVal int) {
	w.mutatingObserver.Set(float64(mutatingVal))

	w.lock.Lock()
	defer w.lock.Unlock()

	if w.mutatingWatermark < mutatingVal {
		w.mutatingWatermark = mutatingVal
	}
}

// recordReadOnly 与 recordMutating 对称。
func (w *requestWatermark) recordReadOnly(readOnlyVal int) {
	w.readOnlyObserver.Set(float64(readOnlyVal))

	w.lock.Lock()
	defer w.lock.Unlock()

	if w.readOnlyWatermark < readOnlyVal {
		w.readOnlyWatermark = readOnlyVal
	}
}

// watermark tracks requests being executed (not waiting in a queue)
var watermark = &requestWatermark{
	phase: metrics.ExecutingPhase,
}

// startWatermarkMaintenance 启动用于观测和维护指定水位（watermark）的 goroutine。
// 每个周期读取并清零峰值，交给指标层累计；这样不同周期的峰值不会叠加。
func startWatermarkMaintenance(watermark *requestWatermark, stopCh <-chan struct{}) {
	// Periodically update the inflight usage metric.
	go wait.Until(func() {
		watermark.lock.Lock()
		readOnlyWatermark := watermark.readOnlyWatermark
		mutatingWatermark := watermark.mutatingWatermark
		watermark.readOnlyWatermark = 0
		watermark.mutatingWatermark = 0
		watermark.lock.Unlock()

		metrics.UpdateInflightRequestMetrics(watermark.phase, readOnlyWatermark, mutatingWatermark)
	}, inflightUsageMetricUpdatePeriod, stopCh)
}

// initMaxInFlightOnce 保证限流器只能初始化一次：指标分母与全局 watermark
// 都是进程级状态，重复初始化会覆盖已有配置。
var initMaxInFlightOnce sync.Once

// initMaxInFlight 延迟获取指标对象并设置分母（限流上限）。延迟到此处是为了
// 等指标注册完成，拿到高效实现；0 表示不设上限，保持分母不变。
func initMaxInFlight(nonMutatingLimit, mutatingLimit int) {
	initMaxInFlightOnce.Do(func() {
		// Fetching these gauges is delayed until after their underlying metric has been registered
		// so that this latches onto the efficient implementation.
		watermark.readOnlyObserver = fcmetrics.GetExecutingReadonlyConcurrency()
		watermark.mutatingObserver = fcmetrics.GetExecutingMutatingConcurrency()
		if nonMutatingLimit != 0 {
			watermark.readOnlyObserver.SetDenominator(float64(nonMutatingLimit))
			klog.V(2).InfoS("Set denominator for readonly requests", "limit", nonMutatingLimit)
		}
		if mutatingLimit != 0 {
			watermark.mutatingObserver.SetDenominator(float64(mutatingLimit))
			klog.V(2).InfoS("Set denominator for mutating requests", "limit", mutatingLimit)
		}
	})
}

// 用带缓冲 channel 当信号量：容量即并发上限。两类请求（只读/变更）各自
// 独立计数，避免慢写请求耗尽只读能力。
func WithMaxInFlightLimit(
	handler http.Handler,
	nonMutatingLimit int,
	mutatingLimit int,
	longRunningRequestCheck apirequest.LongRunningRequestCheck,
) http.Handler {
	// 两边都为 0 表示禁用限流，直接返回原 handler 以省掉包装开销。
	if nonMutatingLimit == 0 && mutatingLimit == 0 {
		return handler
	}
	var nonMutatingChan chan bool
	var mutatingChan chan bool
	if nonMutatingLimit != 0 {
		nonMutatingChan = make(chan bool, nonMutatingLimit)
		klog.V(2).InfoS("Initialized nonMutatingChan", "len", nonMutatingLimit)
	} else {
		klog.V(2).InfoS("Running with nil nonMutatingChan")
	}
	if mutatingLimit != 0 {
		mutatingChan = make(chan bool, mutatingLimit)
		klog.V(2).InfoS("Initialized mutatingChan", "len", mutatingLimit)
	} else {
		klog.V(2).InfoS("Running with nil mutatingChan")
	}
	initMaxInFlight(nonMutatingLimit, mutatingLimit)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if !ok {
			handleError(w, r, fmt.Errorf("no RequestInfo found in context, handler chain must be wrong"))
			return
		}

		// 跳过对长时运行请求的跟踪：watch 等长连接会长期占住并发名额，若计入会使正常请求被饿死。
		if longRunningRequestCheck != nil && longRunningRequestCheck(r, requestInfo) {
			handler.ServeHTTP(w, r)
			return
		}

		var c chan bool
		isMutatingRequest := !nonMutatingRequestVerbs.Has(requestInfo.Verb)
		if isMutatingRequest {
			c = mutatingChan
		} else {
			c = nonMutatingChan
		}

		// 该类请求未配置上限（channel 为 nil）时不限流。
		if c == nil {
			handler.ServeHTTP(w, r)
		} else {

			select {
			case c <- true:
				// 请求处理中与处理完成后都记录并发水位：
				// 进入与退出都采样：指标按时间点抓取，只记进入会低估尾部。
				if isMutatingRequest {
					watermark.recordMutating(len(c))
				} else {
					watermark.recordReadOnly(len(c))
				}
				defer func() {
					<-c
					if isMutatingRequest {
						watermark.recordMutating(len(c))
					} else {
						watermark.recordReadOnly(len(c))
					}
				}()
				handler.ServeHTTP(w, r)

			default:
				// 此时我们即将返回 429，但并非所有调用方都应被限流：system:master 足够强大，应当总能得到响应——即超级管理员或回环连接。
				// system:masters 用于集群恢复（如 kubelet/loopback），即使过载也必须放行，否则会失去自救能力。
				if currUser, ok := apirequest.UserFrom(ctx); ok {
					for _, group := range currUser.GetGroups() {
						if group == user.SystemPrivilegedGroup {
							handler.ServeHTTP(w, r)
							return
						}
					}
				}
				// We need to split this data between buckets used for throttling.
				metrics.RecordDroppedRequest(r, requestInfo, metrics.APIServerComponent, isMutatingRequest)
				metrics.RecordRequestTermination(r, requestInfo, metrics.APIServerComponent, http.StatusTooManyRequests)
				tooManyRequests(r, w, retryAfter)
			}
		}
	})
}

// StartMaxInFlightWatermarkMaintenance 启动 goroutine，观测并维护 max-in-flight 请求的水位。
// 由 apiserver 启动阶段调用一次，stopCh 关闭时停止后台指标刷新。
func StartMaxInFlightWatermarkMaintenance(stopCh <-chan struct{}) {
	startWatermarkMaintenance(watermark, stopCh)
}
