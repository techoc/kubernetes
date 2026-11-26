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

package metrics

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	epmetrics "k8s.io/apiserver/pkg/endpoints/metrics"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	basemetricstestutil "k8s.io/component-base/metrics/testutil"
)

// 指标命名空间/子系统：所有 APF 指标统一为 apiserver_flowcontrol_*。
const (
	namespace = "apiserver"
	subsystem = "flowcontrol"
)

// 标签名与取值。phase 同时是导出常量，因为 flowcontrol 内部需要复用同一字符串。
const (
	requestKind         = "request_kind"
	priorityLevel       = "priority_level"
	flowSchema          = "flow_schema"
	phase               = "phase"
	LabelNamePhase      = "phase"
	LabelValueWaiting   = "waiting"
	LabelValueExecuting = "executing"
)

var (
	// 队列长度桶：按 0/10/25/50/100/250/500/1000 分档，覆盖从空闲到严重积压。
	queueLengthBuckets = []float64{0, 10, 25, 50, 100, 250, 500, 1000}
	// 请求耗时桶：从 0 到 30s，关注 APF 关心的排队/执行时延量级。
	requestDurationSecondsBuckets = []float64{0, 0.005, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 15, 30}
)

// registerMetrics 保证指标只注册一次；重复注册会 panic。
var registerMetrics sync.Once

// 注册所有指标。
func Register() {
	registerMetrics.Do(func() {
		for _, metric := range metrics {
			legacyregistry.MustRegister(metric)
		}
	})
}

// resettable 抽象出支持 Reset 的指标，用于测试隔离。
type resettable interface {
	Reset()
}

// 把所有可重置的指标清零。
//
// 仅供测试：生产环境不会重置累计计数器。
func Reset() {
	for _, metric := range metrics {
		if rm, ok := metric.(resettable); ok {
			rm.Reset()
		}
	}
}

// 以 Prometheus 文本格式的期望值校验给定指标。
func GatherAndCompare(expected string, metricNames ...string) error {
	return basemetricstestutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected), metricNames...)
}

// Registerables 是 Registerable 组成的切片。
type Registerables []compbasemetrics.Registerable

// 追加更多指标。
//
// 返回新切片而非原地追加，避免多个指标列表共享底层数组互相污染。
func (rs Registerables) Append(more ...compbasemetrics.Registerable) Registerables {
	return append(rs, more...)
}

// APF 指标定义。字段含义直接写在各自 Help 中；这里补充“为什么这样设计”的说明。
var (
	// 被拒绝请求数，按原因细分，用于区分“超时拒绝”与“并发超限拒绝”。
	apiserverRejectedRequestsTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "rejected_requests_total",
			Help:           "Number of requests rejected by API Priority and Fairness subsystem",
			StabilityLevel: compbasemetrics.BETA,
		},
		[]string{priorityLevel, flowSchema, "reason"},
	)
	// 已分发（开始执行）的请求数，用于计算拒绝率的分母。
	apiserverDispatchedRequestsTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "dispatched_requests_total",
			Help:           "Number of requests executed by API Priority and Fairness subsystem",
			StabilityLevel: compbasemetrics.BETA,
		},
		[]string{priorityLevel, flowSchema},
	)
	// 观测各优先级在执行阶段占用的 seat 数。
	PriorityLevelExecutionSeatsGaugeVec = NewTimingRatioHistogramVec(
		&compbasemetrics.TimingHistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "priority_level_seat_utilization",
			Help:      "Observations, at the end of every nanosecond, of utilization of seats for any stage of execution (but only initial stage for WATCHes)",
			// 同时设置 0.99 与 1.0 两个桶，PromQL 的 histogram_quantile 才能反映饱和状态。
			Buckets:        []float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1},
			ConstLabels:    map[string]string{phase: "executing"},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		priorityLevel,
	)
	// 按阶段、优先级统计的并发量指标。
	PriorityLevelConcurrencyGaugeVec = NewTimingRatioHistogramVec(
		&compbasemetrics.TimingHistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "priority_level_request_utilization",
			Help:      "Observations, at the end of every nanosecond, of number of requests (as a fraction of the relevant limit) waiting or in any stage of execution (but only initial stage for WATCHes)",
			// 执行阶段的分母是 seat 数，指标会偏低；
			// 等待阶段的队列总容量通常很宽裕，同样偏低。
			Buckets:        []float64{0, 0.001, 0.003, 0.01, 0.03, 0.1, 0.25, 0.5, 0.75, 1},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		LabelNamePhase, priorityLevel,
	)
	// 按阶段与读/写类型统计请求数占限额比例。
	readWriteConcurrencyGaugeVec = NewTimingRatioHistogramVec(
		&compbasemetrics.TimingHistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "read_vs_write_current_requests",
			Help:      "Observations, at the end of every nanosecond, of the number of requests (as a fraction of the relevant limit) waiting or in regular stage of execution",
			// 与优先级指标同理会偏低；而且 APF 对读写共用同一限额。
			Buckets:        []float64{0, 0.001, 0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		LabelNamePhase, requestKind,
	)
	// 虚拟时间相关的内部指标（R/S），用于理解调度器的进度计量；
	// 普通用户一般不需要关注，排查饥饿或调度异常时才有用。
	apiserverCurrentR = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "current_r",
			Help:           "R(time of last change)",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel},
	)
	apiserverDispatchR = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "dispatch_r",
			Help:           "R(time of last dispatch)",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel},
	)
	apiserverLatestS = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "latest_s",
			Help:           "S(most recently dispatched request)",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel},
	)
	apiserverNextSBounds = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "next_s_bounds",
			Help:           "min and max, over queues, of S(oldest waiting request in queue)",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel, "bound"},
	)
	apiserverNextDiscountedSBounds = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "next_discounted_s_bounds",
			Help:           "min and max, over queues, of S(oldest waiting request in queue) - estimated work in progress",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel, "bound"},
	)
	// 队列中待处理请求数：判断 APF 是否成为瓶颈的首要指标。
	apiserverCurrentInqueueRequests = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "current_inqueue_requests",
			Help:           "Number of requests currently pending in queues of the API Priority and Fairness subsystem",
			StabilityLevel: compbasemetrics.BETA,
		},
		[]string{priorityLevel, flowSchema},
	)
	apiserverCurrentInqueueSeats = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "current_inqueue_seats",
			Help:           "Number of seats currently pending in queues of the API Priority and Fairness subsystem",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel, flowSchema},
	)
	// 请求入队后看到的队列长度；与 current_inqueue_requests 不同，它反映“入队时刻的瞬时值”。
	apiserverRequestQueueLength = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "request_queue_length_after_enqueue",
			Help:           "Length of queue in the API Priority and Fairness subsystem, as seen by each request after it is enqueued",
			Buckets:        queueLengthBuckets,
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel, flowSchema},
	)
	// 正在执行的请求数（WATCH 只计初始阶段），用于观察并发占用。
	apiserverCurrentExecutingRequests = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "current_executing_requests",
			Help:           "Number of requests in initial (for a WATCH) or any (for a non-WATCH) execution stage in the API Priority and Fairness subsystem",
			StabilityLevel: compbasemetrics.BETA,
		},
		[]string{priorityLevel, flowSchema},
	)
	// 正在占用的并发席位；与请求数的差异体现不同请求的“成本”不同。
	apiserverCurrentExecutingSeats = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "current_executing_seats",
			Help:           "Concurrency (number of seats) occupied by the currently executing (initial stage for a WATCH, any stage otherwise) requests in the API Priority and Fairness subsystem",
			StabilityLevel: compbasemetrics.BETA,
		},
		[]string{priorityLevel, flowSchema},
	)
	// 排队等待时长；按是否最终执行（execute 标签）区分，可发现被拒绝请求的长等待。
	apiserverRequestWaitingSeconds = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "request_wait_duration_seconds",
			Help:           "Length of time a request spent waiting in its queue",
			Buckets:        requestDurationSecondsBuckets,
			StabilityLevel: compbasemetrics.BETA,
		},
		[]string{priorityLevel, flowSchema, "execute"},
	)
	// 执行时长；type 标签区分 watch 与 regular。
	apiserverRequestExecutionSeconds = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "request_execution_seconds",
			Help:           "Duration of initial stage (for a WATCH) or any (for a non-WATCH) stage of request execution in the API Priority and Fairness subsystem",
			Buckets:        requestDurationSecondsBuckets,
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel, flowSchema, "type"},
	)
	// watch 数量采样；watch 在 APF 中占用的席位与普通请求不同，需单独观测。
	watchCountSamples = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "watch_count_samples",
			Help:           "count of watchers for mutating requests in API Priority and Fairness",
			Buckets:        []float64{0, 1, 10, 100, 1000, 10000},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel, flowSchema},
	)
	// 进度计回跳次数；回跳意味着虚拟时间基线被重置，属于异常信号。
	apiserverEpochAdvances = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "epoch_advance_total",
			Help:           "Number of times the queueset's progress meter jumped backward",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel, "success"},
	)
	// 估计席位；桶上限 100 覆盖当前单请求最大席位（10）并留出余量。
	apiserverWorkEstimatedSeats = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "work_estimated_seats",
			Help:      "Number of estimated seats (maximum of initial and final seats) associated with requests in API Priority and Fairness",
			// 上界取单个请求可占用的最大 seat 数，目前为 10。
			Buckets:        []float64{1, 2, 4, 8, 16, 32, 64, 100},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel, flowSchema},
	)
	// 因无可用席位而未能分发的次数；持续增长说明优先级配置过于拥挤。
	apiserverDispatchWithNoAccommodation = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "request_dispatch_no_accommodation_total",
			Help:           "Number of times a dispatch attempt resulted in a non accommodation due to lack of available seats",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel, flowSchema},
	)
	// 以下三个 gauge 分别对应优先级配置中的 nominal/min/max 席位，便于与当前值对照。
	apiserverNominalConcurrencyLimits = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "nominal_limit_seats",
			Help:           "Nominal number of execution seats configured for each priority level",
			StabilityLevel: compbasemetrics.BETA,
		},
		[]string{priorityLevel},
	)
	apiserverMinimumConcurrencyLimits = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "lower_limit_seats",
			Help:           "Configured lower bound on number of execution seats available to each priority level",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel},
	)
	apiserverMaximumConcurrencyLimits = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "upper_limit_seats",
			Help:           "Configured upper bound on number of execution seats available to each priority level",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel},
	)
	// ApiserverSeatDemands 记录“可用需求/名义席位”的比值，用于观察各优先级是否长期超载。
	ApiserverSeatDemands = NewTimingRatioHistogramVec(
		&compbasemetrics.TimingHistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "demand_seats",
			Help:      "Observations, at the end of every nanosecond, of (the number of seats each priority level could use) / (nominal number of seats for that level)",
			// 桶边界的选择依据：
			// 0--1 均匀且数量不多；1--2 大致按 sqrt(sqrt(2)) 的幂递增；
			// 2--6 大致按 sqrt(2) 的幂递增。
			// 必须覆盖 1 附近，又不能桶太多。
			Buckets:        []float64{0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4, 1.7, 2, 2.8, 4, 6},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		priorityLevel,
	)
	// 席位需求的统计量（高水位/均值/标准差/平滑值/目标值），供自动调整并发限制的算法使用。
	apiserverSeatDemandHighWatermarks = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "demand_seats_high_watermark",
			Help:           "High watermark, over last adjustment period, of demand_seats",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel},
	)
	apiserverSeatDemandAverages = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "demand_seats_average",
			Help:           "Time-weighted average, over last adjustment period, of demand_seats",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel},
	)
	apiserverSeatDemandStandardDeviations = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "demand_seats_stdev",
			Help:           "Time-weighted standard deviation, over last adjustment period, of demand_seats",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel},
	)
	apiserverSeatDemandSmootheds = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "demand_seats_smoothed",
			Help:           "Smoothed seat demands",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel},
	)
	apiserverSeatDemandTargets = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "target_seats",
			Help:           "Seat allocation targets",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel},
	)
	// 每个优先级可获得的公平并发份额；无标签，反映全局分配结果。
	apiserverFairFracs = compbasemetrics.NewGauge(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "seat_fair_frac",
			Help:           "Fair fraction of server's concurrency to allocate to each priority level that can use it",
			StabilityLevel: compbasemetrics.ALPHA,
		})
	// 当前推导出的并发限制；与 nominal 的差异体现自动调整的即时效果。
	apiserverCurrentConcurrencyLimits = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "current_limit_seats",
			Help:           "current derived number of execution seats available to each priority level",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{priorityLevel},
	)

	// metrics 是注册与重置的统一清单；复合直方图通过 metrics() 展开其内部指标。
	metrics = Registerables{
		apiserverRejectedRequestsTotal,
		apiserverDispatchedRequestsTotal,
		apiserverCurrentR,
		apiserverDispatchR,
		apiserverLatestS,
		apiserverNextSBounds,
		apiserverNextDiscountedSBounds,
		apiserverCurrentInqueueRequests,
		apiserverCurrentInqueueSeats,
		apiserverRequestQueueLength,
		apiserverCurrentExecutingSeats,
		apiserverCurrentExecutingRequests,
		apiserverRequestWaitingSeconds,
		apiserverRequestExecutionSeconds,
		watchCountSamples,
		apiserverEpochAdvances,
		apiserverWorkEstimatedSeats,
		apiserverDispatchWithNoAccommodation,
		apiserverNominalConcurrencyLimits,
		apiserverMinimumConcurrencyLimits,
		apiserverMaximumConcurrencyLimits,
		apiserverSeatDemandHighWatermarks,
		apiserverSeatDemandAverages,
		apiserverSeatDemandStandardDeviations,
		apiserverSeatDemandSmootheds,
		apiserverSeatDemandTargets,
		apiserverFairFracs,
		apiserverCurrentConcurrencyLimits,
	}.
		Append(PriorityLevelExecutionSeatsGaugeVec.metrics()...).
		Append(PriorityLevelConcurrencyGaugeVec.metrics()...).
		Append(readWriteConcurrencyGaugeVec.metrics()...).
		Append(ApiserverSeatDemands.metrics()...)
)

// indexOnce 惰性创建并缓存某个标签组合的 RatioedGauge。
// 用 sync.Once 是因为创建过程涉及向直方图注册标签值，重复创建会浪费内存。
type indexOnce struct {
	labelValues []string
	once        sync.Once
	gauge       RatioedGauge
}

// getGauge 返回缓存的 gauge，首次调用时以 (0,1) 的初始比率创建。
func (io *indexOnce) getGauge() RatioedGauge {
	io.once.Do(func() {
		io.gauge = readWriteConcurrencyGaugeVec.NewForLabelValuesSafe(0, 1, io.labelValues)
	})
	return io.gauge
}

// 读写 × 等待/执行 四种组合的惰性 gauge，暴露为包级函数变量以便调用点零分配获取。
var waitingReadonly = indexOnce{labelValues: []string{LabelValueWaiting, epmetrics.ReadOnlyKind}}
var executingReadonly = indexOnce{labelValues: []string{LabelValueExecuting, epmetrics.ReadOnlyKind}}
var waitingMutating = indexOnce{labelValues: []string{LabelValueWaiting, epmetrics.MutatingKind}}
var executingMutating = indexOnce{labelValues: []string{LabelValueExecuting, epmetrics.MutatingKind}}

// 返回等待中的只读请求数占其限额的比例指标。
var GetWaitingReadonlyConcurrency = waitingReadonly.getGauge

// 返回执行中的只读请求数占其限额的比例指标。
var GetExecutingReadonlyConcurrency = executingReadonly.getGauge

// 返回等待中的写请求数占其限额的比例指标。
var GetWaitingMutatingConcurrency = waitingMutating.getGauge

// 返回执行中的写请求数占其限额的比例指标。
var GetExecutingMutatingConcurrency = executingMutating.getGauge

// 增减指定 FlowSchema 与优先级下队列中的请求数。
func AddRequestsInQueues(ctx context.Context, priorityLevel, flowSchema string, delta int) {
	apiserverCurrentInqueueRequests.WithLabelValues(priorityLevel, flowSchema).Add(float64(delta))
}

// 增减指定 FlowSchema 与优先级下队列中的 seat 数。
func AddSeatsInQueues(ctx context.Context, priorityLevel, flowSchema string, delta int) {
	apiserverCurrentInqueueSeats.WithLabelValues(priorityLevel, flowSchema).Add(float64(delta))
}

// 增减指定 FlowSchema 与优先级下正在执行的请求数。
func AddRequestsExecuting(ctx context.Context, priorityLevel, flowSchema string, delta int) {
	apiserverCurrentExecutingRequests.WithLabelValues(priorityLevel, flowSchema).Add(float64(delta))
}

// 设置给定优先级的 current-R（虚拟时间）。
func SetCurrentR(priorityLevel string, r float64) {
	apiserverCurrentR.WithLabelValues(priorityLevel).Set(r)
}

// 设置给定优先级的 latest-S（最近派发请求的虚拟时间）。
//
// 同时更新当前 R、最新 S 以及“下一批可分发请求”的 S 边界（含折扣版本），
// 这些量共同描述队列调度进度，必须一次性原子地写入同一次调整结果。
func SetDispatchMetrics(priorityLevel string, r, s, sMin, sMax, discountedSMin, discountedSMax float64) {
	apiserverDispatchR.WithLabelValues(priorityLevel).Set(r)
	apiserverLatestS.WithLabelValues(priorityLevel).Set(s)
	apiserverNextSBounds.WithLabelValues(priorityLevel, "min").Set(sMin)
	apiserverNextSBounds.WithLabelValues(priorityLevel, "max").Set(sMax)
	apiserverNextDiscountedSBounds.WithLabelValues(priorityLevel, "min").Set(discountedSMin)
	apiserverNextDiscountedSBounds.WithLabelValues(priorityLevel, "max").Set(discountedSMax)
}

// 增减指定 FlowSchema 与优先级下执行中请求占用的 seat 数。
func AddSeatConcurrencyInUse(priorityLevel, flowSchema string, delta int) {
	apiserverCurrentExecutingSeats.WithLabelValues(priorityLevel, flowSchema).Add(float64(delta))
}

// 流控拒绝请求数加一。
func AddReject(ctx context.Context, priorityLevel, flowSchema, reason string) {
	apiserverRejectedRequestsTotal.WithContext(ctx).WithLabelValues(priorityLevel, flowSchema, reason).Add(1)
}

// 流控派发请求数加一。
func AddDispatch(ctx context.Context, priorityLevel, flowSchema string) {
	apiserverDispatchedRequestsTotal.WithContext(ctx).WithLabelValues(priorityLevel, flowSchema).Add(1)
}

// 记录流控队列长度采样。
func ObserveQueueLength(ctx context.Context, priorityLevel, flowSchema string, length int) {
	apiserverRequestQueueLength.WithContext(ctx).WithLabelValues(priorityLevel, flowSchema).Observe(float64(length))
}

// 记录流控等待时长采样。
func ObserveWaitingDuration(ctx context.Context, priorityLevel, flowSchema, execute string, waitTime time.Duration) {
	apiserverRequestWaitingSeconds.WithContext(ctx).WithLabelValues(priorityLevel, flowSchema, execute).Observe(waitTime.Seconds())
}

// 记录流控执行时长采样。
//
// watch 请求只统计初始阶段，因此单独打上 type=watch 标签，避免与普通请求的耗时混在一起。
func ObserveExecutionDuration(ctx context.Context, priorityLevel, flowSchema string, executionTime time.Duration) {
	reqType := "regular"
	if requestInfo, ok := apirequest.RequestInfoFrom(ctx); ok && requestInfo.Verb == "watch" {
		reqType = requestInfo.Verb
	}
	apiserverRequestExecutionSeconds.WithContext(ctx).WithLabelValues(priorityLevel, flowSchema, reqType).Observe(executionTime.Seconds())
}

// 记录 watch 数量采样。
func ObserveWatchCount(ctx context.Context, priorityLevel, flowSchema string, count int) {
	watchCountSamples.WithLabelValues(priorityLevel, flowSchema).Observe(float64(count))
}

// 记录给定优先级下进度计量基线的推进。
func AddEpochAdvance(ctx context.Context, priorityLevel string, success bool) {
	apiserverEpochAdvances.WithContext(ctx).WithLabelValues(priorityLevel, strconv.FormatBool(success)).Inc()
}

// 记录请求预估 seat 数的采样。
func ObserveWorkEstimatedSeats(priorityLevel, flowSchema string, seats int) {
	apiserverWorkEstimatedSeats.WithLabelValues(priorityLevel, flowSchema).Observe(float64(seats))
}

// 记录因可用 seat 不足而派发失败的次数。
func AddDispatchWithNoAccommodation(priorityLevel, flowSchema string) {
	apiserverDispatchWithNoAccommodation.WithLabelValues(priorityLevel, flowSchema).Inc()
}

// SetPriorityLevelConfiguration 记录某优先级配置的三个席位边界。
func SetPriorityLevelConfiguration(priorityLevel string, nominalCL, minCL, maxCL int) {
	apiserverNominalConcurrencyLimits.WithLabelValues(priorityLevel).Set(float64(nominalCL))
	apiserverMinimumConcurrencyLimits.WithLabelValues(priorityLevel).Set(float64(minCL))
	apiserverMaximumConcurrencyLimits.WithLabelValues(priorityLevel).Set(float64(maxCL))
}

// NotePriorityLevelConcurrencyAdjustment 记录一次自动调整的输入与结果，便于回溯限制变化的原因。
func NotePriorityLevelConcurrencyAdjustment(priorityLevel string, seatDemandHWM, seatDemandAvg, seatDemandStdev, seatDemandSmoothed, seatDemandTarget float64, currentCL int) {
	apiserverSeatDemandHighWatermarks.WithLabelValues(priorityLevel).Set(seatDemandHWM)
	apiserverSeatDemandAverages.WithLabelValues(priorityLevel).Set(seatDemandAvg)
	apiserverSeatDemandStandardDeviations.WithLabelValues(priorityLevel).Set(seatDemandStdev)
	apiserverSeatDemandSmootheds.WithLabelValues(priorityLevel).Set(seatDemandSmoothed)
	apiserverSeatDemandTargets.WithLabelValues(priorityLevel).Set(seatDemandTarget)
	apiserverCurrentConcurrencyLimits.WithLabelValues(priorityLevel).Set(float64(currentCL))
}

// SetFairFrac 记录服务器并发中分配给各优先级的公平份额。
func SetFairFrac(fairFrac float64) {
	apiserverFairFracs.Set(fairFrac)
}
