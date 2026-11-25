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

// Package metrics 注册并记录 apiserver 的请求级指标。
//
// 指标标签（verb、scope、subresource 等）经过规范化，以保证基数可控：
// 未知动词归入 "other"，未知响应码走 strconv 回退，dryRun 会去重排序。
package metrics

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	restful "github.com/emicklei/go-restful/v3"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilsets "k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/endpoints/responsewriter"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// resettableCollector 是 prometheus.MetricVec 实现的接口，供 Prometheus 采集指标并重置其值。
// 测试与重新注册时需要清空累计值，故统一保留 Reset 能力。
type resettableCollector interface {
	compbasemetrics.Registerable
	Reset()
}

const (
	APIServerComponent string = "apiserver"
	// OtherRequestMethod 是所有未知动词的聚合标签，用于限制指标基数。
	OtherRequestMethod string = "other"
)

/*
 * 默认情况下，以下所有指标都被定义为 ALPHA 稳定性级别（参见
 * https://github.com/kubernetes/enhancements/blob/master/keps/sig-instrumentation/1209-metrics-stability/kubernetes-control-plane-metrics-stability.md#stability-classes）。
 *
 * 提升指标的稳定性级别是组件负责人的职责，因为这相当于明确承诺
 * 在多个版本中支持该指标，需遵循指标稳定性策略。
 */
var (
	deprecatedRequestGauge = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Subsystem:      APIServerComponent,
			Name:           "requested_deprecated_apis",
			Help:           "Gauge of deprecated APIs that have been requested, broken out by API group, version, resource, subresource, and removed_release.",
			StabilityLevel: compbasemetrics.STABLE,
		},
		[]string{"group", "version", "resource", "subresource", "removed_release"},
	)

	// TODO(a-robinson): 待上游库支持这些指标的处理方式后，为其补充单元测试。
	requestCounter = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Subsystem:      APIServerComponent,
			Name:           "request_total",
			Help:           "Counter of apiserver requests broken out for each verb, dry run value, group, version, resource, scope, component, and HTTP response code.",
			StabilityLevel: compbasemetrics.STABLE,
		},
		[]string{"verb", "dry_run", "group", "version", "resource", "subresource", "scope", "component", "code"},
	)
	longRunningRequestsGauge = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Subsystem:      APIServerComponent,
			Name:           "longrunning_requests",
			Help:           "Gauge of all active long-running apiserver requests broken out by verb, group, version, resource, scope and component. Not all requests are tracked this way.",
			StabilityLevel: compbasemetrics.STABLE,
		},
		[]string{"verb", "group", "version", "resource", "subresource", "scope", "component"},
	)
	requestLatencies = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Subsystem: APIServerComponent,
			Name:      "request_duration_seconds",
			Help:      "Response latency distribution in seconds for each verb, dry run value, group, version, resource, subresource, scope and component.",
			// 该指标用于校验 API 调用延迟的 SLO，并跟踪该方面的性能回退，
			// 因此我们对桶做了较大定制，以同时满足这两种用途。
			Buckets: []float64{0.005, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2, 3,
				4, 5, 6, 8, 10, 15, 20, 30, 45, 60},
			StabilityLevel: compbasemetrics.STABLE,
		},
		[]string{"verb", "dry_run", "group", "version", "resource", "subresource", "scope", "component"},
	)
	requestSliLatencies = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Subsystem: APIServerComponent,
			Name:      "request_sli_duration_seconds",
			Help:      "Response latency distribution (not counting webhook duration and priority & fairness queue wait times) in seconds for each verb, group, version, resource, subresource, scope and component.",
			// 该指标是对 requestLatencies 的补充：它统计不含 webhook 耗时的请求时长，
			// 因为 webhook 耗时主要取决于用户自己的配置。
			Buckets: []float64{0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2, 3,
				4, 5, 6, 8, 10, 15, 20, 30, 45, 60},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"verb", "group", "version", "resource", "subresource", "scope", "component"},
	)
	fieldValidationRequestLatencies = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Name: "field_validation_request_duration_seconds",
			Help: "Response latency distribution in seconds for each field validation value",
			// 该指标是对 requestLatencies 的补充：它按不同字段校验（field validation）
			// 取值统计请求时长。
			Buckets: []float64{0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2, 3,
				4, 5, 6, 8, 10, 15, 20, 30, 45, 60},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"field_validation"},
	)
	responseSizes = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Subsystem: APIServerComponent,
			Name:      "response_sizes",
			Help:      "Response size distribution in bytes for each group, version, verb, resource, subresource, scope and component.",
			// 桶的取值范围从 1000 字节（1KB）到 10^9 字节（1GB）。
			Buckets:        compbasemetrics.ExponentialBuckets(1000, 10.0, 7),
			StabilityLevel: compbasemetrics.STABLE,
		},
		[]string{"verb", "group", "version", "resource", "subresource", "scope", "component"},
	)
	// TLSHandshakeErrors 统计因 'TLS handshake error from' 错误而被丢弃的请求数。
	// TLS 握手失败通常来自证书不匹配或客户端配置错误，可据此定位接入问题。
	TLSHandshakeErrors = compbasemetrics.NewCounter(
		&compbasemetrics.CounterOpts{
			Subsystem:      APIServerComponent,
			Name:           "tls_handshake_errors_total",
			Help:           "Number of requests dropped with 'TLS handshake error from' error",
			StabilityLevel: compbasemetrics.ALPHA,
		},
	)
	// WatchEvents 统计发送给 watch 客户端的事件数，按资源维度拆分。
	WatchEvents = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Subsystem:      APIServerComponent,
			Name:           "watch_events_total",
			Help:           "Number of events sent in watch clients",
			StabilityLevel: compbasemetrics.BETA,
		},
		[]string{"group", "version", "resource"},
	)
	WatchEventsSizes = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Subsystem:      APIServerComponent,
			Name:           "watch_events_sizes",
			Help:           "Watch event size distribution in bytes",
			Buckets:        compbasemetrics.ExponentialBuckets(1024, 2.0, 8), // 1K, 2K, 4K, 8K, ..., 128K.
			StabilityLevel: compbasemetrics.BETA,
		},
		[]string{"group", "version", "resource"},
	)
	// 由于底层指标波动很大，这里是预聚合结果：不实时报告当前用量，
	// 而是报告最近一秒内的最大用量。
	currentInflightRequests = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Subsystem:      APIServerComponent,
			Name:           "current_inflight_requests",
			Help:           "Maximal number of currently used inflight request limit of this apiserver per request kind in last second.",
			StabilityLevel: compbasemetrics.STABLE,
		},
		[]string{"request_kind"},
	)
	currentInqueueRequests = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Subsystem:      APIServerComponent,
			Name:           "current_inqueue_requests",
			Help:           "Maximal number of queued requests in this apiserver per request kind in last second.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"request_kind"},
	)

	requestTerminationsTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Subsystem:      APIServerComponent,
			Name:           "request_terminations_total",
			Help:           "Number of requests which apiserver terminated in self-defense.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"verb", "group", "version", "resource", "subresource", "scope", "component", "code"},
	)

	apiSelfRequestCounter = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Subsystem:      APIServerComponent,
			Name:           "selfrequest_total",
			Help:           "Counter of apiserver self-requests broken out for each verb, API resource and subresource.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"verb", "group", "resource", "subresource"},
	)

	requestFilterDuration = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Subsystem:      APIServerComponent,
			Name:           "request_filter_duration_seconds",
			Help:           "Request filter latency distribution in seconds, for each filter type",
			Buckets:        []float64{0.0001, 0.0003, 0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 5.0, 10.0, 15.0, 30.0},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"filter"},
	)

	// requestAbortsTotal 统计因 http.ErrAbortHandler 而中止的请求数。
	requestAbortsTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Subsystem:      APIServerComponent,
			Name:           "request_aborts_total",
			Help:           "Number of requests which apiserver aborted possibly due to a timeout, for each group, version, verb, resource, subresource and scope",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"verb", "group", "version", "resource", "subresource", "scope"},
	)

	// requestPostTimeoutTotal 跟踪请求被 apiserver 判定超时之后，仍在执行中的 handler 的活动情况。
	// source：记录该指标的 handler 名称，目前有两个：
	//  - timeout-handler：“执行中”的 handler 在超时过滤器判定请求超时之后返回。
	//  - rest-handler：“执行中”的 handler 在 REST 层判定请求超时之后返回。
	// status：handler 是 panic 还是返回错误，可能取值：
	//  - 'panic'：handler 发生了 panic
	//  - 'error'：handler 返回了错误
	//  - 'ok'：handler 返回了结果（既无错误也无 panic）
	//  - 'pending'：handler 仍在后台运行，在等待阈值内没有返回。
	requestPostTimeoutTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Subsystem:      APIServerComponent,
			Name:           "request_post_timeout_total",
			Help:           "Tracks the activity of the request handlers after the associated requests have been timed out by the apiserver",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"source", "status"},
	)

	requestTimestampComparisonDuration = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Subsystem:      APIServerComponent,
			Name:           "request_timestamp_comparison_time",
			Help:           "Time taken for comparison of old vs new objects in UPDATE or PATCH requests",
			Buckets:        []float64{0.0001, 0.0003, 0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 5.0},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		// code_path 表示代码得出结论所走的路径，
		// 即 unequalObjectsFast、unequalObjectsSlow、equalObjectsSlow。
		[]string{"code_path"},
	)

	watchListLatencies = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Subsystem: APIServerComponent,
			Name:      "watch_list_duration_seconds",
			Help:      "Response latency distribution in seconds for watch list requests broken by group, version, resource and scope.",
			// 该指标用于校验 watch list 请求延迟的 SLO，并跟踪该方面的性能回退，
			// 因此我们对桶做了较大定制，以同时满足这两种用途。
			Buckets: []float64{0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2, 3,
				4, 5, 6, 8, 10, 15, 20, 30, 45, 60, 90, 120, 180, 300},
			StabilityLevel: compbasemetrics.BETA,
		},
		[]string{"group", "version", "resource", "scope"},
	)

	metrics = []resettableCollector{
		deprecatedRequestGauge,
		requestCounter,
		longRunningRequestsGauge,
		requestLatencies,
		requestSliLatencies,
		fieldValidationRequestLatencies,
		responseSizes,
		TLSHandshakeErrors,
		WatchEvents,
		WatchEventsSizes,
		currentInflightRequests,
		currentInqueueRequests,
		requestTerminationsTotal,
		apiSelfRequestCounter,
		requestFilterDuration,
		requestAbortsTotal,
		requestPostTimeoutTotal,
		requestTimestampComparisonDuration,
		watchListLatencies,
	}

	// 这些是我们在指标中上报的合法请求方法，其余方法一律归入 'unknown'。
	validRequestMethods = utilsets.NewString(
		MethodApply,
		MethodConnect,
		MethodCreate,
		MethodDelete,
		MethodDeleteCollection,
		MethodGet,
		MethodList,
		MethodPatch,
		MethodPost,
		MethodProxy,
		MethodPut,
		MethodUpdate,
		MethodWatch,
		MethodWatchList)

	// 这些是我们在指标中上报的合法 connect 请求。
	validConnectRequests = utilsets.NewString(
		"log",
		"exec",
		"portforward",
		"attach",
		"proxy")
)

const (
	MethodApply            string = request.MethodApply
	MethodConnect          string = request.MethodConnect
	MethodCreate           string = request.MethodCreate
	MethodDelete           string = request.MethodDelete
	MethodDeleteCollection string = request.MethodDeleteCollection
	MethodGet              string = request.MethodGet
	MethodHead             string = request.MethodHead // TODO: 为什么不在 validRequestMethods 里？
	MethodList             string = request.MethodList
	MethodPatch            string = request.MethodPatch
	MethodPost             string = request.MethodPost
	MethodProxy            string = request.MethodProxy
	MethodPut              string = request.MethodPut
	MethodUpdate           string = request.MethodUpdate
	MethodWatch            string = request.MethodWatch
	MethodWatchList        string = request.MethodWatchList
)

const (
	// ReadOnlyKind 是标识只读请求类别的字符串。
	ReadOnlyKind = "readOnly"
	// MutatingKind 是标识变更类请求类别的字符串。
	MutatingKind = "mutating"

	// WaitingPhase 是请求在队列中等待时的 phase 取值。
	WaitingPhase = "waiting"
	// ExecutingPhase 是请求正在执行时的 phase 取值。
	ExecutingPhase = "executing"
)

const (
	// deprecatedAnnotationKey 是一个审计注解的键：访问已弃用 API 版本的请求
	// 会被设置为 "true"。
	deprecatedAnnotationKey = "k8s.io/deprecated"
	// removedReleaseAnnotationKey 是一个审计注解的键：对于已设定移除版本的弃用 API，
	// 该注解取值为目标移除版本，格式为 "<major>.<minor>"。
	removedReleaseAnnotationKey = "k8s.io/removed-release"
)

const (
	// 记录 apiserver_request_post_timeout_total 指标的来源：
	// “执行中”的请求 handler 在超时过滤器判定请求超时之后返回。
	PostTimeoutSourceTimeoutHandler = "timeout-handler"

	// 记录 apiserver_post_timeout_total 指标的来源：
	// “执行中”的请求 handler 在 REST 层判定请求超时之后返回。
	PostTimeoutSourceRestHandler = "rest-handler"
)

const (
	// 请求已被 apiserver 判定超时之后，执行中的请求 handler 发生了 panic。
	PostTimeoutHandlerPanic = "panic"

	// 请求已被 apiserver 判定超时之后，执行中的请求 handler 向超时后的接收方返回了错误。
	PostTimeoutHandlerError = "error"

	// 请求已被 apiserver 判定超时之后，执行中的请求 handler 向超时后的接收方返回了结果。
	PostTimeoutHandlerOK = "ok"

	// 请求已被 apiserver 判定超时之后，执行中的请求 handler 既未 panic，也尚未向超时后的接收方
	// 返回任何错误或结果。超时后的接收方在等待一定阈值后会放弃，此时若 handler 仍未返回，
	// 就使用该标签。
	PostTimeoutHandlerPending = "pending"
)

var registerMetrics sync.Once

// Register 注册所有指标。使用 sync.Once 保证重复调用（例如多个 server 实例初始化）不会重复注册。
func Register() {
	registerMetrics.Do(func() {
		for _, metric := range metrics {
			legacyregistry.MustRegister(metric)
		}
	})
}

// Reset 重置所有指标。仅供测试使用；生产环境重置会导致累计值丢失。
func Reset() {
	for _, metric := range metrics {
		metric.Reset()
	}
}

// ResetLabelAllowLists 重置所有指标的标签白名单。
// 注意：仅供测试使用。指标稳定后可能启用标签白名单，测试需要还原白名单状态以免污染后续用例。
func ResetLabelAllowLists() {
	for _, metric := range metrics {
		if counterVec, ok := metric.(*compbasemetrics.CounterVec); ok {
			counterVec.ResetLabelAllowLists()
			continue
		}
		if gaugeVec, ok := metric.(*compbasemetrics.GaugeVec); ok {
			gaugeVec.ResetLabelAllowLists()
			continue
		}
		if histogramVec, ok := metric.(*compbasemetrics.HistogramVec); ok {
			histogramVec.ResetLabelAllowLists()
			continue
		}
		if summaryVec, ok := metric.(*compbasemetrics.SummaryVec); ok {
			summaryVec.ResetLabelAllowLists()
			continue
		}
		if timingHistogramVec, ok := metric.(*compbasemetrics.TimingHistogramVec); ok {
			timingHistogramVec.ResetLabelAllowLists()
			continue
		}
	}
}

// UpdateInflightRequestMetrics 上报按变更类与只读类划分的并发指标。
// 执行阶段写入 current_inflight_requests；等待阶段写入 current_inqueue_requests。
func UpdateInflightRequestMetrics(phase string, nonmutating, mutating int) {
	for _, kc := range []struct {
		kind  string
		count int
	}{{ReadOnlyKind, nonmutating}, {MutatingKind, mutating}} {
		if phase == ExecutingPhase {
			currentInflightRequests.WithLabelValues(kc.kind).Set(float64(kc.count))
		} else {
			currentInqueueRequests.WithLabelValues(kc.kind).Set(float64(kc.count))
		}
	}
}

// RecordFilterLatency 记录单个过滤器的耗时（秒），用于定位请求处理链中的瓶颈。
func RecordFilterLatency(ctx context.Context, name string, elapsed time.Duration) {
	requestFilterDuration.WithContext(ctx).WithLabelValues(name).Observe(elapsed.Seconds())
}

// RecordTimestampComparisonLatency 记录 UPDATE/PATCH 中旧对象与新对象比较的耗时，
// codePath 标识实际走的比较路径，便于发现回退到慢路径的情况。
func RecordTimestampComparisonLatency(codePath string, elapsed time.Duration) {
	requestTimestampComparisonDuration.WithLabelValues(codePath).Observe(elapsed.Seconds())
}

// RecordRequestPostTimeout 记录请求超时后执行 handler 的状态（panic/error/ok/pending）。
func RecordRequestPostTimeout(source string, status string) {
	requestPostTimeoutTotal.WithLabelValues(source, status).Inc()
}

// RecordRequestAbort 记录请求被中止的事件，中止可能由超时引起。
func RecordRequestAbort(req *http.Request, requestInfo *request.RequestInfo) {
	if requestInfo == nil {
		// 非标准请求流中没有 RequestInfo，用请求方法与路径兜底，保证指标仍可记录。
		requestInfo = &request.RequestInfo{Verb: req.Method, Path: req.URL.Path}
	}

	scope := CleanScope(requestInfo)
	reportedVerb := cleanVerb(CanonicalVerb(strings.ToUpper(req.Method), scope), "", req, requestInfo)
	resource := requestInfo.Resource
	subresource := requestInfo.Subresource
	group := requestInfo.APIGroup
	version := requestInfo.APIVersion

	requestAbortsTotal.WithContext(req.Context()).WithLabelValues(reportedVerb, group, version, resource, subresource, scope).Inc()
}

// RecordDroppedRequest 记录被以 http.TooManyRequests 拒绝的请求。
// 队列满时调用；以 429 计入常规请求计数器，使限流与普通请求口径一致。
func RecordDroppedRequest(req *http.Request, requestInfo *request.RequestInfo, component string, isMutatingRequest bool) {
	if requestInfo == nil {
		requestInfo = &request.RequestInfo{Verb: req.Method, Path: req.URL.Path}
	}
	scope := CleanScope(requestInfo)
	dryRun := cleanDryRun(req.URL)

	// 我们不使用 <requestInfo> 中的 verb：它可能来自 InstrumentRouteFunc，而后者在
	// installer.go 中注册时用的是一份预定义 verb 列表（与转换成 RequestInfo 的那套不同）。
	// 但仍需对它做调整，例如区分 GET 与 LIST。
	reportedVerb := cleanVerb(CanonicalVerb(strings.ToUpper(req.Method), scope), "", req, requestInfo)

	if requestInfo.IsResourceRequest {
		requestCounter.WithContext(req.Context()).WithLabelValues(reportedVerb, dryRun, requestInfo.APIGroup, requestInfo.APIVersion, requestInfo.Resource, requestInfo.Subresource, scope, component, codeToString(http.StatusTooManyRequests)).Inc()
	} else {
		// 非资源请求没有 group/version/resource，用空标签占位以保持标签维度数量一致。
		requestCounter.WithContext(req.Context()).WithLabelValues(reportedVerb, dryRun, "", "", "", requestInfo.Subresource, scope, component, codeToString(http.StatusTooManyRequests)).Inc()
	}
}

// RecordRequestTermination 记录因资源保护或 apiserver 自我保护机制（如超时、
// maxinflight 限流、proxyHandler 错误）而提前终止的请求。
// 每个请求只应调用 RecordRequestTermination 零次或一次。
func RecordRequestTermination(req *http.Request, requestInfo *request.RequestInfo, component string, code int) {
	if requestInfo == nil {
		requestInfo = &request.RequestInfo{Verb: req.Method, Path: req.URL.Path}
	}
	scope := CleanScope(requestInfo)

	// 我们不使用 <requestInfo> 中的 verb：它可能来自 InstrumentRouteFunc，而后者在
	// installer.go 中注册时用的是一份预定义 verb 列表（与转换成 RequestInfo 的那套不同）。
	// 但仍需对它做调整，例如区分 GET 与 LIST。
	reportedVerb := cleanVerb(CanonicalVerb(strings.ToUpper(req.Method), scope), "", req, requestInfo)

	if requestInfo.IsResourceRequest {
		requestTerminationsTotal.WithContext(req.Context()).WithLabelValues(reportedVerb, requestInfo.APIGroup, requestInfo.APIVersion, requestInfo.Resource, requestInfo.Subresource, scope, component, codeToString(code)).Inc()
	} else {
		requestTerminationsTotal.WithContext(req.Context()).WithLabelValues(reportedVerb, "", "", "", requestInfo.Path, scope, component, codeToString(code)).Inc()
	}
}

// RecordLongRunning 跟踪长时间运行请求的执行情况，提供开启中的长时请求总数的精确计数。
// 若调用方不处于正常的请求流程中，requestInfo 可能为 nil。
func RecordLongRunning(req *http.Request, requestInfo *request.RequestInfo, component string, fn func()) {
	if requestInfo == nil {
		requestInfo = &request.RequestInfo{Verb: req.Method, Path: req.URL.Path}
	}
	var g compbasemetrics.GaugeMetric
	scope := CleanScope(requestInfo)

	// 我们不使用 <requestInfo> 中的 verb：它可能来自 InstrumentRouteFunc，而后者在
	// installer.go 中注册时用的是一份预定义 verb 列表（与转换成 RequestInfo 的那套不同）。
	// 但仍需对它做调整，例如区分 GET 与 LIST。
	reportedVerb := cleanVerb(CanonicalVerb(strings.ToUpper(req.Method), scope), "", req, requestInfo)

	if requestInfo.IsResourceRequest {
		g = longRunningRequestsGauge.WithContext(req.Context()).WithLabelValues(reportedVerb, requestInfo.APIGroup, requestInfo.APIVersion, requestInfo.Resource, requestInfo.Subresource, scope, component)
	} else {
		g = longRunningRequestsGauge.WithContext(req.Context()).WithLabelValues(reportedVerb, "", "", "", requestInfo.Path, scope, component)
	}
	g.Inc()
	defer g.Dec()
	fn()
}

// RecordWatchListLatency 直接记录 watch list 请求的响应延迟。
func RecordWatchListLatency(ctx context.Context, gvr schema.GroupVersionResource, metricsScope string, elapsed time.Duration) {
	watchListLatencies.WithContext(ctx).WithLabelValues(gvr.Group, gvr.Version, gvr.Resource, metricsScope).Observe(elapsed.Seconds())
}

// MonitorRequest 先对客户端和上报 verb 做标准转换，再调用 Monitor 记录请求。
// verb 必须为大写，以与现有监控工具保持向后兼容。
func MonitorRequest(req *http.Request, verb, group, version, resource, subresource, scope, component string, deprecated bool, removedRelease string, httpCode, respSize int, elapsed time.Duration) {
	requestInfo, ok := request.RequestInfoFrom(req.Context())
	if !ok || requestInfo == nil {
		requestInfo = &request.RequestInfo{Verb: req.Method, Path: req.URL.Path}
	}
	// 我们不使用 <requestInfo> 中的 verb：它可能来自 InstrumentRouteFunc，而后者在
	// installer.go 中注册时用的是一份预定义 verb 列表（与转换成 RequestInfo 的那套不同）。
	// 但仍需对它做调整，例如区分 GET 与 LIST。
	reportedVerb := cleanVerb(CanonicalVerb(strings.ToUpper(req.Method), scope), verb, req, requestInfo)

	dryRun := cleanDryRun(req.URL)
	elapsedSeconds := elapsed.Seconds()
	requestCounter.WithContext(req.Context()).WithLabelValues(reportedVerb, dryRun, group, version, resource, subresource, scope, component, codeToString(httpCode)).Inc()
	// MonitorRequest 发生在认证之后，因此可以信任请求中给出的用户名。
	// 认证之后才能区分内部自请求，避免客户端伪造身份刷指标。
	info, ok := request.UserFrom(req.Context())
	if ok && info.GetName() == user.APIServerUser {
		apiSelfRequestCounter.WithContext(req.Context()).WithLabelValues(reportedVerb, group, resource, subresource).Inc()
	}
	if deprecated {
		// 已弃用 API 的调用同时在审计事件中打注解，便于迁移统计。
		deprecatedRequestGauge.WithContext(req.Context()).WithLabelValues(group, version, resource, subresource, removedRelease).Set(1)
		audit.AddAuditAnnotation(req.Context(), deprecatedAnnotationKey, "true")
		if len(removedRelease) > 0 {
			audit.AddAuditAnnotation(req.Context(), removedReleaseAnnotationKey, removedRelease)
		}
	}
	requestLatencies.WithContext(req.Context()).WithLabelValues(reportedVerb, dryRun, group, version, resource, subresource, scope, component).Observe(elapsedSeconds)
	fieldValidation := cleanFieldValidation(req.URL)
	fieldValidationRequestLatencies.WithContext(req.Context()).WithLabelValues(fieldValidation)

	// SLI 延迟剔除 webhook 与 APF 排队时间：这些时间取决于用户配置而非 apiserver 自身性能。
	// dryRun 请求不计入，避免干扰真实负载的服务等级指标。
	if wd, ok := request.LatencyTrackersFrom(req.Context()); ok && dryRun == "" {
		sliLatency := elapsedSeconds - (wd.MutatingWebhookTracker.GetLatency() + wd.ValidatingWebhookTracker.GetLatency() + wd.APFQueueWaitTracker.GetLatency()).Seconds()
		requestSliLatencies.WithContext(req.Context()).WithLabelValues(reportedVerb, group, version, resource, subresource, scope, component).Observe(sliLatency)
	}
	// 我们只关心读请求的响应大小。
	if verb == MethodGet || verb == MethodList {
		responseSizes.WithContext(req.Context()).WithLabelValues(reportedVerb, group, version, resource, subresource, scope, component).Observe(float64(respSize))
	}
}

// InstrumentRouteFunc 的作用类似 Prometheus 的 InstrumentHandlerFunc，
// 但包装的是 go-restful 的 RouteFunction 而非 HandlerFunc，并附加 Kubernetes 端点相关的信息。
func InstrumentRouteFunc(verb, group, version, resource, subresource, scope, component string, deprecated bool, removedRelease string, routeFunc restful.RouteFunction) restful.RouteFunction {
	return restful.RouteFunction(func(req *restful.Request, response *restful.Response) {
		requestReceivedTimestamp, ok := request.ReceivedTimestampFrom(req.Request.Context())
		if !ok {
			requestReceivedTimestamp = time.Now()
		}

		// 包装 ResponseWriter 以捕获状态码与响应体大小。
		delegate := &ResponseWriterDelegator{ResponseWriter: response.ResponseWriter}

		rw := responsewriter.WrapForHTTP1Or2(delegate)
		response.ResponseWriter = rw

		routeFunc(req, response)

		MonitorRequest(req.Request, verb, group, version, resource, subresource, scope, component, deprecated, removedRelease, delegate.Status(), delegate.ContentLength(), time.Since(requestReceivedTimestamp))
	})
}

// InstrumentHandlerFunc 类似 Prometheus 的 InstrumentHandlerFunc，但附加了 Kubernetes 端点相关的信息。
func InstrumentHandlerFunc(verb, group, version, resource, subresource, scope, component string, deprecated bool, removedRelease string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		requestReceivedTimestamp, ok := request.ReceivedTimestampFrom(req.Context())
		if !ok {
			requestReceivedTimestamp = time.Now()
		}

		delegate := &ResponseWriterDelegator{ResponseWriter: w}
		w = responsewriter.WrapForHTTP1Or2(delegate)

		handler(w, req)

		MonitorRequest(req, verb, group, version, resource, subresource, scope, component, deprecated, removedRelease, delegate.Status(), delegate.ContentLength(), time.Since(requestReceivedTimestamp))
	}
}

// NormalizedVerb 返回归一化后的 verb。
func NormalizedVerb(req *http.Request) string {
	verb := req.Method
	requestInfo, ok := request.RequestInfoFrom(req.Context())
	if ok {
		// 若能取到 requestInfo，就能得到 scope，进而按需把 GET 转换成 LIST。
		scope := CleanScope(requestInfo)
		verb = CanonicalVerb(strings.ToUpper(verb), scope)
	}

	// 以便正确标记 APPLY、WATCH 和 CONNECT 请求。
	return CleanVerb(verb, req, requestInfo)
}

// CleanScope 返回请求的作用域。
func CleanScope(requestInfo *request.RequestInfo) string {
	if requestInfo.Name != "" || requestInfo.Verb == "create" {
		return "resource"
	}
	if requestInfo.Namespace != "" {
		return "namespace"
	}
	if requestInfo.IsResourceRequest {
		return "cluster"
	}
	// 这是空作用域。
	return ""
}

// CleanListScope 计算用于指标的请求作用域。
// 通常情况下我们会直接用 CleanScope 来计算，
// 但由于 determineRequestNamespaceAndName 中提到的同样的原因，这里不能这么做。
func CleanListScope(ctx context.Context, opts *metainternalversion.ListOptions) string {
	namespace, name := determineRequestNamespaceAndName(ctx, opts)
	if len(name) > 0 {
		return "resource"
	}
	if len(namespace) > 0 {
		return "namespace"
	}
	if requestInfo, ok := request.RequestInfoFrom(ctx); ok {
		if requestInfo.IsResourceRequest {
			return "cluster"
		}
	}
	return ""
}

// CanonicalVerb 区分 LIST 与 GET（以及 HEAD），它假定 verb 为大写。
func CanonicalVerb(verb string, scope string) string {
	switch verb {
	case MethodGet, MethodHead:
		// 集合范围内（cluster/namespace）的 GET 实为 LIST。
		if scope != "resource" && scope != "" {
			return request.MethodList
		}
		return MethodGet
	default:
		return verb
	}
}

// CleanVerb 返回归一化后的 verb，便于区分 WATCH 与 LIST、APPLY 与 PATCH、
// CONNECT 与其他请求。
func CleanVerb(verb string, request *http.Request, requestInfo *request.RequestInfo) string {
	reportedVerb := verb
	if suggestedVerb := getVerbIfWatch(request); suggestedVerb == MethodWatch {
		reportedVerb = MethodWatch
	}
	// 把历史遗留的 WATCHLIST 归一化为 WATCH，避免用户被指标标签的变化弄得困惑。
	if verb == MethodWatchList {
		reportedVerb = MethodWatch
	}
	// Apply patch 在语义上是独立的操作，单独归类便于区分普通 patch。
	if verb == MethodPatch && request.Header.Get("Content-Type") == string(types.ApplyPatchType) {
		reportedVerb = MethodApply
	}
	if requestInfo != nil && requestInfo.IsResourceRequest && len(requestInfo.Subresource) > 0 && validConnectRequests.Has(requestInfo.Subresource) {
		reportedVerb = MethodConnect
	}
	return reportedVerb
}

// determineRequestNamespaceAndName 计算给定请求的 name 与 namespace。
// 注意：本函数的逻辑是从 cacher.go 复制过来的，
// 此前曾尝试把它下沉到 RequestInfo，但未成功。
// 参见：https://github.com/kubernetes/kubernetes/pull/120520
func determineRequestNamespaceAndName(ctx context.Context, opts *metainternalversion.ListOptions) (namespace, name string) {
	if requestNamespace, ok := request.NamespaceFrom(ctx); ok && len(requestNamespace) > 0 {
		namespace = requestNamespace
	} else if opts != nil && opts.FieldSelector != nil {
		// 路径未带命名空间时，字段选择器可能是唯一来源。
		if selectorNamespace, ok := opts.FieldSelector.RequiresExactMatch("metadata.namespace"); ok {
			namespace = selectorNamespace
		}
	}
	if requestInfo, ok := request.RequestInfoFrom(ctx); ok && requestInfo != nil && len(requestInfo.Name) > 0 {
		name = requestInfo.Name
	} else if opts != nil && opts.FieldSelector != nil {
		if selectorName, ok := opts.FieldSelector.RequiresExactMatch("metadata.name"); ok {
			name = selectorName
		}
	}
	return
}

// cleanVerb 还会确保未知 verb 不会污染指标。
func cleanVerb(verb, suggestedVerb string, request *http.Request, requestInfo *request.RequestInfo) string {
	// CanonicalVerb（作为本函数的输入）无法正确处理已弃用的 watch 路径形式：
	//   GET /api/{version}/watch/{resource}
	// 因此这里根据 installer 传入的 verb 手工修正。
	if suggestedVerb == MethodWatch || suggestedVerb == MethodWatchList {
		return MethodWatch
	}
	reportedVerb := CleanVerb(verb, request, requestInfo)
	if validRequestMethods.Has(reportedVerb) {
		return reportedVerb
	}
	return OtherRequestMethod
}

// getVerbIfWatch 额外确保 GET 或 LIST 在需要时被转换成 WATCH。
func getVerbIfWatch(req *http.Request) string {
	if strings.ToUpper(req.Method) == MethodGet || strings.ToUpper(req.Method) == request.MethodList {
		// 参见 apimachinery/pkg/runtime/conversion.go 中的 Convert_Slice_string_To_bool。
		// 遵循与 API 解码一致的布尔判定：0/false 以外都视为 true。
		if values := req.URL.Query()["watch"]; len(values) > 0 {
			if value := strings.ToLower(values[0]); value != "0" && value != "false" {
				return request.MethodWatch
			}
		}
	}
	return ""
}

// cleanDryRun 把 dryRun 查询参数规范化为空、逗号分隔的去重列表或 "invalid"。
func cleanDryRun(u *url.URL) string {
	// 查询串里没有 dryRun 时避免分配内存。
	if !strings.Contains(u.RawQuery, "dryRun") {
		return ""
	}
	dryRun := u.Query()["dryRun"]
	if errs := validation.ValidateDryRun(nil, dryRun); len(errs) > 0 {
		return "invalid"
	}
	// 由于合法的 dryRun 取值长度可能任意大，
	// 必须先去重并排序后再拼接。
	// TODO: 相对于其功能而言这个分配不小，可以考虑
	//   用一趟排序加去重完成。
	return strings.Join(utilsets.NewString(dryRun...).List(), ",")
}

// cleanFieldValidation 规范化 fieldValidation 参数；非法或缺失统一标记，
// 防止用户传入任意值导致标签基数膨胀。
func cleanFieldValidation(u *url.URL) string {
	// 查询串里没有 dryRun 时避免分配内存。
	if !strings.Contains(u.RawQuery, "fieldValidation") {
		return ""
	}
	fieldValidation := u.Query()["fieldValidation"]
	if len(fieldValidation) != 1 {
		return "invalid"
	}
	if errs := validation.ValidateFieldValidation(nil, fieldValidation[0]); len(errs) > 0 {
		return "invalid"
	}
	return fieldValidation[0]
}

var _ http.ResponseWriter = (*ResponseWriterDelegator)(nil)
var _ responsewriter.UserProvidedDecorator = (*ResponseWriterDelegator)(nil)

// ResponseWriterDelegator 接口包装 http.ResponseWriter，额外记录 content-length、状态码等信息。
type ResponseWriterDelegator struct {
	http.ResponseWriter

	status      int
	written     int64
	wroteHeader bool
}

// Unwrap 让上层可以拿到原始 ResponseWriter（例如 http.ResponseController 需要）。
func (r *ResponseWriterDelegator) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// WriteHeader 记录状态码；重复调用以第一次为准（与 net/http 语义一致）。
func (r *ResponseWriterDelegator) WriteHeader(code int) {
	r.status = code
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(code)
}

// Write 在未显式写状态码时补记 200，并累计已写入字节数。
func (r *ResponseWriterDelegator) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.written += int64(n)
	return n, err
}

// Status 返回已记录的状态码。
func (r *ResponseWriterDelegator) Status() int {
	return r.status
}

// ContentLength 返回响应体累计字节数。
func (r *ResponseWriterDelegator) ContentLength() int {
	return int(r.written)
}

// 比 Itoa 略快的一个小优化。
// 常见状态码直接返回常量字符串，避免热路径上的整数转字符串分配。
func codeToString(s int) string {
	switch s {
	case 100:
		return "100"
	case 101:
		return "101"

	case 200:
		return "200"
	case 201:
		return "201"
	case 202:
		return "202"
	case 203:
		return "203"
	case 204:
		return "204"
	case 205:
		return "205"
	case 206:
		return "206"

	case 300:
		return "300"
	case 301:
		return "301"
	case 302:
		return "302"
	case 304:
		return "304"
	case 305:
		return "305"
	case 307:
		return "307"

	case 400:
		return "400"
	case 401:
		return "401"
	case 402:
		return "402"
	case 403:
		return "403"
	case 404:
		return "404"
	case 405:
		return "405"
	case 406:
		return "406"
	case 407:
		return "407"
	case 408:
		return "408"
	case 409:
		return "409"
	case 410:
		return "410"
	case 411:
		return "411"
	case 412:
		return "412"
	case 413:
		return "413"
	case 414:
		return "414"
	case 415:
		return "415"
	case 416:
		return "416"
	case 417:
		return "417"
	case 418:
		return "418"

	case 500:
		return "500"
	case 501:
		return "501"
	case 502:
		return "502"
	case 503:
		return "503"
	case 504:
		return "504"
	case 505:
		return "505"

	case 428:
		return "428"
	case 429:
		return "429"
	case 431:
		return "431"
	case 511:
		return "511"

	default:
		return strconv.Itoa(s)
	}
}
