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

// resettableCollector is the interface implemented by prometheus.MetricVec
// that can be used by Prometheus to collect metrics and reset their values.
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
 * By default, all the following metrics are defined as falling under
 * ALPHA stability level https://github.com/kubernetes/enhancements/blob/master/keps/sig-instrumentation/1209-metrics-stability/kubernetes-control-plane-metrics-stability.md#stability-classes)
 *
 * Promoting the stability level of the metric is a responsibility of the component owner, since it
 * involves explicitly acknowledging support for the metric across multiple releases, in accordance with
 * the metric stability policy.
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

	// TODO(a-robinson): Add unit tests for the handling of these metrics once
	// the upstream library supports it.
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
			// This metric is used for verifying api call latencies SLO,
			// as well as tracking regressions in this aspects.
			// Thus we customize buckets significantly, to empower both usecases.
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
			// This metric is supplementary to the requestLatencies metric.
			// It measures request duration excluding webhooks as they are mostly
			// dependant on user configuration.
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
			// This metric is supplementary to the requestLatencies metric.
			// It measures request durations for the various field validation
			// values.
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
			// Use buckets ranging from 1000 bytes (1KB) to 10^9 bytes (1GB).
			Buckets:        compbasemetrics.ExponentialBuckets(1000, 10.0, 7),
			StabilityLevel: compbasemetrics.STABLE,
		},
		[]string{"verb", "group", "version", "resource", "subresource", "scope", "component"},
	)
	// TLSHandshakeErrors is a number of requests dropped with 'TLS handshake error from' error
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
	// Because of volatility of the base metric this is pre-aggregated one. Instead of reporting current usage all the time
	// it reports maximal usage during the last second.
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

	// requestAbortsTotal is a number of aborted requests with http.ErrAbortHandler
	requestAbortsTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Subsystem:      APIServerComponent,
			Name:           "request_aborts_total",
			Help:           "Number of requests which apiserver aborted possibly due to a timeout, for each group, version, verb, resource, subresource and scope",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"verb", "group", "version", "resource", "subresource", "scope"},
	)

	// requestPostTimeoutTotal tracks the activity of the executing request handler after the associated request
	// has been timed out by the apiserver.
	// source: the name of the handler that is recording this metric. Currently, we have two:
	//  - timeout-handler: the "executing" handler returns after the timeout filter times out the request.
	//  - rest-handler: the "executing" handler returns after the rest layer times out the request.
	// status: whether the handler panicked or threw an error, possible values:
	//  - 'panic': the handler panicked
	//  - 'error': the handler return an error
	//  - 'ok': the handler returned a result (no error and no panic)
	//  - 'pending': the handler is still running in the background and it did not return
	//    within the wait threshold.
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
		// Path the code takes to reach a conclusion:
		// i.e. unequalObjectsFast, unequalObjectsSlow, equalObjectsSlow
		[]string{"code_path"},
	)

	watchListLatencies = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Subsystem: APIServerComponent,
			Name:      "watch_list_duration_seconds",
			Help:      "Response latency distribution in seconds for watch list requests broken by group, version, resource and scope.",
			// This metric is used for verifying api call latencies SLO,
			// as well as tracking regressions in this aspects.
			// Thus we customize buckets significantly, to empower both usecases.
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

	// these are the valid request methods which we report in our metrics. Any other request methods
	// will be aggregated under 'unknown'
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

	// These are the valid connect requests which we report in our metrics.
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
	MethodHead             string = request.MethodHead // TODO: why not in validRequestMethods?
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
	// ReadOnlyKind is a string identifying read only request kind
	ReadOnlyKind = "readOnly"
	// MutatingKind is a string identifying mutating request kind
	MutatingKind = "mutating"

	// WaitingPhase is the phase value for a request waiting in a queue
	WaitingPhase = "waiting"
	// ExecutingPhase is the phase value for an executing request
	ExecutingPhase = "executing"
)

const (
	// deprecatedAnnotationKey is a key for an audit annotation set to
	// "true" on requests made to deprecated API versions
	deprecatedAnnotationKey = "k8s.io/deprecated"
	// removedReleaseAnnotationKey is a key for an audit annotation set to
	// the target removal release, in "<major>.<minor>" format,
	// on requests made to deprecated API versions with a target removal release
	removedReleaseAnnotationKey = "k8s.io/removed-release"
)

const (
	// The source that is recording the apiserver_request_post_timeout_total metric.
	// The "executing" request handler returns after the timeout filter times out the request.
	PostTimeoutSourceTimeoutHandler = "timeout-handler"

	// The source that is recording the apiserver_request_post_timeout_total metric.
	// The "executing" request handler returns after the rest layer times out the request.
	PostTimeoutSourceRestHandler = "rest-handler"
)

const (
	// The executing request handler panicked after the request had
	// been timed out by the apiserver.
	PostTimeoutHandlerPanic = "panic"

	// The executing request handler has returned an error to the post-timeout
	// receiver after the request had been timed out by the apiserver.
	PostTimeoutHandlerError = "error"

	// The executing request handler has returned a result to the post-timeout
	// receiver after the request had been timed out by the apiserver.
	PostTimeoutHandlerOK = "ok"

	// The executing request handler has not panicked or returned any error/result to
	// the post-timeout receiver yet after the request had been timed out by the apiserver.
	// The post-timeout receiver gives up after waiting for certain threshold and if the
	// executing request handler has not returned yet we use the following label.
	PostTimeoutHandlerPending = "pending"
)

var registerMetrics sync.Once

// Register all metrics.
// 使用 sync.Once 保证重复调用（例如多个 server 实例初始化）不会重复注册。
func Register() {
	registerMetrics.Do(func() {
		for _, metric := range metrics {
			legacyregistry.MustRegister(metric)
		}
	})
}

// Reset all metrics.
// 仅供测试使用；生产环境重置会导致累计值丢失。
func Reset() {
	for _, metric := range metrics {
		metric.Reset()
	}
}

// ResetLabelAllowLists resets the label allow lists for all metrics.
// NOTE: This is only used for testing.
// 指标稳定后可能启用标签白名单，测试需要还原白名单状态以免污染后续用例。
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

// UpdateInflightRequestMetrics reports concurrency metrics classified by
// mutating vs Readonly.
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

// RecordRequestAbort records that the request was aborted possibly due to a timeout.
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

// RecordDroppedRequest records that the request was rejected via http.TooManyRequests.
// 队列满时调用；以 429 计入常规请求计数器，使限流与普通请求口径一致。
func RecordDroppedRequest(req *http.Request, requestInfo *request.RequestInfo, component string, isMutatingRequest bool) {
	if requestInfo == nil {
		requestInfo = &request.RequestInfo{Verb: req.Method, Path: req.URL.Path}
	}
	scope := CleanScope(requestInfo)
	dryRun := cleanDryRun(req.URL)

	// We don't use verb from <requestInfo>, as this may be propagated from
	// InstrumentRouteFunc which is registered in installer.go with predefined
	// list of verbs (different than those translated to RequestInfo).
	// However, we need to tweak it e.g. to differentiate GET from LIST.
	reportedVerb := cleanVerb(CanonicalVerb(strings.ToUpper(req.Method), scope), "", req, requestInfo)

	if requestInfo.IsResourceRequest {
		requestCounter.WithContext(req.Context()).WithLabelValues(reportedVerb, dryRun, requestInfo.APIGroup, requestInfo.APIVersion, requestInfo.Resource, requestInfo.Subresource, scope, component, codeToString(http.StatusTooManyRequests)).Inc()
	} else {
		// 非资源请求没有 group/version/resource，用空标签占位以保持标签维度数量一致。
		requestCounter.WithContext(req.Context()).WithLabelValues(reportedVerb, dryRun, "", "", "", requestInfo.Subresource, scope, component, codeToString(http.StatusTooManyRequests)).Inc()
	}
}

// RecordRequestTermination records that the request was terminated early as part of a resource
// preservation or apiserver self-defense mechanism (e.g. timeouts, maxinflight throttling,
// proxyHandler errors). RecordRequestTermination should only be called zero or one times
// per request.
func RecordRequestTermination(req *http.Request, requestInfo *request.RequestInfo, component string, code int) {
	if requestInfo == nil {
		requestInfo = &request.RequestInfo{Verb: req.Method, Path: req.URL.Path}
	}
	scope := CleanScope(requestInfo)

	// We don't use verb from <requestInfo>, as this may be propagated from
	// InstrumentRouteFunc which is registered in installer.go with predefined
	// list of verbs (different than those translated to RequestInfo).
	// However, we need to tweak it e.g. to differentiate GET from LIST.
	reportedVerb := cleanVerb(CanonicalVerb(strings.ToUpper(req.Method), scope), "", req, requestInfo)

	if requestInfo.IsResourceRequest {
		requestTerminationsTotal.WithContext(req.Context()).WithLabelValues(reportedVerb, requestInfo.APIGroup, requestInfo.APIVersion, requestInfo.Resource, requestInfo.Subresource, scope, component, codeToString(code)).Inc()
	} else {
		requestTerminationsTotal.WithContext(req.Context()).WithLabelValues(reportedVerb, "", "", "", requestInfo.Path, scope, component, codeToString(code)).Inc()
	}
}

// RecordLongRunning tracks the execution of a long running request against the API server. It provides an accurate count
// of the total number of open long running requests. requestInfo may be nil if the caller is not in the normal request flow.
func RecordLongRunning(req *http.Request, requestInfo *request.RequestInfo, component string, fn func()) {
	if requestInfo == nil {
		requestInfo = &request.RequestInfo{Verb: req.Method, Path: req.URL.Path}
	}
	var g compbasemetrics.GaugeMetric
	scope := CleanScope(requestInfo)

	// We don't use verb from <requestInfo>, as this may be propagated from
	// InstrumentRouteFunc which is registered in installer.go with predefined
	// list of verbs (different than those translated to RequestInfo).
	// However, we need to tweak it e.g. to differentiate GET from LIST.
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

// RecordWatchListLatency simply records response latency for watch list requests.
func RecordWatchListLatency(ctx context.Context, gvr schema.GroupVersionResource, metricsScope string, elapsed time.Duration) {
	watchListLatencies.WithContext(ctx).WithLabelValues(gvr.Group, gvr.Version, gvr.Resource, metricsScope).Observe(elapsed.Seconds())
}

// MonitorRequest handles standard transformations for client and the reported verb and then invokes Monitor to record
// a request. verb must be uppercase to be backwards compatible with existing monitoring tooling.
func MonitorRequest(req *http.Request, verb, group, version, resource, subresource, scope, component string, deprecated bool, removedRelease string, httpCode, respSize int, elapsed time.Duration) {
	requestInfo, ok := request.RequestInfoFrom(req.Context())
	if !ok || requestInfo == nil {
		requestInfo = &request.RequestInfo{Verb: req.Method, Path: req.URL.Path}
	}
	// We don't use verb from <requestInfo>, as this may be propagated from
	// InstrumentRouteFunc which is registered in installer.go with predefined
	// list of verbs (different than those translated to RequestInfo).
	// However, we need to tweak it e.g. to differentiate GET from LIST.
	reportedVerb := cleanVerb(CanonicalVerb(strings.ToUpper(req.Method), scope), verb, req, requestInfo)

	dryRun := cleanDryRun(req.URL)
	elapsedSeconds := elapsed.Seconds()
	requestCounter.WithContext(req.Context()).WithLabelValues(reportedVerb, dryRun, group, version, resource, subresource, scope, component, codeToString(httpCode)).Inc()
	// MonitorRequest happens after authentication, so we can trust the username given by the request
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
	// We are only interested in response sizes of read requests.
	if verb == MethodGet || verb == MethodList {
		responseSizes.WithContext(req.Context()).WithLabelValues(reportedVerb, group, version, resource, subresource, scope, component).Observe(float64(respSize))
	}
}

// InstrumentRouteFunc works like Prometheus' InstrumentHandlerFunc but wraps
// the go-restful RouteFunction instead of a HandlerFunc plus some Kubernetes endpoint specific information.
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

// InstrumentHandlerFunc works like Prometheus' InstrumentHandlerFunc but adds some Kubernetes endpoint specific information.
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

// NormalizedVerb returns normalized verb
func NormalizedVerb(req *http.Request) string {
	verb := req.Method
	requestInfo, ok := request.RequestInfoFrom(req.Context())
	if ok {
		// If we can find a requestInfo, we can get a scope, and then
		// we can convert GETs to LISTs when needed.
		scope := CleanScope(requestInfo)
		verb = CanonicalVerb(strings.ToUpper(verb), scope)
	}

	// mark APPLY requests, WATCH requests and CONNECT requests correctly.
	return CleanVerb(verb, req, requestInfo)
}

// CleanScope returns the scope of the request.
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
	// this is the empty scope
	return ""
}

// CleanListScope computes the request scope for metrics.
//
// Note that normally we would use CleanScope for computation.
// But due to the same reasons mentioned in determineRequestNamespaceAndName we cannot.
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

// CanonicalVerb distinguishes LISTs from GETs (and HEADs). It assumes verb is
// UPPERCASE.
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

// CleanVerb returns a normalized verb, so that it is easy to tell WATCH from
// LIST, APPLY from PATCH and CONNECT from others.
func CleanVerb(verb string, request *http.Request, requestInfo *request.RequestInfo) string {
	reportedVerb := verb
	if suggestedVerb := getVerbIfWatch(request); suggestedVerb == MethodWatch {
		reportedVerb = MethodWatch
	}
	// normalize the legacy WATCHLIST to WATCH to ensure users aren't surprised by metrics
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

// determineRequestNamespaceAndName computes name and namespace for the given requests
//
// note that the logic of this function was copy&pasted from cacher.go
// after an unsuccessful attempt of moving it to RequestInfo
//
// see: https://github.com/kubernetes/kubernetes/pull/120520
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

// cleanVerb additionally ensures that unknown verbs don't clog up the metrics.
func cleanVerb(verb, suggestedVerb string, request *http.Request, requestInfo *request.RequestInfo) string {
	// CanonicalVerb (being an input for this function) doesn't handle correctly the
	// deprecated path pattern for watch of:
	//   GET /api/{version}/watch/{resource}
	// We correct it manually based on the pass verb from the installer.
	if suggestedVerb == MethodWatch || suggestedVerb == MethodWatchList {
		return MethodWatch
	}
	reportedVerb := CleanVerb(verb, request, requestInfo)
	if validRequestMethods.Has(reportedVerb) {
		return reportedVerb
	}
	return OtherRequestMethod
}

// getVerbIfWatch additionally ensures that GET or List would be transformed to WATCH
func getVerbIfWatch(req *http.Request) string {
	if strings.ToUpper(req.Method) == MethodGet || strings.ToUpper(req.Method) == request.MethodList {
		// see apimachinery/pkg/runtime/conversion.go Convert_Slice_string_To_bool
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
	// avoid allocating when we don't see dryRun in the query
	if !strings.Contains(u.RawQuery, "dryRun") {
		return ""
	}
	dryRun := u.Query()["dryRun"]
	if errs := validation.ValidateDryRun(nil, dryRun); len(errs) > 0 {
		return "invalid"
	}
	// Since dryRun could be valid with any arbitrarily long length
	// we have to dedup and sort the elements before joining them together
	// TODO: this is a fairly large allocation for what it does, consider
	//   a sort and dedup in a single pass
	return strings.Join(utilsets.NewString(dryRun...).List(), ",")
}

// cleanFieldValidation 规范化 fieldValidation 参数；非法或缺失统一标记，
// 防止用户传入任意值导致标签基数膨胀。
func cleanFieldValidation(u *url.URL) string {
	// avoid allocating when we don't see dryRun in the query
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

// ResponseWriterDelegator interface wraps http.ResponseWriter to additionally record content-length, status-code, etc.
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

// Small optimization over Itoa
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
