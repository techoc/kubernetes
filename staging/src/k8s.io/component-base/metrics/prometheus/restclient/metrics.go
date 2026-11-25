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

// Package restclient 把 client-go 的 REST 客户端监控钩子接到 Prometheus。
//
// 导入本包即安装适配器；真正的指标注册被推迟到第一次构造 REST client 时
// （metrics.EnsureRegistered），避免在特性门控生效前读取全局状态。
package restclient

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"time"

	"k8s.io/client-go/tools/metrics"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	// requestLatency 是按 "verb" 和 "host" 标签划分的 REST 客户端请求延迟直方图。
	requestLatency = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Name:           "rest_client_request_duration_seconds",
			Help:           "Request latency in seconds. Broken down by verb, and host.",
			StabilityLevel: k8smetrics.ALPHA,
			Buckets:        []float64{0.005, 0.025, 0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 60.0},
		},
		[]string{"verb", "host"},
	)

	// resolverLatency 是按 "host" 标签划分的 REST 客户端 DNS 解析延迟直方图。
	resolverLatency = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Name:           "rest_client_dns_resolution_duration_seconds",
			Help:           "DNS resolver latency in seconds. Broken down by host.",
			StabilityLevel: k8smetrics.ALPHA,
			Buckets:        []float64{0.005, 0.025, 0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 30.0},
		},
		[]string{"host"},
	)

	// requestSize/responseSize 的桶按 64B 到 16MB 指数分布，
	// 覆盖从空响应到较大 list 的常见范围。
	requestSize = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Name:           "rest_client_request_size_bytes",
			Help:           "Request size in bytes. Broken down by verb and host.",
			StabilityLevel: k8smetrics.ALPHA,
			// 64 字节到 16MB
			Buckets: []float64{64, 256, 512, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216},
		},
		[]string{"verb", "host"},
	)

	responseSize = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Name:           "rest_client_response_size_bytes",
			Help:           "Response size in bytes. Broken down by verb and host.",
			StabilityLevel: k8smetrics.ALPHA,
			// 64 字节到 16MB
			Buckets: []float64{64, 256, 512, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216},
		},
		[]string{"verb", "host"},
	)

	rateLimiterLatency = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Name:           "rest_client_rate_limiter_duration_seconds",
			Help:           "Client side rate limiter latency in seconds. Broken down by verb, and host.",
			StabilityLevel: k8smetrics.ALPHA,
			Buckets:        []float64{0.005, 0.025, 0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 60.0},
		},
		[]string{"verb", "host"},
	)

	requestResult = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Name:           "rest_client_requests_total",
			StabilityLevel: k8smetrics.ALPHA,
			Help:           "Number of HTTP requests, partitioned by status code, method, and host.",
		},
		[]string{"code", "method", "host"},
	)

	requestRetry = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Name:           "rest_client_request_retries_total",
			StabilityLevel: k8smetrics.ALPHA,
			Help:           "Number of request retries, partitioned by status code, verb, and host.",
		},
		[]string{"code", "verb", "host"},
	)

	// execPluginCertTTLAdapter 缓存最近一次看到的最短证书过期时间；
	// GaugeFunc 在抓取时再计算 TTL，因此需要一个可变的共享指针。
	execPluginCertTTLAdapter = &expiryToTTLAdapter{}

	// execPluginCertTTL 以 TTL 形式暴露证书剩余有效期：
	// 无证书时返回 +Inf，已过期时为负值。
	execPluginCertTTL = k8smetrics.NewGaugeFunc(
		&k8smetrics.GaugeOpts{
			Name: "rest_client_exec_plugin_ttl_seconds",
			Help: "Gauge of the shortest TTL (time-to-live) of the client " +
				"certificate(s) managed by the auth exec plugin. The value " +
				"is in seconds until certificate expiry (negative if " +
				"already expired). If auth exec plugins are unused or manage no " +
				"TLS certificates, the value will be +INF.",
			StabilityLevel: k8smetrics.ALPHA,
		},
		func() float64 {
			if execPluginCertTTLAdapter.e == nil {
				return math.Inf(1)
			}
			return execPluginCertTTLAdapter.e.Sub(time.Now()).Seconds()
		},
	)

	execPluginCertRotation = k8smetrics.NewHistogram(
		&k8smetrics.HistogramOpts{
			Name: "rest_client_exec_plugin_certificate_rotation_age",
			Help: "Histogram of the number of seconds the last auth exec " +
				"plugin client certificate lived before being rotated. " +
				"If auth exec plugin client certificates are unused, " +
				"histogram will contain no data.",
			// 这些桶覆盖三类轮转周期：
			//   - 10 至 60 分钟：轮转过于频繁。
			//   - 4 小时至 1 个月：理想的轮转周期。
			//   - 3 个月至 4 年：轮转可能过慢或非常慢。
			StabilityLevel: k8smetrics.ALPHA,
			Buckets: []float64{
				600,       // 10 分钟
				1800,      // 30 分钟
				3600,      // 1 小时
				14400,     // 4 小时
				86400,     // 1 天
				604800,    // 1 周
				2592000,   // 1 个月
				7776000,   // 3 个月
				15552000,  // 6 个月
				31104000,  // 1 年
				124416000, // 4 年
			},
		},
	)

	execPluginCalls = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			StabilityLevel: k8smetrics.ALPHA,
			Name:           "rest_client_exec_plugin_call_total",
			Help: "Number of calls to an exec plugin, partitioned by the type of " +
				"event encountered (no_error, plugin_execution_error, plugin_not_found_error, " +
				"client_internal_error) and an optional exit code. The exit code will " +
				"be set to 0 if and only if the plugin call was successful.",
		},
		[]string{"code", "call_status"},
	)

	execPluginPolicyCalls = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			StabilityLevel: k8smetrics.ALPHA,
			Name:           "rest_client_exec_plugin_policy_call_total",
			Help: "Number of comparisons of an exec plugin to the plugin policy " +
				"and allowlist (if any), partitioned by whether or not the policy " +
				"permits the plugin",
		},
		[]string{"allowed", "denied"},
	)

	transportCacheEntries = k8smetrics.NewGauge(
		&k8smetrics.GaugeOpts{
			Name:           "rest_client_transport_cache_entries",
			StabilityLevel: k8smetrics.ALPHA,
			Help:           "Number of transport entries in the internal cache.",
		},
	)

	transportCacheCalls = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Name:           "rest_client_transport_create_calls_total",
			StabilityLevel: k8smetrics.ALPHA,
			Help: "Number of calls to get a new transport, partitioned by the result of the operation " +
				"hit: obtained from the cache, miss: created and added to the cache, miss-gc: recreated and added back to the cache after being garbage collected, uncacheable: created and not cached",
		},
		[]string{"result"},
	)

	transportCAReloads = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Name:           "rest_client_transport_ca_reload_total",
			StabilityLevel: k8smetrics.ALPHA,
			Help:           "Number of times a CA reload is attempted, partitioned by the result and reason for the reload attempt",
		},
		[]string{"result", "reason"},
	)

	transportCertRotationGCCalls = k8smetrics.NewCounter(
		&k8smetrics.CounterOpts{
			Name:           "rest_client_transport_cert_rotation_gc_calls_total",
			StabilityLevel: k8smetrics.ALPHA,
			Help:           "Number of times a cert rotation goroutine cancel func is called via GC cleanup of the associated transport",
		},
	)

	transportCacheGCCalls = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Name:           "rest_client_transport_cache_gc_calls_total",
			StabilityLevel: k8smetrics.ALPHA,
			Help:           "Number of times a GC cleanup attempts to delete a transport cache entry, partitioned by the result: deleted, skipped",
		},
		[]string{"result"},
	)
)

// init 将适配器指针（RequestLatency、ClientCertExpiry 等）安装到
// client-go/tools/metrics，并提供执行 legacyregistry.MustRegister 的 RegisterFn 回调。
// rest.RESTClientForConfigAndClient 在首次构造 REST client 时调用 metrics.EnsureRegistered，
// 从而触发该回调。
func init() {
	metrics.Register(metrics.RegisterOpts{
		ClientCertExpiry:             execPluginCertTTLAdapter,
		ClientCertRotationAge:        &rotationAdapter{m: execPluginCertRotation},
		RequestLatency:               &latencyAdapter{m: requestLatency},
		ResolverLatency:              &resolverLatencyAdapter{m: resolverLatency},
		RequestSize:                  &sizeAdapter{m: requestSize},
		ResponseSize:                 &sizeAdapter{m: responseSize},
		RateLimiterLatency:           &latencyAdapter{m: rateLimiterLatency},
		RequestResult:                &resultAdapter{requestResult},
		RequestRetry:                 &retryAdapter{requestRetry},
		ExecPluginCalls:              &callsAdapter{m: execPluginCalls},
		ExecPluginPolicyCalls:        &policyAdapter{m: execPluginPolicyCalls},
		TransportCacheEntries:        &transportCacheAdapter{m: transportCacheEntries},
		TransportCreateCalls:         &transportCacheCallsAdapter{m: transportCacheCalls},
		TransportCAReloads:           &transportCAReloadsAdapter{m: transportCAReloads},
		TransportCertRotationGCCalls: &transportCertRotationGCCallsAdapter{m: transportCertRotationGCCalls},
		TransportCacheGCCalls:        &transportCacheGCCallsAdapter{m: transportCacheGCCalls},
		RegisterFn: func() {
			// 这里集中注册，是因为 client-go 的 metrics 包要求延迟到
			// EnsureRegistered 调用时再触碰 legacyregistry。
			legacyregistry.MustRegister(requestLatency)
			legacyregistry.MustRegister(requestSize)
			legacyregistry.MustRegister(responseSize)
			legacyregistry.MustRegister(rateLimiterLatency)
			legacyregistry.MustRegister(requestResult)
			legacyregistry.MustRegister(requestRetry)
			legacyregistry.RawMustRegister(execPluginCertTTL)
			legacyregistry.MustRegister(execPluginCertRotation)
			legacyregistry.MustRegister(execPluginCalls)
			legacyregistry.MustRegister(transportCacheEntries)
			legacyregistry.MustRegister(transportCacheCalls)
			legacyregistry.MustRegister(transportCAReloads)
			legacyregistry.MustRegister(transportCertRotationGCCalls)
			legacyregistry.MustRegister(transportCacheGCCalls)
		},
	})
}

type latencyAdapter struct {
	m *k8smetrics.HistogramVec
}

// Observe 记录一次请求延迟；EnsureRegistered 保证注册已完成后再取子指标。
func (l *latencyAdapter) Observe(ctx context.Context, verb string, u url.URL, latency time.Duration) {
	metrics.EnsureRegistered()
	l.m.WithContext(ctx).WithLabelValues(verb, u.Host).Observe(latency.Seconds())
}

type resolverLatencyAdapter struct {
	m *k8smetrics.HistogramVec
}

// Observe 记录 DNS 解析延迟。
func (l *resolverLatencyAdapter) Observe(ctx context.Context, host string, latency time.Duration) {
	metrics.EnsureRegistered()
	l.m.WithContext(ctx).WithLabelValues(host).Observe(latency.Seconds())
}

type sizeAdapter struct {
	m *k8smetrics.HistogramVec
}

// Observe 记录请求/响应字节数。
func (s *sizeAdapter) Observe(ctx context.Context, verb string, host string, size float64) {
	metrics.EnsureRegistered()
	s.m.WithContext(ctx).WithLabelValues(verb, host).Observe(size)
}

type resultAdapter struct {
	m *k8smetrics.CounterVec
}

// Increment 累加按状态码、方法、host 分组的请求次数。
func (r *resultAdapter) Increment(ctx context.Context, code, method, host string) {
	metrics.EnsureRegistered()
	r.m.WithContext(ctx).WithLabelValues(code, method, host).Inc()
}

type expiryToTTLAdapter struct {
	// 并发安全由调用方保证；这里只保存最近写入的过期时间。
	e *time.Time
}

// Set 更新证书过期时间；实际 TTL 在 GaugeFunc 抓取时计算。
func (e *expiryToTTLAdapter) Set(expiry *time.Time) {
	metrics.EnsureRegistered()
	e.e = expiry
}

type rotationAdapter struct {
	m *k8smetrics.Histogram
}

// Observe 记录证书轮转前的存活时长（秒）。
func (r *rotationAdapter) Observe(d time.Duration) {
	metrics.EnsureRegistered()
	r.m.Observe(d.Seconds())
}

type callsAdapter struct {
	m *k8smetrics.CounterVec
}

// Increment 记录 exec 插件调用，标签为退出码与调用状态。
func (r *callsAdapter) Increment(code int, callStatus string) {
	metrics.EnsureRegistered()
	r.m.WithLabelValues(fmt.Sprintf("%d", code), callStatus).Inc()
}

type policyAdapter struct {
	m *k8smetrics.CounterVec
}

// Increment 记录策略允许/拒绝插件的判断次数。
func (r *policyAdapter) Increment(status string) {
	metrics.EnsureRegistered()
	r.m.WithLabelValues(status).Inc()
}

type retryAdapter struct {
	m *k8smetrics.CounterVec
}

// IncrementRetry 记录一次请求重试。
func (r *retryAdapter) IncrementRetry(ctx context.Context, code, method, host string) {
	metrics.EnsureRegistered()
	r.m.WithContext(ctx).WithLabelValues(code, method, host).Inc()
}

type transportCacheAdapter struct {
	m *k8smetrics.Gauge
}

// Observe 设置 transport 缓存当前条目数。
func (t *transportCacheAdapter) Observe(value int) {
	metrics.EnsureRegistered()
	t.m.Set(float64(value))
}

type transportCacheCallsAdapter struct {
	m *k8smetrics.CounterVec
}

// Increment 记录获取 transport 的结果（hit/miss 等）。
func (t *transportCacheCallsAdapter) Increment(result string) {
	metrics.EnsureRegistered()
	t.m.WithLabelValues(result).Inc()
}

type transportCAReloadsAdapter struct {
	m *k8smetrics.CounterVec
}

// Increment 记录 CA 重载尝试及其结果/原因。
func (t *transportCAReloadsAdapter) Increment(result, reason string) {
	metrics.EnsureRegistered()
	t.m.WithLabelValues(result, reason).Inc()
}

type transportCertRotationGCCallsAdapter struct {
	m *k8smetrics.Counter
}

// Increment 记录证书轮转 goroutine 因 transport 被 GC 而取消的次数。
func (t *transportCertRotationGCCallsAdapter) Increment() {
	metrics.EnsureRegistered()
	t.m.Inc()
}

type transportCacheGCCallsAdapter struct {
	m *k8smetrics.CounterVec
}

// Increment 记录 transport 缓存 GC 清理的尝试结果。
func (t *transportCacheGCCallsAdapter) Increment(result string) {
	metrics.EnsureRegistered()
	t.m.WithLabelValues(result).Inc()
}
