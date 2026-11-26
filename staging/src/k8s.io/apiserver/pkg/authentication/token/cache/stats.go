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

package cache

import (
	"context"
	"sync"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	// requestLatency 记录缓存查找耗时，按 hit/miss 区分：miss 时包含回源验证的延迟，
	// 两条曲线分离才能看出缓存到底省了多少。
	requestLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      "authentication",
			Subsystem:      "token_cache",
			Name:           "request_duration_seconds",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"status"},
	)
	requestCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "authentication",
			Subsystem:      "token_cache",
			Name:           "request_total",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"status"},
	)
	fetchCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "authentication",
			Subsystem:      "token_cache",
			Name:           "fetch_total",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"status"},
	)
	activeFetchCount = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "authentication",
			Subsystem:      "token_cache",
			Name:           "active_fetch_count",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"status"},
	)
)

var registerMetricsOnce sync.Once

// registerMetrics 向 legacy registry 注册 token 缓存指标。
// 不使用 init()，因为 feature gate（如 NativeHistograms）
// 必须在创建并注册 histogram 指标之前完成解析。
func registerMetrics() {
	// sync.Once 保证重复构造 authenticator 时不会重复注册（重复注册会 panic）。
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(
			requestLatency,
			requestCount,
			fetchCount,
			activeFetchCount,
		)
	})
}

const (
	// hitTag/missTag 是缓存查找结果的标签值。
	hitTag  = "hit"
	missTag = "miss"

	// fetchFailedTag/fetchOkTag 是回源验证（token review）结果的标签值。
	fetchFailedTag = "error"
	fetchOkTag     = "ok"

	// fetchInFlightTag 统计正在回源的请求；fetchBlockedTag 统计因 singleflight
	// 合并而等待其他回源结果的请求，二者之差反映实际的回源并发度。
	fetchInFlightTag = "in_flight"
	fetchBlockedTag  = "blocked"
)

// statsCollector 是无状态的方法集合，仅用于给调用点提供可读的指标接口。
type statsCollector struct{}

var stats = statsCollector{}

// authenticating 返回一个回调，在认证结束时按命中与否记录计数与延迟。
// 采用“开始返回闭包”的形式，避免调用方手工记录开始时间。
func (statsCollector) authenticating(ctx context.Context) func(hit bool) {
	start := time.Now()
	return func(hit bool) {
		var tag string
		if hit {
			tag = hitTag
		} else {
			tag = missTag
		}

		latency := time.Since(start)

		requestCount.WithContext(ctx).WithLabelValues(tag).Inc()
		requestLatency.WithContext(ctx).WithLabelValues(tag).Observe(float64(latency.Milliseconds()) / 1000)
	}
}

// blocking 标记一次等待他人回源的阻塞，返回递减 gauge 的清理函数。
func (statsCollector) blocking(ctx context.Context) func() {
	activeFetchCount.WithContext(ctx).WithLabelValues(fetchBlockedTag).Inc()
	return activeFetchCount.WithContext(ctx).WithLabelValues(fetchBlockedTag).Dec
}

// fetching 标记一次真实的回源请求，返回记录结果并递减 gauge 的清理函数。
// gauge 的递减放在返回的闭包里而不是 defer 中，是为了让调用方能精确控制结束点。
func (statsCollector) fetching(ctx context.Context) func(ok bool) {
	activeFetchCount.WithContext(ctx).WithLabelValues(fetchInFlightTag).Inc()
	return func(ok bool) {
		var tag string
		if ok {
			tag = fetchOkTag
		} else {
			tag = fetchFailedTag
		}

		fetchCount.WithContext(ctx).WithLabelValues(tag).Inc()

		activeFetchCount.WithContext(ctx).WithLabelValues(fetchInFlightTag).Dec()
	}
}
