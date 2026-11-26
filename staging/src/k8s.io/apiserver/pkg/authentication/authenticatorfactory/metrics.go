/*
Copyright 2021 The Kubernetes Authors.

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

// Package authenticatorfactory 构造 apiserver 的各类认证器：
// 委派认证（webhook）、请求头认证与 loopback 认证。
package authenticatorfactory

import (
	"context"
	"sync"

	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// registerables 是待注册指标的集合，便于一次性遍历注册。
type registerables []compbasemetrics.Registerable

// 保证重复调用 RegisterMetrics 不会因重复注册而 panic。
var registerMetricsOnce sync.Once

// RegisterMetrics 把委托认证指标注册到旧 registry。
// 不用 init()，因为特性门控（如 NativeHistograms）必须在
// 直方图指标创建并注册前完成解析。
//
// 因此注册必须由启动流程在 flag 解析之后显式调用。
func RegisterMetrics() {
	registerMetricsOnce.Do(func() {
		for _, metric := range metrics {
			legacyregistry.MustRegister(metric)
		}
	})
}

var (
	// requestTotal 按响应状态码统计委派认证的 HTTP 请求数。
	requestTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Name:           "apiserver_delegated_authn_request_total",
			Help:           "Number of HTTP requests partitioned by status code.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"code"},
	)

	// requestLatency 的桶从 250ms 起，覆盖委派认证常见的超时区间（默认 5s）。
	requestLatency = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Name:           "apiserver_delegated_authn_request_duration_seconds",
			Help:           "Request latency in seconds. Broken down by status code.",
			Buckets:        []float64{0.25, 0.5, 0.7, 1, 1.5, 3, 5, 10},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"code"},
	)

	metrics = registerables{
		requestTotal,
		requestLatency,
	}
)

// RecordRequestTotal 增加委托认证的请求总数。
func RecordRequestTotal(ctx context.Context, code string) {
	requestTotal.WithContext(ctx).WithLabelValues(code).Inc()
}

// RecordRequestLatency 记录委托认证的请求耗时（秒），按状态码细分。
func RecordRequestLatency(ctx context.Context, code string, latency float64) {
	requestLatency.WithContext(ctx).WithLabelValues(code).Observe(latency)
}
