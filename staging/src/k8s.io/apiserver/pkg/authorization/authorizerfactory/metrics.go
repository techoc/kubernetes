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

package authorizerfactory

import (
	"context"
	"sync"

	celmetrics "k8s.io/apiserver/pkg/authorization/cel"
	webhookmetrics "k8s.io/apiserver/plugin/pkg/authorizer/webhook/metrics"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var registerMetrics sync.Once

// RegisterMetrics 注册授权器指标。
func RegisterMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(requestTotal)
		legacyregistry.MustRegister(requestLatency)
	})
}

var (
	requestTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Name:           "apiserver_delegated_authz_request_total",
			Help:           "Number of HTTP requests partitioned by status code.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"code"},
	)

	requestLatency = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Name:           "apiserver_delegated_authz_request_duration_seconds",
			Help:           "Request latency in seconds. Broken down by status code.",
			Buckets:        []float64{0.25, 0.5, 0.7, 1, 1.5, 3, 5, 10},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"code"},
	)
)

var _ = webhookmetrics.AuthorizerMetrics(delegatingAuthorizerMetrics{})

// delegatingAuthorizerMetrics 只为委托授权实现总请求数与延迟指标；
// webhook 自身的指标与 matchConditions 指标都使用 no-op 实现，
// 因为委托场景下这两类信息由被委托的 apiserver 负责。
type delegatingAuthorizerMetrics struct {
	// webhook 指标暂用 no-op：委托授权会上报原始的总请求数与延迟指标。
	webhookmetrics.NoopWebhookMetrics
	// matchCondition 指标暂用 no-op：委托授权不配置 match conditions。
	celmetrics.NoopMatcherMetrics
}

// NewDelegatingAuthorizerMetrics 创建指标记录器，并确保指标已注册。
func NewDelegatingAuthorizerMetrics() delegatingAuthorizerMetrics {
	RegisterMetrics()
	return delegatingAuthorizerMetrics{}
}

// RecordRequestTotal 累加委托授权的总请求数。
func (delegatingAuthorizerMetrics) RecordRequestTotal(ctx context.Context, code string) {
	requestTotal.WithContext(ctx).WithLabelValues(code).Add(1)
}

// RecordRequestLatency 以秒为单位记录委托授权的请求延迟，并按状态码拆分。
func (delegatingAuthorizerMetrics) RecordRequestLatency(ctx context.Context, code string, latency float64) {
	requestLatency.WithContext(ctx).WithLabelValues(code).Observe(latency)
}
