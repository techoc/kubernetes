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

// Package metrics 定义 webhook 授权器的指标接口与实现，覆盖请求计数/延迟、
// 单次 webhook 调用结果以及 fail open 次数。
package metrics

import (
	"context"
	"sync"

	"k8s.io/apiserver/pkg/authorization/cel"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// AuthorizerMetrics specifies a set of methods that are used to register various metrics for the webhook authorizer
type AuthorizerMetrics interface {
	// Request total and latency metrics
	RequestMetrics
	// Webhook count, latency, and fail open metrics
	WebhookMetrics
	// match condition metrics
	cel.MatcherMetrics
}

// NoopAuthorizerMetrics 是关闭指标时的空实现，避免调用点到处做 nil 判断。
type NoopAuthorizerMetrics struct {
	NoopRequestMetrics
	NoopWebhookMetrics
	cel.NoopMatcherMetrics
}

// RequestMetrics 记录整个 webhook 授权请求的计数与延迟。
type RequestMetrics interface {
	// RecordRequestTotal increments the total number of requests for the webhook authorizer
	RecordRequestTotal(ctx context.Context, code string)

	// RecordRequestLatency measures request latency in seconds for webhooks. Broken down by status code.
	RecordRequestLatency(ctx context.Context, code string, latency float64)
}

// NoopRequestMetrics 是 RequestMetrics 的空实现。
type NoopRequestMetrics struct{}

// RecordRequestTotal 不做任何记录。
func (NoopRequestMetrics) RecordRequestTotal(context.Context, string) {}

// RecordRequestLatency 不做任何记录。
func (NoopRequestMetrics) RecordRequestLatency(context.Context, string, float64) {}

// WebhookMetrics 按单个 webhook 维度记录调用结果。
type WebhookMetrics interface {
	// RecordWebhookEvaluation increments with each round-trip of a webhook authorizer.
	// result is one of:
	// - canceled: the call invoking the webhook request was canceled
	// - timeout: the webhook request timed out
	// - error: the webhook response completed and was invalid
	// - success: the webhook response completed and was well-formed
	RecordWebhookEvaluation(ctx context.Context, name, result string)
	// RecordWebhookDuration records latency for each round-trip of a webhook authorizer.
	// result is one of:
	// - canceled: the call invoking the webhook request was canceled
	// - timeout: the webhook request timed out
	// - error: the webhook response completed and was invalid
	// - success: the webhook response completed and was well-formed
	RecordWebhookDuration(ctx context.Context, name, result string, duration float64)
	// RecordWebhookFailOpen increments when a webhook timeout or error results in a fail open
	// of a request which has not been canceled.
	// result is one of:
	// - timeout: the webhook request timed out
	// - error: the webhook response completed and was invalid
	RecordWebhookFailOpen(ctx context.Context, name, result string)
}

// NoopWebhookMetrics 是 WebhookMetrics 的空实现。
type NoopWebhookMetrics struct{}

// RecordWebhookEvaluation 不做任何记录。
func (NoopWebhookMetrics) RecordWebhookEvaluation(ctx context.Context, name, result string) {}

// RecordWebhookDuration 不做任何记录。
func (NoopWebhookMetrics) RecordWebhookDuration(ctx context.Context, name, result string, duration float64) {
}

// RecordWebhookFailOpen 不做任何记录。
func (NoopWebhookMetrics) RecordWebhookFailOpen(ctx context.Context, name, result string) {}

// 指标只注册一次；重复注册会 panic，用 Once 保证并发初始化安全。
var registerWebhookMetrics sync.Once

// RegisterMetrics registers authorizer metrics.
func RegisterWebhookMetrics() {
	registerWebhookMetrics.Do(func() {
		legacyregistry.MustRegister(webhookEvaluations)
		legacyregistry.MustRegister(webhookDuration)
		legacyregistry.MustRegister(webhookFailOpen)
	})
}

// ResetMetricsForTest 清空指标，仅供测试使用。
func ResetMetricsForTest() {
	webhookEvaluations.Reset()
	webhookDuration.Reset()
	webhookFailOpen.Reset()
}

const (
	namespace = "apiserver"
	subsystem = "authorization"
)

var (
	// result 标签区分 canceled/timeout/error/success，便于区分"客户端放弃"与"后端故障"。
	webhookEvaluations = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "webhook_evaluations_total",
			Help:           "Round-trips to authorization webhooks.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"name", "result"},
	)

	webhookDuration = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "webhook_duration_seconds",
			Help:           "Request latency in seconds.",
			Buckets:        compbasemetrics.DefBuckets,
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"name", "result"},
	)

	// fail open 计数是安全相关的关键信号：它意味着授权被跳过，请求可能被放行。
	webhookFailOpen = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "webhook_evaluations_fail_open_total",
			Help:           "NoOpinion results due to webhook timeout or error.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"name", "result"},
	)
)

type webhookMetrics struct{}

// NewWebhookMetrics 注册并返回真实指标实现。
func NewWebhookMetrics() WebhookMetrics {
	RegisterWebhookMetrics()
	return webhookMetrics{}
}

// ResetWebhookMetricsForTest 清空指标，仅供测试使用。
func ResetWebhookMetricsForTest() {
	webhookEvaluations.Reset()
	webhookDuration.Reset()
	webhookFailOpen.Reset()
}

// RecordWebhookEvaluation 增加一次 webhook 调用计数。
func (webhookMetrics) RecordWebhookEvaluation(ctx context.Context, name, result string) {
	webhookEvaluations.WithContext(ctx).WithLabelValues(name, result).Inc()
}

// RecordWebhookDuration 记录一次 webhook 调用耗时。
func (webhookMetrics) RecordWebhookDuration(ctx context.Context, name, result string, duration float64) {
	webhookDuration.WithContext(ctx).WithLabelValues(name, result).Observe(duration)
}

// RecordWebhookFailOpen 记录一次因超时/错误导致的 fail open。
func (webhookMetrics) RecordWebhookFailOpen(ctx context.Context, name, result string) {
	webhookFailOpen.WithContext(ctx).WithLabelValues(name, result).Inc()
}
