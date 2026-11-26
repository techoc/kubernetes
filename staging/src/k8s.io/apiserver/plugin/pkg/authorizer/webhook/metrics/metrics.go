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

// AuthorizerMetrics 指定一组方法，用于为 webhook 授权器注册各种指标
type AuthorizerMetrics interface {
	// 请求总数与延迟指标
	RequestMetrics
	// webhook 次数、延迟与 fail open 指标
	WebhookMetrics
	// 匹配条件指标
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
	// RecordRequestTotal 增加 webhook 授权器的请求总数。
	RecordRequestTotal(ctx context.Context, code string)

	// RecordRequestLatency 记录 webhook 请求延迟（秒），按状态码细分。
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
	// RecordWebhookEvaluation 在 webhook 授权器每次往返时递增。
	// result 是以下之一：
	// - canceled：调用 webhook 请求的操作被取消
	// - timeout：webhook 请求超时
	// - error：webhook 响应完成但无效
	// - success：webhook 响应完成且格式正确
	RecordWebhookEvaluation(ctx context.Context, name, result string)
	// RecordWebhookDuration 记录 webhook 授权器每次往返的延迟。
	// result 是以下之一：
	// - canceled：调用 webhook 请求的操作被取消
	// - timeout：webhook 请求超时
	// - error：webhook 响应完成但无效
	// - success：webhook 响应完成且格式正确
	RecordWebhookDuration(ctx context.Context, name, result string, duration float64)
	// webhook 超时或错误导致一个未被取消的请求 fail open 时，
	// RecordWebhookFailOpen 递增。
	// result 是以下之一：
	// - timeout：webhook 请求超时
	// - error：webhook 响应完成但无效
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

// RegisterMetrics 注册授权器指标。
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
