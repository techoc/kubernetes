/*
Copyright 2024 The Kubernetes Authors.

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

package cel

import (
	"context"
	"sync"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// MatcherMetrics 定义上报 matchCondition 指标的方法。
//
// 接口化是为了让调用方在不需要指标时注入 Noop 实现，避免在生产路径上引入空指针判断。
type MatcherMetrics interface {
	// RecordAuthorizationMatchConditionEvaluation 记录一次 Authorize 中求值 matchConditions 的总耗时。
	RecordAuthorizationMatchConditionEvaluation(ctx context.Context, authorizerType, authorizerName string, elapsed time.Duration)
	// RecordAuthorizationMatchConditionEvaluationFailure 在求值 matchConditions 遇到任意错误时累加计数。
	RecordAuthorizationMatchConditionEvaluationFailure(ctx context.Context, authorizerType, authorizerName string)
	// RecordAuthorizationMatchConditionExclusion 在至少一个 matchCondition 求值为 false 且跳过 webhook 时累加计数。
	RecordAuthorizationMatchConditionExclusion(ctx context.Context, authorizerType, authorizerName string)
}

// NoopMatcherMetrics 是空实现，用于未配置指标或测试场景，避免调用方判空。
type NoopMatcherMetrics struct{}

// RecordAuthorizationMatchConditionEvaluation 空实现。
func (NoopMatcherMetrics) RecordAuthorizationMatchConditionEvaluation(ctx context.Context, authorizerType, authorizerName string, elapsed time.Duration) {
}

// RecordAuthorizationMatchConditionEvaluationFailure 空实现。
func (NoopMatcherMetrics) RecordAuthorizationMatchConditionEvaluationFailure(ctx context.Context, authorizerType, authorizerName string) {
}

// RecordAuthorizationMatchConditionExclusion 空实现。
func (NoopMatcherMetrics) RecordAuthorizationMatchConditionExclusion(ctx context.Context, authorizerType, authorizerName string) {
}

// matcherMetrics 是真实实现，指标通过 legacyregistry 注册到全局。
type matcherMetrics struct{}

// NewMatcherMetrics 注册（幂等）并返回真实指标实现。
func NewMatcherMetrics() MatcherMetrics {
	RegisterMetrics()
	return matcherMetrics{}
}

// 指标命名空间与子系统，与其余 apiserver 指标保持一致的 apiserver_authorization_* 前缀。
const (
	namespace = "apiserver"
	subsystem = "authorization"
)

var (
	// 条件求值错误计数：出现即说明策略表达式有缺陷，需要告警。
	authorizationMatchConditionEvaluationErrorsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "match_condition_evaluation_errors_total",
			Help:           "Total number of errors when an authorization webhook encounters a match condition error split by authorizer type and name.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"type", "name"},
	)
	// 条件排除计数：条件为 false 导致 webhook 被跳过。
	authorizationMatchConditionExclusionsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "match_condition_exclusions_total",
			Help:           "Total number of exclusions when an authorization webhook is skipped because match conditions exclude it.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"type", "name"},
	)
	// 求值耗时直方图；桶上限 0.25s，覆盖条件求值通常应在毫秒级的预期。
	authorizationMatchConditionEvaluationSeconds = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "match_condition_evaluation_seconds",
			Help:           "Authorization match condition evaluation time in seconds, split by authorizer type and name.",
			Buckets:        []float64{0.001, 0.005, 0.01, 0.025, 0.1, 0.2, 0.25},
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"type", "name"},
	)
)

// registerMetrics 保证指标只注册一次，重复注册会 panic。
var registerMetrics sync.Once

// RegisterMetrics 幂等地把匹配条件相关指标注册到 legacyregistry。
func RegisterMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(authorizationMatchConditionEvaluationErrorsTotal)
		legacyregistry.MustRegister(authorizationMatchConditionExclusionsTotal)
		legacyregistry.MustRegister(authorizationMatchConditionEvaluationSeconds)
	})
}

// ResetMetricsForTest 清空指标，仅供测试在用例间隔离状态。
func ResetMetricsForTest() {
	authorizationMatchConditionEvaluationErrorsTotal.Reset()
	authorizationMatchConditionExclusionsTotal.Reset()
	authorizationMatchConditionEvaluationSeconds.Reset()
}

// RecordAuthorizationMatchConditionEvaluationFailure 累加错误计数。
func (matcherMetrics) RecordAuthorizationMatchConditionEvaluationFailure(ctx context.Context, authorizerType, authorizerName string) {
	authorizationMatchConditionEvaluationErrorsTotal.WithContext(ctx).WithLabelValues(authorizerType, authorizerName).Inc()
}

// RecordAuthorizationMatchConditionExclusion 累加排除计数。
func (matcherMetrics) RecordAuthorizationMatchConditionExclusion(ctx context.Context, authorizerType, authorizerName string) {
	authorizationMatchConditionExclusionsTotal.WithContext(ctx).WithLabelValues(authorizerType, authorizerName).Inc()
}

// RecordAuthorizationMatchConditionEvaluation 以秒为单位记录耗时。
func (matcherMetrics) RecordAuthorizationMatchConditionEvaluation(ctx context.Context, authorizerType, authorizerName string, elapsed time.Duration) {
	elapsedSeconds := elapsed.Seconds()
	authorizationMatchConditionEvaluationSeconds.WithContext(ctx).WithLabelValues(authorizerType, authorizerName).Observe(elapsedSeconds)
}
