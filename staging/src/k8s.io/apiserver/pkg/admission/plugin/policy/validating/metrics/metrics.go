/*
Copyright 2022 The Kubernetes Authors.

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

// Package metrics 暴露 ValidatingAdmissionPolicy 的检查计数与耗时指标。
//
// 中文说明：之所以单列一个包，是因为指标需要被策略插件与测试同时引用，
// 放在插件内部会造成循环依赖；同时集中定义标签集可避免各处自行拼装导致基数失控。
package metrics

import (
	"context"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	metricsNamespace = "apiserver"
	metricsSubsystem = "validating_admission_policy"
)

// ValidationErrorType defines different error types that happen to a validation expression
//
// 中文说明：把表达式失败原因归类为有限集合，作为指标标签。
// 标签基数必须受控，因此错误详情不进入标签，只保留类别。
type ValidationErrorType string

const (
	// ValidationCompileError indicates that the expression fails to compile.
	ValidationCompileError ValidationErrorType = "compile_error"
	// ValidatingInvalidError indicates that the expression fails due to internal
	// errors that are out of the control of the user.
	ValidatingInvalidError ValidationErrorType = "invalid_error"
	// ValidatingOutOfBudget indicates that the expression fails due to running
	// out of cost budget, or the budget cannot be obtained.
	ValidatingOutOfBudget ValidationErrorType = "out_of_budget"
	// ValidationNoError indicates that the expression returns without an error.
	ValidationNoError ValidationErrorType = "no_error"
)

var (
	// Metrics provides access to validation admission metrics.
	//
	// 中文说明：包级单例，构造时即注册到 legacyregistry；因此测试中必须调用 Reset
	// 清理状态，且不能重复注册同一指标。
	Metrics = newValidationAdmissionMetrics()
)

// ValidatingAdmissionPolicyMetrics aggregates Prometheus metrics related to validation admission control.
//
// 中文说明：check_total 与 check_duration_seconds 共享同一组标签，
// 便于按 policy/binding/error_type/enforcement_action 交叉分析命中率与耗时。
type ValidatingAdmissionPolicyMetrics struct {
	policyCheck   *metrics.CounterVec
	policyLatency *metrics.HistogramVec
}

// newValidationAdmissionMetrics 构造并注册两个指标。
// 中文说明：指标为 BETA 稳定性，命名与标签属于对外契约，改名需遵循弃用流程。
func newValidationAdmissionMetrics() *ValidatingAdmissionPolicyMetrics {
	check := metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      metricsNamespace,
			Subsystem:      metricsSubsystem,
			Name:           "check_total",
			Help:           "Validation admission policy check total, labeled by policy and further identified by binding and enforcement action taken.",
			StabilityLevel: metrics.BETA,
		},
		[]string{"policy", "policy_binding", "error_type", "enforcement_action"},
	)
	latency := metrics.NewHistogramVec(&metrics.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "check_duration_seconds",
		Help:      "Validation admission latency for individual validation expressions in seconds, labeled by policy and further including binding and enforcement action taken.",
		// the bucket distribution here is based on the benchmark suite at
		// github.com/DangerOnTheRanger/cel-benchmark performed on 16-core Intel Xeon
		// the lowest bucket was based around the 180ns/op figure for BenchmarkAccess,
		// plus some additional leeway to account for the apiserver doing other things
		// the largest bucket was chosen based on the fact that benchmarks indicate the
		// same Xeon running a CEL expression close to the estimated cost limit takes
		// around 760ms, so that bucket should only ever have the slowest CEL expressions
		// in it
		Buckets:        []float64{0.0000005, 0.001, 0.01, 0.1, 1.0},
		StabilityLevel: metrics.BETA,
	},
		[]string{"policy", "policy_binding", "error_type", "enforcement_action"},
	)

	legacyregistry.MustRegister(check)
	legacyregistry.MustRegister(latency)
	return &ValidatingAdmissionPolicyMetrics{policyCheck: check, policyLatency: latency}
}

// Reset resets all validation admission-related Prometheus metrics.
//
// 中文说明：主要供测试在用例之间清零；生产代码不应调用，否则会丢失累计值。
func (m *ValidatingAdmissionPolicyMetrics) Reset() {
	m.policyCheck.Reset()
	m.policyLatency.Reset()
}

// ObserveAdmission observes a policy validation, with an optional error to indicate the error that may occur but ignored.
//
// 中文说明：enforcement_action 固定为 "allow"——表示该策略没有阻止请求。
// errorType 仍会记录，因为「失败但被忽略」（如 failurePolicy=Ignore）正是需要观测的形态。
func (m *ValidatingAdmissionPolicyMetrics) ObserveAdmission(ctx context.Context, elapsed time.Duration, policy, binding string, errorType ValidationErrorType) {
	m.policyCheck.WithContext(ctx).WithLabelValues(policy, binding, string(errorType), "allow").Inc()
	m.policyLatency.WithContext(ctx).WithLabelValues(policy, binding, string(errorType), "allow").Observe(elapsed.Seconds())
}

// ObserveRejection observes a policy validation error that was at least one of the reasons for a deny.
//
// 中文说明：使用 "deny" 标签，与 allow 区分，便于计算拒绝率。
func (m *ValidatingAdmissionPolicyMetrics) ObserveRejection(ctx context.Context, elapsed time.Duration, policy, binding string, errorType ValidationErrorType) {
	m.policyCheck.WithContext(ctx).WithLabelValues(policy, binding, string(errorType), "deny").Inc()
	m.policyLatency.WithContext(ctx).WithLabelValues(policy, binding, string(errorType), "deny").Observe(elapsed.Seconds())
}

// ObserveAudit observes a policy validation audit annotation was published for a validation failure.
//
// 中文说明：failurePolicy=Audit 时失败不阻止请求，只写审计注解；
// 单独的 "audit" 标签让运维能区分「真正放行」与「因审计策略放行」。
func (m *ValidatingAdmissionPolicyMetrics) ObserveAudit(ctx context.Context, elapsed time.Duration, policy, binding string, errorType ValidationErrorType) {
	m.policyCheck.WithContext(ctx).WithLabelValues(policy, binding, string(errorType), "audit").Inc()
	m.policyLatency.WithContext(ctx).WithLabelValues(policy, binding, string(errorType), "audit").Observe(elapsed.Seconds())
}

// ObserveWarn observes a policy validation warning was published for a validation failure.
//
// 中文说明：failurePolicy=Warn 时向客户端返回 warning 但不拒绝，对应 "warn" 标签。
func (m *ValidatingAdmissionPolicyMetrics) ObserveWarn(ctx context.Context, elapsed time.Duration, policy, binding string, errorType ValidationErrorType) {
	m.policyCheck.WithContext(ctx).WithLabelValues(policy, binding, string(errorType), "warn").Inc()
	m.policyLatency.WithContext(ctx).WithLabelValues(policy, binding, string(errorType), "warn").Observe(elapsed.Seconds())
}
