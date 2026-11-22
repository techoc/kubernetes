/*
Copyright 2025 The Kubernetes Authors.

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

// Package metrics 定义 MutatingAdmissionPolicy 的 Prometheus 指标。
//
// 中文说明：指标按 policy/policy_binding 双标签聚合，便于区分"策略本身写得慢"
// 与"某个绑定引入了昂贵参数"；标签基数受限于策略对象数量。
package metrics

import (
	"context"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	// 指标命名与 validating 策略保持一致，便于统一看板。
	metricsNamespace = "apiserver"
	metricsSubsystem = "mutating_admission_policy"
)

// MutationErrorType defines different error types that happen to a mutation expression
//
// 中文说明：错误类型作为标签而非独立指标，避免指标数量随错误种类膨胀。
type MutationErrorType string

const (
	// MutationCompileError indicates that the expression fails to compile.
	// 中文说明：编译错误通常源于策略作者，可通过重新提交策略修复。
	MutationCompileError MutationErrorType = "compile_error"
	// MutatingInvalidError indicates that the expression fails due to internal
	// errors that are out of the control of the user.
	// 中文说明：此类错误不应归咎于策略作者，告警时应与用户错误分开统计。
	MutatingInvalidError MutationErrorType = "invalid_error"
	// MutatingOutOfBudget indicates that the expression fails due to running
	// out of cost budget, or the budget cannot be obtained.
	// 中文说明：超出 CEL 成本预算意味着表达式被判定为过于昂贵而拒绝执行。
	MutatingOutOfBudget MutationErrorType = "out_of_budget"
	// MutationNoError indicates that the expression returns without an error.
	// 中文说明：作为"成功"标签值，让成功率可以直接用同一指标计算。
	MutationNoError MutationErrorType = "no_error"
)

var (
	// Metrics provides access to mutation admission metrics.
	//
	// 中文说明：包级单例，注册到 legacyregistry 供 /metrics 直接暴露。
	Metrics = newMutationAdmissionMetrics()
)

// MutatingAdmissionPolicyMetrics aggregates Prometheus metrics related to mutation admission control.
type MutatingAdmissionPolicyMetrics struct {
	policyCheck   *metrics.CounterVec
	policyLatency *metrics.HistogramVec
}

// newMutationAdmissionMetrics 构造并注册指标；注册失败即 panic，
// 因为重复注册说明二进制内存在冲突的指标定义，属于开发期错误。
func newMutationAdmissionMetrics() *MutatingAdmissionPolicyMetrics {
	check := metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      metricsNamespace,
			Subsystem:      metricsSubsystem,
			Name:           "check_total",
			Help:           "Mutation admission policy check total, labeled by policy and further identified by binding.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"policy", "policy_binding", "error_type"},
	)
	latency := metrics.NewHistogramVec(&metrics.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "check_duration_seconds",
		Help:      "Mutation admission latency for individual mutation expressions in seconds, labeled by policy and binding.",
		// the bucket distribution here is based oo the benchmark suite at
		// github.com/DangerOnTheRanger/cel-benchmark performed on 16-core Intel Xeon
		// the lowest bucket was based around the 180ns/op figure for BenchmarkAccess,
		// plus some additional leeway to account for the apiserver doing other things
		// the largest bucket was chosen based on the fact that benchmarks indicate the
		// same Xeon running a CEL expression close to the estimated cost limit takes
		// around 760ms, so that bucket should only ever have the slowest CEL expressions
		// in it
		Buckets:        []float64{0.0000005, 0.001, 0.01, 0.1, 1.0},
		StabilityLevel: metrics.ALPHA,
	},
		[]string{"policy", "policy_binding", "error_type"},
	)

	legacyregistry.MustRegister(check)
	legacyregistry.MustRegister(latency)
	return &MutatingAdmissionPolicyMetrics{policyCheck: check, policyLatency: latency}
}

// Reset resets all mutation admission-related Prometheus metrics.
//
// 中文说明：仅供测试使用，重置可避免用例间指标互相污染。
func (m *MutatingAdmissionPolicyMetrics) Reset() {
	m.policyCheck.Reset()
	m.policyLatency.Reset()
}

// ObserveAdmission observes a policy mutation, with an optional error to indicate the error that may occur but ignored.
//
// 中文说明：计数值与延迟同时记录，确保任何一次检查都能在两张图上对应起来。
func (m *MutatingAdmissionPolicyMetrics) ObserveAdmission(ctx context.Context, elapsed time.Duration, policy, binding string, errorType MutationErrorType) {
	m.policyCheck.WithContext(ctx).WithLabelValues(policy, binding, string(errorType)).Inc()
	m.policyLatency.WithContext(ctx).WithLabelValues(policy, binding, string(errorType)).Observe(elapsed.Seconds())
}

// ObserveRejection observes a policy mutation error that was at least one of the reasons for a deny.
//
// 中文说明：与 ObserveAdmission 写入相同指标，区别只在调用语义；拒绝会被
// 计入带具体 error_type 的计数，因此拒绝率可直接由 error_type != no_error 得出。
func (m *MutatingAdmissionPolicyMetrics) ObserveRejection(ctx context.Context, elapsed time.Duration, policy, binding string, errorType MutationErrorType) {
	m.policyCheck.WithContext(ctx).WithLabelValues(policy, binding, string(errorType)).Inc()
	m.policyLatency.WithContext(ctx).WithLabelValues(policy, binding, string(errorType)).Observe(elapsed.Seconds())
}
