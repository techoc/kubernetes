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
// 指标按 policy/policy_binding 双标签聚合，便于区分"策略本身写得慢"
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

// MutationErrorType 定义了变更表达式执行时出现的不同错误类型。
//
// 错误类型作为标签而非独立指标，避免指标数量随错误种类膨胀。
type MutationErrorType string

const (
	// MutationCompileError 表示表达式编译失败。
	// 编译错误通常源于策略作者，可通过重新提交策略修复。
	MutationCompileError MutationErrorType = "compile_error"
	// MutatingInvalidError 表示表达式因用户无法控制的内部错误而失败。
	// 此类错误不应归咎于策略作者，告警时应与用户错误分开统计。
	MutatingInvalidError MutationErrorType = "invalid_error"
	// MutatingOutOfBudget 表示表达式因耗尽成本预算或无法获取预算而失败。
	// 超出 CEL 成本预算意味着表达式被判定为过于昂贵而拒绝执行。
	MutatingOutOfBudget MutationErrorType = "out_of_budget"
	// MutationNoError 表示表达式无错误地返回。
	// 作为"成功"标签值，让成功率可以直接用同一指标计算。
	MutationNoError MutationErrorType = "no_error"
)

var (
	// Metrics 提供对变更准入指标的访问。
	//
	// 包级单例，注册到 legacyregistry 供 /metrics 直接暴露。
	Metrics = newMutationAdmissionMetrics()
)

// MutatingAdmissionPolicyMetrics 聚合与变更准入控制相关的 Prometheus 指标。
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
		// 此处的桶分布基于在 16 核 Intel Xeon 上运行的
		// github.com/DangerOnTheRanger/cel-benchmark 基准测试套件；
		// 最小桶基于 BenchmarkAccess 约 180ns/op 的数值，
		// 并预留了一些余量以容纳 apiserver 的其他工作；
		// 最大桶的选取依据是：基准测试显示同一 Xeon 求值接近成本预算上限的
		// CEL 表达式约需 760ms，因此该桶只会落入最慢的 CEL 表达式。
		Buckets:        []float64{0.0000005, 0.001, 0.01, 0.1, 1.0},
		StabilityLevel: metrics.ALPHA,
	},
		[]string{"policy", "policy_binding", "error_type"},
	)

	legacyregistry.MustRegister(check)
	legacyregistry.MustRegister(latency)
	return &MutatingAdmissionPolicyMetrics{policyCheck: check, policyLatency: latency}
}

// Reset 重置所有变更准入相关的 Prometheus 指标。
//
// 仅供测试使用，重置可避免用例间指标互相污染。
func (m *MutatingAdmissionPolicyMetrics) Reset() {
	m.policyCheck.Reset()
	m.policyLatency.Reset()
}

// ObserveAdmission 记录一次策略变更，errorType 可为被忽略的错误。
//
// 计数值与延迟同时记录，确保任何一次检查都能在两张图上对应起来。
func (m *MutatingAdmissionPolicyMetrics) ObserveAdmission(ctx context.Context, elapsed time.Duration, policy, binding string, errorType MutationErrorType) {
	m.policyCheck.WithContext(ctx).WithLabelValues(policy, binding, string(errorType)).Inc()
	m.policyLatency.WithContext(ctx).WithLabelValues(policy, binding, string(errorType)).Observe(elapsed.Seconds())
}

// ObserveRejection 记录至少构成某次拒绝原因之一的策略变更错误。
//
// 与 ObserveAdmission 写入相同指标，区别只在调用语义；拒绝会被
// 计入带具体 error_type 的计数，因此拒绝率可直接由 error_type != no_error 得出。
func (m *MutatingAdmissionPolicyMetrics) ObserveRejection(ctx context.Context, elapsed time.Duration, policy, binding string, errorType MutationErrorType) {
	m.policyCheck.WithContext(ctx).WithLabelValues(policy, binding, string(errorType)).Inc()
	m.policyLatency.WithContext(ctx).WithLabelValues(policy, binding, string(errorType)).Observe(elapsed.Seconds())
}
