/*
Copyright 2023 The Kubernetes Authors.

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

package monitor

import (
	"time"

	v2 "k8s.io/api/autoscaling/v2"
)

// ActionLabel 标记一次协调或指标计算的动作结果，作为监控指标的标签维度。
// 区分扩容、缩容与无动作，才能分别观察两类决策的频率与耗时。
type ActionLabel string

// ErrorLabel 标记失败来源，便于把用户配置错误与控制器自身故障区分开。
// 二者混在一个指标里会让配置问题被系统噪声掩盖。
type ErrorLabel string

const (
	// ActionLabelScaleUp 表示本次计算得出扩容决策。
	ActionLabelScaleUp ActionLabel = "scale_up"
	// ActionLabelScaleDown 表示本次计算得出缩容决策。
	ActionLabelScaleDown ActionLabel = "scale_down"
	// ActionLabelNone 表示副本数保持不变，这是稳态下的常见结果。
	ActionLabelNone ActionLabel = "none"

	// ErrorLabelSpec 表示错误源自 HPA 对象自身的非法 spec（用户的配置问题）。
	ErrorLabelSpec ErrorLabel = "spec"
	// ErrorLabelInternal 表示错误来自内部计算或与其他组件的通信（控制器侧问题）。
	ErrorLabelInternal ErrorLabel = "internal"
	// ErrorLabelNone 表示本次操作成功，没有错误。
	ErrorLabelNone ErrorLabel = "none"
)

// Monitor 汇总 HPA 控制器的可观测性指标：协调次数与耗时、指标计算次数与耗时、
// HPA 对象总数、期望副本数以及因限流而跳过的重新入队次数。
// 拆成独立接口是为了让控制器逻辑与具体指标后端解耦，测试时可替换实现。
type Monitor interface {
	// ObserveReconciliationResult 记录一次完整协调的结果标签与耗时。
	ObserveReconciliationResult(action ActionLabel, err ErrorLabel, duration time.Duration)
	// ObserveMetricComputationResult 记录一次指标计算的结果，额外的 metricType 维度
	// 用于区分资源指标、自定义指标等不同来源的延迟表现。
	ObserveMetricComputationResult(action ActionLabel, err ErrorLabel, duration time.Duration, metricType v2.MetricSourceType)
	// ObserveHPAAddition 记录新增一个 HPA 对象。
	ObserveHPAAddition()
	// ObserveHPADeletion 记录删除一个 HPA 对象。
	ObserveHPADeletion()
	// ObserveDesiredReplicas 记录指定 HPA 当前算出的期望副本数。
	ObserveDesiredReplicas(namespace, hpaName string, desiredReplicas int32)
	// ObserveHPARequeueSkips 记录因限流而跳过的重新入队次数，按 API 组和资源配置区分。
	ObserveHPARequeueSkips(group string, resource string)
}

// monitor 是 Monitor 的 Prometheus 实现，本身无状态，直接使用全局指标向量。
type monitor struct{}

// New 返回一个基于全局 Prometheus 指标向量的 Monitor 实现。
// 指标向量本身是进程级共享的，因此这里只需返回空结构体。
func New() Monitor {
	return &monitor{}
}

// ObserveReconciliationResult 记录一次完整协调的动作、错误类别与耗时。
// 同时更新计数和直方图：计数反映决策频率，直方图反映延迟分布。
func (r *monitor) ObserveReconciliationResult(action ActionLabel, err ErrorLabel, duration time.Duration) {
	ReconciliationsTotal.WithLabelValues(string(action), string(err)).Inc()
	ReconciliationsDuration.WithLabelValues(string(action), string(err)).Observe(duration.Seconds())
}

// ObserveMetricComputationResult 记录一次指标计算的结果，按动作、错误类别和指标来源细分。
// 多一个 metricType 维度是为了定位到底是哪种指标源（如外部指标服务）拖慢了协调。
func (r *monitor) ObserveMetricComputationResult(action ActionLabel, err ErrorLabel, duration time.Duration, metricType v2.MetricSourceType) {
	MetricComputationTotal.WithLabelValues(string(action), string(err), string(metricType)).Inc()
	MetricComputationDuration.WithLabelValues(string(action), string(err), string(metricType)).Observe(duration.Seconds())
}

// ObserveHPAAddition 在新建 HPA 对象时递增总数仪表。
func (r *monitor) ObserveHPAAddition() {
	NumHorizontalPodAutoscalers.Inc()
}

// ObserveHPADeletion 在删除 HPA 对象时递减总数仪表。
func (r *monitor) ObserveHPADeletion() {
	NumHorizontalPodAutoscalers.Dec()
}

// ObserveDesiredReplicas 以 Gauge 记录某个 HPA 的期望副本数。
// 使用 Set 而非 Inc，因为期望副本数会随负载在扩容和缩容之间来回变化。
func (r *monitor) ObserveDesiredReplicas(namespace, hpaName string, desiredReplicas int32) {
	DesiredReplicasCount.WithLabelValues(namespace, hpaName).Set(float64(desiredReplicas))
}

// ObserveHPARequeueSkips 记录被限流跳过的重新入队次数。
// 该指标能暴露 API 请求节流是否过于激进导致 HPA 状态更新延迟。
func (r *monitor) ObserveHPARequeueSkips(group string, resource string) {
	HPARequeueSkips.WithLabelValues(group, resource).Inc()
}
