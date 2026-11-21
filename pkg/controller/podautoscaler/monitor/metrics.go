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

// metrics 包暴露 HPA 控制器的监控指标。
package monitor

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	// hpaControllerSubsystem 是 HPA 控制器所有指标共用的子系统名。
	hpaControllerSubsystem = "horizontal_pod_autoscaler_controller"
)

var (
	// ReconciliationsTotal 统计 HPA 协调次数，按 action 与 error 标签拆分。
	// action 表示扩缩容决策，error 记录首个发生的错误类型，便于按结果维度分析。
	// ReconciliationsTotal 统计 HPA 协调次数，按 action 与 error 标签拆分。
	// action 表示扩缩容决策，error 记录首个发生的错误类型，便于按结果维度分析。
	ReconciliationsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      hpaControllerSubsystem,
			Name:           "reconciliations_total",
			Help:           "Number of reconciliations of HPA controller. The label 'action' should be either 'scale_down', 'scale_up', or 'none'. Also, the label 'error' should be either 'spec', 'internal', or 'none'. Note that if both spec and internal errors happen during a reconciliation, the first one to occur is reported in `error` label.",
			StabilityLevel: metrics.BETA,
		}, []string{"action", "error"})
	// ReconciliationsDuration 统计单次 HPA 协调耗时，标签含义与计数指标一致；
	// 指数桶覆盖毫秒到数十秒，便于观察长尾延迟。
	// ReconciliationsDuration 统计单次 HPA 协调耗时，标签含义与计数指标一致；
	// 指数桶覆盖毫秒到数十秒，便于观察长尾延迟。
	ReconciliationsDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      hpaControllerSubsystem,
			Name:           "reconciliation_duration_seconds",
			Help:           "The time(seconds) that the HPA controller takes to reconcile once. The label 'action' should be either 'scale_down', 'scale_up', or 'none'. Also, the label 'error' should be either 'spec', 'internal', or 'none'. Note that if both spec and internal errors happen during a reconciliation, the first one to occur is reported in `error` label.",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.BETA,
		}, []string{"action", "error"})
	// MetricComputationTotal 统计指标计算次数，额外按 metric_type 区分 HPA 中配置的指标类型。
	// MetricComputationTotal 统计指标计算次数，额外按 metric_type 区分 HPA 中配置的指标类型。
	MetricComputationTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      hpaControllerSubsystem,
			Name:           "metric_computation_total",
			Help:           "Number of metric computations. The label 'action' should be either 'scale_down', 'scale_up', or 'none'. Also, the label 'error' should be either 'spec', 'internal', or 'none'. The label 'metric_type' corresponds to HPA.spec.metrics[*].type",
			StabilityLevel: metrics.BETA,
		}, []string{"action", "error", "metric_type"})
	// MetricComputationDuration 统计单个指标的计算耗时，用于定位特定类型指标的性能瓶颈。
	// MetricComputationDuration 统计单个指标的计算耗时，用于定位特定类型指标的性能瓶颈。
	MetricComputationDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      hpaControllerSubsystem,
			Name:           "metric_computation_duration_seconds",
			Help:           "The time(seconds) that the HPA controller takes to calculate one metric. The label 'action' should be either 'scale_down', 'scale_up', or 'none'. The label 'error' should be either 'spec', 'internal', or 'none'. The label 'metric_type' corresponds to HPA.spec.metrics[*].type",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.BETA,
		}, []string{"action", "error", "metric_type"})
	// NumHorizontalPodAutoscalers 是受控 HPA 对象当前数量的仪表盘指标。
	// NumHorizontalPodAutoscalers 是受控 HPA 对象当前数量的仪表盘指标。
	NumHorizontalPodAutoscalers = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      hpaControllerSubsystem,
			Name:           "num_horizontal_pod_autoscalers",
			Help:           "Current number of controlled HPA objects.",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// DesiredReplicasCount 记录每个 HPA 当前期望的副本数，按命名空间与名称拆分，
	// 便于直接查看具体 HPA 的扩缩容决策结果。
	// DesiredReplicasCount 记录每个 HPA 当前期望的副本数，按命名空间与名称拆分，
	// 便于直接查看具体 HPA 的扩缩容决策结果。
	DesiredReplicasCount = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      hpaControllerSubsystem,
			Name:           "desired_replicas",
			Help:           "Current desired replica count for HPA objects.",
			StabilityLevel: metrics.ALPHA,
		}, []string{"namespace", "hpa_name"})

	// HPARequeueSkips 统计因监听缓存过期而跳过的 HPA 同步次数。
	// 使用过期缓存做出扩缩容决策可能基于错误的副本数，因此必须跳过等待重新入队；
	// 该指标用于监控跳过的频率。
	HPARequeueSkips = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      hpaControllerSubsystem,
			Name:           "stale_sync_skips_total",
			Help:           "Total number of HPA syncs skipped due to a stale watch cache.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	// metricsList 汇总所有需要注册的指标，避免注册时遗漏。
	// metricsList 汇总所有需要注册的指标，避免注册时遗漏。
	metricsList = []metrics.Registerable{
		ReconciliationsTotal,
		ReconciliationsDuration,
		MetricComputationTotal,
		MetricComputationDuration,
		NumHorizontalPodAutoscalers,
		DesiredReplicasCount,
		HPARequeueSkips,
	}
)

var register sync.Once

// Register 注册所有 HPA 控制器指标。
// sync.Once 保证重复初始化时不会因重复注册同一指标而 panic。
func Register() {
	// 注册指标。
	register.Do(func() {
		registerMetrics(metricsList...)
	})
}

// registerMetrics 逐个把给定的指标注册到 legacy registry。
func registerMetrics(extraMetrics ...metrics.Registerable) {
	for _, metric := range extraMetrics {
		legacyregistry.MustRegister(metric)
	}
}
