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

package metrics

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// StatefulSetControllerSubsystem 是 StatefulSet 控制器所有指标共用的 Prometheus subsystem 前缀。
const StatefulSetControllerSubsystem = "statefulset_controller"

var (
	// MaxUnavailable 记录在启用 MaxUnavailableStatefulSet 特性时，
	// 当前 .spec.updateStrategy.rollingUpdate.maxUnavailable 的配置值。
	// 该 gauge 反映滚动更新期间允许不可用的 Pod 数上限，便于观察可用性约束；默认值为 1。
	//
	// 示例监控查询：
	// - 当前 maxUnavailable 设置：statefulset_max_unavailable
	// - 与实际不可用数对比：statefulset_unavailable_replicas - statefulset_max_unavailable
	// - 超出上限时告警：statefulset_unavailable_replicas > statefulset_max_unavailable
	// - 观察配置变化：changes(statefulset_max_unavailable[1h])
	MaxUnavailable = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      StatefulSetControllerSubsystem,
			Name:           "statefulset_max_unavailable",
			Help:           "Maximum number of unavailable pods allowed during StatefulSet rolling updates",
			StabilityLevel: metrics.ALPHA,
		}, []string{"statefulset_namespace", "statefulset_name", "pod_management_policy"},
	)

	// UnavailableReplicas 记录 StatefulSet 中当前不可用的 Pod 数量。
	// 该 gauge 实时反映缺失或未就绪（未满足 .spec.minReadySeconds）的 Pod 数。
	//
	// 示例监控查询：
	// - 当前不可用 Pod 数：statefulset_unavailable_replicas
	// - 可用率：(statefulset_replicas - statefulset_unavailable_replicas) / statefulset_replicas * 100
	// - 不可用数过高时告警：statefulset_unavailable_replicas > statefulset_max_unavailable
	// - 观察可用性趋势：statefulset_unavailable_replicas
	UnavailableReplicas = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      StatefulSetControllerSubsystem,
			Name:           "statefulset_unavailable_replicas",
			Help:           "Current number of unavailable pods in StatefulSet",
			StabilityLevel: metrics.ALPHA,
		}, []string{"statefulset_namespace", "statefulset_name", "pod_management_policy"},
	)

	// StatefulSetRequeueSkips 记录因 watch cache 过期而跳过的 StatefulSet 同步次数。
	// 跳过过时的同步可以避免基于陈旧缓存做出错误的扩缩容决策。
	StatefulSetRequeueSkips = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      StatefulSetControllerSubsystem,
			Name:           "stale_sync_skips_total",
			Help:           "Total number of StatefulSet syncs skipped due to a stale watch cache.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"group", "resource"},
	)
)

var registerMetrics sync.Once

// Register 将所有 StatefulSet 控制器指标注册到 legacy registry。
// 使用 sync.Once 保证重复调用时不会因重复注册而 panic。
func Register() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(MaxUnavailable)
		legacyregistry.MustRegister(UnavailableReplicas)
		legacyregistry.MustRegister(StatefulSetRequeueSkips)
	})
}
