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

package metrics

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const taintEvictionControllerSubsystem = "taint_eviction_controller"

var (
	// PodDeletionsTotal 统计 TaintEvictionController 自启动以来删除的 Pod 数量。
	PodDeletionsTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      taintEvictionControllerSubsystem,
			Name:           "pod_deletions_total",
			Help:           "Total number of Pods deleted by TaintEvictionController since its start.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// PodDeletionsLatency 记录从 Pod 上的污点效果生效到其被删除之间的延迟（秒）。
	PodDeletionsLatency = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      taintEvictionControllerSubsystem,
			Name:           "pod_deletion_duration_seconds",
			Help:           "Latency, in seconds, between the time when a taint effect has been activated for the Pod and its deletion via TaintEvictionController.",
			Buckets:        []float64{0.005, 0.025, 0.1, 0.5, 1, 2.5, 10, 30, 60, 120, 180, 240}, // 5 毫秒至 4 分钟
			StabilityLevel: metrics.ALPHA,
		},
	)
)

var registerMetrics sync.Once

// Register 注册 TaintEvictionController 的各项指标。
func Register() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(PodDeletionsTotal)
		legacyregistry.MustRegister(PodDeletionsLatency)
	})
}
