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

// controllerSubsystem 必须与 cmd/kube-controller-manager/names 中的控制器名称保持一致，
// 否则指标名与控制器注册表不匹配会导致命名冲突或指标缺失。
const controllerSubsystem = "device_taint_eviction_controller"

var (
	// Global 是全局默认指标实例，供控制器运行时使用；
	// 测试应使用 New 创建独立实例，避免并行测试间共享状态。
	Global = New()
)

// registerMetrics 保证指标只注册一次，重复注册会导致 panic。
var registerMetrics sync.Once

// Register 注册设备污点驱逐控制器的指标。
func Register() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(Global.PodDeletionsTotal)
		legacyregistry.MustRegister(Global.PodDeletionsLatency)
	})
}

// New 创建该控制器的全套新指标实例，供并行测试各自持有独立状态。
// 可选地指定直方图的桶边界；不指定时使用默认边界。
func New(buckets ...float64) Metrics {
	if len(buckets) == 0 {
		buckets = []float64{0.005, 0.025, 0.1, 0.5, 1, 2.5, 10, 30, 60, 120, 180, 240} // 5ms 到 4m
	}
	m := Metrics{
		KubeRegistry: metrics.NewKubeRegistry(),
		PodDeletionsTotal: metrics.NewCounter(
			&metrics.CounterOpts{
				Subsystem:      controllerSubsystem,
				Name:           "pod_deletions_total",
				Help:           "Total number of Pods deleted by DeviceTaintEvictionController since its start.",
				StabilityLevel: metrics.ALPHA,
			},
		),
		PodDeletionsLatency: metrics.NewHistogram(
			&metrics.HistogramOpts{
				Subsystem:      controllerSubsystem,
				Name:           "pod_deletion_duration_seconds",
				Help:           "Latency, in seconds, between the time when a device taint effect has been activated and a Pod's deletion via DeviceTaintEvictionController.",
				Buckets:        []float64{0.005, 0.025, 0.1, 0.5, 1, 2.5, 10, 30, 60, 120, 180, 240}, // 5ms 到 4m，
				StabilityLevel: metrics.ALPHA,
			},
		),
	}

	// 必须在构造完成后再覆盖桶边界，否则 ./hack/update-generated-stable-metrics.sh
	// 无法找到默认的桶定义。
	if len(buckets) > 0 {
		m.PodDeletionsLatency.HistogramOpts.Buckets = buckets
	}

	m.KubeRegistry.MustRegister(m.PodDeletionsTotal, m.PodDeletionsLatency)
	return m
}

// Metrics 包含设备污点驱逐控制器支持的所有指标。
// 它实现了 [metrics.Gatherer]，便于测试直接采集指标值。
type Metrics struct {
	metrics.KubeRegistry
	PodDeletionsTotal   *metrics.Counter
	PodDeletionsLatency *metrics.Histogram
}

var _ metrics.Gatherer = Metrics{}
