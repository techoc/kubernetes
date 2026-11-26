/*
Copyright The Kubernetes Authors.

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

package nodelifecycle

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	// 指标子系统名与控制器名称保持一致，便于在 Prometheus 中定位。
	metricsSubsystem = "cloud_node_lifecycle_controller"
)

var (
	// monitorNodesDuration 是单轮 MonitorNodes 执行耗时的直方图（秒）。
	monitorNodesDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      metricsSubsystem,
			Name:           "monitor_nodes_duration_seconds",
			Help:           "A metric measuring the duration in seconds of a single MonitorNodes loop execution.",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
	)

	// cloudProviderCalls 统计云 provider API 调用总数，
	// 按操作（如 "instance_exists"、"instance_shutdown"、"instance_metadata"）与结果
	// （"success"、"error"、"instance_not_found"、"not_implemented"、"canceled"）细分。
	cloudProviderCalls = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      metricsSubsystem,
			Name:           "cloud_provider_calls_total",
			Help:           "The total number of cloud provider API calls, labeled by operation and result.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation", "result"},
	)
)

var metricRegistration sync.Once

// registerMetrics 注册本控制器需要上报的指标。
// 通过 sync.Once 保证全局注册只执行一次，重复调用是安全的。
func registerMetrics() {
	metricRegistration.Do(func() {
		legacyregistry.MustRegister(monitorNodesDuration)
		legacyregistry.MustRegister(cloudProviderCalls)
	})
}
