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

package node

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	// 指标名统一挂在 node_controller 子系统下，便于按控制器聚合。
	metricsSubsystem = "node_controller"
)

var (
	// 记录从节点创建到移除 cloud-provider taint 的延迟，用于衡量节点初始化速度；
	// 指数桶覆盖 1s 到约 17m，兼顾快速与慢速云环境。
	removeCloudProviderTaintDelay = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      metricsSubsystem,
			Name:           "cloud_provider_taint_removal_delay_seconds",
			Help:           "Number of seconds after node creation when NodeController removed the cloud-provider taint of a single node.",
			Buckets:        metrics.ExponentialBuckets(1, 4, 6), // 1s -> ~17m
			StabilityLevel: metrics.ALPHA,
		},
	)
	// 记录节点首次同步完成所需时间，反映云 API 与节点注册的整体时延。
	initialNodeSyncDelay = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      metricsSubsystem,
			Name:           "initial_node_sync_delay_seconds",
			Help:           "Number of seconds after node creation when NodeController finished the initial synchronization of a single node.",
			Buckets:        metrics.ExponentialBuckets(1, 4, 6), // 1s -> ~17m
			StabilityLevel: metrics.ALPHA,
		},
	)
)

var metricRegistration sync.Once

// registerMetrics 注册本控制器需要上报的指标。
// 使用 sync.Once 保证重复创建控制器时不会因重复注册而 panic。
func registerMetrics() {
	metricRegistration.Do(func() {
		legacyregistry.MustRegister(removeCloudProviderTaintDelay)
		legacyregistry.MustRegister(initialNodeSyncDelay)
	})
}
