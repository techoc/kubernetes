/*
Copyright 2020 The Kubernetes Authors.

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

package service

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	// subSystemName 是 Prometheus 指标中使用的子系统名，与控制器名称保持一致，
	// 指标形如 service_controller_loadbalancer_sync_total。
	subSystemName = "service_controller"
)

// register 保证指标只向全局 registry 注册一次。
// New 可能被重复调用（例如测试），重复注册会 panic，因此用 sync.Once 保护。
var register sync.Once

// registerMetrics 注册 Service 控制器的指标。
func registerMetrics() {
	register.Do(func() {
		legacyregistry.MustRegister(loadBalancerSyncCount)
		legacyregistry.MustRegister(nodeSyncLatency)
		legacyregistry.MustRegister(nodeSyncErrorCount)
		legacyregistry.MustRegister(updateLoadBalancerHostLatency)
	})
}

var (
	// loadBalancerSyncCount 统计负载均衡器被重新配置的次数（由 Service/Node 变更触发）。
	loadBalancerSyncCount = metrics.NewCounter(&metrics.CounterOpts{
		Name:           "loadbalancer_sync_total",
		Subsystem:      subSystemName,
		Help:           "A metric counting the amount of times any load balancer has been configured, as an effect of service/node changes on the cluster",
		StabilityLevel: metrics.ALPHA,
	})
	// nodeSyncErrorCount 统计节点变化触发的负载均衡器同步失败次数。
	nodeSyncErrorCount = metrics.NewCounter(&metrics.CounterOpts{
		Name:           "nodesync_error_total",
		Subsystem:      subSystemName,
		Help:           "A metric counting the amount of times any load balancer has been configured and errored, as an effect of node changes on the cluster",
		StabilityLevel: metrics.ALPHA,
	})
	nodeSyncLatency = metrics.NewHistogram(&metrics.HistogramOpts{
		Name:      "nodesync_latency_seconds",
		Subsystem: subSystemName,
		Help:      "A metric measuring the latency for nodesync which updates loadbalancer hosts on cluster node updates.",
		// 分桶从 1s 到 16384s：云侧后端更新可能耗时数分钟，指数桶覆盖 1s~4.5h，
		// 便于观察长尾。
		Buckets:        metrics.ExponentialBuckets(1, 2, 15),
		StabilityLevel: metrics.ALPHA,
	})
	updateLoadBalancerHostLatency = metrics.NewHistogram(&metrics.HistogramOpts{
		Name:      "update_loadbalancer_host_latency_seconds",
		Subsystem: subSystemName,
		Help:      "A metric measuring the latency for updating each load balancer hosts.",
		// 桶覆盖 1 秒到 16384 秒。
		Buckets:        metrics.ExponentialBuckets(1, 2, 15),
		StabilityLevel: metrics.ALPHA,
	})
)
