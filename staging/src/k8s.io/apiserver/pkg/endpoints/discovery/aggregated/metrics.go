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

package aggregated

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// subsystem 是聚合发现相关指标的子系统前缀，最终指标名为
// apiserver_aggregator_discovery_*。
const subsystem = "aggregator_discovery"

var (
	// registerPeerMetricsOnce 保证 peer 相关指标只注册一次：它们仅在
	// 对等聚合特性启用时才被注册，而初始化路径可能被多次调用。
	registerPeerMetricsOnce sync.Once
	// regenerationCounter 统计文档重建次数；该值异常增长说明缓存频繁失效。
	regenerationCounter = metrics.NewCounter(
		&metrics.CounterOpts{
			Name:           "aggregation_count_total",
			Subsystem:      subsystem,
			Help:           "Counter of number of times discovery was aggregated",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// PeerAggregatedCacheHitsCounter 统计从对等缓存命中的次数。
	PeerAggregatedCacheHitsCounter = metrics.NewCounter(
		&metrics.CounterOpts{
			Name:           "peer_aggregated_cache_hits_total",
			Subsystem:      subsystem,
			Help:           "Counter of number of times discovery was served from peer-aggregated cache",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// PeerAggregatedCacheMissesCounter 统计需要跨所有 API server 重新聚合的次数。
	PeerAggregatedCacheMissesCounter = metrics.NewCounter(
		&metrics.CounterOpts{
			Name:           "peer_aggregated_cache_misses_total",
			Subsystem:      subsystem,
			Help:           "Counter of number of times discovery was aggregated across all API servers",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// NoPeerDiscoveryRequestCounter 统计客户端明确要求非对等聚合结果的次数。
	NoPeerDiscoveryRequestCounter = metrics.NewCounter(
		&metrics.CounterOpts{
			Name:           "nopeer_requests_total",
			Subsystem:      subsystem,
			Help:           "Counter of number of times no-peer (non peer-aggregated) discovery was requested",
			StabilityLevel: metrics.ALPHA,
		},
	)
)

func init() {
	// 重建计数器无条件注册；peer 指标则延迟注册，避免未启用该特性时暴露空指标。
	legacyregistry.MustRegister(regenerationCounter)
}

// registerPeerMetrics 幂等注册对等聚合指标。
func registerPeerMetrics() {
	registerPeerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(PeerAggregatedCacheHitsCounter)
		legacyregistry.MustRegister(PeerAggregatedCacheMissesCounter)
		legacyregistry.MustRegister(NoPeerDiscoveryRequestCounter)
	})
}
