/*
Copyright 2017 The Kubernetes Authors.

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
	nodeControllerSubsystem = "node_collector"
	zoneHealthStatisticKey  = "zone_health"
	zoneSizeKey             = "zone_size"
	zoneNoUnhealthyNodesKey = "unhealthy_nodes_in_zone"
	evictionsTotalKey       = "evictions_total"

	updateNodeHealthKey     = "update_node_health_duration_seconds"
	updateAllNodesHealthKey = "update_all_nodes_health_duration_seconds"
	// 以上键名同时用于指标名称与 legacy registry 的注册，命名变更会影响监控查询。
)

var (
	// zoneHealth 表示每个可用区中健康节点的百分比。
	zoneHealth = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      nodeControllerSubsystem,
			Name:           zoneHealthStatisticKey,
			Help:           "Gauge measuring percentage of healthy nodes per zone.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"zone"},
	)
	// zoneSize 表示每个可用区中已注册的节点数。
	zoneSize = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      nodeControllerSubsystem,
			Name:           zoneSizeKey,
			Help:           "Gauge measuring number of registered Nodes per zones.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"zone"},
	)
	// unhealthyNodes 表示每个可用区中非 Ready 状态的节点数。
	unhealthyNodes = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      nodeControllerSubsystem,
			Name:           zoneNoUnhealthyNodesKey,
			Help:           "Gauge measuring number of not Ready Nodes per zones.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"zone"},
	)
	// evictionsTotal 记录节点生命周期控制器启动以来发生的节点驱逐次数。
	evictionsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      nodeControllerSubsystem,
			Name:           evictionsTotalKey,
			Help:           "Number of Node evictions that happened since current instance of NodeController started.",
			StabilityLevel: metrics.STABLE,
		},
		[]string{"zone"},
	)

	// updateNodeHealthDuration 统计更新单个节点健康状态所需的时间。
	updateNodeHealthDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      nodeControllerSubsystem,
			Name:           updateNodeHealthKey,
			Help:           "Duration in seconds for NodeController to update the health of a single node.",
			Buckets:        metrics.ExponentialBuckets(0.001, 4, 8), // 1ms -> ~15s
			StabilityLevel: metrics.ALPHA,
		},
	)
	// updateAllNodesHealthDuration 统计更新全部节点健康状态所需的时间。
	updateAllNodesHealthDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      nodeControllerSubsystem,
			Name:           updateAllNodesHealthKey,
			Help:           "Duration in seconds for NodeController to update the health of all nodes.",
			Buckets:        metrics.ExponentialBuckets(0.01, 4, 8), // 10ms -> ~3m
			StabilityLevel: metrics.ALPHA,
		},
	)
)

var registerMetrics sync.Once

// Register 将需要监控的指标注册到 legacy registry。
// sync.Once 确保即使多个调用方触发注册，也不会出现重复注册导致的 panic。
func Register() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(zoneHealth)
		legacyregistry.MustRegister(zoneSize)
		legacyregistry.MustRegister(unhealthyNodes)
		legacyregistry.MustRegister(evictionsTotal)
		legacyregistry.MustRegister(updateNodeHealthDuration)
		legacyregistry.MustRegister(updateAllNodesHealthDuration)
	})
}
