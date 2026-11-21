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

package cidrset

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// nodeIpamSubsystem 是本包所有指标使用的 Prometheus 子系统前缀。
const nodeIpamSubsystem = "node_ipam_controller"

var (
	// cidrSetAllocations 统计 CIDR 分配的总次数，按集群 CIDR 区分。
	cidrSetAllocations = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      nodeIpamSubsystem,
			Name:           "cidrset_cidrs_allocations_total",
			Help:           "Counter measuring total number of CIDR allocations.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"clusterCIDR"},
	)
	// cidrSetReleases 统计 CIDR 释放的总次数，按集群 CIDR 区分。
	cidrSetReleases = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      nodeIpamSubsystem,
			Name:           "cidrset_cidrs_releases_total",
			Help:           "Counter measuring total number of CIDR releases.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"clusterCIDR"},
	)
	// 使用 Gauge 而非 Counter：理论上 CIDR 上限可以增加或减少。
	cidrSetMaxCidrs = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      nodeIpamSubsystem,
			Name:           "cirdset_max_cidrs",
			Help:           "Maximum number of CIDRs that can be allocated.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"clusterCIDR"},
	)
	// cidrSetUsage 记录已分配 CIDR 占总量的百分比。
	cidrSetUsage = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      nodeIpamSubsystem,
			Name:           "cidrset_usage_cidrs",
			Help:           "Gauge measuring percentage of allocated CIDRs.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"clusterCIDR"},
	)
	// cidrSetAllocationTriesPerRequest 记录单次分配请求的尝试次数分布，便于发现 CIDR 碎片化问题。
	cidrSetAllocationTriesPerRequest = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      nodeIpamSubsystem,
			Name:           "cidrset_allocation_tries_per_request",
			Help:           "Number of endpoints added on each Service sync",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(1, 5, 5),
		},
		[]string{"clusterCIDR"},
	)
)

// registerMetrics 保证指标只注册一次；重复注册会导致 panic。
var registerMetrics sync.Once

// registerCidrsetMetrics 将 CIDR 集合相关的指标注册到 legacy registry。
func registerCidrsetMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(cidrSetAllocations)
		legacyregistry.MustRegister(cidrSetReleases)
		legacyregistry.MustRegister(cidrSetMaxCidrs)
		legacyregistry.MustRegister(cidrSetUsage)
		legacyregistry.MustRegister(cidrSetAllocationTriesPerRequest)
	})
}
