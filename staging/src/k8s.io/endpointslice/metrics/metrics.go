/*
Copyright 2019 The Kubernetes Authors.

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

// EndpointSliceSubsystem - subsystem name used for Endpoint Slices.
// EndpointSliceSubsystem 是 EndpointSlice 相关指标使用的子系统名称。
const EndpointSliceSubsystem = "endpoint_slice_controller"

var (
	// EndpointsAddedPerSync tracks the number of endpoints added on each
	// Service sync.
	// EndpointsAddedPerSync 记录每次 Service 同步时新增的端点数量。
	EndpointsAddedPerSync = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      EndpointSliceSubsystem,
			Name:           "endpoints_added_per_sync",
			Help:           "Number of endpoints added on each Service sync",
			StabilityLevel: metrics.BETA,
			Buckets:        metrics.ExponentialBuckets(2, 2, 15),
		},
		[]string{},
	)
	// EndpointsRemovedPerSync tracks the number of endpoints removed on each
	// Service sync.
	// EndpointsRemovedPerSync 记录每次 Service 同步时移除的端点数量。
	EndpointsRemovedPerSync = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      EndpointSliceSubsystem,
			Name:           "endpoints_removed_per_sync",
			Help:           "Number of endpoints removed on each Service sync",
			StabilityLevel: metrics.BETA,
			Buckets:        metrics.ExponentialBuckets(2, 2, 15),
		},
		[]string{},
	)
	// EndpointsDesired tracks the total number of desired endpoints.
	// EndpointsDesired 记录期望的端点总数。
	EndpointsDesired = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      EndpointSliceSubsystem,
			Name:           "endpoints_desired",
			Help:           "Number of endpoints desired",
			StabilityLevel: metrics.BETA,
		},
		[]string{},
	)
	// NumEndpointSlices tracks the number of EndpointSlices in a cluster.
	// NumEndpointSlices 记录集群中 EndpointSlice 的实际数量。
	NumEndpointSlices = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      EndpointSliceSubsystem,
			Name:           "num_endpoint_slices",
			Help:           "Number of EndpointSlices",
			StabilityLevel: metrics.BETA,
		},
		[]string{},
	)
	// DesiredEndpointSlices tracks the number of EndpointSlices that would
	// exist with perfect endpoint allocation.
	// DesiredEndpointSlices 记录在端点完美分配情况下本应存在的 EndpointSlice 数量。
	DesiredEndpointSlices = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      EndpointSliceSubsystem,
			Name:           "desired_endpoint_slices",
			Help:           "Number of EndpointSlices that would exist with perfect endpoint allocation",
			StabilityLevel: metrics.BETA,
		},
		[]string{},
	)

	// EndpointSliceChanges tracks the number of changes to Endpoint Slices.
	// EndpointSliceChanges 记录 EndpointSlice 发生变更的次数，按操作类型（operation）区分。
	EndpointSliceChanges = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      EndpointSliceSubsystem,
			Name:           "changes",
			Help:           "Number of EndpointSlice changes",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation"},
	)

	// EndpointSlicesChangedPerSync observes the number of EndpointSlices
	// changed per sync.
	// EndpointSlicesChangedPerSync 观察每次 Service 同步时变更的 EndpointSlice 数量，按拓扑模式和流量分布区分。
	EndpointSlicesChangedPerSync = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: EndpointSliceSubsystem,
			Name:      "endpointslices_changed_per_sync",
			Help:      "Number of EndpointSlices changed on each Service sync",
		},
		[]string{
			"topology",             // either "Auto" or "Disabled"
			"traffic_distribution", // a trafficDistribution value or <empty>
		},
	)

	// EndpointSliceSyncs tracks the number of sync operations the controller
	// runs along with their result.
	// EndpointSliceSyncs 记录控制器执行的同步操作次数，并按结果（result）标签区分成功、过期或错误。
	EndpointSliceSyncs = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      EndpointSliceSubsystem,
			Name:           "syncs",
			Help:           "Number of EndpointSlice syncs",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"}, // either "success", "stale", or "error"
	)

	// ServicesCountByTrafficDistribution tracks the number of Services using some
	// specific trafficDistribution
	// ServicesCountByTrafficDistribution 记录使用某种特定 trafficDistribution 的 Service 数量。
	ServicesCountByTrafficDistribution = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      EndpointSliceSubsystem,
			Name:           "services_count_by_traffic_distribution",
			Help:           "Number of Services using some specific trafficDistribution",
			StabilityLevel: metrics.BETA,
		},
		[]string{"traffic_distribution"}, // A trafficDistribution value
	)
)

// registerMetrics 保证 RegisterMetrics 只执行一次。
var registerMetrics sync.Once

// RegisterMetrics registers EndpointSlice metrics.
// RegisterMetrics 将本包定义的所有 EndpointSlice 指标注册到 legacyregistry。
func RegisterMetrics() {
	// 借助 sync.Once 保证即使被并发或重复调用，指标也只注册一次（重复注册会 panic）。
	registerMetrics.Do(func() {
		// 逐个注册所有指标向量；MustRegister 在失败时会直接 panic。
		legacyregistry.MustRegister(EndpointsAddedPerSync)
		legacyregistry.MustRegister(EndpointsRemovedPerSync)
		legacyregistry.MustRegister(EndpointsDesired)
		legacyregistry.MustRegister(NumEndpointSlices)
		legacyregistry.MustRegister(DesiredEndpointSlices)
		legacyregistry.MustRegister(EndpointSliceChanges)
		legacyregistry.MustRegister(EndpointSlicesChangedPerSync)
		legacyregistry.MustRegister(EndpointSliceSyncs)
		legacyregistry.MustRegister(ServicesCountByTrafficDistribution)
	})
}
