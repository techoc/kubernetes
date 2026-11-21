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

package metrics

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// EndpointSliceMirroringSubsystem 是 EndpointSliceMirroring 控制器指标使用的子系统名称。
const EndpointSliceMirroringSubsystem = "endpoint_slice_mirroring_controller"

var (
	// EndpointsAddedPerSync 记录每次 Endpoints 同步时新增的端点数量。
	EndpointsAddedPerSync = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      EndpointSliceMirroringSubsystem,
			Name:           "endpoints_added_per_sync",
			Help:           "Number of endpoints added on each Endpoints sync",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(2, 2, 15),
		},
		[]string{},
	)
	// EndpointsUpdatedPerSync 记录每次 Endpoints 同步时更新的端点数量。
	EndpointsUpdatedPerSync = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      EndpointSliceMirroringSubsystem,
			Name:           "endpoints_updated_per_sync",
			Help:           "Number of endpoints updated on each Endpoints sync",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(2, 2, 15),
		},
		[]string{},
	)
	// EndpointsRemovedPerSync 记录每次 Endpoints 同步时移除的端点数量。
	EndpointsRemovedPerSync = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      EndpointSliceMirroringSubsystem,
			Name:           "endpoints_removed_per_sync",
			Help:           "Number of endpoints removed on each Endpoints sync",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(2, 2, 15),
		},
		[]string{},
	)
	// AddressesSkippedPerSync 记录每次同步时因地址非法或超出 MaxEndpointsPerSubset 而被跳过的地址数量。
	AddressesSkippedPerSync = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      EndpointSliceMirroringSubsystem,
			Name:           "addresses_skipped_per_sync",
			Help:           "Number of addresses skipped on each Endpoints sync due to being invalid or exceeding MaxEndpointsPerSubset",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(2, 2, 15),
		},
		[]string{},
	)
	// EndpointsSyncDuration 记录 syncEndpoints() 的执行耗时（秒）。
	EndpointsSyncDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      EndpointSliceMirroringSubsystem,
			Name:           "endpoints_sync_duration",
			Help:           "Duration of syncEndpoints() in seconds",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
		},
		[]string{},
	)
	// EndpointsDesired 记录期望的端点总数。
	EndpointsDesired = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      EndpointSliceMirroringSubsystem,
			Name:           "endpoints_desired",
			Help:           "Number of endpoints desired",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{},
	)
	// NumEndpointSlices 记录集群中 EndpointSlice 的数量。
	NumEndpointSlices = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      EndpointSliceMirroringSubsystem,
			Name:           "num_endpoint_slices",
			Help:           "Number of EndpointSlices",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{},
	)
	// DesiredEndpointSlices 记录理想分配情况下应当存在的 EndpointSlice 数量，用于与实际数量对比评估碎片化程度。
	DesiredEndpointSlices = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      EndpointSliceMirroringSubsystem,
			Name:           "desired_endpoint_slices",
			Help:           "Number of EndpointSlices that would exist with perfect endpoint allocation",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{},
	)
	// EndpointSliceChanges 记录 EndpointSlice 的变更次数，按操作类型（add/update/delete）区分。
	EndpointSliceChanges = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      EndpointSliceMirroringSubsystem,
			Name:           "changes",
			Help:           "Number of EndpointSlice changes",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation"},
	)
)

// registerMetrics 保证指标只注册一次；重复注册会导致 panic。
var registerMetrics sync.Once

// RegisterMetrics 将 EndpointSliceMirroring 控制器的指标注册到 legacy registry。
func RegisterMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(EndpointsAddedPerSync)
		legacyregistry.MustRegister(EndpointsUpdatedPerSync)
		legacyregistry.MustRegister(EndpointsRemovedPerSync)
		legacyregistry.MustRegister(AddressesSkippedPerSync)
		legacyregistry.MustRegister(EndpointsSyncDuration)
		legacyregistry.MustRegister(EndpointsDesired)
		legacyregistry.MustRegister(NumEndpointSlices)
		legacyregistry.MustRegister(DesiredEndpointSlices)
		legacyregistry.MustRegister(EndpointSliceChanges)
	})
}
