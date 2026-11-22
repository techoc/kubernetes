/*
Copyright 2015 The Kubernetes Authors.

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

// Package metrics 暴露 etcd3 存储后端的指标：请求延迟/计数/错误、对象数量与体积估算、
// 事件与 bookmark 计数、解码错误、LIST 延迟，以及 etcd 存储文件大小。
// 这些指标是 apiserver SLO 与容量规划的主要数据来源。
package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	storagemetrics "k8s.io/apiserver/pkg/storage/metrics"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
)

/*
 * By default, all the following metrics are defined as falling under
 * ALPHA stability level https://github.com/kubernetes/enhancements/blob/master/keps/sig-instrumentation/1209-metrics-stability/kubernetes-control-plane-metrics-stability.md#stability-classes)
 *
 * Promoting the stability level of the metric is a responsibility of the component owner, since it
 * involves explicitly acknowledging support for the metric across multiple releases, in accordance with
 * the metric stability policy.
 */
var (
	etcdRequestLatency = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Name: "etcd_request_duration_seconds",
			Help: "Etcd request latency in seconds for each operation and object type.",
			// Etcd request latency in seconds for each operation and object type.
			// This metric is used for verifying etcd api call latencies SLO
			// keep consistent with apiserver metric 'requestLatencies' in
			// staging/src/k8s.io/apiserver/pkg/endpoints/metrics/metrics.go
			Buckets: []float64{0.005, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2, 3,
				4, 5, 6, 8, 10, 15, 20, 30, 45, 60},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"operation", "group", "resource"},
	)
	etcdRequestCounts = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Name:           "etcd_requests_total",
			Help:           "Etcd request counts for each operation and object type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"operation", "group", "resource"},
	)
	etcdRequestErrorCounts = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Name:           "etcd_request_errors_total",
			Help:           "Etcd failed request counts for each operation and object type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"operation", "group", "resource"},
	)
	objectCounts = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:              "apiserver_storage_objects",
			Help:              "[DEPRECATED, consider using apiserver_resource_objects instead] Number of stored objects at the time of last check split by kind. In case of a fetching error, the value will be -1.",
			StabilityLevel:    compbasemetrics.STABLE,
			DeprecatedVersion: "1.34.0",
		},
		[]string{"resource"},
	)
	resourceSizeEstimate = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "apiserver_resource_size_estimate_bytes",
			Help:           "Estimated size of stored objects in database. Estimate is based on sum of last observed sizes of serialized objects. In case of a fetching error, the value will be -1.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)
	newObjectCounts = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:           "apiserver_resource_objects",
			Help:           "Number of stored objects at the time of last check split by kind. In case of a fetching error, the value will be -1.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)
	storageSizeDescription   = compbasemetrics.NewDesc("apiserver_storage_size_bytes", "Size of the storage database file physically allocated in bytes.", []string{"storage_cluster_id"}, nil, compbasemetrics.STABLE, "")
	storageMonitor           = &monitorCollector{monitorGetter: func() ([]Monitor, error) { return nil, nil }}
	etcdEventsReceivedCounts = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Subsystem:      "apiserver",
			Name:           "storage_events_received_total",
			Help:           "Number of etcd events received split by kind.",
			StabilityLevel: compbasemetrics.BETA,
		},
		[]string{"group", "resource"},
	)
	etcdBookmarkCounts = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Name:              "etcd_bookmark_counts",
			Help:              "Number of etcd bookmarks (progress notify events) split by kind.",
			StabilityLevel:    compbasemetrics.ALPHA,
			DeprecatedVersion: "1.36.0",
		},
		[]string{"group", "resource"},
	)
	etcdBookmarkTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Name:           "etcd_bookmark_total",
			Help:           "Number of etcd bookmarks (progress notify events) split by kind.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)
	etcdLeaseObjectCounts = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Name:           "etcd_lease_object_counts",
			Help:           "Number of objects attached to a single etcd lease.",
			Buckets:        []float64{10, 50, 100, 500, 1000, 2500, 5000},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{},
	)
	decodeErrorCounts = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      "apiserver",
			Name:           "storage_decode_errors_total",
			Help:           "Number of stored object decode errors split by object type",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)
	listLatency = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Namespace: "apiserver",
			Name:      "storage_list_duration_seconds",
			Help:      "Latency of the storage layer GetList call in seconds, including object decode, split by whether etcd RangeStream was used.",
			Buckets: []float64{0.005, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2, 3,
				4, 5, 6, 8, 10, 15, 20, 30, 45, 60},
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"streamed", "group", "resource"},
	)
)

var registerMetrics sync.Once

// Register all metrics.
// 幂等注册：多个入口（如 /metrics 路由与测试）都可能调用，重复注册会 panic。
func Register() {
	// Register the metrics.
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(etcdRequestLatency)
		legacyregistry.MustRegister(etcdRequestCounts)
		legacyregistry.MustRegister(etcdRequestErrorCounts)
		legacyregistry.MustRegister(objectCounts)
		legacyregistry.MustRegister(resourceSizeEstimate)
		legacyregistry.MustRegister(newObjectCounts)
		legacyregistry.CustomMustRegister(storageMonitor)
		legacyregistry.MustRegister(etcdEventsReceivedCounts)
		legacyregistry.MustRegister(etcdBookmarkCounts)
		legacyregistry.MustRegister(etcdBookmarkTotal)
		legacyregistry.MustRegister(etcdLeaseObjectCounts)
		legacyregistry.MustRegister(decodeErrorCounts)
		legacyregistry.MustRegister(listLatency)
	})
}

// UpdateStoreStats sets the stats metrics.
// 获取统计失败时写入 -1 作为哨兵值，让告警规则能区分“0 个对象”和“读取失败”。
func UpdateStoreStats(groupResource schema.GroupResource, stats storage.Stats, err error) {
	if err != nil {
		objectCounts.WithLabelValues(groupResource.String()).Set(-1)
		newObjectCounts.WithLabelValues(groupResource.Group, groupResource.Resource).Set(-1)
		if utilfeature.DefaultFeatureGate.Enabled(features.SizeBasedListCostEstimate) {
			resourceSizeEstimate.WithLabelValues(groupResource.Group, groupResource.Resource).Set(-1)
		}
		return
	}
	objectCounts.WithLabelValues(groupResource.String()).Set(float64(stats.ObjectCount))
	newObjectCounts.WithLabelValues(groupResource.Group, groupResource.Resource).Set(float64(stats.ObjectCount))
	if utilfeature.DefaultFeatureGate.Enabled(features.SizeBasedListCostEstimate) {
		// 有对象却估算平均大小为 0，说明统计不完整，同样记为 -1，避免低估 LIST 成本。
		if stats.ObjectCount > 0 && stats.EstimatedAverageObjectSizeBytes == 0 {
			resourceSizeEstimate.WithLabelValues(groupResource.Group, groupResource.Resource).Set(-1)
		} else {
			resourceSizeEstimate.WithLabelValues(groupResource.Group, groupResource.Resource).Set(float64(stats.EstimatedAverageObjectSizeBytes * stats.ObjectCount))
		}
	}
}

// DeleteStoreStats delete the stats metrics.
// 资源被删除时同步清理标签值，防止指标基数无限增长。
func DeleteStoreStats(groupResource schema.GroupResource) {
	objectCounts.Delete(map[string]string{"resource": groupResource.String()})
	newObjectCounts.Delete(map[string]string{"group": groupResource.Group, "resource": groupResource.Resource})
	if utilfeature.DefaultFeatureGate.Enabled(features.SizeBasedListCostEstimate) {
		resourceSizeEstimate.DeleteLabelValues(groupResource.Group, groupResource.Resource)
	}
}

// RecordEtcdRequest updates and sets the etcd_request_duration_seconds,
// etcd_request_total, etcd_request_errors_total metrics.
// 无论成败都记录延迟与计数，失败时额外累加错误计数。
func RecordEtcdRequest(verb string, groupResource schema.GroupResource, err error, startTime time.Time) {
	etcdRequestLatency.WithLabelValues(verb, groupResource.Group, groupResource.Resource).Observe(sinceInSeconds(startTime))
	etcdRequestCounts.WithLabelValues(verb, groupResource.Group, groupResource.Resource).Inc()
	if err != nil {
		etcdRequestErrorCounts.WithLabelValues(verb, groupResource.Group, groupResource.Resource).Inc()
	}
}

// RecordListLatency sets the storage_list_duration_seconds metric.
// streamed 标签区分是否走 etcd RangeStream，便于比较两种读取路径的性能。
func RecordListLatency(groupResource schema.GroupResource, streamed bool, startTime time.Time) {
	streamedLabel := "false"
	if streamed {
		streamedLabel = "true"
	}
	listLatency.WithLabelValues(streamedLabel, groupResource.Group, groupResource.Resource).Observe(sinceInSeconds(startTime))
}

// RecordEtcdEvent updated the etcd_events_received_total metric.
func RecordEtcdEvent(groupResource schema.GroupResource) {
	etcdEventsReceivedCounts.WithLabelValues(groupResource.Group, groupResource.Resource).Inc()
}

// RecordEtcdBookmark updates the etcd_bookmark_total metric.
// 同时更新已弃用的 gauge 与新的 counter，兼容旧仪表盘与告警。
func RecordEtcdBookmark(groupResource schema.GroupResource) {
	etcdBookmarkCounts.WithLabelValues(groupResource.Group, groupResource.Resource).Inc()
	etcdBookmarkTotal.WithLabelValues(groupResource.Group, groupResource.Resource).Inc()
}

// RecordDecodeError sets the storage_decode_errors metrics.
func RecordDecodeError(groupResource schema.GroupResource) {
	decodeErrorCounts.WithLabelValues(groupResource.Group, groupResource.Resource).Inc()
}

// Reset resets the etcd_request_duration_seconds metric.
func Reset() {
	etcdRequestLatency.Reset()
}

// sinceInSeconds gets the time since the specified start in seconds.
//
// This is a variable to facilitate testing.
// 用变量而非函数，便于测试替换为确定性实现，避免依赖真实时间。
var sinceInSeconds = func(start time.Time) float64 {
	return time.Since(start).Seconds()
}

// SetStorageMonitorGetter sets monitor getter to allow monitoring etcd stats.
// 通过 getter 间接访问 monitor，避免 metrics 包直接依赖 etcd 客户端实现。
func SetStorageMonitorGetter(getter func() ([]Monitor, error)) {
	storageMonitor.setGetter(getter)
}

// UpdateLeaseObjectCount sets the etcd_lease_object_counts metric.
func UpdateLeaseObjectCount(count int64) {
	// Currently we only store one previous lease, since all the events have the same ttl.
	// See pkg/storage/etcd3/lease_manager.go
	etcdLeaseObjectCounts.WithLabelValues().Observe(float64(count))
}

// RecordStorageListMetrics notes various metrics of the cost to serve a LIST request.
// 委托给通用 storage metrics，后端标识固定为 etcd。
func RecordStorageListMetrics(groupResource schema.GroupResource, index string, numFetched, numEvald, numReturned int) {
	storagemetrics.RecordStorageListMetrics(groupResource, storagemetrics.StorageBackendEtcd, index, numFetched, numEvald, numReturned)
}

// Monitor 抽象对存储后端的运行状态探测（当前仅存储文件大小）。
type Monitor interface {
	Monitor(ctx context.Context) (StorageMetrics, error)
	Close() error
}

// StorageMetrics 是 Monitor 返回的快照。
type StorageMetrics struct {
	Size int64
}

// monitorCollector 在 Prometheus 抓取时实时查询各 monitor；
// 抓取发生在独立 goroutine，因此 getter 用互斥锁保护。
type monitorCollector struct {
	compbasemetrics.BaseStableCollector

	mutex         sync.Mutex
	monitorGetter func() ([]Monitor, error)
}

// setGetter 替换 monitor 提供者；注册后由外部组件注入实际实现。
func (m *monitorCollector) setGetter(monitorGetter func() ([]Monitor, error)) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.monitorGetter = monitorGetter
}

// getGetter 读取 monitor 提供者。
func (m *monitorCollector) getGetter() func() ([]Monitor, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.monitorGetter
}

// DescribeWithStability implements compbasemetrics.StableColletor
func (c *monitorCollector) DescribeWithStability(ch chan<- *compbasemetrics.Desc) {
	ch <- storageSizeDescription
}

// CollectWithStability implements compbasemetrics.StableColletor
// 抓取时对每个 monitor 单独设置 1 秒超时：某个 etcd 实例无响应不应拖垮整个 /metrics 端点。
func (c *monitorCollector) CollectWithStability(ch chan<- compbasemetrics.Metric) {
	monitors, err := c.getGetter()()
	if err != nil {
		return
	}

	for i, m := range monitors {
		// 存储集群 ID 由下标生成，因为 metrics 层不感知 etcd 成员标识。
		storageClusterID := fmt.Sprintf("etcd-%d", i)

		klog.V(4).InfoS("Start collecting storage metrics", "storage_cluster_id", storageClusterID)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		metrics, err := m.Monitor(ctx)
		cancel()
		if err != nil {
			klog.InfoS("Failed to get storage metrics", "storage_cluster_id", storageClusterID, "err", err)
			continue
		}

		metric, err := compbasemetrics.NewConstMetric(storageSizeDescription, compbasemetrics.GaugeValue, float64(metrics.Size), storageClusterID)
		if err != nil {
			klog.ErrorS(err, "Failed to create metric", "storage_cluster_id", storageClusterID)
		}
		ch <- metric
	}
}
