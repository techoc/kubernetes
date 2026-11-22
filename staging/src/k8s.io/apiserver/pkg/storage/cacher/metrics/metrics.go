/*
Copyright 2021 The Kubernetes Authors.

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

// Package metrics 定义 watch cache 的可观测指标：初始化耗时、事件收发与分发、
// 容量变化、一致性读以及 watch 分片等。所有指标均为 ALPHA 稳定性。
package metrics

import (
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/features"
	storagemetrics "k8s.io/apiserver/pkg/storage/metrics"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	namespace = "apiserver"
	subsystem = "watch_cache"
)

// DispatchStage identifies a single stage of an event's lifecycle as it moves
// through the watch cache dispatch pipeline. It is used as the "stage" label
// value on the dispatchStageDuration metric.
//
// StageTotal is the end-to-end latency of a successfully delivered event.
// The remaining stages measure individual segments of that path.
//
// 分段设计便于定位瓶颈：storage_to_cache 高说明 etcd 解码或反射器处理慢，
// cache_to_watcher 高说明下游客户端消费不过来（背压）。
type DispatchStage int

const (
	// StageTotal: end-to-end, etcd decode -> written to the result channel.
	StageTotal DispatchStage = iota

	// StageStorageToCache: event decoded from etcd -> event received by cacher.
	// Captures the delay between when an event is decoded from the storage backend
	// and when it is first processed by the cacher's reflector loop.
	StageStorageToCache

	// StageCacheToWatcher: watch.Event built -> written to watcher's result channel.
	// Captures time spent blocked handing the event off to the client,
	// i.e. downstream (result channel) backpressure.
	StageCacheToWatcher

	numDispatchStages
)

var dispatchStageName = [numDispatchStages]string{
	StageTotal:          "total",
	StageStorageToCache: "storage_to_cache",
	StageCacheToWatcher: "cache_to_watcher",
}

/*
 * By default, all the following metrics are defined as falling under
 * ALPHA stability level https://github.com/kubernetes/enhancements/blob/master/keps/sig-instrumentation/1209-metrics-stability/kubernetes-control-plane-metrics-stability.md#stability-classes)
 *
 * Promoting the stability level of the metric is a responsibility of the component owner, since it
 * involves explicitly acknowledging support for the metric across multiple releases, in accordance with
 * the metric stability policy.
 */
var (
	// 注意：cache_list_* 系列已标记 DeprecatedVersion 1.37.0，
	// 由 storage 层统一的指标替代；保留是为了兼容现有监控面板。
	listCacheCount = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:         namespace,
			Name:              "cache_list_total",
			Help:              "Number of LIST requests served from watch cache",
			StabilityLevel:    compbasemetrics.ALPHA,
			DeprecatedVersion: "1.37.0",
		},
		[]string{"group", "resource", "index"},
	)
	listCacheNumFetched = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:         namespace,
			Name:              "cache_list_fetched_objects_total",
			Help:              "Number of objects read from watch cache in the course of serving a LIST request",
			StabilityLevel:    compbasemetrics.ALPHA,
			DeprecatedVersion: "1.37.0",
		},
		[]string{"group", "resource", "index"},
	)
	listCacheNumReturned = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:         namespace,
			Name:              "cache_list_returned_objects_total",
			Help:              "Number of objects returned for a LIST request from watch cache",
			StabilityLevel:    compbasemetrics.ALPHA,
			DeprecatedVersion: "1.37.0",
		},
		[]string{"group", "resource"},
	)
	InitCounter = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Name:           "init_events_total",
			Help:           "Counter of init events processed in watch cache broken by resource type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	EventsReceivedCounter = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "events_received_total",
			Help:           "Counter of events received in watch cache broken by resource type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	EventsCounter = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "events_dispatched_total",
			Help:           "Counter of events dispatched in watch cache broken by resource type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	TerminatedWatchersCounter = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Name:           "terminated_watchers_total",
			Help:           "Counter of watchers closed due to unresponsiveness broken by resource type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	watchCacheResourceVersion = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "resource_version",
			Help:           "Current resource version of watch cache broken by resource type. This is truncated to the 15 least significant digits.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	watchCacheCapacityIncreaseTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Subsystem:      subsystem,
			Name:           "capacity_increase_total",
			Help:           "Total number of watch cache capacity increase events broken by resource type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	watchCacheCapacityDecreaseTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Subsystem:      subsystem,
			Name:           "capacity_decrease_total",
			Help:           "Total number of watch cache capacity decrease events broken by resource type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	WatchCacheCapacity = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Subsystem:      subsystem,
			Name:           "capacity",
			Help:           "Total capacity of watch cache broken by resource type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	WatchCacheInitializations = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "initializations_total",
			Help:           "Counter of watch cache initializations broken by resource type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	WatchCacheInitializationErrors = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "initialization_errors_total",
			Help:           "Counter of watch cache initialization errors broken by resource type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	WatchCacheInitializationDuration = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "initialization_duration_seconds",
			Help:           "Histogram of watch cache initialization duration in seconds, broken by resource type.",
			StabilityLevel: compbasemetrics.ALPHA,
			Buckets:        []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 180, 600},
		},
		[]string{"group", "resource"},
	)

	WatchCacheReadWait = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "read_wait_seconds",
			Help:           "Histogram of time spent waiting for a watch cache to become fresh.",
			StabilityLevel: compbasemetrics.ALPHA,
			Buckets:        []float64{0.005, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.25, 1.5, 2, 3},
		}, []string{"group", "resource"})

	ConsistentReadTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "consistent_read_total",
			Help:           "Counter for consistent reads from cache.",
			StabilityLevel: compbasemetrics.ALPHA,
		}, []string{"group", "resource", "success", "fallback"})

	StorageConsistencyCheckTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Name:           "storage_consistency_checks_total",
			Help:           "Counter for status of consistency checks between etcd and watch cache",
			StabilityLevel: compbasemetrics.ALPHA,
		}, []string{"group", "resource", "status"})

	WatchShardsTotal = compbasemetrics.NewGaugeVec(
		&compbasemetrics.GaugeOpts{
			Namespace:      namespace,
			Name:           "watch_shards_total",
			Help:           "Number of active sharded watch connections broken by resource type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	WatchFilteredEventsTotal = compbasemetrics.NewCounterVec(
		&compbasemetrics.CounterOpts{
			Namespace:      namespace,
			Name:           "watch_filtered_events_total",
			Help:           "Counter of events filtered out by shard selector during watch dispatch, broken by resource type.",
			StabilityLevel: compbasemetrics.ALPHA,
		},
		[]string{"group", "resource"},
	)

	DispatchStageDuration = compbasemetrics.NewHistogramVec(
		&compbasemetrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      "watch_events",
			Name:           "dispatch_duration_seconds",
			Help:           "Histogram of watch event dispatch latency broken by resource type and pipeline stage. The 'total' stage is the end-to-end latency of a delivered event.",
			StabilityLevel: compbasemetrics.ALPHA,
			Buckets:        []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"group", "resource", "stage"})
)

var registerMetrics sync.Once

// Register all metrics.
//
// 使用 sync.Once 保证重复调用安全；分片相关指标只在特性门控开启时注册，
// 否则会在指标暴露时凭空多出恒为零的 series。
func Register() {
	// Register the metrics.
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(listCacheCount)
		legacyregistry.MustRegister(listCacheNumFetched)
		legacyregistry.MustRegister(listCacheNumReturned)
		legacyregistry.MustRegister(InitCounter)
		legacyregistry.MustRegister(EventsReceivedCounter)
		legacyregistry.MustRegister(EventsCounter)
		legacyregistry.MustRegister(TerminatedWatchersCounter)
		legacyregistry.MustRegister(watchCacheResourceVersion)
		legacyregistry.MustRegister(watchCacheCapacityIncreaseTotal)
		legacyregistry.MustRegister(watchCacheCapacityDecreaseTotal)
		legacyregistry.MustRegister(WatchCacheCapacity)
		legacyregistry.MustRegister(WatchCacheInitializations)
		legacyregistry.MustRegister(WatchCacheInitializationErrors)
		legacyregistry.MustRegister(WatchCacheInitializationDuration)
		legacyregistry.MustRegister(WatchCacheReadWait)
		legacyregistry.MustRegister(ConsistentReadTotal)
		legacyregistry.MustRegister(StorageConsistencyCheckTotal)
		if utilfeature.DefaultFeatureGate.Enabled(features.ShardedListAndWatch) {
			legacyregistry.MustRegister(WatchShardsTotal)
			legacyregistry.MustRegister(WatchFilteredEventsTotal)
		}
		legacyregistry.MustRegister(DispatchStageDuration)
	})
}

// RecordListCacheMetrics notes various metrics of the cost to serve a LIST request
func RecordListCacheMetrics(groupResource schema.GroupResource, indexName string, numFetched, numReturned int) {
	listCacheCount.WithLabelValues(groupResource.Group, groupResource.Resource, indexName).Inc()
	listCacheNumFetched.WithLabelValues(groupResource.Group, groupResource.Resource, indexName).Add(float64(numFetched))
	listCacheNumReturned.WithLabelValues(groupResource.Group, groupResource.Resource).Add(float64(numReturned))
	storagemetrics.RecordStorageListMetrics(groupResource, storagemetrics.StorageBackendWatchCache, indexName, numFetched, 0, numReturned)
}

// RecordResourceVersion sets the current resource version for a given resource type.
// The resource version is truncated to the 15 least significant digits to prevent
// the metric from growing indefinitely and losing precision when it exceeds 2^53-1.
//
// 指标只用于观察增长趋势，取模不影响该用途。
func RecordResourceVersion(groupResource schema.GroupResource, resourceVersion uint64) {
	watchCacheResourceVersion.WithLabelValues(groupResource.Group, groupResource.Resource).Set(float64(resourceVersion % 1000000000000000))
}

// RecordShardedWatchStarted increments the active sharded watch gauge for the given resource.
// 必须与 RecordShardedWatchStopped 成对调用，否则 gauge 会漂移。
func RecordShardedWatchStarted(groupResource schema.GroupResource) {
	WatchShardsTotal.WithLabelValues(groupResource.Group, groupResource.Resource).Inc()
}

// RecordShardedWatchStopped decrements the active sharded watch gauge for the given resource.
func RecordShardedWatchStopped(groupResource schema.GroupResource) {
	WatchShardsTotal.WithLabelValues(groupResource.Group, groupResource.Resource).Dec()
}

// RecordWatchFilteredEvent increments the counter for events filtered by shard selector.
func RecordWatchFilteredEvent(groupResource schema.GroupResource) {
	WatchFilteredEventsTotal.WithLabelValues(groupResource.Group, groupResource.Resource).Inc()
}

// RecordsWatchCacheCapacityChange records watchCache capacity resize(increase or decrease) operations.
func RecordsWatchCacheCapacityChange(groupResource schema.GroupResource, old, new int) {
	WatchCacheCapacity.WithLabelValues(groupResource.Group, groupResource.Resource).Set(float64(new))
	if old < new {
		watchCacheCapacityIncreaseTotal.WithLabelValues(groupResource.Group, groupResource.Resource).Inc()
		return
	}
	watchCacheCapacityDecreaseTotal.WithLabelValues(groupResource.Group, groupResource.Resource).Inc()
}

// WatcherMetricsObservers holds pre-resolved (group, resource) observers for
// every dispatch stage, so the hot path never touches the label map.
//
// 预解析（WithLabelValues）把标签查找从每次事件分发移到连接建立时，
// 这对高频 watch 事件路径是显著优化。
type WatcherMetricsObservers struct {
	stageDurations [numDispatchStages]compbasemetrics.ObserverMetric
}

// NewWatcherMetricsObservers creates a pre-resolved metrics observer for watch connections.
func NewWatcherMetricsObservers(groupResource schema.GroupResource) *WatcherMetricsObservers {
	o := &WatcherMetricsObservers{}
	for s := range numDispatchStages {
		o.stageDurations[s] = DispatchStageDuration.WithLabelValues(groupResource.Group, groupResource.Resource, dispatchStageName[s])
	}
	return o
}

// ObserveStage records the duration spent in the given dispatch stage.
func (d *WatcherMetricsObservers) ObserveStage(stage DispatchStage, duration time.Duration) {
	// 越界的 stage 直接忽略，避免热路径 panic。
	if stage < 0 || stage >= numDispatchStages {
		return
	}
	observe(d.stageDurations[stage], duration)
}

// observe 把负时长钳制为 0：时钟回拨或计算误差可能产生负值，
// 直方图不接受负数。
func observe(m compbasemetrics.ObserverMetric, duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	m.Observe(duration.Seconds())
}

// noopObserver 用于未启用指标或测试场景，避免调用方到处判空。
type noopObserver struct{}

func (noopObserver) Observe(float64) {}

var noopObs noopObserver

// NewNoopWatcherMetricsObservers returns a metrics observers struct that does nothing.
func NewNoopWatcherMetricsObservers() *WatcherMetricsObservers {
	o := &WatcherMetricsObservers{}
	for s := range o.stageDurations {
		o.stageDurations[s] = noopObs
	}
	return o
}
