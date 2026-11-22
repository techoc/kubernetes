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

// Package fifo 把 client-go informer 的 FIFO 队列指标接入 Kubernetes 的
// legacyregistry，包括排队数量、处理延迟和 store 的 resourceVersion。
// 包被导入时只安装 provider，实际注册延迟到首次创建 informer 时，
// 以确保注册发生在 feature gate 状态传播之后。
package fifo

import (
	"sync"

	"k8s.io/client-go/tools/cache"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	subsystem = "informer"

	// fifoQueuedItems 记录 FIFO 中当前排队的事件数。
	fifoQueuedItems = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Subsystem:      subsystem,
			Name:           "queued_items",
			Help:           "Number of items currently queued in the FIFO.",
			StabilityLevel: k8smetrics.ALPHA,
		},
		[]string{"name", "group", "version", "resource"},
	)
	// fifoProcessingLatency 记录事件出队后的处理耗时，桶按常见处理时间分布设置。
	fifoProcessingLatency = k8smetrics.NewHistogramVec(
		&k8smetrics.HistogramOpts{
			Subsystem:      subsystem,
			Name:           "processing_latency_seconds",
			Help:           "Time taken to process events after popping from the queue.",
			Buckets:        []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			StabilityLevel: k8smetrics.ALPHA,
		},
		[]string{"name", "group", "version", "resource"},
	)
	// storeResourceVersion 以 15 位有效数字暴露 store 的 resourceVersion，
	// 避免 float 精度丢失导致版本号显示错误。
	storeResourceVersion = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Subsystem:      subsystem,
			Name:           "store_resource_version",
			Help:           "The 15 least significant digits of the resource version of the store.",
			StabilityLevel: k8smetrics.ALPHA,
		},
		[]string{"name", "group", "version", "resource"},
	)
	registerOnce sync.Once
)

// init only installs the provider with cache. The actual metric
// registration with legacyregistry is deferred until the first factory
// method is called (which happens at runtime when the first informer is
// constructed). This ensures Create() — and therefore the read of the
// NativeHistograms feature gate inside toPromHistogramOpts — runs after
// ApplyFeatureGates has propagated the gate state.
//
// Register() is still exported and idempotent; callers that invoke it
// directly (instead of relying on first-factory-call activation) get the
// same behaviour as before.
func init() {
	cache.SetInformerMetricsProvider(informerMetricsProvider{})
}

// Register registers FIFO metrics and sets the metrics provider. It is
// safe (and idempotent) to call multiple times. Callers do not normally
// need to invoke this; importing the package and constructing informers
// is sufficient.
func Register() {
	registerOnce.Do(func() {
		legacyregistry.MustRegister(fifoQueuedItems)
		legacyregistry.MustRegister(fifoProcessingLatency)
		legacyregistry.MustRegister(storeResourceVersion)
	})
	// 重复设置 provider 是幂等的，保证直接调用 Register 的旧用法仍有效。
	cache.SetInformerMetricsProvider(informerMetricsProvider{})
}

// informerMetricsProvider 实现 cache 的指标 provider 接口，
// 每个 factory 方法返回绑定到具体 informer 的指标句柄。
type informerMetricsProvider struct{}

// NewQueuedItemMetric 返回指定 informer 的排队数量 gauge。
func (informerMetricsProvider) NewQueuedItemMetric(id cache.InformerNameAndResource) cache.GaugeMetric {
	Register()
	return &reservedGaugeMetric{
		id: id,
		gauge: fifoQueuedItems.WithLabelValues(
			id.Name(),
			id.GroupVersionResource().Group,
			id.GroupVersionResource().Version,
			id.GroupVersionResource().Resource,
		),
	}
}

// NewProcessingLatencyMetric 返回指定 informer 的处理延迟 histogram。
func (informerMetricsProvider) NewProcessingLatencyMetric(id cache.InformerNameAndResource) cache.HistogramMetric {
	Register()
	return &reservedHistogramMetric{
		id: id,
		histogram: fifoProcessingLatency.WithLabelValues(
			id.Name(),
			id.GroupVersionResource().Group,
			id.GroupVersionResource().Version,
			id.GroupVersionResource().Resource,
		),
	}
}

// NewStoreResourceVersionMetric 返回指定 informer 的 store resourceVersion gauge。
func (informerMetricsProvider) NewStoreResourceVersionMetric(id cache.InformerNameAndResource) cache.GaugeMetric {
	Register()
	return &reservedGaugeMetric{
		id: id,
		gauge: storeResourceVersion.WithLabelValues(
			id.Name(),
			id.GroupVersionResource().Group,
			id.GroupVersionResource().Version,
			id.GroupVersionResource().Resource,
		),
	}
}

// reservedGaugeMetric wraps a gauge and only updates it if the identifier
// is still reserved. This supports dynamic informers (e.g., GC, ResourceQuota)
// that may shut down while the process is still running.
type reservedGaugeMetric struct {
	id    cache.InformerNameAndResource
	gauge cache.GaugeMetric
}

// Set 仅在 informer 标识仍被占用时更新，防止已关闭 informer 的指标复活。
func (r *reservedGaugeMetric) Set(value float64) {
	if r.id.Reserved() {
		r.gauge.Set(value)
	}
}

// reservedHistogramMetric wraps a histogram and only updates it if the identifier
// is still reserved. This supports dynamic informers (e.g., GC, ResourceQuota)
// that may shut down while the process is still running.
type reservedHistogramMetric struct {
	id        cache.InformerNameAndResource
	histogram cache.HistogramMetric
}

// Observe 仅在标识仍被占用时记录，理由同上。
func (r *reservedHistogramMetric) Observe(value float64) {
	if r.id.Reserved() {
		r.histogram.Observe(value)
	}
}
