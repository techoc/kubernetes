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

// Package cache is a client-side caching mechanism. It is useful for
// reducing the number of server calls you'd otherwise need to make.
// Reflector watches a server and updates a Store. Two stores are provided;
// one that simply caches objects (for example, to allow a scheduler to
// list currently available nodes), and one that additionally acts as
// a FIFO queue (for example, to allow a scheduler to process incoming
// pods).
package cache

import (
	"sync"
)

// 全局默认指标提供者与"仅首次设置生效"的同步保护。
var (
	globalInformerMetricsProvider  InformerMetricsProvider = noopInformerMetricsProvider{}
	setInformerMetricsProviderOnce sync.Once
)

// noopInformerMetricsProvider 是不做任何事的默认实现，避免未配置指标时各处判空。
type noopInformerMetricsProvider struct{}

// InformerMetricsProvider defines an interface for creating metrics that track informer operations.
// 实现方（如 metrics 注册模块）按 informer 标识创建对应指标；返回的指标需在更新前检查
// id.Reserved()，以支持运行期被销毁的动态 informer，防止向已注销的指标继续写入。
type InformerMetricsProvider interface {
	// NewQueuedItemMetric returns a gauge metric for tracking the total number of items
	// currently queued and waiting to be processed.
	// The returned metric should check id.Reserved() before updating to support
	// dynamic informers that may shut down while the process is still running.
	//
	// For DeltaFIFO: Represents len(f.items) - the number of unique keys with pending deltas
	// For RealFIFO: Represents len(f.items) - the total number of individual delta events queued
	NewQueuedItemMetric(id InformerNameAndResource) GaugeMetric

	// NewProcessingLatencyMetric returns a histogram metric for tracking the time taken
	// to process events (execute handlers) after they are popped from the queue.
	// The latency is measured in seconds.
	// The returned metric should check id.Reserved() before updating to support
	// dynamic informers that may shut down while the process is still running.
	NewProcessingLatencyMetric(id InformerNameAndResource) HistogramMetric

	// NewStoreResourceVersionMetric returns a gauge metric for tracking the resource version of the store.
	// The returned metric should check id.Reserved() before updating to support
	// dynamic informers that may shut down while the process is still running.
	NewStoreResourceVersionMetric(id InformerNameAndResource) GaugeMetric
}

// fifoMetrics holds all metrics for a FIFO.
type fifoMetrics struct {
	numberOfQueuedItem GaugeMetric
	processingLatency  HistogramMetric
}

// storeMetrics holds all metrics for a store.
type storeMetrics struct {
	storeResourceVersion GaugeMetric
}

// SetInformerMetricsProvider sets the metrics provider for all subsequently created
// informers. Only the first call has an effect.
// 之所以只允许首次调用生效，是因为指标注册通常幂等且全局唯一，重复覆盖会破坏已有指标。
func SetInformerMetricsProvider(metricsProvider InformerMetricsProvider) {
	setInformerMetricsProviderOnce.Do(func() {
		globalInformerMetricsProvider = metricsProvider
	})
}

// newFIFOMetrics 按需创建 FIFO 指标；未显式传 provider 时回退到全局 provider。
// 只有 id.Reserved() 的 informer 才创建真实指标，其余使用 noopMetric，避免指标基数膨胀。
func newFIFOMetrics(id InformerNameAndResource, metricsProvider InformerMetricsProvider) *fifoMetrics {
	if metricsProvider == nil {
		metricsProvider = globalInformerMetricsProvider
	}
	metrics := &fifoMetrics{
		numberOfQueuedItem: noopMetric{},
		processingLatency:  noopMetric{},
	}

	if id.Reserved() {
		metrics.numberOfQueuedItem = metricsProvider.NewQueuedItemMetric(id)
		metrics.processingLatency = metricsProvider.NewProcessingLatencyMetric(id)
	}

	return metrics
}

// newStoreMetrics 按需创建 store 指标，逻辑与 newFIFOMetrics 一致。
func newStoreMetrics(id InformerNameAndResource, metricsProvider InformerMetricsProvider) *storeMetrics {
	if metricsProvider == nil {
		metricsProvider = globalInformerMetricsProvider
	}
	metrics := &storeMetrics{
		storeResourceVersion: noopMetric{},
	}

	if id.Reserved() {
		metrics.storeResourceVersion = metricsProvider.NewStoreResourceVersionMetric(id)
	}

	return metrics
}

// 以下方法均返回 noopMetric，确保默认配置下监控路径几乎零开销。

func (noopInformerMetricsProvider) NewQueuedItemMetric(InformerNameAndResource) GaugeMetric {
	return noopMetric{}
}

func (noopInformerMetricsProvider) NewProcessingLatencyMetric(InformerNameAndResource) HistogramMetric {
	return noopMetric{}
}

func (noopInformerMetricsProvider) NewStoreResourceVersionMetric(InformerNameAndResource) GaugeMetric {
	return noopMetric{}
}
