/*
Copyright 2016 The Kubernetes Authors.

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

// 本文件为设置指标提供者（例如 prometheus）提供抽象层。
//
// 采集端（kubelet、kube-controller-manager 等）通过 SetReflectorMetricsProvider 注入
// 具体实现；未注入时使用 no-op 实现，保证 client-go 本身不依赖任何指标库。

package cache

import (
	"sync"
)

// GaugeMetric 表示一个可以任意升降的单一数值。
//
// 反射器用它记录最近一次观察到的 resourceVersion 之类的瞬时状态。
type GaugeMetric interface {
	Set(float64)
}

// CounterMetric 表示一个只会单调递增的单一数值。
type CounterMetric interface {
	Inc()
}

// SummaryMetric 记录单次观测值。
type SummaryMetric interface {
	Observe(float64)
}

// HistogramMetric 把单次观测值记录到可配置的桶中，同时提供观测值的总和与计数。
type HistogramMetric interface {
	Observe(float64)
}

// noopMetric 同时实现上述所有接口，供未配置指标提供者时静默吞掉上报。
type noopMetric struct{}

func (noopMetric) Inc()            {}
func (noopMetric) Dec()            {}
func (noopMetric) Observe(float64) {}
func (noopMetric) Set(float64)     {}

// MetricsProvider 生成反射器使用的各类指标。
//
// 每个 New*Metric 方法都以指标名为参数，允许同一进程内为不同的 informer 创建
// 独立的指标实例（名字通常包含资源类型等信息）。
type MetricsProvider interface {
	NewListsMetric(name string) CounterMetric
	NewListDurationMetric(name string) SummaryMetric
	NewItemsInListMetric(name string) SummaryMetric

	NewWatchesMetric(name string) CounterMetric
	NewShortWatchesMetric(name string) CounterMetric
	NewWatchDurationMetric(name string) SummaryMetric
	NewItemsInWatchMetric(name string) SummaryMetric

	NewLastResourceVersionMetric(name string) GaugeMetric
}

// noopMetricsProvider 是默认提供者，让所有指标创建请求都返回无副作用的实例。
type noopMetricsProvider struct{}

func (noopMetricsProvider) NewListsMetric(name string) CounterMetric         { return noopMetric{} }
func (noopMetricsProvider) NewListDurationMetric(name string) SummaryMetric  { return noopMetric{} }
func (noopMetricsProvider) NewItemsInListMetric(name string) SummaryMetric   { return noopMetric{} }
func (noopMetricsProvider) NewWatchesMetric(name string) CounterMetric       { return noopMetric{} }
func (noopMetricsProvider) NewShortWatchesMetric(name string) CounterMetric  { return noopMetric{} }
func (noopMetricsProvider) NewWatchDurationMetric(name string) SummaryMetric { return noopMetric{} }
func (noopMetricsProvider) NewItemsInWatchMetric(name string) SummaryMetric  { return noopMetric{} }
func (noopMetricsProvider) NewLastResourceVersionMetric(name string) GaugeMetric {
	return noopMetric{}
}

var metricsFactory = struct {
	metricsProvider MetricsProvider
	setProviders    sync.Once
}{
	metricsProvider: noopMetricsProvider{},
}

// SetReflectorMetricsProvider 设置指标提供者。
//
// 仅第一次调用生效：指标注册必须在进程初始化阶段完成，重复设置会导致指标重复注册，
// 因此用 sync.Once 让后续调用静默失败。
func SetReflectorMetricsProvider(metricsProvider MetricsProvider) {
	metricsFactory.setProviders.Do(func() {
		metricsFactory.metricsProvider = metricsProvider
	})
}
