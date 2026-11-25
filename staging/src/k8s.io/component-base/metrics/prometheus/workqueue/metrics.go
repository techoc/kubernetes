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

// Package prometheus 把 workqueue 的默认指标工厂替换为 Prometheus 实现；
// 作为副作用导入（import _）即完成安装，无需调用方显式初始化。
package workqueue

import (
	"sync"

	"k8s.io/client-go/util/workqueue"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// workqueue 使用的指标子系统和键。
//
// 指标名与 client-go 约定的 key 一一对应；workqueue 只持有这些字符串，
// 具体指标类型与 label 由本包决定。
const (
	// WorkQueueSubsystem 是 workqueue 指标的子系统名称。
	WorkQueueSubsystem = "workqueue"
	// DepthKey 是队列当前深度指标的名称键。
	DepthKey = "depth"
	// AddsKey 是入队次数指标的名称键。
	AddsKey = "adds_total"
	// QueueLatencyKey 是条目排队时长指标的名称键。
	QueueLatencyKey = "queue_duration_seconds"
	// WorkDurationKey 是条目处理时长指标的名称键。
	WorkDurationKey = "work_duration_seconds"
	// UnfinishedWorkKey 是未完成工作时长的名称键。
	UnfinishedWorkKey = "unfinished_work_seconds"
	// LongestRunningProcessorKey 是最长运行中处理器时长的名称键。
	LongestRunningProcessorKey = "longest_running_processor_seconds"
	// RetriesKey 是重试次数指标的名称键。
	RetriesKey = "retries_total"
)

var (
	// depth 与其余指标一样按队列名打 label，便于区分不同控制器的工作队列。
	depth = k8smetrics.NewGaugeVec(&k8smetrics.GaugeOpts{
		Subsystem:      WorkQueueSubsystem,
		Name:           DepthKey,
		StabilityLevel: k8smetrics.BETA,
		Help:           "Current depth of workqueue",
	}, []string{"name"})

	adds = k8smetrics.NewCounterVec(&k8smetrics.CounterOpts{
		Subsystem:      WorkQueueSubsystem,
		Name:           AddsKey,
		StabilityLevel: k8smetrics.BETA,
		Help:           "Total number of adds handled by workqueue",
	}, []string{"name"})

	latency = k8smetrics.NewHistogramVec(&k8smetrics.HistogramOpts{
		Subsystem:      WorkQueueSubsystem,
		Name:           QueueLatencyKey,
		StabilityLevel: k8smetrics.BETA,
		Help:           "How long in seconds an item stays in workqueue before being requested.",
		Buckets:        k8smetrics.ExponentialBuckets(10e-9, 10, 10),
	}, []string{"name"})

	workDuration = k8smetrics.NewHistogramVec(&k8smetrics.HistogramOpts{
		Subsystem:      WorkQueueSubsystem,
		Name:           WorkDurationKey,
		StabilityLevel: k8smetrics.BETA,
		Help:           "How long in seconds processing an item from workqueue takes.",
		Buckets:        k8smetrics.ExponentialBuckets(10e-9, 10, 10),
	}, []string{"name"})

	unfinished = k8smetrics.NewGaugeVec(&k8smetrics.GaugeOpts{
		Subsystem:      WorkQueueSubsystem,
		Name:           UnfinishedWorkKey,
		StabilityLevel: k8smetrics.BETA,
		Help: "How many seconds of work has done that " +
			"is in progress and hasn't been observed by work_duration. Large " +
			"values indicate stuck threads. One can deduce the number of stuck " +
			"threads by observing the rate at which this increases.",
	}, []string{"name"})

	longestRunningProcessor = k8smetrics.NewGaugeVec(&k8smetrics.GaugeOpts{
		Subsystem:      WorkQueueSubsystem,
		Name:           LongestRunningProcessorKey,
		StabilityLevel: k8smetrics.BETA,
		Help: "How many seconds has the longest running " +
			"processor for workqueue been running.",
	}, []string{"name"})

	retries = k8smetrics.NewCounterVec(&k8smetrics.CounterOpts{
		Subsystem:      WorkQueueSubsystem,
		Name:           RetriesKey,
		StabilityLevel: k8smetrics.BETA,
		Help:           "Total number of retries handled by workqueue",
	}, []string{"name"})

	// metrics 是注册时需要全部挂到 legacyregistry 的集合，
	// 集中列出可避免新增指标时漏注册。
	metrics = []k8smetrics.Registerable{
		depth, adds, latency, workDuration, unfinished, longestRunningProcessor, retries,
	}

	// registerOnce 保证注册只执行一次：多个 workqueue 会并发调用 provider 工厂方法。
	registerOnce sync.Once
)

// prometheusMetricsProvider 是 workqueue.MetricsProvider 的实现，
// 每个方法创建/获取对应 workqueue 名称的指标句柄。
type prometheusMetricsProvider struct {
}

// init 只向 workqueue 安装 provider，实际到 legacyregistry 的注册延迟到首次调用 factory
// 方法（运行时常在首次构造 workqueue 时发生）。这样可确保 Create() 以及对 feature gate
// 的读取发生在 ApplyFeatureGates 传播 gate 状态之后。
func init() {
	workqueue.SetProvider(prometheusMetricsProvider{})
}

// register 向旧版注册表注册全部 workqueue 指标。每个 provider factory 方法均通过
// registerOnce 调用它，使注册（以及 Create）在 ApplyFeatureGates 之后的运行时发生。
func register() {
	registerOnce.Do(func() {
		for _, m := range metrics {
			legacyregistry.MustRegister(m)
		}
	})
}

// NewDepthMetric 返回给定队列的 depth gauge；调用前确保已完成注册。
func (prometheusMetricsProvider) NewDepthMetric(name string) workqueue.GaugeMetric {
	register()
	return depth.WithLabelValues(name)
}

// NewAddsMetric 返回给定队列的入队计数 counter。
func (prometheusMetricsProvider) NewAddsMetric(name string) workqueue.CounterMetric {
	register()
	return adds.WithLabelValues(name)
}

// NewLatencyMetric 返回条目在队列中等待时长的 histogram。
func (prometheusMetricsProvider) NewLatencyMetric(name string) workqueue.HistogramMetric {
	register()
	return latency.WithLabelValues(name)
}

// NewWorkDurationMetric 返回处理单条目耗时的 histogram。
func (prometheusMetricsProvider) NewWorkDurationMetric(name string) workqueue.HistogramMetric {
	register()
	return workDuration.WithLabelValues(name)
}

// NewUnfinishedWorkSecondsMetric 返回尚未被 work_duration 观测到的在途工作秒数。
func (prometheusMetricsProvider) NewUnfinishedWorkSecondsMetric(name string) workqueue.SettableGaugeMetric {
	register()
	return unfinished.WithLabelValues(name)
}

// NewLongestRunningProcessorSecondsMetric 返回最长运行中处理器的持续秒数。
func (prometheusMetricsProvider) NewLongestRunningProcessorSecondsMetric(name string) workqueue.SettableGaugeMetric {
	register()
	return longestRunningProcessor.WithLabelValues(name)
}

// NewRetriesMetric 返回给定队列的重试次数 counter。
func (prometheusMetricsProvider) NewRetriesMetric(name string) workqueue.CounterMetric {
	register()
	return retries.WithLabelValues(name)
}
