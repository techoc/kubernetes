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

package collectors

import (
	"context"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
	summary "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
)

// 资源指标的描述符：节点/容器/Pod 的 CPU、内存、swap 以及采集错误状态。
var (
	nodeCPUUsageDesc = metrics.NewDesc("node_cpu_usage_seconds_total",
		"Cumulative cpu time consumed by the node in core-seconds",
		nil,
		nil,
		metrics.STABLE,
		"")

	nodeMemoryUsageDesc = metrics.NewDesc("node_memory_working_set_bytes",
		"Current working set of the node in bytes",
		nil,
		nil,
		metrics.STABLE,
		"")

	nodeSwapUsageDesc = metrics.NewDesc("node_swap_usage_bytes",
		"Current swap usage of the node in bytes. Reported only on non-windows systems",
		nil,
		nil,
		metrics.ALPHA,
		"")

	containerCPUUsageDesc = metrics.NewDesc("container_cpu_usage_seconds_total",
		"Cumulative cpu time consumed by the container in core-seconds",
		[]string{"container", "pod", "namespace"},
		nil,
		metrics.STABLE,
		"")

	containerMemoryUsageDesc = metrics.NewDesc("container_memory_working_set_bytes",
		"Current working set of the container in bytes",
		[]string{"container", "pod", "namespace"},
		nil,
		metrics.STABLE,
		"")

	containerSwapUsageDesc = metrics.NewDesc("container_swap_usage_bytes",
		"Current amount of the container swap usage in bytes. Reported only on non-windows systems",
		[]string{"container", "pod", "namespace"},
		nil,
		metrics.ALPHA,
		"")

	containerSwapLimitDesc = metrics.NewDesc("container_swap_limit_bytes",
		"Current amount of the container swap limit in bytes. Reported only on non-windows systems",
		[]string{"container", "pod", "namespace"},
		nil,
		metrics.ALPHA,
		"")

	podCPUUsageDesc = metrics.NewDesc("pod_cpu_usage_seconds_total",
		"Cumulative cpu time consumed by the pod in core-seconds",
		[]string{"pod", "namespace"},
		nil,
		metrics.STABLE,
		"")

	podMemoryUsageDesc = metrics.NewDesc("pod_memory_working_set_bytes",
		"Current working set of the pod in bytes",
		[]string{"pod", "namespace"},
		nil,
		metrics.STABLE,
		"")

	podSwapUsageDesc = metrics.NewDesc("pod_swap_usage_bytes",
		"Current amount of the pod swap usage in bytes. Reported only on non-windows systems",
		[]string{"pod", "namespace"},
		nil,
		metrics.ALPHA,
		"")

	resourceScrapeResultDesc = metrics.NewDesc("scrape_error",
		"1 if there was an error while getting container metrics, 0 otherwise",
		nil,
		nil,
		metrics.ALPHA,
		"1.29.0")

	resourceScrapeErrorResultDesc = metrics.NewDesc("resource_scrape_error",
		"1 if there was an error while getting container metrics, 0 otherwise",
		nil,
		nil,
		metrics.STABLE,
		"")

	containerStartTimeDesc = metrics.NewDesc("container_start_time_seconds",
		"Start time of the container since unix epoch in seconds",
		[]string{"container", "pod", "namespace"},
		nil,
		metrics.STABLE,
		"")
)

// NewResourceMetricsCollector 返回导出资源指标的 StableCollector。
func NewResourceMetricsCollector(provider stats.SummaryProvider) metrics.StableCollector {
	return &resourceMetricsCollector{
		provider: provider,
	}
}

// resourceMetricsCollector 从 SummaryProvider 采集节点与 Pod 的资源指标。
type resourceMetricsCollector struct {
	metrics.BaseStableCollector

	provider stats.SummaryProvider
}

// 编译期断言：resourceMetricsCollector 实现了 StableCollector。
var _ metrics.StableCollector = &resourceMetricsCollector{}

// DescribeWithStability 向 registry 描述本收集器导出的所有指标。
func (rc *resourceMetricsCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- nodeCPUUsageDesc
	ch <- nodeMemoryUsageDesc
	ch <- nodeSwapUsageDesc
	ch <- containerStartTimeDesc
	ch <- containerCPUUsageDesc
	ch <- containerMemoryUsageDesc
	ch <- containerSwapUsageDesc
	ch <- containerSwapLimitDesc
	ch <- podCPUUsageDesc
	ch <- podMemoryUsageDesc
	ch <- podSwapUsageDesc
	ch <- resourceScrapeResultDesc
	ch <- resourceScrapeErrorResultDesc
}

// CollectWithStability 采集一次资源指标。
// 容器频繁创建删除，使用 Gauge 会为已消失的容器泄漏收集器；
// 因此自定义收集器只采集当前活跃容器。
func (rc *resourceMetricsCollector) CollectWithStability(ch chan<- metrics.Metric) {
	// 暂时使用 context.TODO()，因为当前没有合适的 context 可传入；
	// 后续重构该函数接收 context 参数时应替换掉。
	ctx := context.TODO()
	var errorCount float64
	// 无论成功与否都上报 scrape 结果指标，便于告警与排查。
	defer func() {
		ch <- metrics.NewLazyConstMetric(resourceScrapeResultDesc, metrics.GaugeValue, errorCount)
		ch <- metrics.NewLazyConstMetric(resourceScrapeErrorResultDesc, metrics.GaugeValue, errorCount)
	}()
	// 只采集 CPU/内存，避免触发文件系统等昂贵统计。
	statsSummary, err := rc.provider.GetCPUAndMemoryStats(ctx)
	if err != nil {
		logger := klog.FromContext(ctx)
		errorCount = 1
		logger.Error(err, "Error getting summary for resourceMetric prometheus endpoint")
		return
	}

	// 先上报节点级指标，再逐个 Pod/容器上报。
	rc.collectNodeCPUMetrics(ch, statsSummary.Node)
	rc.collectNodeMemoryMetrics(ch, statsSummary.Node)
	rc.collectNodeSwapMetrics(ch, statsSummary.Node)

	for _, pod := range statsSummary.Pods {
		for _, container := range pod.Containers {
			rc.collectContainerStartTime(ch, pod, container)
			rc.collectContainerCPUMetrics(ch, pod, container)
			rc.collectContainerMemoryMetrics(ch, pod, container)
			rc.collectContainerSwapMetrics(ch, pod, container)
		}
		rc.collectPodCPUMetrics(ch, pod)
		rc.collectPodMemoryMetrics(ch, pod)
		rc.collectPodSwapMetrics(ch, pod)
	}
}

// collectNodeCPUMetrics 上报节点累计 CPU 时间（秒）。
func (rc *resourceMetricsCollector) collectNodeCPUMetrics(ch chan<- metrics.Metric, s summary.NodeStats) {
	if s.CPU == nil || s.CPU.UsageCoreNanoSeconds == nil {
		return
	}

	ch <- metrics.NewLazyMetricWithTimestamp(s.CPU.Time.Time,
		metrics.NewLazyConstMetric(nodeCPUUsageDesc, metrics.CounterValue, float64(*s.CPU.UsageCoreNanoSeconds)/float64(time.Second)))
}

// collectNodeMemoryMetrics 上报节点内存工作集。
func (rc *resourceMetricsCollector) collectNodeMemoryMetrics(ch chan<- metrics.Metric, s summary.NodeStats) {
	if s.Memory == nil || s.Memory.WorkingSetBytes == nil {
		return
	}

	ch <- metrics.NewLazyMetricWithTimestamp(s.Memory.Time.Time,
		metrics.NewLazyConstMetric(nodeMemoryUsageDesc, metrics.GaugeValue, float64(*s.Memory.WorkingSetBytes)))
}

// collectNodeSwapMetrics 上报节点 swap 使用量（仅非 Windows）。
func (rc *resourceMetricsCollector) collectNodeSwapMetrics(ch chan<- metrics.Metric, s summary.NodeStats) {
	if s.Swap == nil || s.Swap.SwapUsageBytes == nil {
		return
	}

	ch <- metrics.NewLazyMetricWithTimestamp(s.Swap.Time.Time,
		metrics.NewLazyConstMetric(nodeSwapUsageDesc, metrics.GaugeValue, float64(*s.Swap.SwapUsageBytes)))
}

// collectContainerStartTime 上报容器启动时间（Unix 秒）。
func (rc *resourceMetricsCollector) collectContainerStartTime(ch chan<- metrics.Metric, pod summary.PodStats, s summary.ContainerStats) {
	// 启动时间无效时跳过，避免上报 1970 年的时间戳。
	if s.StartTime.Unix() <= 0 {
		return
	}

	ch <- metrics.NewLazyConstMetric(containerStartTimeDesc, metrics.GaugeValue, float64(s.StartTime.UnixNano())/float64(time.Second), s.Name, pod.PodRef.Name, pod.PodRef.Namespace)
}

// collectContainerCPUMetrics 上报容器累计 CPU 时间（秒）。
func (rc *resourceMetricsCollector) collectContainerCPUMetrics(ch chan<- metrics.Metric, pod summary.PodStats, s summary.ContainerStats) {
	if s.CPU == nil || s.CPU.UsageCoreNanoSeconds == nil {
		return
	}

	ch <- metrics.NewLazyMetricWithTimestamp(s.CPU.Time.Time,
		metrics.NewLazyConstMetric(containerCPUUsageDesc, metrics.CounterValue,
			float64(*s.CPU.UsageCoreNanoSeconds)/float64(time.Second), s.Name, pod.PodRef.Name, pod.PodRef.Namespace))
}

// collectContainerMemoryMetrics 上报容器内存工作集。
func (rc *resourceMetricsCollector) collectContainerMemoryMetrics(ch chan<- metrics.Metric, pod summary.PodStats, s summary.ContainerStats) {
	if s.Memory == nil || s.Memory.WorkingSetBytes == nil {
		return
	}

	ch <- metrics.NewLazyMetricWithTimestamp(s.Memory.Time.Time,
		metrics.NewLazyConstMetric(containerMemoryUsageDesc, metrics.GaugeValue,
			float64(*s.Memory.WorkingSetBytes), s.Name, pod.PodRef.Name, pod.PodRef.Namespace))
}

// collectContainerSwapMetrics 上报容器 swap 使用量与限额。
func (rc *resourceMetricsCollector) collectContainerSwapMetrics(ch chan<- metrics.Metric, pod summary.PodStats, s summary.ContainerStats) {
	if s.Swap == nil {
		return
	}

	swapUsageBytes := float64(0)
	if s.Swap.SwapUsageBytes != nil {
		swapUsageBytes = float64(*s.Swap.SwapUsageBytes)
		ch <- metrics.NewLazyMetricWithTimestamp(s.Swap.Time.Time,
			metrics.NewLazyConstMetric(containerSwapUsageDesc, metrics.GaugeValue,
				swapUsageBytes, s.Name, pod.PodRef.Name, pod.PodRef.Namespace))
	}

	// 限额 = 已用 swap + 可用 swap，两者取自同一统计时刻。
	if s.Swap.SwapAvailableBytes != nil {
		ch <- metrics.NewLazyMetricWithTimestamp(s.Swap.Time.Time,
			metrics.NewLazyConstMetric(containerSwapLimitDesc, metrics.GaugeValue,
				float64(*s.Swap.SwapAvailableBytes)+swapUsageBytes, s.Name, pod.PodRef.Name, pod.PodRef.Namespace))
	}
}

// collectPodCPUMetrics 上报 Pod 累计 CPU 时间（秒）。
func (rc *resourceMetricsCollector) collectPodCPUMetrics(ch chan<- metrics.Metric, pod summary.PodStats) {
	if pod.CPU == nil || pod.CPU.UsageCoreNanoSeconds == nil {
		return
	}

	ch <- metrics.NewLazyMetricWithTimestamp(pod.CPU.Time.Time,
		metrics.NewLazyConstMetric(podCPUUsageDesc, metrics.CounterValue,
			float64(*pod.CPU.UsageCoreNanoSeconds)/float64(time.Second), pod.PodRef.Name, pod.PodRef.Namespace))
}

// collectPodMemoryMetrics 上报 Pod 内存工作集。
func (rc *resourceMetricsCollector) collectPodMemoryMetrics(ch chan<- metrics.Metric, pod summary.PodStats) {
	if pod.Memory == nil || pod.Memory.WorkingSetBytes == nil {
		return
	}

	ch <- metrics.NewLazyMetricWithTimestamp(pod.Memory.Time.Time,
		metrics.NewLazyConstMetric(podMemoryUsageDesc, metrics.GaugeValue,
			float64(*pod.Memory.WorkingSetBytes), pod.PodRef.Name, pod.PodRef.Namespace))
}

// collectPodSwapMetrics 上报 Pod swap 使用量（仅非 Windows）。
func (rc *resourceMetricsCollector) collectPodSwapMetrics(ch chan<- metrics.Metric, pod summary.PodStats) {
	if pod.Swap == nil || pod.Swap.SwapUsageBytes == nil {
		return
	}

	ch <- metrics.NewLazyMetricWithTimestamp(pod.Swap.Time.Time,
		metrics.NewLazyConstMetric(podSwapUsageDesc, metrics.GaugeValue,
			float64(*pod.Swap.SwapUsageBytes), pod.PodRef.Name, pod.PodRef.Namespace))
}
