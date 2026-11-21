/*
Copyright 2022 The Kubernetes Authors.

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
	"fmt"
	"time"

	"k8s.io/component-base/metrics"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

// criMetricsCollector 从 CRI 运行时采集 Pod 与容器指标。
type criMetricsCollector struct {
	metrics.BaseStableCollector
	// descriptors 在启动时通过一次 ListMetricDescriptors 调用填充，
	// 键为指标名称，值为对应的 Desc。
	descriptors             map[string]*metrics.Desc
	listPodSandboxMetricsFn func(context.Context) ([]*runtimeapi.PodSandboxMetrics, error)
}

// 编译期断言：criMetricsCollector 实现了 StableCollector 接口。
var _ metrics.StableCollector = &criMetricsCollector{}

// NewCRIMetricsCollector 创建 CRI 指标采集器，并预取指标描述符。
func NewCRIMetricsCollector(ctx context.Context, listPodSandboxMetricsFn func(context.Context) ([]*runtimeapi.PodSandboxMetrics, error), listMetricDescriptorsFn func(context.Context) ([]*runtimeapi.MetricDescriptor, error)) metrics.StableCollector {
	// 描述符只获取一次；失败时降级为仅采集指标。
	descs, err := listMetricDescriptorsFn(ctx)
	if err != nil {
		logger := klog.FromContext(ctx)
		// 描述符读取失败不阻塞采集，仅记录错误。
		logger.Error(err, "Error reading MetricDescriptors")
		return &criMetricsCollector{
			listPodSandboxMetricsFn: listPodSandboxMetricsFn,
		}
	}
	c := &criMetricsCollector{
		listPodSandboxMetricsFn: listPodSandboxMetricsFn,
		descriptors:             make(map[string]*metrics.Desc, len(descs)),
	}

	for _, desc := range descs {
		c.descriptors[desc.Name] = criDescToProm(desc)
	}

	return c
}

// DescribeWithStability 输出所有预取的指标描述符。
func (c *criMetricsCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	for _, desc := range c.descriptors {
		ch <- desc
	}
}

// CollectWithStability 从运行时拉取 Pod 指标并转换为 Prometheus 指标。
// TODO(haircommander): would it be better if these were processed async?
func (c *criMetricsCollector) CollectWithStability(ch chan<- metrics.Metric) {
	// 当前没有可用的上下文，只能使用 context.TODO()；
	// 后续重构为接收 context 参数时应替换为真实上下文。
	ctx := context.TODO()
	logger := klog.FromContext(ctx)
	podMetrics, err := c.listPodSandboxMetricsFn(ctx)
	if err != nil {
		logger.Error(err, "Failed to get pod metrics")
		return
	}

	// 依次转换 Pod 级与容器级指标，转换失败的指标直接丢弃。
	for _, podMetric := range podMetrics {
		for _, metric := range podMetric.GetMetrics() {
			promMetric, err := c.criMetricToProm(logger, metric)
			if err == nil {
				ch <- promMetric
			}
		}
		for _, ctrMetric := range podMetric.GetContainerMetrics() {
			for _, metric := range ctrMetric.GetMetrics() {
				promMetric, err := c.criMetricToProm(logger, metric)
				if err == nil {
					ch <- promMetric
				}
			}
		}
	}
}

// criDescToProm 把 CRI 指标描述符转换为 Prometheus 描述符。
func criDescToProm(m *runtimeapi.MetricDescriptor) *metrics.Desc {
	// 标签按可变标签处理而非常量标签，
	// 因为每个容器对应的标签值都不同。
	return metrics.NewDesc(m.Name, m.Help, m.LabelKeys, nil, metrics.INTERNAL, "")
}

// criMetricToProm 把单个 CRI 指标转换为 Prometheus 指标。
func (c *criMetricsCollector) criMetricToProm(logger klog.Logger, m *runtimeapi.Metric) (metrics.Metric, error) {
	// 只接受启动时已声明过的指标，避免暴露未知指标。
	desc, ok := c.descriptors[m.Name]
	if !ok {
		err := fmt.Errorf("error converting CRI Metric to prometheus format")
		logger.V(5).Info("Descriptor not present in pre-populated list of descriptors", "name", m.Name, "err", err)
		return nil, err
	}

	typ := criTypeToProm[m.MetricType]

	pm, err := metrics.NewConstMetric(desc, typ, float64(m.GetValue().Value), m.LabelValues...)
	if err != nil {
		logger.Error(err, "Error getting CRI prometheus metric", "descriptor", desc.String())
		return nil, err
	}
	// Timestamp 为 0 表示运行时未缓存结果：按需采集的指标没有时间戳。
	// 非 0 则说明结果是提前缓存的，需要附带原始时间戳。
	if m.Timestamp == 0 {
		return pm, nil
	}
	return metrics.NewLazyMetricWithTimestamp(time.Unix(0, m.Timestamp), pm), nil
}

var criTypeToProm = map[runtimeapi.MetricType]metrics.ValueType{
	runtimeapi.MetricType_COUNTER: metrics.CounterValue,
	runtimeapi.MetricType_GAUGE:   metrics.GaugeValue,
}
