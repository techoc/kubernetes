/*
Copyright 2018 The Kubernetes Authors.

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

	"k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

var (
	// descLogSize 描述容器日志占用文件系统字节数的指标。
	descLogSize = metrics.NewDesc(
		"kubelet_container_log_filesystem_used_bytes",
		"Bytes used by the container's logs on the filesystem.",
		[]string{
			"uid",
			"namespace",
			"pod",
			"container",
		}, nil,
		metrics.ALPHA,
		"",
	)
)

// logMetricsCollector 采集容器日志占用的文件系统空间。
type logMetricsCollector struct {
	metrics.BaseStableCollector

	// podStats 返回 Pod 统计信息。
	podStats func(ctx context.Context) ([]statsapi.PodStats, error)
}

// 编译期断言：logMetricsCollector 实现了 StableCollector 接口。
var _ metrics.StableCollector = &logMetricsCollector{}

// NewLogMetricsCollector 创建日志指标采集器，
// 暴露容器日志占用空间大小的指标。
func NewLogMetricsCollector(podStats func(ctx context.Context) ([]statsapi.PodStats, error)) metrics.StableCollector {
	return &logMetricsCollector{
		podStats: podStats,
	}
}

// DescribeWithStability 输出日志大小指标描述符。
func (c *logMetricsCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- descLogSize
}

// CollectWithStability 遍历 Pod 统计并输出容器日志大小指标。
func (c *logMetricsCollector) CollectWithStability(ch chan<- metrics.Metric) {
	// 当前没有可用的上下文，只能使用 context.TODO()；
	// 后续重构为接收 context 参数时应替换为真实上下文。
	ctx := context.TODO()
	logger := klog.FromContext(ctx)
	podStats, err := c.podStats(ctx)
	if err != nil {
		logger.Error(err, "Failed to get pod stats")
		return
	}

	// 只为有日志统计的容器输出指标。
	for _, ps := range podStats {
		for _, c := range ps.Containers {
			if c.Logs != nil && c.Logs.UsedBytes != nil {
				ch <- metrics.NewLazyConstMetric(
					descLogSize,
					metrics.GaugeValue,
					float64(*c.Logs.UsedBytes),
					ps.PodRef.UID,
					ps.PodRef.Namespace,
					ps.PodRef.Name,
					c.Name,
				)
			}
		}
	}
}
