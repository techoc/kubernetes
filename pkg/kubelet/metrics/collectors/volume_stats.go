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

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-base/metrics"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	kubeletmetrics "k8s.io/kubernetes/pkg/kubelet/metrics"
	serverstats "k8s.io/kubernetes/pkg/kubelet/server/stats"
)

// 卷统计指标的 Prometheus 描述符。
var (
	volumeStatsCapacityBytesDesc = metrics.NewDesc(
		metrics.BuildFQName("", kubeletmetrics.KubeletSubsystem, kubeletmetrics.VolumeStatsCapacityBytesKey),
		"Capacity in bytes of the volume",
		[]string{"namespace", "persistentvolumeclaim"}, nil,
		metrics.ALPHA, "",
	)
	volumeStatsAvailableBytesDesc = metrics.NewDesc(
		metrics.BuildFQName("", kubeletmetrics.KubeletSubsystem, kubeletmetrics.VolumeStatsAvailableBytesKey),
		"Number of available bytes in the volume",
		[]string{"namespace", "persistentvolumeclaim"}, nil,
		metrics.ALPHA, "",
	)
	volumeStatsUsedBytesDesc = metrics.NewDesc(
		metrics.BuildFQName("", kubeletmetrics.KubeletSubsystem, kubeletmetrics.VolumeStatsUsedBytesKey),
		"Number of used bytes in the volume",
		[]string{"namespace", "persistentvolumeclaim"}, nil,
		metrics.ALPHA, "",
	)
	volumeStatsInodesDesc = metrics.NewDesc(
		metrics.BuildFQName("", kubeletmetrics.KubeletSubsystem, kubeletmetrics.VolumeStatsInodesKey),
		"Maximum number of inodes in the volume",
		[]string{"namespace", "persistentvolumeclaim"}, nil,
		metrics.ALPHA, "",
	)
	volumeStatsInodesFreeDesc = metrics.NewDesc(
		metrics.BuildFQName("", kubeletmetrics.KubeletSubsystem, kubeletmetrics.VolumeStatsInodesFreeKey),
		"Number of free inodes in the volume",
		[]string{"namespace", "persistentvolumeclaim"}, nil,
		metrics.ALPHA, "",
	)
	volumeStatsInodesUsedDesc = metrics.NewDesc(
		metrics.BuildFQName("", kubeletmetrics.KubeletSubsystem, kubeletmetrics.VolumeStatsInodesUsedKey),
		"Number of used inodes in the volume",
		[]string{"namespace", "persistentvolumeclaim"}, nil,
		metrics.ALPHA, "",
	)
)

// volumeStatsCollector 从 kubelet 统计提供者采集 PVC 卷指标。
type volumeStatsCollector struct {
	metrics.BaseStableCollector

	statsProvider serverstats.Provider
}

// 编译期断言：volumeStatsCollector 实现了 StableCollector 接口。
var _ metrics.StableCollector = &volumeStatsCollector{}

// NewVolumeStatsCollector 创建卷统计指标采集器。
func NewVolumeStatsCollector(statsProvider serverstats.Provider) metrics.StableCollector {
	return &volumeStatsCollector{statsProvider: statsProvider}
}

// DescribeWithStability 输出全部卷统计指标描述符。
func (collector *volumeStatsCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- volumeStatsCapacityBytesDesc
	ch <- volumeStatsAvailableBytesDesc
	ch <- volumeStatsUsedBytesDesc
	ch <- volumeStatsInodesDesc
	ch <- volumeStatsInodesFreeDesc
	ch <- volumeStatsInodesUsedDesc
}

// CollectWithStability 遍历 Pod 统计信息并输出 PVC 卷指标。
func (collector *volumeStatsCollector) CollectWithStability(ch chan<- metrics.Metric) {
	// 当前没有可用的上下文，只能使用 context.TODO()；
	// 后续重构为接收 context 参数时应替换为真实上下文。
	ctx := context.TODO()
	podStats, err := collector.statsProvider.ListPodStats(ctx)
	if err != nil {
		return
	}
	// addGauge 统一为指标追加 PVC 命名空间与名称标签。
	addGauge := func(desc *metrics.Desc, pvcRef *stats.PVCReference, v float64, lv ...string) {
		lv = append([]string{pvcRef.Namespace, pvcRef.Name}, lv...)
		ch <- metrics.NewLazyConstMetric(desc, metrics.GaugeValue, v, lv...)
	}
	// 记录已上报的 PVC，避免同一 PVC 被多个 Pod 重复计数。
	allPVCs := sets.Set[stats.PVCReference]{}
	for _, podStat := range podStats {
		if podStat.VolumeStats == nil {
			continue
		}
		for _, volumeStat := range podStat.VolumeStats {
			pvcRef := volumeStat.PVCRef
			if pvcRef == nil {
				// 没有 PVC 引用的卷不产生指标。
				continue
			}
			if allPVCs.Has(*pvcRef) {
				// 同一 PVC 只上报一次。
				continue
			}
			addGauge(volumeStatsCapacityBytesDesc, pvcRef, float64(*volumeStat.CapacityBytes))
			addGauge(volumeStatsAvailableBytesDesc, pvcRef, float64(*volumeStat.AvailableBytes))
			addGauge(volumeStatsUsedBytesDesc, pvcRef, float64(*volumeStat.UsedBytes))
			addGauge(volumeStatsInodesDesc, pvcRef, float64(*volumeStat.Inodes))
			addGauge(volumeStatsInodesFreeDesc, pvcRef, float64(*volumeStat.InodesFree))
			addGauge(volumeStatsInodesUsedDesc, pvcRef, float64(*volumeStat.InodesUsed))
			allPVCs.Insert(*pvcRef)
		}
	}
}
