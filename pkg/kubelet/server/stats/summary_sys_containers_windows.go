//go:build windows

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

package stats

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/winstats"
)

// GetSystemContainersStats 返回 Windows 上系统容器的统计信息（Pod 汇总与全局提交内存）。
func (sp *summaryProviderImpl) GetSystemContainersStats(_ context.Context, nodeConfig cm.NodeConfig, podStats []statsapi.PodStats, updateStats bool) (stats []statsapi.ContainerStats) {
	stats = append(stats, sp.getSystemPodsCPUAndMemoryStats(nodeConfig, podStats, updateStats))
	stats = append(stats, sp.getSystemWindowsGlobalmemoryStats())
	return stats
}

// GetSystemContainersCPUAndMemoryStats 返回系统容器的 CPU 与内存统计信息。
func (sp *summaryProviderImpl) GetSystemContainersCPUAndMemoryStats(_ context.Context, nodeConfig cm.NodeConfig, podStats []statsapi.PodStats, updateStats bool) (stats []statsapi.ContainerStats) {
	stats = append(stats, sp.getSystemPodsCPUAndMemoryStats(nodeConfig, podStats, updateStats))
	stats = append(stats, sp.getSystemWindowsGlobalmemoryStats())
	return stats
}

// getSystemPodsCPUAndMemoryStats 汇总所有 Pod 的 CPU 与内存统计。
func (sp *summaryProviderImpl) getSystemPodsCPUAndMemoryStats(nodeConfig cm.NodeConfig, podStats []statsapi.PodStats, updateStats bool) statsapi.ContainerStats {
	now := metav1.NewTime(time.Now())
	podsSummary := statsapi.ContainerStats{
		StartTime: now,
		CPU:       &statsapi.CPUStats{},
		Memory:    &statsapi.MemoryStats{},
		Name:      statsapi.SystemContainerPods,
	}

	// 累加所有 Pod 的统计信息。
	var usageCoreNanoSeconds uint64
	var usageNanoCores uint64
	var availableBytes uint64
	var usageBytes uint64
	var workingSetBytes uint64
	// 逐 Pod 累加 CPU 与内存用量。
	for _, pod := range podStats {
		if pod.CPU != nil {
			podsSummary.CPU.Time = now
			if pod.CPU.UsageCoreNanoSeconds != nil {
				usageCoreNanoSeconds = usageCoreNanoSeconds + *pod.CPU.UsageCoreNanoSeconds
			}
			if pod.CPU.UsageNanoCores != nil {
				usageNanoCores = usageNanoCores + *pod.CPU.UsageNanoCores
			}
		}

		if pod.Memory != nil {
			podsSummary.Memory.Time = now
			if pod.Memory.AvailableBytes != nil {
				availableBytes = availableBytes + *pod.Memory.AvailableBytes
			}
			if pod.Memory.UsageBytes != nil {
				usageBytes = usageBytes + *pod.Memory.UsageBytes
			}
			if pod.Memory.WorkingSetBytes != nil {
				workingSetBytes = workingSetBytes + *pod.Memory.WorkingSetBytes
			}
		}
	}

	// 仅在统计值非零时写入结果，避免上报无意义的零值。
	if usageCoreNanoSeconds != 0 {
		podsSummary.CPU.UsageCoreNanoSeconds = &usageCoreNanoSeconds
	}
	if usageNanoCores != 0 {
		podsSummary.CPU.UsageNanoCores = &usageNanoCores
	}
	if availableBytes != 0 {
		podsSummary.Memory.AvailableBytes = &availableBytes
	}
	if usageBytes != 0 {
		podsSummary.Memory.UsageBytes = &usageBytes
	}
	if workingSetBytes != 0 {
		podsSummary.Memory.WorkingSetBytes = &workingSetBytes
	}

	return podsSummary
}

// getSystemWindowsGlobalmemoryStats 读取 Windows 全局提交内存统计。
func (sp *summaryProviderImpl) getSystemWindowsGlobalmemoryStats() statsapi.ContainerStats {
	now := metav1.NewTime(time.Now())
	globalMemorySummary := statsapi.ContainerStats{
		StartTime: now,
		Memory:    &statsapi.MemoryStats{},
		Name:      statsapi.SystemContainerWindowsGlobalCommitMemory,
	}

	// 全局提交内存来自 Windows 性能计数器。
	perfInfo, err := winstats.GetPerformanceInfo()
	if err != nil {
		klog.Errorf("Failed to get Windows performance info: %v", err)
		return globalMemorySummary
	}

	commitLimitBytes := perfInfo.CommitLimitPages * perfInfo.PageSize
	commitTotalBytes := perfInfo.CommitTotalPages * perfInfo.PageSize
	// 可用提交内存 = 提交上限 - 已提交量。
	commitAvailableBytes := commitLimitBytes - commitTotalBytes
	globalMemorySummary.Memory.Time = now
	globalMemorySummary.Memory.AvailableBytes = &commitAvailableBytes
	globalMemorySummary.Memory.UsageBytes = &commitTotalBytes

	return globalMemorySummary
}
