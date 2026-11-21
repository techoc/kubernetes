//go:build linux

/*
Copyright 2023 The Kubernetes Authors.

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
	"fmt"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/features"
)

// addCRIPodContainerStats 把 sandbox 中每个容器的完整统计信息补充到 PodStats。
func (p *criStatsProvider) addCRIPodContainerStats(
	logger klog.Logger,
	criSandboxStat *runtimeapi.PodSandboxStats,
	ps *statsapi.PodStats,
	fsIDtoInfo map[string]*cadvisorapi.FsInfo,
	containerMap map[string]*runtimeapi.Container,
	podSandbox *runtimeapi.PodSandbox,
	rootFsInfo *cadvisorapi.FsInfo, updateCPUNanoCoreUsage bool) error {
	for _, criContainerStat := range criSandboxStat.Linux.Containers {
		// 忽略已不在 containerMap 中的容器（可能已被删除）。
		container, found := containerMap[criContainerStat.Attributes.Id]
		if !found {
			continue
		}
		// 填充 PodStats 所需的完整容器统计。
		cs, err := p.makeContainerStats(logger, criContainerStat, container, rootFsInfo, fsIDtoInfo, podSandbox.GetMetadata(),
			updateCPUNanoCoreUsage)
		if err != nil {
			return fmt.Errorf("make container stats: %w", err)
		}
		ps.Containers = append(ps.Containers, *cs)
	}
	return nil
}

// addCRIPodContainerCPUAndMemoryStats 把 CRI 中的容器 CPU/内存统计补充到 PodStats。
// 这是 addCRIPodContainerStats 的轻量版本，只填充资源指标端点所需的 CPU 与内存数据。
func (p *criStatsProvider) addCRIPodContainerCPUAndMemoryStats(
	criSandboxStat *runtimeapi.PodSandboxStats,
	ps *statsapi.PodStats,
	containerMap map[string]*runtimeapi.Container) {
	// Linux 字段为空说明运行时未提供对应数据，直接跳过。
	if criSandboxStat == nil || criSandboxStat.Linux == nil {
		return
	}
	for _, criContainerStat := range criSandboxStat.Linux.Containers {
		container, found := containerMap[criContainerStat.Attributes.Id]
		if !found {
			continue
		}
		// 只填充资源指标所需的 CPU 与内存统计。
		cs := p.makeContainerCPUAndMemoryStats(criContainerStat, time.Unix(0, container.CreatedAt), true)
		ps.Containers = append(ps.Containers, *cs)
	}
}

// addCRIPodNetworkStats 把 sandbox 网络统计补充到 PodStats。
func addCRIPodNetworkStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
	// 任一层为空都说明运行时未提供网络统计，保持 PodStats 对应字段为 nil。
	if criPodStat == nil || criPodStat.Linux == nil || criPodStat.Linux.Network == nil {
		return
	}
	criNetwork := criPodStat.Linux.Network
	iStats := statsapi.NetworkStats{
		Time:           metav1.NewTime(time.Unix(0, criNetwork.Timestamp)),
		InterfaceStats: criInterfaceToSummary(criNetwork.DefaultInterface),
		Interfaces:     make([]statsapi.InterfaceStats, 0, len(criNetwork.Interfaces)),
	}
	// 除默认接口外，还要记录所有附加接口。
	for _, iface := range criNetwork.Interfaces {
		iStats.Interfaces = append(iStats.Interfaces, criInterfaceToSummary(iface))
	}
	ps.Network = &iStats
}

// addCRIPodMemoryStats 把 sandbox 内存统计补充到 PodStats。
func addCRIPodMemoryStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
	if criPodStat == nil || criPodStat.Linux == nil || criPodStat.Linux.Memory == nil {
		return
	}
	criMemory := criPodStat.Linux.Memory
	ps.Memory = &statsapi.MemoryStats{
		Time:            metav1.NewTime(time.Unix(0, criMemory.Timestamp)),
		AvailableBytes:  valueOfUInt64Value(criMemory.AvailableBytes),
		UsageBytes:      valueOfUInt64Value(criMemory.UsageBytes),
		WorkingSetBytes: valueOfUInt64Value(criMemory.WorkingSetBytes),
		RSSBytes:        valueOfUInt64Value(criMemory.RssBytes),
		PageFaults:      valueOfUInt64Value(criMemory.PageFaults),
		MajorPageFaults: valueOfUInt64Value(criMemory.MajorPageFaults),
		PSI:             makePSIStats(criMemory.Psi),
	}
}

// addCRIPodCPUStats 把 sandbox CPU 统计补充到 PodStats。
func addCRIPodCPUStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
	if criPodStat == nil || criPodStat.Linux == nil || criPodStat.Linux.Cpu == nil {
		return
	}
	criCPU := criPodStat.Linux.Cpu
	ps.CPU = &statsapi.CPUStats{
		Time:                 metav1.NewTime(time.Unix(0, criCPU.Timestamp)),
		UsageNanoCores:       valueOfUInt64Value(criCPU.UsageNanoCores),
		UsageCoreNanoSeconds: valueOfUInt64Value(criCPU.UsageCoreNanoSeconds),
		PSI:                  makePSIStats(criCPU.Psi),
	}
}

// addCRIPodIOStats 在启用 KubeletPSI 时补充 IO/PSI 统计。
func addCRIPodIOStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
	// PSI 特性未启用时不上报 IO 统计，避免暴露不稳定的指标。
	if !utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		return
	}
	if criPodStat == nil || criPodStat.Linux == nil || criPodStat.Linux.Io == nil {
		return
	}
	criIO := criPodStat.Linux.Io
	ps.IO = &statsapi.IOStats{
		Time: metav1.NewTime(time.Unix(0, criIO.Timestamp)),
		PSI:  makePSIStats(criIO.Psi),
	}
}

// addCRIPodProcessStats 把 sandbox 进程数补充到 PodStats。
func addCRIPodProcessStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
	if criPodStat == nil || criPodStat.Linux == nil || criPodStat.Linux.Process == nil {
		return
	}
	ps.ProcessStats = &statsapi.ProcessStats{
		ProcessCount: valueOfUInt64Value(criPodStat.Linux.Process.ProcessCount),
	}
}

// listContainerNetworkStats 返回所有运行中容器的网络统计；非 Windows 平台返回 (nil, nil)。
func (p *criStatsProvider) listContainerNetworkStats(klog.Logger) (map[string]*statsapi.NetworkStats, error) {
	// Linux 上网络统计由 cadvisor 提供，这里无需额外处理。
	return nil, nil
}
