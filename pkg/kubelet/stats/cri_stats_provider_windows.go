//go:build windows

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

package stats

import (
	"fmt"
	"time"

	"github.com/Microsoft/hnslib"
	cadvisorapi "github.com/google/cadvisor/lib/model"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/utils/ptr"
)

// windowsNetworkStatsProvider 抽象 HNS 查询接口，便于在测试中替换实现，
// 无需真正创建容器。
type windowsNetworkStatsProvider interface {
	HNSListEndpointRequest() ([]hnslib.HNSEndpoint, error)
	GetHNSEndpointStats(endpointName string) (*hnslib.HNSEndpointStats, error)
}

// networkStats 是 hnslib 的薄封装，暴露本场景所需的 HNS 查询能力。
type networkStats struct{}

// HNSListEndpointRequest 列出当前主机上的所有 HNS 网络端点。
func (s networkStats) HNSListEndpointRequest() ([]hnslib.HNSEndpoint, error) {
	return hnslib.HNSListEndpointRequest()
}

// GetHNSEndpointStats 读取指定 HNS 端点的收发统计。
func (s networkStats) GetHNSEndpointStats(endpointName string) (*hnslib.HNSEndpointStats, error) {
	return hnslib.GetHNSEndpointStats(endpointName)
}

// listContainerNetworkStats 返回所有运行中容器的网络统计，按容器 ID 索引。
func (p *criStatsProvider) listContainerNetworkStats(logger klog.Logger) (map[string]*statsapi.NetworkStats, error) {
	networkStatsProvider := newNetworkStatsProvider(p)
	// 先列出所有 HNS 端点，再逐个读取统计。
	endpoints, err := networkStatsProvider.HNSListEndpointRequest()
	if err != nil {
		logger.Error(err, "Failed to fetch current HNS endpoints")
		return nil, err
	}

	networkStats := make(map[string]*statsapi.NetworkStats)
	for _, endpoint := range endpoints {
		// 单个端点读取失败只跳过该端点，不影响其他容器的统计。
		endpointStats, err := networkStatsProvider.GetHNSEndpointStats(endpoint.Id)
		if err != nil {
			logger.V(2).Info("Failed to fetch statistics for endpoint, continue to get stats for other endpoints", "endpointId", endpoint.Id, "containers", endpoint.SharedContainers)
			continue
		}

		// 同一容器可能共享多个端点：已存在且端点名不同时追加为额外接口。
		for _, cId := range endpoint.SharedContainers {
			networkStat, found := networkStats[cId]
			if found && networkStat.Name != endpoint.Name {
				iStat := hcsStatToInterfaceStat(endpointStats, endpoint.Name)
				networkStat.Interfaces = append(networkStat.Interfaces, iStat)
				continue
			}
			networkStats[cId] = hcsStatsToNetworkStats(p.clock.Now(), endpointStats, endpoint.Name)
		}
	}

	return networkStats, nil
}

// addCRIPodContainerStats 把 CRI 沙箱统计中的容器数据补充到 PodStats。
func (p *criStatsProvider) addCRIPodContainerStats(logger klog.Logger, criSandboxStat *runtimeapi.PodSandboxStats,
	ps *statsapi.PodStats, fsIDtoInfo map[string]*cadvisorapi.FsInfo,
	containerMap map[string]*runtimeapi.Container,
	podSandbox *runtimeapi.PodSandbox,
	rootFsInfo *cadvisorapi.FsInfo,
	updateCPUNanoCoreUsage bool) error {
	for _, criContainerStat := range criSandboxStat.GetWindows().GetContainers() {
		container, found := containerMap[criContainerStat.Attributes.Id]
		if !found {
			continue
		}
		// 填充 summary API 所需的容器级统计。
		cs, err := p.makeWinContainerStats(logger, criContainerStat, container, rootFsInfo, fsIDtoInfo, podSandbox.GetMetadata())
		if err != nil {
			return fmt.Errorf("make container stats: %w", err)

		}
		ps.Containers = append(ps.Containers, *cs)
	}

	return nil
}

// makeWinContainerStats 把 CRI 的 Windows 容器统计转换为 summary API 结构，
// 包括 CPU、内存、可写层和日志。
func (p *criStatsProvider) makeWinContainerStats(
	logger klog.Logger,
	stats *runtimeapi.WindowsContainerStats,
	container *runtimeapi.Container,
	rootFsInfo *cadvisorapi.FsInfo,
	fsIDtoInfo map[string]*cadvisorapi.FsInfo,
	meta *runtimeapi.PodSandboxMetadata) (*statsapi.ContainerStats, error) {
	result := &statsapi.ContainerStats{
		Name: stats.Attributes.Metadata.Name,
		// summary API 的 StartTime 取容器创建时间。
		StartTime: metav1.NewTime(time.Unix(0, container.CreatedAt)),
		CPU:       &statsapi.CPUStats{},
		Memory:    &statsapi.MemoryStats{},
		Rootfs:    &statsapi.FsStats{},
		// CRI 不支持用户自定义指标；CPU/内存缺失时下方会填零值，保证字段不为 nil。
	}
	if stats.Cpu != nil {
		result.CPU.Time = metav1.NewTime(time.Unix(0, stats.Cpu.Timestamp))
		if stats.Cpu.UsageCoreNanoSeconds != nil {
			result.CPU.UsageCoreNanoSeconds = &stats.Cpu.UsageCoreNanoSeconds.Value
		}
		if stats.Cpu.UsageNanoCores != nil {
			result.CPU.UsageNanoCores = &stats.Cpu.UsageNanoCores.Value
		}
	} else {
		result.CPU.Time = metav1.NewTime(time.Unix(0, time.Now().UnixNano()))
		result.CPU.UsageCoreNanoSeconds = ptr.To[uint64](0)
		result.CPU.UsageNanoCores = ptr.To[uint64](0)
	}
	if stats.Memory != nil {
		result.Memory.Time = metav1.NewTime(time.Unix(0, stats.Memory.Timestamp))
		if stats.Memory.WorkingSetBytes != nil {
			result.Memory.WorkingSetBytes = &stats.Memory.WorkingSetBytes.Value
		}
		if stats.Memory.AvailableBytes != nil {
			result.Memory.AvailableBytes = &stats.Memory.AvailableBytes.Value
		}
		if stats.Memory.PageFaults != nil {
			result.Memory.PageFaults = &stats.Memory.PageFaults.Value
		}
		if stats.Memory.CommitMemoryBytes != nil {
			result.Memory.UsageBytes = &stats.Memory.CommitMemoryBytes.Value
		}
	} else {
		result.Memory.Time = metav1.NewTime(time.Unix(0, time.Now().UnixNano()))
		result.Memory.WorkingSetBytes = ptr.To[uint64](0)
		result.Memory.AvailableBytes = ptr.To[uint64](0)
		result.Memory.PageFaults = ptr.To[uint64](0)
		result.Memory.UsageBytes = ptr.To[uint64](0)
	}
	if stats.WritableLayer != nil {
		result.Rootfs.Time = metav1.NewTime(time.Unix(0, stats.WritableLayer.Timestamp))
		if stats.WritableLayer.UsedBytes != nil {
			result.Rootfs.UsedBytes = &stats.WritableLayer.UsedBytes.Value
		}
	}
	var err error
	fsID := stats.GetWritableLayer().GetFsId()
	if fsID != nil {
		imageFsInfo, found := fsIDtoInfo[fsID.Mountpoint]
		if !found {
			imageFsInfo, err = p.getFsInfo(logger, fsID)
			if err != nil {
				return nil, fmt.Errorf("get filesystem info: %w", err)
			}
			fsIDtoInfo[fsID.Mountpoint] = imageFsInfo
		}
		if imageFsInfo != nil {
			// 镜像文件系统 ID 在本节点未知或读取统计出错时，省略该部分数据，
			// 返回尽力而为的部分结果。参见
			// https://github.com/kubernetes/heapster/issues/1793。
			result.Rootfs.AvailableBytes = &imageFsInfo.Available
			result.Rootfs.CapacityBytes = &imageFsInfo.Capacity
		}
	}
	// 注意：这里不支持旧的 Pod 日志路径 /var/log/pods/UID；使用旧路径的容器会得到
	// 空的日志统计。由于官方本就不支持就地升级，这一行为可以接受。
	result.Logs, err = p.hostStatsProvider.getPodContainerLogStats(meta.GetNamespace(), meta.GetName(), types.UID(meta.GetUid()), container.GetMetadata().GetName(), rootFsInfo)
	if err != nil {
		logger.Error(err, "Unable to fetch container log stats", "containerName", container.GetMetadata().GetName())
	}
	return result, nil
}

// hcsStatsToNetworkStats 把 HNS 端点统计转换为 statsapi.NetworkStats。
func hcsStatsToNetworkStats(timestamp time.Time, hcsStats *hnslib.HNSEndpointStats, endpointName string) *statsapi.NetworkStats {
	result := &statsapi.NetworkStats{
		Time:       metav1.NewTime(timestamp),
		Interfaces: make([]statsapi.InterfaceStats, 0),
	}

	iStat := hcsStatToInterfaceStat(hcsStats, endpointName)

	// TODO: add support of multiple interfaces for getting default interface.
	// 当前只能把单个接口同时作为默认接口和接口列表的唯一元素。
	result.Interfaces = append(result.Interfaces, iStat)
	result.InterfaceStats = iStat

	return result
}

// hcsStatToInterfaceStat 把 HNS 端点统计转换为单个接口的收发字节数。
func hcsStatToInterfaceStat(hcsStats *hnslib.HNSEndpointStats, endpointName string) statsapi.InterfaceStats {
	iStat := statsapi.InterfaceStats{
		Name:    endpointName,
		RxBytes: &hcsStats.BytesReceived,
		TxBytes: &hcsStats.BytesSent,
	}
	return iStat
}

// addCRIPodCPUStats 填充 Pod 级 CPU 统计，缺少数据时保持字段为空。
func addCRIPodCPUStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
	if criPodStat == nil || criPodStat.Windows == nil || criPodStat.Windows.Cpu == nil {
		return
	}
	criCPU := criPodStat.Windows.Cpu
	ps.CPU = &statsapi.CPUStats{
		Time:                 metav1.NewTime(time.Unix(0, criCPU.Timestamp)),
		UsageNanoCores:       valueOfUInt64Value(criCPU.UsageNanoCores),
		UsageCoreNanoSeconds: valueOfUInt64Value(criCPU.UsageCoreNanoSeconds),
	}
}

// addCRIPodMemoryStats 填充 Pod 级内存统计，提交内存映射为 UsageBytes。
func addCRIPodMemoryStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
	if criPodStat == nil || criPodStat.Windows == nil || criPodStat.Windows.Memory == nil {
		return
	}
	criMemory := criPodStat.Windows.Memory
	ps.Memory = &statsapi.MemoryStats{
		Time:            metav1.NewTime(time.Unix(0, criMemory.Timestamp)),
		AvailableBytes:  valueOfUInt64Value(criMemory.AvailableBytes),
		UsageBytes:      valueOfUInt64Value(criMemory.CommitMemoryBytes),
		WorkingSetBytes: valueOfUInt64Value(criMemory.WorkingSetBytes),
		PageFaults:      valueOfUInt64Value(criMemory.PageFaults),
	}
}

// addCRIPodIOStats 在 Windows 上暂不提供 IO 统计，保留空实现以对齐接口。
func addCRIPodIOStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
}

// addCRIPodProcessStats 填充 Pod 级进程数量统计。
func addCRIPodProcessStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
	if criPodStat == nil || criPodStat.Windows == nil || criPodStat.Windows.Process == nil {
		return
	}
	ps.ProcessStats = &statsapi.ProcessStats{
		ProcessCount: valueOfUInt64Value(criPodStat.Windows.Process.ProcessCount),
	}
}

// addCRIPodNetworkStats 填充 Pod 级网络统计：默认接口加全部接口列表。
func addCRIPodNetworkStats(ps *statsapi.PodStats, criPodStat *runtimeapi.PodSandboxStats) {
	if criPodStat == nil || criPodStat.Windows == nil || criPodStat.Windows.Network == nil {
		return
	}
	criNetwork := criPodStat.Windows.Network
	iStats := statsapi.NetworkStats{
		Time:           metav1.NewTime(time.Unix(0, criNetwork.Timestamp)),
		InterfaceStats: criInterfaceToWinSummary(criNetwork.DefaultInterface),
		Interfaces:     make([]statsapi.InterfaceStats, 0, len(criNetwork.Interfaces)),
	}
	for _, iface := range criNetwork.Interfaces {
		iStats.Interfaces = append(iStats.Interfaces, criInterfaceToWinSummary(iface))
	}
	ps.Network = &iStats
}

// criInterfaceToWinSummary 把 CRI 网络接口用量转换为 summary API 的接口统计。
func criInterfaceToWinSummary(criIface *runtimeapi.WindowsNetworkInterfaceUsage) statsapi.InterfaceStats {
	return statsapi.InterfaceStats{
		Name:    criIface.Name,
		RxBytes: valueOfUInt64Value(criIface.RxBytes),
		TxBytes: valueOfUInt64Value(criIface.TxBytes),
	}
}

// addCRIPodContainerCPUAndMemoryStats 只填充容器 CPU 和内存统计，供资源指标接口使用。
func (p *criStatsProvider) addCRIPodContainerCPUAndMemoryStats(
	criSandboxStat *runtimeapi.PodSandboxStats,
	ps *statsapi.PodStats,
	containerMap map[string]*runtimeapi.Container) {
	if criSandboxStat == nil || criSandboxStat.Windows == nil {
		return
	}
	for _, criContainerStat := range criSandboxStat.Windows.Containers {
		container, found := containerMap[criContainerStat.Attributes.Id]
		if !found {
			continue
		}
		// 资源指标只需要 CPU/内存，避免做文件系统和日志等昂贵采集。
		cs := p.makeWinContainerCPUAndMemoryStats(criContainerStat, time.Unix(0, container.CreatedAt))
		ps.Containers = append(ps.Containers, *cs)
	}
}

// makeWinContainerCPUAndMemoryStats 创建只含 CPU 和内存的容器统计，
// 是资源指标接口的轻量实现。
func (p *criStatsProvider) makeWinContainerCPUAndMemoryStats(
	stats *runtimeapi.WindowsContainerStats,
	startTime time.Time,
) *statsapi.ContainerStats {
	result := &statsapi.ContainerStats{
		Name:      stats.Attributes.Metadata.Name,
		StartTime: metav1.NewTime(startTime),
	}
	if stats.Cpu != nil {
		result.CPU = &statsapi.CPUStats{
			Time:                 metav1.NewTime(time.Unix(0, stats.Cpu.Timestamp)),
			UsageCoreNanoSeconds: ptr.To(stats.Cpu.UsageCoreNanoSeconds.GetValue()),
			UsageNanoCores:       ptr.To(stats.Cpu.UsageNanoCores.GetValue()),
		}
	} else {
		result.CPU = &statsapi.CPUStats{
			Time:                 metav1.NewTime(time.Unix(0, time.Now().UnixNano())),
			UsageCoreNanoSeconds: ptr.To[uint64](0),
			UsageNanoCores:       ptr.To[uint64](0),
		}
	}
	if stats.Memory != nil {
		result.Memory = &statsapi.MemoryStats{
			Time:            metav1.NewTime(time.Unix(0, stats.Memory.Timestamp)),
			WorkingSetBytes: ptr.To(stats.Memory.WorkingSetBytes.GetValue()),
			AvailableBytes:  ptr.To(stats.Memory.AvailableBytes.GetValue()),
			PageFaults:      ptr.To(stats.Memory.PageFaults.GetValue()),
		}
	} else {
		result.Memory = &statsapi.MemoryStats{
			Time:            metav1.NewTime(time.Unix(0, time.Now().UnixNano())),
			WorkingSetBytes: ptr.To[uint64](0),
			AvailableBytes:  ptr.To[uint64](0),
			PageFaults:      ptr.To[uint64](0),
		}
	}
	return result
}

// newNetworkStatsProvider 默认使用真实的 Windows hnslib；
// 测试场景下如果 criStatsProvider 注入了实现，则使用注入的版本。
func newNetworkStatsProvider(p *criStatsProvider) windowsNetworkStatsProvider {
	var statsProvider windowsNetworkStatsProvider
	if p.windowsNetworkStatsProvider == nil {
		statsProvider = networkStats{}
	} else {
		statsProvider = p.windowsNetworkStatsProvider.(windowsNetworkStatsProvider)
	}
	return statsProvider
}
