/*
Copyright 2017 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
	"k8s.io/utils/ptr"
)

// defaultNetworkInterfaceName 用于采集网络统计时识别默认网卡。
// 该逻辑依赖对容器运行时实现的既有认知，并不可靠。
const defaultNetworkInterfaceName = "eth0"

// cadvisorInfoToCPUandMemoryStats 把 cAdvisor 的容器信息转换为 CPU 与内存统计。
// 没有最新统计时返回空值；对应资源未启用时只返回零值统计，交由调用方处理。
func cadvisorInfoToCPUandMemoryStats(info *cadvisorapi.ContainerInfo) (*statsapi.CPUStats, *statsapi.MemoryStats) {
	cstat, found := latestContainerStats(info)
	if !found {
		return nil, nil
	}
	var cpuStats *statsapi.CPUStats
	var memoryStats *statsapi.MemoryStats
	// 先填充零值统计，保证即使 cAdvisor 未上报细项也有可展示的结构。
	cpuStats = &statsapi.CPUStats{
		Time:                 metav1.NewTime(cstat.Timestamp),
		UsageNanoCores:       ptr.To[uint64](0),
		UsageCoreNanoSeconds: ptr.To[uint64](0),
	}
	// 仅当容器声明了 CPU 资源时才读取 CPU 用量，避免取到不相关的数据。
	if info.Spec.HasCpu {
		if cstat.CpuInst != nil {
			// 指令级统计给出瞬时 CPU 用量，存在时优先采用。
			cpuStats.UsageNanoCores = &cstat.CpuInst.Usage.Total
		}
		if cstat.Cpu != nil {
			// 累计用量与 PSI 来自周期采样。
			cpuStats.UsageCoreNanoSeconds = &cstat.Cpu.Usage.Total
			// PSI 仅在启用 KubeletPSI 特性门控时输出，避免为未启用的节点引入额外开销。
			if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
				cpuStats.PSI = cadvisorPSIToStatsPSI(&cstat.Cpu.PSI)
			}
		}
	}
	// 内存统计要求容器声明内存且 cAdvisor 上报了内存数据。
	if info.Spec.HasMemory && cstat.Memory != nil {
		pageFaults := cstat.Memory.ContainerData.Pgfault
		majorPageFaults := cstat.Memory.ContainerData.Pgmajfault
		memoryStats = &statsapi.MemoryStats{
			Time:            metav1.NewTime(cstat.Timestamp),
			UsageBytes:      &cstat.Memory.Usage,
			WorkingSetBytes: &cstat.Memory.WorkingSet,
			RSSBytes:        &cstat.Memory.RSS,
			PageFaults:      &pageFaults,
			MajorPageFaults: &majorPageFaults,
		}
		// 可用内存 = 内存上限（已知时） - 工作集。
		if !isMemoryUnlimited(info.Spec.Memory.Limit) {
			availableBytes := info.Spec.Memory.Limit - cstat.Memory.WorkingSet
			memoryStats.AvailableBytes = &availableBytes
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
			memoryStats.PSI = cadvisorPSIToStatsPSI(&cstat.Memory.PSI)
		}
	} else {
		memoryStats = &statsapi.MemoryStats{
			Time:            metav1.NewTime(cstat.Timestamp),
			WorkingSetBytes: ptr.To[uint64](0),
		}
	}
	return cpuStats, memoryStats
}

// cadvisorInfoToContainerStats 基于容器信息与文件系统信息，转换为 statsapi.ContainerStats。
func cadvisorInfoToContainerStats(logger klog.Logger, name string, info *cadvisorapi.ContainerInfo, rootFs, imageFs *cadvisorapi.FsInfo) *statsapi.ContainerStats {
	result := &statsapi.ContainerStats{
		StartTime: metav1.NewTime(info.Spec.CreationTime),
		Name:      name,
	}
	cstat, found := latestContainerStats(info)
	if !found {
		// 没有可用的 cAdvisor 统计时，只返回含基础元信息的空统计。
		return result
	}

	cpu, memory := cadvisorInfoToCPUandMemoryStats(info)
	result.CPU = cpu
	result.Memory = memory
	result.Swap = cadvisorInfoToSwapStats(info)
	// IO 统计依赖 PSI 特性门控，未启用时直接跳过，减少无谓的转换。
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		result.IO = cadvisorInfoToIOStats(info)
	}

	// NOTE: 若能获取到更准确的日志统计，调用方会覆盖这里的值，
	// 因为调用方掌握更多 pod 信息，可据此确定日志大小。
	if rootFs != nil {
		// 容器日志存放在节点根文件系统上，因此从 rootFs 构建日志统计。
		result.Logs = buildLogsStats(cstat, rootFs)
	}

	if imageFs != nil {
		// 容器根文件系统可能位于镜像文件系统设备上（未必是节点根文件系统），故从 imageFs 构建。
		result.Rootfs = buildRootfsStats(cstat, imageFs)
	}

	// 只有单个文件系统设备时才填充汇总统计，多设备容器直接跳过。
	var cfs *cadvisorapi.FilesystemSummary
	if len(cstat.Filesystem) == 1 {
		fsStat := cstat.Filesystem[0]
		cfs = &cadvisorapi.FilesystemSummary{
			BaseUsageBytes:  &fsStat.BaseUsage,
			TotalUsageBytes: &fsStat.Usage,
			InodeUsage:      &fsStat.Inodes,
		}
	}
	if cfs != nil {
		if cfs.BaseUsageBytes != nil {
			if result.Rootfs != nil {
				rootfsUsage := *cfs.BaseUsageBytes
				result.Rootfs.UsedBytes = &rootfsUsage
			}
			if cfs.TotalUsageBytes != nil && result.Logs != nil {
				// 单设备汇总中，总用量减去基础用量即日志用量。
				logsUsage := *cfs.TotalUsageBytes - *cfs.BaseUsageBytes
				result.Logs.UsedBytes = &logsUsage
			}
		}
		if cfs.InodeUsage != nil && result.Rootfs != nil {
			rootInodes := *cfs.InodeUsage
			result.Rootfs.InodesUsed = &rootInodes
		}
	}

	for _, acc := range cstat.Accelerators {
		result.Accelerators = append(result.Accelerators, statsapi.AcceleratorStats{
			Make:        acc.Make,
			Model:       acc.Model,
			ID:          acc.ID,
			MemoryTotal: acc.MemoryTotal,
			MemoryUsed:  acc.MemoryUsed,
			DutyCycle:   acc.DutyCycle,
		})
	}

	result.UserDefinedMetrics = cadvisorInfoToUserDefinedMetrics(logger, info)

	return result
}

// cadvisorInfoToContainerCPUAndMemoryStats 基于容器信息转换为仅含 CPU、内存与 swap 的
// statsapi.ContainerStats，用于只关心核心资源用量的统计场景。
func cadvisorInfoToContainerCPUAndMemoryStats(name string, info *cadvisorapi.ContainerInfo) *statsapi.ContainerStats {
	result := &statsapi.ContainerStats{
		StartTime: metav1.NewTime(info.Spec.CreationTime),
		Name:      name,
	}

	cpu, memory := cadvisorInfoToCPUandMemoryStats(info)
	result.CPU = cpu
	result.Memory = memory
	result.Swap = cadvisorInfoToSwapStats(info)

	return result
}

// cadvisorInfoToProcessStats 提取 cAdvisor 容器信息中的进程数量统计。
// 无最新统计或进程数据缺失时返回 nil，表示该容器没有可用的进程统计。
func cadvisorInfoToProcessStats(info *cadvisorapi.ContainerInfo) *statsapi.ProcessStats {
	cstat, found := latestContainerStats(info)
	if !found || cstat.Processes == nil {
		return nil
	}
	num := cstat.Processes.ProcessCount
	return &statsapi.ProcessStats{ProcessCount: ptr.To[uint64](num)}
}

// mergeProcessStats 合并两个进程统计，把各自的进程数相加。
// 任一侧为 nil 时直接返回另一侧，两侧均为 nil 时返回 nil。
func mergeProcessStats(first *statsapi.ProcessStats, second *statsapi.ProcessStats) *statsapi.ProcessStats {
	if first == nil && second == nil {
		return nil
	}

	if first == nil {
		return second
	}
	if second == nil {
		return first
	}

	firstProcessCount := uint64(0)
	// 指针解引用前先判空，避免 nil 指针解引用。
	if first.ProcessCount != nil {
		firstProcessCount = *first.ProcessCount
	}

	secondProcessCount := uint64(0)
	if second.ProcessCount != nil {
		secondProcessCount = *second.ProcessCount
	}

	return &statsapi.ProcessStats{ProcessCount: ptr.To[uint64](firstProcessCount + secondProcessCount)}
}

// cadvisorInfoToNetworkStats 把 cAdvisor 容器信息转换为 statsapi.NetworkStats。
// 容器未启用网络、无统计或网络数据缺失时返回 nil。
func cadvisorInfoToNetworkStats(info *cadvisorapi.ContainerInfo) *statsapi.NetworkStats {
	// 容器未声明网络时直接返回 nil，避免构造无意义的空统计。
	if !info.Spec.HasNetwork {
		return nil
	}
	cstat, found := latestContainerStats(info)
	if !found {
		return nil
	}

	if cstat.Network == nil {
		return nil
	}

	iStats := statsapi.NetworkStats{
		Time: metav1.NewTime(cstat.Timestamp),
	}

	for i := range cstat.Network.Interfaces {
		inter := cstat.Network.Interfaces[i]
		iStat := statsapi.InterfaceStats{
			Name:     inter.Name,
			RxBytes:  &inter.RxBytes,
			RxErrors: &inter.RxErrors,
			TxBytes:  &inter.TxBytes,
			TxErrors: &inter.TxErrors,
		}

		if inter.Name == defaultNetworkInterfaceName {
			// 仅把默认网卡设为主接口统计，其余网卡仍保留在列表里供查询。
			iStats.InterfaceStats = iStat
		}

		iStats.Interfaces = append(iStats.Interfaces, iStat)
	}

	return &iStats
}

// cadvisorInfoToUserDefinedMetrics 把 cAdvisor 容器信息中的自定义指标
// 转换为 statsapi.UserDefinedMetric 列表。
func cadvisorInfoToUserDefinedMetrics(logger klog.Logger, info *cadvisorapi.ContainerInfo) []statsapi.UserDefinedMetric {
	type specVal struct {
		ref     statsapi.UserDefinedMetricDescriptor
		valType cadvisorapi.DataType
		time    time.Time
		value   float64
	}
	// 先用 spec 建立指标描述与取值类型的映射，后续按名称把采样值与之对齐。
	udmMap := map[string]*specVal{}
	for _, spec := range info.Spec.CustomMetrics {
		udmMap[spec.Name] = &specVal{
			ref: statsapi.UserDefinedMetricDescriptor{
				Name:  spec.Name,
				Type:  statsapi.UserDefinedMetricType(spec.Type),
				Units: spec.Units,
			},
			valType: spec.Format,
		}
	}
	for _, stat := range info.Stats {
		for name, values := range stat.CustomMetrics {
			specVal, ok := udmMap[name]
			if !ok {
				// cAdvisor 输出缺少该指标的 spec 时无法确认单位与类型，记录日志后跳过。
				logger.Info("Spec for custom metric is missing from cAdvisor output", "metric", name, "spec", info.Spec, "metrics", stat.CustomMetrics)
				continue
			}
			for _, value := range values {
				// 同一指标可能有多个采样点，只保留时间戳最新的一个作为当前值。
				if value.Timestamp.Before(specVal.time) {
					continue
				}
				specVal.time = value.Timestamp
				specVal.value = value.FloatValue
				// 整数型指标用 IntValue 换算，避免浮点精度丢失。
				if specVal.valType == cadvisorapi.IntType {
					specVal.value = float64(value.IntValue)
				}
			}
		}
	}
	var udm []statsapi.UserDefinedMetric
	for _, specVal := range udmMap {
		udm = append(udm, statsapi.UserDefinedMetric{
			UserDefinedMetricDescriptor: specVal.ref,
			Time:                        metav1.NewTime(specVal.time),
			Value:                       specVal.value,
		})
	}
	return udm
}

// cadvisorInfoToSwapStats 把 cAdvisor 容器信息转换为 swap 统计。
// 容器未启用内存或缺少内存统计时返回 nil。
func cadvisorInfoToSwapStats(info *cadvisorapi.ContainerInfo) *statsapi.SwapStats {
	cstat, found := latestContainerStats(info)
	if !found {
		return nil
	}

	var swapStats *statsapi.SwapStats

	if info.Spec.HasMemory && cstat.Memory != nil {
		swapStats = &statsapi.SwapStats{
			Time:           metav1.NewTime(cstat.Timestamp),
			SwapUsageBytes: &cstat.Memory.Swap,
		}

		if !isMemoryUnlimited(info.Spec.Memory.SwapLimit) {
			// 仅在上限已知时才给出可用 swap 估算，否则没有意义。
			swapAvailableBytes := info.Spec.Memory.SwapLimit - cstat.Memory.Swap
			swapStats.SwapAvailableBytes = &swapAvailableBytes
		}
	}

	return swapStats
}

// cadvisorInfoToIOStats 把 cAdvisor 容器信息转换为 IO 统计并附带 PSI 压力数据；
// 容器未启用磁盘 IO 或缺少统计时返回 nil。
func cadvisorInfoToIOStats(info *cadvisorapi.ContainerInfo) *statsapi.IOStats {
	cstat, found := latestContainerStats(info)
	if !found {
		return nil
	}

	var ioStats *statsapi.IOStats

	if info.Spec.HasDiskIo && cstat.DiskIo != nil {
		ioStats = &statsapi.IOStats{
			Time: metav1.NewTime(cstat.Timestamp),
			PSI:  cadvisorPSIToStatsPSI(&cstat.DiskIo.PSI),
		}
	}

	return ioStats
}

// latestContainerStats 返回 cAdvisor 容器信息中最新的一条统计；
// 没有任何统计或最新一条为空时返回 (nil, false)。
func latestContainerStats(info *cadvisorapi.ContainerInfo) (*cadvisorapi.ContainerStats, bool) {
	stats := info.Stats
	if len(stats) < 1 {
		return nil, false
	}
	latest := stats[len(stats)-1]
	if latest == nil {
		return nil, false
	}
	return latest, true
}

// isMemoryUnlimited 判断给定的内存/swap 大小是否应视为“无上限”。
func isMemoryUnlimited(v uint64) bool {
	// 超过该大小即视作“无上限”。未使用 MaxInt64，是因为内核换算时会做舍入。
	// TODO: cadvisor should export this https://github.com/google/cadvisor/blob/master/metrics/prometheus.go#L596
	const maxMemorySize = uint64(1 << 62)

	return v > maxMemorySize
}

// getCgroupInfo 从 cAdvisor 获取指定容器名的容器信息。
// updateStats 为 true 时要求 cAdvisor 返回实时统计，以便得到最新的指标。
func getCgroupInfo(cadvisor cadvisor.Interface, containerName string, updateStats bool) (*cadvisorapi.ContainerInfo, error) {
	var maxAge *time.Duration
	if updateStats {
		// 把最大样本年龄设为 0，强制 cAdvisor 返回当前统计而非缓存值。
		age := 0 * time.Second
		maxAge = &age
	}
	// 需要两个样本才能计算出“瞬时”CPU 用量。
	infoMap, err := cadvisor.ContainerInfoV2(containerName, cadvisorapi.RequestOptions{
		IdType:    cadvisorapi.TypeName,
		Count:     2,
		Recursive: false,
		MaxAge:    maxAge,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get container info for %q: %w", containerName, err)
	}
	// 期望按容器名恰好命中一个容器，数量不符说明查询条件或运行时行为异常。
	if len(infoMap) != 1 {
		return nil, fmt.Errorf("unexpected number of containers: %v", len(infoMap))
	}
	info := infoMap[containerName]
	return &info, nil
}

// getCgroupStats 从 cAdvisor 获取指定容器的最新统计。
func getCgroupStats(cadvisor cadvisor.Interface, containerName string, updateStats bool) (*cadvisorapi.ContainerStats, error) {
	info, err := getCgroupInfo(cadvisor, containerName, updateStats)
	if err != nil {
		return nil, err
	}
	stats, found := latestContainerStats(info)
	if !found {
		return nil, fmt.Errorf("failed to get latest stats from container info for %q", containerName)
	}
	return stats, nil
}

// buildLogsStats 基于容器统计与节点根文件系统信息构建容器日志文件系统统计。
func buildLogsStats(cstat *cadvisorapi.ContainerStats, rootFs *cadvisorapi.FsInfo) *statsapi.FsStats {
	fsStats := &statsapi.FsStats{
		Time:           metav1.NewTime(cstat.Timestamp),
		AvailableBytes: &rootFs.Available,
		CapacityBytes:  &rootFs.Capacity,
		InodesFree:     rootFs.InodesFree,
		Inodes:         rootFs.Inodes,
	}

	if rootFs.Inodes != nil && rootFs.InodesFree != nil {
		// inode 使用量 = 总 inode - 空闲 inode，两者都已知时才能计算。
		logsInodesUsed := *rootFs.Inodes - *rootFs.InodesFree
		fsStats.InodesUsed = &logsInodesUsed
	}
	return fsStats
}

// buildRootfsStats 基于容器统计与镜像文件系统信息构建容器根文件系统统计。
func buildRootfsStats(cstat *cadvisorapi.ContainerStats, imageFs *cadvisorapi.FsInfo) *statsapi.FsStats {
	return &statsapi.FsStats{
		Time:           metav1.NewTime(cstat.Timestamp),
		AvailableBytes: &imageFs.Available,
		CapacityBytes:  &imageFs.Capacity,
		InodesFree:     imageFs.InodesFree,
		Inodes:         imageFs.Inodes,
	}
}

// calcEphemeralStorage 汇总容器、卷、pod 日志与 /etc/hosts 的用量，
// 得到 pod 临时存储的整体文件系统统计。
func calcEphemeralStorage(containers []statsapi.ContainerStats, volumes []statsapi.VolumeStats, rootFsInfo *cadvisorapi.FsInfo,
	podLogStats *statsapi.FsStats, etcHostsStats *statsapi.FsStats, isCRIStatsProvider bool) *statsapi.FsStats {
	result := &statsapi.FsStats{
		Time:           metav1.NewTime(rootFsInfo.Timestamp),
		AvailableBytes: &rootFsInfo.Available,
		CapacityBytes:  &rootFsInfo.Capacity,
		InodesFree:     rootFsInfo.InodesFree,
		Inodes:         rootFsInfo.Inodes,
	}
	for _, container := range containers {
		addContainerUsage(result, &container, isCRIStatsProvider)
	}
	// 卷用量按字段累加，时间戳取各来源中最晚的一次更新，保证结果反映最新状态。
	for _, volume := range volumes {
		result.UsedBytes = addUsage(result.UsedBytes, volume.FsStats.UsedBytes)
		result.InodesUsed = addUsage(result.InodesUsed, volume.InodesUsed)
		result.Time = maxUpdateTime(&result.Time, &volume.FsStats.Time)
	}
	if podLogStats != nil {
		result.UsedBytes = addUsage(result.UsedBytes, podLogStats.UsedBytes)
		result.InodesUsed = addUsage(result.InodesUsed, podLogStats.InodesUsed)
		result.Time = maxUpdateTime(&result.Time, &podLogStats.Time)
	}
	if etcHostsStats != nil {
		result.UsedBytes = addUsage(result.UsedBytes, etcHostsStats.UsedBytes)
		result.InodesUsed = addUsage(result.InodesUsed, etcHostsStats.InodesUsed)
		result.Time = maxUpdateTime(&result.Time, &etcHostsStats.Time)
	}
	return result
}

// addContainerUsage 把单个容器的根文件系统与日志用量累加进汇总统计。
func addContainerUsage(stat *statsapi.FsStats, container *statsapi.ContainerStats, isCRIStatsProvider bool) {
	if rootFs := container.Rootfs; rootFs != nil {
		stat.Time = maxUpdateTime(&stat.Time, &rootFs.Time)
		stat.InodesUsed = addUsage(stat.InodesUsed, rootFs.InodesUsed)
		stat.UsedBytes = addUsage(stat.UsedBytes, rootFs.UsedBytes)
		if logs := container.Logs; logs != nil {
			stat.UsedBytes = addUsage(stat.UsedBytes, logs.UsedBytes)
			// 日志的 inode 用量只在 CRI 统计提供方下可信，其他提供方不计入，以免重复计算。
			if isCRIStatsProvider {
				stat.InodesUsed = addUsage(stat.InodesUsed, logs.InodesUsed)
			}
			stat.Time = maxUpdateTime(&stat.Time, &logs.Time)
		}
	}
}

// maxUpdateTime 返回两个时间中较晚的一个，使统计结果反映最新采样时刻。
func maxUpdateTime(first, second *metav1.Time) metav1.Time {
	if first.Before(second) {
		return *second
	}
	return *first
}

// addUsage 累加两个可空的用量指针：任一侧为 nil 时返回另一侧，
// 避免用 0 覆盖本来缺失的数据。
func addUsage(first, second *uint64) *uint64 {
	if first == nil {
		return second
	} else if second == nil {
		return first
	}
	total := *first + *second
	return &total
}

// makePodStorageStats 填充 pod 的临时存储统计：合并卷用量，并基于日志与
// /etc/hosts 统计计算整个 pod 的临时存储占用。
func makePodStorageStats(logger klog.Logger, s *statsapi.PodStats, rootFsInfo *cadvisorapi.FsInfo, resourceAnalyzer stats.ResourceAnalyzer, hostStatsProvider HostStatsProvider, isCRIStatsProvider bool) {
	podNs := s.PodRef.Namespace
	podName := s.PodRef.Name
	podUID := types.UID(s.PodRef.UID)
	var ephemeralStats []statsapi.VolumeStats
	if vstats, found := resourceAnalyzer.GetPodVolumeStats(podUID); found {
		// 先拷贝一份临时卷统计，避免与资源分析器内部的切片共享底层数组。
		ephemeralStats = make([]statsapi.VolumeStats, len(vstats.EphemeralVolumes))
		copy(ephemeralStats, vstats.EphemeralVolumes)
		s.VolumeStats = append(append([]statsapi.VolumeStats{}, vstats.EphemeralVolumes...), vstats.PersistentVolumes...)
	}

	logStats, err := hostStatsProvider.getPodLogStats(podNs, podName, podUID, rootFsInfo)
	if err != nil {
		logger.V(6).Info("Unable to fetch pod log stats", "pod", klog.KRef(podNs, podName), "err", err)
		// 原地升级后，部分 pod 可能仍在使用旧日志路径，此时取不到日志统计。
		// 这种情况应继续生成其余统计，calcEphemeralStorage 能容忍 logStats 为 nil。
	}
	etcHostsStats, err := hostStatsProvider.getPodEtcHostsStats(podUID, rootFsInfo)
	if err != nil {
		logger.V(6).Info("Unable to fetch pod etc hosts stats", "pod", klog.KRef(podNs, podName), "err", err)
	}
	s.EphemeralStorage = calcEphemeralStorage(s.Containers, ephemeralStats, rootFsInfo, logStats, etcHostsStats, isCRIStatsProvider)
}

// cadvisorPSIToStatsPSI 把 cAdvisor 的 PSI 统计转换为 statsapi.PSIStats；入参为 nil 时返回 nil。
func cadvisorPSIToStatsPSI(psi *cadvisorapi.PSIStats) *statsapi.PSIStats {
	if psi == nil {
		return nil
	}
	return &statsapi.PSIStats{
		Full: statsapi.PSIData{
			Total:  psi.Full.Total,
			Avg10:  psi.Full.Avg10,
			Avg60:  psi.Full.Avg60,
			Avg300: psi.Full.Avg300,
		},
		Some: statsapi.PSIData{
			Total:  psi.Some.Total,
			Avg10:  psi.Some.Avg10,
			Avg60:  psi.Some.Avg60,
			Avg300: psi.Some.Avg300,
		},
	}
}
