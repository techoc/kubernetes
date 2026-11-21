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
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	kubetypes "k8s.io/kubelet/pkg/types"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
	"k8s.io/kubernetes/pkg/kubelet/status"
)

// cadvisorStatsProvider 通过 cAdvisor 获取容器统计信息，实现 containerStatsProvider 接口。
// 该实现面向不从 CRI 提供统计信息的容器运行时集成；判断哪些集成不提供 CRI 统计的逻辑
// 见 `pkg/kubelet/cadvisor/util.go#UsingLegacyCadvisorStats`。
type cadvisorStatsProvider struct {
	// cadvisor 用于获取由 Pod 管理的容器的 cgroup 统计信息。
	cadvisor cadvisor.Interface
	// resourceAnalyzer 用于获取 Pod 的卷统计信息。
	resourceAnalyzer stats.ResourceAnalyzer
	// imageService 用于获取镜像文件系统的统计信息。
	imageService kubecontainer.ImageService
	// statusProvider 用于获取 Pod 元数据。
	statusProvider status.PodStatusProvider
	// hostStatsProvider 用于获取 Pod 在宿主机上的统计用量。
	hostStatsProvider HostStatsProvider
	// containerManager 用于生成 Pod 的 cgroup 路径。
	containerManager cm.ContainerManager
}

// newCadvisorStatsProvider 返回一个从 cAdvisor 获取容器统计信息的 containerStatsProvider。
func newCadvisorStatsProvider(
	cadvisor cadvisor.Interface,
	resourceAnalyzer stats.ResourceAnalyzer,
	imageService kubecontainer.ImageService,
	statusProvider status.PodStatusProvider,
	hostStatsProvider HostStatsProvider,
	containerManager cm.ContainerManager,
) containerStatsProvider {
	return &cadvisorStatsProvider{
		cadvisor:          cadvisor,
		resourceAnalyzer:  resourceAnalyzer,
		imageService:      imageService,
		statusProvider:    statusProvider,
		hostStatsProvider: hostStatsProvider,
		containerManager:  containerManager,
	}
}

// ListPodStats 返回所有由 Pod 管理的容器的统计信息。
func (p *cadvisorStatsProvider) ListPodStats(ctx context.Context) ([]statsapi.PodStats, error) {
	// 获取节点根文件系统信息和镜像文件系统统计，用于填充容器统计中的可用与容量字节/inode。
	rootFsInfo, err := p.cadvisor.RootFsInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get rootFs info: %v", err)
	}
	imageFsInfo, err := p.cadvisor.ImagesFsInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get imageFs info: %v", err)
	}
	logger := klog.FromContext(ctx)
	infos, err := getCadvisorContainerInfo(logger, p.cadvisor)
	if err != nil {
		return nil, fmt.Errorf("failed to get container info from cadvisor: %v", err)
	}
	filteredInfos, allInfos := filterTerminatedContainerInfoAndAssembleByPodCgroupKey(logger, infos)
	// 把每个容器映射到所属 Pod，并用容器数据更新 PodStats。
	podToStats := map[statsapi.PodReference]*statsapi.PodStats{}
	for key, cinfo := range filteredInfos {
		// systemd 使用 devicemapper 时，容器的每个挂载都会对应一个 cgroup。
		// 这里将其忽略，以确保汇总结果中不出现重复条目。.mount 单元的详细信息见：
		// http://man7.org/linux/man-pages/man5/systemd.mount.5.html
		if strings.HasSuffix(key, ".mount") {
			continue
		}
		// 如果该容器由 Pod 管理，则构建其所属 Pod 的标识键
		if !isPodManagedContainer(logger, &cinfo) {
			continue
		}
		ref := buildPodRef(cinfo.Spec.Labels)

		// 用 PodRef 查找对应的 PodStats；若尚不存在，则初始化一个新条目。
		podStats, found := podToStats[ref]
		if !found {
			podStats = &statsapi.PodStats{PodRef: ref}
			podToStats[ref] = podStats
		}

		// 把容器统计追加到 podStats.Containers，从而将容器数据更新到对应的 PodStats 条目。
		containerName := kubetypes.GetContainerName(cinfo.Spec.Labels)
		if containerName == kubetypes.PodInfraContainerName {
			// 基础容器是用户不可见的特殊情况，其统计信息就是网络统计。
			podStats.Network = cadvisorInfoToNetworkStats(&cinfo)
		} else {
			containerStat := cadvisorInfoToContainerStats(logger, containerName, &cinfo, &rootFsInfo, &imageFsInfo)
			// 注意：此处不支持旧的 Pod 日志路径 `/var/log/pods/UID`。使用旧日志路径的容器，
			// 其日志统计由 cadvisorInfoToContainerStats 填充。
			podUID := types.UID(podStats.PodRef.UID)
			logs, err := p.hostStatsProvider.getPodContainerLogStats(podStats.PodRef.Namespace, podStats.PodRef.Name, podUID, containerName, &rootFsInfo)
			if err != nil {
				logger.Error(err, "Unable to fetch container log stats", "containerName", containerName)
			} else {
				containerStat.Logs = logs
			}
			podStats.Containers = append(podStats.Containers, *containerStat)
		}
		// 无论哪种分支，都要收集进程统计信息
		podStats.ProcessStats = mergeProcessStats(podStats.ProcessStats, cadvisorInfoToProcessStats(&cinfo))
	}

	// 将每个 PodStats 追加到结果中。
	result := make([]statsapi.PodStats, 0, len(podToStats))
	for _, podStats := range podToStats {
		makePodStorageStats(logger, podStats, &rootFsInfo, p.resourceAnalyzer, p.hostStatsProvider, false)

		podUID := types.UID(podStats.PodRef.UID)
		// 查询 Pod 级 cgroup 的 CPU 和内存统计信息
		podInfo := getCadvisorPodInfoFromPodUID(podUID, allInfos)
		if podInfo != nil {
			cpu, memory := cadvisorInfoToCPUandMemoryStats(podInfo)
			podStats.CPU = cpu
			podStats.Memory = memory
			podStats.Swap = cadvisorInfoToSwapStats(podInfo)
			if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
				podStats.IO = cadvisorInfoToIOStats(podInfo)
			}
			// ProcessStats 已在遍历容器的过程中累积完成，此处无需额外处理。
		}

		status, found := p.statusProvider.GetPodStatus(podUID)
		if found && status.StartTime != nil && !status.StartTime.IsZero() {
			podStats.StartTime = *status.StartTime
			// 只有在成功获取到 Pod 启动时间后，才把该统计条目追加进结果。
			result = append(result, *podStats)
		}
	}

	return result, nil
}

// ListPodStatsAndUpdateCPUNanoCoreUsage 更新容器的 CPU 纳核使用量，并返回所有由 Pod 管理的
// 容器的统计信息。cAdvisor 的 CPU 纳核使用量是预先计算并缓存的，因此该函数直接复用
// ListPodStats。
func (p *cadvisorStatsProvider) ListPodStatsAndUpdateCPUNanoCoreUsage(ctx context.Context) ([]statsapi.PodStats, error) {
	return p.ListPodStats(ctx)
}

// PodCPUAndMemoryStats 返回指定 Pod 的 CPU 与内存统计信息，包括 Pod 级 cgroup 以及
// 各容器（不含基础容器）的 CPU/内存统计。
func (p *cadvisorStatsProvider) PodCPUAndMemoryStats(ctx context.Context, pod *v1.Pod, _ *kubecontainer.PodStatus) (*statsapi.PodStats, error) {
	_, podName := p.containerManager.NewPodContainerManager().GetPodContainerName(pod)
	infos, err := p.cadvisor.ContainerInfoV2(podName, cadvisorapi.RequestOptions{
		IdType:    cadvisorapi.TypeName,
		Count:     2,
		Recursive: true,
	})
	if err != nil {
		return nil, err
	}

	podStats := &statsapi.PodStats{
		PodRef: statsapi.PodReference{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			UID:       string(pod.UID),
		},
	}

	for name, info := range infos {
		if name == podName {
			// Pod 容器（即 Pod 级 cgroup）
			podStats.StartTime = metav1.NewTime(info.Spec.CreationTime)
			cpu, memory := cadvisorInfoToCPUandMemoryStats(&info)
			podStats.CPU = cpu
			podStats.Memory = memory
		} else {
			containerName := kubetypes.GetContainerName(info.Spec.Labels)
			if containerName != kubetypes.PodInfraContainerName && containerName != "" {
				podStats.Containers = append(podStats.Containers, *cadvisorInfoToContainerCPUAndMemoryStats(containerName, &info))
			}
		}
	}

	return podStats, nil
}

// ListPodCPUAndMemoryStats 返回所有由 Pod 管理的容器的 CPU 与内存统计信息。
func (p *cadvisorStatsProvider) ListPodCPUAndMemoryStats(ctx context.Context) ([]statsapi.PodStats, error) {
	logger := klog.FromContext(ctx)
	infos, err := getCadvisorContainerInfo(logger, p.cadvisor)
	if err != nil {
		return nil, fmt.Errorf("failed to get container info from cadvisor: %v", err)
	}
	filteredInfos, allInfos := filterTerminatedContainerInfoAndAssembleByPodCgroupKey(logger, infos)
	// 把每个容器映射到所属 Pod，并用容器数据更新 PodStats。
	podToStats := map[statsapi.PodReference]*statsapi.PodStats{}
	for key, cinfo := range filteredInfos {
		// systemd 使用 devicemapper 时，容器的每个挂载都会对应一个 cgroup。
		// 这里将其忽略，以确保汇总结果中不出现重复条目。.mount 单元的详细信息见：
		// http://man7.org/linux/man-pages/man5/systemd.mount.5.html
		if strings.HasSuffix(key, ".mount") {
			continue
		}
		// 如果该容器由 Pod 管理，则构建其所属 Pod 的标识键
		if !isPodManagedContainer(logger, &cinfo) {
			continue
		}
		ref := buildPodRef(cinfo.Spec.Labels)

		// 用 PodRef 查找对应的 PodStats；若尚不存在，则初始化一个新条目。
		podStats, found := podToStats[ref]
		if !found {
			podStats = &statsapi.PodStats{PodRef: ref}
			podToStats[ref] = podStats
		}

		// 把容器统计追加到 podStats.Containers，从而将容器数据更新到对应的 PodStats 条目。
		containerName := kubetypes.GetContainerName(cinfo.Spec.Labels)
		if containerName == kubetypes.PodInfraContainerName {
			// 基础容器对用户不可见，这里用它的创建时间作为 Pod 的启动时间。
			podStats.StartTime = metav1.NewTime(cinfo.Spec.CreationTime)
		} else {
			podStats.Containers = append(podStats.Containers, *cadvisorInfoToContainerCPUAndMemoryStats(containerName, &cinfo))
		}
	}

	// 将每个 PodStats 追加到结果中。
	result := make([]statsapi.PodStats, 0, len(podToStats))
	for _, podStats := range podToStats {
		podUID := types.UID(podStats.PodRef.UID)
		// 查询 Pod 级 cgroup 的 CPU 和内存统计信息
		podInfo := getCadvisorPodInfoFromPodUID(podUID, allInfos)
		if podInfo != nil {
			cpu, memory := cadvisorInfoToCPUandMemoryStats(podInfo)
			podStats.CPU = cpu
			podStats.Memory = memory
			podStats.Swap = cadvisorInfoToSwapStats(podInfo)
		}
		result = append(result, *podStats)
	}

	return result, nil
}

// ImageFsStats 返回用于存放镜像的文件系统的统计信息。
func (p *cadvisorStatsProvider) ImageFsStats(ctx context.Context) (imageFsRet *statsapi.FsStats, containerFsRet *statsapi.FsStats, errCall error) {
	imageFsInfo, err := p.cadvisor.ImagesFsInfo(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get imageFs info: %v", err)
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.KubeletSeparateDiskGC) {
		imageStats, err := p.imageService.ImageStats(ctx)
		if err != nil || imageStats == nil {
			return nil, nil, fmt.Errorf("failed to get image stats: %v", err)
		}

		var imageFsInodesUsed *uint64
		if imageFsInfo.Inodes != nil && imageFsInfo.InodesFree != nil {
			imageFsIU := *imageFsInfo.Inodes - *imageFsInfo.InodesFree
			imageFsInodesUsed = &imageFsIU
		}

		imageFs := &statsapi.FsStats{
			Time:           metav1.NewTime(imageFsInfo.Timestamp),
			AvailableBytes: &imageFsInfo.Available,
			CapacityBytes:  &imageFsInfo.Capacity,
			UsedBytes:      &imageStats.TotalStorageBytes,
			InodesFree:     imageFsInfo.InodesFree,
			Inodes:         imageFsInfo.Inodes,
			InodesUsed:     imageFsInodesUsed,
		}
		return imageFs, imageFs, nil
	}
	imageStats, err := p.imageService.ImageFsInfo(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get image stats: %w", err)
	}
	if imageStats == nil || len(imageStats.ImageFilesystems) == 0 || len(imageStats.ContainerFilesystems) == 0 {
		return nil, nil, fmt.Errorf("missing image stats: %+v", imageStats)
	}

	splitFileSystem := false
	imageFs := imageStats.ImageFilesystems[0]
	containerFs := imageStats.ContainerFilesystems[0]
	if imageFs.FsId != nil && containerFs.FsId != nil && imageFs.FsId.Mountpoint != containerFs.FsId.Mountpoint {
		splitFileSystem = true
	}
	var imageFsInodesUsed *uint64
	if imageFsInfo.Inodes != nil && imageFsInfo.InodesFree != nil {
		imageFsIU := *imageFsInfo.Inodes - *imageFsInfo.InodesFree
		imageFsInodesUsed = &imageFsIU
	}
	var usedBytes uint64
	if imageFs.GetUsedBytes() != nil {
		usedBytes = imageFs.GetUsedBytes().GetValue()
	}

	fsStats := &statsapi.FsStats{
		Time:           metav1.NewTime(imageFsInfo.Timestamp),
		AvailableBytes: &imageFsInfo.Available,
		CapacityBytes:  &imageFsInfo.Capacity,
		UsedBytes:      &usedBytes,
		InodesFree:     imageFsInfo.InodesFree,
		Inodes:         imageFsInfo.Inodes,
		InodesUsed:     imageFsInodesUsed,
	}
	// 拆分文件系统场景依赖 cadvisor 提供 crio-containers 标签。
	// 这里直接返回，避免再去查询 ContainerFsInfo。
	if !splitFileSystem {
		return fsStats, fsStats, nil
	}

	containerFsInfo, err := p.cadvisor.ContainerFsInfo(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get container fs info: %w", err)
	}
	// 镜像文件系统和容器文件系统可能位于同一设备的不同路径上。
	if containerFsInfo.Device == imageFsInfo.Device {
		return fsStats, fsStats, nil
	}

	logger := klog.FromContext(ctx)
	logger.Info("Detect Split Filesystem", "ImageFilesystems", imageStats.ImageFilesystems[0], "ContainerFilesystems", imageStats.ContainerFilesystems[0])

	var containerFsInodesUsed *uint64
	if containerFsInfo.Inodes != nil && containerFsInfo.InodesFree != nil {
		containerFsIU := *containerFsInfo.Inodes - *containerFsInfo.InodesFree
		containerFsInodesUsed = &containerFsIU
	}
	var usedContainerBytes uint64
	if containerFs.GetUsedBytes() != nil {
		usedContainerBytes = containerFs.GetUsedBytes().GetValue()
	}

	fsContainerStats := &statsapi.FsStats{
		Time:           metav1.NewTime(containerFsInfo.Timestamp),
		AvailableBytes: &containerFsInfo.Available,
		CapacityBytes:  &containerFsInfo.Capacity,
		UsedBytes:      &usedContainerBytes,
		InodesFree:     containerFsInfo.InodesFree,
		Inodes:         containerFsInfo.Inodes,
		InodesUsed:     containerFsInodesUsed,
	}

	return fsStats, fsContainerStats, nil
}

// ImageFsDevice 返回镜像文件系统所在设备的名称，例如 /dev/sda1。
func (p *cadvisorStatsProvider) ImageFsDevice(ctx context.Context) (string, error) {
	imageFsInfo, err := p.cadvisor.ImagesFsInfo(ctx)
	if err != nil {
		return "", err
	}
	return imageFsInfo.Device, nil
}

// buildPodRef 返回标识管理 cinfo 容器的 Pod 的 PodReference。
func buildPodRef(containerLabels map[string]string) statsapi.PodReference {
	podName := kubetypes.GetPodName(containerLabels)
	podNamespace := kubetypes.GetPodNamespace(containerLabels)
	podUID := kubetypes.GetPodUID(containerLabels)
	return statsapi.PodReference{Name: podName, Namespace: podNamespace, UID: podUID}
}

// isPodManagedContainer 判断 cinfo 容器是否由某个 Pod 管理，是则返回 true。
func isPodManagedContainer(logger klog.Logger, cinfo *cadvisorapi.ContainerInfo) bool {
	podName := kubetypes.GetPodName(cinfo.Spec.Labels)
	podNamespace := kubetypes.GetPodNamespace(cinfo.Spec.Labels)
	managed := podName != "" && podNamespace != ""
	if !managed && podName != podNamespace {
		logger.Info(
			"Expect container to have either both podName and podNamespace labels, or neither",
			"podNameLabel", podName, "podNamespaceLabel", podNamespace)
	}
	return managed
}

// getCadvisorPodInfoFromPodUID 通过把 podUID 与其 CgroupName 标识的基准名匹配，返回对应的 Pod cgroup 信息。
func getCadvisorPodInfoFromPodUID(podUID types.UID, infos map[string]cadvisorapi.ContainerInfo) *cadvisorapi.ContainerInfo {
	if info, found := infos[cm.GetPodCgroupNameSuffix(podUID)]; found {
		return &info
	}
	return nil
}

// filterTerminatedContainerInfoAndAssembleByPodCgroupKey 返回过滤后的容器信息：
// 移除已终止容器的统计，并把所有容器信息按 Pod 的 cgroup 键重新组装。
// 第一个返回值是 容器 cgroup 名 <-> ContainerInfo 的映射，
// 第二个返回值是 Pod cgroup 键 <-> ContainerInfo 的映射。
// 若某个 ContainerInfo 的创建时间更旧，且 CPU 瞬时用量与内存 RSS 用量均为零，
// 则视为已终止容器。
func filterTerminatedContainerInfoAndAssembleByPodCgroupKey(logger klog.Logger, containerInfo map[string]cadvisorapi.ContainerInfo) (map[string]cadvisorapi.ContainerInfo, map[string]cadvisorapi.ContainerInfo) {
	cinfoMap := make(map[containerID][]containerInfoWithCgroup)
	cinfosByPodCgroupKey := make(map[string]cadvisorapi.ContainerInfo)
	for key, cinfo := range containerInfo {
		var podCgroupKey string
		if cm.IsSystemdStyleName(key) {
			// 转换为内部 cgroup 名称，只取最后一段。
			internalCgroupName := cm.ParseSystemdToCgroupName(key)
			podCgroupKey = internalCgroupName[len(internalCgroupName)-1]
		} else {
			// 只取最后一段。
			podCgroupKey = filepath.Base(key)
		}
		cinfosByPodCgroupKey[podCgroupKey] = cinfo
		if !isPodManagedContainer(logger, &cinfo) {
			continue
		}
		cinfoID := containerID{
			podRef:        buildPodRef(cinfo.Spec.Labels),
			containerName: kubetypes.GetContainerName(cinfo.Spec.Labels),
		}
		cinfoMap[cinfoID] = append(cinfoMap[cinfoID], containerInfoWithCgroup{
			cinfo:  cinfo,
			cgroup: key,
		})
	}
	result := make(map[string]cadvisorapi.ContainerInfo)
	for _, refs := range cinfoMap {
		if len(refs) == 1 {
			// 已终止容器未清理的 cgroup 中，ContainerInfo 不含 CPU/内存/网络用量，
			// 不应出现在结果中。
			if !isContainerTerminated(&refs[0].cinfo) {
				result[refs[0].cgroup] = refs[0].cinfo
			}
			continue
		}
		sort.Sort(ByCreationTime(refs))
		for i := len(refs) - 1; i >= 0; i-- {
			if hasMemoryAndCPUInstUsage(&refs[i].cinfo) {
				result[refs[i].cgroup] = refs[i].cinfo
				break
			}
		}
	}
	return result, cinfosByPodCgroupKey
}

// ByCreationTime 基于 cinfo.Spec.CreationTime 字段实现 sort.Interface，
// 用于对 []containerInfoWithCgroup 排序。
type ByCreationTime []containerInfoWithCgroup

// Len 返回切片长度，实现 sort.Interface。
func (a ByCreationTime) Len() int { return len(a) }

// Swap 交换索引 i 和 j 处的元素，实现 sort.Interface。
func (a ByCreationTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less 按创建时间升序比较，创建时间相同时把没有 CPU/内存用量的一方排到前面。
func (a ByCreationTime) Less(i, j int) bool {
	if a[i].cinfo.Spec.CreationTime.Equal(a[j].cinfo.Spec.CreationTime) {
		// 正常情况下不会出现两个同名或同一创建时间的容器。但为了让此处逻辑更健壮，
		// 通过把没有 CPU 瞬时用量或内存 RSS 用量的一方排到前面来打破平局。
		return hasMemoryAndCPUInstUsage(&a[j].cinfo)
	}
	return a[i].cinfo.Spec.CreationTime.Before(a[j].cinfo.Spec.CreationTime)
}

// containerID 标识 Pod 内一个容器的身份。
type containerID struct {
	podRef        statsapi.PodReference
	containerName string
}

// containerInfoWithCgroup 包含 ContainerInfo 及其对应的 cgroup 名称。
type containerInfoWithCgroup struct {
	cinfo  cadvisorapi.ContainerInfo
	cgroup string
}

// hasMemoryAndCPUInstUsage 判断指定的容器信息是否同时具有非零的 CPU 瞬时用量和非零的
// 内存 RSS 用量；否则返回 false。
func hasMemoryAndCPUInstUsage(info *cadvisorapi.ContainerInfo) bool {
	if !info.Spec.HasCpu || !info.Spec.HasMemory {
		return false
	}
	cstat, found := latestContainerStats(info)
	if !found {
		return false
	}
	// Spec.HasMemory 并不保证存在内存样本：lib/model 把 Memory 暴露为指针，
	// 未采集到内存统计时该指针为 nil，因此必须先判空再解引用。
	if cstat.CpuInst == nil || cstat.Memory == nil {
		return false
	}
	return cstat.CpuInst.Usage.Total != 0 && cstat.Memory.RSS != 0
}

// isContainerTerminated 在满足以下任一条件时返回 true，否则返回 false：
// 1. info.spec 中 cpu、memory 和 network 均为 false。
// 2. info.Stats 中 network 以及 cpu 或 memory 均为 nil。
// 3. CPU 瞬时用量、内存 RSS 用量和网络用量均为零。
func isContainerTerminated(info *cadvisorapi.ContainerInfo) bool {
	if !info.Spec.HasCpu && !info.Spec.HasMemory && !info.Spec.HasNetwork {
		return true
	}
	cstat, found := latestContainerStats(info)
	if !found {
		return true
	}
	if cstat.Network != nil {
		iStats := cadvisorInfoToNetworkStats(info)
		if iStats != nil {
			for _, iStat := range iStats.Interfaces {
				if *iStat.RxErrors != 0 || *iStat.TxErrors != 0 || *iStat.RxBytes != 0 || *iStat.TxBytes != 0 {
					return false
				}
			}
		}
	}
	if cstat.CpuInst == nil || cstat.Memory == nil {
		return true
	}
	return cstat.CpuInst.Usage.Total == 0 && cstat.Memory.RSS == 0
}

// getCadvisorContainerInfo 通过 cAdvisor 递归获取根 cgroup 下所有容器的统计信息。
func getCadvisorContainerInfo(logger klog.Logger, ca cadvisor.Interface) (map[string]cadvisorapi.ContainerInfo, error) {
	// 计算“瞬时” CPU 需要 2 个样本。
	infos, err := ca.ContainerInfoV2("/", cadvisorapi.RequestOptions{
		IdType:    cadvisorapi.TypeName,
		Count:     2,
		Recursive: true,
	})
	if err != nil {
		if _, ok := infos["/"]; ok {
			// 部分失败时只记录日志，返回尽力而为的结果。
			logger.Error(err, "Partial failure issuing cadvisor.ContainerInfoV2")
		} else {
			return nil, fmt.Errorf("failed to get root cgroup stats: %w", err)
		}
	}
	return infos, nil
}
