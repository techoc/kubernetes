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

// Package stats 实现 kubelet 的 Summary API 数据采集，包括从 CRI 与 cAdvisor 获取容器/节点统计。
package stats

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	cadvisormemory "github.com/google/cadvisor/lib/cache/memory"
	cadvisorfs "github.com/google/cadvisor/lib/fs"
	cadvisorapi "github.com/google/cadvisor/lib/model"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	kubetypes "k8s.io/kubelet/pkg/types"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

var (
	// defaultCachePeriod 是 cpuUsage 缓存的默认有效期，超过后条目会被清理。
	defaultCachePeriod = 10 * time.Minute
)

// cpuUsageRecord 保存一次 CPU 统计快照及其计算出的 usageNanoCores。
type cpuUsageRecord struct {
	stats          *runtimeapi.CpuUsage
	usageNanoCores *uint64
}

// criStatsProvider 通过 CRI 获取容器统计，实现 containerStatsProvider 接口。
// 它是默认的统计提供者，cAdvisor 仅作为补充。
type criStatsProvider struct {
	// cadvisor 用于获取节点根文件系统统计（容量、可用字节与 inode），
	// 这些值会填充到每个容器的文件系统统计中。
	// 注意：CRI 不提供节点级文件系统容量，因此必须依赖 cAdvisor。
	cadvisor cadvisor.Interface
	// resourceAnalyzer 用于获取 Pod 的卷统计。
	resourceAnalyzer stats.ResourceAnalyzer
	// runtimeService 用于获取 Pod 及其容器的状态与统计。
	// 所有 CRI 调用都经由它完成。
	runtimeService internalapi.RuntimeService
	// imageService 用于获取镜像文件系统统计。
	imageService internalapi.ImageManagerService
	// hostStatsProvider 用于获取 Pod 消耗的宿主机文件系统统计（如容器日志）。
	hostStatsProvider HostStatsProvider
	// windowsNetworkStatsProvider 在 Windows 上用于采集网络统计；
	windowsNetworkStatsProvider interface{} //nolint:unused // U1000 We can't import hcsshim due to Build constraints in hcsshim
	// clock 用于报告当前时间，便于测试注入。
	clock clock.Clock
	// fallbackStatsProvider 是 cAdvisor 实现的兜底提供者，
	// 在 CRI 未实现 PodSandboxStats 时使用。
	fallbackStatsProvider containerStatsProvider
	// useCRIPodSandboxStats 表示是否完全依赖 CRI 的 PodSandboxStats。
	// 仅在特性门控开启且 CRI 支持时才会为 true。
	useCRIPodSandboxStats bool

	// cpuUsageCache 按容器 ID 缓存 CPU 使用量，用于计算 usageNanoCores。
	cpuUsageCache map[string]*cpuUsageRecord
	mutex         sync.RWMutex
}

// newCRIStatsProvider 创建一个基于 CRI 的 containerStatsProvider 实现。
// useCRIPodSandboxStats 只有在特性开启且 CRI 支持时才为 true。
func newCRIStatsProvider(
	cadvisor cadvisor.Interface,
	resourceAnalyzer stats.ResourceAnalyzer,
	runtimeService internalapi.RuntimeService,
	imageService internalapi.ImageManagerService,
	hostStatsProvider HostStatsProvider,
	podAndContainerStatsFromCRI bool,
	podSandboxStatsUnimplemented bool,
	fallbackStatsProvider containerStatsProvider,
) containerStatsProvider {
	return &criStatsProvider{
		cadvisor:              cadvisor,
		resourceAnalyzer:      resourceAnalyzer,
		runtimeService:        runtimeService,
		imageService:          imageService,
		hostStatsProvider:     hostStatsProvider,
		cpuUsageCache:         make(map[string]*cpuUsageRecord),
		useCRIPodSandboxStats: podAndContainerStatsFromCRI && !podSandboxStatsUnimplemented,
		clock:                 clock.RealClock{},
		fallbackStatsProvider: fallbackStatsProvider,
	}
}

// ListPodStats 返回所有 Pod 管理容器的统计。
func (p *criStatsProvider) ListPodStats(ctx context.Context) ([]statsapi.PodStats, error) {
	// 只读路径不更新缓存，避免与周期性调用者产生竞争。
	return p.listPodStats(ctx, false)
}

// ListPodStatsAndUpdateCPUNanoCoreUsage 更新容器 CPU usageNanoCores 并返回全部 Pod 统计。
// CRI 不提供该指标，因此用当前与上次（缓存）的 CPU 统计差值计算。
// 实现假设只有一个调用者周期性调用，否则采样周期不一致会导致数值抖动。
// 目前 eviction manager 是唯一调用者，每 10s 调用一次。
// 若无人调用，usageNanoCores 保持为 nil。
// 这是 CRI 能力不足的折中方案。
// 多调用者场景下数值可能不连贯。
// 因此必须保持单调用者约定。
// 调用者应按固定周期调用以获得平滑结果。
// 该函数只应由一个组件调用。
func (p *criStatsProvider) ListPodStatsAndUpdateCPUNanoCoreUsage(ctx context.Context) ([]statsapi.PodStats, error) {
	// 更新路径会读写缓存，因此只允许周期性调用。
	return p.listPodStats(ctx, true)
}

// listPodStats 是 ListPodStats 与 ListPodStatsAndUpdateCPUNanoCoreUsage 的共同实现。
func (p *criStatsProvider) listPodStats(ctx context.Context, updateCPUNanoCoreUsage bool) ([]statsapi.PodStats, error) {
	// 获取节点根文件系统信息，用于填充容器统计中的可用量与容量。
	// 这一步失败则整个统计不可用，因此直接返回错误。
	rootFsInfo, err := p.cadvisor.RootFsInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get rootFs info: %v", err)
	}

	containerMap, podSandboxMap, err := p.getPodAndContainerMaps(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get pod or container map: %v", err)
	}

	// 完全依赖 CRI 还是部分依赖 cAdvisor，取决于运行时能力与特性门控。
	// CRI 支持时直接从 sandbox 统计读取 Pod 级 CPU/内存。
	if p.useCRIPodSandboxStats {
		return p.listPodStatsStrictlyFromCRI(ctx, updateCPUNanoCoreUsage, containerMap, podSandboxMap, &rootFsInfo)
	}
	return p.listPodStatsPartiallyFromCRI(ctx, updateCPUNanoCoreUsage, containerMap, podSandboxMap, &rootFsInfo)
}

// listPodStatsPartiallyFromCRI 以 CRI 统计为主、cAdvisor 统计为补充构建 Pod 统计。
func (p *criStatsProvider) listPodStatsPartiallyFromCRI(ctx context.Context, updateCPUNanoCoreUsage bool, containerMap map[string]*runtimeapi.Container, podSandboxMap map[string]*runtimeapi.PodSandbox, rootFsInfo *cadvisorapi.FsInfo) ([]statsapi.PodStats, error) {
	// fsIDtoInfo 按挂载点缓存文件系统统计，
	// 避免对同一文件系统重复查询 cAdvisor。
	// 同一 Pod 的多个容器通常共享文件系统，缓存收益明显。
	fsIDtoInfo := make(map[string]*cadvisorapi.FsInfo)

	// 按 sandbox ID 聚合 Pod 统计，容器统计到达顺序不影响结果。
	sandboxIDToPodStats := make(map[string]*statsapi.PodStats)

	resp, err := p.runtimeService.ListContainerStats(ctx, &runtimeapi.ContainerStatsFilter{})
	if err != nil {
		return nil, fmt.Errorf("failed to list all container stats: %v", err)
	}

	logger := klog.FromContext(ctx)
	allInfos, err := getCadvisorContainerInfo(logger, p.cadvisor)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch cadvisor stats: %v", err)
	}
	caInfos, allInfos := getCRICadvisorStats(logger, allInfos)

	// 获取容器网络统计：仅 Windows 有值，其他平台返回 (nil, nil)。
	// 非 Windows 平台走 cAdvisor 的网络统计路径。
	containerNetworkStats, err := p.listContainerNetworkStats(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to list container network stats: %v", err)
	}
	// 逐个容器补充统计；未知容器可能是已删除的残留记录。
	for _, stats := range resp {
		containerID := stats.Attributes.Id
		container, found := containerMap[containerID]
		if !found {
			continue
		}

		podSandboxID := container.PodSandboxId
		podSandbox, found := podSandboxMap[podSandboxID]
		if !found {
			continue
		}

		// 首次遇到该 sandbox 的容器时创建 Pod 统计对象。
		// 后续容器统计会追加到同一个对象。
		ps, found := sandboxIDToPodStats[podSandboxID]
		if !found {
			ps = buildPodStats(podSandbox)
			sandboxIDToPodStats[podSandboxID] = ps
		}

		// 用 CRI 统计构造容器统计，并汇总 Pod 级网络、CPU/内存、swap 与 IO。
		cs, err := p.makeContainerStats(logger, stats, container, rootFsInfo, fsIDtoInfo, podSandbox.GetMetadata(), updateCPUNanoCoreUsage)
		if err != nil {
			return nil, fmt.Errorf("make container stats: %w", err)
		}
		p.addPodNetworkStats(logger, ps, podSandboxID, caInfos, cs, containerNetworkStats[podSandboxID])
		p.addPodCPUMemoryStats(ps, types.UID(podSandbox.Metadata.Uid), allInfos, cs)
		p.addSwapStats(ps, types.UID(podSandbox.Metadata.Uid), allInfos, cs)
		p.addIOStats(ps, types.UID(podSandbox.Metadata.Uid), allInfos, cs)

		// cAdvisor 能提供 CRI 缺失的进程数与自定义指标，因此有则补充。
		// 找不到时仅记录日志，不阻断统计生成。
		caStats, caFound := caInfos[containerID]
		if !caFound {
			logger.V(5).Info("Unable to find cadvisor stats for container", "containerID", containerID)
		} else {
			p.addCadvisorContainerStats(logger, cs, &caStats)
			p.addProcessStats(ps, &caStats)
		}

		ps.Containers = append(ps.Containers, *cs)
	}
	// 清理过期 CPU 缓存，防止长期运行的节点内存持续增长。
	p.cleanupOutdatedCaches()

	result := make([]statsapi.PodStats, 0, len(sandboxIDToPodStats))
	for _, s := range sandboxIDToPodStats {
		makePodStorageStats(logger, s, rootFsInfo, p.resourceAnalyzer, p.hostStatsProvider, true)
		result = append(result, *s)
	}
	return result, nil
}

// listPodStatsStrictlyFromCRI 仅使用 CRI 的 PodSandboxStats 构建 Pod 统计。
func (p *criStatsProvider) listPodStatsStrictlyFromCRI(ctx context.Context, updateCPUNanoCoreUsage bool, containerMap map[string]*runtimeapi.Container, podSandboxMap map[string]*runtimeapi.PodSandbox, rootFsInfo *cadvisorapi.FsInfo) ([]statsapi.PodStats, error) {
	criSandboxStats, err := p.runtimeService.ListPodSandboxStats(ctx, &runtimeapi.PodSandboxStatsFilter{})
	if err != nil {
		return nil, err
	}
	logger := klog.FromContext(ctx)

	fsIDtoInfo := make(map[string]*cadvisorapi.FsInfo)
	summarySandboxStats := make([]statsapi.PodStats, 0, len(podSandboxMap))
	for _, criSandboxStat := range criSandboxStats {
		// 运行时可能返回不完整的统计，跳过而不是让整个请求失败。
		if criSandboxStat == nil || criSandboxStat.Attributes == nil {
			logger.V(5).Info("Unable to find CRI stats for sandbox")
			continue
		}
		podSandbox, found := podSandboxMap[criSandboxStat.Attributes.Id]
		if !found {
			continue
		}
		ps := buildPodStats(podSandbox)
		if err := p.addCRIPodContainerStats(logger, criSandboxStat, ps, fsIDtoInfo, containerMap, podSandbox, rootFsInfo, updateCPUNanoCoreUsage); err != nil {
			return nil, fmt.Errorf("add CRI pod container stats: %w", err)
		}
		// 各 add* 函数只负责一个统计维度，便于按需扩展。
		addCRIPodNetworkStats(ps, criSandboxStat)
		addCRIPodCPUStats(ps, criSandboxStat)
		addCRIPodMemoryStats(ps, criSandboxStat)
		addCRIPodProcessStats(ps, criSandboxStat)
		addCRIPodIOStats(ps, criSandboxStat)
		makePodStorageStats(logger, ps, rootFsInfo, p.resourceAnalyzer, p.hostStatsProvider, true)
		summarySandboxStats = append(summarySandboxStats, *ps)
	}
	return summarySandboxStats, nil
}

// PodCPUAndMemoryStats 返回单个 Pod 的 CPU 与内存统计，用于 Pod 级资源管理。
func (p *criStatsProvider) PodCPUAndMemoryStats(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus) (*statsapi.PodStats, error) {
	logger := klog.FromContext(ctx)
	// 没有 sandbox 状态就无法定位容器，直接报错。
	if len(podStatus.SandboxStatuses) == 0 {
		return nil, fmt.Errorf("missing sandbox for pod %s", format.Pod(pod))
	}
	podSandbox := podStatus.SandboxStatuses[0]
	ps := &statsapi.PodStats{
		PodRef: statsapi.PodReference{
			Name:      podSandbox.Metadata.Name,
			UID:       podSandbox.Metadata.Uid,
			Namespace: podSandbox.Metadata.Namespace,
		},
		// Summary API 中的 StartTime 取 Pod（sandbox）创建时间。
		StartTime: metav1.NewTime(time.Unix(0, podSandbox.CreatedAt)),
	}
	if p.useCRIPodSandboxStats {
		criSandboxStats, err := p.runtimeService.PodSandboxStats(ctx, podSandbox.Id)
		if err != nil {
			return nil, err
		}
		addCRIPodCPUStats(ps, criSandboxStats)
		addCRIPodMemoryStats(ps, criSandboxStats)
	}
	resp, err := p.runtimeService.ListContainerStats(ctx, &runtimeapi.ContainerStatsFilter{
		PodSandboxId: podSandbox.Id,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list container stats from pod %s (sandbox %s): %w", format.Pod(pod), podSandbox.Id, err)
	}

	for _, stats := range resp {
		containerStatus := podStatus.FindContainerStatusByName(stats.Attributes.Metadata.Name)
		if containerStatus == nil {
			logger.V(4).Info("Received stats for unknown container", "pod", klog.KObj(pod), "container", stats.Attributes.Metadata)
			continue
		}
		cs := p.makeContainerCPUAndMemoryStats(stats, containerStatus.CreatedAt, false)
		ps.Containers = append(ps.Containers, *cs)
	}

	// CRI 不提供 Pod 级统计时，用兜底提供者（cAdvisor）补齐，并合并到已有容器统计。
	if !p.useCRIPodSandboxStats {
		fallbackStats, err := p.fallbackStatsProvider.PodCPUAndMemoryStats(ctx, pod, podStatus)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch stats for pod %s from fallback provider: %w", format.Pod(pod), err)
		}
		ps.CPU = fallbackStats.CPU
		ps.Memory = fallbackStats.Memory

		for _, fb := range fallbackStats.Containers {
			var container *statsapi.ContainerStats
			for i, cs := range ps.Containers {
				if fb.Name == cs.Name {
					container = &ps.Containers[i]
					break
				}
			}
			if container != nil {
				// 只填补缺失字段，保留 CRI 已提供的值。
				if container.CPU == nil {
					container.CPU = fb.CPU
				}
				if container.Memory == nil {
					container.Memory = fb.Memory
				}
			} else {
				ps.Containers = append(ps.Containers, fb)
			}
		}
	}

	return ps, nil
}

// ListPodCPUAndMemoryStats 返回所有 Pod 管理容器的 CPU 与内存统计。
func (p *criStatsProvider) ListPodCPUAndMemoryStats(ctx context.Context) ([]statsapi.PodStats, error) {
	// 按 sandbox ID 聚合，避免依赖容器统计的返回顺序。
	sandboxIDToPodStats := make(map[string]*statsapi.PodStats)
	containerMap, podSandboxMap, err := p.getPodAndContainerMaps(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get pod or container map: %v", err)
	}
	logger := klog.FromContext(ctx)

	result := make([]statsapi.PodStats, 0, len(podSandboxMap))
	if p.useCRIPodSandboxStats {
		criSandboxStats, err := p.runtimeService.ListPodSandboxStats(ctx, &runtimeapi.PodSandboxStatsFilter{})
		if err != nil {
			return nil, err
		}
		for _, criSandboxStat := range criSandboxStats {
			podSandbox, found := podSandboxMap[criSandboxStat.Attributes.Id]
			if !found {
				continue
			}
			ps := buildPodStats(podSandbox)
			p.addCRIPodContainerCPUAndMemoryStats(criSandboxStat, ps, containerMap)
			addCRIPodCPUStats(ps, criSandboxStat)
			addCRIPodMemoryStats(ps, criSandboxStat)
			aggregatePodSwapStats(ps)
			result = append(result, *ps)
		}
		return result, nil
	}

	resp, err := p.runtimeService.ListContainerStats(ctx, &runtimeapi.ContainerStatsFilter{})
	if err != nil {
		return nil, fmt.Errorf("failed to list all container stats: %v", err)
	}

	allInfos, err := getCadvisorContainerInfo(logger, p.cadvisor)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch cadvisor stats: %v", err)
	}
	caInfos, allInfos := getCRICadvisorStats(logger, allInfos)

	for _, stats := range resp {
		containerID := stats.Attributes.Id
		container, found := containerMap[containerID]
		if !found {
			continue
		}

		podSandboxID := container.PodSandboxId
		podSandbox, found := podSandboxMap[podSandboxID]
		if !found {
			continue
		}

		// 首次遇到该 sandbox 的容器时创建 Pod 统计对象。
		// 后续容器统计会追加到同一个对象。
		ps, found := sandboxIDToPodStats[podSandboxID]
		if !found {
			ps = buildPodStats(podSandbox)
			sandboxIDToPodStats[podSandboxID] = ps
		}

		// 只填充 CPU/内存/swap，避免做无谓的磁盘统计。
		cs := p.makeContainerCPUAndMemoryStats(stats, time.Unix(0, container.CreatedAt), true)
		p.addPodCPUMemoryStats(ps, types.UID(podSandbox.Metadata.Uid), allInfos, cs)
		p.addSwapStats(ps, types.UID(podSandbox.Metadata.Uid), allInfos, cs)

		// cAdvisor 统计可补全 CRI 缺失的字段。
		// 找不到时仅记录日志，不影响整体结果。
		caStats, caFound := caInfos[containerID]
		if !caFound {
			logger.V(4).Info("Unable to find cadvisor stats for container", "containerID", containerID)
		} else {
			p.addCadvisorContainerCPUAndMemoryStats(logger, cs, &caStats)
		}
		ps.Containers = append(ps.Containers, *cs)
	}
	// 清理过期缓存，避免长期运行内存增长。
	p.cleanupOutdatedCaches()

	for _, s := range sandboxIDToPodStats {
		result = append(result, *s)
	}
	return result, nil
}

// getPodAndContainerMaps 构建容器 ID 与 sandbox ID 到 CRI 对象的映射。
func (p *criStatsProvider) getPodAndContainerMaps(ctx context.Context) (map[string]*runtimeapi.Container, map[string]*runtimeapi.PodSandbox, error) {
	containers, err := p.runtimeService.ListContainers(ctx, &runtimeapi.ContainerFilter{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list all containers: %v", err)
	}

	// 先过滤已终止的 sandbox，避免已删除 Pod 的统计混入结果。
	podSandboxMap := make(map[string]*runtimeapi.PodSandbox)
	podSandboxes, err := p.runtimeService.ListPodSandbox(ctx, &runtimeapi.PodSandboxFilter{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list all pod sandboxes: %v", err)
	}
	podSandboxes = removeTerminatedPods(podSandboxes)
	for _, s := range podSandboxes {
		podSandboxMap[s.Id] = s
	}

	containers = removeTerminatedContainers(containers)
	// 已终止容器不参与用量计算，因此同样过滤掉。
	containerMap := make(map[string]*runtimeapi.Container)
	for _, c := range containers {
		containerMap[c.Id] = c
	}
	return containerMap, podSandboxMap, nil
}

// ImageFsStats 返回镜像文件系统统计；容器文件系统与镜像共用时返回同一份数据。
func (p *criStatsProvider) ImageFsStats(ctx context.Context) (imageFsRet *statsapi.FsStats, containerFsRet *statsapi.FsStats, errRet error) {
	resp, err := p.imageService.ImageFsInfo(ctx)
	if err != nil {
		return nil, nil, err
	}

	// CRI 可能返回多个镜像文件系统，但目前只使用第一个。
	// 多文件系统支持见下方 TODO。
	//
	// TODO(yguo0905): 支持返回多个镜像文件系统的统计。
	if len(resp.GetImageFilesystems()) == 0 {
		return nil, nil, fmt.Errorf("imageFs information is unavailable")
	}
	fs := resp.GetImageFilesystems()[0]
	imageFsRet = &statsapi.FsStats{
		Time:      metav1.NewTime(time.Unix(0, fs.Timestamp)),
		UsedBytes: &fs.UsedBytes.Value,
	}
	if fs.InodesUsed != nil {
		imageFsRet.InodesUsed = &fs.InodesUsed.Value
	}
	imageFsInfo, err := p.getFsInfo(klog.FromContext(ctx), fs.GetFsId())
	if err != nil {
		return nil, nil, fmt.Errorf("get filesystem info: %w", err)
	}
	if imageFsInfo != nil {
		// 文件系统 ID 未知或查询出错时，省略容量信息，返回尽力而为的部分结果。
		// 这与 heapster#1793 的处理方式保持一致。
		// 只读统计不应因容量未知而整体失败。
		// 参见 heapster#1793。
		imageFsRet.AvailableBytes = &imageFsInfo.Available
		imageFsRet.CapacityBytes = &imageFsInfo.Capacity
		imageFsRet.InodesFree = imageFsInfo.InodesFree
		imageFsRet.Inodes = imageFsInfo.Inodes
	}
	// TODO: CRI 统计提供者尚不支持独立的容器文件系统，因此两者返回同一份数据。
	return imageFsRet, imageFsRet, nil
}

// ImageFsDevice 返回镜像文件系统所在设备名，例如 /dev/sda1。
// 多个镜像文件系统时返回第一个能查到设备信息的结果。
func (p *criStatsProvider) ImageFsDevice(ctx context.Context) (string, error) {
	resp, err := p.imageService.ImageFsInfo(ctx)
	if err != nil {
		return "", err
	}
	for _, fs := range resp.GetImageFilesystems() {
		fsInfo, err := p.getFsInfo(klog.FromContext(ctx), fs.GetFsId())
		if err != nil {
			return "", fmt.Errorf("get filesystem info: %w", err)
		}
		if fsInfo != nil {
			return fsInfo.Device, nil
		}
	}
	return "", errors.New("imagefs device is not found")
}

// getFsInfo 返回指定 fsID 对应的文件系统信息；
// 出错时记录日志并返回 nil，让调用方降级处理。
// 只有非预期错误才返回 error。
func (p *criStatsProvider) getFsInfo(logger klog.Logger, fsID *runtimeapi.FilesystemIdentifier) (*cadvisorapi.FsInfo, error) {
	if fsID == nil {
		logger.V(2).Info("Failed to get filesystem info: fsID is nil")
		return nil, nil
	}
	mountpoint := fsID.GetMountpoint()
	fsInfo, err := p.cadvisor.GetDirFsInfo(mountpoint)
	if err != nil {
		msg := "Failed to get the info of the filesystem with mountpoint"
		// 这些错误表示设备已消失或数据缺失，属于可降级的正常情况。
		if errors.Is(err, cadvisorfs.ErrNoSuchDevice) ||
			errors.Is(err, cadvisorfs.ErrDeviceNotInPartitionsMap) ||
			errors.Is(err, cadvisormemory.ErrDataNotFound) {
			logger.V(2).Info(msg, "mountpoint", mountpoint, "err", err)
		} else {
			logger.Error(err, msg, "mountpoint", mountpoint)
			return nil, fmt.Errorf("%s: %w", msg, err)
		}
		return nil, nil
	}
	return &fsInfo, nil
}

// buildPodStats 根据 sandbox 元数据构造仅含 Pod 标识与启动时间的 PodStats。
func buildPodStats(podSandbox *runtimeapi.PodSandbox) *statsapi.PodStats {
	return &statsapi.PodStats{
		PodRef: statsapi.PodReference{
			Name:      podSandbox.Metadata.Name,
			UID:       podSandbox.Metadata.Uid,
			Namespace: podSandbox.Metadata.Namespace,
		},
		// Summary API 中的 StartTime 取 Pod（sandbox）创建时间。
		StartTime: metav1.NewTime(time.Unix(0, podSandbox.CreatedAt)),
	}
}

// addPodNetworkStats 为 Pod 统计补充网络数据。
func (p *criStatsProvider) addPodNetworkStats(
	logger klog.Logger,
	ps *statsapi.PodStats,
	podSandboxID string,
	caInfos map[string]cadvisorapi.ContainerInfo,
	cs *statsapi.ContainerStats,
	netStats *statsapi.NetworkStats,
) {
	caPodSandbox, found := caInfos[podSandboxID]
	// 优先使用 cAdvisor 的 sandbox 网络统计，它比 CRI 更完整。
	if found {
		networkStats := cadvisorInfoToNetworkStats(&caPodSandbox)
		if networkStats != nil {
			ps.Network = networkStats
			return
		}
	}

	// cAdvisor 无数据时退回运行时提供的网络统计（主要针对 Windows）。
	if netStats != nil {
		ps.Network = netStats
		return
	}

	// TODO: 从容器统计汇总 Pod 网络统计。
	logger.V(4).Info("Unable to find network stats for sandbox", "sandboxID", podSandboxID)
}

// addPodCPUMemoryStats 汇总 Pod 级 CPU 与内存统计。
func (p *criStatsProvider) addPodCPUMemoryStats(
	ps *statsapi.PodStats,
	podUID types.UID,
	allInfos map[string]cadvisorapi.ContainerInfo,
	cs *statsapi.ContainerStats,
) {
	// cAdvisor 直接提供 Pod cgroup 统计，优先级高于按容器累加。
	podCgroupInfo := getCadvisorPodInfoFromPodUID(podUID, allInfos)
	if podCgroupInfo != nil {
		cpu, memory := cadvisorInfoToCPUandMemoryStats(podCgroupInfo)
		ps.CPU = cpu
		ps.Memory = memory
		return
	}

	// 没有 Pod cgroup 数据时退化为容器统计求和。
	if cs.CPU != nil {
		if ps.CPU == nil {
			ps.CPU = &statsapi.CPUStats{}
		}

		ps.CPU.Time = cs.CPU.Time
		usageCoreNanoSeconds := ptr.Deref(cs.CPU.UsageCoreNanoSeconds, 0) + ptr.Deref(ps.CPU.UsageCoreNanoSeconds, 0)
		usageNanoCores := ptr.Deref(cs.CPU.UsageNanoCores, 0) + ptr.Deref(ps.CPU.UsageNanoCores, 0)
		ps.CPU.UsageCoreNanoSeconds = &usageCoreNanoSeconds
		ps.CPU.UsageNanoCores = &usageNanoCores
		// Pod 级 PSI 无法由容器级数据推导，因此留空。
	}

	if cs.Memory != nil {
		if ps.Memory == nil {
			ps.Memory = &statsapi.MemoryStats{}
		}

		ps.Memory.Time = cs.Memory.Time
		availableBytes := ptr.Deref(cs.Memory.AvailableBytes, 0) + ptr.Deref(ps.Memory.AvailableBytes, 0)
		usageBytes := ptr.Deref(cs.Memory.UsageBytes, 0) + ptr.Deref(ps.Memory.UsageBytes, 0)
		workingSetBytes := ptr.Deref(cs.Memory.WorkingSetBytes, 0) + ptr.Deref(ps.Memory.WorkingSetBytes, 0)
		rSSBytes := ptr.Deref(cs.Memory.RSSBytes, 0) + ptr.Deref(ps.Memory.RSSBytes, 0)
		pageFaults := ptr.Deref(cs.Memory.PageFaults, 0) + ptr.Deref(ps.Memory.PageFaults, 0)
		majorPageFaults := ptr.Deref(cs.Memory.MajorPageFaults, 0) + ptr.Deref(ps.Memory.MajorPageFaults, 0)
		ps.Memory.AvailableBytes = &availableBytes
		ps.Memory.UsageBytes = &usageBytes
		ps.Memory.WorkingSetBytes = &workingSetBytes
		ps.Memory.RSSBytes = &rSSBytes
		ps.Memory.PageFaults = &pageFaults
		ps.Memory.MajorPageFaults = &majorPageFaults
		// Pod 级 PSI 无法由容器级数据推导，因此仅保留时间戳。
	}
}

// addSwapStats 汇总 Pod 级 swap 统计。
func (p *criStatsProvider) addSwapStats(
	ps *statsapi.PodStats,
	podUID types.UID,
	allInfos map[string]cadvisorapi.ContainerInfo,
	cs *statsapi.ContainerStats,
) {
	// 优先使用 cAdvisor 的 Pod cgroup swap 统计。
	podCgroupInfo := getCadvisorPodInfoFromPodUID(podUID, allInfos)
	if podCgroupInfo != nil {
		ps.Swap = cadvisorInfoToSwapStats(podCgroupInfo)
		return
	}

	// 没有 Pod 级数据时按容器累加。
	if cs.Swap != nil {
		if ps.Swap == nil {
			ps.Swap = &statsapi.SwapStats{Time: cs.Swap.Time}
		}
		swapAvailableBytes := ptr.Deref(cs.Swap.SwapAvailableBytes, 0) + ptr.Deref(ps.Swap.SwapAvailableBytes, 0)
		swapUsageBytes := ptr.Deref(cs.Swap.SwapUsageBytes, 0) + ptr.Deref(ps.Swap.SwapUsageBytes, 0)
		ps.Swap.SwapAvailableBytes = &swapAvailableBytes
		ps.Swap.SwapUsageBytes = &swapUsageBytes
	}
}

// aggregatePodSwapStats 从容器 swap 统计聚合出 Pod 级 swap 统计，
// 用于 CRI 未提供 Pod 级 swap 的场景（如 LinuxPodSandboxStats 没有 Swap 字段）。
func aggregatePodSwapStats(ps *statsapi.PodStats) {
	if len(ps.Containers) == 0 {
		return
	}
	var swapAvailableBytes, swapUsageBytes uint64
	var hasSwapStats bool
	var swapTime metav1.Time
	for _, cs := range ps.Containers {
		if cs.Swap != nil {
			hasSwapStats = true
			// TODO: 考虑取所有容器中最新时间，而不是直接使用最后一个容器的时间。
			swapTime = cs.Swap.Time
			swapAvailableBytes += ptr.Deref(cs.Swap.SwapAvailableBytes, 0)
			swapUsageBytes += ptr.Deref(cs.Swap.SwapUsageBytes, 0)
		}
	}
	if hasSwapStats {
		ps.Swap = &statsapi.SwapStats{
			Time:               swapTime,
			SwapAvailableBytes: &swapAvailableBytes,
			SwapUsageBytes:     &swapUsageBytes,
		}
	}
}

// addIOStats 为 Pod 统计补充 IO/PSI 数据；特性门控关闭时直接跳过。
func (p *criStatsProvider) addIOStats(
	ps *statsapi.PodStats,
	podUID types.UID,
	allInfos map[string]cadvisorapi.ContainerInfo,
	cs *statsapi.ContainerStats,
) {
	// 特性关闭时返回 nil，保证 API 输出与旧版本一致。
	if !utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		return
	}
	// 优先使用 cAdvisor 的 Pod cgroup IO 统计。
	podCgroupInfo := getCadvisorPodInfoFromPodUID(podUID, allInfos)
	if podCgroupInfo != nil {
		ps.IO = cadvisorInfoToIOStats(podCgroupInfo)
		return
	}

	if cs.IO != nil {
		if ps.IO == nil {
			ps.IO = &statsapi.IOStats{Time: cs.IO.Time}
		}
		// Pod 级 PSI 无法由容器级数据推导，因此留空。
	}
}

// addProcessStats 将容器进程数累加到 Pod 级进程统计。
func (p *criStatsProvider) addProcessStats(
	ps *statsapi.PodStats,
	container *cadvisorapi.ContainerInfo,
) {
	processStats := cadvisorInfoToProcessStats(container)
	// 逐个容器累加，得到 Pod 级进程总数。
	ps.ProcessStats = mergeProcessStats(ps.ProcessStats, processStats)
}

// makeContainerStats 由 CRI 容器统计构造 summary API 的 ContainerStats。
func (p *criStatsProvider) makeContainerStats(
	logger klog.Logger,
	stats *runtimeapi.ContainerStats,
	container *runtimeapi.Container,
	rootFsInfo *cadvisorapi.FsInfo,
	fsIDtoInfo map[string]*cadvisorapi.FsInfo,
	meta *runtimeapi.PodSandboxMetadata,
	updateCPUNanoCoreUsage bool,
) (*statsapi.ContainerStats, error) {
	result := &statsapi.ContainerStats{
		Name: stats.Attributes.Metadata.Name,
		// StartTime 取容器创建时间，而非统计采样时间。
		StartTime: metav1.NewTime(time.Unix(0, container.CreatedAt)),
		CPU:       &statsapi.CPUStats{},
		Memory:    &statsapi.MemoryStats{},
		Rootfs:    &statsapi.FsStats{},
		Swap:      &statsapi.SwapStats{},
		// CRI 不支持自定义指标，只能由 cAdvisor 补充。
	}
	// 统计字段缺失时，只有在 zeroMissingValues 为真时才填充零值，
	if stats.Cpu != nil {
		result.CPU.Time = metav1.NewTime(time.Unix(0, stats.Cpu.Timestamp))
		if stats.Cpu.UsageCoreNanoSeconds != nil {
			result.CPU.UsageCoreNanoSeconds = &stats.Cpu.UsageCoreNanoSeconds.Value
		}
		var usageNanoCores *uint64
		// 更新模式下顺便刷新缓存；只读模式直接读缓存。
		if updateCPUNanoCoreUsage {
			usageNanoCores = p.getAndUpdateContainerUsageNanoCores(logger, stats)
		} else {
			usageNanoCores = p.getContainerUsageNanoCores(stats)
		}
		if usageNanoCores != nil {
			result.CPU.UsageNanoCores = usageNanoCores
		}
		result.CPU.PSI = makePSIStats(stats.Cpu.Psi)
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
		if stats.Memory.UsageBytes != nil {
			result.Memory.UsageBytes = &stats.Memory.UsageBytes.Value
		}
		if stats.Memory.RssBytes != nil {
			result.Memory.RSSBytes = &stats.Memory.RssBytes.Value
		}
		if stats.Memory.AvailableBytes != nil {
			result.Memory.AvailableBytes = &stats.Memory.AvailableBytes.Value
		}
		if stats.Memory.PageFaults != nil {
			result.Memory.PageFaults = &stats.Memory.PageFaults.Value
		}
		if stats.Memory.MajorPageFaults != nil {
			result.Memory.MajorPageFaults = &stats.Memory.MajorPageFaults.Value
		}
		result.Memory.PSI = makePSIStats(stats.Memory.Psi)
	} else {
		result.Memory.Time = metav1.NewTime(time.Unix(0, time.Now().UnixNano()))
		result.Memory.WorkingSetBytes = ptr.To[uint64](0)
		result.Memory.UsageBytes = ptr.To[uint64](0)
		result.Memory.RSSBytes = ptr.To[uint64](0)
	}
	if stats.Swap != nil {
		result.Swap.Time = metav1.NewTime(time.Unix(0, stats.Swap.Timestamp))
		if stats.Swap.SwapUsageBytes != nil {
			result.Swap.SwapUsageBytes = &stats.Swap.SwapUsageBytes.Value
		}
		if stats.Swap.SwapAvailableBytes != nil {
			result.Swap.SwapAvailableBytes = &stats.Swap.SwapAvailableBytes.Value
		}
	} else {
		result.Swap.Time = metav1.NewTime(time.Unix(0, time.Now().UnixNano()))
		result.Swap.SwapUsageBytes = ptr.To[uint64](0)
		result.Swap.SwapAvailableBytes = ptr.To[uint64](0)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		result.IO = &statsapi.IOStats{}
		if stats.Io != nil {
			result.IO.Time = metav1.NewTime(time.Unix(0, stats.Io.Timestamp))
			result.IO.PSI = makePSIStats(stats.Io.Psi)
		} else {
			result.IO.Time = metav1.NewTime(time.Unix(0, time.Now().UnixNano()))
		}
	}
	if stats.WritableLayer != nil {
		result.Rootfs.Time = metav1.NewTime(time.Unix(0, stats.WritableLayer.Timestamp))
		if stats.WritableLayer.UsedBytes != nil {
			result.Rootfs.UsedBytes = &stats.WritableLayer.UsedBytes.Value
		}
		if stats.WritableLayer.InodesUsed != nil {
			result.Rootfs.InodesUsed = &stats.WritableLayer.InodesUsed.Value
		}
	}
	fsID := stats.GetWritableLayer().GetFsId()
	var err error
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
			// 文件系统 ID 未知或查询出错时，省略容量信息，返回尽力而为的部分结果。
			// 这与 heapster#1793 的处理方式保持一致。
			// 只读统计不应因容量未知而整体失败。
			// 参见 heapster#1793。
			result.Rootfs.AvailableBytes = &imageFsInfo.Available
			result.Rootfs.CapacityBytes = &imageFsInfo.Capacity
			result.Rootfs.InodesFree = imageFsInfo.InodesFree
			result.Rootfs.Inodes = imageFsInfo.Inodes
		}
	}
	// 注意：不支持旧的 Pod 日志路径 `/var/log/pods/UID`；
	// 使用旧路径的容器会得到空的日志统计。
	// 由于官方不支持原地升级，这一限制可以接受。
	result.Logs, err = p.hostStatsProvider.getPodContainerLogStats(meta.GetNamespace(), meta.GetName(), types.UID(meta.GetUid()), container.GetMetadata().GetName(), rootFsInfo)
	if err != nil {
		logger.Error(err, "Unable to fetch container log stats", "containerName", container.GetMetadata().GetName())
	}
	return result, nil
}

// makeContainerCPUAndMemoryStats 只构造容器 CPU/内存/swap 统计，用于轻量查询。
func (p *criStatsProvider) makeContainerCPUAndMemoryStats(
	stats *runtimeapi.ContainerStats,
	startTime time.Time,
	zeroMissingValues bool, // whether to write zeros to missing values
) *statsapi.ContainerStats {
	result := &statsapi.ContainerStats{
		Name: stats.Attributes.Metadata.Name,
		// StartTime 取容器创建时间，而非统计采样时间。
		StartTime: metav1.NewTime(startTime),
		// CRI 不支持自定义指标，只能由 cAdvisor 补充。
	}
	// getUint64 统一处理可选字段：缺失时按调用方要求返回 nil 或 0。
	getUint64 := func(val *runtimeapi.UInt64Value) *uint64 {
		if val != nil {
			return &val.Value
		} else if zeroMissingValues {
			return ptr.To[uint64](0)
		} else {
			return nil
		}
	}
	if stats.Cpu != nil {
		result.CPU = &statsapi.CPUStats{
			Time:                 metav1.NewTime(time.Unix(0, stats.Cpu.Timestamp)),
			UsageNanoCores:       p.getContainerUsageNanoCores(stats),
			UsageCoreNanoSeconds: getUint64(stats.Cpu.UsageCoreNanoSeconds),
			PSI:                  makePSIStats(stats.Cpu.Psi),
		}
		// 否则保持 nil，让调用方区分“没有数据”和“数据为零”。
	} else if zeroMissingValues {
		result.CPU = &statsapi.CPUStats{
			Time:                 metav1.NewTime(time.Unix(0, time.Now().UnixNano())),
			UsageCoreNanoSeconds: ptr.To[uint64](0),
			UsageNanoCores:       ptr.To[uint64](0),
		}
	}
	if stats.Memory != nil {
		result.Memory = &statsapi.MemoryStats{
			Time:            metav1.NewTime(time.Unix(0, stats.Memory.Timestamp)),
			AvailableBytes:  getUint64(stats.Memory.AvailableBytes),
			UsageBytes:      getUint64(stats.Memory.UsageBytes),
			WorkingSetBytes: getUint64(stats.Memory.WorkingSetBytes),
			RSSBytes:        getUint64(stats.Memory.RssBytes),
			PageFaults:      getUint64(stats.Memory.PageFaults),
			MajorPageFaults: getUint64(stats.Memory.MajorPageFaults),
			PSI:             makePSIStats(stats.Memory.Psi),
		}
	} else if zeroMissingValues {
		result.Memory = &statsapi.MemoryStats{
			Time:            metav1.NewTime(time.Unix(0, time.Now().UnixNano())),
			AvailableBytes:  ptr.To[uint64](0),
			UsageBytes:      ptr.To[uint64](0),
			WorkingSetBytes: ptr.To[uint64](0),
			RSSBytes:        ptr.To[uint64](0),
			PageFaults:      ptr.To[uint64](0),
			MajorPageFaults: ptr.To[uint64](0),
		}
	}
	if stats.Swap != nil {
		result.Swap = &statsapi.SwapStats{
			Time:               metav1.NewTime(time.Unix(0, stats.Swap.Timestamp)),
			SwapUsageBytes:     getUint64(stats.Swap.SwapUsageBytes),
			SwapAvailableBytes: getUint64(stats.Swap.SwapAvailableBytes),
		}
	} else if zeroMissingValues {
		result.Swap = &statsapi.SwapStats{
			Time:               metav1.NewTime(time.Unix(0, time.Now().UnixNano())),
			SwapUsageBytes:     ptr.To[uint64](0),
			SwapAvailableBytes: ptr.To[uint64](0),
		}
	}

	return result
}

// makePSIStats 将 CRI 的 PSI 统计转换为 summary API 结构。
func makePSIStats(stats *runtimeapi.PsiStats) *statsapi.PSIStats {
	if !utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		return nil
	}
	if stats == nil {
		return nil
	}
	result := &statsapi.PSIStats{}
	if stats.Full != nil {
		result.Full = statsapi.PSIData{
			Total:  stats.Full.Total,
			Avg10:  stats.Full.Avg10,
			Avg60:  stats.Full.Avg60,
			Avg300: stats.Full.Avg300,
		}
	}
	if stats.Some != nil {
		result.Some = statsapi.PSIData{
			Total:  stats.Some.Total,
			Avg10:  stats.Some.Avg10,
			Avg60:  stats.Some.Avg60,
			Avg300: stats.Some.Avg300,
		}
	}
	return result
}

// getContainerUsageNanoCores 优先使用 CRI 上报的 usageNanoCores，
// 否则从缓存读取上次计算的结果。
func (p *criStatsProvider) getContainerUsageNanoCores(stats *runtimeapi.ContainerStats) *uint64 {
	if stats == nil || stats.Attributes == nil {
		return nil
	}

	// CRI 已提供该指标时无需缓存，直接返回。
	if stats.Cpu != nil && stats.Cpu.UsageNanoCores != nil {
		return &stats.Cpu.UsageNanoCores.Value
	}

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	cached, ok := p.cpuUsageCache[stats.Attributes.Id]
	if !ok || cached.usageNanoCores == nil {
		return nil
	}
	// 返回副本，避免调用方修改缓存内部数据。
	latestUsage := *cached.usageNanoCores
	return &latestUsage
}

// getAndUpdateContainerUsageNanoCores 优先使用 CRI 上报的 usageNanoCores，
// 否则用本次与缓存的 usageCoreNanoSeconds 差值计算，
// 并更新缓存后返回。
func (p *criStatsProvider) getAndUpdateContainerUsageNanoCores(logger klog.Logger, stats *runtimeapi.ContainerStats) *uint64 {
	if stats == nil || stats.Attributes == nil || stats.Cpu == nil {
		return nil
	}
	// CRI 已提供该指标时无需缓存，直接返回。
	if stats.Cpu.UsageNanoCores != nil {
		return &stats.Cpu.UsageNanoCores.Value
	}
	// 两个字段都缺失时无法计算，直接返回 nil。
	if stats.Cpu.UsageCoreNanoSeconds == nil {
		return nil
	}
	id := stats.Attributes.Id
	usage, err := func() (*uint64, error) {
		p.mutex.Lock()
		defer p.mutex.Unlock()

		cached, ok := p.cpuUsageCache[id]
		if !ok || cached.stats.UsageCoreNanoSeconds == nil || stats.Cpu.UsageCoreNanoSeconds.Value < cached.stats.UsageCoreNanoSeconds.Value {
			// 首次采样或计数器回绕时无法计算，但仍需更新缓存作为下次的基准。
			p.cpuUsageCache[id] = &cpuUsageRecord{stats: stats.Cpu, usageNanoCores: nil}
			return nil, nil
		}

		newStats := stats.Cpu
		cachedStats := cached.stats
		nanoSeconds := newStats.Timestamp - cachedStats.Timestamp
		if nanoSeconds <= 0 {
			return nil, fmt.Errorf("zero or negative interval (%v - %v)", newStats.Timestamp, cachedStats.Timestamp)
		}
		usageNanoCores := uint64(float64(newStats.UsageCoreNanoSeconds.Value-cachedStats.UsageCoreNanoSeconds.Value) /
			float64(nanoSeconds) * float64(time.Second/time.Nanosecond))

		// 用最新采样覆盖缓存，供下次计算使用。
		usageToUpdate := usageNanoCores
		p.cpuUsageCache[id] = &cpuUsageRecord{stats: newStats, usageNanoCores: &usageToUpdate}

		return &usageNanoCores, nil
	}()

	if err != nil {
		// 正常情况下不应出错；记录日志以提高可见性。
		logger.Error(err, "Failed updating cpu usage nano core")
	}
	return usage
}

// cleanupOutdatedCaches 删除过期或无效的 CPU 缓存条目。
func (p *criStatsProvider) cleanupOutdatedCaches() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for k, v := range p.cpuUsageCache {
		if v == nil {
			delete(p.cpuUsageCache, k)
			continue
		}

		// 超过缓存有效期说明容器可能已消失，条目不再有用。
		if time.Since(time.Unix(0, v.stats.Timestamp)) > defaultCachePeriod {
			delete(p.cpuUsageCache, k)
		}
	}
}

// removeTerminatedPods 返回去掉已终止实例后的 sandbox 列表。
// 仅当存在同名同命名空间的运行实例时才移除已终止实例。
// 这样做是因为：
// 1) PodSandbox 可能被重建；
// 2) Pod 可能以同名同命名空间被重建。
// UID 为空表示按名称聚合，跨越了 UID 变化。
func removeTerminatedPods(pods []*runtimeapi.PodSandbox) []*runtimeapi.PodSandbox {
	podMap := make(map[statsapi.PodReference][]*runtimeapi.PodSandbox)
	// 按创建时间排序，保证结果稳定且优先保留较新的实例。
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].CreatedAt < pods[j].CreatedAt
	})
	for _, pod := range pods {
		refID := statsapi.PodReference{
			Name:      pod.GetMetadata().GetName(),
			Namespace: pod.GetMetadata().GetNamespace(),
			// 故意不填 UID：重建后的 Pod UID 会变，但名称与命名空间不变。
		}
		podMap[refID] = append(podMap[refID], pod)
	}

	result := make([]*runtimeapi.PodSandbox, 0)
	for _, refs := range podMap {
		if len(refs) == 1 {
			result = append(result, refs[0])
			continue
		}
		found := false
		for i := 0; i < len(refs); i++ {
			if refs[i].State == runtimeapi.PodSandboxState_SANDBOX_READY {
				found = true
				result = append(result, refs[i])
			}
		}
		if !found {
			result = append(result, refs[len(refs)-1])
		}
	}
	return result
}

// removeTerminatedContainers 移除所有已终止容器，
// 因为已终止容器不应计入用量统计。
func removeTerminatedContainers(containers []*runtimeapi.Container) []*runtimeapi.Container {
	containerMap := make(map[containerID][]*runtimeapi.Container)
	// 按创建时间排序，便于稳定地选择运行中的实例。
	sort.Slice(containers, func(i, j int) bool {
		return containers[i].CreatedAt < containers[j].CreatedAt
	})
	for _, container := range containers {
		refID := containerID{
			podRef:        buildPodRef(container.Labels),
			containerName: kubetypes.GetContainerName(container.Labels),
		}
		containerMap[refID] = append(containerMap[refID], container)
	}

	result := make([]*runtimeapi.Container, 0)
	for _, refs := range containerMap {
		for i := 0; i < len(refs); i++ {
			if refs[i].State == runtimeapi.ContainerState_CONTAINER_RUNNING {
				result = append(result, refs[i])
			}
		}
	}
	return result
}

// addCadvisorContainerStats 用 cAdvisor 数据补全容器统计（含 CPU、内存、swap 与 IO）。
func (p *criStatsProvider) addCadvisorContainerStats(
	logger klog.Logger,
	cs *statsapi.ContainerStats,
	caPodStats *cadvisorapi.ContainerInfo,
) {
	// 只有声明了自定义指标时才需要解析，避免无谓开销。
	if caPodStats.Spec.HasCustomMetrics {
		cs.UserDefinedMetrics = cadvisorInfoToUserDefinedMetrics(logger, caPodStats)
	}

	cpu, memory := cadvisorInfoToCPUandMemoryStats(caPodStats)
	if cpu != nil {
		cs.CPU = cpu
	}
	if memory != nil {
		cs.Memory = memory
	}

	swap := cadvisorInfoToSwapStats(caPodStats)
	if swap != nil {
		cs.Swap = swap
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		io := cadvisorInfoToIOStats(caPodStats)
		if io != nil {
			cs.IO = io
		}
	}
}

// addCadvisorContainerCPUAndMemoryStats 用 cAdvisor 数据补全容器 CPU/内存/swap 统计。
func (p *criStatsProvider) addCadvisorContainerCPUAndMemoryStats(
	logger klog.Logger,
	cs *statsapi.ContainerStats,
	caPodStats *cadvisorapi.ContainerInfo,
) {
	if caPodStats.Spec.HasCustomMetrics {
		cs.UserDefinedMetrics = cadvisorInfoToUserDefinedMetrics(logger, caPodStats)
	}

	cpu, memory := cadvisorInfoToCPUandMemoryStats(caPodStats)
	if cpu != nil {
		cs.CPU = cpu
	}
	if memory != nil {
		cs.Memory = memory
	}

	swap := cadvisorInfoToSwapStats(caPodStats)
	if swap != nil {
		cs.Swap = swap
	}
}

// getCRICadvisorStats 从 cAdvisor 信息中提取按容器 ID 索引的统计，并返回按 Pod cgroup 聚合的信息。
func getCRICadvisorStats(logger klog.Logger, infos map[string]cadvisorapi.ContainerInfo) (map[string]cadvisorapi.ContainerInfo, map[string]cadvisorapi.ContainerInfo) {
	stats := make(map[string]cadvisorapi.ContainerInfo)
	filteredInfos, cinfosByPodCgroupKey := filterTerminatedContainerInfoAndAssembleByPodCgroupKey(logger, infos)
	for key, info := range filteredInfos {
		// systemd + devicemapper 下每个容器挂载点都有独立 cgroup，
		// 忽略 .mount 单元，避免 summary 中出现重复条目。
		// 参见 systemd.mount(5)。
		// 否则统计会被重复累加。
		if strings.HasSuffix(key, ".mount") {
			continue
		}
		// 只保留由 Pod 管理的容器，其他容器不计入 summary。
		if !isPodManagedContainer(logger, &info) {
			continue
		}
		stats[extractIDFromCgroupPath(key)] = info
	}
	return stats, cinfosByPodCgroupKey
}

// extractIDFromCgroupPath 从 cgroup 路径中提取容器 ID，兼容 cgroupfs 与 systemd 两种格式。
func extractIDFromCgroupPath(cgroupPath string) string {
	// 格式 0（cgroupfs）：容器 ID 是路径最后一段。
	id := filepath.Base(cgroupPath)

	// 格式 1（systemd）：文件名形如 cri-containerd-<id>.scope。
	// 去掉 .scope 后缀并取最后一个 '-' 之后的部分。
	systemdSuffix := ".scope"
	if strings.HasSuffix(id, systemdSuffix) {
		id = strings.TrimSuffix(id, systemdSuffix)
		components := strings.Split(id, "-")
		if len(components) > 1 {
			id = components[len(components)-1]
		}
	}
	return id
}

// criInterfaceToSummary 将 CRI 网络接口统计转换为 summary API 结构。
func criInterfaceToSummary(criIface *runtimeapi.NetworkInterfaceUsage) statsapi.InterfaceStats {
	return statsapi.InterfaceStats{
		Name:     criIface.Name,
		RxBytes:  valueOfUInt64Value(criIface.RxBytes),
		RxErrors: valueOfUInt64Value(criIface.RxErrors),
		TxBytes:  valueOfUInt64Value(criIface.TxBytes),
		TxErrors: valueOfUInt64Value(criIface.TxErrors),
	}
}

// valueOfUInt64Value 解包 CRI 的可选 uint64 值，nil 时返回 nil。
func valueOfUInt64Value(value *runtimeapi.UInt64Value) *uint64 {
	if value == nil {
		return nil
	}
	return &value.Value
}
