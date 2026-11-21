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
	"errors"
	"fmt"

	cadvisormemory "github.com/google/cadvisor/lib/cache/memory"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	internalapi "k8s.io/cri-api/pkg/apis"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
	"k8s.io/kubernetes/pkg/kubelet/stats/pidlimit"
	"k8s.io/kubernetes/pkg/kubelet/status"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/utils/ptr"
)

// PodManager 是统计管理器观察 kubelet 实际状态所需的方法子集。
// 各方法的文档见 pkg/k8s.io/kubernetes/pkg/kubelet/pod.Manager。
type PodManager interface {
	TranslatePodUID(uid types.UID) kubetypes.ResolvedPodUID
}

// NewCRIStatsProvider 返回 Provider：节点统计来自 cAdvisor，
// 容器统计来自 CRI。
func NewCRIStatsProvider(
	cadvisor cadvisor.Interface,
	resourceAnalyzer stats.ResourceAnalyzer,
	podManager PodManager,
	runtimeService internalapi.RuntimeService,
	imageService internalapi.ImageManagerService,
	hostStatsProvider HostStatsProvider,
	podAndContainerStatsFromCRI bool,
	podSandboxStatsUnimplemented bool,
	fallbackStatsProvider containerStatsProvider,
) *Provider {
	return newStatsProvider(cadvisor, podManager, newCRIStatsProvider(cadvisor, resourceAnalyzer,
		runtimeService, imageService, hostStatsProvider, podAndContainerStatsFromCRI, podSandboxStatsUnimplemented, fallbackStatsProvider))
}

// NewCadvisorStatsProvider 返回 Provider：节点与容器统计都来自 cAdvisor。
func NewCadvisorStatsProvider(
	cadvisor cadvisor.Interface,
	resourceAnalyzer stats.ResourceAnalyzer,
	podManager PodManager,
	imageService kubecontainer.ImageService,
	statusProvider status.PodStatusProvider,
	hostStatsProvider HostStatsProvider,
	containerManager cm.ContainerManager,
) *Provider {
	return newStatsProvider(cadvisor, podManager, newCadvisorStatsProvider(cadvisor, resourceAnalyzer, imageService, statusProvider, hostStatsProvider, containerManager))
}

// newStatsProvider 返回新的 Provider：节点统计来自 cAdvisor，
// 容器统计由 containerStatsProvider 提供。
func newStatsProvider(
	cadvisor cadvisor.Interface,
	podManager PodManager,
	containerStatsProvider containerStatsProvider,
) *Provider {
	return &Provider{
		cadvisor:               cadvisor,
		podManager:             podManager,
		containerStatsProvider: containerStatsProvider,
	}
}

// Provider 提供节点与 Pod 所管理容器的统计。
type Provider struct {
	cadvisor   cadvisor.Interface
	podManager PodManager
	containerStatsProvider
}

// containerStatsProvider 是提供 Pod 所管理容器统计的接口。
type containerStatsProvider interface {
	// PodCPUAndMemoryStats 返回 Pod 及其所有运行中容器的最新 CPU 与内存统计。
	PodCPUAndMemoryStats(context.Context, *v1.Pod, *kubecontainer.PodStatus) (*statsapi.PodStats, error)
	// ListPodStats 返回所有 Pod 的统计。
	ListPodStats(ctx context.Context) ([]statsapi.PodStats, error)
	// ListPodStatsAndUpdateCPUNanoCoreUsage 返回所有 Pod 的统计，并刷新 CPU 使用量缓存。
	ListPodStatsAndUpdateCPUNanoCoreUsage(ctx context.Context) ([]statsapi.PodStats, error)
	// ListPodCPUAndMemoryStats 只返回所有 Pod 的 CPU 与内存统计。
	ListPodCPUAndMemoryStats(ctx context.Context) ([]statsapi.PodStats, error)
	// ImageFsStats 返回镜像文件系统与容器文件系统的统计。
	ImageFsStats(ctx context.Context) (*statsapi.FsStats, *statsapi.FsStats, error)
	// ImageFsDevice 返回镜像文件系统所在的设备。
	ImageFsDevice(ctx context.Context) (string, error)
}

// RlimitStats 返回进程数量相关的基础信息。
func (p *Provider) RlimitStats() (*statsapi.RlimitStats, error) {
	return pidlimit.Stats()
}

// GetCgroupStats 返回指定 cgroup 的统计；该函数不生成文件系统统计。
func (p *Provider) GetCgroupStats(ctx context.Context, cgroupName string, updateStats bool) (*statsapi.ContainerStats, *statsapi.NetworkStats, error) {
	info, err := getCgroupInfo(p.cadvisor, cgroupName, updateStats)
	if err != nil {
		// cAdvisor 尚未采集到该 cgroup 数据时返回可识别的错误。
		if errors.Is(err, cadvisormemory.ErrDataNotFound) {
			return nil, nil, fmt.Errorf("cgroup stats not found for %q: %w", cgroupName, cadvisormemory.ErrDataNotFound)
		}
		return nil, nil, fmt.Errorf("failed to get cgroup stats for %q: %v", cgroupName, err)
	}
	logger := klog.FromContext(ctx)
	// 对裸 cgroup 而言，rootfs 与 imagefs 没有意义。
	s := cadvisorInfoToContainerStats(logger, cgroupName, info, nil, nil)
	n := cadvisorInfoToNetworkStats(info)
	return s, n, nil
}

// GetCgroupCPUAndMemoryStats 返回指定 cgroup 的 CPU 与内存统计；
// 该函数不生成文件系统统计。
func (p *Provider) GetCgroupCPUAndMemoryStats(cgroupName string, updateStats bool) (*statsapi.ContainerStats, error) {
	info, err := getCgroupInfo(p.cadvisor, cgroupName, updateStats)
	if err != nil {
		// cAdvisor 尚未采集到该 cgroup 数据时返回可识别的错误。
		if errors.Is(err, cadvisormemory.ErrDataNotFound) {
			return nil, fmt.Errorf("cgroup stats not found for %q: %w", cgroupName, cadvisormemory.ErrDataNotFound)
		}
		return nil, fmt.Errorf("failed to get cgroup stats for %q: %v", cgroupName, err)
	}
	// 对裸 cgroup 而言，rootfs 与 imagefs 没有意义。
	s := cadvisorInfoToContainerCPUAndMemoryStats(cgroupName, info)
	return s, nil
}

// RootFsStats 返回节点根文件系统的统计。
func (p *Provider) RootFsStats() (*statsapi.FsStats, error) {
	rootFsInfo, err := p.cadvisor.RootFsInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get rootFs info: %v", err)
	}

	var nodeFsInodesUsed *uint64
	// inode 总数与空闲数都存在时才能计算已用 inode 数。
	if rootFsInfo.Inodes != nil && rootFsInfo.InodesFree != nil {
		nodeFsIU := *rootFsInfo.Inodes - *rootFsInfo.InodesFree
		nodeFsInodesUsed = &nodeFsIU
	}

	// 获取根容器统计的时间戳，用作 imageFs 统计的时间戳；
	// 这里不强制刷新统计，只需要时间戳。
	rootStats, err := getCgroupStats(p.cadvisor, "/", false)
	if err != nil {
		return nil, fmt.Errorf("failed to get root container stats: %v", err)
	}

	return &statsapi.FsStats{
		Time:           metav1.NewTime(rootStats.Timestamp),
		AvailableBytes: &rootFsInfo.Available,
		CapacityBytes:  &rootFsInfo.Capacity,
		UsedBytes:      &rootFsInfo.Usage,
		InodesFree:     rootFsInfo.InodesFree,
		Inodes:         rootFsInfo.Inodes,
		InodesUsed:     nodeFsInodesUsed,
	}, nil
}

// HasDedicatedImageFs 返回 true 表示存在用于存放镜像的独立文件系统。
// KEP Issue 4191：扩展为允许容器与镜像使用不同的文件系统。
func (p *Provider) HasDedicatedImageFs(ctx context.Context) (bool, error) {
	device, err := p.containerStatsProvider.ImageFsDevice(ctx)
	if err != nil {
		return false, err
	}
	rootFsInfo, err := p.cadvisor.RootFsInfo()
	if err != nil {
		return false, err
	}
	// KEP 增强：DedicatedImageFs 可能指容器文件系统或镜像文件系统与根文件系统分离。
	// cAdvisor 与容器运行时对此的报告方式略有不同。
	if device == rootFsInfo.Device {
		imageFs, containerFs, err := p.ImageFsStats(ctx)
		if err != nil {
			return false, err
		}
		if !equalFileSystems(imageFs, containerFs) {
			return true, nil
		}
	}
	return device != rootFsInfo.Device, nil
}

// HasDedicatedContainerFs 返回 true 表示容器文件系统与镜像文件系统分离。
// KEP Issue 4191：扩展为允许容器与镜像使用不同的文件系统。
func (p *Provider) HasDedicatedContainerFs(ctx context.Context) (bool, error) {
	imageFs, err := p.cadvisor.ImagesFsInfo(ctx)
	if err != nil {
		return false, err
	}
	containerFs, err := p.cadvisor.ContainerFsInfo(ctx)
	if err != nil {
		return false, err
	}
	return imageFs.Device != containerFs.Device, nil
}

// equalFileSystems 比较两个文件系统统计的关键字段是否一致。
func equalFileSystems(a, b *statsapi.FsStats) bool {
	if a == nil || b == nil {
		return false
	}
	if !ptr.Equal(a.AvailableBytes, b.AvailableBytes) {
		return false
	}
	if !ptr.Equal(a.CapacityBytes, b.CapacityBytes) {
		return false
	}
	if !ptr.Equal(a.InodesUsed, b.InodesUsed) {
		return false
	}
	if !ptr.Equal(a.InodesFree, b.InodesFree) {
		return false
	}
	if !ptr.Equal(a.Inodes, b.Inodes) {
		return false
	}
	return true
}
