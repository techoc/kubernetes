/*
Copyright 2016 The Kubernetes Authors.

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

//go:generate mockery
package stats

import (
	"context"
	"fmt"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/util"
)

// SummaryProvider 提供来自 Kubelet 的统计摘要。
type SummaryProvider interface {
	// Get 返回包含 Kubelet 统计的新 Summary；当 updateStats 为 true 时
	// 会顺带更新部分统计。
	Get(ctx context.Context, updateStats bool) (*statsapi.Summary, error)
	// GetCPUAndMemoryStats 返回只包含 CPU 与内存统计的新 Summary。
	GetCPUAndMemoryStats(ctx context.Context) (*statsapi.Summary, error)
}

// summaryProviderImpl 实现 SummaryProvider 接口。
type summaryProviderImpl struct {
	// kubeletCreationTime 是 summaryProvider 的创建时间。
	kubeletCreationTime metav1.Time
	// systemBootTime 是系统启动时间。
	systemBootTime metav1.Time

	provider Provider
}

// 编译期断言：summaryProviderImpl 必须实现 SummaryProvider 接口。
var _ SummaryProvider = &summaryProviderImpl{}

// NewSummaryProvider 使用指定 statsProvider 提供的统计数据返回 SummaryProvider。
func NewSummaryProvider(ctx context.Context, statsProvider Provider) SummaryProvider {
	logger := klog.FromContext(ctx)
	kubeletCreationTime := metav1.Now()
	bootTime, err := util.GetBootTime(logger)
	if err != nil {
		// 获取启动时间失败时 bootTime 为零值，节点指标的起始时间会不准确。
		logger.Info("Error getting system boot time. Node metrics will have an incorrect start time", "err", err)
	}

	return &summaryProviderImpl{
		kubeletCreationTime: kubeletCreationTime,
		systemBootTime:      metav1.NewTime(bootTime),
		provider:            statsProvider,
	}
}

// Get 汇总节点、根 cgroup、文件系统与各 Pod 的统计，生成完整的 Summary。
func (sp *summaryProviderImpl) Get(ctx context.Context, updateStats bool) (*statsapi.Summary, error) {
	// TODO(timstclair): Consider returning a best-effort response if any of
	// the following errors occur.
	node, err := sp.provider.GetNode(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get node info: %v", err)
	}
	nodeConfig := sp.provider.GetNodeConfig()
	rootStats, networkStats, err := sp.provider.GetCgroupStats(ctx, "/", updateStats)
	if err != nil {
		return nil, fmt.Errorf("failed to get root cgroup stats: %v", err)
	}
	rootFsStats, err := sp.provider.RootFsStats()
	if err != nil {
		return nil, fmt.Errorf("failed to get rootFs stats: %v", err)
	}
	imageFsStats, containerFsStats, err := sp.provider.ImageFsStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get imageFs stats: %v", err)
	}
	var podStats []statsapi.PodStats
	// updateStats 为 true 时，在列举 Pod 统计的同时刷新 CPU 使用量缓存。
	if updateStats {
		podStats, err = sp.provider.ListPodStatsAndUpdateCPUNanoCoreUsage(ctx)
	} else {
		podStats, err = sp.provider.ListPodStats(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list pod stats: %v", err)
	}

	rlimit, err := sp.provider.RlimitStats()
	if err != nil {
		return nil, fmt.Errorf("failed to get rlimit stats: %v", err)
	}

	// HugepageAwareEviction 开启时，从可用内存中扣除节点预留的大页容量。
	memory := rootStats.Memory
	if utilfeature.DefaultFeatureGate.Enabled(features.HugepageAwareEviction) {
		memory = adjustForHugePages(memory, node)
	}
	nodeStats := statsapi.NodeStats{
		NodeName:         node.Name,
		CPU:              rootStats.CPU,
		Memory:           memory,
		Swap:             rootStats.Swap,
		Network:          networkStats,
		StartTime:        sp.systemBootTime,
		Fs:               rootFsStats,
		Runtime:          &statsapi.RuntimeStats{ContainerFs: containerFsStats, ImageFs: imageFsStats},
		Rlimit:           rlimit,
		SystemContainers: sp.GetSystemContainersStats(ctx, nodeConfig, podStats, updateStats),
	}
	// PSI 指标受特性门控控制，未开启时不填充 IO 统计。
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		nodeStats.IO = rootStats.IO
	}
	summary := statsapi.Summary{
		Node: nodeStats,
		Pods: podStats,
	}
	return &summary, nil
}

// GetCPUAndMemoryStats 只采集 CPU 与内存相关统计，避免触发文件系统等开销较大的采集。
func (sp *summaryProviderImpl) GetCPUAndMemoryStats(ctx context.Context) (*statsapi.Summary, error) {
	// TODO(timstclair): Consider returning a best-effort response if any of
	// the following errors occur.
	node, err := sp.provider.GetNode(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get node info: %v", err)
	}
	nodeConfig := sp.provider.GetNodeConfig()
	rootStats, err := sp.provider.GetCgroupCPUAndMemoryStats("/", false)
	if err != nil {
		return nil, fmt.Errorf("failed to get root cgroup stats: %v", err)
	}

	podStats, err := sp.provider.ListPodCPUAndMemoryStats(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list pod stats: %v", err)
	}

	// 与 Get 一致，按特性门控修正大页对可用内存的影响。
	memory := rootStats.Memory
	if utilfeature.DefaultFeatureGate.Enabled(features.HugepageAwareEviction) {
		memory = adjustForHugePages(memory, node)
	}
	nodeStats := statsapi.NodeStats{
		NodeName:         node.Name,
		CPU:              rootStats.CPU,
		Memory:           memory,
		Swap:             rootStats.Swap,
		StartTime:        rootStats.StartTime,
		SystemContainers: sp.GetSystemContainersCPUAndMemoryStats(ctx, nodeConfig, podStats, false),
	}
	summary := statsapi.Summary{
		Node: nodeStats,
		Pods: podStats,
	}
	return &summary, nil
}

// adjustForHugePages 返回 memoryStats 的副本，并把 AvailableBytes 扣除节点的大页总容量。
//
// 根 cgroup 的内存上限被设置为 MemTotal（其中包含为大页预留的内存），但内存 cgroup
// 控制器不会在 WorkingSet 中统计 hugetlb 分配。这导致 AvailableBytes
// （limit - WorkingSet）比真实可用的普通内存多出大页预留量，从而推迟驱逐，
// 直到节点陷入严重内存压力。
func adjustForHugePages(memory *statsapi.MemoryStats, node *v1.Node) *statsapi.MemoryStats {
	// 缺少可用内存信息或节点信息时无需修正。
	if memory == nil || memory.AvailableBytes == nil || node == nil {
		return memory
	}

	// 汇总节点上所有大页资源（不同页大小）的容量。
	var totalHugePageBytes uint64
	for name, quantity := range node.Status.Capacity {
		if v1helper.IsHugePageResourceName(name) {
			totalHugePageBytes += uint64(quantity.Value())
		}
	}
	// 没有预留大页时直接返回原值。
	if totalHugePageBytes == 0 {
		return memory
	}

	adjusted := *memory
	if *adjusted.AvailableBytes > totalHugePageBytes {
		available := *adjusted.AvailableBytes - totalHugePageBytes
		adjusted.AvailableBytes = &available
	} else {
		// 未修正的 AvailableBytes（limit - WorkingSet）仍包含无法作为普通内存使用的大页内存。
		// 如果它小于大页预留量，说明普通内存已经耗尽。
		available := uint64(0)
		adjusted.AvailableBytes = &available
	}
	return &adjusted
}
