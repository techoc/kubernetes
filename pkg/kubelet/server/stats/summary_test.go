//go:build !windows

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

package stats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/randfill"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	statstest "k8s.io/kubernetes/pkg/kubelet/server/stats/testing"
)

// 测试共享的节点、cgroup 配置和随机统计样本，避免每个用例重复构造。
var (
	imageFsStats = getFsStats()
	rootFsStats  = getFsStats()
	node         = &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "test-node"}}
	nodeConfig   = cm.NodeConfig{
		RuntimeCgroupsName: "/runtime",
		SystemCgroupsName:  "/misc",
		KubeletCgroupsName: "/kubelet",
	}
	cgroupRoot  = "/kubepods"
	rlimitStats = getRlimitStats()
)

// TestSummaryProviderGetStatsNoSplitFileSystem 验证 root fs 与 image fs 未分离时，
// Summary 的节点、系统容器、运行时和 Pod 统计都来自预期数据源。
func TestSummaryProviderGetStatsNoSplitFileSystem(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletPSI, true)
	ctx := t.Context()
	assert := assert.New(t)

	podStats := []statsapi.PodStats{
		{
			PodRef:      statsapi.PodReference{Name: "test-pod", Namespace: "test-namespace", UID: "UID_test-pod"},
			StartTime:   metav1.NewTime(time.Now()),
			Containers:  []statsapi.ContainerStats{*getContainerStats()},
			Network:     getNetworkStats(),
			VolumeStats: []statsapi.VolumeStats{*getVolumeStats()},
		},
	}
	cgroupStatsMap := map[string]struct {
		cs *statsapi.ContainerStats
		ns *statsapi.NetworkStats
	}{
		"/":        {cs: getContainerStats(), ns: getNetworkStats()},
		"/runtime": {cs: getContainerStats(), ns: getNetworkStats()},
		"/misc":    {cs: getContainerStats(), ns: getNetworkStats()},
		"/kubelet": {cs: getContainerStats(), ns: getNetworkStats()},
		"/pods":    {cs: getContainerStats(), ns: getNetworkStats()},
	}

	mockStatsProvider := statstest.NewMockProvider(t)

	mockStatsProvider.EXPECT().GetNode(ctx).Return(node, nil)
	mockStatsProvider.EXPECT().GetNodeConfig().Return(nodeConfig)
	mockStatsProvider.EXPECT().GetPodCgroupRoot().Return(cgroupRoot)
	mockStatsProvider.EXPECT().ListPodStats(ctx).Return(podStats, nil).Maybe()
	mockStatsProvider.EXPECT().ListPodStatsAndUpdateCPUNanoCoreUsage(ctx).Return(podStats, nil)
	mockStatsProvider.EXPECT().ImageFsStats(ctx).Return(imageFsStats, imageFsStats, nil)
	// 未分离文件系统：image fs 与 container fs 相同，RuntimeStats 两个字段都取 imageFsStats。
	mockStatsProvider.EXPECT().RootFsStats().Return(rootFsStats, nil)
	mockStatsProvider.EXPECT().RlimitStats().Return(rlimitStats, nil)
	mockStatsProvider.EXPECT().GetCgroupStats(ctx, "/", true).Return(cgroupStatsMap["/"].cs, cgroupStatsMap["/"].ns, nil)
	mockStatsProvider.EXPECT().GetCgroupStats(ctx, "/runtime", false).Return(cgroupStatsMap["/runtime"].cs, cgroupStatsMap["/runtime"].ns, nil)
	mockStatsProvider.EXPECT().GetCgroupStats(ctx, "/misc", false).Return(cgroupStatsMap["/misc"].cs, cgroupStatsMap["/misc"].ns, nil)
	mockStatsProvider.EXPECT().GetCgroupStats(ctx, "/kubelet", false).Return(cgroupStatsMap["/kubelet"].cs, cgroupStatsMap["/kubelet"].ns, nil)
	mockStatsProvider.EXPECT().GetCgroupStats(ctx, "/kubepods", true).Return(cgroupStatsMap["/pods"].cs, cgroupStatsMap["/pods"].ns, nil)

	kubeletCreationTime := metav1.Now()
	systemBootTime := metav1.Now()
	provider := summaryProviderImpl{kubeletCreationTime: kubeletCreationTime, systemBootTime: systemBootTime, provider: mockStatsProvider}
	summary, err := provider.Get(ctx, true)
	assert.NoError(err)

	assert.Equal("test-node", summary.Node.NodeName)
	assert.Equal(summary.Node.StartTime, systemBootTime)
	assert.Equal(summary.Node.CPU, cgroupStatsMap["/"].cs.CPU)
	assert.Equal(summary.Node.Memory, cgroupStatsMap["/"].cs.Memory)
	assert.Equal(summary.Node.Swap, cgroupStatsMap["/"].cs.Swap)
	assert.Equal(summary.Node.IO, cgroupStatsMap["/"].cs.IO)
	assert.Equal(summary.Node.Network, cgroupStatsMap["/"].ns)
	assert.Equal(summary.Node.Fs, rootFsStats)
	// 节点运行时统计：未分离文件系统时 container fs 与 image fs 相同。
	assert.Equal(&statsapi.RuntimeStats{ContainerFs: imageFsStats, ImageFs: imageFsStats}, summary.Node.Runtime)

	assert.Len(summary.Node.SystemContainers, 4)
	assert.Contains(summary.Node.SystemContainers, statsapi.ContainerStats{
		Name:               "kubelet",
		StartTime:          kubeletCreationTime,
		CPU:                cgroupStatsMap["/kubelet"].cs.CPU,
		Memory:             cgroupStatsMap["/kubelet"].cs.Memory,
		Accelerators:       cgroupStatsMap["/kubelet"].cs.Accelerators,
		UserDefinedMetrics: cgroupStatsMap["/kubelet"].cs.UserDefinedMetrics,
		Swap:               cgroupStatsMap["/kubelet"].cs.Swap,
		IO:                 cgroupStatsMap["/kubelet"].cs.IO,
	})
	assert.Contains(summary.Node.SystemContainers, statsapi.ContainerStats{
		Name:               "misc",
		StartTime:          cgroupStatsMap["/misc"].cs.StartTime,
		CPU:                cgroupStatsMap["/misc"].cs.CPU,
		Memory:             cgroupStatsMap["/misc"].cs.Memory,
		Accelerators:       cgroupStatsMap["/misc"].cs.Accelerators,
		UserDefinedMetrics: cgroupStatsMap["/misc"].cs.UserDefinedMetrics,
		Swap:               cgroupStatsMap["/misc"].cs.Swap,
		IO:                 cgroupStatsMap["/misc"].cs.IO,
	})
	assert.Contains(summary.Node.SystemContainers, statsapi.ContainerStats{
		Name:               "runtime",
		StartTime:          cgroupStatsMap["/runtime"].cs.StartTime,
		CPU:                cgroupStatsMap["/runtime"].cs.CPU,
		Memory:             cgroupStatsMap["/runtime"].cs.Memory,
		Accelerators:       cgroupStatsMap["/runtime"].cs.Accelerators,
		UserDefinedMetrics: cgroupStatsMap["/runtime"].cs.UserDefinedMetrics,
		Swap:               cgroupStatsMap["/runtime"].cs.Swap,
		IO:                 cgroupStatsMap["/runtime"].cs.IO,
	})
	assert.Contains(summary.Node.SystemContainers, statsapi.ContainerStats{
		Name:               "pods",
		StartTime:          cgroupStatsMap["/pods"].cs.StartTime,
		CPU:                cgroupStatsMap["/pods"].cs.CPU,
		Memory:             cgroupStatsMap["/pods"].cs.Memory,
		Accelerators:       cgroupStatsMap["/pods"].cs.Accelerators,
		UserDefinedMetrics: cgroupStatsMap["/pods"].cs.UserDefinedMetrics,
		Swap:               cgroupStatsMap["/pods"].cs.Swap,
		IO:                 cgroupStatsMap["/pods"].cs.IO,
	})
	assert.Equal(summary.Pods, podStats)
}

// TestSummaryProviderGetStatsSplitImageFs 验证 root fs 与 image fs 分离时，
// RuntimeStats.ContainerFs 取 root fs，ImageFs 取镜像文件系统。
func TestSummaryProviderGetStatsSplitImageFs(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletPSI, true)
	ctx := t.Context()
	assert := assert.New(t)

	podStats := []statsapi.PodStats{
		{
			PodRef:      statsapi.PodReference{Name: "test-pod", Namespace: "test-namespace", UID: "UID_test-pod"},
			StartTime:   metav1.NewTime(time.Now()),
			Containers:  []statsapi.ContainerStats{*getContainerStats()},
			Network:     getNetworkStats(),
			VolumeStats: []statsapi.VolumeStats{*getVolumeStats()},
		},
	}
	cgroupStatsMap := map[string]struct {
		cs *statsapi.ContainerStats
		ns *statsapi.NetworkStats
	}{
		"/":        {cs: getContainerStats(), ns: getNetworkStats()},
		"/runtime": {cs: getContainerStats(), ns: getNetworkStats()},
		"/misc":    {cs: getContainerStats(), ns: getNetworkStats()},
		"/kubelet": {cs: getContainerStats(), ns: getNetworkStats()},
		"/pods":    {cs: getContainerStats(), ns: getNetworkStats()},
	}

	mockStatsProvider := statstest.NewMockProvider(t)

	mockStatsProvider.EXPECT().GetNode(ctx).Return(node, nil)
	mockStatsProvider.EXPECT().GetNodeConfig().Return(nodeConfig)
	mockStatsProvider.EXPECT().GetPodCgroupRoot().Return(cgroupRoot)
	mockStatsProvider.EXPECT().ListPodStats(ctx).Return(podStats, nil).Maybe()
	mockStatsProvider.EXPECT().ListPodStatsAndUpdateCPUNanoCoreUsage(ctx).Return(podStats, nil)
	mockStatsProvider.EXPECT().RootFsStats().Return(rootFsStats, nil)
	mockStatsProvider.EXPECT().RlimitStats().Return(rlimitStats, nil)
	mockStatsProvider.EXPECT().GetCgroupStats(ctx, "/", true).Return(cgroupStatsMap["/"].cs, cgroupStatsMap["/"].ns, nil)
	mockStatsProvider.EXPECT().GetCgroupStats(ctx, "/runtime", false).Return(cgroupStatsMap["/runtime"].cs, cgroupStatsMap["/runtime"].ns, nil)
	mockStatsProvider.EXPECT().GetCgroupStats(ctx, "/misc", false).Return(cgroupStatsMap["/misc"].cs, cgroupStatsMap["/misc"].ns, nil)
	mockStatsProvider.EXPECT().GetCgroupStats(ctx, "/kubelet", false).Return(cgroupStatsMap["/kubelet"].cs, cgroupStatsMap["/kubelet"].ns, nil)
	mockStatsProvider.EXPECT().GetCgroupStats(ctx, "/kubepods", true).Return(cgroupStatsMap["/pods"].cs, cgroupStatsMap["/pods"].ns, nil)

	mockStatsProvider.EXPECT().ImageFsStats(ctx).Return(imageFsStats, rootFsStats, nil)
	// 分离文件系统场景：两个返回值分别代表镜像文件系统和容器文件系统。

	kubeletCreationTime := metav1.Now()
	systemBootTime := metav1.Now()
	provider := summaryProviderImpl{kubeletCreationTime: kubeletCreationTime, systemBootTime: systemBootTime, provider: mockStatsProvider}
	summary, err := provider.Get(ctx, true)
	assert.NoError(err)

	assert.Equal("test-node", summary.Node.NodeName)
	assert.Equal(summary.Node.StartTime, systemBootTime)
	assert.Equal(summary.Node.CPU, cgroupStatsMap["/"].cs.CPU)
	assert.Equal(summary.Node.Memory, cgroupStatsMap["/"].cs.Memory)
	assert.Equal(summary.Node.Swap, cgroupStatsMap["/"].cs.Swap)
	assert.Equal(summary.Node.IO, cgroupStatsMap["/"].cs.IO)
	assert.Equal(summary.Node.Network, cgroupStatsMap["/"].ns)
	assert.Equal(summary.Node.Fs, rootFsStats)
	// 文件系统分离时，容器文件系统取 root fs，镜像文件系统取 image fs。
	// 节点运行时统计：分离场景下容器文件系统与镜像文件系统分别上报。
	assert.Equal(&statsapi.RuntimeStats{ContainerFs: rootFsStats, ImageFs: imageFsStats}, summary.Node.Runtime)

	assert.Len(summary.Node.SystemContainers, 4)
	assert.Contains(summary.Node.SystemContainers, statsapi.ContainerStats{
		Name:               "kubelet",
		StartTime:          kubeletCreationTime,
		CPU:                cgroupStatsMap["/kubelet"].cs.CPU,
		Memory:             cgroupStatsMap["/kubelet"].cs.Memory,
		Accelerators:       cgroupStatsMap["/kubelet"].cs.Accelerators,
		UserDefinedMetrics: cgroupStatsMap["/kubelet"].cs.UserDefinedMetrics,
		Swap:               cgroupStatsMap["/kubelet"].cs.Swap,
		IO:                 cgroupStatsMap["/kubelet"].cs.IO,
	})
	assert.Contains(summary.Node.SystemContainers, statsapi.ContainerStats{
		Name:               "misc",
		StartTime:          cgroupStatsMap["/misc"].cs.StartTime,
		CPU:                cgroupStatsMap["/misc"].cs.CPU,
		Memory:             cgroupStatsMap["/misc"].cs.Memory,
		Accelerators:       cgroupStatsMap["/misc"].cs.Accelerators,
		UserDefinedMetrics: cgroupStatsMap["/misc"].cs.UserDefinedMetrics,
		Swap:               cgroupStatsMap["/misc"].cs.Swap,
		IO:                 cgroupStatsMap["/misc"].cs.IO,
	})
	assert.Contains(summary.Node.SystemContainers, statsapi.ContainerStats{
		Name:               "runtime",
		StartTime:          cgroupStatsMap["/runtime"].cs.StartTime,
		CPU:                cgroupStatsMap["/runtime"].cs.CPU,
		Memory:             cgroupStatsMap["/runtime"].cs.Memory,
		Accelerators:       cgroupStatsMap["/runtime"].cs.Accelerators,
		UserDefinedMetrics: cgroupStatsMap["/runtime"].cs.UserDefinedMetrics,
		Swap:               cgroupStatsMap["/runtime"].cs.Swap,
		IO:                 cgroupStatsMap["/runtime"].cs.IO,
	})
	assert.Contains(summary.Node.SystemContainers, statsapi.ContainerStats{
		Name:               "pods",
		StartTime:          cgroupStatsMap["/pods"].cs.StartTime,
		CPU:                cgroupStatsMap["/pods"].cs.CPU,
		Memory:             cgroupStatsMap["/pods"].cs.Memory,
		Accelerators:       cgroupStatsMap["/pods"].cs.Accelerators,
		UserDefinedMetrics: cgroupStatsMap["/pods"].cs.UserDefinedMetrics,
		Swap:               cgroupStatsMap["/pods"].cs.Swap,
		IO:                 cgroupStatsMap["/pods"].cs.IO,
	})
	assert.Equal(summary.Pods, podStats)
}

// TestSummaryProviderGetCPUAndMemoryStats 验证只采集 CPU/内存的接口不会填充网络、文件系统、
// 运行时和 IO 等无关字段，同时仍上报节点与四个系统容器。
func TestSummaryProviderGetCPUAndMemoryStats(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletPSI, true)
	ctx := t.Context()
	assert := assert.New(t)

	podStats := []statsapi.PodStats{
		{
			PodRef:     statsapi.PodReference{Name: "test-pod", Namespace: "test-namespace", UID: "UID_test-pod"},
			StartTime:  metav1.NewTime(time.Now()),
			Containers: []statsapi.ContainerStats{*getContainerStats()},
		},
	}
	cgroupStatsMap := map[string]struct {
		cs *statsapi.ContainerStats
	}{
		"/":        {cs: getVolumeCPUAndMemoryStats()},
		"/runtime": {cs: getVolumeCPUAndMemoryStats()},
		"/misc":    {cs: getVolumeCPUAndMemoryStats()},
		"/kubelet": {cs: getVolumeCPUAndMemoryStats()},
		"/pods":    {cs: getVolumeCPUAndMemoryStats()},
	}

	mockStatsProvider := statstest.NewMockProvider(t)

	mockStatsProvider.EXPECT().GetNode(ctx).Return(node, nil)
	mockStatsProvider.EXPECT().GetNodeConfig().Return(nodeConfig)
	mockStatsProvider.EXPECT().GetPodCgroupRoot().Return(cgroupRoot)
	mockStatsProvider.EXPECT().ListPodCPUAndMemoryStats(ctx).Return(podStats, nil)
	mockStatsProvider.EXPECT().GetCgroupCPUAndMemoryStats("/", false).Return(cgroupStatsMap["/"].cs, nil)
	mockStatsProvider.EXPECT().GetCgroupCPUAndMemoryStats("/runtime", false).Return(cgroupStatsMap["/runtime"].cs, nil)
	mockStatsProvider.EXPECT().GetCgroupCPUAndMemoryStats("/misc", false).Return(cgroupStatsMap["/misc"].cs, nil)
	mockStatsProvider.EXPECT().GetCgroupCPUAndMemoryStats("/kubelet", false).Return(cgroupStatsMap["/kubelet"].cs, nil)
	mockStatsProvider.EXPECT().GetCgroupCPUAndMemoryStats("/kubepods", false).Return(cgroupStatsMap["/pods"].cs, nil)

	// 通过构造函数创建 provider，覆盖生产代码使用的初始化路径。
	provider := NewSummaryProvider(ctx, mockStatsProvider)
	summary, err := provider.GetCPUAndMemoryStats(ctx)
	assert.NoError(err)

	assert.Equal("test-node", summary.Node.NodeName)
	assert.Equal(summary.Node.StartTime, cgroupStatsMap["/"].cs.StartTime)
	assert.Equal(summary.Node.CPU, cgroupStatsMap["/"].cs.CPU)
	assert.Equal(summary.Node.Memory, cgroupStatsMap["/"].cs.Memory)
	// CPU/内存统计接口不采集网络、文件系统、运行时和 IO，这些字段必须保持 nil。
	assert.Nil(summary.Node.Network)
	assert.Nil(summary.Node.Fs)
	assert.Nil(summary.Node.Runtime)
	assert.Nil(summary.Node.IO)

	assert.Len(summary.Node.SystemContainers, 4)
	assert.Contains(summary.Node.SystemContainers, statsapi.ContainerStats{
		Name:      "kubelet",
		StartTime: cgroupStatsMap["/kubelet"].cs.StartTime,
		CPU:       cgroupStatsMap["/kubelet"].cs.CPU,
		Memory:    cgroupStatsMap["/kubelet"].cs.Memory,
	})
	assert.Contains(summary.Node.SystemContainers, statsapi.ContainerStats{
		Name:      "misc",
		StartTime: cgroupStatsMap["/misc"].cs.StartTime,
		CPU:       cgroupStatsMap["/misc"].cs.CPU,
		Memory:    cgroupStatsMap["/misc"].cs.Memory,
	})
	assert.Contains(summary.Node.SystemContainers, statsapi.ContainerStats{
		Name:      "runtime",
		StartTime: cgroupStatsMap["/runtime"].cs.StartTime,
		CPU:       cgroupStatsMap["/runtime"].cs.CPU,
		Memory:    cgroupStatsMap["/runtime"].cs.Memory,
	})
	assert.Contains(summary.Node.SystemContainers, statsapi.ContainerStats{
		Name:      "pods",
		StartTime: cgroupStatsMap["/pods"].cs.StartTime,
		CPU:       cgroupStatsMap["/pods"].cs.CPU,
		Memory:    cgroupStatsMap["/pods"].cs.Memory,
	})
	assert.Equal(summary.Pods, podStats)
}

// getFsStats 用 randfill 生成随机的文件系统统计，避免测试依赖固定数值。
func getFsStats() *statsapi.FsStats {
	f := randfill.New().NilChance(0)
	v := &statsapi.FsStats{}
	f.Fill(v)
	return v
}

// getContainerStats 生成随机容器统计；NilChance(0) 保证指针字段都被填充，便于逐字段比较。
func getContainerStats() *statsapi.ContainerStats {
	f := randfill.New().NilChance(0)
	v := &statsapi.ContainerStats{}
	f.Fill(v)
	return v
}

// getVolumeCPUAndMemoryStats 只填充 CPU/内存接口关心的字段：名称、启动时间、CPU 和内存。
func getVolumeCPUAndMemoryStats() *statsapi.ContainerStats {
	f := randfill.New().NilChance(0)
	v := &statsapi.ContainerStats{}
	f.Fill(&v.Name)
	f.Fill(&v.StartTime)
	f.Fill(v.CPU)
	f.Fill(v.Memory)
	return v
}

// getVolumeStats 生成随机卷统计。
func getVolumeStats() *statsapi.VolumeStats {
	f := randfill.New().NilChance(0)
	v := &statsapi.VolumeStats{}
	f.Fill(v)
	return v
}

// getNetworkStats 生成随机网络统计。
func getNetworkStats() *statsapi.NetworkStats {
	f := randfill.New().NilChance(0)
	v := &statsapi.NetworkStats{}
	f.Fill(v)
	return v
}

// getRlimitStats 生成随机 rlimit 统计。
func getRlimitStats() *statsapi.RlimitStats {
	f := randfill.New().NilChance(0)
	v := &statsapi.RlimitStats{}
	f.Fill(v)
	return v
}
