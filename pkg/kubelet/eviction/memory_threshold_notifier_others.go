//go:build !windows

/*
Copyright 2024 The Kubernetes Authors.

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

package eviction

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
)

const (
	// memoryUsageAttribute 是 cgroup v1 中记录内存用量的控制文件。
	memoryUsageAttribute = "memory.usage_in_bytes"
)

// linuxMemoryThresholdNotifier 通过 cgroup 内存事件在越过阈值时通知驱逐管理器。
type linuxMemoryThresholdNotifier struct {
	// threshold 是触发通知的驱逐阈值。
	threshold evictionapi.Threshold
	// cgroupPath 是设置阈值的 cgroup 路径。
	cgroupPath string
	// events 在阈值被越过时收到信号。
	events chan struct{}
	// factory 用于创建底层的 cgroup 通知器。
	factory NotifierFactory
	// handler 在阈值被越过时被调用。
	handler func(string)
	// notifier 是当前生效的底层通知器。
	notifier CgroupNotifier
}

// 编译期断言：linuxMemoryThresholdNotifier 实现了 ThresholdNotifier 接口。
var _ ThresholdNotifier = &linuxMemoryThresholdNotifier{}

// NewMemoryThresholdNotifier 创建响应指定阈值的 ThresholdNotifier。
// 在阈值生效前必须先调用一次 UpdateThreshold。
func NewMemoryThresholdNotifier(logger klog.Logger, threshold evictionapi.Threshold, cgroupRoot string, factory NotifierFactory, handler func(string)) (ThresholdNotifier, error) {
	// 需要 memory 控制器的挂载点才能设置 memcg 阈值。
	cgroups, err := cm.GetCgroupSubsystems()
	if err != nil {
		return nil, err
	}
	cgpath, found := cgroups.MountPoints["memory"]
	if !found || len(cgpath) == 0 {
		return nil, fmt.Errorf("memory cgroup mount point not found")
	}
	if isAllocatableEvictionThreshold(threshold) {
		// allocatable 阈值需要把通知器指向 allocatable cgroup。
		cgpath += cgroupRoot
	}
	return &linuxMemoryThresholdNotifier{
		threshold:  threshold,
		cgroupPath: cgpath,
		events:     make(chan struct{}),
		handler:    handler,
		factory:    factory,
	}, nil
}

// UpdateThreshold 根据最新统计信息重新计算并设置 cgroup 阈值。
func (m *linuxMemoryThresholdNotifier) UpdateThreshold(ctx context.Context, summary *statsapi.Summary) error {
	logger := klog.FromContext(ctx)
	// 默认使用节点级内存统计。
	memoryStats := summary.Node.Memory
	if isAllocatableEvictionThreshold(m.threshold) {
		allocatableContainer, err := getSysContainer(summary.Node.SystemContainers, statsapi.SystemContainerPods)
		if err != nil {
			return err
		}
		memoryStats = allocatableContainer.Memory
	}
	// 统计信息不完整时无法计算阈值。
	if memoryStats == nil || memoryStats.UsageBytes == nil || memoryStats.WorkingSetBytes == nil || memoryStats.AvailableBytes == nil {
		return fmt.Errorf("summary was incomplete.  Expected MemoryStats and all subfields to be non-nil, but got %+v", memoryStats)
	}
	// 把阈值换算到 usage 口径：capacity - eviction_hard + inactive_file，
	// 因为内核在 working_set = capacity - eviction_hard 时才应触发通知。
	// usage - working_set 即 inactive_file。
	inactiveFile := resource.NewQuantity(int64(*memoryStats.UsageBytes-*memoryStats.WorkingSetBytes), resource.BinarySI)
	capacity := resource.NewQuantity(int64(*memoryStats.AvailableBytes+*memoryStats.WorkingSetBytes), resource.BinarySI)
	evictionThresholdQuantity := evictionapi.GetThresholdQuantity(m.threshold.Value, capacity)
	memcgThreshold := capacity.DeepCopy()
	memcgThreshold.Sub(*evictionThresholdQuantity)
	memcgThreshold.Add(*inactiveFile)
	logger.V(3).Info("Eviction manager: setting notifier to capacity", "notifier", m.Description(), "capacity", memcgThreshold.String())
	// 更新阈值前先停掉旧通知器，避免重复监听。
	if m.notifier != nil {
		m.notifier.Stop()
	}
	newNotifier, err := m.factory.NewCgroupNotifier(logger, m.cgroupPath, memoryUsageAttribute, memcgThreshold.Value())
	if err != nil {
		return err
	}
	m.notifier = newNotifier
	// 通知器在后台运行，越过阈值时向 events 发送信号。
	go m.notifier.Start(ctx, m.events)
	return nil
}

// Start 等待阈值事件并调用 handler 通知驱逐管理器。
func (m *linuxMemoryThresholdNotifier) Start(ctx context.Context) {
	logger := klog.FromContext(ctx)
	logger.Info("Eviction manager: created memoryThresholdNotifier", "notifier", m.Description())
	for range m.events {
		m.handler(fmt.Sprintf("eviction manager: %s crossed", m.Description()))
	}
}

// Description 返回阈值的可读描述，包含 hard/soft 与 allocatable 标记。
func (m *linuxMemoryThresholdNotifier) Description() string {
	var hard, allocatable string
	if isHardEvictionThreshold(m.threshold) {
		hard = "hard "
	} else {
		hard = "soft "
	}
	if isAllocatableEvictionThreshold(m.threshold) {
		allocatable = "allocatable "
	}
	return fmt.Sprintf("%s%smemory eviction threshold", hard, allocatable)
}
