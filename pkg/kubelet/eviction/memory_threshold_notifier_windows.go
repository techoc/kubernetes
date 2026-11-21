//go:build windows

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
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
	"k8s.io/kubernetes/pkg/kubelet/winstats"
)

// windowsMemoryThresholdNotifier 通过轮询全局提交内存来触发驱逐通知。
type windowsMemoryThresholdNotifier struct {
	// threshold 是触发通知的驱逐阈值。
	threshold evictionapi.Threshold
	// events 在阈值被越过时收到信号。
	events chan struct{}
	// handler 在阈值被越过时被调用。
	handler func(string)
}

// 编译期断言：windowsMemoryThresholdNotifier 实现了 ThresholdNotifier 接口。
var _ ThresholdNotifier = &windowsMemoryThresholdNotifier{}

// NewMemoryThresholdNotifier 创建 Windows 内存阈值通知器。
func NewMemoryThresholdNotifier(logger klog.Logger, threshold evictionapi.Threshold, cgroupRoot string, factory NotifierFactory, handler func(string)) (ThresholdNotifier, error) {
	logger.Info("Eviction manager: creating new WindowsMemoryThresholdNotifier")
	return &windowsMemoryThresholdNotifier{
		threshold: threshold,
		events:    make(chan struct{}),
		handler:   handler,
	}, nil
}

// Start 启动后台轮询并等待阈值事件。
func (m *windowsMemoryThresholdNotifier) Start(ctx context.Context) {
	logger := klog.FromContext(ctx)
	logger.Info("Eviction manager: starting windowsMemoryThresholdNotifier", "notifier", m.Description())
	// 后台协程定期轮询内存用量并触发阈值事件。
	go func() {
		for true {
			time.Sleep(notifierRefreshInterval)
			m.checkMemoryUsage(logger)
		}
	}()

	for range m.events {
		m.handler(fmt.Sprintf("eviction manager: %s crossed", m.Description()))
	}
}

// checkMemoryUsage 读取全局提交内存，低于阈值时发送事件。
func (m *windowsMemoryThresholdNotifier) checkMemoryUsage(logger klog.Logger) {
	// 获取全局提交上限。
	perfInfo, err := winstats.GetPerformanceInfo()
	if err != nil {
		logger.Error(err, "Eviction manager: error getting global memory status for node")
		return
	}

	commitLimitBytes := perfInfo.CommitLimitPages * perfInfo.PageSize
	capacity := resource.NewQuantity(int64(commitLimitBytes), resource.BinarySI)
	evictionThresholdQuantity := evictionapi.GetThresholdQuantity(m.threshold.Value, capacity)

	commitTotalBytes := perfInfo.CommitTotalPages * perfInfo.PageSize
	commitAvailableBytes := commitLimitBytes - commitTotalBytes

	// 可用提交内存不高于阈值时触发通知。
	if commitAvailableBytes <= uint64(evictionThresholdQuantity.Value()) {
		m.events <- struct{}{}
	}
}

// UpdateThreshold 在 Windows 上是空操作，由 Start 中的轮询协程负责检测。
func (m *windowsMemoryThresholdNotifier) UpdateThreshold(ctx context.Context, summary *statsapi.Summary) error {
	// Windows 不使用 cgroup 通知器触发驱逐，因此该函数为空操作。
	// Start() 中启动的协程会轮询系统内存用量，在越过阈值时触发驱逐。
	return nil
}

// Description 返回阈值的可读描述，包含 hard/soft 与 allocatable 标记。
func (m *windowsMemoryThresholdNotifier) Description() string {
	var hard, allocatable string
	if isHardEvictionThreshold(m.threshold) {
		hard = "hard"
	} else {
		hard = "soft"
	}
	if isAllocatableEvictionThreshold(m.threshold) {
		allocatable = "allocatable"
	}
	return fmt.Sprintf("%s %s memory eviction threshold", hard, allocatable)
}
