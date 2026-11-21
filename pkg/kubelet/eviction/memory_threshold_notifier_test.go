//go:build linux

/*
Copyright 2018 The Kubernetes Authors.

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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
	klogtesting "k8s.io/klog/v2/ktesting"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
	"k8s.io/kubernetes/test/utils/ktesting"
)

const testCgroupPath = "/sys/fs/cgroups/memory"

// nodeSummary 构造节点内存统计；allocatable 为 true 时使用 pods 系统容器的数据。
func nodeSummary(available, workingSet, usage resource.Quantity, allocatable bool) *statsapi.Summary {
	availableBytes := uint64(available.Value())
	workingSetBytes := uint64(workingSet.Value())
	usageBytes := uint64(usage.Value())
	memoryStats := statsapi.MemoryStats{
		AvailableBytes:  &availableBytes,
		WorkingSetBytes: &workingSetBytes,
		UsageBytes:      &usageBytes,
	}
	if allocatable {
		return &statsapi.Summary{
			Node: statsapi.NodeStats{
				SystemContainers: []statsapi.ContainerStats{
					{
						Name:   statsapi.SystemContainerPods,
						Memory: &memoryStats,
					},
				},
			},
		}
	}
	return &statsapi.Summary{
		Node: statsapi.NodeStats{
			Memory: &memoryStats,
		},
	}
}

// newTestMemoryThresholdNotifier 用固定 cgroup 路径和注入的 factory/handler 构造通知器。
func newTestMemoryThresholdNotifier(threshold evictionapi.Threshold, factory NotifierFactory, handler func(string)) *linuxMemoryThresholdNotifier {
	return &linuxMemoryThresholdNotifier{
		threshold:  threshold,
		cgroupPath: testCgroupPath,
		events:     make(chan struct{}),
		factory:    factory,
		handler:    handler,
	}
}

// TestUpdateThreshold 验证根据 summary 计算 cgroup 阈值：节点级与可分配级信号，
// 以及底层创建通知器失败时的错误传递。
func TestUpdateThreshold(t *testing.T) {
	testCases := []struct {
		description        string
		available          resource.Quantity
		workingSet         resource.Quantity
		usage              resource.Quantity
		evictionThreshold  evictionapi.Threshold
		expectedThreshold  resource.Quantity
		updateThresholdErr error
		expectErr          bool
	}{
		{
			description: "node level threshold",
			available:   resource.MustParse("3Gi"),
			usage:       resource.MustParse("2Gi"),
			workingSet:  resource.MustParse("1Gi"),
			evictionThreshold: evictionapi.Threshold{
				Signal:   evictionapi.SignalMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedThreshold:  resource.MustParse("4Gi"),
			updateThresholdErr: nil,
			expectErr:          false,
		},
		{
			description: "allocatable threshold",
			available:   resource.MustParse("4Gi"),
			usage:       resource.MustParse("3Gi"),
			workingSet:  resource.MustParse("1Gi"),
			evictionThreshold: evictionapi.Threshold{
				Signal:   evictionapi.SignalAllocatableMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedThreshold:  resource.MustParse("6Gi"),
			updateThresholdErr: nil,
			expectErr:          false,
		},
		{
			description: "error updating node level threshold",
			available:   resource.MustParse("3Gi"),
			usage:       resource.MustParse("2Gi"),
			workingSet:  resource.MustParse("1Gi"),
			evictionThreshold: evictionapi.Threshold{
				Signal:   evictionapi.SignalMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			expectedThreshold:  resource.MustParse("4Gi"),
			updateThresholdErr: fmt.Errorf("unexpected error"),
			expectErr:          true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			logger, tCtx := klogtesting.NewTestContext(t)
			notifierFactory := NewMockNotifierFactory(t)
			notifier := NewMockCgroupNotifier(t)

			// notifier 与 factory 均为 mock，按期望参数调用一次。
			m := newTestMemoryThresholdNotifier(tc.evictionThreshold, notifierFactory, nil)
			notifierFactory.EXPECT().NewCgroupNotifier(logger, testCgroupPath, memoryUsageAttribute, tc.expectedThreshold.Value()).Return(notifier, tc.updateThresholdErr).Times(1)
			var events chan<- struct{} = m.events

			// 用 WaitGroup 确保 UpdateThreshold 启动的 goroutine 在测试结束前完成。
			var wg sync.WaitGroup
			wg.Add(1)
			notifier.EXPECT().Start(tCtx, events).Run(func(ctx context.Context, eventCh chan<- struct{}) {
				defer wg.Done()
				// mock 实现立即完成，只负责释放 WaitGroup。
			}).Maybe()

			// UpdateThreshold 读取 summary 并计算新的绝对值阈值。
			err := m.UpdateThreshold(tCtx, nodeSummary(tc.available, tc.workingSet, tc.usage, isAllocatableEvictionThreshold(tc.evictionThreshold)))

			// 等待 UpdateThreshold 内部启动的 goroutine 结束，避免泄漏。
			if err == nil {
				wg.Wait()
			}

			if err != nil && !tc.expectErr {
				t.Errorf("Unexpected error updating threshold: %v", err)
			} else if err == nil && tc.expectErr {
				t.Errorf("Expected error updating threshold, but got nil")
			}
		})
	}
}

// TestUpdateThresholdWithInvalidSummary 验证 summary 不完整或找不到系统容器时返回错误。
func TestUpdateThresholdWithInvalidSummary(t *testing.T) {
	testCases := []struct {
		description                  string
		summary                      *statsapi.Summary
		allocatableEvictionThreshold bool
	}{
		{
			description: "incomplete summary",
			summary: &statsapi.Summary{
				Node: statsapi.NodeStats{
					Memory: &statsapi.MemoryStats{},
				},
			},
		},
		{
			description:                  "system container not found in metrics",
			allocatableEvictionThreshold: true,
			summary: &statsapi.Summary{
				Node: statsapi.NodeStats{
					SystemContainers: []statsapi.ContainerStats{
						{
							Name: "invalid",
						},
					},
					Memory: &statsapi.MemoryStats{},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			m := newTestMemoryThresholdNotifier(evictionapi.Threshold{}, nil, nil)
			if tc.allocatableEvictionThreshold {
				m.threshold.Signal = evictionapi.SignalAllocatableMemoryAvailable
			}
			assert.Error(t, m.UpdateThreshold(tCtx, tc.summary))
		})
	}
}

// TestStart 验证收到 cgroup 事件后会调用 handler：mock 连续发送 4 个事件，
// handler 被调用 4 次后 WaitGroup 归零。
func TestStart(t *testing.T) {
	logger, tCtx := klogtesting.NewTestContext(t)
	noResources := resource.MustParse("0")
	threshold := evictionapi.Threshold{
		Signal:   evictionapi.SignalMemoryAvailable,
		Operator: evictionapi.OpLessThan,
		Value: evictionapi.ThresholdValue{
			Quantity: &noResources,
		},
	}
	notifierFactory := NewMockNotifierFactory(t)
	notifier := NewMockCgroupNotifier(t)

	var wg sync.WaitGroup
	// 四个事件对应四次 handler 调用，全部完成后测试结束。
	wg.Add(4)
	m := newTestMemoryThresholdNotifier(threshold, notifierFactory, func(string) {
		wg.Done()
	})
	notifierFactory.EXPECT().NewCgroupNotifier(logger, testCgroupPath, memoryUsageAttribute, int64(0)).Return(notifier, nil).Times(1)

	var events chan<- struct{} = m.events
	notifier.EXPECT().Start(tCtx, events).Run(func(ctx context.Context, events chan<- struct{}) {
		for i := 0; i < 4; i++ {
			events <- struct{}{}
		}
	})

	err := m.UpdateThreshold(tCtx, nodeSummary(noResources, noResources, noResources, isAllocatableEvictionThreshold(threshold)))
	if err != nil {
		t.Errorf("Unexpected error updating threshold: %v", err)
	}

	// Start 会阻塞读取事件 channel，因此放到 goroutine 中执行。
	go m.Start(tCtx)

	wg.Wait()
}

// TestThresholdDescription 验证描述文本包含 hard/soft 以及 allocatable 标记。
func TestThresholdDescription(t *testing.T) {
	testCases := []struct {
		description        string
		evictionThreshold  evictionapi.Threshold
		expectedSubstrings []string
		omittedSubstrings  []string
	}{
		{
			description: "hard node level threshold",
			evictionThreshold: evictionapi.Threshold{
				Signal:   evictionapi.SignalMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
			},
			expectedSubstrings: []string{"hard"},
			omittedSubstrings:  []string{"allocatable", "soft"},
		},
		{
			description: "soft node level threshold",
			evictionThreshold: evictionapi.Threshold{
				Signal:   evictionapi.SignalMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
				GracePeriod: time.Minute * 2,
			},
			expectedSubstrings: []string{"soft"},
			omittedSubstrings:  []string{"allocatable", "hard"},
		},
		{
			description: "hard allocatable threshold",
			evictionThreshold: evictionapi.Threshold{
				Signal:   evictionapi.SignalAllocatableMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
				GracePeriod: time.Minute * 2,
			},
			expectedSubstrings: []string{"soft", "allocatable"},
			omittedSubstrings:  []string{"hard"},
		},
		{
			description: "soft allocatable threshold",
			evictionThreshold: evictionapi.Threshold{
				Signal:   evictionapi.SignalAllocatableMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
			},
			expectedSubstrings: []string{"hard", "allocatable"},
			omittedSubstrings:  []string{"soft"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			m := &linuxMemoryThresholdNotifier{
				notifier:   &MockCgroupNotifier{},
				threshold:  tc.evictionThreshold,
				cgroupPath: testCgroupPath,
			}
			// 描述用于日志和事件，必须能区分硬/软阈值与可分配信号。
			desc := m.Description()
			for _, expected := range tc.expectedSubstrings {
				if !strings.Contains(desc, expected) {
					t.Errorf("expected description for notifier with threshold %+v to contain %s, but it did not", tc.evictionThreshold, expected)
				}
			}
			for _, omitted := range tc.omittedSubstrings {
				if strings.Contains(desc, omitted) {
					t.Errorf("expected description for notifier with threshold %+v NOT to contain %s, but it did", tc.evictionThreshold, omitted)
				}
			}
		})
	}
}
