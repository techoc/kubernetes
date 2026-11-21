/*
Copyright 2025 The Kubernetes Authors.

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

package memorymanager

import (
	"runtime"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// 在本测试范围内，任何带有 Pod 级资源且开启 PodLevelResourceManagers 特性的 Pod
// 都会由 AllocatePod 处理。
// TestMemoryManagerRestoreState 验证 Memory Manager 在 kubelet 重启后能恢复 Pod 级与容器级内存分配。
func TestMemoryManagerRestoreState(t *testing.T) {
	// Memory Manager 静态策略在 Windows 上不可用。
	if runtime.GOOS == "windows" {
		t.Skip("Memory Manager static policy is not available on Windows")
	}

	tCtx := ktesting.Init(t)

	testCases := []struct {
		description                     string
		podLevelResourcesEnabled        bool
		podLevelResourceManagersEnabled bool
		podMemoryRequest                string
		containers                      []containerSpec
		expectPodBlocks                 bool
	}{
		{
			description:                     "PodLevelResources and PodLevelResourceManagers enabled",
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			podMemoryRequest:                "128Mi",
			containers: []containerSpec{
				{name: "container1", memRequest: "100Mi", memLimit: "100Mi"},
			},
			expectPodBlocks: true,
		},
		{
			description:                     "PodLevelResources enabled, PodLevelResourceManagers disabled",
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: false,
			podMemoryRequest:                "128Mi",
			containers: []containerSpec{
				{name: "container1", memRequest: "100Mi", memLimit: "100Mi"},
			},
			expectPodBlocks: false,
		},
		{
			description:                     "Container-level pod, features enabled",
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			podMemoryRequest:                "",
			containers: []containerSpec{
				{name: "container1", memRequest: "100Mi", memLimit: "100Mi"},
				{name: "container2", memRequest: "100Mi", memLimit: "100Mi"},
			},
			expectPodBlocks: false,
		},
		{
			description:                     "Container-level pod, features disabled",
			podLevelResourcesEnabled:        false,
			podLevelResourceManagersEnabled: false,
			podMemoryRequest:                "",
			containers: []containerSpec{
				{name: "container1", memRequest: "100Mi", memLimit: "100Mi"},
				{name: "container2", memRequest: "100Mi", memLimit: "100Mi"},
			},
			expectPodBlocks: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 切换特性门控到用例指定的组合。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, tc.podLevelResourcesEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResourceManagers, tc.podLevelResourceManagersEnabled)

			logger, ctx := ktesting.NewTestContext(t)
			machineInfo := returnMachineInfo()
			nodeAllocatableReservation := v1.ResourceList{
				v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI),
			}
			systemReservedMemory := []kubeletconfig.MemoryReservation{
				{
					NumaNode: 0,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI),
					},
				},
				{
					NumaNode: 1,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI),
					},
				},
			}
			affinity := topologymanager.NewFakeManager(logger)

			// 创建内存管理器。
			sDir := t.TempDir()
			mgr, err := NewManager(logger, string(PolicyTypeStatic), &machineInfo, nodeAllocatableReservation, systemReservedMemory, sDir, affinity)
			if err != nil {
				t.Fatalf("could not create manager: %v", err)
			}

			// 构造带 Pod 级内存请求的 Pod。
			pod := getPodWithContainersAndPodLevelResources("pod1", tc.podMemoryRequest, tc.podMemoryRequest, nil, tc.containers)

			// 启动管理器以初始化状态。
			err = mgr.Start(tCtx, func() []*v1.Pod { return []*v1.Pod{pod} }, &sourcesReadyStub{}, mockPodStatusProvider{}, mockRuntimeService{}, containermap.NewContainerMap())
			if err != nil {
				t.Fatalf("could not start manager: %v", err)
			}

			// 按特性门控组合选择 Pod 级或容器级分配路径。
			if tc.podLevelResourceManagersEnabled && resourcehelper.IsPodLevelResourcesSet(pod) {
				err = mgr.AllocatePod(logger, pod, lifecycle.AddOperation)
				if err != nil {
					t.Fatalf("could not allocate pod: %v", err)
				}
			} else {
				// 容器级分配会从 Pod 池中划分独占内存。
				for i := range pod.Spec.Containers {
					container := &pod.Spec.Containers[i]
					err = mgr.Allocate(ctx, pod, container, lifecycle.AddOperation)
					if err != nil {
						t.Fatalf("could not allocate container %s: %v", container.Name, err)
					}
					mgr.AddContainer(logger, pod, container, container.Name)
				}
			}

			// 重启前校验 Pod 级内存块是否符合预期。
			podBlocks := mgr.State().GetPodMemoryBlocks(string(pod.UID))
			if tc.expectPodBlocks && len(podBlocks) == 0 {
				t.Errorf("expected pod memory blocks to be present")
			} else if !tc.expectPodBlocks && len(podBlocks) > 0 {
				t.Errorf("expected no pod memory blocks, but got some")
			}

			// 重新创建管理器，模拟 kubelet 重启。
			mgr2, err := NewManager(logger, string(PolicyTypeStatic), &machineInfo, nodeAllocatableReservation, systemReservedMemory, sDir, affinity)
			if err != nil {
				t.Fatalf("could not create manager 2: %v", err)
			}

			err = mgr2.Start(tCtx, func() []*v1.Pod { return []*v1.Pod{pod} }, &sourcesReadyStub{}, mockPodStatusProvider{}, mockRuntimeService{}, containermap.NewContainerMap())
			if err != nil {
				t.Fatalf("could not start manager 2: %v", err)
			}

			// 校验重启后 Pod 级内存块已恢复且数量一致。
			podBlocksRestored := mgr2.State().GetPodMemoryBlocks(string(pod.UID))
			if tc.expectPodBlocks {
				if len(podBlocksRestored) == 0 {
					t.Errorf("expected pod memory blocks to be present after restore")
				}
				if len(podBlocksRestored) != len(podBlocks) {
					t.Errorf("expected pod memory blocks count to match, got %d want %d", len(podBlocksRestored), len(podBlocks))
				}
			} else if len(podBlocksRestored) > 0 {
				t.Errorf("expected no pod memory blocks after restore, but got some")
			}

			// 校验各容器的内存块已恢复。
			for _, container := range pod.Spec.Containers {
				containerBlocksRestored := mgr2.State().GetMemoryBlocks(string(pod.UID), container.Name)
				// 若开启 Pod 级资源但未开启 Pod 级管理器，则分配被跳过，不应存在内存块。
				if tc.podLevelResourcesEnabled && !tc.podLevelResourceManagersEnabled {
					if len(containerBlocksRestored) > 0 {
						t.Errorf("expected no container memory blocks after restore (allocation skipped) for %s, but got some", container.Name)
					}
				} else {
					if len(containerBlocksRestored) == 0 {
						t.Errorf("expected container memory blocks to be present after restore for %s", container.Name)
					}
				}
			}
		})
	}
}
