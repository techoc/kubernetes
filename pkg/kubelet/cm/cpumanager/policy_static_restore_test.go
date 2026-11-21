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

package cpumanager

import (
	"runtime"
	"testing"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	resourcehelper "k8s.io/component-helpers/resource"
	pkgfeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/cpuset"
)

// 在本测试范围内，任何带有 Pod 级资源且开启 PodLevelResourceManagers 特性的 Pod
// 都会由 AllocatePod 处理。
// TestCPUManagerRestoreState 验证 CPU Manager 在 kubelet 重启后能恢复 Pod 级与容器级 CPU 分配。
func TestCPUManagerRestoreState(t *testing.T) {
	// CPU Manager 静态策略在 Windows 上不可用。
	if runtime.GOOS == "windows" {
		t.Skip("CPU Manager static policy is not available on Windows")
	}

	testCases := []struct {
		description                     string
		podLevelResourcesEnabled        bool
		podLevelResourceManagersEnabled bool
		podCPURequest                   string
		containers                      []*containerOptions
		expectPodCPUSet                 bool
	}{
		{
			description:   "Pod-level resources and managers enabled",
			podCPURequest: "2",
			containers: []*containerOptions{
				{name: "container1", request: "1", limit: "1"},
			},
			expectPodCPUSet:                 true,
			podLevelResourceManagersEnabled: true,
			podLevelResourcesEnabled:        true,
		},
		{
			description:   "Pod-level resources enabled, managers disabled",
			podCPURequest: "2",
			containers: []*containerOptions{
				{name: "container1", request: "1", limit: "1"},
			},
			expectPodCPUSet:                 false,
			podLevelResourceManagersEnabled: false,
			podLevelResourcesEnabled:        true,
		},
		{
			description:   "Container-level pod, features enabled",
			podCPURequest: "",
			containers: []*containerOptions{
				{name: "container1", request: "1", limit: "1"},
				{name: "container2", request: "1", limit: "1"},
			},
			expectPodCPUSet:                 false,
			podLevelResourceManagersEnabled: true,
			podLevelResourcesEnabled:        true,
		},
		{
			description:   "Container-level pod, features disabled",
			podCPURequest: "",
			containers: []*containerOptions{
				{name: "container1", request: "1", limit: "1"},
				{name: "container2", request: "1", limit: "1"},
			},
			expectPodCPUSet:                 false,
			podLevelResourceManagersEnabled: false,
			podLevelResourcesEnabled:        false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 切换特性门控到用例指定的组合。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.PodLevelResources, tc.podLevelResourcesEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, pkgfeatures.PodLevelResourceManagers, tc.podLevelResourceManagersEnabled)

			// 使用临时目录保存 checkpoint，供重启后的管理器恢复。
			sDir := t.TempDir()
			logger, tCtx := ktesting.NewTestContext(t)
			mgr, err := NewManager(
				logger,
				"static",
				nil,
				5*time.Second,
				&cadvisorapi.MachineInfo{
					NumCores: 4,
					Topology: []cadvisorapi.Node{
						{
							Cores: []cadvisorapi.Core{
								{Id: 0, Threads: []int{0}},
								{Id: 1, Threads: []int{1}},
								{Id: 2, Threads: []int{2}},
								{Id: 3, Threads: []int{3}},
							},
						},
					},
				},
				cpuset.New(),
				v1.ResourceList{v1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI)},
				sDir,
				topologymanager.NewFakeManager(logger),
			)
			if err != nil {
				t.Fatalf("could not create manager: %v", err)
			}

			// 按用例构造带或不带 Pod 级 CPU 请求的 Pod。
			var pod *v1.Pod
			if tc.podCPURequest != "" {
				pod = makeMultiContainerPodWithOptionsAndPodLevelResources(tc.podCPURequest, nil, tc.containers)
			} else {
				pod = makeMultiContainerPodWithOptions(nil, tc.containers)
			}
			pod.Name = "pod1"
			pod.UID = types.UID("pod1")

			// 启动管理器以初始化状态与 activePods。
			err = mgr.Start(tCtx, func() []*v1.Pod { return []*v1.Pod{pod} }, &sourcesReadyStub{}, mockPodStatusProvider{}, mockRuntimeService{}, containermap.NewContainerMap())
			if err != nil {
				t.Fatalf("could not start manager: %v", err)
			}

			// Pod 级分配路径：由 AllocatePod 统一分配。
			if tc.podLevelResourceManagersEnabled && resourcehelper.IsPodLevelResourcesSet(pod) {
				err = mgr.AllocatePod(logger, pod, lifecycle.AddOperation)
				if err != nil {
					t.Fatalf("could not allocate pod: %v", err)
				}
			} else {
				// 容器级（旧路径）分配：逐个容器分配。
				for i := range pod.Spec.Containers {
					container := &pod.Spec.Containers[i]
					err = mgr.Allocate(tCtx, pod, container, lifecycle.AddOperation)
					if err != nil {
						t.Fatalf("could not allocate container %s: %v", container.Name, err)
					}
				}
			}

			// 把容器加入管理器，模拟容器正在运行。
			for i := range pod.Spec.Containers {
				container := &pod.Spec.Containers[i]
				mgr.AddContainer(logger, pod, container, container.Name)
			}

			// 重启前校验 Pod 级 CPU 集合是否符合预期。
			podCPUSet, _ := mgr.State().GetPodCPUSet(string(pod.UID))
			if tc.expectPodCPUSet && podCPUSet.IsEmpty() {
				t.Errorf("expected pod cpu set to be present")
			} else if !tc.expectPodCPUSet && !podCPUSet.IsEmpty() {
				t.Errorf("expected no pod cpu set, but got some")
			}

			// 重新创建管理器，模拟 kubelet 重启。
			mgr2, err := NewManager(
				logger,
				"static",
				nil,
				5*time.Second,
				&cadvisorapi.MachineInfo{
					NumCores: 4,
					Topology: []cadvisorapi.Node{
						{
							Cores: []cadvisorapi.Core{
								{Id: 0, Threads: []int{0}},
								{Id: 1, Threads: []int{1}},
								{Id: 2, Threads: []int{2}},
								{Id: 3, Threads: []int{3}},
							},
						},
					},
				},
				cpuset.New(),
				v1.ResourceList{v1.ResourceCPU: *resource.NewQuantity(1, resource.DecimalSI)},
				sDir,
				topologymanager.NewFakeManager(logger),
			)
			if err != nil {
				t.Fatalf("could not create manager 2: %v", err)
			}

			err = mgr2.Start(tCtx, func() []*v1.Pod { return []*v1.Pod{pod} }, &sourcesReadyStub{}, mockPodStatusProvider{}, mockRuntimeService{}, containermap.NewContainerMap())
			if err != nil {
				t.Fatalf("could not start manager 2: %v", err)
			}

			// 校验重启后 Pod 级 CPU 集合已恢复且大小一致。
			podCPUSetRestored, _ := mgr2.State().GetPodCPUSet(string(pod.UID))
			if tc.expectPodCPUSet {
				if podCPUSetRestored.IsEmpty() {
					t.Errorf("expected pod cpu set to be present after restore")
				}
				if podCPUSetRestored.Size() != podCPUSet.Size() {
					t.Errorf("expected pod cpu set size to be %d, got %d", podCPUSet.Size(), podCPUSetRestored.Size())
				}
			} else if !podCPUSetRestored.IsEmpty() {
				t.Errorf("expected no pod cpu set after restore, but got some")
			}

			for i := range pod.Spec.Containers {
				container := &pod.Spec.Containers[i]
				containerCPUSet, ok := mgr2.State().GetCPUSet(string(pod.UID), container.Name)
				// 若分配被跳过（开启 Pod 级资源但未开启 Pod 级管理器），则不应存在 CPU 集合。
				allocationSkipped := tc.podLevelResourcesEnabled && !tc.podLevelResourceManagersEnabled && resourcehelper.IsPodLevelResourcesSet(pod)

				if allocationSkipped {
					if ok {
						t.Errorf("expected no container cpu set for %s after restore (allocation skipped), but got some", container.Name)
					}
				} else {
					if !ok {
						t.Errorf("expected container cpu set to be present for %s after restore", container.Name)
					}
					// 只有 Guaranteed 容器才保证有分配，仅在此时校验集合非空。
					if p := mgr2.(*manager).policy.(*staticPolicy); p.guaranteedCPUs(logger, pod, container) > 0 {
						if containerCPUSet.IsEmpty() {
							t.Errorf("expected container cpu set to be non-empty for guaranteed container %s", container.Name)
						}
					}
				}
			}
		})
	}
}
