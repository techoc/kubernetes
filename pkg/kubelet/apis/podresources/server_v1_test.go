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

package podresources

import (
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/mock"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	podresourcetest "k8s.io/kubernetes/pkg/kubelet/apis/podresources/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestListPodResourcesV1 验证 v1 ListPodResources 在设备、CPU、内存、
// 动态资源以及 pod-level resources 等组合下的返回结果。
func TestListPodResourcesV1(t *testing.T) {
	// 测试上下文携带 logger，Provider 打桩时需要同一个 logger 才能匹配调用。
	logger, tCtx := ktesting.NewTestContext(t)
	podName := "pod-name"
	podNamespace := "pod-namespace"
	podUID := types.UID("pod-uid")
	containerName := "container-name"
	numaID := int64(1)

	devs := []*podresourcesapi.ContainerDevices{
		{
			ResourceName: "resource",
			DeviceIds:    []string{"dev0", "dev1"},
			Topology:     &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
	}

	cpus := []int64{12, 23, 30}

	memory := []*podresourcesapi.ContainerMemory{
		{
			MemoryType: "memory",
			Size:       1073741824,
			Topology:   &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
		{
			MemoryType: "hugepages-1Gi",
			Size:       1073741824,
			Topology:   &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
	}

	containers := []v1.Container{
		{
			Name: containerName,
		},
	}
	pods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: podNamespace,
				UID:       podUID,
			},
			Spec: v1.PodSpec{
				Containers: containers,
			},
		},
	}
	podsWithPodLevelResources := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: podNamespace,
				UID:       podUID,
			},
			Spec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("5"),
						v1.ResourceMemory: resource.MustParse("5Gi"),
					},
					Limits: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("5"),
						v1.ResourceMemory: resource.MustParse("5Gi"),
					},
				},
				Containers: containers,
			},
		},
	}
	podsWithMultipleContainers := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: podNamespace,
				UID:       podUID,
			},
			Spec: v1.PodSpec{
				Resources: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("5"),
						v1.ResourceMemory: resource.MustParse("5Gi"),
					},
					Limits: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("5"),
						v1.ResourceMemory: resource.MustParse("5Gi"),
					},
				},
				Containers: []v1.Container{
					{
						Name: "exclusive-container",
					},
					{
						Name: "shared-container",
					},
				},
			},
		},
	}

	pluginCDIDevices := []*podresourcesapi.CDIDevice{{Name: "dra-dev0"}, {Name: "dra-dev1"}}
	draDriverName := "dra.example.com"
	poolName := "worker-1-pool"
	deviceName := "gpu-1"
	draDevs := []*podresourcesapi.DynamicResource{
		{
			ClaimName:      "claim-name",
			ClaimNamespace: "default",
			ClaimResources: []*podresourcesapi.ClaimResource{{CdiDevices: pluginCDIDevices, DriverName: draDriverName, PoolName: poolName, DeviceName: deviceName}},
		},
	}

	// 表驱动用例：每个用例声明 mock 返回值与期望响应。
	// 不变量：响应只包含当前 pod 及其容器的资源，pod-level 资源仅在特性门控开启时出现。
	for _, tc := range []struct {
		desc                     string
		pods                     []*v1.Pod
		devices                  []*podresourcesapi.ContainerDevices
		containerCpus            []int64
		containerMemory          []*podresourcesapi.ContainerMemory
		podCpus                  []int64
		podMemory                []*podresourcesapi.ContainerMemory
		dynamicResources         []*podresourcesapi.DynamicResource
		expectedResponse         *podresourcesapi.ListPodResourcesResponse
		podLevelResourcesEnabled bool
		targetContainerName      string
	}{
		{
			desc:             "no pods",
			pods:             []*v1.Pod{},
			devices:          []*podresourcesapi.ContainerDevices{},
			containerCpus:    []int64{},
			containerMemory:  []*podresourcesapi.ContainerMemory{},
			dynamicResources: []*podresourcesapi.DynamicResource{},
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{},
		},
		{
			desc:             "pod without devices",
			pods:             pods,
			devices:          []*podresourcesapi.ContainerDevices{},
			containerCpus:    []int64{},
			containerMemory:  []*podresourcesapi.ContainerMemory{},
			dynamicResources: []*podresourcesapi.DynamicResource{},
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             containerName,
								Devices:          []*podresourcesapi.ContainerDevices{},
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
						},
					},
				},
			},
		},
		{
			desc:             "pod with devices",
			pods:             pods,
			devices:          devs,
			containerCpus:    cpus,
			containerMemory:  memory,
			dynamicResources: []*podresourcesapi.DynamicResource{},
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             containerName,
								Devices:          devs,
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
						},
					},
				},
			},
		},
		{
			desc:                     "Topology Manager scope: pod, pod-level resources, exclusive container",
			pods:                     podsWithPodLevelResources,
			devices:                  []*podresourcesapi.ContainerDevices{},
			containerCpus:            cpus,
			containerMemory:          memory,
			podCpus:                  []int64{12, 23, 30, 31, 32},
			podMemory:                []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
			dynamicResources:         []*podresourcesapi.DynamicResource{},
			podLevelResourcesEnabled: true,
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						CpuIds:    []int64{12, 23, 30, 31, 32},
						Memory:    []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             containerName,
								Devices:          []*podresourcesapi.ContainerDevices{},
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
						},
					},
				},
			},
		},
		{
			desc:                     "Topology Manager scope: pod, pod-level resources, shared pool container",
			pods:                     podsWithPodLevelResources,
			devices:                  []*podresourcesapi.ContainerDevices{},
			containerCpus:            []int64{},
			containerMemory:          []*podresourcesapi.ContainerMemory{},
			podCpus:                  []int64{12, 23, 30, 31, 32},
			podMemory:                []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
			dynamicResources:         []*podresourcesapi.DynamicResource{},
			podLevelResourcesEnabled: true,
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						CpuIds:    []int64{12, 23, 30, 31, 32},
						Memory:    []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             containerName,
								Devices:          []*podresourcesapi.ContainerDevices{},
								CpuIds:           []int64{},
								Memory:           []*podresourcesapi.ContainerMemory{},
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
						},
					},
				},
			},
		},
		{
			desc:                     "Topology Manager scope: pod, pod-level resources, mix of exclusive and shared pool containers",
			pods:                     podsWithMultipleContainers,
			devices:                  []*podresourcesapi.ContainerDevices{},
			containerCpus:            cpus,
			containerMemory:          memory,
			podCpus:                  []int64{12, 23, 30, 31, 32},
			podMemory:                []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
			dynamicResources:         []*podresourcesapi.DynamicResource{},
			podLevelResourcesEnabled: true,
			targetContainerName:      "exclusive-container",
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						CpuIds:    []int64{12, 23, 30, 31, 32},
						Memory:    []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             "exclusive-container",
								Devices:          []*podresourcesapi.ContainerDevices{},
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
							{
								Name:             "shared-container",
								Devices:          []*podresourcesapi.ContainerDevices{},
								CpuIds:           []int64{},
								Memory:           []*podresourcesapi.ContainerMemory{},
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
						},
					},
				},
			},
		},
		{
			desc:                     "Topology Manager scope: container, pod-level resources, exclusive container",
			pods:                     podsWithPodLevelResources,
			devices:                  []*podresourcesapi.ContainerDevices{},
			containerCpus:            cpus,
			containerMemory:          memory,
			podCpus:                  []int64{},
			podMemory:                []*podresourcesapi.ContainerMemory{},
			dynamicResources:         []*podresourcesapi.DynamicResource{},
			podLevelResourcesEnabled: true,
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						CpuIds:    []int64{},
						Memory:    []*podresourcesapi.ContainerMemory{},
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             containerName,
								Devices:          []*podresourcesapi.ContainerDevices{},
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
						},
					},
				},
			},
		},
		{
			desc:                     "Topology Manager scope: container, pod-level resources, mix of exclusive and shared pool containers",
			pods:                     podsWithMultipleContainers,
			devices:                  []*podresourcesapi.ContainerDevices{},
			containerCpus:            cpus,
			containerMemory:          memory,
			podCpus:                  []int64{},
			podMemory:                []*podresourcesapi.ContainerMemory{},
			dynamicResources:         []*podresourcesapi.DynamicResource{},
			podLevelResourcesEnabled: true,
			targetContainerName:      "exclusive-container",
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						CpuIds:    []int64{},
						Memory:    []*podresourcesapi.ContainerMemory{},
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             "exclusive-container",
								Devices:          []*podresourcesapi.ContainerDevices{},
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
							{
								Name:             "shared-container",
								Devices:          []*podresourcesapi.ContainerDevices{},
								CpuIds:           []int64{},
								Memory:           []*podresourcesapi.ContainerMemory{},
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
						},
					},
				},
			},
		},
		{
			desc:             "pod with dynamic resources",
			pods:             pods,
			devices:          []*podresourcesapi.ContainerDevices{},
			containerCpus:    cpus,
			containerMemory:  memory,
			dynamicResources: draDevs,
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             containerName,
								Devices:          []*podresourcesapi.ContainerDevices{},
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: draDevs,
							},
						},
					},
				},
			},
		},
		{
			desc:             "pod with dynamic resources and devices",
			pods:             pods,
			devices:          devs,
			containerCpus:    cpus,
			containerMemory:  memory,
			dynamicResources: draDevs,
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             containerName,
								Devices:          devs,
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: draDevs,
							},
						},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			// 仅在 pod-level 资源用例中打开门控，避免影响其他用例的默认行为。
			if tc.podLevelResourcesEnabled {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResources, tc.podLevelResourcesEnabled)
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResourceManagers, tc.podLevelResourcesEnabled)
			}

			mockDevicesProvider := podresourcetest.NewMockDevicesProvider(t)
			mockPodsProvider := podresourcetest.NewMockPodsProvider(t)
			mockCPUsProvider := podresourcetest.NewMockCPUsProvider(t)
			mockMemoryProvider := podresourcetest.NewMockMemoryProvider(t)
			mockDynamicResourcesProvider := podresourcetest.NewMockDynamicResourcesProvider(t)

			mockPodsProvider.EXPECT().GetPods().Return(tc.pods).Maybe()
			mockPodsProvider.EXPECT().GetActivePods().Return(tc.pods).Maybe()
			// 1. 按用例声明的值精确打桩，只覆盖该用例真正关心的资源类型。
			// 未指定目标容器时默认使用普通容器名，覆盖大多数用例。
			targetName := tc.targetContainerName
			if targetName == "" {
				targetName = containerName
			}

			if len(tc.devices) > 0 {
				mockDevicesProvider.EXPECT().GetDevices(string(podUID), targetName).Return(tc.devices).Maybe()
			}
			if len(tc.containerCpus) > 0 {
				mockCPUsProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == targetName })).Return(tc.containerCpus).Maybe()
			}
			if len(tc.containerMemory) > 0 {
				mockMemoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == targetName })).Return(tc.containerMemory).Maybe()
			}
			if tc.podLevelResourcesEnabled {
				mockCPUsProvider.EXPECT().GetPodCPUs(string(podUID)).Return(tc.podCpus).Maybe()
				mockMemoryProvider.EXPECT().GetPodMemory(logger, string(podUID)).Return(tc.podMemory).Maybe()
			}
			if len(tc.dynamicResources) > 0 {
				mockDynamicResourcesProvider.EXPECT().GetDynamicResources(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == targetName })).Return(tc.dynamicResources).Maybe()
			}

			// 2. 兜底打桩：未声明的调用返回空切片；testify 按注册顺序匹配，具体期望优先生效。
			mockCPUsProvider.EXPECT().GetCPUs(mock.Anything, mock.Anything).Return([]int64{}).Maybe()
			mockCPUsProvider.EXPECT().GetPodCPUs(mock.Anything).Return([]int64{}).Maybe()
			mockCPUsProvider.EXPECT().GetAllocatableCPUs().Return([]int64{}).Maybe()
			mockMemoryProvider.EXPECT().GetMemory(logger, mock.Anything, mock.Anything).Return([]*podresourcesapi.ContainerMemory{}).Maybe()
			mockMemoryProvider.EXPECT().GetPodMemory(logger, mock.Anything).Return([]*podresourcesapi.ContainerMemory{}).Maybe()
			mockMemoryProvider.EXPECT().GetAllocatableMemory(logger).Return([]*podresourcesapi.ContainerMemory{}).Maybe()
			mockDevicesProvider.EXPECT().GetDevices(mock.Anything, mock.Anything).Return([]*podresourcesapi.ContainerDevices{}).Maybe()
			mockDevicesProvider.EXPECT().GetAllocatableDevices(logger).Return([]*podresourcesapi.ContainerDevices{}).Maybe()
			mockDevicesProvider.EXPECT().UpdateAllocatedDevices(logger).Return().Maybe()
			mockDynamicResourcesProvider.EXPECT().GetDynamicResources(logger, mock.Anything, mock.Anything).Return([]*podresourcesapi.DynamicResource{}).Maybe()

			// 组装 Provider 集合，模拟 kubelet 各资源管理器对外暴露的视图。
			providers := PodResourcesProviders{
				Pods:             mockPodsProvider,
				Devices:          mockDevicesProvider,
				Cpus:             mockCPUsProvider,
				Memory:           mockMemoryProvider,
				DynamicResources: mockDynamicResourcesProvider,
			}
			// 构造 gRPC 服务实现；List 是 podresources v1 API 的查询入口。
			server := NewV1PodResourcesServer(tCtx, providers)
			resp, err := server.List(tCtx, &podresourcesapi.ListPodResourcesRequest{})
			if err != nil {
				t.Errorf("want err = %v, got %q", nil, err)
			}
			// 用 protobuf String() 比较，规避 nil 与空切片在内部表示上的差异。
			if tc.expectedResponse.String() != resp.String() {
				t.Fatalf("want: %+v; got: %+v", tc.expectedResponse, resp)
			}
		})
	}
}

// makePod 构造第 idx 个测试 pod，名称与 UID 均带索引以便区分。
func makePod(idx int) *v1.Pod {
	// 带上索引保证每个 pod 唯一，便于按名字断言过滤结果。
	podNamespace := "pod-namespace"
	podName := fmt.Sprintf("pod-name-%d", idx)
	podUID := types.UID(fmt.Sprintf("pod-uid-%d", idx))
	containerName := fmt.Sprintf("container-name-%d", idx)
	containers := []v1.Container{
		{
			Name: containerName,
		},
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: podNamespace,
			UID:       podUID,
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
}

// collectNamespacedNamesFromPods 提取 pod 的 namespace/name 并排序。
func collectNamespacedNamesFromPods(pods []*v1.Pod) []string {
	ret := make([]string, 0, len(pods))
	for _, pod := range pods {
		ret = append(ret, pod.Namespace+"/"+pod.Name)
	}
	// 排序后比较，消除 Provider 返回顺序对断言的影响。
	sort.Strings(ret)
	return ret
}

// collectNamespacedNamesFromPodResources 提取响应中 pod 的 namespace/name 并排序。
func collectNamespacedNamesFromPodResources(prs []*podresourcesapi.PodResources) []string {
	ret := make([]string, 0, len(prs))
	for _, pr := range prs {
		ret = append(ret, pr.Namespace+"/"+pr.Name)
	}
	sort.Strings(ret)
	return ret
}

// TestListPodResourcesUsesOnlyActivePodsV1 验证 ListPodResources 只返回 active pod，
// 已终止但仍被 GetPods 返回的 pod 必须被过滤掉。
func TestListPodResourcesUsesOnlyActivePodsV1(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	numaID := int64(1)

	// 这里刻意对所有 pod 复用同一份分配结果：本测试只关心哪些 pod 出现在响应里，
	// 不关心具体分配内容，所以这种复用并不符合真实语义。
	devs := []*podresourcesapi.ContainerDevices{
		{
			ResourceName: "resource",
			DeviceIds:    []string{"dev0"},
			Topology:     &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
	}

	cpus := []int64{1, 9}
	podCpus := []int64{1, 9, 10, 11, 12}

	mems := []*podresourcesapi.ContainerMemory{
		{
			MemoryType: "memory",
			Size:       1073741824,
			Topology:   &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
		{
			MemoryType: "hugepages-1Gi",
			Size:       1073741824,
			Topology:   &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
	}

	// 表驱动用例：GetPods 与 GetActivePods 返回不同集合，用于验证按 active pod 过滤。
	for _, tc := range []struct {
		desc                     string
		pods                     []*v1.Pod
		activePods               []*v1.Pod
		podLevelResourcesEnabled bool
	}{
		{
			desc:       "no pods",
			pods:       []*v1.Pod{},
			activePods: []*v1.Pod{},
		},
		{
			desc: "no differences",
			pods: []*v1.Pod{
				makePod(1),
				makePod(2),
				makePod(3),
				makePod(4),
				makePod(5),
			},
			activePods: []*v1.Pod{
				makePod(1),
				makePod(2),
				makePod(3),
				makePod(4),
				makePod(5),
			},
		},
		{
			desc: "some terminated pods",
			pods: []*v1.Pod{
				makePod(1),
				makePod(2),
				makePod(3),
				makePod(4),
				makePod(5),
				makePod(6),
				makePod(7),
			},
			activePods: []*v1.Pod{
				makePod(1),
				makePod(3),
				makePod(4),
				makePod(5),
				makePod(6),
			},
		},
		{
			desc: "some terminated pods AND pod level resources enabled",
			pods: []*v1.Pod{
				makePod(1),
				makePod(2),
				makePod(3),
				makePod(4),
				makePod(5),
				makePod(6),
				makePod(7),
			},
			activePods: []*v1.Pod{
				makePod(1),
				makePod(3),
				makePod(4),
				makePod(5),
				makePod(6),
			},
			podLevelResourcesEnabled: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.podLevelResourcesEnabled {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResources, tc.podLevelResourcesEnabled)
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResourceManagers, tc.podLevelResourcesEnabled)
			}

			// 每个用例单独创建 mock，避免调用期望在用例之间泄漏。
			mockDevicesProvider := podresourcetest.NewMockDevicesProvider(t)
			mockPodsProvider := podresourcetest.NewMockPodsProvider(t)
			mockCPUsProvider := podresourcetest.NewMockCPUsProvider(t)
			mockMemoryProvider := podresourcetest.NewMockMemoryProvider(t)
			mockDynamicResourcesProvider := podresourcetest.NewMockDynamicResourcesProvider(t)

			mockPodsProvider.EXPECT().GetPods().Return(tc.pods).Maybe()
			mockPodsProvider.EXPECT().GetActivePods().Return(tc.activePods).Maybe()
			mockDevicesProvider.EXPECT().GetDevices(mock.Anything, mock.Anything).Return(devs).Maybe()
			mockCPUsProvider.EXPECT().GetCPUs(mock.Anything, mock.Anything).Return(cpus).Maybe()
			if tc.podLevelResourcesEnabled {
				mockCPUsProvider.EXPECT().GetPodCPUs(mock.Anything).Return(podCpus).Maybe()
				mockMemoryProvider.EXPECT().GetPodMemory(logger, mock.Anything).Return([]*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}}).Maybe()
			}
			mockMemoryProvider.EXPECT().GetMemory(logger, mock.Anything, mock.Anything).Return(mems).Maybe()
			mockDevicesProvider.EXPECT().UpdateAllocatedDevices(logger).Return().Maybe()
			mockCPUsProvider.EXPECT().GetAllocatableCPUs().Return([]int64{}).Maybe()
			mockCPUsProvider.EXPECT().GetPodCPUs(mock.Anything).Return([]int64{}).Maybe()
			mockMemoryProvider.EXPECT().GetPodMemory(logger, mock.Anything).Return([]*podresourcesapi.ContainerMemory{}).Maybe()
			mockDevicesProvider.EXPECT().GetAllocatableDevices(logger).Return([]*podresourcesapi.ContainerDevices{}).Maybe()
			mockMemoryProvider.EXPECT().GetAllocatableMemory(logger).Return([]*podresourcesapi.ContainerMemory{}).Maybe()
			mockDynamicResourcesProvider.EXPECT().GetDynamicResources(logger, mock.Anything, mock.Anything).Return([]*podresourcesapi.DynamicResource{}).Maybe()

			providers := PodResourcesProviders{
				Pods:             mockPodsProvider,
				Devices:          mockDevicesProvider,
				Cpus:             mockCPUsProvider,
				Memory:           mockMemoryProvider,
				DynamicResources: mockDynamicResourcesProvider,
			}
			server := NewV1PodResourcesServer(tCtx, providers)
			resp, err := server.List(tCtx, &podresourcesapi.ListPodResourcesRequest{})
			if err != nil {
				t.Errorf("want err = %v, got %q", nil, err)
			}
			// 核心不变量：响应中的 pod 集合必须与 activePods 完全一致。
			expectedNames := collectNamespacedNamesFromPods(tc.activePods)
			gotNames := collectNamespacedNamesFromPodResources(resp.GetPodResources())
			if diff := cmp.Diff(expectedNames, gotNames, cmpopts.EquateEmpty()); diff != "" {
				t.Fatal(diff)
			}
		})
	}
}

// TestListPodResourcesWithInitContainersV1 验证普通 init container 与可重启 init container
// 是否作为容器资源出现在 ListPodResources 响应中。
func TestListPodResourcesWithInitContainersV1(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	podName := "pod-name"
	podNamespace := "pod-namespace"
	podUID := types.UID("pod-uid")
	initContainerName := "init-container-name"
	containerName := "container-name"
	numaID := int64(1)
	containerRestartPolicyAlways := v1.ContainerRestartPolicyAlways

	devs := []*podresourcesapi.ContainerDevices{
		{
			ResourceName: "resource",
			DeviceIds:    []string{"dev0", "dev1"},
			Topology:     &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
	}

	cpus := []int64{12, 23, 30}
	podCpus := []int64{12, 23, 30, 31, 32}

	memory := []*podresourcesapi.ContainerMemory{
		{
			MemoryType: "memory",
			Size:       1073741824,
			Topology:   &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
		{
			MemoryType: "hugepages-1Gi",
			Size:       1073741824,
			Topology:   &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
	}

	containers := []v1.Container{
		{
			Name: containerName,
		},
	}

	// 表驱动用例：init container 仅在可重启（sidecar）时才会被当作长期运行的容器返回。
	for _, tc := range []struct {
		desc     string
		pods     []*v1.Pod
		mockFunc func(
			[]*v1.Pod,
			*podresourcetest.MockDevicesProvider,
			*podresourcetest.MockCPUsProvider,
			*podresourcetest.MockMemoryProvider,
			*podresourcetest.MockDynamicResourcesProvider)
		expectedResponse         *podresourcesapi.ListPodResourcesResponse
		podLevelResourcesEnabled bool
	}{
		{
			desc: "pod having an init container AND pod level resources enabled",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: podNamespace,
						UID:       podUID,
					},
					Spec: v1.PodSpec{
						Resources: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("5"),
								v1.ResourceMemory: resource.MustParse("5Gi"),
							},
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("5"),
								v1.ResourceMemory: resource.MustParse("5Gi"),
							},
						},
						InitContainers: []v1.Container{
							{
								Name: initContainerName,
							},
						},
						Containers: containers,
					},
				},
			},
			mockFunc: func(
				pods []*v1.Pod,
				devicesProvider *podresourcetest.MockDevicesProvider,
				cpusProvider *podresourcetest.MockCPUsProvider,
				memoryProvider *podresourcetest.MockMemoryProvider,
				dynamicResourcesProvider *podresourcetest.MockDynamicResourcesProvider) {
				devicesProvider.EXPECT().UpdateAllocatedDevices(logger).Return().Maybe()
				devicesProvider.EXPECT().GetDevices(string(podUID), containerName).Return([]*podresourcesapi.ContainerDevices{}).Maybe()
				cpusProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(cpus).Maybe()
				memoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(memory).Maybe()
				dynamicResourcesProvider.EXPECT().GetDynamicResources(logger, pods[0], &pods[0].Spec.Containers[0]).Return([]*podresourcesapi.DynamicResource{}).Maybe()

				cpusProvider.EXPECT().GetPodCPUs(mock.Anything).Return(podCpus).Maybe()
				memoryProvider.EXPECT().GetPodMemory(logger, mock.Anything).Return([]*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}}).Maybe()
			},
			podLevelResourcesEnabled: true,
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						CpuIds:    podCpus,
						Memory:    []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             containerName,
								Devices:          []*podresourcesapi.ContainerDevices{},
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
						},
					},
				},
			},
		},
		{
			desc: "pod having a restartable init container AND pod level resources enabled",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: podNamespace,
						UID:       podUID,
					},
					Spec: v1.PodSpec{
						Resources: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("5"),
								v1.ResourceMemory: resource.MustParse("5Gi"),
							},
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("5"),
								v1.ResourceMemory: resource.MustParse("5Gi"),
							},
						},
						InitContainers: []v1.Container{
							{
								Name:          initContainerName,
								RestartPolicy: &containerRestartPolicyAlways,
							},
						},
						Containers: containers,
					},
				},
			},
			mockFunc: func(
				pods []*v1.Pod,
				devicesProvider *podresourcetest.MockDevicesProvider,
				cpusProvider *podresourcetest.MockCPUsProvider,
				memoryProvider *podresourcetest.MockMemoryProvider,
				dynamicResourcesProvider *podresourcetest.MockDynamicResourcesProvider) {
				devicesProvider.EXPECT().UpdateAllocatedDevices(logger).Return().Maybe()

				devicesProvider.EXPECT().GetDevices(string(podUID), initContainerName).Return([]*podresourcesapi.ContainerDevices{}).Maybe()
				cpusProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == initContainerName })).Return(cpus).Maybe()
				memoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == initContainerName })).Return(memory).Maybe()
				dynamicResourcesProvider.EXPECT().GetDynamicResources(logger, pods[0], &pods[0].Spec.InitContainers[0]).Return([]*podresourcesapi.DynamicResource{}).Maybe()

				devicesProvider.EXPECT().GetDevices(string(podUID), containerName).Return([]*podresourcesapi.ContainerDevices{}).Maybe()
				cpusProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(cpus).Maybe()
				memoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(memory).Maybe()
				dynamicResourcesProvider.EXPECT().GetDynamicResources(logger, pods[0], &pods[0].Spec.Containers[0]).Return([]*podresourcesapi.DynamicResource{}).Maybe()

				cpusProvider.EXPECT().GetPodCPUs(mock.Anything).Return(podCpus).Maybe()
				memoryProvider.EXPECT().GetPodMemory(logger, mock.Anything).Return([]*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}}).Maybe()
			},
			podLevelResourcesEnabled: true,
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						CpuIds:    podCpus,
						Memory:    []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             initContainerName,
								Devices:          []*podresourcesapi.ContainerDevices{},
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
							{
								Name:             containerName,
								Devices:          []*podresourcesapi.ContainerDevices{},
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
						},
					},
				},
			},
		},
		{
			desc: "pod having an init container",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: podNamespace,
						UID:       podUID,
					},
					Spec: v1.PodSpec{
						InitContainers: []v1.Container{
							{
								Name: initContainerName,
							},
						},
						Containers: containers,
					},
				},
			},
			mockFunc: func(
				pods []*v1.Pod,
				devicesProvider *podresourcetest.MockDevicesProvider,
				cpusProvider *podresourcetest.MockCPUsProvider,
				memoryProvider *podresourcetest.MockMemoryProvider,
				dynamicResourcesProvider *podresourcetest.MockDynamicResourcesProvider) {
				devicesProvider.EXPECT().UpdateAllocatedDevices(logger).Return().Maybe()
				devicesProvider.EXPECT().GetDevices(string(podUID), containerName).Return(devs).Maybe()
				cpusProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(cpus).Maybe()
				memoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(memory).Maybe()
				dynamicResourcesProvider.EXPECT().GetDynamicResources(logger, pods[0], &pods[0].Spec.Containers[0]).Return([]*podresourcesapi.DynamicResource{}).Maybe()

			},
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             containerName,
								Devices:          devs,
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
						},
					},
				},
			},
		},
		{
			desc: "pod having a restartable init container",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: podNamespace,
						UID:       podUID,
					},
					Spec: v1.PodSpec{
						InitContainers: []v1.Container{
							{
								Name:          initContainerName,
								RestartPolicy: &containerRestartPolicyAlways,
							},
						},
						Containers: containers,
					},
				},
			},
			mockFunc: func(
				pods []*v1.Pod,
				devicesProvider *podresourcetest.MockDevicesProvider,
				cpusProvider *podresourcetest.MockCPUsProvider,
				memoryProvider *podresourcetest.MockMemoryProvider,
				dynamicResourcesProvider *podresourcetest.MockDynamicResourcesProvider) {
				devicesProvider.EXPECT().UpdateAllocatedDevices(logger).Return().Maybe()

				devicesProvider.EXPECT().GetDevices(string(podUID), initContainerName).Return(devs).Maybe()
				cpusProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == initContainerName })).Return(cpus).Maybe()
				memoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == initContainerName })).Return(memory).Maybe()
				dynamicResourcesProvider.EXPECT().GetDynamicResources(logger, pods[0], &pods[0].Spec.InitContainers[0]).Return([]*podresourcesapi.DynamicResource{}).Maybe()

				devicesProvider.EXPECT().GetDevices(string(podUID), containerName).Return(devs).Maybe()
				cpusProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(cpus).Maybe()
				memoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(memory).Maybe()
				dynamicResourcesProvider.EXPECT().GetDynamicResources(logger, pods[0], &pods[0].Spec.Containers[0]).Return([]*podresourcesapi.DynamicResource{}).Maybe()

			},
			expectedResponse: &podresourcesapi.ListPodResourcesResponse{
				PodResources: []*podresourcesapi.PodResources{
					{
						Name:      podName,
						Namespace: podNamespace,
						Containers: []*podresourcesapi.ContainerResources{
							{
								Name:             initContainerName,
								Devices:          devs,
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
							{
								Name:             containerName,
								Devices:          devs,
								CpuIds:           cpus,
								Memory:           memory,
								DynamicResources: []*podresourcesapi.DynamicResource{},
							},
						},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.podLevelResourcesEnabled {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResources, tc.podLevelResourcesEnabled)
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResourceManagers, tc.podLevelResourcesEnabled)
			}

			mockDevicesProvider := podresourcetest.NewMockDevicesProvider(t)
			mockPodsProvider := podresourcetest.NewMockPodsProvider(t)
			mockCPUsProvider := podresourcetest.NewMockCPUsProvider(t)
			mockMemoryProvider := podresourcetest.NewMockMemoryProvider(t)
			mockDynamicResourcesProvider := podresourcetest.NewMockDynamicResourcesProvider(t)

			mockPodsProvider.EXPECT().GetPods().Return(tc.pods).Maybe()
			mockPodsProvider.EXPECT().GetActivePods().Return(tc.pods).Maybe()
			// 先注册用例自定义期望，再注册兜底；testify 按注册顺序匹配，具体期望优先生效。
			tc.mockFunc(tc.pods, mockDevicesProvider, mockCPUsProvider, mockMemoryProvider, mockDynamicResourcesProvider)
			mockCPUsProvider.EXPECT().GetPodCPUs(mock.Anything).Return([]int64{}).Maybe()
			mockMemoryProvider.EXPECT().GetPodMemory(logger, mock.Anything).Return([]*podresourcesapi.ContainerMemory{}).Maybe()

			providers := PodResourcesProviders{
				Pods:             mockPodsProvider,
				Devices:          mockDevicesProvider,
				Cpus:             mockCPUsProvider,
				Memory:           mockMemoryProvider,
				DynamicResources: mockDynamicResourcesProvider,
			}
			server := NewV1PodResourcesServer(tCtx, providers)
			resp, err := server.List(tCtx, &podresourcesapi.ListPodResourcesRequest{})
			if err != nil {
				t.Errorf("want err = %v, got %q", nil, err)
			}
			if tc.expectedResponse.String() != resp.String() {
				t.Fatalf("want: %+v; got: %+v", tc.expectedResponse, resp)
			}
		})
	}
}

// TestAllocatableResources 验证 GetAllocatableResources 聚合设备、CPU 与内存的
// 可分配总量，且结果与 pod 无关。
func TestAllocatableResources(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	allDevs := []*podresourcesapi.ContainerDevices{
		{
			ResourceName: "resource",
			DeviceIds:    []string{"dev0"},
			Topology: &podresourcesapi.TopologyInfo{
				Nodes: []*podresourcesapi.NUMANode{
					{
						ID: 0,
					},
				},
			},
		},
		{
			ResourceName: "resource",
			DeviceIds:    []string{"dev1"},
			Topology: &podresourcesapi.TopologyInfo{
				Nodes: []*podresourcesapi.NUMANode{
					{
						ID: 1,
					},
				},
			},
		},
		{
			ResourceName: "resource-nt",
			DeviceIds:    []string{"devA"},
		},
		{
			ResourceName: "resource-mm",
			DeviceIds:    []string{"devM0"},
			Topology: &podresourcesapi.TopologyInfo{
				Nodes: []*podresourcesapi.NUMANode{
					{
						ID: 0,
					},
				},
			},
		},
		{
			ResourceName: "resource-mm",
			DeviceIds:    []string{"devMM"},
			Topology: &podresourcesapi.TopologyInfo{
				Nodes: []*podresourcesapi.NUMANode{
					{
						ID: 0,
					},
					{
						ID: 1,
					},
				},
			},
		},
	}

	allCPUs := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	allMemory := []*podresourcesapi.ContainerMemory{
		{
			MemoryType: "memory",
			Size:       5368709120,
			Topology: &podresourcesapi.TopologyInfo{
				Nodes: []*podresourcesapi.NUMANode{
					{
						ID: 0,
					},
				},
			},
		},
		{
			MemoryType: "hugepages-2Mi",
			Size:       1073741824,
			Topology: &podresourcesapi.TopologyInfo{
				Nodes: []*podresourcesapi.NUMANode{
					{
						ID: 0,
					},
				},
			},
		},
		{
			MemoryType: "memory",
			Size:       5368709120,
			Topology: &podresourcesapi.TopologyInfo{
				Nodes: []*podresourcesapi.NUMANode{
					{
						ID: 1,
					},
				},
			},
		},
		{
			MemoryType: "hugepages-2Mi",
			Size:       1073741824,
			Topology: &podresourcesapi.TopologyInfo{
				Nodes: []*podresourcesapi.NUMANode{
					{
						ID: 1,
					},
				},
			},
		},
	}

	// 表驱动用例：分别验证仅 CPU、仅内存、仅设备以及组合场景下的聚合结果。
	for _, tc := range []struct {
		desc                                 string
		allCPUs                              []int64
		allDevices                           []*podresourcesapi.ContainerDevices
		allMemory                            []*podresourcesapi.ContainerMemory
		expectedAllocatableResourcesResponse *podresourcesapi.AllocatableResourcesResponse
	}{
		{
			desc:                                 "no devices, no CPUs",
			allCPUs:                              []int64{},
			allDevices:                           []*podresourcesapi.ContainerDevices{},
			allMemory:                            []*podresourcesapi.ContainerMemory{},
			expectedAllocatableResourcesResponse: &podresourcesapi.AllocatableResourcesResponse{},
		},
		{
			desc:       "no devices, all CPUs",
			allCPUs:    allCPUs,
			allDevices: []*podresourcesapi.ContainerDevices{},
			expectedAllocatableResourcesResponse: &podresourcesapi.AllocatableResourcesResponse{
				CpuIds: allCPUs,
			},
		},
		{
			desc:       "no devices, no CPUs, all memory",
			allCPUs:    []int64{},
			allDevices: []*podresourcesapi.ContainerDevices{},
			allMemory:  allMemory,
			expectedAllocatableResourcesResponse: &podresourcesapi.AllocatableResourcesResponse{
				Memory: allMemory,
			},
		},
		{
			desc:       "with devices, all CPUs",
			allCPUs:    allCPUs,
			allDevices: allDevs,
			expectedAllocatableResourcesResponse: &podresourcesapi.AllocatableResourcesResponse{
				CpuIds: allCPUs,
				Devices: []*podresourcesapi.ContainerDevices{
					{
						ResourceName: "resource",
						DeviceIds:    []string{"dev0"},
						Topology: &podresourcesapi.TopologyInfo{
							Nodes: []*podresourcesapi.NUMANode{
								{
									ID: 0,
								},
							},
						},
					},
					{
						ResourceName: "resource",
						DeviceIds:    []string{"dev1"},
						Topology: &podresourcesapi.TopologyInfo{
							Nodes: []*podresourcesapi.NUMANode{
								{
									ID: 1,
								},
							},
						},
					},
					{
						ResourceName: "resource-nt",
						DeviceIds:    []string{"devA"},
					},
					{
						ResourceName: "resource-mm",
						DeviceIds:    []string{"devM0"},
						Topology: &podresourcesapi.TopologyInfo{
							Nodes: []*podresourcesapi.NUMANode{
								{
									ID: 0,
								},
							},
						},
					},
					{
						ResourceName: "resource-mm",
						DeviceIds:    []string{"devMM"},
						Topology: &podresourcesapi.TopologyInfo{
							Nodes: []*podresourcesapi.NUMANode{
								{
									ID: 0,
								},
								{
									ID: 1,
								},
							},
						},
					},
				},
			},
		},
		{
			desc:       "with devices, no CPUs",
			allCPUs:    []int64{},
			allDevices: allDevs,
			expectedAllocatableResourcesResponse: &podresourcesapi.AllocatableResourcesResponse{
				Devices: []*podresourcesapi.ContainerDevices{
					{
						ResourceName: "resource",
						DeviceIds:    []string{"dev0"},
						Topology: &podresourcesapi.TopologyInfo{
							Nodes: []*podresourcesapi.NUMANode{
								{
									ID: 0,
								},
							},
						},
					},
					{
						ResourceName: "resource",
						DeviceIds:    []string{"dev1"},
						Topology: &podresourcesapi.TopologyInfo{
							Nodes: []*podresourcesapi.NUMANode{
								{
									ID: 1,
								},
							},
						},
					},
					{
						ResourceName: "resource-nt",
						DeviceIds:    []string{"devA"},
					},
					{
						ResourceName: "resource-mm",
						DeviceIds:    []string{"devM0"},
						Topology: &podresourcesapi.TopologyInfo{
							Nodes: []*podresourcesapi.NUMANode{
								{
									ID: 0,
								},
							},
						},
					},
					{
						ResourceName: "resource-mm",
						DeviceIds:    []string{"devMM"},
						Topology: &podresourcesapi.TopologyInfo{
							Nodes: []*podresourcesapi.NUMANode{
								{
									ID: 0,
								},
								{
									ID: 1,
								},
							},
						},
					},
				},
			},
		},
	} {
		// 每个用例独立创建 mock，避免期望跨用例累积。
		t.Run(tc.desc, func(t *testing.T) {
			mockDevicesProvider := podresourcetest.NewMockDevicesProvider(t)
			mockPodsProvider := podresourcetest.NewMockPodsProvider(t)
			mockCPUsProvider := podresourcetest.NewMockCPUsProvider(t)
			mockMemoryProvider := podresourcetest.NewMockMemoryProvider(t)

			// 可分配资源查询不针对具体 pod/容器，因此用空参数打桩以防误用。
			mockDevicesProvider.EXPECT().GetDevices("", "").Return([]*podresourcesapi.ContainerDevices{}).Maybe()
			mockCPUsProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == "" }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == "" })).Return([]int64{}).Maybe()
			mockMemoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == "" }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == "" })).Return([]*podresourcesapi.ContainerMemory{}).Maybe()
			mockDevicesProvider.EXPECT().UpdateAllocatedDevices(logger).Return().Maybe()
			mockDevicesProvider.EXPECT().GetAllocatableDevices(logger).Return(tc.allDevices).Maybe()
			mockCPUsProvider.EXPECT().GetAllocatableCPUs().Return(tc.allCPUs).Maybe()
			mockMemoryProvider.EXPECT().GetAllocatableMemory(logger).Return(tc.allMemory).Maybe()

			providers := PodResourcesProviders{
				Pods:    mockPodsProvider,
				Devices: mockDevicesProvider,
				Cpus:    mockCPUsProvider,
				Memory:  mockMemoryProvider,
			}
			server := NewV1PodResourcesServer(tCtx, providers)

			// 直接查询设备、CPU、内存管理器的可分配状态，不经过 pod 列表。
			resp, err := server.GetAllocatableResources(tCtx, &podresourcesapi.AllocatableResourcesRequest{})
			if err != nil {
				t.Errorf("want err = %v, got %q", nil, err)
			}

			if tc.expectedAllocatableResourcesResponse.String() != resp.String() {
				t.Fatalf("want: %+v; got: %+v", tc.expectedAllocatableResourcesResponse, resp)
			}
		})
	}
}

// TestGetPodResourcesV1 验证 GetPodResources 按 namespace/name 查询单个 pod 的资源，
// 覆盖 pod 不存在、pod-level 资源以及多容器等场景。
func TestGetPodResourcesV1(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	podName := "pod-name"
	podNamespace := "pod-namespace"
	podUID := types.UID("pod-uid")
	containerName := "container-name"
	numaID := int64(1)

	devs := []*podresourcesapi.ContainerDevices{
		{
			ResourceName: "resource",
			DeviceIds:    []string{"dev0", "dev1"},
			Topology:     &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
	}

	cpus := []int64{12, 23, 30}
	podCpus := []int64{12, 23, 30, 31, 32}

	memory := []*podresourcesapi.ContainerMemory{
		{
			MemoryType: "memory",
			Size:       1073741824,
			Topology:   &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
		{
			MemoryType: "hugepages-1Gi",
			Size:       1073741824,
			Topology:   &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
	}

	containers := []v1.Container{
		{
			Name: containerName,
		},
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: podNamespace,
			UID:       podUID,
		},
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
	podWithPodLevelResources := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: podNamespace,
			UID:       podUID,
		},
		Spec: v1.PodSpec{
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("5"),
					v1.ResourceMemory: resource.MustParse("5Gi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("5"),
					v1.ResourceMemory: resource.MustParse("5Gi"),
				},
			},
			Containers: containers,
		},
	}
	podWithMultipleContainers := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: podNamespace,
			UID:       podUID,
		},
		Spec: v1.PodSpec{
			Resources: &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("5"),
					v1.ResourceMemory: resource.MustParse("5Gi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("5"),
					v1.ResourceMemory: resource.MustParse("5Gi"),
				},
			},
			Containers: []v1.Container{
				{
					Name: "exclusive-container",
				},
				{
					Name: "shared-container",
				},
			},
		},
	}

	pluginCDIDevices := []*podresourcesapi.CDIDevice{{Name: "dra-dev0"}, {Name: "dra-dev1"}}
	draDriverName := "dra.example.com"
	poolName := "worker-1-pool"
	deviceName := "gpu-1"
	draDevs := []*podresourcesapi.DynamicResource{
		{
			ClaimName:      "claim-name",
			ClaimNamespace: "default",
			ClaimResources: []*podresourcesapi.ClaimResource{{CdiDevices: pluginCDIDevices, DriverName: draDriverName, PoolName: poolName, DeviceName: deviceName}},
		},
	}

	// 表驱动用例：不变量是 pod 不存在时返回错误，存在时响应等价于 List 中该 pod 的子集。
	for _, tc := range []struct {
		desc                     string
		err                      error
		exist                    bool
		pod                      *v1.Pod
		devices                  []*podresourcesapi.ContainerDevices
		containerCpus            []int64
		containerMemory          []*podresourcesapi.ContainerMemory
		podCpus                  []int64
		podMemory                []*podresourcesapi.ContainerMemory
		dynamicResources         []*podresourcesapi.DynamicResource
		expectedResponse         *podresourcesapi.GetPodResourcesResponse
		podLevelResourcesEnabled bool
		targetContainerName      string
	}{
		{
			desc:             "pod not exist",
			err:              fmt.Errorf("pod %s in namespace %s not found", podName, podNamespace),
			exist:            false,
			pod:              nil,
			devices:          []*podresourcesapi.ContainerDevices{},
			containerCpus:    []int64{},
			containerMemory:  []*podresourcesapi.ContainerMemory{},
			dynamicResources: []*podresourcesapi.DynamicResource{},

			expectedResponse: &podresourcesapi.GetPodResourcesResponse{},
		},
		{
			desc:             "pod without devices",
			err:              nil,
			exist:            true,
			pod:              pod,
			devices:          []*podresourcesapi.ContainerDevices{},
			containerCpus:    []int64{},
			containerMemory:  []*podresourcesapi.ContainerMemory{},
			dynamicResources: []*podresourcesapi.DynamicResource{},
			expectedResponse: &podresourcesapi.GetPodResourcesResponse{
				PodResources: &podresourcesapi.PodResources{
					Name:      podName,
					Namespace: podNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:             containerName,
							Devices:          []*podresourcesapi.ContainerDevices{},
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
					},
				},
			},
		},
		{
			desc:             "pod with devices",
			err:              nil,
			exist:            true,
			pod:              pod,
			devices:          devs,
			containerCpus:    cpus,
			containerMemory:  memory,
			dynamicResources: draDevs,
			expectedResponse: &podresourcesapi.GetPodResourcesResponse{
				PodResources: &podresourcesapi.PodResources{
					Name:      podName,
					Namespace: podNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:             containerName,
							Devices:          devs,
							CpuIds:           cpus,
							Memory:           memory,
							DynamicResources: draDevs,
						},
					},
				},
			},
		},
		{
			desc:                     "Topology Manager scope: pod, pod-level resources, exclusive container",
			err:                      nil,
			exist:                    true,
			pod:                      podWithPodLevelResources,
			devices:                  []*podresourcesapi.ContainerDevices{},
			containerCpus:            cpus,
			containerMemory:          memory,
			podCpus:                  podCpus,
			podMemory:                []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
			dynamicResources:         []*podresourcesapi.DynamicResource{},
			podLevelResourcesEnabled: true,
			expectedResponse: &podresourcesapi.GetPodResourcesResponse{
				PodResources: &podresourcesapi.PodResources{
					Name:      podName,
					Namespace: podNamespace,
					CpuIds:    podCpus,
					Memory:    []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:             containerName,
							Devices:          []*podresourcesapi.ContainerDevices{},
							CpuIds:           cpus,
							Memory:           memory,
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
					},
				},
			},
		},
		{
			desc:                     "Topology Manager scope: pod, pod-level resources, shared pool container",
			err:                      nil,
			exist:                    true,
			pod:                      podWithPodLevelResources,
			devices:                  []*podresourcesapi.ContainerDevices{},
			containerCpus:            []int64{},
			containerMemory:          []*podresourcesapi.ContainerMemory{},
			podCpus:                  podCpus,
			podMemory:                []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
			dynamicResources:         []*podresourcesapi.DynamicResource{},
			podLevelResourcesEnabled: true,
			expectedResponse: &podresourcesapi.GetPodResourcesResponse{
				PodResources: &podresourcesapi.PodResources{
					Name:      podName,
					Namespace: podNamespace,
					CpuIds:    podCpus,
					Memory:    []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:             containerName,
							Devices:          []*podresourcesapi.ContainerDevices{},
							CpuIds:           []int64{},
							Memory:           []*podresourcesapi.ContainerMemory{},
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
					},
				},
			},
		},
		{
			desc:                     "Topology Manager scope: pod, pod-level resources, mix of exclusive and shared pool containers",
			err:                      nil,
			exist:                    true,
			pod:                      podWithMultipleContainers,
			devices:                  []*podresourcesapi.ContainerDevices{},
			containerCpus:            cpus,
			containerMemory:          memory,
			podCpus:                  podCpus,
			podMemory:                []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
			dynamicResources:         []*podresourcesapi.DynamicResource{},
			podLevelResourcesEnabled: true,
			targetContainerName:      "exclusive-container",
			expectedResponse: &podresourcesapi.GetPodResourcesResponse{
				PodResources: &podresourcesapi.PodResources{
					Name:      podName,
					Namespace: podNamespace,
					CpuIds:    podCpus,
					Memory:    []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:             "exclusive-container",
							Devices:          []*podresourcesapi.ContainerDevices{},
							CpuIds:           cpus,
							Memory:           memory,
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
						{
							Name:             "shared-container",
							Devices:          []*podresourcesapi.ContainerDevices{},
							CpuIds:           []int64{},
							Memory:           []*podresourcesapi.ContainerMemory{},
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
					},
				},
			},
		},
		{
			desc:                     "Topology Manager scope: container, pod-level resources, exclusive container",
			err:                      nil,
			exist:                    true,
			pod:                      podWithPodLevelResources,
			devices:                  []*podresourcesapi.ContainerDevices{},
			containerCpus:            cpus,
			containerMemory:          memory,
			podCpus:                  []int64{},
			podMemory:                []*podresourcesapi.ContainerMemory{},
			dynamicResources:         []*podresourcesapi.DynamicResource{},
			podLevelResourcesEnabled: true,
			expectedResponse: &podresourcesapi.GetPodResourcesResponse{
				PodResources: &podresourcesapi.PodResources{
					Name:      podName,
					Namespace: podNamespace,
					CpuIds:    []int64{},
					Memory:    []*podresourcesapi.ContainerMemory{},
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:             containerName,
							Devices:          []*podresourcesapi.ContainerDevices{},
							CpuIds:           cpus,
							Memory:           memory,
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
					},
				},
			},
		},
		{
			desc:                     "Topology Manager scope: container, pod-level resources, mix of exclusive and shared pool containers",
			err:                      nil,
			exist:                    true,
			pod:                      podWithMultipleContainers,
			devices:                  []*podresourcesapi.ContainerDevices{},
			containerCpus:            cpus,
			containerMemory:          memory,
			podCpus:                  []int64{},
			podMemory:                []*podresourcesapi.ContainerMemory{},
			dynamicResources:         []*podresourcesapi.DynamicResource{},
			podLevelResourcesEnabled: true,
			targetContainerName:      "exclusive-container",
			expectedResponse: &podresourcesapi.GetPodResourcesResponse{
				PodResources: &podresourcesapi.PodResources{
					Name:      podName,
					Namespace: podNamespace,
					CpuIds:    []int64{},
					Memory:    []*podresourcesapi.ContainerMemory{},
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:             "exclusive-container",
							Devices:          []*podresourcesapi.ContainerDevices{},
							CpuIds:           cpus,
							Memory:           memory,
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
						{
							Name:             "shared-container",
							Devices:          []*podresourcesapi.ContainerDevices{},
							CpuIds:           []int64{},
							Memory:           []*podresourcesapi.ContainerMemory{},
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.podLevelResourcesEnabled {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResources, tc.podLevelResourcesEnabled)
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResourceManagers, tc.podLevelResourcesEnabled)
			}

			mockDevicesProvider := podresourcetest.NewMockDevicesProvider(t)
			mockPodsProvider := podresourcetest.NewMockPodsProvider(t)
			mockCPUsProvider := podresourcetest.NewMockCPUsProvider(t)
			mockMemoryProvider := podresourcetest.NewMockMemoryProvider(t)
			mockDynamicResourcesProvider := podresourcetest.NewMockDynamicResourcesProvider(t)

			// Get 按 namespace/name 精确查找 pod，与 List 遍历全部 pod 的路径不同。
			mockPodsProvider.EXPECT().GetPodByName(podNamespace, podName).Return(tc.pod, tc.exist).Maybe()
			// 1. 按用例声明的值精确打桩，只覆盖该用例真正关心的资源类型。
			// 未指定目标容器时默认使用普通容器名。
			targetName := tc.targetContainerName
			if targetName == "" {
				targetName = containerName
			}

			if len(tc.devices) > 0 {
				mockDevicesProvider.EXPECT().GetDevices(string(podUID), targetName).Return(tc.devices).Maybe()
			}
			if len(tc.containerCpus) > 0 {
				mockCPUsProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == targetName })).Return(tc.containerCpus).Maybe()
			}
			if len(tc.containerMemory) > 0 {
				mockMemoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == targetName })).Return(tc.containerMemory).Maybe()
			}
			if tc.podLevelResourcesEnabled {
				mockCPUsProvider.EXPECT().GetPodCPUs(string(podUID)).Return(tc.podCpus).Maybe()
				mockMemoryProvider.EXPECT().GetPodMemory(logger, string(podUID)).Return(tc.podMemory).Maybe()
			}
			if len(tc.dynamicResources) > 0 {
				mockDynamicResourcesProvider.EXPECT().GetDynamicResources(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == targetName })).Return(tc.dynamicResources).Maybe()
			}

			// 2. 兜底打桩：未声明的调用返回空切片；testify 按注册顺序匹配，具体期望优先生效。
			mockCPUsProvider.EXPECT().GetCPUs(mock.Anything, mock.Anything).Return([]int64{}).Maybe()
			mockCPUsProvider.EXPECT().GetPodCPUs(mock.Anything).Return([]int64{}).Maybe()
			mockCPUsProvider.EXPECT().GetAllocatableCPUs().Return([]int64{}).Maybe()
			mockMemoryProvider.EXPECT().GetMemory(logger, mock.Anything, mock.Anything).Return([]*podresourcesapi.ContainerMemory{}).Maybe()
			mockMemoryProvider.EXPECT().GetPodMemory(logger, mock.Anything).Return([]*podresourcesapi.ContainerMemory{}).Maybe()
			mockMemoryProvider.EXPECT().GetAllocatableMemory(logger).Return([]*podresourcesapi.ContainerMemory{}).Maybe()
			mockDevicesProvider.EXPECT().GetDevices(mock.Anything, mock.Anything).Return([]*podresourcesapi.ContainerDevices{}).Maybe()
			mockDevicesProvider.EXPECT().GetAllocatableDevices(logger).Return([]*podresourcesapi.ContainerDevices{}).Maybe()
			mockDevicesProvider.EXPECT().UpdateAllocatedDevices(logger).Return().Maybe()
			mockDynamicResourcesProvider.EXPECT().GetDynamicResources(logger, mock.Anything, mock.Anything).Return([]*podresourcesapi.DynamicResource{}).Maybe()

			// 组装 Provider 集合并构造 v1 服务。
			providers := PodResourcesProviders{
				Pods:             mockPodsProvider,
				Devices:          mockDevicesProvider,
				Cpus:             mockCPUsProvider,
				Memory:           mockMemoryProvider,
				DynamicResources: mockDynamicResourcesProvider,
			}
			server := NewV1PodResourcesServer(tCtx, providers)
			// 请求携带 namespace/name，由服务内部转换为 pod 查找。
			podReq := &podresourcesapi.GetPodResourcesRequest{PodName: podName, PodNamespace: podNamespace}
			resp, err := server.Get(tCtx, podReq)

			// pod 不存在时只比较错误文本；存在时比较完整响应。
			if err != nil {
				if err.Error() != tc.err.Error() {
					t.Errorf("want exit = %v, got %v", tc.err, err)
				}
			} else {
				if err != tc.err {
					t.Errorf("want exit = %v, got %v", tc.err, err)
				} else {
					if tc.expectedResponse.String() != resp.String() {
						t.Fatalf("want: %+v; got: %+v", tc.expectedResponse, resp)
					}
				}
			}
		})
	}

}

// TestGetPodResourcesWithInitContainersV1 验证单 pod 查询时 init container 的返回规则，
// 与 List 路径保持一致。
func TestGetPodResourcesWithInitContainersV1(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	podName := "pod-name"
	podNamespace := "pod-namespace"
	podUID := types.UID("pod-uid")
	initContainerName := "init-container-name"
	containerName := "container-name"
	numaID := int64(1)
	containerRestartPolicyAlways := v1.ContainerRestartPolicyAlways

	devs := []*podresourcesapi.ContainerDevices{
		{
			ResourceName: "resource",
			DeviceIds:    []string{"dev0", "dev1"},
			Topology:     &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
	}

	cpus := []int64{12, 23, 30}
	podCpus := []int64{12, 23, 30, 31, 32}

	memory := []*podresourcesapi.ContainerMemory{
		{
			MemoryType: "memory",
			Size:       1073741824,
			Topology:   &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
		{
			MemoryType: "hugepages-1Gi",
			Size:       1073741824,
			Topology:   &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: numaID}}},
		},
	}

	containers := []v1.Container{
		{
			Name: containerName,
		},
	}

	// 表驱动用例：可重启 init container 会与普通容器一起返回，普通 init container 不返回。
	for _, tc := range []struct {
		desc     string
		pod      *v1.Pod
		mockFunc func(
			*v1.Pod,
			*podresourcetest.MockDevicesProvider,
			*podresourcetest.MockCPUsProvider,
			*podresourcetest.MockMemoryProvider,
			*podresourcetest.MockDynamicResourcesProvider)
		expectedResponse         *podresourcesapi.GetPodResourcesResponse
		podLevelResourcesEnabled bool
	}{
		{
			desc: "pod having an init container AND pod level resources enabled",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					Namespace: podNamespace,
					UID:       podUID,
				},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("5"),
							v1.ResourceMemory: resource.MustParse("5Gi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("5"),
							v1.ResourceMemory: resource.MustParse("5Gi"),
						},
					},
					InitContainers: []v1.Container{
						{
							Name: initContainerName,
						},
					},
					Containers: containers,
				},
			},
			mockFunc: func(
				pod *v1.Pod,
				devicesProvider *podresourcetest.MockDevicesProvider,
				cpusProvider *podresourcetest.MockCPUsProvider,
				memoryProvider *podresourcetest.MockMemoryProvider,
				dynamicResourcesProvider *podresourcetest.MockDynamicResourcesProvider) {
				devicesProvider.EXPECT().UpdateAllocatedDevices(logger).Return().Maybe()
				devicesProvider.EXPECT().GetDevices(string(podUID), containerName).Return([]*podresourcesapi.ContainerDevices{}).Maybe()
				cpusProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(cpus).Maybe()
				memoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(memory).Maybe()
				dynamicResourcesProvider.EXPECT().GetDynamicResources(logger, pod, &pod.Spec.Containers[0]).Return([]*podresourcesapi.DynamicResource{}).Maybe()

				cpusProvider.EXPECT().GetPodCPUs(mock.Anything).Return(podCpus).Maybe()
				memoryProvider.EXPECT().GetPodMemory(logger, mock.Anything).Return([]*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}}).Maybe()
			},
			podLevelResourcesEnabled: true,
			expectedResponse: &podresourcesapi.GetPodResourcesResponse{
				PodResources: &podresourcesapi.PodResources{
					Name:      podName,
					Namespace: podNamespace,
					CpuIds:    podCpus,
					Memory:    []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:             containerName,
							Devices:          []*podresourcesapi.ContainerDevices{},
							CpuIds:           cpus,
							Memory:           memory,
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
					},
				},
			},
		},
		{
			desc: "pod having a restartable init container AND pod level resources enabled",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					Namespace: podNamespace,
					UID:       podUID,
				},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("5"),
							v1.ResourceMemory: resource.MustParse("5Gi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("5"),
							v1.ResourceMemory: resource.MustParse("5Gi"),
						},
					},
					InitContainers: []v1.Container{
						{
							Name:          initContainerName,
							RestartPolicy: &containerRestartPolicyAlways,
						},
					},
					Containers: containers,
				},
			},
			mockFunc: func(
				pod *v1.Pod,
				devicesProvider *podresourcetest.MockDevicesProvider,
				cpusProvider *podresourcetest.MockCPUsProvider,
				memoryProvider *podresourcetest.MockMemoryProvider,
				dynamicResourcesProvider *podresourcetest.MockDynamicResourcesProvider) {
				devicesProvider.EXPECT().UpdateAllocatedDevices(logger).Return().Maybe()

				devicesProvider.EXPECT().GetDevices(string(podUID), initContainerName).Return([]*podresourcesapi.ContainerDevices{}).Maybe()
				cpusProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == initContainerName })).Return(cpus).Maybe()
				memoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == initContainerName })).Return(memory).Maybe()
				dynamicResourcesProvider.EXPECT().GetDynamicResources(logger, pod, &pod.Spec.InitContainers[0]).Return([]*podresourcesapi.DynamicResource{}).Maybe()

				devicesProvider.EXPECT().GetDevices(string(podUID), containerName).Return([]*podresourcesapi.ContainerDevices{}).Maybe()
				cpusProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(cpus).Maybe()
				memoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(memory).Maybe()
				dynamicResourcesProvider.EXPECT().GetDynamicResources(logger, pod, &pod.Spec.Containers[0]).Return([]*podresourcesapi.DynamicResource{}).Maybe()

				cpusProvider.EXPECT().GetPodCPUs(mock.Anything).Return(podCpus).Maybe()
				memoryProvider.EXPECT().GetPodMemory(logger, mock.Anything).Return([]*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}}).Maybe()
			},
			podLevelResourcesEnabled: true,
			expectedResponse: &podresourcesapi.GetPodResourcesResponse{
				PodResources: &podresourcesapi.PodResources{
					Name:      podName,
					Namespace: podNamespace,
					CpuIds:    podCpus,
					Memory:    []*podresourcesapi.ContainerMemory{{MemoryType: "memory", Size: 2147483648, Topology: &podresourcesapi.TopologyInfo{Nodes: []*podresourcesapi.NUMANode{{ID: 1}}}}},
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:             initContainerName,
							Devices:          []*podresourcesapi.ContainerDevices{},
							CpuIds:           cpus,
							Memory:           memory,
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
						{
							Name:             containerName,
							Devices:          []*podresourcesapi.ContainerDevices{},
							CpuIds:           cpus,
							Memory:           memory,
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
					},
				},
			},
		},
		{
			desc: "pod having an init container",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					Namespace: podNamespace,
					UID:       podUID,
				},
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Name: initContainerName,
						},
					},
					Containers: containers,
				},
			},
			mockFunc: func(
				pod *v1.Pod,
				devicesProvider *podresourcetest.MockDevicesProvider,
				cpusProvider *podresourcetest.MockCPUsProvider,
				memoryProvider *podresourcetest.MockMemoryProvider,
				dynamicResourcesProvider *podresourcetest.MockDynamicResourcesProvider) {
				devicesProvider.EXPECT().UpdateAllocatedDevices(logger).Return().Maybe()
				devicesProvider.EXPECT().GetDevices(string(podUID), containerName).Return(devs).Maybe()
				cpusProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(cpus).Maybe()
				memoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(memory).Maybe()
				dynamicResourcesProvider.EXPECT().GetDynamicResources(logger, pod, &pod.Spec.Containers[0]).Return([]*podresourcesapi.DynamicResource{}).Maybe()

			},
			expectedResponse: &podresourcesapi.GetPodResourcesResponse{
				PodResources: &podresourcesapi.PodResources{
					Name:      podName,
					Namespace: podNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:             containerName,
							Devices:          devs,
							CpuIds:           cpus,
							Memory:           memory,
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
					},
				},
			},
		},
		{
			desc: "pod having a restartable init container",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					Namespace: podNamespace,
					UID:       podUID,
				},
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Name:          initContainerName,
							RestartPolicy: &containerRestartPolicyAlways,
						},
					},
					Containers: containers,
				},
			},
			mockFunc: func(
				pod *v1.Pod,
				devicesProvider *podresourcetest.MockDevicesProvider,
				cpusProvider *podresourcetest.MockCPUsProvider,
				memoryProvider *podresourcetest.MockMemoryProvider,
				dynamicResourcesProvider *podresourcetest.MockDynamicResourcesProvider) {
				devicesProvider.EXPECT().UpdateAllocatedDevices(logger).Return().Maybe()

				devicesProvider.EXPECT().GetDevices(string(podUID), initContainerName).Return(devs).Maybe()
				cpusProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == initContainerName })).Return(cpus).Maybe()
				memoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == initContainerName })).Return(memory).Maybe()
				dynamicResourcesProvider.EXPECT().GetDynamicResources(logger, pod, &pod.Spec.InitContainers[0]).Return([]*podresourcesapi.DynamicResource{}).Maybe()

				devicesProvider.EXPECT().GetDevices(string(podUID), containerName).Return(devs).Maybe()
				cpusProvider.EXPECT().GetCPUs(mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(cpus).Maybe()
				memoryProvider.EXPECT().GetMemory(logger, mock.MatchedBy(func(p *v1.Pod) bool { return string(p.UID) == string(podUID) }), mock.MatchedBy(func(c *v1.Container) bool { return c.Name == containerName })).Return(memory).Maybe()
				dynamicResourcesProvider.EXPECT().GetDynamicResources(logger, pod, &pod.Spec.Containers[0]).Return([]*podresourcesapi.DynamicResource{}).Maybe()
			},
			expectedResponse: &podresourcesapi.GetPodResourcesResponse{
				PodResources: &podresourcesapi.PodResources{
					Name:      podName,
					Namespace: podNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:             initContainerName,
							Devices:          devs,
							CpuIds:           cpus,
							Memory:           memory,
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
						{
							Name:             containerName,
							Devices:          devs,
							CpuIds:           cpus,
							Memory:           memory,
							DynamicResources: []*podresourcesapi.DynamicResource{},
						},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.podLevelResourcesEnabled {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResources, tc.podLevelResourcesEnabled)
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResourceManagers, tc.podLevelResourcesEnabled)
			}

			mockDevicesProvider := podresourcetest.NewMockDevicesProvider(t)
			mockPodsProvider := podresourcetest.NewMockPodsProvider(t)
			mockCPUsProvider := podresourcetest.NewMockCPUsProvider(t)
			mockMemoryProvider := podresourcetest.NewMockMemoryProvider(t)
			mockDynamicResourcesProvider := podresourcetest.NewMockDynamicResourcesProvider(t)

			// 用例保证 pod 存在，因此固定返回 exist=true。
			mockPodsProvider.EXPECT().GetPodByName(podNamespace, podName).Return(tc.pod, true).Maybe()
			// 先注册用例期望、后注册兜底，testify 按注册顺序匹配，具体期望优先生效。
			tc.mockFunc(tc.pod, mockDevicesProvider, mockCPUsProvider, mockMemoryProvider, mockDynamicResourcesProvider)
			mockCPUsProvider.EXPECT().GetPodCPUs(mock.Anything).Return([]int64{}).Maybe()
			mockMemoryProvider.EXPECT().GetPodMemory(logger, mock.Anything).Return([]*podresourcesapi.ContainerMemory{}).Maybe()

			providers := PodResourcesProviders{
				Pods:             mockPodsProvider,
				Devices:          mockDevicesProvider,
				Cpus:             mockCPUsProvider,
				Memory:           mockMemoryProvider,
				DynamicResources: mockDynamicResourcesProvider,
			}
			server := NewV1PodResourcesServer(tCtx, providers)
			podReq := &podresourcesapi.GetPodResourcesRequest{PodName: podName, PodNamespace: podNamespace}
			resp, err := server.Get(tCtx, podReq)
			if err != nil {
				t.Errorf("want err = %v, got %q", nil, err)
			}
			if tc.expectedResponse.String() != resp.String() {
				t.Fatalf("want: %+v; got: %+v", tc.expectedResponse, resp)
			}
		})
	}
}
