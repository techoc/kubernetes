/*
Copyright 2020 The Kubernetes Authors.

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
	"context"
	"fmt"
	"os"
	"reflect"
	goruntime "runtime"
	"strings"
	"testing"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// 测试用的 hugepages 资源名，需与 kubelet 配置中的写法保持一致。
const (
	hugepages2M = "hugepages-2Mi"
	hugepages1G = "hugepages-1Gi"
)

// policyTypeMock 是注入可控错误的假 policy，用于验证 manager 的错误传播路径。
const policyTypeMock policyType = "mock"

// testMemoryManager 汇总各表驱动用例的输入与期望值。
// 复用同一结构体让用例只需填充相关字段，避免为每个场景定义独立类型。
type testMemoryManager struct {
	description                string
	machineInfo                cadvisorapi.MachineInfo
	assignments                state.ContainerMemoryAssignments
	expectedAssignments        state.ContainerMemoryAssignments
	machineState               state.NUMANodeMap
	expectedMachineState       state.NUMANodeMap
	expectedError              error
	expectedAllocateError      error
	expectedAddContainerError  error
	updateError                error
	removeContainerID          string
	nodeAllocatableReservation v1.ResourceList
	policyName                 policyType
	affinity                   topologymanager.Store
	systemReservedMemory       []kubeletconfig.MemoryReservation
	expectedHints              map[string][]topologymanager.TopologyHint
	expectedReserved           systemReservedMemory
	reserved                   systemReservedMemory
	podAllocate                *v1.Pod
	firstPod                   *v1.Pod
	activePods                 []*v1.Pod
}

// returnPolicyByName 按用例的 policyName 构造对应 policy。
// 用例只需声明策略名，省去重复的构造样板代码。
func returnPolicyByName(logger klog.Logger, testCase testMemoryManager) Policy {
	switch testCase.policyName {
	case policyTypeMock:
		return &mockPolicy{
			err: fmt.Errorf("fake reg error"),
		}
	case PolicyTypeStatic:
		policy, _ := NewPolicyStatic(logger, &testCase.machineInfo, testCase.reserved, topologymanager.NewFakeManager(logger))
		return policy
	case policyTypeNone:
		return NewPolicyNone(logger)
	}
	return nil
}

// mockPolicy 的所有方法都返回预设错误，用于模拟 policy 失败。
type mockPolicy struct {
	err error
}

// Name 返回 mock policy 的名称，便于断言 manager 选中的策略。
func (p *mockPolicy) Name() string {
	return string(policyTypeMock)
}

// Start 返回预设错误，模拟 policy 初始化失败。
func (p *mockPolicy) Start(klog.Logger, state.State) error {
	return p.err
}

// Allocate 返回预设错误，模拟内存分配失败。
func (p *mockPolicy) Allocate(context.Context, state.State, *v1.Pod, *v1.Container, lifecycle.Operation) error {
	return p.err
}

// RemoveContainer 为空实现，mock 场景无需真正释放状态。
func (p *mockPolicy) RemoveContainer(klog.Logger, state.State, string, string) {
}

// GetTopologyHints 返回 nil，mock 场景不提供 NUMA hint。
func (p *mockPolicy) GetTopologyHints(klog.Logger, state.State, *v1.Pod, *v1.Container, lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	return nil
}

// GetPodTopologyHints 返回 nil，mock 场景不提供 pod 级 hint。
func (p *mockPolicy) GetPodTopologyHints(klog.Logger, state.State, *v1.Pod, lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	return nil
}

// AllocatePod 返回预设错误，模拟 pod 级分配失败。
func (p *mockPolicy) AllocatePod(_ klog.Logger, s state.State, pod *v1.Pod, _ lifecycle.Operation) error {
	return p.err
}

// GetAllocatableMemory 返回空切片：mock 场景不参与可分配内存计算。
func (p *mockPolicy) GetAllocatableMemory(state.State) []state.Block {
	return []state.Block{}
}

// mockRuntimeService 只实现 manager 用到的 UpdateContainerResources。
type mockRuntimeService struct {
	err error
}

// UpdateContainerResources 返回预设错误，用于验证 manager 对 CRI 调用失败的处理。
func (rt mockRuntimeService) UpdateContainerResources(_ context.Context, id string, resources *runtimeapi.ContainerResources) error {
	return rt.err
}

// mockPodStatusProvider 固定返回同一 pod 状态，避免测试依赖真实的 pod 缓存。
type mockPodStatusProvider struct {
	podStatus v1.PodStatus
	found     bool
}

// GetPodStatus 忽略 uid，始终返回预置状态与 found 标志。
func (psp mockPodStatusProvider) GetPodStatus(uid types.UID) (v1.PodStatus, bool) {
	return psp.podStatus, psp.found
}

// getPod 构造仅含单个 container 的最小 pod，便于按 UID/名称定位。
func getPod(podUID string, containerName string, requirements *v1.ResourceRequirements) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(podUID),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:      containerName,
					Resources: *requirements,
				},
			},
		},
	}
}

// getPodWithPodLevelResources 在 getPod 基础上补充 pod 级 resources，
// 用于覆盖 pod-level resources 特性下的内存分配。
func getPodWithPodLevelResources(podUID string, podRequirements *v1.ResourceRequirements, containerName string, containerRequirements *v1.ResourceRequirements) *v1.Pod {
	pod := getPod(podUID, containerName, containerRequirements)
	pod.Spec.Resources = podRequirements

	return pod
}

// getPodWithInitContainers 构造同时含 init container 与普通 container 的 pod，
// 用于验证 init container 占用的内存在 app container 启动后被清理。
func getPodWithInitContainers(podUID string, containers []v1.Container, initContainers []v1.Container) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID(podUID),
		},
		Spec: v1.PodSpec{
			InitContainers: initContainers,
			Containers:     containers,
		},
	}
}

// TestValidateReservedMemory 校验预留内存与 Node Allocatable 的一致性检查：
// 各 NUMA 节点的预留量之和必须等于 nodeAllocatableReservation 中对应资源的总量。
func TestValidateReservedMemory(t *testing.T) {
	machineInfo := &cadvisorapi.MachineInfo{
		Topology: []cadvisorapi.Node{
			{Id: 0},
			{Id: 1},
		},
	}
	// msgNotEqual 复用错误文案，避免各用例重复硬编码格式串。
	const msgNotEqual = "the total amount %q of type %q is not equal to the value %q determined by Node Allocatable feature"
	testCases := []struct {
		description                string
		nodeAllocatableReservation v1.ResourceList
		machineInfo                *cadvisorapi.MachineInfo
		systemReservedMemory       []kubeletconfig.MemoryReservation
		expectedError              string
	}{
		{
			"Node Allocatable not set, reserved not set",
			v1.ResourceList{},
			machineInfo,
			[]kubeletconfig.MemoryReservation{},
			"",
		},
		{
			"Node Allocatable set to zero, reserved set to zero",
			v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(0, resource.DecimalSI)},
			machineInfo,
			[]kubeletconfig.MemoryReservation{
				{
					NumaNode: 0,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(0, resource.DecimalSI),
					},
				},
			},
			"",
		},
		{
			"Node Allocatable not set (equal zero), reserved set",
			v1.ResourceList{},
			machineInfo,
			[]kubeletconfig.MemoryReservation{
				{
					NumaNode: 0,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI),
					},
				},
			},
			fmt.Sprintf(msgNotEqual, "12", v1.ResourceMemory, "0"),
		},
		{
			"Node Allocatable set, reserved not set",
			v1.ResourceList{hugepages2M: *resource.NewQuantity(5, resource.DecimalSI)},
			machineInfo,
			[]kubeletconfig.MemoryReservation{},
			fmt.Sprintf(msgNotEqual, "0", hugepages2M, "5"),
		},
		{
			"Reserved not equal to Node Allocatable",
			v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(5, resource.DecimalSI)},
			machineInfo,
			[]kubeletconfig.MemoryReservation{
				{
					NumaNode: 0,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI),
					},
				},
			},
			fmt.Sprintf(msgNotEqual, "12", v1.ResourceMemory, "5"),
		},
		{
			"Reserved contains the NUMA node that does not exist under the machine",
			v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(17, resource.DecimalSI)},
			machineInfo,
			[]kubeletconfig.MemoryReservation{
				{
					NumaNode: 0,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI),
					},
				},
				{
					NumaNode: 2,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(5, resource.DecimalSI),
					},
				},
			},
			"the reserved memory configuration references a NUMA node 2 that does not exist on this machine",
		},
		{
			"Reserved total equal to Node Allocatable",
			v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(17, resource.DecimalSI),
				hugepages2M: *resource.NewQuantity(77, resource.DecimalSI),
				hugepages1G: *resource.NewQuantity(13, resource.DecimalSI)},
			machineInfo,
			[]kubeletconfig.MemoryReservation{
				{
					NumaNode: 0,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI),
						hugepages2M:       *resource.NewQuantity(70, resource.DecimalSI),
						hugepages1G:       *resource.NewQuantity(13, resource.DecimalSI),
					},
				},
				{
					NumaNode: 1,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(5, resource.DecimalSI),
						hugepages2M:       *resource.NewQuantity(7, resource.DecimalSI),
					},
				},
			},
			"",
		},
		{
			"Reserved total hugapages-2M not equal to Node Allocatable",
			v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(17, resource.DecimalSI),
				hugepages2M: *resource.NewQuantity(14, resource.DecimalSI),
				hugepages1G: *resource.NewQuantity(13, resource.DecimalSI)},
			machineInfo,
			[]kubeletconfig.MemoryReservation{
				{
					NumaNode: 0,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI),
						hugepages2M:       *resource.NewQuantity(70, resource.DecimalSI),
						hugepages1G:       *resource.NewQuantity(13, resource.DecimalSI),
					},
				},
				{
					NumaNode: 1,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(5, resource.DecimalSI),
						hugepages2M:       *resource.NewQuantity(7, resource.DecimalSI),
					},
				},
			},

			fmt.Sprintf(msgNotEqual, "77", hugepages2M, "14"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := validateReservedMemory(tc.machineInfo, tc.nodeAllocatableReservation, tc.systemReservedMemory)
			if strings.TrimSpace(tc.expectedError) != "" {
				assert.Error(t, err)
				assert.Equal(t, tc.expectedError, err.Error())
			}
		})
	}
}

// TestConvertPreReserved 验证将 kubelet 配置的 MemoryReservation 转换为
// 以 NUMA 节点为键的内部 map，未配置的节点应得到空 map 而非 nil。
func TestConvertPreReserved(t *testing.T) {
	machineInfo := cadvisorapi.MachineInfo{
		Topology: []cadvisorapi.Node{
			{Id: 0},
			{Id: 1},
		},
	}

	testCases := []struct {
		description            string
		systemReserved         []kubeletconfig.MemoryReservation
		systemReservedExpected systemReservedMemory
		expectedError          string
	}{
		{
			"Empty",
			[]kubeletconfig.MemoryReservation{},
			systemReservedMemory{
				0: map[v1.ResourceName]uint64{},
				1: map[v1.ResourceName]uint64{},
			},
			"",
		},
		{
			"Single NUMA node is reserved",
			[]kubeletconfig.MemoryReservation{
				{
					NumaNode: 0,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI),
						hugepages2M:       *resource.NewQuantity(70, resource.DecimalSI),
						hugepages1G:       *resource.NewQuantity(13, resource.DecimalSI),
					},
				},
			},
			systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 12,
					hugepages2M:       70,
					hugepages1G:       13,
				},
				1: map[v1.ResourceName]uint64{},
			},
			"",
		},
		{
			"Both NUMA nodes are reserved",
			[]kubeletconfig.MemoryReservation{
				{
					NumaNode: 0,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(12, resource.DecimalSI),
						hugepages2M:       *resource.NewQuantity(70, resource.DecimalSI),
						hugepages1G:       *resource.NewQuantity(13, resource.DecimalSI),
					},
				},
				{
					NumaNode: 1,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(5, resource.DecimalSI),
						hugepages2M:       *resource.NewQuantity(7, resource.DecimalSI),
					},
				},
			},
			systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 12,
					hugepages2M:       70,
					hugepages1G:       13,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 5,
					hugepages2M:       7,
				},
			},
			"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// convertReserved 会按 machineInfo 的 NUMA 节点初始化结果，空配置也应得到 0/1 两个键。
			reserved, _ := convertReserved(&machineInfo, tc.systemReserved)
			if !reflect.DeepEqual(reserved, tc.systemReservedExpected) {
				t.Errorf("got %v, expected %v", reserved, tc.systemReservedExpected)
			}
		})
	}
}

// TestGetSystemReservedMemory 验证 getSystemReservedMemory：预留量与 Node Allocatable
// 不一致时返回错误，一致时返回按 NUMA 节点聚合的预留量。
func TestGetSystemReservedMemory(t *testing.T) {
	machineInfo := returnMachineInfo()
	testCases := []testMemoryManager{
		{
			description:                "Should return empty map when reservation is not done",
			nodeAllocatableReservation: v1.ResourceList{},
			systemReservedMemory:       []kubeletconfig.MemoryReservation{},
			expectedReserved: systemReservedMemory{
				0: {},
				1: {},
			},
			expectedError: nil,
			machineInfo:   machineInfo,
		},
		{
			description:                "Should return error when Allocatable reservation is not equal to the reserved memory",
			nodeAllocatableReservation: v1.ResourceList{},
			systemReservedMemory: []kubeletconfig.MemoryReservation{
				{
					NumaNode: 0,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI),
					},
				},
			},
			expectedReserved: nil,
			expectedError:    fmt.Errorf("the total amount \"1Gi\" of type \"memory\" is not equal to the value \"0\" determined by Node Allocatable feature"),
			machineInfo:      machineInfo,
		},
		{
			description:                "Reserved should be equal to systemReservedMemory",
			nodeAllocatableReservation: v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI)},
			systemReservedMemory: []kubeletconfig.MemoryReservation{
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
			},
			expectedReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
			},
			expectedError: nil,
			machineInfo:   machineInfo,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			res, err := getSystemReservedMemory(&testCase.machineInfo, testCase.nodeAllocatableReservation, testCase.systemReservedMemory)

			if !reflect.DeepEqual(res, testCase.expectedReserved) {
				t.Errorf("Memory Manager getReservedMemory() error, expected reserved %+v, but got: %+v",
					testCase.expectedReserved, res)
			}
			if !reflect.DeepEqual(err, testCase.expectedError) {
				t.Errorf("Memory Manager getReservedMemory() error, expected error %v, but got: %v",
					testCase.expectedError, err)
			}

		})
	}
}

// TestRemoveStaleState 验证 kubelet 重启后清理状态文件中已不存在的容器分配：
// 仍属于 activePods 的分配保留，其余分配被释放并把内存归还 NUMA 节点。
func TestRemoveStaleState(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	machineInfo := returnMachineInfo()
	testCases := []testMemoryManager{
		{
			description: "Should fail - policy returns an error",
			policyName:  policyTypeMock,
			machineInfo: machineInfo,
			reserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
			},
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
		},
		{
			description: "Stale state successfully removed, without multi NUMA assignments",
			policyName:  PolicyTypeStatic,
			machineInfo: machineInfo,
			reserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
			},
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
		},
		{
			description: "Stale state successfully removed, with multi NUMA assignments",
			policyName:  PolicyTypeStatic,
			machineInfo: machineInfo,
			reserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
			},
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         12 * gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0, 1},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           0 * gb,
							Reserved:       9 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{0, 1},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           5 * gb,
							Reserved:       4 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			mgr := &manager{
				policy:       returnPolicyByName(logger, testCase),
				state:        state.NewMemoryState(logger),
				containerMap: containermap.NewContainerMap(),
				containerRuntime: mockRuntimeService{
					err: nil,
				},
				activePods:        func() []*v1.Pod { return nil },
				podStatusProvider: mockPodStatusProvider{},
			}
			// sourcesReady 桩让 removeStaleState 认为 pod 源已就绪，否则它会提前返回、不做清理。
			mgr.sourcesReady = &sourcesReadyStub{}
			mgr.state.SetMemoryAssignments(testCase.assignments)
			mgr.state.SetMachineState(testCase.machineState)

			mgr.removeStaleState(logger)

			if !areContainerMemoryAssignmentsEqual(t, mgr.state.GetMemoryAssignments(), testCase.expectedAssignments) {
				t.Errorf("Memory Manager removeStaleState() error, expected assignments %v, but got: %v",
					testCase.expectedAssignments, mgr.state.GetMemoryAssignments())
			}
			if !areMachineStatesEqual(logger, mgr.state.GetMachineState(), testCase.expectedMachineState) {
				t.Fatalf("The actual machine state: %v is different from the expected one: %v", mgr.state.GetMachineState(), testCase.expectedMachineState)
			}
		})

	}
}

// TestAddContainer 覆盖 Allocate + AddContainer 两条路径：分配成功时机器状态应扣减，
// policy 报错时状态不变，需求超过单 NUMA 容量时应跨节点分配。
func TestAddContainer(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	machineInfo := returnMachineInfo()
	reserved := systemReservedMemory{
		0: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
		1: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
	}
	pod := getPod("fakePod1", "fakeContainer1", requirementsGuaranteed)
	testCases := []testMemoryManager{
		{
			description: "Correct allocation and adding container on NUMA 0",
			policyName:  PolicyTypeStatic,
			machineInfo: machineInfo,
			reserved:    reserved,
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           8 * gb,
							Reserved:       1 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedAllocateError:     nil,
			expectedAddContainerError: nil,
			updateError:               nil,
			podAllocate:               pod,
			assignments:               state.ContainerMemoryAssignments{},
			activePods:                nil,
		},
		{
			description:               "Shouldn't return any error when policy is set as None",
			updateError:               nil,
			policyName:                policyTypeNone,
			machineInfo:               machineInfo,
			reserved:                  reserved,
			machineState:              state.NUMANodeMap{},
			expectedMachineState:      state.NUMANodeMap{},
			expectedAllocateError:     nil,
			expectedAddContainerError: nil,
			podAllocate:               pod,
			assignments:               state.ContainerMemoryAssignments{},
			activePods:                nil,
		},
		{
			description: "Allocation should fail if policy returns an error",
			updateError: nil,
			policyName:  policyTypeMock,
			machineInfo: machineInfo,
			reserved:    reserved,
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedAllocateError:     fmt.Errorf("fake reg error"),
			expectedAddContainerError: nil,
			podAllocate:               pod,
			assignments:               state.ContainerMemoryAssignments{},
			activePods:                nil,
		},
		{
			description: "Correct allocation of container requiring amount of memory higher than capacity of one NUMA node",
			policyName:  PolicyTypeStatic,
			machineInfo: machineInfo,
			reserved:    reserved,
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           0 * gb,
							Reserved:       9 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           1 * gb,
							Reserved:       4 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           6 * gb,
							Reserved:       3 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedAllocateError:     nil,
			expectedAddContainerError: nil,
			podAllocate: getPod("fakePod1", "fakeContainer1", &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1000Mi"),
					v1.ResourceMemory: resource.MustParse("12Gi"),
					hugepages1Gi:      resource.MustParse("4Gi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1000Mi"),
					v1.ResourceMemory: resource.MustParse("12Gi"),
					hugepages1Gi:      resource.MustParse("4Gi"),
				},
			}),
			assignments: state.ContainerMemoryAssignments{},
			activePods:  nil,
		},
		{
			description: "Should fail if try to allocate container requiring amount of memory higher than capacity of one NUMA node but a small pod is already allocated",
			policyName:  PolicyTypeStatic,
			machineInfo: machineInfo,
			firstPod:    pod,
			reserved:    reserved,
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           8 * gb,
							Reserved:       1 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           8 * gb,
							Reserved:       1 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedAllocateError:     fmt.Errorf("[memorymanager] failed to get the default NUMA affinity, no NUMA nodes with enough memory is available"),
			expectedAddContainerError: nil,
			podAllocate: getPod("fakePod2", "fakeContainer2", &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1000Mi"),
					v1.ResourceMemory: resource.MustParse("12Gi"),
					hugepages1Gi:      resource.MustParse("4Gi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1000Mi"),
					v1.ResourceMemory: resource.MustParse("12Gi"),
					hugepages1Gi:      resource.MustParse("4Gi"),
				},
			}),
			activePods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						UID: types.UID("fakePod1"),
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:      "fakeContainer1",
								Resources: *requirementsGuaranteed,
							},
						},
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			// activePods 由用例注入，静态策略据此判断哪些已分配内存可以回收。
			mgr := &manager{
				policy:       returnPolicyByName(logger, testCase),
				state:        state.NewMemoryState(logger),
				containerMap: containermap.NewContainerMap(),
				containerRuntime: mockRuntimeService{
					err: testCase.updateError,
				},
				activePods:        func() []*v1.Pod { return testCase.activePods },
				podStatusProvider: mockPodStatusProvider{},
			}
			mgr.sourcesReady = &sourcesReadyStub{}
			mgr.state.SetMachineState(testCase.machineState)
			mgr.state.SetMemoryAssignments(testCase.assignments)
			// 预先登记已有容器，模拟它正在运行并占用着机器状态中的内存。
			if testCase.firstPod != nil {
				mgr.containerMap.Add(testCase.firstPod.Name, testCase.firstPod.Spec.Containers[0].Name, "fakeID0")
			}
			pod := testCase.podAllocate
			container := &pod.Spec.Containers[0]
			// Allocate 只计算并预留内存，不写入 containerMap，故需随后单独调用 AddContainer。
			err := mgr.Allocate(ctx, pod, container, lifecycle.AddOperation)
			if !reflect.DeepEqual(err, testCase.expectedAllocateError) {
				t.Errorf("Memory Manager Allocate() error (%v), expected error: %v, but got: %v",
					testCase.description, testCase.expectedAllocateError, err)
			}
			mgr.AddContainer(logger, pod, container, "fakeID")
			_, _, err = mgr.containerMap.GetContainerRef("fakeID")
			if !reflect.DeepEqual(err, testCase.expectedAddContainerError) {
				t.Errorf("Memory Manager AddContainer() error (%v), expected error: %v, but got: %v",
					testCase.description, testCase.expectedAddContainerError, err)
			}

			if !areMachineStatesEqual(logger, mgr.state.GetMachineState(), testCase.expectedMachineState) {
				t.Errorf("[test] %+v", mgr.state.GetMemoryAssignments())
				t.Fatalf("The actual machine state: %v is different from the expected one: %v", mgr.state.GetMachineState(), testCase.expectedMachineState)
			}

		})
	}
}

// TestRemoveContainer 验证 RemoveContainer 释放分配并归还 NUMA 节点内存；
// 容器不在 containerMap 中时应静默跳过，避免对未知容器报错。
func TestRemoveContainer(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	machineInfo := returnMachineInfo()
	reserved := systemReservedMemory{
		0: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
		1: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
	}
	testCases := []testMemoryManager{
		{
			description:       "Correct removing of a container",
			removeContainerID: "fakeID2",
			policyName:        PolicyTypeStatic,
			machineInfo:       machineInfo,
			reserved:          reserved,
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           8 * gb,
							Reserved:       1 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			description:       "Correct removing of a multi NUMA container",
			removeContainerID: "fakeID2",
			policyName:        PolicyTypeStatic,
			machineInfo:       machineInfo,
			reserved:          reserved,
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         12 * gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0, 1},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           0 * gb,
							Reserved:       9 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{0, 1},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           5 * gb,
							Reserved:       4 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           4 * gb,
							Reserved:       1 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           8 * gb,
							Reserved:       1 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedError: nil,
		},
		{
			description:       "Should do nothing if container is not in containerMap",
			removeContainerID: "fakeID3",
			policyName:        PolicyTypeStatic,
			machineInfo:       machineInfo,
			reserved:          reserved,
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedError: nil,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			// 预置两个容器映射，只有 removeContainerID 命中 fakeID2 时才会真正释放内存。
			iniContainerMap := containermap.NewContainerMap()
			iniContainerMap.Add("fakePod1", "fakeContainer1", "fakeID1")
			iniContainerMap.Add("fakePod1", "fakeContainer2", "fakeID2")
			mgr := &manager{
				policy:       returnPolicyByName(logger, testCase),
				state:        state.NewMemoryState(logger),
				containerMap: iniContainerMap,
				containerRuntime: mockRuntimeService{
					err: testCase.expectedError,
				},
				activePods:        func() []*v1.Pod { return nil },
				podStatusProvider: mockPodStatusProvider{},
			}
			mgr.sourcesReady = &sourcesReadyStub{}
			mgr.state.SetMemoryAssignments(testCase.assignments)
			mgr.state.SetMachineState(testCase.machineState)

			err := mgr.RemoveContainer(logger, testCase.removeContainerID)
			if !reflect.DeepEqual(err, testCase.expectedError) {
				t.Errorf("Memory Manager RemoveContainer() error (%v), expected error: %v, but got: %v",
					testCase.description, testCase.expectedError, err)
			}

			if !areContainerMemoryAssignmentsEqual(t, mgr.state.GetMemoryAssignments(), testCase.expectedAssignments) {
				t.Fatalf("Memory Manager RemoveContainer() inconsistent assignment, expected: %+v, but got: %+v, start %+v",
					testCase.expectedAssignments, mgr.state.GetMemoryAssignments(), testCase.expectedAssignments)
			}

			if !areMachineStatesEqual(logger, mgr.state.GetMachineState(), testCase.expectedMachineState) {
				t.Errorf("[test] %+v", mgr.state.GetMemoryAssignments())
				t.Errorf("[test] %+v, %+v", mgr.state.GetMachineState()[0].MemoryMap["memory"], mgr.state.GetMachineState()[1].MemoryMap["memory"])
				t.Fatalf("The actual machine state: %v is different from the expected one: %v", mgr.state.GetMachineState(), testCase.expectedMachineState)
			}
		})
	}
}

// getPolicyNameForOs 返回当前平台可用的 policy：Windows 没有 static policy，只能用 best-effort。
func getPolicyNameForOs() policyType {
	if goruntime.GOOS == "windows" {
		return policyTypeBestEffort
	}
	return PolicyTypeStatic
}

// getAffinityForOs 返回与 getPolicyNameForOs 所选 policy 匹配的 affinity Store。
// Windows 上 BestEffort policy 需要 AuthoritativeStore，普通 fake manager 不够用。
func getAffinityForOs(logger klog.Logger) topologymanager.Store {
	if goruntime.GOOS == "windows" {
		return &fakeBestEffortAffinity{}
	}
	return topologymanager.NewFakeManager(logger)
}

// TestNewManager 验证 NewManager 的构造逻辑：policy 选择、systemReserved 校验，
// 以及静态策略下预留内存的转换结果。
func TestNewManager(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	machineInfo := returnMachineInfo()
	expectedReserved := systemReservedMemory{
		0: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
		1: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
	}
	testCases := []testMemoryManager{
		{
			description:                "Successful creation of Memory Manager instance",
			policyName:                 getPolicyNameForOs(),
			machineInfo:                machineInfo,
			nodeAllocatableReservation: v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI)},
			systemReservedMemory: []kubeletconfig.MemoryReservation{
				{
					NumaNode: 0,
					Limits:   v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
				},
				{
					NumaNode: 1,
					Limits:   v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI)},
				},
			},
			affinity:         getAffinityForOs(logger),
			expectedError:    nil,
			expectedReserved: expectedReserved,
		},
		{
			description:                "Should return an error when systemReservedMemory (configured with kubelet flag) does not comply with Node Allocatable feature values",
			policyName:                 getPolicyNameForOs(),
			machineInfo:                machineInfo,
			nodeAllocatableReservation: v1.ResourceList{v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI)},
			systemReservedMemory: []kubeletconfig.MemoryReservation{
				{
					NumaNode: 0,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(gb, resource.BinarySI),
					},
				},
				{
					NumaNode: 1,
					Limits: v1.ResourceList{
						v1.ResourceMemory: *resource.NewQuantity(2*gb, resource.BinarySI),
					},
				},
			},
			affinity:         getAffinityForOs(logger),
			expectedError:    fmt.Errorf("the total amount \"3Gi\" of type %q is not equal to the value \"2Gi\" determined by Node Allocatable feature", v1.ResourceMemory),
			expectedReserved: expectedReserved,
		},
		{
			description:                "Should return an error when memory reserved for system is empty (systemReservedMemory)",
			policyName:                 getPolicyNameForOs(),
			machineInfo:                machineInfo,
			nodeAllocatableReservation: v1.ResourceList{},
			systemReservedMemory:       []kubeletconfig.MemoryReservation{},
			affinity:                   getAffinityForOs(logger),
			expectedError:              fmt.Errorf("[memorymanager] you should specify the system reserved memory"),
			expectedReserved:           expectedReserved,
		},
		{
			description:                "Should return an error when policy name is not correct",
			policyName:                 "fake",
			machineInfo:                machineInfo,
			nodeAllocatableReservation: v1.ResourceList{},
			systemReservedMemory:       []kubeletconfig.MemoryReservation{},
			affinity:                   topologymanager.NewFakeManager(logger),
			expectedError:              fmt.Errorf("unknown policy: \"fake\""),
			expectedReserved:           expectedReserved,
		},
		{
			description:                "Should create manager with \"none\" policy",
			policyName:                 policyTypeNone,
			machineInfo:                machineInfo,
			nodeAllocatableReservation: v1.ResourceList{},
			systemReservedMemory:       []kubeletconfig.MemoryReservation{},
			affinity:                   topologymanager.NewFakeManager(logger),
			expectedError:              nil,
			expectedReserved:           expectedReserved,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			// 每个用例使用独立临时目录存放状态文件，避免用例间状态串扰。
			stateFileDirectory, err := os.MkdirTemp("", "memory_manager_tests")
			if err != nil {
				t.Errorf("Cannot create state file: %s", err.Error())
			}
			t.Cleanup(func() {
				require.NoErrorf(t, os.RemoveAll(stateFileDirectory), "unable to remove dir %s", stateFileDirectory)
			})

			mgr, err := NewManager(logger, string(testCase.policyName), &testCase.machineInfo, testCase.nodeAllocatableReservation, testCase.systemReservedMemory, stateFileDirectory, testCase.affinity)

			if !reflect.DeepEqual(err, testCase.expectedError) {
				t.Errorf("Could not create the Memory Manager. Expected error: '%v', but got: '%v'",
					testCase.expectedError, err)
			}

			if testCase.expectedError == nil {
				if mgr != nil {
					rawMgr := mgr.(*manager)
					if !reflect.DeepEqual(rawMgr.policy.Name(), string(testCase.policyName)) {
						t.Errorf("Could not create the Memory Manager. Expected policy name: %v, but got: %v",
							testCase.policyName, rawMgr.policy.Name())
					}
					if testCase.policyName == PolicyTypeStatic {
						if !reflect.DeepEqual(rawMgr.policy.(*staticPolicy).systemReserved, testCase.expectedReserved) {
							t.Errorf("Could not create the Memory Manager. Expected system reserved: %+v, but got: %+v",
								testCase.expectedReserved, rawMgr.policy.(*staticPolicy).systemReserved)
						}
					}
				} else {
					t.Errorf("Could not create the Memory Manager - manager is nil, but it should not be.")
				}

			}
		})
	}
}

// TestGetTopologyHints 验证静态策略生成的 NUMA hint：单节点 hint 为 preferred，
// 跨节点 hint 为非 preferred。
func TestGetTopologyHints(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []testMemoryManager{
		{
			description: "Successful hint generation",
			policyName:  PolicyTypeStatic,
			machineInfo: returnMachineInfo(),
			reserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1 * gb,
				},
			},
			assignments: state.ContainerMemoryAssignments{
				"fakePod1": map[string][]state.Block{
					"fakeContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
					"fakeContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         1 * gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					Cells:               []int{0},
					NumberOfAssignments: 4,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           7 * gb,
							Reserved:       2 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           3 * gb,
							Reserved:       2 * gb,
							SystemReserved: 0 * gb,
							TotalMemSize:   5 * gb,
						},
					},
				},
				1: &state.NUMANodeState{
					Cells:               []int{1},
					NumberOfAssignments: 0,
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9 * gb,
							Free:           9 * gb,
							Reserved:       0 * gb,
							SystemReserved: 1 * gb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
				},
			},
			expectedError: nil,
			expectedHints: map[string][]topologymanager.TopologyHint{
				string(v1.ResourceMemory): {
					{
						NUMANodeAffinity: newNUMAAffinity(0),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: newNUMAAffinity(1),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: newNUMAAffinity(0, 1),
						Preferred:        false,
					},
				},
				string(hugepages1Gi): {
					{
						NUMANodeAffinity: newNUMAAffinity(0),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: newNUMAAffinity(1),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: newNUMAAffinity(0, 1),
						Preferred:        false,
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			mgr := &manager{
				policy:       returnPolicyByName(logger, testCase),
				state:        state.NewMemoryState(logger),
				containerMap: containermap.NewContainerMap(),
				containerRuntime: mockRuntimeService{
					err: nil,
				},
				activePods:        func() []*v1.Pod { return nil },
				podStatusProvider: mockPodStatusProvider{},
			}
			mgr.sourcesReady = &sourcesReadyStub{}
			// 使用 Clone 隔离状态，防止被测代码的改动污染用例数据、影响其他断言。
			mgr.state.SetMachineState(testCase.machineState.Clone())
			mgr.state.SetMemoryAssignments(testCase.assignments.Clone())

			pod := getPod("fakePod1", "fakeContainer1", requirementsGuaranteed)
			container := &pod.Spec.Containers[0]
			hints := mgr.GetTopologyHints(logger, pod, container, lifecycle.AddOperation)
			if !reflect.DeepEqual(hints, testCase.expectedHints) {
				t.Errorf("Hints were not generated correctly. Hints generated: %+v, hints expected: %+v",
					hints, testCase.expectedHints)
			}
		})
	}
}

// TestAllocateAndAddPodWithInitContainers 验证 init container 占用的内存在 app container
// 启动后从状态文件中被移除，只保留 app container 的分配。
func TestAllocateAndAddPodWithInitContainers(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	testCases := []testMemoryManager{
		{
			description: "should remove init containers from the state file, once app container started",
			policyName:  PolicyTypeStatic,
			machineInfo: returnMachineInfo(),
			assignments: state.ContainerMemoryAssignments{},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         4 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         4 * gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9728 * mb,
							Free:           9728 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
					Cells: []int{0},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9728 * mb,
							Free:           9728 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
					Cells: []int{1},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9728 * mb,
							Free:           5632 * mb,
							Reserved:       4 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           gb,
							Reserved:       4 * gb,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 2,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    9728 * mb,
							Free:           9728 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    5 * gb,
							Free:           5 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   5 * gb,
						},
					},
					Cells: []int{1},
				},
			},
			reserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			podAllocate: getPodWithInitContainers(
				"pod1",
				[]v1.Container{
					{
						Name: "container1",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("4Gi"),
								hugepages1Gi:      resource.MustParse("4Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("4Gi"),
								hugepages1Gi:      resource.MustParse("4Gi"),
							},
						},
					},
				},
				[]v1.Container{
					{
						Name: "initContainer1",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("7Gi"),
								hugepages1Gi:      resource.MustParse("5Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("7Gi"),
								hugepages1Gi:      resource.MustParse("5Gi"),
							},
						},
					},
				},
			),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			logger.Info("TestAllocateAndAddPodWithInitContainers", "name", testCase.description)
			mgr := &manager{
				policy:       returnPolicyByName(logger, testCase),
				state:        state.NewMemoryState(logger),
				containerMap: containermap.NewContainerMap(),
				containerRuntime: mockRuntimeService{
					err: nil,
				},
				activePods:        func() []*v1.Pod { return []*v1.Pod{testCase.podAllocate} },
				podStatusProvider: mockPodStatusProvider{},
			}
			mgr.sourcesReady = &sourcesReadyStub{}
			mgr.state.SetMachineState(testCase.machineState.Clone())
			mgr.state.SetMemoryAssignments(testCase.assignments.Clone())

			// 为 init container 分配内存，此时状态中应记录 init container 的占用。
			for i := range testCase.podAllocate.Spec.InitContainers {
				err := mgr.Allocate(ctx, testCase.podAllocate, &testCase.podAllocate.Spec.InitContainers[i], lifecycle.AddOperation)
				if !reflect.DeepEqual(err, testCase.expectedError) {
					t.Fatalf("The actual error %v is different from the expected one %v", err, testCase.expectedError)
				}
			}

			// 为 app container 分配内存，此步骤应触发 init container 分配的回收。
			for i := range testCase.podAllocate.Spec.Containers {
				err := mgr.Allocate(ctx, testCase.podAllocate, &testCase.podAllocate.Spec.Containers[i], lifecycle.AddOperation)
				if !reflect.DeepEqual(err, testCase.expectedError) {
					t.Fatalf("The actual error %v is different from the expected one %v", err, testCase.expectedError)
				}
			}

			// 将 init container 登记到 containerMap。
			for i, initContainer := range testCase.podAllocate.Spec.InitContainers {
				mgr.AddContainer(logger, testCase.podAllocate, &testCase.podAllocate.Spec.InitContainers[i], initContainer.Name)
			}

			// 将 app container 登记到 containerMap。
			for i, appContainer := range testCase.podAllocate.Spec.Containers {
				mgr.AddContainer(logger, testCase.podAllocate, &testCase.podAllocate.Spec.Containers[i], appContainer.Name)
			}

			assignments := mgr.state.GetMemoryAssignments()
			if !areContainerMemoryAssignmentsEqual(t, assignments, testCase.expectedAssignments) {
				t.Fatalf("Actual assignments %v are different from the expected %v", assignments, testCase.expectedAssignments)
			}

			machineState := mgr.state.GetMachineState()
			if !areMachineStatesEqual(logger, machineState, testCase.expectedMachineState) {
				t.Fatalf("The actual machine state %v is different from the expected %v", machineState, testCase.expectedMachineState)
			}
		})
	}
}

// returnMachineInfo 构造两个 NUMA 节点的机器拓扑：每节点 10Gi 普通内存与 5 个 1Gi hugepage，
// 作为大多数用例的固定基准。
func returnMachineInfo() cadvisorapi.MachineInfo {
	return cadvisorapi.MachineInfo{
		Topology: []cadvisorapi.Node{
			{
				Id:     0,
				Memory: 10 * gb,
				HugePages: []cadvisorapi.HugePagesInfo{
					{
						PageSize: pageSize1Gb,
						NumPages: 5,
					},
				},
			},
			{
				Id:     1,
				Memory: 10 * gb,
				HugePages: []cadvisorapi.HugePagesInfo{
					{
						PageSize: pageSize1Gb,
						NumPages: 5,
					},
				},
			},
		},
	}
}
