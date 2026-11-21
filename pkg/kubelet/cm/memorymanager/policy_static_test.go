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
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/admission"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/kubernetes/test/utils/ktesting/initoption"
)

// 测试统一使用二进制单位，避免 Mi/Gi 换算时出现精度差异。
const (
	mb           = 1024 * 1024
	gb           = mb * 1024
	pageSize1Gb  = 1048576
	hugepages1Gi = v1.ResourceName(v1.ResourceHugePagesPrefix + "1Gi")
)

// 预置几组常用的资源配置，测试用例直接引用，避免重复构造。
var (
	containerRestartPolicyAlways = v1.ContainerRestartPolicyAlways

	podLevelRequirementsGuaranteed = &v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1000m"),
			v1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1000m"),
			v1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}

	podLevelRequirementsBurstable = &v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1000m"),
			v1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("500m"),
			v1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}

	requirementsGuaranteed = &v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1000Mi"),
			v1.ResourceMemory: resource.MustParse("1Gi"),
			hugepages1Gi:      resource.MustParse("1Gi"),
		},
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1000Mi"),
			v1.ResourceMemory: resource.MustParse("1Gi"),
			hugepages1Gi:      resource.MustParse("1Gi"),
		},
	}
	requirementsBurstable = &v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1000Mi"),
			v1.ResourceMemory: resource.MustParse("2Gi"),
			hugepages1Gi:      resource.MustParse("2Gi"),
		},
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1000Mi"),
			v1.ResourceMemory: resource.MustParse("1Gi"),
			hugepages1Gi:      resource.MustParse("1Gi"),
		},
	}
)

// areMemoryBlocksEqual 以顺序无关的方式比较两组内存块：
// 从副本中逐个摘除匹配项，最后看副本是否被清空。
func areMemoryBlocksEqual(mb1, mb2 []state.Block) bool {
	if len(mb1) != len(mb2) {
		return false
	}

	copyMemoryBlocks := make([]state.Block, len(mb2))
	copy(copyMemoryBlocks, mb2)
	for _, block := range mb1 {
		for i, copyBlock := range copyMemoryBlocks {
			if reflect.DeepEqual(block, copyBlock) {
				// 用末尾元素覆盖已匹配项再截断，避免 O(n^2) 的切片删除开销。
				// move the element that equals to the block to the end of the slice
				copyMemoryBlocks[i] = copyMemoryBlocks[len(copyMemoryBlocks)-1]

				// 截断末尾，副本长度即剩余未匹配数量。
				// remove the last element from our slice
				copyMemoryBlocks = copyMemoryBlocks[:len(copyMemoryBlocks)-1]

				break
			}
		}
	}

	return len(copyMemoryBlocks) == 0
}

// areContainerMemoryAssignmentsEqual 比较两份 Pod/容器的内存分配记录，
// 内存块内部顺序不敏感，但 Pod UID 与容器名必须完全对应。
func areContainerMemoryAssignmentsEqual(t *testing.T, cma1, cma2 state.ContainerMemoryAssignments) bool {
	if len(cma1) != len(cma2) {
		return false
	}

	for podUID, container := range cma1 {
		if _, ok := cma2[podUID]; !ok {
			t.Logf("[memorymanager_tests] the assignment does not have pod UID %s", podUID)
			return false
		}

		for containerName, memoryBlocks := range container {
			if _, ok := cma2[podUID][containerName]; !ok {
				t.Logf("[memorymanager_tests] the assignment does not have container name %s", containerName)
				return false
			}

			if !areMemoryBlocksEqual(memoryBlocks, cma2[podUID][containerName]) {
				t.Logf("[memorymanager_tests] assignments memory blocks are different: %v != %v", memoryBlocks, cma2[podUID][containerName])
				return false
			}
		}
	}
	return true
}

// testStaticPolicy 是 static policy 各测试共用的表驱动用例结构。
type testStaticPolicy struct {
	description                     string
	assignments                     state.ContainerMemoryAssignments
	expectedAssignments             state.ContainerMemoryAssignments
	machineState                    state.NUMANodeMap
	expectedMachineState            state.NUMANodeMap
	systemReserved                  systemReservedMemory
	expectedError                   error
	machineInfo                     *cadvisorapi.MachineInfo
	pod                             *v1.Pod
	topologyHint                    *topologymanager.TopologyHint
	expectedTopologyHints           map[string][]topologymanager.TopologyHint
	initContainersReusableMemory    reusableMemory
	podLevelResourcesEnabled        bool
	podLevelResourceManagersEnabled bool
	requiredMetrics                 requiredMetrics
}

// requiredMetrics 描述某个用例执行后各指标应有的增量。
type requiredMetrics struct {
	expTotalAllocs              int
	expTotalErrors              int
	expExclusiveAssignments     int
	expPodSharedPoolAssignments int
}

// initTests 按用例构造 policy 与 state，并注入 hint 和 init 容器可复用内存的实现。
// hint 为空时使用默认 FakeManager，避免所有用例都依赖拓扑亲和性。
func initTests(t *testing.T, testCase *testStaticPolicy, hint *topologymanager.TopologyHint, initContainersReusableMemory reusableMemory) (Policy, state.State, error) {
	logger, _ := ktesting.NewTestContext(t)
	manager := topologymanager.NewFakeManager(logger)
	if hint != nil {
		manager = topologymanager.NewFakeManagerWithHint(logger, hint)
	}

	p, err := NewPolicyStatic(logger, testCase.machineInfo, testCase.systemReserved, manager)
	if err != nil {
		return nil, nil, err
	}
	// 仅在需要时替换可复用内存实现，保持生产默认路径可被覆盖测试。
	if initContainersReusableMemory != nil {
		p.(*staticPolicy).initContainersReusableMemory = initContainersReusableMemory
	}
	s := state.NewMemoryState(logger)
	s.SetMachineState(testCase.machineState)
	s.SetMemoryAssignments(testCase.assignments)
	return p, s, nil
}

// newNUMAAffinity 用位下标构造 NUMA 亲和掩码；下标非法时直接 panic，
// 因为这只会在测试数据写错时发生。
func newNUMAAffinity(bits ...int) bitmask.BitMask {
	affinity, err := bitmask.NewBitMask(bits...)
	if err != nil {
		panic(err)
	}
	return affinity
}

// TestStaticPolicyNew 验证构造 static policy 时对系统预留内存的校验。
func TestStaticPolicyNew(t *testing.T) {
	testCases := []testStaticPolicy{
		{
			description:   "should fail, when machine does not have reserved memory for the system workloads",
			expectedError: fmt.Errorf("[memorymanager] you should specify the system reserved memory"),
		},
		{
			description: "should succeed, when at least one NUMA node has reserved memory",
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			_, _, err := initTests(t, &testCase, nil, nil)
			if !reflect.DeepEqual(err, testCase.expectedError) {
				t.Fatalf("The actual error: %v is different from the expected one: %v", err, testCase.expectedError)
			}
		})
	}
}

// TestStaticPolicyName 验证策略名与 PolicyTypeStatic 一致。
func TestStaticPolicyName(t *testing.T) {
	testCases := []testStaticPolicy{
		{
			description: "should return the correct policy name",
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			p, _, err := initTests(t, &testCase, nil, nil)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if p.Name() != string(PolicyTypeStatic) {
				t.Errorf("policy name is different, expected: %q, actual: %q", p.Name(), PolicyTypeStatic)
			}
		})
	}
}

// TestStaticPolicyStart 验证启动时按系统预留重建各 NUMA 节点的内存表，
// 并校验已有分配记录与机器状态的一致性。
func TestStaticPolicyStart(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []testStaticPolicy{
		{
			description: "should fail, if machine state is empty, but it has memory assignments",
			assignments: state.ContainerMemoryAssignments{
				"pod": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         512 * mb,
						},
					},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			expectedError: fmt.Errorf("[memorymanager] machine state can not be empty when it has memory assignments"),
		},
		{
			description:         "should fill the state with default values, when the state is empty",
			expectedAssignments: state.ContainerMemoryAssignments{},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   3 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					NumberOfAssignments: 0,
					Cells:               []int{0},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 3 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
		},
		{
			description: "should fail when machine state does not have all NUMA nodes",
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 0,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
					{
						Id:     1,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] the expected machine state is different from the real one"),
		},
		{
			description: "should fail when machine state does not have memory resource",
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 0,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			expectedError: fmt.Errorf("[memorymanager] the expected machine state is different from the real one"),
		},
		{
			description: "should fail when machine state has wrong size of total memory",
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   1536 * mb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 0,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] the expected machine state is different from the real one"),
		},
		{
			description: "should fail when machine state has wrong size of system reserved memory",
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 1024,
							TotalMemSize:   2 * gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 0,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] the expected machine state is different from the real one"),
		},
		{
			description: "should fail when machine state reserved memory is different from the memory of all containers memory assignments",
			assignments: state.ContainerMemoryAssignments{
				"pod": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         512 * mb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 1,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] the expected machine state is different from the real one"),
		},
		{
			description: "should fail when machine state has wrong size of hugepages",
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 0,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 2,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] the expected machine state is different from the real one"),
		},
		{
			description: "should fail when machine state has wrong size of system reserved hugepages",
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: gb,
							TotalMemSize:   2 * gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 0,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 2,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] the expected machine state is different from the real one"),
		},
		{
			description: "should fail when the hugepages reserved machine state is different from the hugepages of all containers memory assignments",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
				"pod2": map[string][]state.Block{
					"container2": {
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    4 * gb,
							Free:           gb,
							Reserved:       3 * gb,
							SystemReserved: 0,
							TotalMemSize:   4 * gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 2,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 4,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] the expected machine state is different from the real one"),
		},
		{
			description: "should fail when machine state does not have NUMA node that used under the memory assignment",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{1},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 0,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] (pod: pod1, container: container1) the memory assignment uses the NUMA that does not exist"),
		},
		{
			description: "should fail when machine state does not have resource that used under the memory assignment",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages2M,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 2,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] (pod: pod1, container: container1) the memory assignment uses memory resource that does not exist"),
		},
		{
			description: "should fail when machine state number of assignments is different from the expected one",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 1,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("[memorymanager] the expected machine state is different from the real one"),
		},
		{
			description: "should validate cross NUMA reserved memory vs container assignments",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         768 * mb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
				"pod2": map[string][]state.Block{
					"container2": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         256 * mb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         768 * mb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
				"pod2": map[string][]state.Block{
					"container2": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         256 * mb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    640 * mb,
							Free:           0,
							Reserved:       640 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2176 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           0,
							Reserved:       gb,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 4,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    640 * mb,
							Free:           256 * mb,
							Reserved:       384 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2176 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           0,
							Reserved:       gb,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 4,
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    640 * mb,
							Free:           0,
							Reserved:       640 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2176 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           0,
							Reserved:       gb,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 4,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    640 * mb,
							Free:           256 * mb,
							Reserved:       384 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2176 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           0,
							Reserved:       gb,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 4,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 2176 * mb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
					{
						Id:     1,
						Memory: 2176 * mb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								// size in KB
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
		},
		{
			description: "should validate the totalFree and totalReserved size for a resource within a group",
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         240 * mb,
						},
					},
				},
				"pod2": map[string][]state.Block{
					"container2": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         10 * mb,
						},
					},
				},
			},
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         240 * mb,
						},
					},
				},
				"pod2": map[string][]state.Block{
					"container2": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         10 * mb,
						},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    220 * mb,
							Free:           30 * mb,
							Reserved:       190 * mb,
							SystemReserved: 20 * mb,
							TotalMemSize:   240 * mb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    220 * mb,
							Free:           160 * mb,
							Reserved:       60 * mb,
							SystemReserved: 20 * mb,
							TotalMemSize:   240 * mb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    220 * mb,
							Free:           10 * mb,
							Reserved:       210 * mb,
							SystemReserved: 20 * mb,
							TotalMemSize:   240 * mb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    220 * mb,
							Free:           180 * mb,
							Reserved:       40 * mb,
							SystemReserved: 20 * mb,
							TotalMemSize:   240 * mb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 20 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 20 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 240 * mb,
					},
					{
						Id:     1,
						Memory: 240 * mb,
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			t.Logf("[Start] %s", testCase.description)
			p, s, err := initTests(t, &testCase, nil, nil)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			err = p.Start(logger, s)
			if !reflect.DeepEqual(err, testCase.expectedError) {
				t.Fatalf("The actual error: %v is different from the expected one: %v", err, testCase.expectedError)
			}

			if err != nil {
				return
			}

			assignments := s.GetMemoryAssignments()
			if !areContainerMemoryAssignmentsEqual(t, assignments, testCase.expectedAssignments) {
				t.Fatalf("Actual assignments: %v is different from the expected one: %v", assignments, testCase.expectedAssignments)
			}

			machineState := s.GetMachineState()
			if !areMachineStatesEqual(logger, machineState, testCase.expectedMachineState) {
				t.Fatalf("The actual machine state: %v is different from the expected one: %v", machineState, testCase.expectedMachineState)
			}
		})
	}
}

// TestStaticPolicyAllocate 验证容器级内存分配：guaranteed 容器按 NUMA 亲和性分配，
// 非 guaranteed 容器不分配；同时校验分配失败时的错误与状态回滚。
func TestStaticPolicyAllocate(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	testCases := []testStaticPolicy{
		{
			description:         "should do nothing for non-guaranteed pods",
			expectedAssignments: state.ContainerMemoryAssignments{},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod:                   getPod("pod1", "container1", requirementsBurstable),
			expectedTopologyHints: nil,
			topologyHint:          &topologymanager.TopologyHint{},
		},
		{
			description: "should do nothing once container already exists under the state file",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1024 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1024 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod:                   getPod("pod1", "container1", requirementsGuaranteed),
			expectedTopologyHints: nil,
			topologyHint:          &topologymanager.TopologyHint{},
		},
		{
			description: "should calculate a default topology hint when no NUMA affinity was provided by the topology manager hint",
			assignments: state.ContainerMemoryAssignments{},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{0},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1024 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           0,
							Reserved:       gb,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 2,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod:          getPod("pod1", "container1", requirementsGuaranteed),
			topologyHint: &topologymanager.TopologyHint{},
		},
		{
			description: "should fail when no NUMA affinity was provided under the topology manager hint and calculation of the default hint failed",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1024 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           0,
							Reserved:       gb,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 2,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod:           getPod("pod2", "container2", requirementsGuaranteed),
			expectedError: fmt.Errorf("[memorymanager] failed to get the default NUMA affinity, no NUMA nodes with enough memory is available"),
			topologyHint:  &topologymanager.TopologyHint{},
		},
		{
			description: "should fail when no NUMA affinity was provided under the topology manager preferred hint and default hint has preferred false",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         512 * mb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    gb,
							Free:           512 * mb,
							Reserved:       512 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   1536 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 1,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   1536 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{1},
					NumberOfAssignments: 0,
				},
				2: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   1536 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{2},
					NumberOfAssignments: 0,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				2: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod:           getPod("pod2", "container2", requirementsGuaranteed),
			expectedError: fmt.Errorf("[memorymanager] failed to find the default preferred hint"),
			topologyHint:  &topologymanager.TopologyHint{Preferred: true},
		},
		{
			description: "should fail when NUMA affinity provided under the topology manager hint did not satisfy container requirements and extended hint generation failed",
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 0,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{1, 2},
					NumberOfAssignments: 1,
				},
				2: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{1, 2},
					NumberOfAssignments: 1,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				2: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod:           getPod("pod1", "container1", requirementsGuaranteed),
			expectedError: fmt.Errorf("[memorymanager] failed to find NUMA nodes to extend the current topology hint"),
			topologyHint:  &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: false},
		},
		{
			description: "should fail when the topology manager provided the preferred hint and extended hint has preferred false",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         512 * mb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    gb,
							Free:           512 * mb,
							Reserved:       512 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   1536 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 1,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   1536 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{1},
					NumberOfAssignments: 0,
				},
				2: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   1536 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{2},
					NumberOfAssignments: 0,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				2: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod:           getPod("pod2", "container2", requirementsGuaranteed),
			expectedError: fmt.Errorf("[memorymanager] failed to find the extended preferred hint"),
			topologyHint:  &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(1), Preferred: true},
		},
		{
			description: "should succeed to allocate memory from multiple NUMA nodes",
			assignments: state.ContainerMemoryAssignments{},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 0,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{1},
					NumberOfAssignments: 0,
				},
				2: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{2},
					NumberOfAssignments: 0,
				},
				3: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{3},
					NumberOfAssignments: 0,
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           0,
							Reserved:       512 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           0,
							Reserved:       gb,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           0,
							Reserved:       512 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
				},
				2: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{2},
					NumberOfAssignments: 0,
				},
				3: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{3},
					NumberOfAssignments: 0,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				2: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				3: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod:          getPod("pod1", "container1", requirementsGuaranteed),
			topologyHint: &topologymanager.TopologyHint{Preferred: true},
		},
		{
			description: "should validate NUMA node can not have both single and cross NUMA node memory allocations",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1024 * mb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         1024 * mb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1024 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2176 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 1,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2176 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{1},
					NumberOfAssignments: 0,
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1024 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2176 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 1,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2176 * mb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{1},
					NumberOfAssignments: 0,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod:           getPod("pod2", "container1", requirementsGuaranteed),
			topologyHint:  &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0, 1), Preferred: true},
			expectedError: fmt.Errorf("[memorymanager] preferred hint violates NUMA node allocation"),
		},
		{
			description:         "should do nothing for guaranteed pod with pod level resources",
			expectedAssignments: state.ContainerMemoryAssignments{},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod:                             getPodWithPodLevelResources("pod1", podLevelRequirementsGuaranteed, "container1", requirementsGuaranteed),
			expectedTopologyHints:           nil,
			topologyHint:                    &topologymanager.TopologyHint{},
			expectedError:                   nil,
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, testCase.podLevelResourcesEnabled)
			if testCase.podLevelResourcesEnabled && !testCase.podLevelResourceManagersEnabled {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResourceManagers, false)
			}

			t.Logf("TestStaticPolicyAllocate %s", testCase.description)
			p, s, err := initTests(t, &testCase, testCase.topologyHint, nil)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			err = p.Allocate(ctx, s, testCase.pod, &testCase.pod.Spec.Containers[0], lifecycle.AddOperation)
			if (err == nil) != (testCase.expectedError == nil) || (err != nil && testCase.expectedError != nil && err.Error() != testCase.expectedError.Error()) {
				t.Fatalf("The actual error %v is different from the expected one %v", err, testCase.expectedError)
			}

			if err != nil {
				return
			}

			assignments := s.GetMemoryAssignments()
			if !areContainerMemoryAssignmentsEqual(t, assignments, testCase.expectedAssignments) {
				t.Fatalf("Actual assignments %v are different from the expected %v", assignments, testCase.expectedAssignments)
			}

			machineState := s.GetMachineState()
			if !areMachineStatesEqual(logger, machineState, testCase.expectedMachineState) {
				t.Fatalf("The actual machine state %v is different from the expected %v", machineState, testCase.expectedMachineState)
			}
		})
	}
}

// TestStaticPolicyAllocateWithInitContainers 验证 init 容器内存可被后续容器复用：
// init 容器峰值用量与普通容器用量按取大者的规则计算。
func TestStaticPolicyAllocateWithInitContainers(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	testCases := []testStaticPolicy{
		{
			description: "should re-use init containers memory, init containers requests 1Gi and 2Gi, apps containers 3Gi and 4Gi",
			assignments: state.ContainerMemoryAssignments{},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"initContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         0,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         0,
						},
					},
					"initContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         0,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         0,
						},
					},
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         3 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         3 * gb,
						},
					},
					"container2": {
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
							Allocatable:    7680 * mb,
							Free:           7680 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   8 * gb,
						},
						hugepages1Gi: {
							Allocatable:    8 * gb,
							Free:           8 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   8 * gb,
						},
					},
					Cells: []int{0},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    7680 * mb,
							Free:           512 * mb,
							Reserved:       7 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   8 * gb,
						},
						hugepages1Gi: {
							Allocatable:    8 * gb,
							Free:           1 * gb,
							Reserved:       7 * gb,
							SystemReserved: 0,
							TotalMemSize:   8 * gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 8,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod: getPodWithInitContainers(
				"pod1",
				[]v1.Container{
					{
						Name: "container1",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("3Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("3Gi"),
							},
						},
					},
					{
						Name: "container2",
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
								v1.ResourceMemory: resource.MustParse("1Gi"),
								hugepages1Gi:      resource.MustParse("1Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("1Gi"),
								hugepages1Gi:      resource.MustParse("1Gi"),
							},
						},
					},
					{
						Name: "initContainer2",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
						},
					},
				},
			),
			topologyHint: &topologymanager.TopologyHint{},
		},
		{
			description: "should re-use init containers memory, init containers requests 4Gi and 3Gi, apps containers 2Gi and 1Gi",
			assignments: state.ContainerMemoryAssignments{},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"initContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         0,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         0,
						},
					},
					"initContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         2 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         2 * gb,
						},
					},
					"container2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    7680 * mb,
							Free:           7680 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   8 * gb,
						},
						hugepages1Gi: {
							Allocatable:    8 * gb,
							Free:           8 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   8 * gb,
						},
					},
					Cells: []int{0},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    7680 * mb,
							Free:           3584 * mb,
							Reserved:       4 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   8 * gb,
						},
						hugepages1Gi: {
							Allocatable:    8 * gb,
							Free:           4 * gb,
							Reserved:       4 * gb,
							SystemReserved: 0,
							TotalMemSize:   8 * gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 8,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod: getPodWithInitContainers(
				"pod1",
				[]v1.Container{
					{
						Name: "container1",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
						},
					},
					{
						Name: "container2",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("1Gi"),
								hugepages1Gi:      resource.MustParse("1Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("1Gi"),
								hugepages1Gi:      resource.MustParse("1Gi"),
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
					{
						Name: "initContainer2",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("3Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("3Gi"),
							},
						},
					},
				},
			),
			topologyHint: &topologymanager.TopologyHint{},
		},
		{
			description: "should re-use init containers memory, init containers requests 7Gi and 4Gi, apps containers 4Gi and 3Gi",
			assignments: state.ContainerMemoryAssignments{},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"initContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         0,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         0,
						},
					},
					"initContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         0,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         0,
						},
					},
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
					"container2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         3 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         3 * gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    7680 * mb,
							Free:           7680 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   8 * gb,
						},
						hugepages1Gi: {
							Allocatable:    8 * gb,
							Free:           8 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   8 * gb,
						},
					},
					Cells: []int{0},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    7680 * mb,
							Free:           512 * mb,
							Reserved:       7 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   8 * gb,
						},
						hugepages1Gi: {
							Allocatable:    8 * gb,
							Free:           1 * gb,
							Reserved:       7 * gb,
							SystemReserved: 0,
							TotalMemSize:   8 * gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 8,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod: getPodWithInitContainers(
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
					{
						Name: "container2",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("3Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("3Gi"),
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
								hugepages1Gi:      resource.MustParse("7Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("7Gi"),
								hugepages1Gi:      resource.MustParse("7Gi"),
							},
						},
					},
					{
						Name: "initContainer2",
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
			),
			topologyHint: &topologymanager.TopologyHint{},
		},
		{
			description:                  "should re-use init containers memory, init containers requests 7Gi and 4Gi, apps containers 5Gi and 2Gi",
			assignments:                  state.ContainerMemoryAssignments{},
			initContainersReusableMemory: reusableMemory{"pod0": map[string]map[v1.ResourceName]uint64{}},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"initContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         0,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         0,
						},
					},
					"initContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         0,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         0,
						},
					},
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         5 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         5 * gb,
						},
					},
					"container2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         2 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         2 * gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    10240 * mb,
							Free:           10240 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    10 * gb,
							Free:           10 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   10 * gb,
						},
					},
					Cells: []int{0},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    10240 * mb,
							Free:           3072 * mb,
							Reserved:       7 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   10 * gb,
						},
						hugepages1Gi: {
							Allocatable:    10 * gb,
							Free:           3 * gb,
							Reserved:       7 * gb,
							SystemReserved: 0,
							TotalMemSize:   10 * gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 8,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod: getPodWithInitContainers(
				"pod1",
				[]v1.Container{
					{
						Name: "container1",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("5Gi"),
								hugepages1Gi:      resource.MustParse("5Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("5Gi"),
								hugepages1Gi:      resource.MustParse("5Gi"),
							},
						},
					},
					{
						Name: "container2",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
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
								hugepages1Gi:      resource.MustParse("7Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("7Gi"),
								hugepages1Gi:      resource.MustParse("7Gi"),
							},
						},
					},
					{
						Name: "initContainer2",
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
			),
			topologyHint: &topologymanager.TopologyHint{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			logger.Info("TestStaticPolicyAllocateWithInitContainers", "name", testCase.description)
			p, s, err := initTests(t, &testCase, testCase.topologyHint, testCase.initContainersReusableMemory)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			for i := range testCase.pod.Spec.InitContainers {
				err = p.Allocate(ctx, s, testCase.pod, &testCase.pod.Spec.InitContainers[i], lifecycle.AddOperation)
				if !reflect.DeepEqual(err, testCase.expectedError) {
					t.Fatalf("The actual error %v is different from the expected one %v", err, testCase.expectedError)
				}
			}

			for i := range testCase.pod.Spec.Containers {
				err = p.Allocate(ctx, s, testCase.pod, &testCase.pod.Spec.Containers[i], lifecycle.AddOperation)
				if !reflect.DeepEqual(err, testCase.expectedError) {
					t.Fatalf("The actual error %v is different from the expected one %v", err, testCase.expectedError)
				}
			}

			assignments := s.GetMemoryAssignments()
			if !areContainerMemoryAssignmentsEqual(t, assignments, testCase.expectedAssignments) {
				t.Fatalf("Actual assignments %v are different from the expected %v", assignments, testCase.expectedAssignments)
			}

			machineState := s.GetMachineState()
			if !areMachineStatesEqual(logger, machineState, testCase.expectedMachineState) {
				t.Fatalf("The actual machine state %v is different from the expected %v", machineState, testCase.expectedMachineState)
			}
		})
	}
}

// TestStaticPolicyAllocateWithRestartableInitContainers 验证可重启 init 容器
// 与普通 init 容器的内存分配差异：前者需要常驻，不能与后续容器复用。
func TestStaticPolicyAllocateWithRestartableInitContainers(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	testCases := []testStaticPolicy{
		{
			description: "should do nothing once containers already exist under the state file",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"initContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
					},
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"initContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
					},
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    2560 * mb,
							Free:           512 * mb,
							Reserved:       2048 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   3 * gb,
						},
						hugepages1Gi: {
							Allocatable:    2 * gb,
							Free:           2 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   2 * gb,
						},
					},
					Cells: []int{},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    2560 * mb,
							Free:           512 * mb,
							Reserved:       2048 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   3 * gb,
						},
						hugepages1Gi: {
							Allocatable:    2 * gb,
							Free:           2 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   2 * gb,
						},
					},
					Cells: []int{},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod: getPodWithInitContainers(
				"pod1",
				[]v1.Container{
					{
						Name:      "container1",
						Resources: *requirementsGuaranteed,
					},
				},
				[]v1.Container{
					{
						Name:          "initContainer1",
						Resources:     *requirementsGuaranteed,
						RestartPolicy: &containerRestartPolicyAlways,
					},
				},
			),
			expectedTopologyHints: nil,
			topologyHint:          &topologymanager.TopologyHint{},
		},
		{
			description: "should not re-use restartable init containers memory",
			assignments: state.ContainerMemoryAssignments{},
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"initContainer1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         0,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         0,
						},
					},
					"restartableInitContainer2": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         2 * gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         2 * gb,
						},
					},
					"initContainer3": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         0,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         0,
						},
					},
					"restartableInitContainer4": {
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
					"container1": {
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
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    7 * gb,
							Free:           7 * gb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   7680 * mb,
						},
						hugepages1Gi: {
							Allocatable:    7 * gb,
							Free:           7 * gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   7 * gb,
						},
					},
					Cells: []int{0},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    7 * gb,
							Free:           0,
							Reserved:       7 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   7680 * mb,
						},
						hugepages1Gi: {
							Allocatable:    7 * gb,
							Free:           0,
							Reserved:       7 * gb,
							SystemReserved: 0,
							TotalMemSize:   7 * gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 10,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			pod: getPodWithInitContainers(
				"pod1",
				[]v1.Container{
					{
						Name:      "container1",
						Resources: *requirementsGuaranteed,
					},
				},
				[]v1.Container{
					{
						Name: "initContainer1",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("1Gi"),
								hugepages1Gi:      resource.MustParse("1Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("1Gi"),
								hugepages1Gi:      resource.MustParse("1Gi"),
							},
						},
					},
					{
						Name: "restartableInitContainer2",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
						},
						RestartPolicy: &containerRestartPolicyAlways,
					},
					{
						Name: "initContainer3",
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("3Gi"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("1000Mi"),
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("3Gi"),
							},
						},
					},
					{
						Name: "restartableInitContainer4",
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
						RestartPolicy: &containerRestartPolicyAlways,
					},
				},
			),
			topologyHint: &topologymanager.TopologyHint{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			logger.Info("TestStaticPolicyAllocateWithRestartableInitContainers", "name", testCase.description)
			p, s, err := initTests(t, &testCase, testCase.topologyHint, testCase.initContainersReusableMemory)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			for i := range testCase.pod.Spec.InitContainers {
				err = p.Allocate(ctx, s, testCase.pod, &testCase.pod.Spec.InitContainers[i], lifecycle.AddOperation)
				if !reflect.DeepEqual(err, testCase.expectedError) {
					t.Fatalf("The actual error %v is different from the expected one %v", err, testCase.expectedError)
				}
			}

			if err != nil {
				return
			}

			for i := range testCase.pod.Spec.Containers {
				err = p.Allocate(ctx, s, testCase.pod, &testCase.pod.Spec.Containers[i], lifecycle.AddOperation)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}

			assignments := s.GetMemoryAssignments()
			if !areContainerMemoryAssignmentsEqual(t, assignments, testCase.expectedAssignments) {
				t.Fatalf("Actual assignments %v are different from the expected %v", assignments, testCase.expectedAssignments)
			}

			machineState := s.GetMachineState()
			if !areMachineStatesEqual(logger, machineState, testCase.expectedMachineState) {
				t.Fatalf("The actual machine state %v is different from the expected %v", machineState, testCase.expectedMachineState)
			}
		})
	}
}

// TestStaticPolicyRemoveContainer 验证移除容器后，其占用的内存块会被归还到对应的 NUMA 节点。
func TestStaticPolicyRemoveContainer(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []testStaticPolicy{
		{
			description:         "should do nothing when the container does not exist under the state",
			expectedAssignments: state.ContainerMemoryAssignments{},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
		},
		{
			description: "should delete the container assignment and update the machine state",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1024 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           0,
							Reserved:       gb,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					NumberOfAssignments: 2,
					Cells:               []int{0},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0},
					NumberOfAssignments: 0,
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
		},
		{
			description: "should delete the cross NUMA container assignment and update the machine state",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			expectedAssignments: state.ContainerMemoryAssignments{},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           0,
							Reserved:       512 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           0,
							Reserved:       gb,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					NumberOfAssignments: 2,
					Cells:               []int{0, 1},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           0,
							Reserved:       512 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					NumberOfAssignments: 2,
					Cells:               []int{0, 1},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					NumberOfAssignments: 0,
					Cells:               []int{0},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    512 * mb,
							Free:           512 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					NumberOfAssignments: 0,
					Cells:               []int{1},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			p, s, err := initTests(t, &testCase, nil, nil)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			p.RemoveContainer(logger, s, "pod1", "container1")
			assignments := s.GetMemoryAssignments()
			if !areContainerMemoryAssignmentsEqual(t, assignments, testCase.expectedAssignments) {
				t.Fatalf("Actual assignments %v are different from the expected %v", assignments, testCase.expectedAssignments)
			}

			machineState := s.GetMachineState()
			if !areMachineStatesEqual(logger, machineState, testCase.expectedMachineState) {
				t.Fatalf("The actual machine state %v is different from the expected %v", machineState, testCase.expectedMachineState)
			}
		})
	}
}

// TestStaticPolicyGetTopologyHints 验证容器级拓扑提示：guaranteed 容器给出候选 NUMA 掩码，
// 非 guaranteed 或已有分配记录的容器按对应规则返回。
func TestStaticPolicyGetTopologyHints(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []testStaticPolicy{
		{
			description: "should not provide topology hints for non-guaranteed pods",
			pod:         getPod("pod1", "container1", requirementsBurstable),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			expectedTopologyHints: nil,
		},
		{
			description: "should provide topology hints based on the existent memory assignment",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			pod: getPod("pod1", "container1", requirementsGuaranteed),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			expectedTopologyHints: map[string][]topologymanager.TopologyHint{
				string(v1.ResourceMemory): {
					{
						NUMANodeAffinity: newNUMAAffinity(0),
						Preferred:        true,
					},
				},
				string(hugepages1Gi): {
					{
						NUMANodeAffinity: newNUMAAffinity(0),
						Preferred:        true,
					},
				},
			},
		},
		{
			description: "should calculate new topology hints, when the container does not exist under assignments",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         2 * gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages1Gi,
							Size:         2 * gb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           0,
							Reserved:       1536 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           0,
							Reserved:       gb,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           gb,
							Reserved:       512 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           0,
							Reserved:       gb,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
				},
				2: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{2},
					NumberOfAssignments: 0,
				},
				3: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells:               []int{3},
					NumberOfAssignments: 0,
				},
			},
			pod: getPod("pod2", "container2", requirementsGuaranteed),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				2: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				3: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			expectedTopologyHints: map[string][]topologymanager.TopologyHint{
				string(v1.ResourceMemory): {
					{
						NUMANodeAffinity: newNUMAAffinity(2),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: newNUMAAffinity(3),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: newNUMAAffinity(2, 3),
						Preferred:        false,
					},
				},
				string(hugepages1Gi): {
					{
						NUMANodeAffinity: newNUMAAffinity(2),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: newNUMAAffinity(3),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: newNUMAAffinity(2, 3),
						Preferred:        false,
					},
				},
			},
		},
		{
			description: "should fail when number of existing memory assignment resources are different from resources requested by container",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
					},
				},
			},
			pod: getPod("pod1", "container1", requirementsGuaranteed),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			expectedTopologyHints: nil,
		},
		{
			description: "should fail when existing memory assignment resources are different from resources requested by container",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         gb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages2M,
							Size:         gb,
						},
					},
				},
			},
			pod: getPod("pod1", "container1", requirementsGuaranteed),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			expectedTopologyHints: nil,
		},
		{
			description: "should fail when existing memory assignment size is different from one requested by the container",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0},
							Type:         v1.ResourceMemory,
							Size:         512 * mb,
						},
						{
							NUMAAffinity: []int{0},
							Type:         hugepages1Gi,
							Size:         gb,
						},
					},
				},
			},
			pod: getPod("pod1", "container1", requirementsGuaranteed),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			expectedTopologyHints: nil,
		},
		{
			description: "should not return preferred hints with multiple NUMA nodes for the pod with resources satisfied by a single NUMA node",
			assignments: state.ContainerMemoryAssignments{
				"pod1": map[string][]state.Block{
					"container1": {
						{
							NUMAAffinity: []int{0, 1},
							Type:         v1.ResourceMemory,
							Size:         2 * gb,
						},
						{
							NUMAAffinity: []int{0, 1},
							Type:         hugepages2M,
							Size:         24 * mb,
						},
					},
				},
			},
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           0,
							Reserved:       1536 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages2M: {
							Allocatable:    20 * mb,
							Free:           0,
							Reserved:       20 * mb,
							SystemReserved: 0,
							TotalMemSize:   20 * mb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           gb,
							Reserved:       512 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages2M: {
							Allocatable:    20 * mb,
							Free:           16 * mb,
							Reserved:       4 * mb,
							SystemReserved: 0,
							TotalMemSize:   20 * mb,
						},
					},
					Cells:               []int{0, 1},
					NumberOfAssignments: 2,
				},
			},
			pod: getPod("pod2",
				"container2",
				&v1.ResourceRequirements{
					Limits: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("1000Mi"),
						v1.ResourceMemory: resource.MustParse("1Gi"),
						hugepages2M:       resource.MustParse("16Mi"),
					},
					Requests: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("1000Mi"),
						v1.ResourceMemory: resource.MustParse("1Gi"),
						hugepages2M:       resource.MustParse("16Mi"),
					},
				},
			),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 512 * mb,
				},
			},
			expectedTopologyHints: map[string][]topologymanager.TopologyHint{
				string(v1.ResourceMemory): {
					{
						NUMANodeAffinity: newNUMAAffinity(0, 1),
						Preferred:        false,
					},
				},
				hugepages2M: {
					{
						NUMANodeAffinity: newNUMAAffinity(0, 1),
						Preferred:        false,
					},
				},
			},
		},
		{
			description: "should not provide topology hints for guaranteed pod with pod level resources",
			pod:         getPodWithPodLevelResources("pod1", podLevelRequirementsGuaranteed, "container1", requirementsGuaranteed),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
			},
			expectedTopologyHints:    nil,
			podLevelResourcesEnabled: true,
		},
		{
			description: "should provide pod topology hints for guaranteed pod with pod level resources when PodLevelResourceManagers is enabled",
			pod:         getPodWithPodLevelResources("pod1", podLevelRequirementsGuaranteed, "container1", requirementsGuaranteed),
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    2 * gb,
							Free:           2 * gb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   3 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{0},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    2 * gb,
							Free:           2 * gb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   3 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{1},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 3 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
					{
						Id:     1,
						Memory: 3 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedTopologyHints: map[string][]topologymanager.TopologyHint{
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
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
		}, {
			description: "should not provide pod topology hints for burstable pod with pod level resources when PodLevelResourceManagers is enabled",
			pod:         getPodWithPodLevelResources("pod1", podLevelRequirementsBurstable, "container1", requirementsGuaranteed),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
			},
			expectedTopologyHints:           nil,
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			// 用例各自声明所需 feature gate 状态，避免用例之间相互影响。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, testCase.podLevelResourcesEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResourceManagers, testCase.podLevelResourceManagersEnabled)

			p, s, err := initTests(t, &testCase, nil, nil)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			topologyHints := p.GetTopologyHints(logger, s, testCase.pod, &testCase.pod.Spec.Containers[0], lifecycle.AddOperation)
			if !reflect.DeepEqual(topologyHints, testCase.expectedTopologyHints) {
				t.Fatalf("The actual topology hints: '%+v' are different from the expected one: '%+v'", topologyHints, testCase.expectedTopologyHints)
			}
		})
	}
}

// TestStaticPolicyGetPodTopologyHints 验证 pod 级拓扑提示；只有启用
// PodLevelResources 与 PodLevelResourceManagers 且 pod 级资源为 guaranteed 时才会给出提示。
func TestStaticPolicyGetPodTopologyHints(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []testStaticPolicy{
		{
			description: "should provide pod topology hints for guaranteed pod without container level resources when PodLevelResources and PodLevelResourceManagers are disabled",
			pod:         getPod("pod1", "container1", requirementsGuaranteed),
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    2 * gb,
							Free:           2 * gb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   3 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{0},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    2 * gb,
							Free:           2 * gb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   3 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{1},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 3 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
					{
						Id:     1,
						Memory: 3 * gb,
						HugePages: []cadvisorapi.HugePagesInfo{
							{
								PageSize: pageSize1Gb,
								NumPages: 1,
							},
						},
					},
				},
			},
			expectedTopologyHints: map[string][]topologymanager.TopologyHint{
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
			podLevelResourcesEnabled:        false,
			podLevelResourceManagersEnabled: false,
		},
		{
			description: "should not provide pod topology hints for burstable pod with container level resources when PodLevelResources and PodLevelResourceManagers are disabled",
			pod:         getPod("pod1", "container1", requirementsBurstable),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
			},
			expectedTopologyHints:           nil,
			podLevelResourcesEnabled:        false,
			podLevelResourceManagersEnabled: false,
		},
		{
			description: "should not provide pod topology hints for guaranteed pod with pod level resources when PodLevelResourceManagers is disabled",
			pod:         getPodWithPodLevelResources("pod1", podLevelRequirementsGuaranteed, "container1", requirementsGuaranteed),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
			},
			expectedTopologyHints:           nil,
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: false,
		},
		{
			description: "should provide pod topology hints for guaranteed pod with pod level resources when PodLevelResourceManagers is enabled",
			pod:         getPodWithPodLevelResources("pod1", podLevelRequirementsGuaranteed, "container1", requirementsGuaranteed),
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    2 * gb,
							Free:           2 * gb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   3 * gb,
						},
					},
					Cells: []int{0},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    2 * gb,
							Free:           2 * gb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   3 * gb,
						},
					},
					Cells: []int{1},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 3 * gb,
					},
					{
						Id:     1,
						Memory: 3 * gb,
					},
				},
			},
			expectedTopologyHints: map[string][]topologymanager.TopologyHint{
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
			},
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
		},
		{
			description: "should provide pod topology hints for guaranteed pod with pod level resources that overrides container level resources",
			pod:         getPodWithPodLevelResources("pod1", podLevelRequirementsGuaranteed, "container1", requirementsBurstable),
			machineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    2 * gb,
							Free:           2 * gb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   3 * gb,
						},
					},
					Cells: []int{0},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    2 * gb,
							Free:           2 * gb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   3 * gb,
						},
					},
					Cells: []int{1},
				},
			},
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
				1: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
			},
			machineInfo: &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 3 * gb,
					},
					{
						Id:     1,
						Memory: 3 * gb,
					},
				},
			},
			expectedTopologyHints: map[string][]topologymanager.TopologyHint{
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
			},
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
		},
		{
			description: "should not provide pod topology hints for burstable pod with pod level resources when PodLevelResourceManagers is enabled",
			pod:         getPodWithPodLevelResources("pod1", podLevelRequirementsBurstable, "container1", requirementsGuaranteed),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
			},
			expectedTopologyHints:           nil,
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
		},
		{
			description: "should not provide pod topology hints for burstable pod with pod level and container level burstable resources",
			pod:         getPodWithPodLevelResources("pod1", podLevelRequirementsBurstable, "container1", requirementsBurstable),
			systemReserved: systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 1024 * mb,
				},
			},
			expectedTopologyHints:           nil,
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, testCase.podLevelResourcesEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResourceManagers, testCase.podLevelResourceManagersEnabled)

			p, s, err := initTests(t, &testCase, nil, nil)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			topologyHints := p.GetPodTopologyHints(logger, s, testCase.pod, lifecycle.AddOperation)
			if !reflect.DeepEqual(topologyHints, testCase.expectedTopologyHints) {
				t.Fatalf("The actual topology hints: '%+v' are different from the expected one: '%+v'", topologyHints, testCase.expectedTopologyHints)
			}
		})
	}
}

// Test_getPodRequestedResources 验证 Pod 请求量的聚合规则：
// init 容器取最大值（可重启 init 容器累加），再与普通容器总和取大者。
func Test_getPodRequestedResources(t *testing.T) {
	testCases := []struct {
		description string
		pod         *v1.Pod
		expected    map[v1.ResourceName]uint64
	}{
		{
			description: "maximum resources of init containers > total resources of containers",
			pod: getPodWithInitContainers(
				"",
				[]v1.Container{
					{
						Name: "container1",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("1Gi"),
								hugepages1Gi:      resource.MustParse("1Gi"),
							},
						},
					},
					{
						Name: "container2",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
						},
					},
				},
				[]v1.Container{
					{
						Name: "initContainer1",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("1Gi"),
								hugepages1Gi:      resource.MustParse("1Gi"),
							},
						},
					},
					{
						Name: "initContainer2",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("4Gi"),
								hugepages1Gi:      resource.MustParse("4Gi"),
							},
						},
					},
				},
			),
			expected: map[v1.ResourceName]uint64{
				v1.ResourceMemory: 4 * gb,
				hugepages1Gi:      4 * gb,
			},
		},
		{
			description: "maximum resources of init containers < total resources of containers",
			pod: getPodWithInitContainers(
				"",
				[]v1.Container{
					{
						Name: "container1",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
						},
					},
					{
						Name: "container2",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("3Gi"),
							},
						},
					},
				},
				[]v1.Container{
					{
						Name: "initContainer1",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("1Gi"),
								hugepages1Gi:      resource.MustParse("1Gi"),
							},
						},
					},
					{
						Name: "initContainer2",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("3Gi"),
							},
						},
					},
				},
			),
			expected: map[v1.ResourceName]uint64{
				v1.ResourceMemory: 5 * gb,
				hugepages1Gi:      5 * gb,
			},
		},
		{
			description: "calculate different resources independently",
			pod: getPodWithInitContainers(
				"",
				[]v1.Container{
					{
						Name: "container1",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("1Gi"),
							},
						},
					},
					{
						Name: "container2",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
						},
					},
				},
				[]v1.Container{
					{
						Name: "initContainer1",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("1Gi"),
								hugepages1Gi:      resource.MustParse("1Gi"),
							},
						},
					},
					{
						Name: "initContainer2",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("4Gi"),
							},
						},
					},
				},
			),
			expected: map[v1.ResourceName]uint64{
				v1.ResourceMemory: 5 * gb,
				hugepages1Gi:      4 * gb,
			},
		},
		{
			description: "maximum resources of init containers > total resources of long running containers, including restartable init containers",
			pod: getPodWithInitContainers(
				"",
				[]v1.Container{
					{
						Name: "container1",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("1Gi"),
								hugepages1Gi:      resource.MustParse("1Gi"),
							},
						},
					},
					{
						Name: "container2",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
						},
					},
				},
				[]v1.Container{
					{
						Name: "restartableInit1",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
						},
						RestartPolicy: &containerRestartPolicyAlways,
					},
					{
						Name: "initContainer2",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("4Gi"),
								hugepages1Gi:      resource.MustParse("4Gi"),
							},
						},
					},
				},
			),
			expected: map[v1.ResourceName]uint64{
				v1.ResourceMemory: 6 * gb,
				hugepages1Gi:      6 * gb,
			},
		},
		{
			description: "maximum resources of init containers < total resources of long running containers, including restartable init containers",
			pod: getPodWithInitContainers(
				"",
				[]v1.Container{
					{
						Name: "container1",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
						},
					},
					{
						Name: "container2",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("3Gi"),
								hugepages1Gi:      resource.MustParse("3Gi"),
							},
						},
					},
				},
				[]v1.Container{
					{
						Name: "restartableInit1",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("2Gi"),
								hugepages1Gi:      resource.MustParse("2Gi"),
							},
						},
						RestartPolicy: &containerRestartPolicyAlways,
					},
					{
						Name: "initContainer2",
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("4Gi"),
								hugepages1Gi:      resource.MustParse("4Gi"),
							},
						},
					},
				},
			),
			expected: map[v1.ResourceName]uint64{
				v1.ResourceMemory: 7 * gb,
				hugepages1Gi:      7 * gb,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)

			actual, err := getPodRequestedResources(logger, tc.pod)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if diff := cmp.Diff(actual, tc.expected); diff != "" {
				t.Errorf("getPodRequestedResources() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Test_isAffinityViolatingNUMAAllocations 验证给定的 NUMA 亲和掩码是否会破坏
// 已存在分配之间的 cell 一致性约束。
func Test_isAffinityViolatingNUMAAllocations(t *testing.T) {
	testsCases := []struct {
		description         string
		machineState        map[int]*state.NUMANodeState
		topologyHint        *topologymanager.TopologyHint
		isViolationExpected bool
	}{
		{
			description: "violating NUMA allocations because given affinity asks for NUMA ID 1 which is on different cells group",
			machineState: map[int]*state.NUMANodeState{
				0: {
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
				1: {
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
				2: {
					NumberOfAssignments: 1,
					Cells:               []int{2},
				},
				3: {
					NumberOfAssignments: 0,
					Cells:               []int{3},
				},
			},
			topologyHint: &topologymanager.TopologyHint{
				NUMANodeAffinity: newNUMAAffinity(1, 2),
			},
			isViolationExpected: true,
		},
		{
			description: "violating NUMA allocations because given affinity with multiple nodes asks for NUMA ID 1 which is used for a single NUMA node memory allocation",
			machineState: map[int]*state.NUMANodeState{
				0: {
					NumberOfAssignments: 0,
					Cells:               []int{0, 1},
				},
				1: {
					NumberOfAssignments: 1,
					Cells:               []int{1},
				},
			},
			topologyHint: &topologymanager.TopologyHint{
				NUMANodeAffinity: newNUMAAffinity(0, 1),
			},
			isViolationExpected: true,
		},
		{
			description: "valid affinity, no prior assignments",
			machineState: map[int]*state.NUMANodeState{
				0: {
					NumberOfAssignments: 0,
					Cells:               []int{0},
				},
				1: {
					NumberOfAssignments: 0,
					Cells:               []int{1},
				},
			},
			topologyHint: &topologymanager.TopologyHint{
				NUMANodeAffinity: newNUMAAffinity(0, 1),
			},
			isViolationExpected: false,
		},
	}
	for _, tc := range testsCases {
		t.Run(tc.description, func(t *testing.T) {
			if isAffinityViolatingNUMAAllocations(tc.machineState, tc.topologyHint.NUMANodeAffinity) != tc.isViolationExpected {
				t.Errorf("isAffinityViolatingNUMAAllocations with affinity %v expected to return %t, got %t", tc.topologyHint.NUMANodeAffinity.GetBits(), tc.isViolationExpected, !tc.isViolationExpected)
			}
		})
	}
}

// containerOptions 描述构造测试 Pod 时单个容器的资源与重启策略。
type containerOptions struct {
	name          string
	request       string
	limit         string
	restartPolicy *v1.ContainerRestartPolicy
}

// podOption 是构造 Pod 后追加修改的函数式选项。
type podOption func(*v1.Pod)

// withPodResources 给 Pod 设置 pod 级内存请求与限制。
func withPodResources(memRequest, memLimit string) podOption {
	return func(pod *v1.Pod) {
		pod.Spec.Resources = &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse(memRequest),
			},
			Limits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse(memLimit),
			},
		}
	}
}

// makeMultiContainerPodWithOptions 按选项构造多容器 Pod；
// 容器名按索引生成，保证同一用例内名称唯一。
func makeMultiContainerPodWithOptions(initContainers, appContainers []*containerOptions, podOptions ...podOption) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod",
			UID:  "podUID",
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{},
			Containers:     []v1.Container{},
		},
	}

	for i, opts := range initContainers {
		container := v1.Container{
			Name: "initContainer-" + strconv.Itoa(i),
		}
		if opts.request != "" {
			container.Resources = v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse(opts.request),
					v1.ResourceCPU:    resource.MustParse("1"),
				},
				Limits: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse(opts.limit),
					v1.ResourceCPU:    resource.MustParse("1"),
				},
			}
		}
		if opts.restartPolicy != nil {
			container.RestartPolicy = opts.restartPolicy
		}
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, container)
	}

	for i, opts := range appContainers {
		container := v1.Container{
			Name: "appContainer-" + strconv.Itoa(i),
		}
		if opts.request != "" {
			container.Resources = v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse(opts.request),
					v1.ResourceCPU:    resource.MustParse("1"),
				},
				Limits: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse(opts.limit),
					v1.ResourceCPU:    resource.MustParse("1"),
				},
			}
		}
		pod.Spec.Containers = append(pod.Spec.Containers, container)
	}

	// pod 级选项最后应用，便于覆盖前面设置的字段。
	for _, fn := range podOptions {
		fn(pod)
	}

	return pod
}

// makeMultiContainerPodWithOptionsAndPodLevelResources 构造带 pod 级内存资源的 Pod，
// 请求与限制取同一个值。
func makeMultiContainerPodWithOptionsAndPodLevelResources(podLevelMemory string, initContainers, appContainers []*containerOptions) *v1.Pod {
	return makeMultiContainerPodWithOptions(initContainers, appContainers, withPodResources(podLevelMemory, podLevelMemory))
}

// TestValidatePodScopeResources 验证 pod 级资源的作用域校验：
// 容器级与 pod 级混用、guaranteed 判定等场景应返回对应错误。
func TestValidatePodScopeResources(t *testing.T) {
	// 该测试只关心作用域校验逻辑，因此默认开启 pod 级资源管理器。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResourceManagers, true)
	testCases := []struct {
		name        string
		pod         *v1.Pod
		expectedErr error
		scope       string
		features    map[featuregate.Feature]bool
	}{
		{
			name: "Valid: Pod-level != Guaranteed containers, has podSharedPool containers, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2Gi",
				[]*containerOptions{},
				[]*containerOptions{
					{name: "c1", request: "1Gi", limit: "1Gi"},
					{name: "c2"},
				},
			),
			scope:       topologymanager.PodTopologyScope,
			expectedErr: nil,
			features:    map[featuregate.Feature]bool{features.PodLevelResources: true, features.PodLevelResourceManagers: true},
		},
		{
			name: "Valid: Only Guaranteed containers, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2Gi",
				[]*containerOptions{},
				[]*containerOptions{
					{name: "c1", request: "1Gi", limit: "1Gi"},
					{name: "c2", request: "1Gi", limit: "1Gi"},
				},
			),
			scope:       topologymanager.PodTopologyScope,
			expectedErr: nil,
			features:    map[featuregate.Feature]bool{features.PodLevelResources: true, features.PodLevelResourceManagers: true},
		},
		{
			name: "Valid: Only podSharedPool containers, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2Gi",
				[]*containerOptions{},
				[]*containerOptions{
					{name: "c1"},
					{name: "c2"},
				},
			),
			scope:       topologymanager.PodTopologyScope,
			expectedErr: nil,
			features:    map[featuregate.Feature]bool{features.PodLevelResources: true, features.PodLevelResourceManagers: true},
		},
		{
			name: "Valid: Pod-level == Guaranteed containers, has podSharedPool containers with request, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"6Gi",
				[]*containerOptions{},
				[]*containerOptions{
					{name: "c1", request: "3Gi", limit: "3Gi"},
					{name: "c2", request: "2Gi", limit: "2Gi"},
					{name: "c3", request: "500Gi", limit: "1Gi"},
					{name: "c4"},
				},
			),
			scope:       topologymanager.PodTopologyScope,
			expectedErr: nil,
			features:    map[featuregate.Feature]bool{features.PodLevelResources: true, features.PodLevelResourceManagers: true},
		},
		{
			name: "Failure: Pod-level == Guaranteed containers, has podSharedPool containers, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"5Gi",
				[]*containerOptions{},
				[]*containerOptions{
					{name: "c1", request: "2Gi", limit: "2Gi"},
					{name: "c2", request: "3Gi", limit: "3Gi"},
					{name: "c3"},
				},
			),
			scope:       topologymanager.PodTopologyScope,
			expectedErr: fmt.Errorf("pod rejected, sum of exclusive container memory requests equals pod budget, leaving no memory for shared containers"),
			features:    map[featuregate.Feature]bool{features.PodLevelResources: true, features.PodLevelResourceManagers: true},
		},
		{
			name: "Valid: Pod-level resources with standard and restartable init containers, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2Gi",
				[]*containerOptions{
					{name: "init-c1", request: "1Gi", limit: "1Gi"},
					{name: "restartable-init-c1", request: "1Gi", limit: "1Gi", restartPolicy: &containerRestartPolicyAlways},
				},
				[]*containerOptions{
					{name: "c1"},
				},
			),
			scope:       topologymanager.PodTopologyScope,
			expectedErr: nil,
			features:    map[featuregate.Feature]bool{features.PodLevelResources: true, features.PodLevelResourceManagers: true},
		},
		{
			name: "Valid: Pod-level resources equal init and standard container resources without non-guaranteed containers do not produce empty shared pool error, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2Gi",
				[]*containerOptions{
					{name: "restartable-init-c1", request: "1Gi", limit: "1Gi", restartPolicy: &containerRestartPolicyAlways},
				},
				[]*containerOptions{
					{name: "c1", request: "1Gi", limit: "1Gi"},
				},
			),
			scope:       topologymanager.PodTopologyScope,
			expectedErr: nil,
			features:    map[featuregate.Feature]bool{features.PodLevelResources: true, features.PodLevelResourceManagers: true},
		},
		{
			name: "Failure: Pod-level resources when guaranteed sidecar that equals pod resources and then non-guaranteed init container produce empty shared pool error, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2Gi",
				[]*containerOptions{
					{name: "restartable-init-c1", request: "2Gi", limit: "2Gi", restartPolicy: &containerRestartPolicyAlways},
					{name: "init-c1"},
				},
				[]*containerOptions{},
			),
			scope:       topologymanager.PodTopologyScope,
			expectedErr: fmt.Errorf("pod rejected, pod has shared init containers but no memory available for them"),
			features:    map[featuregate.Feature]bool{features.PodLevelResources: true, features.PodLevelResourceManagers: true},
		},
		{
			name: "Valid: Pod-level resources when non-guaranteed standard init container first and then guaranteed sidecar container do not produce empty shared pool error, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2Gi",
				[]*containerOptions{
					{name: "init-c1"},
					{name: "restartable-init-c1", request: "2Gi", limit: "2Gi", restartPolicy: &containerRestartPolicyAlways},
				},
				[]*containerOptions{},
			),
			scope:       topologymanager.PodTopologyScope,
			expectedErr: nil,
			features:    map[featuregate.Feature]bool{features.PodLevelResources: true, features.PodLevelResourceManagers: true},
		},
		{
			name: "Valid: Pod-level resources when guaranteed standard init container first and then guaranteed sidecar container do not produce empty shared pool error, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2Gi",
				[]*containerOptions{
					{name: "init-c1", request: "2Gi", limit: "2Gi"},
					{name: "restartable-init-c1", request: "2Gi", limit: "2Gi", restartPolicy: &containerRestartPolicyAlways},
				},
				[]*containerOptions{},
			),
			scope:       topologymanager.PodTopologyScope,
			expectedErr: nil,
			features:    map[featuregate.Feature]bool{features.PodLevelResources: true, features.PodLevelResourceManagers: true},
		},
		{
			name: "Failure: Pod-level == Restartable guaranteed init containers, has podSharedPool containers, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2Gi",
				[]*containerOptions{
					{name: "restartable-init-c1", request: "2Gi", limit: "2Gi", restartPolicy: &containerRestartPolicyAlways},
					{name: "restartable-init-c2", restartPolicy: &containerRestartPolicyAlways},
				},
				[]*containerOptions{},
			),
			scope:       topologymanager.PodTopologyScope,
			expectedErr: fmt.Errorf("pod rejected, sum of exclusive container memory requests equals pod budget, leaving no memory for shared containers"),
			features:    map[featuregate.Feature]bool{features.PodLevelResources: true, features.PodLevelResourceManagers: true},
		},
		{
			name: "Valid: Pod-level resources with only restartable one init container and one standard container, PLR: Enabled, PLRM: Enabled",
			pod: makeMultiContainerPodWithOptionsAndPodLevelResources(
				"2Gi",
				[]*containerOptions{
					{name: "restartable-init-c", request: "1Gi", limit: "1Gi", restartPolicy: &containerRestartPolicyAlways},
				},
				[]*containerOptions{
					{name: "c1", request: "1Gi", limit: "1Gi"},
				},
			),
			scope:       topologymanager.PodTopologyScope,
			expectedErr: nil,
			features:    map[featuregate.Feature]bool{features.PodLevelResources: true, features.PodLevelResourceManagers: true},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 用例可以覆盖默认 gate 状态，例如临时关闭 PodLevelResources。
			for feature, enabled := range tc.features {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, feature, enabled)
			}

			logger, _ := ktesting.NewTestContext(t)
			machineInfo := &cadvisorapi.MachineInfo{
				Topology: []cadvisorapi.Node{
					{
						Id:     0,
						Memory: 8 * gb,
					},
				},
			}
			systemReserved := systemReservedMemory{
				0: map[v1.ResourceName]uint64{
					v1.ResourceMemory: 256 * mb,
				},
			}
			policy, err := NewPolicyStatic(logger, machineInfo, systemReserved, topologymanager.NewFakeManagerWithScope(tc.scope))
			if err != nil {
				t.Fatalf("NewPolicyStatic() failed: %v", err)
			}

			err = policy.(*staticPolicy).validatePodScopeResources(logger, tc.pod)
			if tc.expectedErr != nil {
				if err == nil {
					t.Errorf("Expected error %q, got nil", tc.expectedErr.Error())
				} else if err.Error() != tc.expectedErr.Error() {
					t.Errorf("Expected error %q, got %q", tc.expectedErr.Error(), err.Error())
				}
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// containerSpec 描述构造带 pod 级资源的 Pod 时单个容器的内存配置。
type containerSpec struct {
	name          string
	memRequest    string
	memLimit      string
	restartPolicy *v1.ContainerRestartPolicy
}

// getPodWithContainersAndPodLevelResources 构造同时含 init 容器、普通容器和
// pod 级资源的 Pod；pod 级资源只在请求或限制非空时设置。
func getPodWithContainersAndPodLevelResources(podName, podMemRequest, podMemLimit string, initContainers, appContainers []containerSpec) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
			UID:  "podUID", // Using a static UID for simplicity in tests, can be overridden.
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{},
			Containers:     []v1.Container{},
			RestartPolicy:  v1.RestartPolicyNever,
		},
	}
	// 用 pod 名充当 UID，测试中 pod 名需唯一。
	pod.ObjectMeta.UID = types.UID(podName)

	for i, c := range initContainers {
		container := v1.Container{
			Name:  c.name,
			Image: "image",
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("1"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("1"),
				},
			},
		}
		if c.name == "" {
			container.Name = "init-container-" + strconv.Itoa(i)
		}
		if c.memRequest != "" {
			container.Resources.Requests[v1.ResourceMemory] = resource.MustParse(c.memRequest)
		}
		if c.memLimit != "" {
			container.Resources.Limits[v1.ResourceMemory] = resource.MustParse(c.memLimit)
		}
		if c.restartPolicy != nil {
			container.RestartPolicy = c.restartPolicy
		}
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, container)
	}

	for i, c := range appContainers {
		container := v1.Container{
			Name:  c.name,
			Image: "image",
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("1"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("1"),
				},
			},
		}
		if c.name == "" {
			container.Name = "app-container-" + strconv.Itoa(i)
		}
		if c.memRequest != "" {
			container.Resources.Requests[v1.ResourceMemory] = resource.MustParse(c.memRequest)
		}
		if c.memLimit != "" {
			container.Resources.Limits[v1.ResourceMemory] = resource.MustParse(c.memLimit)
		}
		pod.Spec.Containers = append(pod.Spec.Containers, container)
	}

	// 只有显式给出内存值时才有 pod 级资源，否则置 nil 以模拟未启用该特性。
	if podMemRequest != "" || podMemLimit != "" {
		pod.Spec.Resources = &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("3"),
			},
			Limits: v1.ResourceList{
				v1.ResourceCPU: resource.MustParse("3"),
			},
		}
		if podMemRequest != "" {
			pod.Spec.Resources.Requests[v1.ResourceMemory] = resource.MustParse(podMemRequest)
		}
		if podMemLimit != "" {
			pod.Spec.Resources.Limits[v1.ResourceMemory] = resource.MustParse(podMemLimit)
		}
	} else {
		pod.Spec.Resources = nil
	}
	return pod
}

// TestStaticPolicyAllocatePod 验证 pod 级内存分配及相应指标：
// 分配次数、错误数、独占/共享池分配数都要符合预期。
func TestStaticPolicyAllocatePod(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	machineInfo := &cadvisorapi.MachineInfo{
		Topology: []cadvisorapi.Node{
			{Id: 0, Memory: 2 * gb},
			{Id: 1, Memory: 2 * gb},
		},
	}

	systemReserved := systemReservedMemory{
		0: map[v1.ResourceName]uint64{v1.ResourceMemory: 512 * mb},
		1: map[v1.ResourceName]uint64{v1.ResourceMemory: 512 * mb},
	}

	machineState := state.NUMANodeMap{
		0: &state.NUMANodeState{
			MemoryMap: map[v1.ResourceName]*state.MemoryTable{
				v1.ResourceMemory: {
					Allocatable:    1536 * mb,
					Free:           1536 * mb,
					Reserved:       0,
					SystemReserved: 512 * mb,
					TotalMemSize:   2 * gb,
				},
			},
			NumberOfAssignments: 0,
			Cells:               []int{0},
		},
		1: &state.NUMANodeState{
			MemoryMap: map[v1.ResourceName]*state.MemoryTable{
				v1.ResourceMemory: {
					Allocatable:    1536 * mb,
					Free:           1536 * mb,
					Reserved:       0,
					SystemReserved: 512 * mb,
					TotalMemSize:   2 * gb,
				},
			},
			NumberOfAssignments: 0,
			Cells:               []int{1},
		},
	}

	testCases := []testStaticPolicy{
		{
			description:    "should successfully allocate memory for a guaranteed pod with pod-level resources",
			machineInfo:    machineInfo,
			systemReserved: systemReserved,
			machineState:   machineState,
			pod: getPodWithContainersAndPodLevelResources("pod1", "1Gi", "1Gi", nil, []containerSpec{
				{name: "container1", memRequest: "512Mi", memLimit: "512Mi"},
			}),
			topologyHint:  &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: true},
			expectedError: nil,
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod1": {
					"container1": []state.Block{
						{NUMAAffinity: []int{0}, Size: 512 * mb, Type: v1.ResourceMemory},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 0,
					Cells:               []int{1},
				},
			},
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              1,
				expExclusiveAssignments:     1,
				expPodSharedPoolAssignments: 0,
			},
		},
		{
			description:    "scope: pod, should allocate exclusive memory to a guaranteed pod with pod-level resources and guaranteed container, multi NUMA node, PodLevelResourceManagers enabled",
			machineInfo:    machineInfo,
			systemReserved: systemReserved,
			machineState:   machineState,
			pod: getPodWithContainersAndPodLevelResources("pod2", "2Gi", "2Gi", nil, []containerSpec{
				{name: "container1", memRequest: "1Gi", memLimit: "1Gi"},
			}),
			topologyHint:  &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(1), Preferred: true},
			expectedError: nil,
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod2": {
					"container1": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 1 * gb, Type: v1.ResourceMemory},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1024 * mb,
							Reserved:       512 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           0,
							Reserved:       1536 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
			},
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              1,
				expExclusiveAssignments:     1,
				expPodSharedPoolAssignments: 0,
			},
		},
		{
			description:    "scope: pod, should allocate exclusive memory to a guaranteed pod with pod-level resources and non-guaranteed container, multi NUMA node, PodLevelResourceManagers enabled",
			machineInfo:    machineInfo,
			systemReserved: systemReserved,
			machineState:   machineState,
			pod: getPodWithContainersAndPodLevelResources("pod3", "2Gi", "2Gi", nil, []containerSpec{
				{name: "container1"},
				{name: "container2"},
			}),
			topologyHint:  &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0, 1), Preferred: true},
			expectedError: nil,
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod3": {
					"container1": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 2 * gb, Type: v1.ResourceMemory},
					},
					"container2": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 2 * gb, Type: v1.ResourceMemory},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
			},
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              2,
				expExclusiveAssignments:     0,
				expPodSharedPoolAssignments: 2,
			},
		},
		{
			description:    "scope: pod, should allocate exclusive memory to a guaranteed pod with pod-level resources and mix of guaranteed and non-guaranteed containers, multi NUMA node, PodLevelResourceManagers enabled",
			machineInfo:    machineInfo,
			systemReserved: systemReserved,
			machineState:   machineState,
			pod: getPodWithContainersAndPodLevelResources("pod3", "2Gi", "2Gi", nil, []containerSpec{
				{name: "container1", memRequest: "1Gi", memLimit: "1Gi"},
				{name: "container2"},
				{name: "container3"},
			}),
			topologyHint:  &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0, 1), Preferred: true},
			expectedError: nil,
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod3": {
					"container1": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 1 * gb, Type: v1.ResourceMemory},
					},
					"container2": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 1 * gb, Type: v1.ResourceMemory},
					},
					"container3": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 1 * gb, Type: v1.ResourceMemory},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
			},
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              3,
				expExclusiveAssignments:     1,
				expPodSharedPoolAssignments: 2,
			},
		},
		{
			description:    "scope: pod, should allocate exclusive memory to a guaranteed pod with pod-level resources and mix of guaranteed standard and guaranteed init containers, multi NUMA node, PodLevelResourceManagers enabled",
			machineInfo:    machineInfo,
			systemReserved: systemReserved,
			machineState:   machineState,
			pod: getPodWithContainersAndPodLevelResources(
				"pod4", "2Gi", "2Gi",
				[]containerSpec{
					{name: "init-container1", memRequest: "2Gi", memLimit: "2Gi"},
				},
				[]containerSpec{
					{name: "container1", memRequest: "1Gi", memLimit: "1Gi"},
					{name: "container2"},
				},
			),
			topologyHint:  &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0, 1), Preferred: true},
			expectedError: nil,
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod4": {
					"init-container1": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 2 * gb, Type: v1.ResourceMemory},
					},
					"container1": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 1 * gb, Type: v1.ResourceMemory},
					},
					"container2": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 1 * gb, Type: v1.ResourceMemory},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
			},
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              3,
				expExclusiveAssignments:     2,
				expPodSharedPoolAssignments: 1,
			},
		},
		{
			description:    "scope: pod, should allocate exclusive memory to a guaranteed pod with pod-level resources and mix of guaranteed standard and guaranteed restartable and non-guaranteed standard init containers, multi NUMA node, PodLevelResourceManagers enabled",
			machineInfo:    machineInfo,
			systemReserved: systemReserved,
			machineState:   machineState,
			pod: getPodWithContainersAndPodLevelResources(
				"pod4", "3Gi", "3Gi",
				[]containerSpec{
					{name: "init-container1"},
					{name: "init-restartable-container2", memRequest: "1Gi", memLimit: "1Gi", restartPolicy: &containerRestartPolicyAlways},
					{name: "init-container3"},
				},
				[]containerSpec{
					{name: "container1", memRequest: "1Gi", memLimit: "1Gi"},
					{name: "container2"},
				},
			),
			topologyHint:  &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0, 1), Preferred: true},
			expectedError: nil,
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod4": {
					"init-container1": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 3 * gb, Type: v1.ResourceMemory},
					},
					"init-restartable-container2": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 1 * gb, Type: v1.ResourceMemory},
					},
					"init-container3": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 2 * gb, Type: v1.ResourceMemory},
					},
					"container1": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 1 * gb, Type: v1.ResourceMemory},
					},
					"container2": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 1 * gb, Type: v1.ResourceMemory},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           0,
							Reserved:       1536 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           0,
							Reserved:       1536 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
			},
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              5,
				expExclusiveAssignments:     2,
				expPodSharedPoolAssignments: 3,
			},
		},
		{
			description:    "scope: pod, should allocate exclusive memory to a guaranteed pod with pod-level resources and mix of guaranteed standard and non-guaranteed restartable and standard init containers, multi NUMA node, PodLevelResourceManagers enabled",
			machineInfo:    machineInfo,
			systemReserved: systemReserved,
			machineState:   machineState,
			pod: getPodWithContainersAndPodLevelResources(
				"pod4", "3Gi", "3Gi",
				[]containerSpec{
					{name: "init-container1"},
					{name: "init-restartable-container2", restartPolicy: &containerRestartPolicyAlways},
					{name: "init-container3"},
				},
				[]containerSpec{
					{name: "container1", memRequest: "1Gi", memLimit: "1Gi"},
					{name: "container2"},
				},
			),
			topologyHint:  &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0, 1), Preferred: true},
			expectedError: nil,
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod4": {
					"init-container1": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 3 * gb, Type: v1.ResourceMemory},
					},
					"init-restartable-container2": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 2 * gb, Type: v1.ResourceMemory},
					},
					"init-container3": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 3 * gb, Type: v1.ResourceMemory},
					},
					"container1": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 1 * gb, Type: v1.ResourceMemory},
					},
					"container2": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 2 * gb, Type: v1.ResourceMemory},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           0,
							Reserved:       1536 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           0,
							Reserved:       1536 * mb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
			},
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              5,
				expExclusiveAssignments:     1,
				expPodSharedPoolAssignments: 4,
			},
		},
		{
			description:    "scope: pod, should allocate exclusive memory to a guaranteed pod with pod-level resources and mix of guaranteed standard and non-guaranteed init containers, multi NUMA node, PodLevelResourceManagers enabled",
			machineInfo:    machineInfo,
			systemReserved: systemReserved,
			machineState:   machineState,
			pod: getPodWithContainersAndPodLevelResources(
				"pod4", "2Gi", "2Gi",
				[]containerSpec{
					{name: "init-container1"},
				},
				[]containerSpec{
					{name: "container1", memRequest: "1Gi", memLimit: "1Gi"},
					{name: "container2"},
				},
			),
			topologyHint:  &topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0, 1), Preferred: true},
			expectedError: nil,
			expectedAssignments: state.ContainerMemoryAssignments{
				"pod4": {
					"init-container1": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 2 * gb, Type: v1.ResourceMemory},
					},
					"container1": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 1 * gb, Type: v1.ResourceMemory},
					},
					"container2": []state.Block{
						{NUMAAffinity: []int{0, 1}, Size: 1 * gb, Type: v1.ResourceMemory},
					},
				},
			},
			expectedMachineState: state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
				1: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           512 * mb,
							Reserved:       1 * gb,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
					},
					NumberOfAssignments: 1,
					Cells:               []int{0, 1},
				},
			},
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalAllocs:              3,
				expExclusiveAssignments:     1,
				expPodSharedPoolAssignments: 2,
			},
		},
		{
			description:    "scope: pod, should reject a pod that would result in an empty pod shared pool",
			machineInfo:    machineInfo,
			systemReserved: systemReserved,
			machineState:   machineState,
			pod: getPodWithContainersAndPodLevelResources("pod3", "2Gi", "2Gi", nil, []containerSpec{
				{name: "container1", memRequest: "1Gi", memLimit: "1Gi"},
				{name: "container2", memRequest: "1Gi", memLimit: "1Gi"},
				{name: "container3"},
			}),
			topologyHint:                    &topologymanager.TopologyHint{},
			expectedError:                   admission.NewEmptyPodSharedPoolError(fmt.Errorf("pod rejected, sum of exclusive container memory requests equals pod budget, leaving no memory for shared containers")),
			expectedAssignments:             state.ContainerMemoryAssignments{},
			expectedMachineState:            machineState,
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalErrors: 1,
			},
		},
		{
			description:    "scope: pod, should not allocate exclusive memory to a non-guaranteed pod with pod-level resources and guaranteed containers, PodLevelResourceManagers enabled",
			machineInfo:    machineInfo,
			systemReserved: systemReserved,
			machineState:   machineState,
			pod: getPodWithContainersAndPodLevelResources("pod3", "1Gi", "2Gi", nil, []containerSpec{
				{name: "container1", memRequest: "1Gi", memLimit: "1Gi"},
				{name: "container2"},
				{name: "container3"},
			}),
			topologyHint:                    &topologymanager.TopologyHint{},
			expectedError:                   nil,
			expectedAssignments:             state.ContainerMemoryAssignments{},
			expectedMachineState:            machineState,
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			requiredMetrics: requiredMetrics{
				expTotalErrors: 0,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, testCase.podLevelResourcesEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResourceManagers, testCase.podLevelResourceManagersEnabled)

			// 指标是全局的，逐个用例重置，保证断言的是本用例产生的增量。
			metrics.Register()
			metrics.ResourceManagerAllocationsTotal.Reset()
			metrics.ResourceManagerAllocationErrorsTotal.Reset()
			metrics.ResourceManagerContainerAssignments.Reset()

			p, s, err := initTests(t, &testCase, testCase.topologyHint, testCase.initContainersReusableMemory)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			err = p.AllocatePod(logger, s, testCase.pod, lifecycle.AddOperation)
			// 出错时只校验错误与错误指标，不再比较分配状态。
			if testCase.expectedError != nil {
				require.Error(t, err)
				require.Equal(t, testCase.expectedError.Error(), err.Error())

				errors, err := testutil.GetCounterMetricValue(metrics.ResourceManagerAllocationErrorsTotal.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerPod))
				require.NoError(t, err)
				require.InDelta(t, float64(testCase.requiredMetrics.expTotalErrors), errors, 0.001, "expected allocation errors to be incremented")

				return
			}
			require.NoError(t, err)

			assignments := s.GetMemoryAssignments()
			if !areContainerMemoryAssignmentsEqual(t, assignments, testCase.expectedAssignments) {
				t.Fatalf("Actual assignments: %v is different from the expected one: %v", assignments, testCase.expectedAssignments)
			}

			machineState := s.GetMachineState()
			if !areMachineStatesEqual(logger, machineState, testCase.expectedMachineState) {
				t.Fatalf("The actual machine state: %v is different from the expected one: %v", machineState, testCase.expectedMachineState)
			}

			allocations, err := testutil.GetCounterMetricValue(metrics.ResourceManagerAllocationsTotal.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerPod))
			require.NoError(t, err)
			require.InDelta(t, float64(testCase.requiredMetrics.expTotalAllocs), allocations, 0.001, "unexpected number of allocations")

			exclusiveAssignments, err := testutil.GetCounterMetricValue(metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerExclusivePod))
			require.NoError(t, err)
			require.InDelta(t, float64(testCase.requiredMetrics.expExclusiveAssignments), exclusiveAssignments, 0.001, "unexpected number of assignments")

			podSharedPoolAssignments, err := testutil.GetCounterMetricValue(metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerSharedPod))
			require.NoError(t, err)
			require.InDelta(t, float64(testCase.requiredMetrics.expPodSharedPoolAssignments), podSharedPoolAssignments, 0.001, "unexpected number of assignments")
		})
	}
}

// 以下生命周期测试验证 static policy 是否按 lifecycle.Operation 处理或跳过操作。
// Allocate* 与 GetTopologyHints* 遇到不支持的操作不会返回错误（而是静默返回
// nil 或空提示，避免中断所有 hint provider 的整体流程），因此测试改为检查日志，
// 以此判断操作是被处理还是被跳过。
//
// 用例中的内存大小、cpuset、Pod 资源请求等取值只为让代码走通正常路径，
// 具体数值没有特殊含义；这些测试不校验分配结果或提示的正确性，
// 只校验给定生命周期操作是否被处理。

// TestStaticPolicyLifecycleAllocate 验证容器级分配只在 AddOperation 时执行。
func TestStaticPolicyLifecycleAllocate(t *testing.T) {
	testCases := []struct {
		description string
		operation   lifecycle.Operation
		skipped     bool
	}{
		{
			description: "MemoryManager static policy processes AddOperation",
			operation:   lifecycle.AddOperation,
			skipped:     false,
		},
		{
			description: "MemoryManager static policy skips ResizeOperation",
			operation:   lifecycle.ResizeOperation,
			skipped:     true,
		},
		{
			description: "MemoryManager static policy skips empty operation",
			operation:   "",
			skipped:     true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			// 缓冲日志后断言"跳过"提示是否出现，这是判断操作是否被处理的唯一可观测信号。
			ktesting.SetDefaultVerbosity(2)
			tCtx := ktesting.Init(t, initoption.BufferLogs(true))
			logger := tCtx.Logger()

			systemReserved := systemReservedMemory{0: map[v1.ResourceName]uint64{v1.ResourceMemory: 512 * mb}}
			policy, err := NewPolicyStatic(logger, nil, systemReserved, topologymanager.NewFakeManager(logger))
			require.NoError(t, err)

			st := state.NewMemoryState(logger)
			st.SetMachineState(state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{},
				},
			})

			pod := getPod("pod1", "container1", requirementsBurstable)
			err = policy.Allocate(tCtx, st, pod, &pod.Spec.Containers[0], testCase.operation)
			require.NoError(t, err)

			// 取出缓冲日志，检查是否出现"仅支持 add 操作"的跳过提示。
			underlier, ok := logger.GetSink().(ktesting.Underlier)
			if !ok {
				t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
			}
			logs := underlier.GetBuffer().String()
			expectedLog := "Container-level memory allocation skipped, Memory manager with static policy supports only add operation"
			if testCase.skipped {
				if !strings.Contains(logs, expectedLog) {
					t.Errorf("Expected log '%s' not found in logs: %s", expectedLog, logs)
				}
			} else {
				if strings.Contains(logs, expectedLog) {
					t.Errorf("Unexpected log '%s' found in logs: %s", expectedLog, logs)
				}
			}
		})
	}
}

// TestStaticPolicyLifecycleAllocatePod 验证 pod 级分配只在 AddOperation 时执行。
func TestStaticPolicyLifecycleAllocatePod(t *testing.T) {
	testCases := []struct {
		description string
		operation   lifecycle.Operation
		skipped     bool
	}{
		{
			description: "MemoryManager static policy processes AddOperation",
			operation:   lifecycle.AddOperation,
			skipped:     false,
		},
		{
			description: "MemoryManager static policy skips ResizeOperation",
			operation:   lifecycle.ResizeOperation,
			skipped:     true,
		},
		{
			description: "MemoryManager static policy skips empty operation",
			operation:   "",
			skipped:     true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			ktesting.SetDefaultVerbosity(2)
			tCtx := ktesting.Init(t, initoption.BufferLogs(true))
			logger := tCtx.Logger()

			systemReserved := systemReservedMemory{0: map[v1.ResourceName]uint64{v1.ResourceMemory: 512 * mb}}
			policy, err := NewPolicyStatic(logger, nil, systemReserved, topologymanager.NewFakeManager(logger))
			require.NoError(t, err)

			st := state.NewMemoryState(logger)
			st.SetMachineState(state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{},
				},
			})

			pod := getPod("pod1", "container1", requirementsBurstable)
			err = policy.AllocatePod(logger, st, pod, testCase.operation)
			require.NoError(t, err)

			underlier, ok := logger.GetSink().(ktesting.Underlier)
			if !ok {
				t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
			}
			logs := underlier.GetBuffer().String()
			expectedLog := "Pod-level memory allocation skipped, memory manager with static policy supports only add operation"
			if testCase.skipped {
				if !strings.Contains(logs, expectedLog) {
					t.Errorf("Expected log '%s' not found in logs: %s", expectedLog, logs)
				}
			} else {
				if strings.Contains(logs, expectedLog) {
					t.Errorf("Unexpected log '%s' found in logs: %s", expectedLog, logs)
				}
			}
		})
	}
}

// TestStaticPolicyLifecycleGetTopologyHints 验证容器级拓扑提示只在 AddOperation 时计算。
func TestStaticPolicyLifecycleGetTopologyHints(t *testing.T) {
	testCases := []struct {
		description string
		operation   lifecycle.Operation
		skipped     bool
	}{
		{
			description: "MemoryManager static policy processes AddOperation",
			operation:   lifecycle.AddOperation,
			skipped:     false,
		},
		{
			description: "MemoryManager static policy skips ResizeOperation",
			operation:   lifecycle.ResizeOperation,
			skipped:     true,
		},
		{
			description: "MemoryManager static policy skips empty operation",
			operation:   "",
			skipped:     true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			ktesting.SetDefaultVerbosity(2)
			tCtx := ktesting.Init(t, initoption.BufferLogs(true))
			logger := tCtx.Logger()

			systemReserved := systemReservedMemory{0: map[v1.ResourceName]uint64{v1.ResourceMemory: 512 * mb}}
			policy, err := NewPolicyStatic(logger, nil, systemReserved, topologymanager.NewFakeManager(logger))
			require.NoError(t, err)

			st := state.NewMemoryState(logger)
			st.SetMachineState(state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{},
				},
			})

			pod := getPod("pod1", "container1", requirementsBurstable)
			hints := policy.GetTopologyHints(logger, st, pod, &pod.Spec.Containers[0], testCase.operation)
			if hints != nil {
				t.Errorf("Unexpected hints: %v", hints)
			}
			require.NoError(t, err)

			underlier, ok := logger.GetSink().(ktesting.Underlier)
			if !ok {
				t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
			}
			logs := underlier.GetBuffer().String()
			expectedLog := "GetTopologyHints skipped, memory manager with static policy supports only add operation"
			if testCase.skipped {
				if !strings.Contains(logs, expectedLog) {
					t.Errorf("Expected log '%s' not found in logs: %s", expectedLog, logs)
				}
			} else {
				if strings.Contains(logs, expectedLog) {
					t.Errorf("Unexpected log '%s' found in logs: %s", expectedLog, logs)
				}
			}
		})
	}
}

// TestStaticPolicyLifecycleGetPodTopologyHints 验证 pod 级拓扑提示只在 AddOperation 时计算。
func TestStaticPolicyLifecycleGetPodTopologyHints(t *testing.T) {
	testCases := []struct {
		description string
		operation   lifecycle.Operation
		skipped     bool
	}{
		{
			description: "MemoryManager static policy processes AddOperation",
			operation:   lifecycle.AddOperation,
			skipped:     false,
		},
		{
			description: "MemoryManager static policy skips ResizeOperation",
			operation:   lifecycle.ResizeOperation,
			skipped:     true,
		},
		{
			description: "MemoryManager static policy skips empty operation",
			operation:   "",
			skipped:     true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			ktesting.SetDefaultVerbosity(2)
			tCtx := ktesting.Init(t, initoption.BufferLogs(true))
			logger := tCtx.Logger()

			systemReserved := systemReservedMemory{0: map[v1.ResourceName]uint64{v1.ResourceMemory: 512 * mb}}
			policy, err := NewPolicyStatic(logger, nil, systemReserved, topologymanager.NewFakeManager(logger))
			require.NoError(t, err)

			st := state.NewMemoryState(logger)
			st.SetMachineState(state.NUMANodeMap{
				0: &state.NUMANodeState{
					MemoryMap: map[v1.ResourceName]*state.MemoryTable{
						v1.ResourceMemory: {
							Allocatable:    1536 * mb,
							Free:           1536 * mb,
							Reserved:       0,
							SystemReserved: 512 * mb,
							TotalMemSize:   2 * gb,
						},
						hugepages1Gi: {
							Allocatable:    gb,
							Free:           gb,
							Reserved:       0,
							SystemReserved: 0,
							TotalMemSize:   gb,
						},
					},
					Cells: []int{},
				},
			})

			pod := getPod("pod1", "container1", requirementsBurstable)
			hints := policy.GetPodTopologyHints(logger, st, pod, testCase.operation)
			if hints != nil {
				t.Errorf("Unexpected hints: %v", hints)
			}
			require.NoError(t, err)

			underlier, ok := logger.GetSink().(ktesting.Underlier)
			if !ok {
				t.Fatalf("Should have had a ktesting LogSink, got %T", logger.GetSink())
			}
			logs := underlier.GetBuffer().String()
			expectedLog := "GetPodTopologyHints skipped, memory manager with static policy supports only add operation"
			if testCase.skipped {
				if !strings.Contains(logs, expectedLog) {
					t.Errorf("Expected log '%s' not found in logs: %s", expectedLog, logs)
				}
			} else {
				if strings.Contains(logs, expectedLog) {
					t.Errorf("Unexpected log '%s' found in logs: %s", expectedLog, logs)
				}
			}
		})
	}
}
