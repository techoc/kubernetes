/*
Copyright The Kubernetes Authors.

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
	"reflect"
	"testing"

	cadvisorapi "github.com/google/cadvisor/lib/model"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"

	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

// fakeBestEffortAffinity 是 topologymanager.AuthoritativeStore 的测试桩，返回预设 hint。
type fakeBestEffortAffinity struct {
	hint              topologymanager.TopologyHint
	hintAuthoritative bool
}

// GetAffinity 返回预设 hint。
func (f *fakeBestEffortAffinity) GetAffinity(_ klog.Logger, podUID, containerName string) topologymanager.TopologyHint {
	return f.hint
}

// GetPolicy 返回 nil，测试不需要真实策略。
func (f *fakeBestEffortAffinity) GetPolicy() topologymanager.Policy { return nil }

// Name 返回桩名称。
func (f *fakeBestEffortAffinity) Name() string { return "fake" }

// IsHintAuthoritative 模拟 Windows cpuFollowingStore：容器跟随 CPU manager 的 NUMA 决策时，
// hint 具有权威性，内存管理器不得再扩展到其他 NUMA 节点。
func (f *fakeBestEffortAffinity) IsHintAuthoritative(_, _ string) bool { return f.hintAuthoritative }

// bestEffortTestMachineState 返回一个全新的双 NUMA 节点状态：
// 节点 0 仅剩 1Gi（不足以满足 4Gi 请求），节点 1 剩余 10Gi。
func bestEffortTestMachineState() state.NUMANodeMap {
	return state.NUMANodeMap{
		0: &state.NUMANodeState{
			Cells: []int{0},
			MemoryMap: map[v1.ResourceName]*state.MemoryTable{
				v1.ResourceMemory: {
					Allocatable:    1 * gb,
					Free:           1 * gb,
					Reserved:       0,
					SystemReserved: 512 * mb,
					TotalMemSize:   1*gb + 512*mb,
				},
			},
		},
		1: &state.NUMANodeState{
			Cells: []int{1},
			MemoryMap: map[v1.ResourceName]*state.MemoryTable{
				v1.ResourceMemory: {
					Allocatable:    10 * gb,
					Free:           10 * gb,
					Reserved:       0,
					SystemReserved: 0,
					TotalMemSize:   10 * gb,
				},
			},
		},
	}
}

// TestBestEffortPolicyAllocateFollowsCPUManager 验证 Windows BestEffort 策略：
// 容器跟随 CPU manager 的 NUMA 决策（拥有独占 CPU）时跳过 hint 扩展；
// 否则与 static 策略一样扩展节点。
func TestBestEffortPolicyAllocateFollowsCPUManager(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	// 构造与状态一致的机器拓扑。
	machineInfo := &cadvisorapi.MachineInfo{
		Topology: []cadvisorapi.Node{
			{Id: 0, Memory: 1*gb + 512*mb},
			{Id: 1, Memory: 10 * gb},
		},
	}
	// 节点 0 预留 512Mi，使可分配量只剩 1Gi。
	systemReserved := systemReservedMemory{
		0: map[v1.ResourceName]uint64{v1.ResourceMemory: 512 * mb},
	}
	// 请求 4Gi 的 Guaranteed 容器：affinity hint 指向只剩 1Gi 的节点 0，
	// 无法满足请求，因此会走到扩展分支。
	requirements := &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("2"),
			v1.ResourceMemory: resource.MustParse("4Gi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("2"),
			v1.ResourceMemory: resource.MustParse("4Gi"),
		},
	}

	// 两个用例分别覆盖"有独占 CPU"与"无独占 CPU"。
	testCases := []struct {
		name             string
		hasExclusiveCPUs bool
		expectedNUMA     []int
	}{
		{
			name:             "has exclusive CPUs: follows CPU manager, no hint extension",
			hasExclusiveCPUs: true,
			expectedNUMA:     []int{0},
		},
		{
			name:             "no exclusive CPUs: extends like the static policy",
			hasExclusiveCPUs: false,
			expectedNUMA:     []int{0, 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 构造 hint 为节点 0 的亲和性桩。
			affinity := &fakeBestEffortAffinity{
				hint:              topologymanager.TopologyHint{NUMANodeAffinity: newNUMAAffinity(0), Preferred: false},
				hintAuthoritative: tc.hasExclusiveCPUs,
			}
			p, err := NewPolicyBestEffort(logger, machineInfo, systemReserved, affinity)
			if err != nil {
				t.Fatalf("NewPolicyBestEffort() failed: %v", err)
			}

			// 每次用例都使用全新的内存状态，避免分配结果相互影响。
			s := state.NewMemoryState(logger)
			s.SetMachineState(bestEffortTestMachineState())

			pod := getPod("pod1", "container1", requirements)
			if err := p.Allocate(context.Background(), s, pod, &pod.Spec.Containers[0], lifecycle.AddOperation); err != nil {
				t.Fatalf("Allocate() failed: %v", err)
			}

			// 断言恰好生成一个内存块，并检查其 NUMA 亲和性与预期一致。
			blocks := s.GetMemoryBlocks("pod1", "container1")
			if len(blocks) != 1 {
				t.Fatalf("expected exactly 1 memory block, got %d: %+v", len(blocks), blocks)
			}
			if got := blocks[0].NUMAAffinity; !reflect.DeepEqual(got, tc.expectedNUMA) {
				t.Errorf("expected memory block on NUMA nodes %v, got %v", tc.expectedNUMA, got)
			}
		})
	}
}
