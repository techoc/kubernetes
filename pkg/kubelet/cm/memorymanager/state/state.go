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

package state

import (
	v1 "k8s.io/api/core/v1"
)

// MemoryTable 保存内存信息。
type MemoryTable struct {
	TotalMemSize   uint64 `json:"total"`
	SystemReserved uint64 `json:"systemReserved"`
	Allocatable    uint64 `json:"allocatable"`
	Reserved       uint64 `json:"reserved"`
	Free           uint64 `json:"free"`
}

// NUMANodeState 保存 NUMA 节点的相关信息。
type NUMANodeState struct {
	// 记录来自该节点的分配次数；容器同时申请内存与大页时会增加两次。
	NumberOfAssignments int `json:"numberOfAssignments"`
	// 保存 NUMA 节点的内存信息。
	MemoryMap map[v1.ResourceName]*MemoryTable `json:"memoryMap"`
	// Cells 保存当前 NUMA 节点以及与其同组的其他节点；
	// 该字段表示当前节点是否被用于跨 NUMA 节点的内存分配；
	// 例如容器绑定 0,1,2 时，节点 0,1,2 的该字段都等于 [0, 1, 2]。
	Cells []int `json:"cells"`
}

// NUMANodeMap 保存每个 NUMA 节点的内存信息。
type NUMANodeMap map[int]*NUMANodeState

// Clone 返回 NUMANodeMap 的深拷贝。
func (nm NUMANodeMap) Clone() NUMANodeMap {
	clone := make(NUMANodeMap)
	for node, s := range nm {
		if s == nil {
			clone[node] = nil
			continue
		}

		clone[node] = &NUMANodeState{}
		clone[node].NumberOfAssignments = s.NumberOfAssignments
		// 切片必须复制，避免副本与原状态共享底层数组。
		clone[node].Cells = append([]int{}, s.Cells...)

		// nil map 保持 nil，避免把"未初始化"变成"空 map"。
		if s.MemoryMap == nil {
			continue
		}

		clone[node].MemoryMap = map[v1.ResourceName]*MemoryTable{}
		for memoryType, memoryTable := range s.MemoryMap {
			clone[node].MemoryMap[memoryType] = &MemoryTable{
				Allocatable:    memoryTable.Allocatable,
				Free:           memoryTable.Free,
				Reserved:       memoryTable.Reserved,
				SystemReserved: memoryTable.SystemReserved,
				TotalMemSize:   memoryTable.TotalMemSize,
			}
		}
	}
	return clone
}

// Block 表示一段内存分配。
type Block struct {
	// NUMAAffinity 包含表示 NUMA 亲和性位掩码的字符串。
	NUMAAffinity []int           `json:"numaAffinity"`
	Type         v1.ResourceName `json:"type"`
	Size         uint64          `json:"size"`
}

// ContainerMemoryAssignments 保存容器的内存分配（Pod -> 容器 -> Block 列表）。
type ContainerMemoryAssignments map[string]map[string][]Block

// Clone 返回 ContainerMemoryAssignments 的深拷贝。
func (as ContainerMemoryAssignments) Clone() ContainerMemoryAssignments {
	clone := make(ContainerMemoryAssignments)
	for pod := range as {
		clone[pod] = make(map[string][]Block)
		for container, blocks := range as[pod] {
			clone[pod][container] = append([]Block{}, blocks...)
		}
	}
	return clone
}

// PodMemoryAssignments 保存 Pod 级内存分配。
type PodMemoryAssignments map[string]PodEntry

// Clone 返回 PodMemoryAssignments 的深拷贝。
func (as PodMemoryAssignments) Clone() PodMemoryAssignments {
	clone := make(PodMemoryAssignments)
	for pod, entry := range as {
		clone[pod] = PodEntry{
			MemoryBlocks: append([]Block{}, entry.MemoryBlocks...),
		}
	}
	return clone
}

// PodEntry 表示 Pod 级内存分配。
type PodEntry struct {
	MemoryBlocks []Block `json:"memoryBlocks,omitempty"`
}

// Reader 接口用于读取当前内存/Pod 分配状态。
type Reader interface {
	// GetMachineState 返回状态中保存的内存映射。
	GetMachineState() NUMANodeMap
	// GetMemoryBlocks 返回容器的内存分配。
	GetMemoryBlocks(podUID string, containerName string) []Block
	// GetPodMemoryBlocks 返回 Pod 级内存分配。
	GetPodMemoryBlocks(podUID string) []Block
	// GetMemoryAssignments 返回 ContainerMemoryAssignments。
	GetMemoryAssignments() ContainerMemoryAssignments
	// GetPodMemoryAssignments 返回所有 Pod 级内存分配。
	GetPodMemoryAssignments() PodMemoryAssignments
}

// writer 接口定义状态写入操作（未导出，仅由 State 组合使用）。
type writer interface {
	// SetMachineState 保存 NUMANodeMap。
	SetMachineState(memoryMap NUMANodeMap)
	// SetMemoryBlocks 保存容器的内存分配。
	SetMemoryBlocks(podUID string, containerName string, blocks []Block)
	// SetPodMemoryBlocks 保存 Pod 级内存分配。
	SetPodMemoryBlocks(podUID string, blocks []Block)
	// SetMemoryAssignments 用参数整体替换 ContainerMemoryAssignments。
	SetMemoryAssignments(assignments ContainerMemoryAssignments)
	// SetPodMemoryAssignments 用参数整体替换 PodMemoryAssignments。
	SetPodMemoryAssignments(assignments PodMemoryAssignments)
	// Delete 从 ContainerMemoryAssignments 删除对应分配。
	Delete(podUID string, containerName string)
	// DeletePod 删除指定 Pod 的全部 Pod 级分配。
	DeletePod(podUID string)
	// ClearState 清空 machineState 与 ContainerMemoryAssignments。
	ClearState()
}

// State 接口提供跟踪与设置内存/Pod 分配的方法。
type State interface {
	Reader
	writer
}
