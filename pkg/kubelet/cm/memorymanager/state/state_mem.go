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
	"sync"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
)

// stateMemory 是基于内存的 State 实现，RWMutex 保护所有字段。
type stateMemory struct {
	sync.RWMutex
	logger         klog.Logger
	assignments    ContainerMemoryAssignments
	podAssignments PodMemoryAssignments
	machineState   NUMANodeMap
}

// 编译期断言：stateMemory 实现 State 接口。
var _ State = &stateMemory{}

// NewMemoryState 创建用于跟踪内存/Pod 分配的状态。
func NewMemoryState(logger klog.Logger) State {
	logger.Info("Initializing new in-memory state store")

	// 启用 Pod 级资源管理器时同时初始化 Pod 级分配映射。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) {
		return &stateMemory{
			logger:         logger,
			assignments:    ContainerMemoryAssignments{},
			podAssignments: PodMemoryAssignments{},
			machineState:   NUMANodeMap{},
		}
	}

	return &stateMemory{
		logger:       logger,
		assignments:  ContainerMemoryAssignments{},
		machineState: NUMANodeMap{},
	}
}

// GetMachineState 返回状态中保存的内存映射（副本）。
func (s *stateMemory) GetMachineState() NUMANodeMap {
	s.RLock()
	defer s.RUnlock()

	// 返回副本，避免调用方在锁外修改内部状态。
	return s.machineState.Clone()
}

// GetMemoryBlocks 返回容器的内存分配（副本）。
func (s *stateMemory) GetMemoryBlocks(podUID string, containerName string) []Block {
	s.RLock()
	defer s.RUnlock()

	if res, ok := s.assignments[podUID][containerName]; ok {
		// 复制切片，避免共享底层数组。
		return append([]Block{}, res...)
	}
	return nil
}

// GetMemoryAssignments 返回 ContainerMemoryAssignments（深拷贝）。
func (s *stateMemory) GetMemoryAssignments() ContainerMemoryAssignments {
	s.RLock()
	defer s.RUnlock()

	return s.assignments.Clone()
}

// GetPodMemoryBlocks 返回 Pod 级内存分配（副本）。
func (s *stateMemory) GetPodMemoryBlocks(podUID string) []Block {
	s.RLock()
	defer s.RUnlock()

	if res, ok := s.podAssignments[podUID]; ok {
		return append([]Block{}, res.MemoryBlocks...)
	}
	return nil
}

// GetPodMemoryAssignments 返回所有 Pod 级内存分配（深拷贝）。
func (s *stateMemory) GetPodMemoryAssignments() PodMemoryAssignments {
	s.RLock()
	defer s.RUnlock()

	return s.podAssignments.Clone()
}

// SetMachineState 保存 NUMANodeMap（深拷贝）。
func (s *stateMemory) SetMachineState(nodeMap NUMANodeMap) {
	s.Lock()
	defer s.Unlock()

	// 保存副本，防止调用方后续修改影响状态。
	s.machineState = nodeMap.Clone()
	s.logger.Info("Updated machine memory state")
}

// SetMemoryBlocks 保存容器的内存分配（复制切片）。
func (s *stateMemory) SetMemoryBlocks(podUID string, containerName string, blocks []Block) {
	s.Lock()
	defer s.Unlock()

	// 首次写入时初始化内层 map。
	if _, ok := s.assignments[podUID]; !ok {
		s.assignments[podUID] = map[string][]Block{}
	}

	s.assignments[podUID][containerName] = append([]Block{}, blocks...)
	s.logger.Info("Updated memory state", "podUID", podUID, "containerName", containerName)
}

// SetMemoryAssignments 用参数整体替换 ContainerMemoryAssignments（深拷贝）。
func (s *stateMemory) SetMemoryAssignments(assignments ContainerMemoryAssignments) {
	s.Lock()
	defer s.Unlock()

	s.assignments = assignments.Clone()
	s.logger.V(5).Info("Updated Memory assignments", "assignments", assignments)
}

// SetPodMemoryAssignments 用参数整体替换 PodMemoryAssignments（深拷贝）。
func (s *stateMemory) SetPodMemoryAssignments(assignments PodMemoryAssignments) {
	s.Lock()
	defer s.Unlock()

	s.podAssignments = assignments.Clone()
	s.logger.V(5).Info("Updated Pod Memory assignments", "assignments", assignments)
}

// SetPodMemoryBlocks 保存 Pod 级内存分配。
func (s *stateMemory) SetPodMemoryBlocks(podUID string, blocks []Block) {
	s.Lock()
	defer s.Unlock()

	// 取出值副本、修改后再写回，保证 map 更新语义。
	podAssigments := s.podAssignments[podUID]
	podAssigments.MemoryBlocks = append([]Block{}, blocks...)
	s.podAssignments[podUID] = podAssigments
	s.logger.Info("Updated pod memory state", "podUID", podUID)
}

// Delete 从 ContainerMemoryAssignments 删除对应分配。
func (s *stateMemory) Delete(podUID string, containerName string) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.assignments[podUID]; !ok {
		return
	}

	// 容器没有剩余分配时一并删除 Pod 条目，避免泄漏。
	delete(s.assignments[podUID], containerName)
	if len(s.assignments[podUID]) == 0 {
		delete(s.assignments, podUID)
	}
	s.logger.V(2).Info("Deleted memory assignment", "podUID", podUID, "containerName", containerName)
}

// DeletePod 删除指定 Pod 的 Pod 级分配，不影响容器级分配。
func (s *stateMemory) DeletePod(podUID string) {
	s.Lock()
	defer s.Unlock()

	delete(s.podAssignments, podUID)
	s.logger.V(2).Info("Deleted pod memory assignment", "podUID", podUID)
}

// ClearState 清空 machineState 与 ContainerMemoryAssignments。
func (s *stateMemory) ClearState() {
	s.Lock()
	defer s.Unlock()

	s.machineState = NUMANodeMap{}
	s.assignments = make(ContainerMemoryAssignments)
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) {
		s.podAssignments = make(PodMemoryAssignments)
	}
	s.logger.V(2).Info("Cleared state")
}
