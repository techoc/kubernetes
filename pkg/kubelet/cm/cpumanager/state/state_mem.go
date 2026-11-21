/*
Copyright 2017 The Kubernetes Authors.

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
	"maps"
	"sync"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/cpuset"
)

// stateMemory 是 State 的内存实现，用读写锁保护 CPU 分配、基线与默认 CPU 集合。
type stateMemory struct {
	sync.RWMutex
	logger         klog.Logger
	assignments    ContainerCPUAssignments
	baselines      ContainerCPUBaselines
	podAssignments PodCPUAssignments
	defaultCPUSet  cpuset.CPUSet
}

// 编译期断言：stateMemory 必须实现 State 接口。
var _ State = &stateMemory{}

// NewMemoryState 创建用于跟踪 CPU/Pod 分配的内存 State。
func NewMemoryState(logger klog.Logger) State {
	// 这里保存 logger 实例，以与 CheckpointState 接口保持一致；
	// 由于会写 checkpoint，使用相对昂贵的 "WithName" 为日志加名称是值得的。
	logger = klog.LoggerWithName(logger, "CPUManager state memory")
	logger.Info("Initialized")
	return &stateMemory{
		logger:         logger,
		assignments:    ContainerCPUAssignments{},
		baselines:      ContainerCPUBaselines{},
		podAssignments: PodCPUAssignments{},
		defaultCPUSet:  cpuset.New(),
	}
}

// GetBaselineCPUSet 返回容器在准入时记录的基线 CPU 集合。
func (s *stateMemory) GetBaselineCPUSet(podUID string, containerName string) (cpuset.CPUSet, bool) {
	s.RLock()
	defer s.RUnlock()

	entry, exists := s.baselines[podUID][containerName]
	// 返回克隆值，避免调用方修改内部状态。
	return entry.Baseline.Clone(), exists
}

// GetCPUSet 返回容器当前的 CPU 集合。
func (s *stateMemory) GetCPUSet(podUID string, containerName string) (cpuset.CPUSet, bool) {
	s.RLock()
	defer s.RUnlock()

	res, ok := s.assignments[podUID][containerName]
	return res.Clone(), ok
}

// GetDefaultCPUSet 返回默认（共享池）CPU 集合的副本。
func (s *stateMemory) GetDefaultCPUSet() cpuset.CPUSet {
	s.RLock()
	defer s.RUnlock()

	return s.defaultCPUSet.Clone()
}

// GetCPUSetOrDefault 返回容器的 CPU 集合；未分配时回退到默认 CPU 集合。
func (s *stateMemory) GetCPUSetOrDefault(podUID string, containerName string) cpuset.CPUSet {
	if res, ok := s.GetCPUSet(podUID, containerName); ok {
		return res
	}
	return s.GetDefaultCPUSet()
}

// GetCPUAssignments 返回全部容器 CPU 分配的深拷贝。
func (s *stateMemory) GetCPUAssignments() ContainerCPUAssignments {
	s.RLock()
	defer s.RUnlock()
	return s.assignments.Clone()
}

// GetPodCPUSet 返回 Pod 级别的 CPU 集合。
func (s *stateMemory) GetPodCPUSet(podUID string) (cpuset.CPUSet, bool) {
	s.RLock()
	defer s.RUnlock()
	res, ok := s.podAssignments[podUID]
	return res.CPUSet.Clone(), ok
}

// GetPodCPUAssignments 返回 Pod 级 CPU 分配的副本。
func (s *stateMemory) GetPodCPUAssignments() PodCPUAssignments {
	s.RLock()
	defer s.RUnlock()
	clone := make(PodCPUAssignments)
	// 复制一份 map，避免调用方直接修改内部映射结构。
	maps.Copy(clone, s.podAssignments)
	return clone
}

// GetCPUBaselines 返回全部容器 CPU 基线的深拷贝。
func (s *stateMemory) GetCPUBaselines() ContainerCPUBaselines {
	s.RLock()
	defer s.RUnlock()
	return s.baselines.Clone()
}

// SetCPUSet 设置容器 CPU 集合；开启原地扩缩容特性时同时记录基线。
func (s *stateMemory) SetCPUSet(podUID string, containerName string, cset cpuset.CPUSet) {
	s.Lock()
	defer s.Unlock()
	// Pod 首次出现时初始化其容器映射。
	if _, ok := s.assignments[podUID]; !ok {
		s.assignments[podUID] = make(map[string]cpuset.CPUSet)
	}

	s.assignments[podUID][containerName] = cset

	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingExclusiveCPUs) {
		if _, ok := s.baselines[podUID]; !ok {
			s.baselines[podUID] = make(map[string]ContainerCPUBaseline)
		}

		// 基线只在首次分配时记录，代表准入时的原始 CPU 集合。
		if _, ok := s.baselines[podUID][containerName]; !ok {
			s.baselines[podUID][containerName] = ContainerCPUBaseline{Baseline: cset.Clone()}
		}

		baselineSet := s.baselines[podUID][containerName].Baseline
		s.logger.Info("Updated desired CPUSet", "podUID", podUID, "containerName", containerName, "CPUSet", cset, "Baseline CPUSet", baselineSet)
	} else {
		s.logger.Info("Updated desired CPUSet", "podUID", podUID, "containerName", containerName, "CPUSet", cset)
	}
}

// SetDefaultCPUSet 设置默认（共享池）CPU 集合。
func (s *stateMemory) SetDefaultCPUSet(cset cpuset.CPUSet) {
	s.Lock()
	defer s.Unlock()

	s.defaultCPUSet = cset
	s.logger.Info("Updated default CPUSet", "cpuSet", cset)
}

// SetPodCPUSet 设置 Pod 级 CPU 集合。
func (s *stateMemory) SetPodCPUSet(podUID string, cset cpuset.CPUSet) {
	s.Lock()
	defer s.Unlock()

	podEntry := s.podAssignments[podUID]
	podEntry.CPUSet = cset
	s.podAssignments[podUID] = podEntry
	s.logger.Info("Updated pod CPUSet", "podUID", podUID, "cpuSet", cset)
}

// SetCPUAssignments 用给定分配整体替换容器 CPU 分配。
func (s *stateMemory) SetCPUAssignments(a ContainerCPUAssignments) {
	s.Lock()
	defer s.Unlock()

	s.assignments = a.Clone()
	s.logger.Info("Updated CPUSet assignments", "assignments", a)
}

// SetPodCPUAssignments 用给定分配整体替换 Pod 级 CPU 分配。
func (s *stateMemory) SetPodCPUAssignments(a PodCPUAssignments) {
	s.Lock()
	defer s.Unlock()

	s.podAssignments = make(PodCPUAssignments)
	maps.Copy(s.podAssignments, a)
	s.logger.Info("Updated pod CPUSet assignments", "assignments", a)
}

// SetCPUBaselines 用给定基线整体替换容器 CPU 基线。
func (s *stateMemory) SetCPUBaselines(a ContainerCPUBaselines) {
	s.Lock()
	defer s.Unlock()

	s.baselines = a.Clone()
	s.logger.Info("Updated CPUSet baselines", "baselines", a)
}

// Delete 删除容器级分配与基线；Pod 下已无容器时一并删除 Pod 条目。
func (s *stateMemory) Delete(podUID string, containerName string) {
	s.Lock()
	defer s.Unlock()

	delete(s.assignments[podUID], containerName)
	// 容器全部删除后清理 Pod 条目，避免残留空映射。
	if len(s.assignments[podUID]) == 0 {
		delete(s.assignments, podUID)
	}
	s.logger.V(2).Info("Deleted CPUSet assignment", "podUID", podUID, "containerName", containerName)

	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingExclusiveCPUs) {
		delete(s.baselines[podUID], containerName)
		// 基线同样在 Pod 下无容器时清理。
		if len(s.baselines[podUID]) == 0 {
			delete(s.baselines, podUID)
		}
		s.logger.V(2).Info("Deleted CPUSet baseline entry", "podUID", podUID, "containerName", containerName)
	}
}

// DeletePod 删除指定 Pod 的 Pod 级 CPU 分配，不影响容器级分配。
func (s *stateMemory) DeletePod(podUID string) {
	s.Lock()
	defer s.Unlock()

	delete(s.podAssignments, podUID)
	s.logger.V(2).Info("Deleted pod", "podUID", podUID)
}

// ClearState 清空所有内存状态，恢复为初始值。
func (s *stateMemory) ClearState() {
	s.Lock()
	defer s.Unlock()

	s.defaultCPUSet = cpuset.New()
	s.assignments = make(ContainerCPUAssignments)
	s.baselines = make(ContainerCPUBaselines)
	s.podAssignments = make(PodCPUAssignments)
	s.logger.V(2).Info("Cleared state")
}
