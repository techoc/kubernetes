//go:build windows

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

package cm

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/winstats"
	"k8s.io/utils/cpuset"
)

// PreCreateContainer 在 Windows 上根据 CPU/Memory Manager 的分配结果设置 CPU 组亲和性。
func (i *internalContainerLifecycleImpl) PreCreateContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerConfig *runtimeapi.ContainerConfig) error {
	// 特性门控关闭时保持默认行为。
	if !utilfeature.DefaultFeatureGate.Enabled(kubefeatures.WindowsCPUAndMemoryAffinity) {
		return nil
	}

	logger.V(4).Info("PreCreateContainer for Windows")

	// 从 CPU Manager 与 Memory Manager 获取 CPU 和 NUMA 亲和性信息（若已启用）。
	var allocatedCPUs cpuset.CPUSet
	if i.cpuManager != nil {
		allocatedCPUs = i.cpuManager.GetCPUAffinity(string(pod.UID), container.Name)
	}

	var numaNodes sets.Set[int]
	if i.memoryManager != nil {
		numaNodes = i.memoryManager.GetMemoryNUMANodes(logger, pod, container)
	}

	// 汇总所选 NUMA 节点关联的所有 CPU。
	// 汇总所选 NUMA 节点关联的所有 CPU。
	var allNumaNodeCPUs []winstats.GroupAffinity
	for _, numaNode := range sets.List(numaNodes) {
		affinities, err := winstats.GetCPUsforNUMANode(uint16(numaNode))
		if err != nil {
			return fmt.Errorf("failed to get CPUs for NUMA node %d: %v", numaNode, err)
		}
		allNumaNodeCPUs = append(allNumaNodeCPUs, affinities...)
	}

	// 由 CPU Manager 的精确分配与 NUMA 关联 CPU 计算最终 CPU 集合。
	var finalCPUSet = computeFinalCpuSet(allocatedCPUs, allNumaNodeCPUs)

	logger.V(4).Info("Setting CPU affinity", "affinity", finalCPUSet, "container", container.Name, "pod", pod.UID)

	// 把 CPU 组亲和性写入容器配置。
	// 仅在有最终 CPU 集合时写入亲和性。
	if finalCPUSet != nil {
		var cpusToGroupAffinities []*runtimeapi.WindowsCpuGroupAffinity
		for group, mask := range groupMasks(finalCPUSet) {

			cpusToGroupAffinities = append(cpusToGroupAffinities, &runtimeapi.WindowsCpuGroupAffinity{
				CpuGroup: uint32(group),
				CpuMask:  uint64(mask),
			})
		}
		containerConfig.Windows.Resources.AffinityCpus = cpusToGroupAffinities
	}

	// 未选中任何 CPU 时返回 nil。
	return nil
}

// computeFinalCpuSet 根据 CPU Manager 与 Memory Manager 的结果计算最终使用的 CPU 集合。
// 该函数被单独提取出来以便测试。
//
// CPU Manager 已分配 CPU 时始终直接使用其分配结果。CPU Manager 的分配是权威的：
// 它已经考虑了 Topology Manager 的拓扑提示并选定了具体 CPU。再用 NUMA CPU 做并集
// （旧实现中的 "Case 3"）会导致记账与实际执行不一致（reconcileState 会覆盖任何扩展），
// 并且可能把专属于其他容器的 CPU 纳入进来，破坏 CPU 隔离保证。
//
// 只有 Memory Manager 生效时，才使用 NUMA 节点关联的 CPU 通过 CPU 亲和性提供内存局部性，
// 因为 Windows 没有直接的 NUMA 内存绑定机制。
func computeFinalCpuSet(allocatedCPUs cpuset.CPUSet, allNumaNodeCPUs []winstats.GroupAffinity) sets.Set[int] {
	if !allocatedCPUs.IsEmpty() {
		// CPU Manager 已分配 CPU，直接使用。
		// 这覆盖了仅启用 CPU Manager 以及两个 Manager 同时启用的情况。
		return sets.New[int](allocatedCPUs.List()...)
	} else if len(allNumaNodeCPUs) > 0 {
		// 仅启用 Memory Manager，使用所选 NUMA 节点关联的 CPU。
		return computeCPUSet(allNumaNodeCPUs)
	}
	return nil
}

// computeCPUSet 把 GroupAffinity 列表转换为 CPU ID 集合。
func computeCPUSet(affinities []winstats.GroupAffinity) sets.Set[int] {
	cpuSet := sets.New[int]()
	for _, affinity := range affinities {
		// 每个 CPU 组用 64 位掩码表示，按位展开为具体 CPU ID。
		for i := 0; i < 64; i++ {
			if (affinity.Mask>>i)&1 == 1 {
				cpuID := int(affinity.Group)*64 + i
				cpuSet.Insert(cpuID)
			}
		}
	}
	return cpuSet
}

// groupMasks 把 CPU ID 集合转换为「组号 -> 掩码」的表示。
func groupMasks(cpuSet sets.Set[int]) map[int]uint64 {
	groupMasks := make(map[int]uint64)
	for cpu := range cpuSet {
		group := cpu / 64
		mask := uint64(1) << (cpu % 64)
		groupMasks[group] |= mask
	}
	return groupMasks
}
