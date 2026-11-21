//go:build windows

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

package cm

import (
	cadvisorapi "github.com/google/cadvisor/lib/model"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/utils/cpuset"

	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
)

// 本文件实现 Windows 专有的"内存跟随 CPU"NUMA 放置机制。
//
// Windows 没有 cpuset.mems：内存无法绑定到 NUMA 节点，内核会从线程当前运行 CPU
// 所在的 NUMA 节点分配内存页。因此 CPU 亲和性是唯一真正生效的 NUMA 手段，
// 而它由 CPU Manager 掌管。为了让 Memory Manager 的按 NUMA 记账与操作系统实际行为
// 一致，并避免 computeFinalCpuSet 中的 CPU 亲和性并集，Memory Manager 必须镜像
// CPU Manager 的 NUMA 决策，而不是自行选择节点。
//
// cpuFollowingStore（见下）就是该机制：作为 Topology Manager Store 的包装器，
// 在 container_manager_windows.go 中注入为 Memory Manager 的 affinity store。
// 它让 Memory Manager 回读容器独占 CPU 所在的 NUMA 节点，并通过 IsHintAuthoritative
// 报告该 hint 是否具有权威性，即是否存在可跟随的 CPU 决策——若不存在
// （CPU Manager 策略为 none，或容器共享 CPU/非 Guaranteed），Memory Manager 回退到自己的计算。

// 当任一 HintProvider 的 Allocate() 被调用时，Topology Manager 已从所有 provider
// （包括 CPU Manager）收集 hint，并保存了合并后的最佳亲和性。当前注册顺序下
// CPU Manager 的 Allocate 先于 Memory Manager 执行，但正确性不依赖该顺序——
// GetAffinity 读取的是已提交的独占 CPU，一旦准入流程进入 Allocate 阶段这些信息就可用。
// 若准入流程改变，此处需要重新审视。
type cpuFollowingStore struct {
	// Store 是被包装的 Topology Manager；GetPolicy 与 Name 由它提升而来，
	// 当没有可跟随的决策时 GetAffinity 也回退到它。
	topologymanager.Store
	cpuManager cpumanager.Manager
	// cpuToNode 把逻辑 CPU 映射到其所属 NUMA 节点，启动时根据机器拓扑构建一次，
	// 与 CPU Manager 使用的映射一致。
	cpuToNode map[int]int
}

// 编译期断言：cpuFollowingStore 实现 AuthoritativeStore 接口。
var _ topologymanager.AuthoritativeStore = &cpuFollowingStore{}

// newCPUFollowingStore 基于基础 Topology Manager store、CPU Manager 与机器拓扑构建包装器。
func newCPUFollowingStore(base topologymanager.Store, cpuManager cpumanager.Manager, machineInfo *cadvisorapi.MachineInfo) *cpuFollowingStore {
	cpuToNode := make(map[int]int)
	for _, node := range machineInfo.Topology {
		for _, core := range node.Cores {
			for _, cpu := range core.Threads {
				cpuToNode[cpu] = node.Id
			}
		}
	}
	return &cpuFollowingStore{
		Store:      base,
		cpuManager: cpuManager,
		cpuToNode:  cpuToNode,
	}
}

// GetAffinity 返回 Memory Manager 对该容器应使用的 NUMA 亲和性。
// 当容器拥有独占 CPU 时，GetAffinity 重建一个更窄的 hint，精确镜像 CPU Manager 的
// NUMA 决策（即这些 CPU 所属的 NUMA 节点集合）。这样在 Windows 上
// （操作系统从运行 CPU 所在 NUMA 节点分配内存页）内存记账与 CPU 亲和性保持一致。
//
// 重建的 hint 可能比 Topology Manager 保存的合并最佳 hint 更窄
// （后者在计入设备管理器 hint 时可能跨越更多 NUMA 节点）。两者不一致时会记录日志，
// 便于观察这种差异；GA 前需要重新审视。
//
// 若容器没有独占 CPU（共享池/非 Guaranteed）或 CPU 无法映射到 NUMA 节点，
// 则回退到被包装的 store。
func (s *cpuFollowingStore) GetAffinity(logger klog.Logger, podUID string, containerName string) topologymanager.TopologyHint {
	base := s.Store.GetAffinity(logger, podUID, containerName)

	exclusiveCPUs := s.cpuManager.GetExclusiveCPUs(podUID, containerName)
	if exclusiveCPUs.IsEmpty() {
		// 没有可跟随的决策：该容器未获得独占 CPU，保留 Topology Manager 自己的 hint。
		logger.V(5).Info("GetAffinity: no exclusive CPUs, using stored hint", "podUID", podUID, "containerName", containerName, "hint", base.NUMANodeAffinity)
		return base
	}

	// 把独占 CPU 映射为 NUMA 节点掩码；映射失败则回退。
	mask := s.numaMaskForCPUs(exclusiveCPUs)
	if mask == nil || mask.IsEmpty() {
		logger.V(4).Info("GetAffinity: exclusive CPUs not mappable to NUMA nodes, falling back to stored hint", "podUID", podUID, "containerName", containerName, "exclusiveCPUs", exclusiveCPUs, "hint", base.NUMANodeAffinity)
		return base
	}

	// 用 CPU Manager 的决策重建 hint，Preferred 沿用原 hint。
	recomputed := topologymanager.TopologyHint{
		NUMANodeAffinity: mask,
		Preferred:        base.Preferred,
	}

	// 重建结果与原 hint 不一致时记录日志，方便定位 CPU/内存放置分歧。
	if !recomputed.IsEqual(base) {
		logger.V(4).Info("GetAffinity: recomputed hint from CPU Manager differs from stored topology hint", "podUID", podUID, "containerName", containerName, "exclusiveCPUs", exclusiveCPUs, "recomputedHint", recomputed.NUMANodeAffinity, "storedHint", base.NUMANodeAffinity)
	} else {
		logger.V(5).Info("GetAffinity: recomputed hint matches stored hint", "podUID", podUID, "containerName", containerName, "hint", recomputed.NUMANodeAffinity)
	}

	return recomputed
}

// IsHintAuthoritative 报告给定容器的亲和性 hint 是否具有权威性、
// 必须原样使用（不得扩展到其他 NUMA 节点）。
// 在 Windows 上，当且仅当 CPU manager 为容器分配了独占 CPU 时为 true：
// 此时内存管理器跟随 CPU manager 的 NUMA 决策（保持同步，不扩展）。
// 没有独占 CPU 时（CPU manager 策略为 none，或共享/非 Guaranteed 容器），
// hint 不具权威性，内存管理器自行计算。
func (s *cpuFollowingStore) IsHintAuthoritative(podUID, containerName string) bool {
	return s.cpuManager.GetExclusiveCPUs(podUID, containerName).Size() > 0
}

// numaMaskForCPUs 返回包含给定 CPU 的 NUMA 节点集合。
func (s *cpuFollowingStore) numaMaskForCPUs(cpus cpuset.CPUSet) bitmask.BitMask {
	nodeSet := sets.New[int]()
	for _, cpu := range cpus.List() {
		if node, ok := s.cpuToNode[cpu]; ok {
			nodeSet.Insert(node)
		}
	}
	if nodeSet.Len() == 0 {
		return nil
	}
	mask, err := bitmask.NewBitMask(sets.List(nodeSet)...)
	if err != nil {
		return nil
	}
	return mask
}
