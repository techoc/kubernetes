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
	"sort"
	"strings"

	cadvisorapi "github.com/google/cadvisor/lib/model"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	corehelper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/admission"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	cmqos "k8s.io/kubernetes/pkg/kubelet/cm/qos"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
)

// PolicyTypeStatic 是静态内存管理策略的类型标识。
const PolicyTypeStatic policyType = "Static"

// systemReservedMemory 按 NUMA 节点记录为系统预留的内存量。
type systemReservedMemory map[int]map[v1.ResourceName]uint64

// reusableMemory 按 Pod UID -> NUMA 亲和性字符串 -> 资源名记录 init 容器可复用的内存。
type reusableMemory map[string]map[string]map[v1.ResourceName]uint64

// staticPolicy 是 policy 接口的静态策略实现。
type staticPolicy struct {
	// machineInfo 保存机器内存相关信息。
	machineInfo *cadvisorapi.MachineInfo
	// systemReserved 保存为 kube 预留的内存。
	systemReserved systemReservedMemory
	// affinity 是拓扑管理器引用，用于获取容器的拓扑亲和性。
	affinity topologymanager.Store
	// initContainersReusableMemory 保存 init 容器已分配、可被后续容器复用的内存。
	// 可重启 init 容器的内存不计入此处，因为它不可复用。
	initContainersReusableMemory reusableMemory
	// skipExtend 为 true 时，即使提示本身不满足容器的内存请求，也不把
	// 拓扑管理器提示扩展到额外的 NUMA 节点。Linux 静态策略始终保持 false。
	// Windows BestEffort 策略会按容器设置它（见 policy_best_effort.go），
	// 让内存放置跟随 CPU 管理器的 NUMA 决策：Windows 没有 cpuset.mems，
	// 因此提示不能扩展到 CPU 管理器独占 CPU 的节点之外。
	skipExtend bool
}

// 编译期断言 staticPolicy 实现了 Policy 接口。
var _ Policy = &staticPolicy{}

// NewPolicyStatic 返回新的静态策略实例。
func NewPolicyStatic(logger klog.Logger, machineInfo *cadvisorapi.MachineInfo, reserved systemReservedMemory, affinity topologymanager.Store) (Policy, error) {
	var totalSystemReserved uint64
	for _, node := range reserved {
		if _, ok := node[v1.ResourceMemory]; !ok {
			continue
		}
		totalSystemReserved += node[v1.ResourceMemory]
	}

	// 静态策略按预留量切分可分配内存，没有系统预留就无法安全分配。
	if totalSystemReserved <= 0 {
		return nil, fmt.Errorf("[memorymanager] you should specify the system reserved memory")
	}

	return &staticPolicy{
		machineInfo:                  machineInfo,
		systemReserved:               reserved,
		affinity:                     affinity,
		initContainersReusableMemory: reusableMemory{},
	}, nil
}

// Name 返回策略名称。
func (p *staticPolicy) Name() string {
	return string(PolicyTypeStatic)
}

// Start 在 kubelet 启动时校验持久化状态是否仍与当前机器状态一致。
func (p *staticPolicy) Start(logger klog.Logger, s state.State) error {
	if err := p.validateState(logger, s); err != nil {
		logger.Error(err, "Invalid state, please drain node and remove policy state file")
		return err
	}
	return nil
}

// validatePodScopeResources 检查"共享池为空"的情况：当所有独占容器的内存请求
// 之和耗尽整个 Pod 级预算时，需要共享池的容器将分不到任何内存并因此启动失败，
// 这种配置必须被拒绝。
func (p *staticPolicy) validatePodScopeResources(logger klog.Logger, pod *v1.Pod) error {
	podTotalMemory, err := getPodRequestedResources(logger, pod)
	if err != nil {
		return err
	}

	hasSharedLongRunningContainers := false
	// 汇总所有生命周期与 Pod 等长的容器（主容器和可重启 init 容器）的内存请求，
	// 以判断可能挤占共享池的独占内存总量。
	sumOfLongRunningExclusiveMemory := make(map[v1.ResourceName]uint64)

	// 检查普通 init 容器是否还有共享池可用。
	for _, container := range pod.Spec.InitContainers {
		containerReqs, err := getContainerRequestedResources(logger, pod, &container)
		if err != nil {
			return err
		}

		if !podutil.IsRestartableInitContainer(&container) {
			if len(containerReqs) == 0 {
				// 若存在需要共享池的普通 init 容器，就必须确保该池非空；
				// 当 Pod 预算已被 sidecar 占满时池会为空。
				for resourceName, poolSize := range podTotalMemory {
					if sumOfLongRunningExclusiveMemory[resourceName] >= poolSize {
						return fmt.Errorf("pod rejected, pod has shared init containers but no %s available for them", resourceName)
					}
				}
			}
		} else {
			// 存在可重启 init 容器时，需记录其资源以参与共享池为空的判断。
			if len(containerReqs) == 0 {
				hasSharedLongRunningContainers = true
			} else {
				for resourceName, reqAmount := range containerReqs {
					sumOfLongRunningExclusiveMemory[resourceName] += reqAmount
				}
			}
		}
	}

	// 检查并发运行的容器（应用容器和可重启 init 容器）是否还有共享池可用。
	for _, container := range pod.Spec.Containers {
		containerReqs, err := getContainerRequestedResources(logger, pod, &container)
		if err != nil {
			return err
		}

		if len(containerReqs) == 0 {
			hasSharedLongRunningContainers = true
		} else {
			for resourceName, val := range containerReqs {
				sumOfLongRunningExclusiveMemory[resourceName] += val
			}
		}
	}

	// 若存在需要共享池的普通容器，则确保该池非空；
	// 当 sidecar 和带独占资源的主容器耗尽 Pod 预算时池会为空。
	if hasSharedLongRunningContainers {
		for resourceName, totalPodResource := range podTotalMemory {
			if sumOfLongRunningExclusiveMemory[resourceName] >= totalPodResource {
				return fmt.Errorf("pod rejected, sum of exclusive container %s requests equals pod budget, leaving no memory for shared containers", resourceName)
			}
		}
	}

	return nil
}

// AllocatePod 是 Pod 级资源分配的入口。
// 拓扑管理器的 pod 作用域准入处理器对每个 Pod 调用一次。
// 这里为整个 Pod 分配一块 NUMA 对齐的内存"气泡"，Pod 内所有容器共享这一 NUMA 绑定。
func (p *staticPolicy) AllocatePod(logger klog.Logger, s state.State, pod *v1.Pod, operation lifecycle.Operation) (rerr error) {
	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod), "operation", operation)
	logger.V(4).Info("AllocatePod called for pod-level managed pod")
	if operation != lifecycle.AddOperation {
		logger.V(2).Info("Pod-level memory allocation skipped, memory manager with static policy supports only add operation")
		return nil
	}
	podUID := string(pod.UID)
	qos := v1qos.GetPodQOS(pod)
	if qos != v1.PodQOSGuaranteed {
		return nil
	}

	// 1. 计算内存资源。
	podTotalMemory, err := getPodRequestedResources(logger, pod)
	if err != nil {
		logger.Error(err, "Failed to get pod requested resources", "podUID", podUID)
		return nil
	}
	logger.V(4).Info("Calculated total pod memory", "podTotalMemory", podTotalMemory)

	metrics.MemoryManagerPinningRequestTotal.Inc()
	defer func() {
		if rerr != nil {
			metrics.MemoryManagerPinningErrorsTotal.Inc()
			metrics.ResourceManagerAllocationErrorsTotal.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerPod).Inc()
		}
	}()

	// 2. 校验"共享池为空"的情况。
	// 即使提示生成阶段已检查过，这里也必须重新评估：当拓扑管理器运行在
	// "best-effort" 或 "none" 策略下时，即使 GetPodTopologyHints 返回空提示列表
	// 也仍会准入该 Pod。这里显式拒绝，避免 kubelet 为共享容器创建非法 cgroup
	// （例如空的 cpuset.mems）。
	if err := p.validatePodScopeResources(logger, pod); err != nil {
		return admission.NewEmptyPodSharedPoolError(err)
	}

	// 3. 处理提示与 NUMA 对齐。
	machineState := s.GetMachineState()
	// 所有容器共享同一份 Pod 级提示，取第一个容器的名称即可。
	bestHint := p.affinity.GetAffinity(logger, podUID, append(pod.Spec.InitContainers, pod.Spec.Containers...)[0].Name)
	if bestHint.NUMANodeAffinity == nil {
		defaultHint, err := p.getDefaultHint(machineState, pod, podTotalMemory)
		if err != nil {
			return err
		}
		if !defaultHint.Preferred && bestHint.Preferred {
			return fmt.Errorf("[memorymanager] failed to find the default preferred hint")
		}
		bestHint = *defaultHint
	}

	if !isAffinitySatisfyRequest(machineState, bestHint.NUMANodeAffinity, podTotalMemory) {
		extendedHint, err := p.extendTopologyManagerHint(machineState, pod, podTotalMemory, bestHint.NUMANodeAffinity)
		if err != nil {
			return err
		}
		if !extendedHint.Preferred && bestHint.Preferred {
			return fmt.Errorf("[memorymanager] failed to find the extended preferred hint")
		}
		bestHint = *extendedHint
	}

	if isAffinityViolatingNUMAAllocations(machineState, bestHint.NUMANodeAffinity) {
		return fmt.Errorf("[memorymanager] preferred hint violates NUMA node allocation")
	}

	maskBits := bestHint.NUMANodeAffinity.GetBits()

	// 4. 切分 Pod 的内存分配，并正确处理 init 容器的复用。
	exclusiveMemory := make(map[string][]state.Block)
	sidecarMemory := make(map[v1.ResourceName]uint64)

	// 先遍历所有 init 容器并分配其内存。
	for _, c := range pod.Spec.InitContainers {
		reqs, err := getContainerRequestedResources(logger, pod, &c)
		if err != nil {
			return err
		}

		metrics.MemoryManagerPinningRequestTotal.Inc()

		// 该 init 容器可用的池是整个 Pod 分配量减去 sidecar 已占用的部分。
		runnablePool := make(map[v1.ResourceName]uint64)
		for resourceName, total := range podTotalMemory {
			runnablePool[resourceName] = total - sidecarMemory[resourceName]
		}

		if len(reqs) == 0 {
			// 可重启 init 容器使用 Pod 的主共享池。
			if podutil.IsRestartableInitContainer(&c) {
				continue
			}

			// 不可重启的 init 容器可使用整个 Pod 池减去此刻之前 sidecar 的独占内存。
			blocks := []state.Block{}
			for resourceName, req := range runnablePool {
				if req > 0 {
					blocks = append(blocks, state.Block{
						NUMAAffinity: maskBits,
						Size:         req,
						Type:         resourceName,
					})
				}
			}
			exclusiveMemory[c.Name] = blocks
			metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerSharedPod).Inc()
			continue
		}

		// 分配内存块。
		blocks := []state.Block{}
		for resourceName, req := range reqs {
			if req > 0 {
				blocks = append(blocks, state.Block{
					NUMAAffinity: maskBits,
					Size:         req,
					Type:         resourceName,
				})
			}
		}
		exclusiveMemory[c.Name] = blocks
		metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerExclusivePod).Inc()

		// 可重启 sidecar 的内存会从 Pod 预算中永久占用。
		if podutil.IsRestartableInitContainer(&c) {
			for resourceName, req := range reqs {
				sidecarMemory[resourceName] += req
			}
		}
	}

	// 重新计算应用容器可用的池：Pod 总预算减去所有 sidecar 占用的部分。
	appContainerPool := make(map[v1.ResourceName]uint64)
	for resourceName, total := range podTotalMemory {
		appContainerPool[resourceName] = total - sidecarMemory[resourceName]
	}

	// 再遍历普通容器，从剩余池中分配。
	for _, c := range pod.Spec.Containers {
		reqs, err := getContainerRequestedResources(logger, pod, &c)
		if err != nil {
			// 理论上不会发生，Pod 资源此前已校验过。
			return err
		}

		// 没有内存请求的容器属于共享池。
		if len(reqs) == 0 {
			continue
		}
		metrics.MemoryManagerPinningRequestTotal.Inc()

		// 分配内存块并从池中扣减。
		blocks := []state.Block{}
		for resourceName, req := range reqs {
			if req > 0 {
				blocks = append(blocks, state.Block{
					NUMAAffinity: maskBits,
					Size:         req,
					Type:         resourceName,
				})
				appContainerPool[resourceName] -= req
			}
		}
		exclusiveMemory[c.Name] = blocks
		metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerExclusivePod).Inc()
	}

	// 最终共享池是所有独占分配之后剩余的部分。
	podSharedPoolBlocks := []state.Block{}
	for resourceName, val := range appContainerPool {
		if val > 0 {
			podSharedPoolBlocks = append(podSharedPoolBlocks, state.Block{
				NUMAAffinity: maskBits,
				Size:         val,
				Type:         resourceName,
			})
		}
	}

	podAllocation := []state.Block{}
	for resourceName, requestedSize := range podTotalMemory {
		podAllocation = append(podAllocation, state.Block{
			NUMAAffinity: maskBits,
			Size:         requestedSize,
			Type:         resourceName,
		})
	}
	s.SetPodMemoryBlocks(podUID, podAllocation)

	// 5. 切分成功后，更新全局机器状态。
	for resourceName, requestedSize := range podTotalMemory {
		p.updateMachineState(machineState, maskBits, resourceName, requestedSize)
	}
	logger.V(4).Info("Partitioned pod-level memory allocation", "exclusiveMemory", podMemoryAllocationToString(exclusiveMemory), "podSharedPool", memoryBlocksToString(podSharedPoolBlocks))

	// 6. 将所有容器分配结果保存到状态中。
	for _, c := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		if blocks, isExclusive := exclusiveMemory[c.Name]; isExclusive {
			s.SetMemoryBlocks(podUID, c.Name, blocks)
		} else {
			s.SetMemoryBlocks(podUID, c.Name, podSharedPoolBlocks)
			metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerSharedPod).Inc()
		}

		metrics.ResourceManagerAllocationsTotal.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerPod).Inc()
	}

	s.SetMachineState(machineState)
	return nil
}

// podMemoryAllocationToString 把容器到内存块的映射格式化为紧凑的日志字符串。
func podMemoryAllocationToString(alloc map[string][]state.Block) string {
	if len(alloc) == 0 {
		return ""
	}
	var sb strings.Builder
	for name, blocks := range alloc {
		fmt.Fprintf(&sb, ",%s=%s", name, memoryBlocksToString(blocks))
	}
	// 走到这里说明至少有一条分配，去掉前导逗号是安全的。
	repr := sb.String()[1:]
	return "<" + repr + ">"
}

// memoryBlocksToString 把内存块列表格式化为紧凑的日志字符串。
func memoryBlocksToString(blocks []state.Block) string {
	if len(blocks) == 0 {
		return "[]"
	}
	var sb strings.Builder
	for _, block := range blocks {
		// 格式：{Type:SizeMi @ [NUMA_Nodes]}
		fmt.Fprintf(&sb, ",{%s:%dMi @ %v}", block.Type, block.Size/(1024*1024), block.NUMAAffinity)
	}
	// 去掉前导逗号
	repr := sb.String()[1:]
	return "[" + repr + "]"
}

// Allocate 是幂等的分配调用。
func (p *staticPolicy) Allocate(ctx context.Context, s state.State, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) (rerr error) {
	// 只为 Guaranteed QoS 的 Pod 分配内存。
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "pod", klog.KObj(pod), "containerName", container.Name, "operation", operation)

	if operation != lifecycle.AddOperation {
		logger.V(2).Info("Container-level memory allocation skipped, Memory manager with static policy supports only add operation")
		return nil
	}

	podUID := string(pod.UID)
	// 只为 Guaranteed QoS 的 Pod 分配内存。
	qos := v1qos.GetPodQOS(pod)
	if qos != v1.PodQOSGuaranteed {
		logger.V(5).Info("Exclusive memory allocation skipped, pod QoS is not guaranteed", "qos", qos)
		return nil
	}

	if (!utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) || !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources)) && resourcehelper.IsPodLevelResourcesSet(pod) {
		logger.V(2).Info("Allocation skipped, pod is using pod-level resources which are not supported by the static Memory manager policy", "podUID", podUID)
		return nil
	}

	requestedResources, err := getContainerRequestedResources(logger, pod, container)
	if err != nil {
		return err
	}
	if len(requestedResources) == 0 {
		logger.V(5).Info("Exclusive memory allocation skipped, container requests no memory resources", "podUID", podUID, "containerName", container.Name)
		return nil
	}

	logger.Info("Allocate")
	// 容器属于独占分配池。
	metrics.MemoryManagerPinningRequestTotal.Inc()
	defer func() {
		if rerr != nil {
			metrics.MemoryManagerPinningErrorsTotal.Inc()
			metrics.ResourceManagerAllocationErrorsTotal.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerNode).Inc()
		}
	}()
	if blocks := s.GetMemoryBlocks(podUID, container.Name); blocks != nil {
		p.updatePodReusableMemory(pod, container, blocks)

		logger.Info("Container already present in state, skipping")
		return nil
	}

	// 调用拓扑管理器获取所有提示提供者对齐后的亲和性。
	hint := p.affinity.GetAffinity(logger, podUID, container.Name)
	logger.Info("Got topology affinity", "hint", hint)

	machineState := s.GetMachineState()
	bestHint := &hint
	// 拓扑管理器返回的提示 NUMA 亲和性为 nil 时，
	// 使用与拓扑管理器相同方式计算出的默认 NUMA 亲和性。
	if hint.NUMANodeAffinity == nil {
		defaultHint, err := p.getDefaultHint(machineState, pod, requestedResources)
		if err != nil {
			return err
		}

		if !defaultHint.Preferred && bestHint.Preferred {
			return fmt.Errorf("[memorymanager] failed to find the default preferred hint")
		}
		bestHint = defaultHint
	}

	// 拓扑管理器返回的提示可能无法完全满足容器请求，
	// 此时应将其扩展为满足请求且包含当前提示的提示，除非 skipExtend 已设置。
	// skipExtend 由 Windows BestEffort 策略按容器设置（见 policy_best_effort.go），
	// 用于跟随 CPU 管理器的 NUMA 决策；Linux 静态策略从不设置它，提示照常扩展。
	if !isAffinitySatisfyRequest(machineState, bestHint.NUMANodeAffinity, requestedResources) && !p.skipExtend {
		extendedHint, err := p.extendTopologyManagerHint(machineState, pod, requestedResources, bestHint.NUMANodeAffinity)
		if err != nil {
			return err
		}

		if !extendedHint.Preferred && bestHint.Preferred {
			return fmt.Errorf("[memorymanager] failed to find the extended preferred hint")
		}
		bestHint = extendedHint
	}

	// 最佳提示可能违反 NUMA 分配规则：同一 NUMA 节点不能同时用于单节点和跨节点分配。
	// https://kubernetes.io/blog/2021/08/11/kubernetes-1-22-feature-memory-manager-moves-to-beta/#single-vs-cross-numa-node-allocation
	if isAffinityViolatingNUMAAllocations(machineState, bestHint.NUMANodeAffinity) {
		return fmt.Errorf("[memorymanager] preferred hint violates NUMA node allocation")
	}

	var containerBlocks []state.Block
	maskBits := bestHint.NUMANodeAffinity.GetBits()
	for resourceName, requestedSize := range requestedResources {
		// 更新内存块
		containerBlocks = append(containerBlocks, state.Block{
			NUMAAffinity: maskBits,
			Size:         requestedSize,
			Type:         resourceName,
		})

		podReusableMemory := p.getPodReusableMemory(pod, bestHint.NUMANodeAffinity, resourceName)
		if podReusableMemory >= requestedSize {
			requestedSize = 0
		} else {
			requestedSize -= podReusableMemory
		}

		// 更新节点的内存状态
		p.updateMachineState(machineState, maskBits, resourceName, requestedSize)
		metrics.ResourceManagerAllocationsTotal.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerNode).Inc()
	}

	p.updatePodReusableMemory(pod, container, containerBlocks)

	s.SetMachineState(machineState)
	s.SetMemoryBlocks(podUID, container.Name, containerBlocks)

	// 更新 init 容器的内存块，以反映其内存已被复用的事实。
	// 当 init 容器的内存全部被复用时，其内存块大小可能为 0。
	// 这样做的唯一目的是保证 sum(所有容器的内存) == 分配给 Pod 的内存总量，
	// 尽管最终状态并不精确反映每个容器实际分配到的内存。
	// TODO: 应重构状态结构以反映被复用的内存量
	p.updateInitContainersMemoryBlocks(logger, s, pod, container, containerBlocks)
	metrics.ResourceManagerContainerAssignments.WithLabelValues(metrics.ResourceManagerMemory, metrics.ResourceManagerExclusiveNode).Inc()

	logger.V(4).Info("Allocated exclusive memory")
	return nil
}

// updateMachineState 在给定 NUMA 亲和性覆盖的节点上按顺序扣减内存，
// 一个节点不够时自动向掩码内后续节点溢出。
func (p *staticPolicy) updateMachineState(machineState state.NUMANodeMap, numaAffinity []int, resourceName v1.ResourceName, requestedSize uint64) {
	for _, nodeID := range numaAffinity {
		machineState[nodeID].NumberOfAssignments++
		machineState[nodeID].Cells = numaAffinity

		// 仍需遍历亲和掩码内的所有节点以更新分配计数
		if requestedSize == 0 {
			continue
		}

		// 更新节点的内存状态
		nodeResourceMemoryState := machineState[nodeID].MemoryMap[resourceName]
		if nodeResourceMemoryState.Free <= 0 {
			continue
		}

		// 节点内存足以满足请求
		if nodeResourceMemoryState.Free >= requestedSize {
			nodeResourceMemoryState.Reserved += requestedSize
			nodeResourceMemoryState.Free -= requestedSize
			requestedSize = 0
			continue
		}

		// 节点内存不足，用尽该节点剩余内存后转向下一个节点
		requestedSize -= nodeResourceMemoryState.Free
		nodeResourceMemoryState.Reserved += nodeResourceMemoryState.Free
		nodeResourceMemoryState.Free = 0
	}
}

// getPodReusableMemory 返回 Pod 在指定 NUMA 亲和性上某资源可复用的内存量。
func (p *staticPolicy) getPodReusableMemory(pod *v1.Pod, numaAffinity bitmask.BitMask, resourceName v1.ResourceName) uint64 {
	podReusableMemory, ok := p.initContainersReusableMemory[string(pod.UID)]
	if !ok {
		return 0
	}

	numaReusableMemory, ok := podReusableMemory[numaAffinity.String()]
	if !ok {
		return 0
	}

	return numaReusableMemory[resourceName]
}

// RemoveContainer 是幂等的移除调用。
func (p *staticPolicy) RemoveContainer(logger klog.Logger, s state.State, podUID string, containerName string) {
	logger = klog.LoggerWithValues(logger, "podUID", podUID, "containerName", containerName)
	logger.Info("RemoveContainer", "podUID", podUID, "containerName", containerName)

	// 检查该 Pod 是否按 Pod 级内存管理。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) {
		if podMemory := s.GetPodMemoryBlocks(podUID); podMemory != nil {
			s.Delete(podUID, containerName)
			// 若这是该 Pod 的最后一个容器，则释放整个 Pod 级内存。
			if len(s.GetMemoryAssignments()[podUID]) == 0 {
				p.releaseMemory(s, podMemory)
				// 容器已全部移除，连同 Pod 级状态一并清理，避免残留占用。
				s.DeletePod(podUID)
				logger.Info("Released pod-level memory")
			}
			// 若还有其他容器存在则不释放内存，
			// Pod 级内存会在最后一个容器移除时释放。
			return
		}
	}

	blocks := s.GetMemoryBlocks(podUID, containerName)
	if blocks == nil {
		return
	}
	s.Delete(podUID, containerName)

	p.releaseMemory(s, blocks)
	logger.Info("Released container-level memory")
}

// releaseMemory 把内存块归还给机器状态，恢复其 free/reserved 计数。
func (p *staticPolicy) releaseMemory(s state.State, blocks []state.Block) {
	// 修改机器内存状态，更新空闲和预留内存
	machineState := s.GetMachineState()
	for _, b := range blocks {
		releasedSize := b.Size
		for _, nodeID := range b.NUMAAffinity {
			machineState[nodeID].NumberOfAssignments--

			// 该节点上不再有任何分配时，清除节点分组
			if machineState[nodeID].NumberOfAssignments == 0 {
				machineState[nodeID].Cells = []int{nodeID}
			}

			// 仍需遍历亲和掩码内的所有节点以更新它们
			if releasedSize == 0 {
				continue
			}

			nodeResourceMemoryState := machineState[nodeID].MemoryMap[b.Type]

			// 该节点没有可释放的预留内存时，转向下一个节点
			if nodeResourceMemoryState.Reserved == 0 {
				continue
			}

			// 预留内存小于待释放量：尽量释放后转向下一个节点
			if nodeResourceMemoryState.Reserved < releasedSize {
				releasedSize -= nodeResourceMemoryState.Reserved
				nodeResourceMemoryState.Free += nodeResourceMemoryState.Reserved
				nodeResourceMemoryState.Reserved = 0
				continue
			}

			// 预留内存足以满足待释放量
			nodeResourceMemoryState.Free += releasedSize
			nodeResourceMemoryState.Reserved -= releasedSize
			releasedSize = 0
		}
	}

	s.SetMachineState(machineState)
}

// regenerateHints 根据容器已分配的内存块重新生成拓扑提示，
// 用于 kubelet 重启等场景下保持与既有分配一致的提示。
func regenerateHints(logger klog.Logger, pod *v1.Pod, ctn *v1.Container, ctnBlocks []state.Block, reqRsrc map[v1.ResourceName]uint64) map[string][]topologymanager.TopologyHint {
	hints := map[string][]topologymanager.TopologyHint{}
	for resourceName := range reqRsrc {
		hints[string(resourceName)] = []topologymanager.TopologyHint{}
	}

	if len(ctnBlocks) != len(reqRsrc) {
		logger.Info("The number of requested resources by the container differs from the number of memory blocks", "containerName", ctn.Name)
		return nil
	}

	for _, b := range ctnBlocks {
		if _, ok := reqRsrc[b.Type]; !ok {
			logger.Info("Container requested resources but none available of this type", "containerName", ctn.Name, "type", b.Type)
			return nil
		}

		if b.Size != reqRsrc[b.Type] {
			logger.Info("Memory already allocated with different numbers than requested", "containerName", ctn.Name, "type", b.Type, "requestedResource", reqRsrc[b.Type], "allocatedSize", b.Size)
			return nil
		}

		containerNUMAAffinity, err := bitmask.NewBitMask(b.NUMAAffinity...)
		if err != nil {
			logger.Error(err, "Failed to generate NUMA bitmask", "containerName", ctn.Name, "type", b.Type)
			return nil
		}

		logger.Info("Regenerating TopologyHints, resource was already allocated to pod", "resourceName", b.Type, "podUID", pod.UID, "containerName", ctn.Name)
		hints[string(b.Type)] = append(hints[string(b.Type)], topologymanager.TopologyHint{
			NUMANodeAffinity: containerNUMAAffinity,
			Preferred:        true,
		})
	}
	return hints
}

// getPodRequestedResources 计算 Pod 需要的内存资源量。
// 若设置了 Pod 级资源则直接采用；否则按 init 容器峰值与长期运行容器之和取较大值。
func getPodRequestedResources(logger klog.Logger, pod *v1.Pod) (map[v1.ResourceName]uint64, error) {
	// 若设置了 Pod 级资源，直接使用。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) && resourcehelper.IsPodLevelResourcesSet(pod) {
		requestedResources := make(map[v1.ResourceName]uint64)
		for resourceName, quantity := range pod.Spec.Resources.Requests {
			if resourceName != v1.ResourceMemory && !corehelper.IsHugePageResourceName(resourceName) {
				continue
			}
			requestedSize, succeed := quantity.AsInt64()
			if !succeed {
				return nil, fmt.Errorf("[memorymanager] failed to represent quantity as int64")
			}
			if requestedSize <= 0 {
				continue
			}
			requestedResources[resourceName] = uint64(requestedSize)
		}
		return requestedResources, nil
	}

	// init 容器在任意时刻请求的最大资源量。
	reqRsrcsByInitCtrs := make(map[v1.ResourceName]uint64)
	// 可重启 init 容器请求的资源总量。
	reqRsrcsByRestartableInitCtrs := make(map[v1.ResourceName]uint64)
	for _, ctr := range pod.Spec.InitContainers {
		reqRsrcs, err := getContainerRequestedResources(logger, pod, &ctr)

		if err != nil {
			return nil, err
		}
		for rsrcName, qty := range reqRsrcs {
			if _, ok := reqRsrcsByInitCtrs[rsrcName]; !ok {
				reqRsrcsByInitCtrs[rsrcName] = uint64(0)
			}

			// 详见 https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/753-sidecar-containers#resources-calculation-for-scheduling-and-pod-admission
			if podutil.IsRestartableInitContainer(&ctr) {
				reqRsrcsByRestartableInitCtrs[rsrcName] += qty
			} else if reqRsrcsByRestartableInitCtrs[rsrcName]+qty > reqRsrcsByInitCtrs[rsrcName] {
				reqRsrcsByInitCtrs[rsrcName] = reqRsrcsByRestartableInitCtrs[rsrcName] + qty
			}
		}
	}

	reqRsrcsByAppCtrs := make(map[v1.ResourceName]uint64)
	for _, ctr := range pod.Spec.Containers {
		reqRsrcs, err := getContainerRequestedResources(logger, pod, &ctr)

		if err != nil {
			return nil, err
		}
		for rsrcName, qty := range reqRsrcs {
			if _, ok := reqRsrcsByAppCtrs[rsrcName]; !ok {
				reqRsrcsByAppCtrs[rsrcName] = uint64(0)
			}

			reqRsrcsByAppCtrs[rsrcName] += qty
		}
	}

	reqRsrcs := make(map[v1.ResourceName]uint64)
	for rsrcName := range reqRsrcsByAppCtrs {
		// 长期运行容器请求的资源总量。
		reqRsrcsByLongRunningCtrs := reqRsrcsByAppCtrs[rsrcName] + reqRsrcsByRestartableInitCtrs[rsrcName]
		reqRsrcs[rsrcName] = reqRsrcsByLongRunningCtrs

		if reqRsrcs[rsrcName] < reqRsrcsByInitCtrs[rsrcName] {
			reqRsrcs[rsrcName] = reqRsrcsByInitCtrs[rsrcName]
		}
	}
	return reqRsrcs, nil
}

// GetPodTopologyHints 返回整个 Pod 的拓扑提示，所有容器共用同一份提示。
func (p *staticPolicy) GetPodTopologyHints(logger klog.Logger, s state.State, pod *v1.Pod, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod), "operation", operation)
	if operation != lifecycle.AddOperation {
		logger.V(2).Info("GetPodTopologyHints skipped, memory manager with static policy supports only add operation")
		return nil
	}

	if v1qos.GetPodQOS(pod) != v1.PodQOSGuaranteed {
		return nil
	}

	// 获取 Pod 请求的内存总量。
	reqRsrcs, err := getPodRequestedResources(logger, pod)
	if err != nil {
		logger.Error(err, "Failed to get pod requested resources", "podUID", pod.UID)
		return nil
	}

	// 若 Pod 设置了 Pod 级资源但特性门控被禁用，记录日志并返回 nil 提示，
	// 让 Pod 在不做对齐的情况下被准入。
	if (!utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) || !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources)) && resourcehelper.IsPodLevelResourcesSet(pod) {
		logger.V(3).Info("Memory Manager pod hint generation skipped, pod is using pod-level resources but the PodLevelResourceManagers feature gate is not enabled", "podUID", pod.UID)
		return nil
	}

	// 校验：若 Pod 中有容器要放入共享池，则在扣除全部独占分配后共享池必须仍有内存。
	// 若独占请求之和耗尽整个 Pod 级内存预算，则不生成任何提示，Pod 会被拓扑管理器拒绝。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) && resourcehelper.IsPodLevelResourcesSet(pod) {
		if err := p.validatePodScopeResources(logger, pod); err != nil {
			logger.V(2).Info("Pod admission will fail", "error", err)
			hints := make(map[string][]topologymanager.TopologyHint)
			for resourceName := range reqRsrcs {
				hints[string(resourceName)] = []topologymanager.TopologyHint{}
			}
			return hints
		}
	}

	if len(reqRsrcs) == 0 {
		return nil
	}

	for _, ctn := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		containerBlocks := s.GetMemoryBlocks(string(pod.UID), ctn.Name)
		// 若容器已有内存分配，直接短路重新生成相同提示。
		// 例如 kubelet 重启后会出现这种情况。
		if containerBlocks != nil {
			return regenerateHints(logger, pod, &ctn, containerBlocks, reqRsrcs)
		}
	}

	// Pod 拓扑提示对所有容器只计算一次，因此无需传入可复用状态。
	return p.calculateHints(s.GetMachineState(), pod, reqRsrcs)
}

// GetTopologyHints 实现 topologymanager.HintProvider 接口，
// 与其他资源控制器共同协商实现 NUMA 感知的资源对齐。
func (p *staticPolicy) GetTopologyHints(logger klog.Logger, s state.State, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod), "operation", operation)
	if operation != lifecycle.AddOperation {
		logger.V(2).Info("GetTopologyHints skipped, memory manager with static policy supports only add operation")
		return nil
	}

	if v1qos.GetPodQOS(pod) != v1.PodQOSGuaranteed {
		return nil
	}

	requestedResources, err := getContainerRequestedResources(logger, pod, container)
	if err != nil {
		logger.Error(err, "Failed to get container requested resources", "podUID", pod.UID, "containerName", container.Name)
		return nil
	}
	if len(requestedResources) == 0 {
		return nil
	}

	// 若 Pod 设置了 Pod 级资源但特性门控被禁用，记录日志并返回 nil 提示，
	// 让 Pod 在不做对齐的情况下被准入。
	if (!utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) || !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources)) && resourcehelper.IsPodLevelResourcesSet(pod) {
		logger.V(3).Info("Memory Manager pod hint generation skipped, pod is using pod-level resources but the PodLevelResourceManagers feature gate is not enabled", "podUID", pod.UID)
		return nil
	}

	containerBlocks := s.GetMemoryBlocks(string(pod.UID), container.Name)
	// 若容器已有内存分配，直接短路重新生成相同提示。
	// 例如 kubelet 重启后会出现这种情况。
	if containerBlocks != nil {
		return regenerateHints(logger, pod, container, containerBlocks, requestedResources)
	}

	return p.calculateHints(s.GetMachineState(), pod, requestedResources)
}

// getContainerRequestedResources 返回容器请求的内存与大页资源量。
func getContainerRequestedResources(logger klog.Logger, pod *v1.Pod, container *v1.Container) (map[v1.ResourceName]uint64, error) {
	requestedResources := map[v1.ResourceName]uint64{}
	// 在 Pod 级资源管理下，只有 CPU 和内存的 request 等于 limit 的容器才参与独占内存分配，
	// 这与容器级资源的 Guaranteed QoS 要求一致。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) && resourcehelper.IsPodLevelResourcesSet(pod) && !cmqos.IsContainerEquivalentQOSGuaranteed(container) {
		logger.V(5).Info("Exclusive Memory allocation skipped, container is not eligible, request and limit are not equal", "pod", klog.KObj(pod), "containerName", container.Name)
		return requestedResources, nil
	}

	for resourceName, quantity := range container.Resources.Requests {
		if resourceName != v1.ResourceMemory && !corehelper.IsHugePageResourceName(resourceName) {
			continue
		}

		requestedSize, succeed := quantity.AsInt64()
		if !succeed {
			return nil, fmt.Errorf("[memorymanager] failed to represent quantity as int64")
		}
		requestedResources[resourceName] = uint64(requestedSize)
	}
	return requestedResources, nil
}

// calculateHints 枚举所有 NUMA 掩码组合，为每种请求资源生成拓扑提示。
func (p *staticPolicy) calculateHints(machineState state.NUMANodeMap, pod *v1.Pod, requestedResources map[v1.ResourceName]uint64) map[string][]topologymanager.TopologyHint {
	var numaNodes []int
	for n := range machineState {
		numaNodes = append(numaNodes, n)
	}
	sort.Ints(numaNodes)

	// minAffinitySize 初始化为覆盖所有 NUMA 单元的数量。
	minAffinitySize := len(numaNodes)

	hints := map[string][]topologymanager.TopologyHint{}
	bitmask.IterateBitMasks(numaNodes, func(mask bitmask.BitMask) {
		maskBits := mask.GetBits()
		singleNUMAHint := len(maskBits) == 1

		totalFreeSize := map[v1.ResourceName]uint64{}
		totalAllocatableSize := map[v1.ResourceName]uint64{}
		// 计算该掩码下总的空闲和可分配内存
		for _, nodeID := range maskBits {
			for resourceName := range requestedResources {
				if _, ok := totalFreeSize[resourceName]; !ok {
					totalFreeSize[resourceName] = 0
				}
				totalFreeSize[resourceName] += machineState[nodeID].MemoryMap[resourceName].Free

				if _, ok := totalAllocatableSize[resourceName]; !ok {
					totalAllocatableSize[resourceName] = 0
				}
				totalAllocatableSize[resourceName] += machineState[nodeID].MemoryMap[resourceName].Allocatable
			}
		}

		// 校验该掩码对所有内存类型的可分配量是否足够
		for resourceName, requestedSize := range requestedResources {
			if totalAllocatableSize[resourceName] < requestedSize {
				return
			}
		}

		// 记录能满足容器资源请求的最少 NUMA 节点数
		if mask.Count() < minAffinitySize {
			minAffinitySize = mask.Count()
		}

		// 该节点已与其他节点成组，不能再用于单 NUMA 节点分配
		if singleNUMAHint && len(machineState[maskBits[0]].Cells) > 1 {
			return
		}

		for _, nodeID := range maskBits {
			// 该节点已用于内存分配
			if !singleNUMAHint && machineState[nodeID].NumberOfAssignments > 0 {
				// 节点已用于单 NUMA 内存分配，不能再用于多 NUMA 节点分配
				if len(machineState[nodeID].Cells) == 1 {
					return
				}

				// 节点已用于不同的节点组，不能用于当前提示
				if !areGroupsEqual(machineState[nodeID].Cells, maskBits) {
					return
				}
			}
		}

		// 校验该掩码对所有内存类型的空闲量是否足够
		for resourceName, requestedSize := range requestedResources {
			podReusableMemory := p.getPodReusableMemory(pod, mask, resourceName)
			if totalFreeSize[resourceName]+podReusableMemory < requestedSize {
				return
			}
		}

		// 把该节点掩码作为所有内存类型的拓扑提示
		for resourceName := range requestedResources {
			if _, ok := hints[string(resourceName)]; !ok {
				hints[string(resourceName)] = []topologymanager.TopologyHint{}
			}
			hints[string(resourceName)] = append(hints[string(resourceName)], topologymanager.TopologyHint{
				NUMANodeAffinity: mask,
				Preferred:        false,
			})
		}
	})

	// 根据 multiNUMAGroups 更新提示的 preferred 值；未提供时，
	// 默认优先选择 NUMA 节点数最少的提示。
	for resourceName := range requestedResources {
		for i, hint := range hints[string(resourceName)] {
			hints[string(resourceName)][i].Preferred = p.isHintPreferred(hint.NUMANodeAffinity.GetBits(), minAffinitySize)
		}
	}

	return hints
}

// isHintPreferred 判断提示是否为优先提示：覆盖的 NUMA 节点数等于最小值。
func (p *staticPolicy) isHintPreferred(maskBits []int, minAffinitySize int) bool {
	return len(maskBits) == minAffinitySize
}

// areGroupsEqual 判断两个 NUMA 节点组是否相同（排序后逐项比较）。
func areGroupsEqual(group1, group2 []int) bool {
	sort.Ints(group1)
	sort.Ints(group2)

	if len(group1) != len(group2) {
		return false
	}

	for i, elm := range group1 {
		if group2[i] != elm {
			return false
		}
	}
	return true
}

// validateState 校验从磁盘恢复的状态与根据机器信息推导出的状态是否一致。
func (p *staticPolicy) validateState(logger klog.Logger, s state.State) error {
	machineState := s.GetMachineState()
	memoryAssignments := s.GetMemoryAssignments()

	if len(machineState) == 0 {
		// 存在分配记录时机器状态不能为空
		if len(memoryAssignments) != 0 {
			return fmt.Errorf("[memorymanager] machine state can not be empty when it has memory assignments")
		}

		defaultMachineState := p.getDefaultMachineState()
		s.SetMachineState(defaultMachineState)

		return nil
	}

	// 计算所有已分配给容器的内存
	expectedMachineState := p.getDefaultMachineState()
	for pod, container := range memoryAssignments {
		if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) {
			if podBlocks := s.GetPodMemoryBlocks(pod); len(podBlocks) > 0 {
				if err := p.updateExpectedMachineState(expectedMachineState, podBlocks, pod, ""); err != nil {
					return err
				}
				continue
			}
		}

		for containerName, blocks := range container {
			if err := p.updateExpectedMachineState(expectedMachineState, blocks, pod, containerName); err != nil {
				return err
			}
		}
	}

	// 状态已从文件初始化（非空），校验总量、系统预留和预留内存是否变化。
	// 以下情况会导致变化：
	// - 节点增删物理内存条
	// - 修改 kubelet 的 system-reserved、kube-reserved 或 pre-reserved-memory-zone 参数
	if !areMachineStatesEqual(logger, machineState, expectedMachineState) {
		return fmt.Errorf("[memorymanager] the expected machine state is different from the real one")
	}

	return nil
}

// updateExpectedMachineState 在期望的机器状态上重放内存块分配，用于与真实状态比对。
func (p *staticPolicy) updateExpectedMachineState(expectedMachineState state.NUMANodeMap, blocks []state.Block, podUID, containerName string) error {
	for _, b := range blocks {
		requestedSize := b.Size
		for _, nodeID := range b.NUMAAffinity {
			nodeState, ok := expectedMachineState[nodeID]
			if !ok {
				return fmt.Errorf("[memorymanager] (pod: %s, container: %s) the memory assignment uses the NUMA that does not exist", podUID, containerName)
			}

			nodeState.NumberOfAssignments++
			nodeState.Cells = b.NUMAAffinity

			memoryState, ok := nodeState.MemoryMap[b.Type]
			if !ok {
				return fmt.Errorf("[memorymanager] (pod: %s, container: %s) the memory assignment uses memory resource that does not exist", podUID, containerName)
			}

			if requestedSize == 0 {
				continue
			}

			// 该节点内存不足，转向下一个节点
			if memoryState.Free <= 0 {
				continue
			}

			// 节点内存足以满足请求
			if memoryState.Free >= requestedSize {
				memoryState.Reserved += requestedSize
				memoryState.Free -= requestedSize
				requestedSize = 0
				continue
			}

			// 节点内存不足，用尽该节点剩余内存后转向下一个节点
			requestedSize -= memoryState.Free
			memoryState.Reserved += memoryState.Free
			memoryState.Free = 0
		}
	}
	return nil
}

// areMachineStatesEqual 比较两个机器状态是否等价，不等时输出差异日志。
func areMachineStatesEqual(logger klog.Logger, ms1, ms2 state.NUMANodeMap) bool {
	if len(ms1) != len(ms2) {
		logger.Info("Node states were different", "lengthNode1", len(ms1), "lengthNode2", len(ms2))
		return false
	}

	for nodeID, nodeState1 := range ms1 {
		nodeState2, ok := ms2[nodeID]
		if !ok {
			logger.Info("Node state didn't have node ID", "nodeID", nodeID)
			return false
		}

		if nodeState1.NumberOfAssignments != nodeState2.NumberOfAssignments {
			logger.Info("Node state had a different number of memory assignments.", "assignment1", nodeState1.NumberOfAssignments, "assignment2", nodeState2.NumberOfAssignments)
			return false
		}

		if !areGroupsEqual(nodeState1.Cells, nodeState2.Cells) {
			logger.Info("Node states had different groups", "stateNode1", nodeState1.Cells, "stateNode2", nodeState2.Cells)
			return false
		}

		if len(nodeState1.MemoryMap) != len(nodeState2.MemoryMap) {
			logger.Info("Node state had memory maps of different lengths", "lengthNode1", len(nodeState1.MemoryMap), "lengthNode2", len(nodeState2.MemoryMap))
			return false
		}

		for resourceName, memoryState1 := range nodeState1.MemoryMap {
			memoryState2, ok := nodeState2.MemoryMap[resourceName]
			if !ok {
				logger.Info("Memory state didn't have resource", "resource", resourceName)
				return false
			}

			if !areMemoryStatesEqual(logger, memoryState1, memoryState2, nodeID, resourceName) {
				return false
			}

			tmpState1 := state.MemoryTable{}
			tmpState2 := state.MemoryTable{}
			for _, nodeID := range nodeState1.Cells {
				tmpState1.Free += ms1[nodeID].MemoryMap[resourceName].Free
				tmpState1.Reserved += ms1[nodeID].MemoryMap[resourceName].Reserved
				tmpState2.Free += ms2[nodeID].MemoryMap[resourceName].Free
				tmpState2.Reserved += ms2[nodeID].MemoryMap[resourceName].Reserved
			}

			if tmpState1.Free != tmpState2.Free {
				logger.Info("NUMA node and resource had different memory states", "node", nodeID, "resource", resourceName, "field", "free", "free1", tmpState1.Free, "free2", tmpState2.Free, "memoryState1", *memoryState1, "memoryState2", *memoryState2)
				return false
			}
			if tmpState1.Reserved != tmpState2.Reserved {
				logger.Info("NUMA node and resource had different memory states", "node", nodeID, "resource", resourceName, "field", "reserved", "reserved1", tmpState1.Reserved, "reserved2", tmpState2.Reserved, "memoryState1", *memoryState1, "memoryState2", *memoryState2)
				return false
			}
		}
	}
	return true
}

// areMemoryStatesEqual 比较单个 NUMA 节点上某资源的两个内存表是否等价。
func areMemoryStatesEqual(logger klog.Logger, memoryState1, memoryState2 *state.MemoryTable, nodeID int, resourceName v1.ResourceName) bool {
	loggerWithValues := klog.LoggerWithValues(logger, "node", nodeID, "resource", resourceName, "memoryState1", *memoryState1, "memoryState2", *memoryState2)
	if memoryState1.TotalMemSize != memoryState2.TotalMemSize {
		logger.Info("Memory states for the NUMA node and resource are different", "field", "TotalMemSize", "TotalMemSize1", memoryState1.TotalMemSize, "TotalMemSize2", memoryState2.TotalMemSize)
		return false
	}

	if memoryState1.SystemReserved != memoryState2.SystemReserved {
		loggerWithValues.Info("Memory states for the NUMA node and resource are different", "field", "SystemReserved", "SystemReserved1", memoryState1.SystemReserved, "SystemReserved2", memoryState2.SystemReserved)
		return false
	}

	if memoryState1.Allocatable != memoryState2.Allocatable {
		loggerWithValues.Info("Memory states for the NUMA node and resource are different", "field", "Allocatable", "Allocatable1", memoryState1.Allocatable, "Allocatable2", memoryState2.Allocatable)
		return false
	}
	return true
}

// getDefaultMachineState 根据机器拓扑和系统预留量构造初始机器状态。
func (p *staticPolicy) getDefaultMachineState() state.NUMANodeMap {
	defaultMachineState := state.NUMANodeMap{}
	nodeHugepages := map[int]uint64{}
	for _, node := range p.machineInfo.Topology {
		defaultMachineState[node.Id] = &state.NUMANodeState{
			NumberOfAssignments: 0,
			MemoryMap:           map[v1.ResourceName]*state.MemoryTable{},
			Cells:               []int{node.Id},
		}

		// 用大页信息填充内存表
		for _, hugepage := range node.HugePages {
			hugepageQuantity := resource.NewQuantity(int64(hugepage.PageSize)*1024, resource.BinarySI)
			resourceName := corehelper.HugePageResourceName(*hugepageQuantity)
			systemReserved := p.getResourceSystemReserved(node.Id, resourceName)
			totalHugepagesSize := hugepage.NumPages * hugepage.PageSize * 1024
			allocatable := totalHugepagesSize - systemReserved
			defaultMachineState[node.Id].MemoryMap[resourceName] = &state.MemoryTable{
				Allocatable:    allocatable,
				Free:           allocatable,
				Reserved:       0,
				SystemReserved: systemReserved,
				TotalMemSize:   totalHugepagesSize,
			}
			if _, ok := nodeHugepages[node.Id]; !ok {
				nodeHugepages[node.Id] = 0
			}
			nodeHugepages[node.Id] += totalHugepagesSize
		}

		// 用普通内存信息填充内存表
		systemReserved := p.getResourceSystemReserved(node.Id, v1.ResourceMemory)

		allocatable := node.Memory - systemReserved
		// 扣除已被大页占用的内存
		if allocatedByHugepages, ok := nodeHugepages[node.Id]; ok {
			allocatable -= allocatedByHugepages
		}
		defaultMachineState[node.Id].MemoryMap[v1.ResourceMemory] = &state.MemoryTable{
			Allocatable:    allocatable,
			Free:           allocatable,
			Reserved:       0,
			SystemReserved: systemReserved,
			TotalMemSize:   node.Memory,
		}
	}
	return defaultMachineState
}

// getResourceSystemReserved 返回指定 NUMA 节点上某资源的系统预留量。
func (p *staticPolicy) getResourceSystemReserved(nodeID int, resourceName v1.ResourceName) uint64 {
	var systemReserved uint64
	if nodeSystemReserved, ok := p.systemReserved[nodeID]; ok {
		if nodeMemorySystemReserved, ok := nodeSystemReserved[resourceName]; ok {
			systemReserved = nodeMemorySystemReserved
		}
	}
	return systemReserved
}

// getDefaultHint 在拓扑管理器未给出亲和性时，计算默认的最佳提示。
func (p *staticPolicy) getDefaultHint(machineState state.NUMANodeMap, pod *v1.Pod, requestedResources map[v1.ResourceName]uint64) (*topologymanager.TopologyHint, error) {
	hints := p.calculateHints(machineState, pod, requestedResources)
	if len(hints) < 1 {
		return nil, fmt.Errorf("[memorymanager] failed to get the default NUMA affinity, no NUMA nodes with enough memory is available")
	}

	// 所有内存类型的提示应一致，因此只检查普通内存类型的提示
	return findBestHint(hints[string(v1.ResourceMemory)]), nil
}

// isAffinitySatisfyRequest 判断给定 NUMA 掩码的空闲内存是否满足所有资源请求。
func isAffinitySatisfyRequest(machineState state.NUMANodeMap, mask bitmask.BitMask, requestedResources map[v1.ResourceName]uint64) bool {
	totalFreeSize := map[v1.ResourceName]uint64{}
	for _, nodeID := range mask.GetBits() {
		for resourceName := range requestedResources {
			if _, ok := totalFreeSize[resourceName]; !ok {
				totalFreeSize[resourceName] = 0
			}
			totalFreeSize[resourceName] += machineState[nodeID].MemoryMap[resourceName].Free
		}
	}

	// 校验该掩码对所有内存类型的资源是否足够
	for resourceName, requestedSize := range requestedResources {
		if totalFreeSize[resourceName] < requestedSize {
			return false
		}
	}

	return true
}

// extendTopologyManagerHint 在拓扑管理器提示不满足容器请求时进行扩展。
// 拓扑管理器用按位与合并所有提示，因此 restricted 策略下可能只得到我们提供的
// 提示的子集；此时需要将其扩展回原始提示。
func (p *staticPolicy) extendTopologyManagerHint(machineState state.NUMANodeMap, pod *v1.Pod, requestedResources map[v1.ResourceName]uint64, mask bitmask.BitMask) (*topologymanager.TopologyHint, error) {
	hints := p.calculateHints(machineState, pod, requestedResources)

	var filteredHints []topologymanager.TopologyHint
	// 所有内存类型的提示应一致，因此只检查普通内存类型的提示
	for _, hint := range hints[string(v1.ResourceMemory)] {
		affinityBits := hint.NUMANodeAffinity.GetBits()
		// 过滤掉不包含当前提示的提示
		if isHintInGroup(mask.GetBits(), affinityBits) {
			filteredHints = append(filteredHints, hint)
		}
	}

	if len(filteredHints) < 1 {
		return nil, fmt.Errorf("[memorymanager] failed to find NUMA nodes to extend the current topology hint")
	}

	// 尝试找到 NUMA 节点数最少的优先提示，适用于 restricted 策略
	return findBestHint(filteredHints), nil
}

// isHintInGroup 判断 hint 是否为 group 的子集（两者排序后逐项匹配）。
func isHintInGroup(hint []int, group []int) bool {
	sort.Ints(hint)
	sort.Ints(group)

	hintIndex := 0
	for i := range group {
		if hintIndex == len(hint) {
			return true
		}

		if group[i] != hint[hintIndex] {
			continue
		}
		hintIndex++
	}

	return hintIndex == len(hint)
}

// findBestHint 从提示列表中选择最佳的提示：优先选择优先标记为 true 且 NUMA 节点数最少的。
func findBestHint(hints []topologymanager.TopologyHint) *topologymanager.TopologyHint {
	// 尝试找到 NUMA 节点数最少的优先提示，适用于 restricted 策略
	bestHint := topologymanager.TopologyHint{}
	for _, hint := range hints {
		if bestHint.NUMANodeAffinity == nil {
			bestHint = hint
			continue
		}

		// 当前提示为 preferred 而 bestHint 不是时，优先选当前提示
		if hint.Preferred && !bestHint.Preferred {
			bestHint = hint
			continue
		}

		// 两者 preferred 相同，但当前提示覆盖的 NUMA 节点更少
		if hint.Preferred == bestHint.Preferred && hint.NUMANodeAffinity.IsNarrowerThan(bestHint.NUMANodeAffinity) {
			bestHint = hint
		}
	}
	return &bestHint
}

// GetAllocatableMemory 返回每个 NUMA 节点的可分配内存量。
func (p *staticPolicy) GetAllocatableMemory(s state.State) []state.Block {
	var allocatableMemory []state.Block
	machineState := s.GetMachineState()
	for numaNodeID, numaNodeState := range machineState {
		for resourceName, memoryTable := range numaNodeState.MemoryMap {
			if memoryTable.Allocatable == 0 {
				continue
			}

			block := state.Block{
				NUMAAffinity: []int{numaNodeID},
				Type:         resourceName,
				Size:         memoryTable.Allocatable,
			}
			allocatableMemory = append(allocatableMemory, block)
		}
	}
	return allocatableMemory
}

// updatePodReusableMemory 维护 init 容器的可复用内存记录：
// 普通 init 容器分配时登记，应用容器使用后扣减。
func (p *staticPolicy) updatePodReusableMemory(pod *v1.Pod, container *v1.Container, memoryBlocks []state.Block) {
	podUID := string(pod.UID)

	// 若 initContainersReusableMemory 中存在非当前 Pod 的条目，将其删除。
	for uid := range p.initContainersReusableMemory {
		if podUID != uid {
			delete(p.initContainersReusableMemory, uid)
		}
	}

	if isRegularInitContainer(pod, container) {
		if _, ok := p.initContainersReusableMemory[podUID]; !ok {
			p.initContainersReusableMemory[podUID] = map[string]map[v1.ResourceName]uint64{}
		}

		for _, block := range memoryBlocks {
			blockBitMask, _ := bitmask.NewBitMask(block.NUMAAffinity...)
			blockBitMaskString := blockBitMask.String()

			if _, ok := p.initContainersReusableMemory[podUID][blockBitMaskString]; !ok {
				p.initContainersReusableMemory[podUID][blockBitMaskString] = map[v1.ResourceName]uint64{}
			}

			if blockReusableMemory := p.initContainersReusableMemory[podUID][blockBitMaskString][block.Type]; block.Size > blockReusableMemory {
				p.initContainersReusableMemory[podUID][blockBitMaskString][block.Type] = block.Size
			}
		}

		return
	}

	// 应用容器使用后更新可复用内存
	for _, block := range memoryBlocks {
		blockBitMask, _ := bitmask.NewBitMask(block.NUMAAffinity...)
		if podReusableMemory := p.getPodReusableMemory(pod, blockBitMask, block.Type); podReusableMemory != 0 {
			if block.Size >= podReusableMemory {
				p.initContainersReusableMemory[podUID][blockBitMask.String()][block.Type] = 0
			} else {
				p.initContainersReusableMemory[podUID][blockBitMask.String()][block.Type] -= block.Size
			}
		}
	}
}

// updateInitContainersMemoryBlocks 扣减被当前容器复用的 init 容器内存块，
// 保证各容器内存块之和等于 Pod 分配总量。
func (p *staticPolicy) updateInitContainersMemoryBlocks(logger klog.Logger, s state.State, pod *v1.Pod, container *v1.Container, containerMemoryBlocks []state.Block) {
	podUID := string(pod.UID)

	for _, containerBlock := range containerMemoryBlocks {
		blockSize := containerBlock.Size
		for _, initContainer := range pod.Spec.InitContainers {
			// 到达当前容器后停止更新
			if initContainer.Name == container.Name {
				break
			}

			if blockSize == 0 {
				break
			}

			if podutil.IsRestartableInitContainer(&initContainer) {
				// 可重启 init 容器的资源不应被复用
				continue
			}

			initContainerBlocks := s.GetMemoryBlocks(podUID, initContainer.Name)
			if len(initContainerBlocks) == 0 {
				continue
			}

			for i := range initContainerBlocks {
				initContainerBlock := &initContainerBlocks[i]
				if initContainerBlock.Size == 0 {
					continue
				}

				if initContainerBlock.Type != containerBlock.Type {
					continue
				}

				if !isNUMAAffinitiesEqual(logger, initContainerBlock.NUMAAffinity, containerBlock.NUMAAffinity) {
					continue
				}

				if initContainerBlock.Size > blockSize {
					initContainerBlock.Size -= blockSize
					blockSize = 0
				} else {
					blockSize -= initContainerBlock.Size
					initContainerBlock.Size = 0
				}
			}

			s.SetMemoryBlocks(podUID, initContainer.Name, initContainerBlocks)
		}
	}
}

// isRegularInitContainer 判断容器是否为非可重启的 init 容器。
func isRegularInitContainer(pod *v1.Pod, container *v1.Container) bool {
	for _, initContainer := range pod.Spec.InitContainers {
		if initContainer.Name == container.Name {
			return !podutil.IsRestartableInitContainer(&initContainer)
		}
	}

	return false
}

// isNUMAAffinitiesEqual 判断两个 NUMA 亲和性切片是否等价。
func isNUMAAffinitiesEqual(logger klog.Logger, numaAffinity1, numaAffinity2 []int) bool {
	bitMask1, err := bitmask.NewBitMask(numaAffinity1...)
	if err != nil {
		logger.Error(err, "failed to create bit mask", "numaAffinity1", numaAffinity1)
		return false
	}

	bitMask2, err := bitmask.NewBitMask(numaAffinity2...)
	if err != nil {
		logger.Error(err, "failed to create bit mask", "numaAffinity2", numaAffinity2)
		return false
	}

	return bitMask1.IsEqual(bitMask2)
}

// isAffinityViolatingNUMAAllocations 判断掩码是否违反 NUMA 分配规则：
// 同一节点不能既用于单 NUMA 节点分配又用于多 NUMA 节点分配，且多节点分配必须使用相同节点组。
func isAffinityViolatingNUMAAllocations(machineState state.NUMANodeMap, mask bitmask.BitMask) bool {
	maskBits := mask.GetBits()
	singleNUMAHint := len(maskBits) == 1
	for _, nodeID := range mask.GetBits() {
		// 该节点从未用于内存分配
		if machineState[nodeID].NumberOfAssignments == 0 {
			continue
		}
		if singleNUMAHint {
			continue
		}
		// 节点已用于单 NUMA 内存分配，不能用于多 NUMA 节点分配
		if len(machineState[nodeID].Cells) == 1 {
			return true
		}
		// 节点已用于不同的节点组，不能用于当前提示
		if !areGroupsEqual(machineState[nodeID].Cells, maskBits) {
			return true
		}
	}
	return false
}
