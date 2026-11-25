/*
Copyright 2015 The Kubernetes Authors.

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

package qos

import (
	"slices"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/types"
)

const (
	// KubeletOOMScoreAdj 是 Kubelet 的 OOM 分数调整值。
	KubeletOOMScoreAdj int = -999
	// KubeProxyOOMScoreAdj 是 kube-proxy 的 OOM 分数调整值。
	KubeProxyOOMScoreAdj  int = -999
	guaranteedOOMScoreAdj int = -997
	besteffortOOMScoreAdj int = 1000
)

// GetContainerOOMScoreAdjust 计算容器内所有进程应设置的 OOM 分数调整值。
// OOM 分数约为进程内存占比乘以 10 再加一个 -1000 到 1000 的可调值；
// 分数越高越先被 OOM killer 杀死。参见 https://lwn.net/Articles/391222/。
// pod 参数必须包含已分配资源，因为分数基于分配量而非实际使用量计算。
func GetContainerOOMScoreAdjust(pod *v1.Pod, container *v1.Container, memoryCapacity int64) int {
	if types.IsNodeCriticalPod(pod) {
		// 节点关键 Pod 必须最后被杀，直接使用保证型分数。
		return guaranteedOOMScoreAdj
	}

	switch v1qos.GetPodQOS(pod) {
	case v1.PodQOSGuaranteed:
		// 保证型（Guaranteed）容器应最后被杀。
		return guaranteedOOMScoreAdj
	case v1.PodQOSBestEffort:
		return besteffortOOMScoreAdj
	}

	// 可突发容器介于 Guaranteed 与 Best-Effort 之间：希望保护实际用量低于请求量的容器。
	// 下面的公式是启发式：请求 10% 系统内存的容器 OOM 分数约为 900；
	// 若进程用量超过 10%，分数为 1000，会成为 OOM killer 的首选目标。
	// 注意：该启发式对"单容器内含大量小进程"的场景不成立。
	containerMemReq := getEffectiveContainerMemoryRequest(pod, container)

	var oomScoreAdjust, remainingReqPerContainer int64
	// 启用 Pod 级资源（PodLevelResources）时，OOM 分数公式会纳入 Pod 级内存请求：
	// 未分配给容器的 Pod 级额外内存请求会平均分摊到每个容器，再计入各自的内存请求；
	// 未启用时仅使用容器级内存请求。详见
	// https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/2837-pod-level-resource-spec/README.md#oom-score-adjustment
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources) &&
		resourcehelper.IsPodLevelRequestsSet(pod) {
		// TODO(ndixita): 重构为所有情况都使用该公式；未设置 Pod 级资源时 remainingReqPerContainer 为 0。
		remainingReqPerContainer = remainingPodMemReqPerContainer(pod)
		oomScoreAdjust = 1000 - (1000 * (containerMemReq + remainingReqPerContainer) / memoryCapacity)
	} else {
		oomScoreAdjust = 1000 - (1000*containerMemReq)/memoryCapacity
	}

	// 边车容器按 max(当前边车内存, 普通容器最小内存) 计算 OOM 分数调整，
	// 避免边车比分摊后的普通容器更早被杀。
	if isSidecarContainer(pod, container) {
		// 先找出普通容器中最小的内存请求。
		minMemoryRequest := minRegularContainerMemory(pod)

		// 启用 PodLevelResources 时，计算边车容器的 minMemoryOomScoreAdjust
		// 还要加上未分配 Pod 内存请求的每容器分摊份额，确保其计算
		// 与普通容器的 Pod 级内存请求处理方式一致。
		if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources) &&
			resourcehelper.IsPodLevelRequestsSet(pod) {
			minMemoryRequest += remainingReqPerContainer
		}
		// 边车容器的 OOM 分数不得高于普通容器中的最低者。
		minMemoryOomScoreAdjust := 1000 - (1000*minMemoryRequest)/memoryCapacity
		// 边车容器的 OOM 分数应与 Pod 内普通容器的最低分数持平或更低。
		if oomScoreAdjust > minMemoryOomScoreAdjust {
			oomScoreAdjust = minMemoryOomScoreAdjust
		}
	}

	// 保证型 Pod 用满内存时分数可能低至 10，需确保可突发容器的分数高于此值。
	if int(oomScoreAdjust) < (1000 + guaranteedOOMScoreAdj) {
		return (1000 + guaranteedOOMScoreAdj)
	}
	// 让可突发容器始终比 Best-Effort 容器略占优势。
	if int(oomScoreAdjust) == besteffortOOMScoreAdj {
		return int(oomScoreAdjust - 1)
	}
	return int(oomScoreAdjust)
}

// isSidecarContainer 判断容器是否为边车容器。
// v1.Container 没有直接的边车标记，只能借助 RestartPolicy==Always 且位于 InitContainers 中推断。
func isSidecarContainer(pod *v1.Pod, container *v1.Container) bool {
	// 只有重启策略为 Always 的 init 容器才具备边车语义。
	if container.RestartPolicy != nil && *container.RestartPolicy == v1.ContainerRestartPolicyAlways {
		for _, initContainer := range pod.Spec.InitContainers {
			if initContainer.Name == container.Name {
				return true
			}
		}
	}
	return false
}

// getEffectiveContainerMemoryRequest 返回容器有效内存请求，包括启用 DRA 时的 DRA 内存分配。
// 若多个容器共享同一个 DRA 内存 claim，则按容器数均分。
func getEffectiveContainerMemoryRequest(pod *v1.Pod, container *v1.Container) int64 {
	memReq := container.Resources.Requests.Memory().Value()
	// 特性未启用时直接使用普通资源请求。
	if !utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources) {
		return memReq
	}

	var draMemoryShare int64
	// 只统计引用了该容器的 claim。
	for _, claimStatus := range pod.Status.NodeAllocatableResourceClaimStatuses {
		// 判断此容器是否引用了该 claim
		if !slices.Contains(claimStatus.Containers, container.Name) {
			continue
		}

		numRefs := int64(len(claimStatus.Containers))

		// Mapping 资源是 Pod 内引用容器共享的固定分配，按容器数均分。
		for _, mapping := range claimStatus.Mapping {
			if mapping.Name == v1.ResourceMemory && mapping.Quantity != nil {
				draMemoryShare += mapping.Quantity.Value() / numRefs
			}
		}

		// Overhead 中的 PerPod 部分按引用容器均分，PerContainer 部分全额计入。
		for _, overhead := range claimStatus.Overhead {
			if overhead.Name == v1.ResourceMemory {
				// PerPod overhead 每个 Pod 只计一次，在引用容器间均分。
				if overhead.PerPod != nil {
					draMemoryShare += overhead.PerPod.Value() / numRefs
				}
				// PerContainer overhead 按容器引用计，每个容器获得全额。
				if overhead.PerContainer != nil {
					draMemoryShare += overhead.PerContainer.Value()
				}
			}
		}
	}

	return memReq + draMemoryShare
}
