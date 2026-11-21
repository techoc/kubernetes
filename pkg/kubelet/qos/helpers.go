/*
Copyright 2024 The Kubernetes Authors.

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

// Package qos 包含服务质量相关的辅助函数。
// 对每种资源（内存、CPU），kubelet 支持三类容器：
// Guaranteed 容器优先级最高，能获得其所需的全部资源；
// Burstable 容器保证获得 request，并可在资源富余时突发使用更多资源；
// Best-Effort 容器未指定 request，只能使用其他 Pod 未占用的资源。

package qos

import (
	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	"k8s.io/kubernetes/pkg/features"
)

// minRegularContainerMemory 返回 Pod.Spec.Containers 中所有普通容器的最小内存请求，
// 不包含 init 容器（无论是否可重启）。
func minRegularContainerMemory(pod *v1.Pod) int64 {
	memoryValue := getEffectiveContainerMemoryRequest(pod, &pod.Spec.Containers[0])
	for i := 1; i < len(pod.Spec.Containers); i++ {
		effectiveMem := getEffectiveContainerMemoryRequest(pod, &pod.Spec.Containers[i])
		if effectiveMem < memoryValue {
			memoryValue = effectiveMem
		}
	}
	return memoryValue
}

// remainingPodMemReqPerContainer 计算 Pod 级内存请求中未分配给容器、
// 需要均摊到每个容器的额外内存，用于 OOM score 调整。
func remainingPodMemReqPerContainer(pod *v1.Pod) int64 {
	var remainingMemory int64
	if pod.Spec.Resources.Requests.Memory().IsZero() {
		return remainingMemory
	}
	numContainers := len(pod.Spec.Containers) + len(pod.Spec.InitContainers)

	opts := resourcehelper.PodResourcesOptions{
		UseDRANodeAllocatableResourceClaimStatus: utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources),
	}
	aggrContainerReqs := resourcehelper.AggregateContainerRequests(pod, opts)
	// 统计所有容器（启用 DRA 时包含 DRA 分配）的聚合请求。
	remainingMemory = pod.Spec.Resources.Requests.Memory().Value() - aggrContainerReqs.Memory().Value()
	remainingMemory = max(remainingMemory, 0)
	remainingMemoryPerContainer := remainingMemory / int64(numContainers)
	return remainingMemoryPerContainer
}
