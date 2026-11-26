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
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	resourcehelper "k8s.io/component-helpers/resource"
)

var supportedQoSComputeResources = sets.New(core.ResourceCPU, core.ResourceMemory)

// 返回 PodStatus.QOSClass 中保存的 QoS 类别。
// 字段为空时调用 ComputePodQOS() 现场计算。
func GetPodQOS(pod *core.Pod) core.PodQOSClass {
	if pod.Status.QOSClass != "" {
		return pod.Status.QOSClass
	}
	return ComputePodQOS(pod)
}

// 根据容器列表计算 Pod 的 QoS 类别。开销比 GetPodQOS 大，
// .Status.QOSClass 非空时应优先用 GetPodQOS。
// 所有容器都未指定 CPU/内存 requests 或 limits 时为 BestEffort。
// 仅当所有容器的 CPU、内存 requests 与 limits 都存在且相等时为 Guaranteed。
// 各容器的 CPU、内存 requests 与 limits 不完全相等时为 Burstable。
func ComputePodQOS(pod *core.Pod) core.PodQOSClass {
	// 指定 Pod 级资源时以它为准判定 QoS。
	if resourcehelper.IsPodLevelResourcesSet(pod) {
		return requirementsQOS(pod.Spec.Resources)
	}

	// 遍历 Init 容器和普通容器。
	containerIter := func(yield func(*core.Container) bool) {
		for _, c := range pod.Spec.InitContainers {
			if !yield(&c) {
				return
			}
		}
		for _, c := range pod.Spec.Containers {
			if !yield(&c) {
				return
			}
		}
	}

	var podQOS core.PodQOSClass
	for container := range containerIter {
		containerQOS := requirementsQOS(&container.Resources)
		if containerQOS == core.PodQOSBurstable {
			return containerQOS // If any container is Burstable, we know the pod isn't BestEffort or Guaranteed
		} else if podQOS == "" {
			podQOS = containerQOS
		} else if podQOS != containerQOS {
			return core.PodQOSBurstable // If one container is BestEffort and another is Guaranteed, the pod is Burstable
		}
	}
	if podQOS == "" { // This can only happen if there aren't any containers (not possible in production).
		podQOS = core.PodQOSBestEffort
	}
	return podQOS
}

// 根据单组资源需求判定 QoSClass，
// 最终结果可能需要聚合得到 Pod 的 QoS。
func requirementsQOS(resources *core.ResourceRequirements) core.PodQOSClass {
	if len(resources.Requests) == 0 && len(resources.Limits) == 0 {
		return core.PodQOSBestEffort
	}

	var qos core.PodQOSClass
	for res := range supportedQoSComputeResources {
		resQOS := resourceQOS(resources, res)
		if resQOS == core.PodQOSBurstable {
			return resQOS // If any resource is Burstable, we know the pod isn't BestEffort or Guaranteed
		} else if qos == "" {
			qos = resQOS
		} else if qos != resQOS {
			// 不一致说明只指定了部分 QoS 资源，因此 Pod 必为
			// Burstable。
			return core.PodQOSBurstable
		}
	}
	return qos
}

// 判定单个资源在需求中的 QoS 形态：
// - BestEffort：Request 和 Limit 均为 0；
// - Burstable：Request 与 Limit 不等；
// - Guaranteed：Request 与 Limit 相等且非 0。
func resourceQOS(resources *core.ResourceRequirements, res core.ResourceName) core.PodQOSClass {
	req := resources.Requests[res]
	lim := resources.Limits[res]

	if !req.Equal(lim) {
		// 不相等时至少有一个非 0，因此既非 Guaranteed 也非 BestEffort。
		return core.PodQOSBurstable
	} else if req.IsZero() {
		// req == lim，无需再检查 lim.IsZero()。
		return core.PodQOSBestEffort
	} else {
		// req == lim != 0。
		return core.PodQOSGuaranteed
	}
}
