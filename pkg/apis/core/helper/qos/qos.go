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

// 注意：不要通过 client-go 使用这些辅助函数，其包路径将来会发生变化。
package qos

import (
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	"k8s.io/kubernetes/pkg/features"
)

var supportedQoSComputeResources = sets.New(core.ResourceCPU, core.ResourceMemory)

// GetPodQOS 返回持久化在 PodStatus.QOSClass 字段中的 Pod QoS 类。
// 若 PodStatus.QOSClass 为空，则返回 ComputePodQOS() 计算出的 QoS 类。
func GetPodQOS(pod *core.Pod) core.PodQOSClass {
	if pod.Status.QOSClass != "" {
		return pod.Status.QOSClass
	}
	return ComputePodQOS(pod)
}

// ComputePodQOS 通过遍历容器列表计算 Pod 的 QoS 类。该函数比 GetPodQOS 开销更大，
// 对于 .Status.QOSClass 非空的 Pod 应优先使用 GetPodQOS。
// 若所有容器都未指定任何 CPU 或内存 request/limit，则 Pod 为 BestEffort。
// 仅当所有容器都指定了 CPU 与内存的 request 和 limit 且两者相等时，Pod 才是 Guaranteed。
// 若各容器的 CPU/内存 limit 与 request 不一致，则 Pod 为 Burstable。
// 注意：相关测试位于 pkg/apis/core/v1/helper/qos/qos_test.go。
func ComputePodQOS(pod *core.Pod) core.PodQOSClass {
	// 指定 Pod 级资源时，用它们来确定 QoS 类。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources) &&
		(pod.Spec.Resources != nil && (!utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourcesFixKubeletQOSClass) || helper.IsPodLevelResourcesSet(pod))) {
		return requirementsQOS(pod.Spec.Resources)
	}

	// 遍历 init 容器与主容器。
	// 这里不能使用 podutil.ContainerIter，否则会产生循环依赖。
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
			return containerQOS // 任一容器为 Burstable，则 Pod 不可能是 BestEffort 或 Guaranteed
		} else if podQOS == "" {
			podQOS = containerQOS
		} else if podQOS != containerQOS {
			return core.PodQOSBurstable // 一个容器 BestEffort、另一个 Guaranteed 时，Pod 为 Burstable
		}
	}
	if podQOS == "" { // 只可能发生在没有任何容器时（生产环境中不可能）。
		podQOS = core.PodQOSBestEffort
	}
	return podQOS
}

// requirementsQOS 根据单组资源需求计算 QoS 类，可能需要进一步聚合才能得到 Pod 的 QoS。
func requirementsQOS(resources *core.ResourceRequirements) core.PodQOSClass {
	if len(resources.Requests) == 0 && len(resources.Limits) == 0 {
		return core.PodQOSBestEffort
	}

	var qos core.PodQOSClass
	for res := range supportedQoSComputeResources {
		resQOS := resourceQOS(resources, res)
		if resQOS == core.PodQOSBurstable {
			return resQOS // 任一资源为 Burstable，则 Pod 不可能是 BestEffort 或 Guaranteed
		} else if qos == "" {
			qos = resQOS
		} else if qos != resQOS {
			// 不同资源的 QoS 不一致，说明只指定了一部分 QoS 资源，Pod 必为 Burstable。
			return core.PodQOSBurstable
		}
	}
	return qos
}

// resourceQOS 判定给定资源在资源需求中的 QoS“形态”：
// - BestEffort：Request 与 Limit 均为零
// - Burstable：Request != Limit
// - Guaranteed：Request 与 Limit 相等且非零
func resourceQOS(resources *core.ResourceRequirements, res core.ResourceName) core.PodQOSClass {
	req := resources.Requests[res]
	lim := resources.Limits[res]

	if !req.Equal(lim) {
		// 不相等说明至少一个非零，因此既不是 Guaranteed 也不是 BestEffort。
		return core.PodQOSBurstable
	} else if req.IsZero() {
		// req == lim，无需再检查 lim.IsZero()。
		return core.PodQOSBestEffort
	} else {
		// req == lim != 0
		return core.PodQOSGuaranteed
	}
}
