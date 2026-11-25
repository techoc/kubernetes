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

package allocation

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
)

// NewPodResizesAdmitHandler 返回用于评估 Pod resize 能否被 kubelet 分配的 PodAdmitHandler。
func NewPodResizesAdmitHandler(containerManager cm.ContainerManager, containerRuntime kubecontainer.Runtime, allocationManager Manager) lifecycle.PodAdmitHandler {
	return &podResizesAdmitHandler{
		containerManager:  containerManager,
		containerRuntime:  containerRuntime,
		allocationManager: allocationManager,
	}
}

// podResizesAdmitHandler 是 PodAdmitHandler 的实现。
type podResizesAdmitHandler struct {
	containerManager  cm.ContainerManager
	containerRuntime  kubecontainer.Runtime
	allocationManager Manager
}

// Admit 在 resize 操作时校验各种限制；非 resize 操作直接放行。
func (h *podResizesAdmitHandler) Admit(ctx context.Context, attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	// 只有 resize 操作需要额外校验，其余直接准入。
	if attrs.Operation != lifecycle.ResizeOperation {
		return lifecycle.PodAdmitResult{Admit: true}
	}

	logger := klog.FromContext(ctx)
	pod := attrs.Pod
	// 取已分配资源视图，作为比较基线。
	allocatedPod, _ := h.allocationManager.UpdatePodFromAllocation(pod)
	if resizable, msg, reason := IsInPlacePodVerticalScalingAllowed(pod); !resizable {
		// 存在待处理的 resize 但不被允许时，始终使用已分配资源，拒绝本次变更。
		metrics.PodInfeasibleResizes.WithLabelValues(reason).Inc()
		return lifecycle.PodAdmitResult{Admit: false, Reason: v1.PodReasonInfeasible, Message: msg}
	}
	if resizeNotAllowed, msg := disallowResizeForSwappableContainers(h.containerRuntime, pod, allocatedPod); resizeNotAllowed {
		// 若本次 resize 涉及 swap 重算，则判定不可行：beta 阶段不支持带 swap 的原地扩缩容。
		metrics.PodInfeasibleResizes.WithLabelValues("swap_limitation").Inc()
		return lifecycle.PodAdmitResult{Admit: false, Reason: v1.PodReasonInfeasible, Message: msg}
	}
	// Pod 级资源发生变化时，同样校验 Pod 级原地扩缩容是否允许。
	if !apiequality.Semantic.DeepEqual(pod.Spec.Resources, allocatedPod.Spec.Resources) {
		if resizable, msg, reason := IsInPlacePodLevelResourcesVerticalScalingAllowed(pod); !resizable {
			// 存在待处理的 Pod 级资源 resize 但不被允许时，始终使用已分配资源。
			metrics.PodInfeasibleResizes.WithLabelValues(reason).Inc()
			return lifecycle.PodAdmitResult{Admit: false, Reason: v1.PodReasonInfeasible, Message: msg}
		}
	}

	// 内存支撑卷（emptyDir）的 resize 受特性门控与 cgroup 版本限制。
	if disallowed, msg, reason := disallowResizeForMemoryBackedVolumes(pod, allocatedPod, h.containerManager.GetNodeConfig().CgroupVersion); disallowed {
		logger.V(3).Info(msg, "pod", format.Pod(pod))
		metrics.PodInfeasibleResizes.WithLabelValues(reason).Inc()
		return lifecycle.PodAdmitResult{Admit: false, Reason: v1.PodReasonInfeasible, Message: msg}
	}

	// Guaranteed Pod 与 static CPU/Memory manager 组合下，
	// 除非对应特性开启，否则拒绝 resize。
	if v1qos.GetPodQOS(pod) == v1.PodQOSGuaranteed {
		if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingExclusiveCPUs) &&
			h.containerManager.GetNodeConfig().CPUManagerPolicy == string(cpumanager.PolicyStatic) &&
			h.guaranteedPodResourceResizeRequired(pod, v1.ResourceCPU) {
			msg := fmt.Sprintf("Resize is infeasible for Guaranteed Pods alongside CPU Manager policy \"%s\"", string(cpumanager.PolicyStatic))
			logger.V(3).Info(msg, "pod", format.Pod(pod))
			metrics.PodInfeasibleResizes.WithLabelValues("guaranteed_pod_cpu_manager_static_policy").Inc()
			return lifecycle.PodAdmitResult{Admit: false, Reason: v1.PodReasonInfeasible, Message: msg}
		}
		if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingExclusiveMemory) &&
			h.containerManager.GetNodeConfig().MemoryManagerPolicy == string(memorymanager.PolicyTypeStatic) &&
			h.guaranteedPodResourceResizeRequired(pod, v1.ResourceMemory) {
			msg := fmt.Sprintf("Resize is infeasible for Guaranteed Pods alongside Memory Manager policy \"%s\"", string(memorymanager.PolicyTypeStatic))
			logger.V(3).Info(msg, "pod", format.Pod(pod))
			metrics.PodInfeasibleResizes.WithLabelValues("guaranteed_pod_memory_manager_static_policy").Inc()
			return lifecycle.PodAdmitResult{Admit: false, Reason: v1.PodReasonInfeasible, Message: msg}
		}
	}

	return lifecycle.PodAdmitResult{Admit: true}
}

// disallowResizeForMemoryBackedVolumes 判断内存支撑卷的 resize 是否被禁止，并返回原因。
func disallowResizeForMemoryBackedVolumes(desiredPod, allocatedPod *v1.Pod, cgroupVersion int) (bool, string, string) {
	if desiredPod == nil || allocatedPod == nil {
		return false, "", ""
	}

	// 只有实际请求变更了 emptyDir 大小限制才需要检查。
	if IsMemoryBackedVolumeResizeRequested(allocatedPod, desiredPod) {
		if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) {
			return true, "Memory-backed emptyDir volume resize is disabled", "memory_volume_resize_gate_off"
		}
		if cgroupVersion == 1 {
			return true, "Memory-backed emptyDir volume resize is not supported on cgroups v1", "cgroups_v1_unsupported"
		}
	}

	return false, "", ""
}

// disallowResizeForSwappableContainers 判断涉及 swap 的容器 resize 是否被禁止。
func disallowResizeForSwappableContainers(runtime kubecontainer.Runtime, desiredPod, allocatedPod *v1.Pod) (bool, string) {
	if desiredPod == nil || allocatedPod == nil {
		return false, ""
	}
	// 只有内存 resize 策略为 RestartContainer 时才允许带 swap 的 resize。
	restartableMemoryResizePolicy := func(resizePolicies []v1.ContainerResizePolicy) bool {
		for _, policy := range resizePolicies {
			if policy.ResourceName == v1.ResourceMemory {
				return policy.RestartPolicy == v1.RestartContainer
			}
		}
		return false
	}
	// 建立已分配容器索引，按名称与期望容器配对。
	allocatedContainers := make(map[string]v1.Container)
	for _, container := range append(allocatedPod.Spec.Containers, allocatedPod.Spec.InitContainers...) {
		allocatedContainers[container.Name] = container
	}
	for _, desiredContainer := range append(desiredPod.Spec.Containers, desiredPod.Spec.InitContainers...) {
		allocatedContainer, ok := allocatedContainers[desiredContainer.Name]
		if !ok {
			continue
		}
		origMemRequest := desiredContainer.Resources.Requests[v1.ResourceMemory]
		newMemRequest := allocatedContainer.Resources.Requests[v1.ResourceMemory]
		// 内存请求变化且不可重启时，检查双方 swap 行为。
		if !origMemRequest.Equal(newMemRequest) && !restartableMemoryResizePolicy(allocatedContainer.ResizePolicy) {
			aSwapBehavior := runtime.GetContainerSwapBehavior(desiredPod, &desiredContainer)
			bSwapBehavior := runtime.GetContainerSwapBehavior(allocatedPod, &allocatedContainer)
			if aSwapBehavior != kubetypes.NoSwap || bSwapBehavior != kubetypes.NoSwap {
				return true, "In-place resize of containers with swap is not supported."
			}
		}
	}
	return false, ""
}

// guaranteedPodResourceResizeRequired 判断 Guaranteed Pod 的指定资源是否发生了 resize。
func (h *podResizesAdmitHandler) guaranteedPodResourceResizeRequired(pod *v1.Pod, resourceName v1.ResourceName) bool {
	for container, containerType := range podutil.ContainerIter(&pod.Spec, podutil.InitContainers|podutil.Containers) {
		if !IsResizableContainer(container, containerType) {
			continue
		}
		requestedResources := container.Resources
		allocatedresources, _ := h.allocationManager.GetContainerResourceAllocation(pod.UID, container.Name)
		// Guaranteed Pod 的 requests 必须等于 limits，因此只比较 requests 即可。
		if !requestedResources.Requests[resourceName].Equal(allocatedresources.Requests[resourceName]) {
			return true
		}
	}
	return false
}

// IsMemoryBackedVolumeResizeRequested 判断 Pod 的任一内存支撑 emptyDir 卷的大小限制是否变化。
func IsMemoryBackedVolumeResizeRequested(desiredPod, allocatedPod *v1.Pod) bool {
	if len(allocatedPod.Spec.Volumes) != len(desiredPod.Spec.Volumes) {
		// 校验应当阻止卷数量不一致；若仍发生，则按无 resize 处理。
		return false
	}
	for i, allocatedVol := range allocatedPod.Spec.Volumes {
		if !VolHasMemoryBackedEmptyDirSizeLimit(&allocatedVol) {
			continue
		}
		desiredVol := desiredPod.Spec.Volumes[i]
		if !apiequality.Semantic.DeepEqual(allocatedVol.EmptyDir.SizeLimit, desiredVol.EmptyDir.SizeLimit) {
			return true
		}
	}
	return false
}
