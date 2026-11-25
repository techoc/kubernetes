/*
Copyright 2014 The Kubernetes Authors.

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

package status

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	runtimeutil "k8s.io/kubernetes/pkg/kubelet/kuberuntime/util"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
)

const (
	// UnknownContainerStatuses 表示所有容器状态未知。
	UnknownContainerStatuses = "UnknownContainerStatuses"
	// PodCompleted 表示所有相关容器都已成功结束。
	PodCompleted = "PodCompleted"
	// PodFailed 表示 Pod 已失败，容器也随之失败。
	PodFailed = "PodFailed"
	// ContainersNotReady 表示一个或多个容器未就绪。
	ContainersNotReady = "ContainersNotReady"
	// ContainersNotInitialized 表示一个或多个 init 容器尚未成功。
	ContainersNotInitialized = "ContainersNotInitialized"
	// ReadinessGatesNotReady 表示一个或多个 Pod 就绪门控未满足。
	ReadinessGatesNotReady = "ReadinessGatesNotReady"
	// RestartAllContainersStarted 表示有容器退出并触发了重启所有容器的动作。
	RestartAllContainersStarted = "RestartAllContainersStarted"
)

// GenerateContainersReadyCondition 生成 ContainersReady 条件：
// 所有容器（包括可重启的 init 容器）就绪时为 True。
func GenerateContainersReadyCondition(pod *v1.Pod, oldPodStatus *v1.PodStatus, containerStatuses []v1.ContainerStatus, podPhase v1.PodPhase) v1.PodCondition {
	// 先检查所有容器是否就绪。
	if containerStatuses == nil {
		return v1.PodCondition{
			Type:               v1.ContainersReady,
			ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.ContainersReady),
			Status:             v1.ConditionFalse,
			Reason:             UnknownContainerStatuses,
		}
	}
	unknownContainers := []string{}
	unreadyContainers := []string{}
	// 可重启的 init 容器在运行期间也参与就绪判断。

	for _, container := range pod.Spec.InitContainers {
		if !podutil.IsRestartableInitContainer(&container) {
			continue
		}

		if containerStatus, ok := podutil.GetContainerStatus(containerStatuses, container.Name); ok {
			if !containerStatus.Ready {
				unreadyContainers = append(unreadyContainers, container.Name)
			}
		} else {
			unknownContainers = append(unknownContainers, container.Name)
		}
	}

	// 普通容器同样必须全部就绪。
	for _, container := range pod.Spec.Containers {
		if containerStatus, ok := podutil.GetContainerStatus(containerStatuses, container.Name); ok {
			if !containerStatus.Ready {
				unreadyContainers = append(unreadyContainers, container.Name)
			}
		} else {
			unknownContainers = append(unknownContainers, container.Name)
		}
	}

	// 所有容器状态已知且 Pod 已成功时，直接返回 PodCompleted。
	if podPhase == v1.PodSucceeded && len(unknownContainers) == 0 {
		return generateContainersReadyConditionForTerminalPhase(pod, oldPodStatus, podPhase)
	}

	// Pod 失败时容器可能仍在终止过程中，显式把就绪条件置为 False。
	if podPhase == v1.PodFailed {
		return generateContainersReadyConditionForTerminalPhase(pod, oldPodStatus, podPhase)
	}

	// 汇总未知和未就绪容器，写入条件消息便于排查。
	unreadyMessages := []string{}
	if len(unknownContainers) > 0 {
		unreadyMessages = append(unreadyMessages, fmt.Sprintf("containers with unknown status: %s", unknownContainers))
	}
	if len(unreadyContainers) > 0 {
		unreadyMessages = append(unreadyMessages, fmt.Sprintf("containers with unready status: %s", unreadyContainers))
	}
	unreadyMessage := strings.Join(unreadyMessages, ", ")
	if unreadyMessage != "" {
		return v1.PodCondition{
			Type:               v1.ContainersReady,
			ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.ContainersReady),
			Status:             v1.ConditionFalse,
			Reason:             ContainersNotReady,
			Message:            unreadyMessage,
		}
	}

	return v1.PodCondition{
		Type:               v1.ContainersReady,
		ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.ContainersReady),
		Status:             v1.ConditionTrue,
	}
}

// GeneratePodReadyCondition 生成 Pod 的 Ready 条件：
// 所有容器就绪，且 ReadinessGates 指定的条件都为 True 时才为 True。
func GeneratePodReadyCondition(pod *v1.Pod, oldPodStatus *v1.PodStatus, conditions []v1.PodCondition, containerStatuses []v1.ContainerStatus, podPhase v1.PodPhase) v1.PodCondition {
	containersReady := GenerateContainersReadyCondition(pod, oldPodStatus, containerStatuses, podPhase)
	// ContainersReady 不为 True 时，Ready 直接复用其状态、原因和消息。
	if containersReady.Status != v1.ConditionTrue {
		return v1.PodCondition{
			Type:               v1.PodReady,
			ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.PodReady),
			Status:             containersReady.Status,
			Reason:             containersReady.Reason,
			Message:            containersReady.Message,
		}
	}

	// 检查 readiness gate 指定的条件，未满足时生成对应的消息。
	unreadyMessages := []string{}
	// 条件不存在或不为 True 都视为未就绪。
	for _, rg := range pod.Spec.ReadinessGates {
		_, c := podutil.GetPodConditionFromList(conditions, rg.ConditionType)
		if c == nil {
			unreadyMessages = append(unreadyMessages, fmt.Sprintf("corresponding condition of pod readiness gate %q does not exist.", string(rg.ConditionType)))
		} else if c.Status != v1.ConditionTrue {
			unreadyMessages = append(unreadyMessages, fmt.Sprintf("the status of pod readiness gate %q is not \"True\", but %v", string(rg.ConditionType), c.Status))
		}
	}

	// 任一 readiness gate 未满足，Ready 即为 False。
	if len(unreadyMessages) != 0 {
		unreadyMessage := strings.Join(unreadyMessages, ", ")
		return v1.PodCondition{
			Type:               v1.PodReady,
			ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.PodReady),
			Status:             v1.ConditionFalse,
			Reason:             ReadinessGatesNotReady,
			Message:            unreadyMessage,
		}
	}

	return v1.PodCondition{
		Type:               v1.PodReady,
		ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.PodReady),
		Status:             v1.ConditionTrue,
	}
}

// isInitContainerInitialized 判断 init 容器是否已完成：
// 可重启 init 容器看 Started，普通 init 容器看 Ready。
func isInitContainerInitialized(initContainer *v1.Container, containerStatus *v1.ContainerStatus) bool {
	if podutil.IsRestartableInitContainer(initContainer) {
		if containerStatus.Started == nil || !*containerStatus.Started {
			return false
		}
	} else { // 普通 init 容器
		if !containerStatus.Ready {
			return false
		}
	}
	return true
}

// GeneratePodInitializedCondition 生成 PodInitialized 条件：
// 所有 init 容器完成时为 True，否则为未初始化状态。
func GeneratePodInitializedCondition(pod *v1.Pod, oldPodStatus *v1.PodStatus, containerStatuses []v1.ContainerStatus, podPhase v1.PodPhase) v1.PodCondition {
	// 检查所有 init 容器是否完成。
	if containerStatuses == nil && len(pod.Spec.InitContainers) > 0 {
		return v1.PodCondition{
			Type:               v1.PodInitialized,
			ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.PodInitialized),
			Status:             v1.ConditionFalse,
			Reason:             UnknownContainerStatuses,
		}
	}

	unknownContainers := []string{}
	incompleteContainers := []string{}
	for _, container := range pod.Spec.InitContainers {
		containerStatus, ok := podutil.GetContainerStatus(containerStatuses, container.Name)
		if !ok {
			unknownContainers = append(unknownContainers, container.Name)
			continue
		}
		if !isInitContainerInitialized(&container, &containerStatus) {
			incompleteContainers = append(incompleteContainers, container.Name)
		}
	}

	// 所有 init 容器状态已知且 Pod 已成功时，直接返回 PodCompleted。
	if podPhase == v1.PodSucceeded && len(unknownContainers) == 0 {
		return v1.PodCondition{
			Type:               v1.PodInitialized,
			ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.PodInitialized),
			Status:             v1.ConditionTrue,
			Reason:             PodCompleted,
		}
	}

	// 只要已有普通容器启动过，就说明 Pod 曾经完成初始化；
	// 这样可以处理“Pod 已初始化、但可重启 init 容器正在重启”的场景。
	if kubecontainer.HasAnyRegularContainerStarted(&pod.Spec, containerStatuses) {
		return v1.PodCondition{
			Type:               v1.PodInitialized,
			ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.PodInitialized),
			Status:             v1.ConditionTrue,
		}
	}

	unreadyMessages := make([]string, 0, len(unknownContainers)+len(incompleteContainers))
	if len(unknownContainers) > 0 {
		unreadyMessages = append(unreadyMessages, fmt.Sprintf("containers with unknown status: %s", unknownContainers))
	}
	if len(incompleteContainers) > 0 {
		unreadyMessages = append(unreadyMessages, fmt.Sprintf("containers with incomplete status: %s", incompleteContainers))
	}
	unreadyMessage := strings.Join(unreadyMessages, ", ")
	if unreadyMessage != "" {
		// 就地重启期间 init 容器状态可能从 completed 变回 waiting，
		// 但已初始化的 Pod 不应回退为未初始化；条件为 True 时保持不变。
		if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
			for _, cond := range oldPodStatus.Conditions {
				if cond.Type == v1.PodInitialized && cond.Status == v1.ConditionTrue {
					return v1.PodCondition{
						Type:               v1.PodInitialized,
						ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.PodInitialized),
						Status:             v1.ConditionTrue,
					}
				}
			}
		}
		return v1.PodCondition{
			Type:               v1.PodInitialized,
			ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.PodInitialized),
			Status:             v1.ConditionFalse,
			Reason:             ContainersNotInitialized,
			Message:            unreadyMessage,
		}
	}

	return v1.PodCondition{
		Type:               v1.PodInitialized,
		ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.PodInitialized),
		Status:             v1.ConditionTrue,
	}
}

// GeneratePodReadyToStartContainersCondition 根据沙箱是否需要重建，
// 生成 PodReadyToStartContainers 条件。
func GeneratePodReadyToStartContainersCondition(pod *v1.Pod, oldPodStatus *v1.PodStatus, podStatus *kubecontainer.PodStatus) v1.PodCondition {
	newSandboxNeeded, _, _, reason := runtimeutil.PodSandboxChanged(pod, podStatus)
	// 不需要新建沙箱，说明已存在配置好网络的沙箱，可以启动容器；
	// 否则 kubelet 必须先让运行时创建新沙箱并配置网络。
	if !newSandboxNeeded {
		return v1.PodCondition{
			Type:               v1.PodReadyToStartContainers,
			ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.PodReadyToStartContainers),
			Status:             v1.ConditionTrue,
		}
	}
	return v1.PodCondition{
		Type:               v1.PodReadyToStartContainers,
		ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.PodReadyToStartContainers),
		Status:             v1.ConditionFalse,
		Reason:             kubetypes.PodSandboxNotReadyReason,
		Message:            reason,
	}
}

// generateContainersReadyConditionForTerminalPhase 为已终止的 Pod 生成 ContainersReady=False，
// 并按成功/失败标记原因。
func generateContainersReadyConditionForTerminalPhase(pod *v1.Pod, oldPodStatus *v1.PodStatus, podPhase v1.PodPhase) v1.PodCondition {
	condition := v1.PodCondition{
		Type:               v1.ContainersReady,
		ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.ContainersReady),
		Status:             v1.ConditionFalse,
	}

	if podPhase == v1.PodFailed {
		condition.Reason = PodFailed
	} else if podPhase == v1.PodSucceeded {
		condition.Reason = PodCompleted
	}

	return condition
}

// generatePodReadyConditionForTerminalPhase 为已终止的 Pod 生成 PodReady=False，
// 并按成功/失败标记原因。
func generatePodReadyConditionForTerminalPhase(pod *v1.Pod, oldPodStatus *v1.PodStatus, podPhase v1.PodPhase) v1.PodCondition {
	condition := v1.PodCondition{
		Type:               v1.PodReady,
		ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(oldPodStatus, pod.Generation, v1.PodReady),
		Status:             v1.ConditionFalse,
	}

	if podPhase == v1.PodFailed {
		condition.Reason = PodFailed
	} else if podPhase == v1.PodSucceeded {
		condition.Reason = PodCompleted
	}

	return condition
}

// GenerateAllContainersRestartingCondition 生成 AllContainersRestarting 条件：
// 仅在容器退出触发整体重启且清理尚未完成时为 True。
func GenerateAllContainersRestartingCondition(pod *v1.Pod, podStatus *kubecontainer.PodStatus, oldPodStatus *v1.PodStatus, podPhase v1.PodPhase) v1.PodCondition {
	if podPhase == v1.PodSucceeded {
		return v1.PodCondition{
			Type:   v1.AllContainersRestarting,
			Status: v1.ConditionFalse,
			Reason: PodCompleted,
		}
	}
	if podPhase == v1.PodFailed {
		return v1.PodCondition{
			Type:   v1.AllContainersRestarting,
			Status: v1.ConditionFalse,
			Reason: PodFailed,
		}
	}

	// 没有触发整体重启时，条件为 False。
	if !kubecontainer.ShouldAllContainersRestart(pod, podStatus, oldPodStatus) {
		return v1.PodCondition{
			Type:   v1.AllContainersRestarting,
			Status: v1.ConditionFalse,
		}
	}
	// 清理完成说明重启流程已结束，条件回到 False。
	if kubecontainer.AllContainersRestartCleanedUp(pod, podStatus) {
		return v1.PodCondition{
			Type:   v1.AllContainersRestarting,
			Status: v1.ConditionFalse,
		}
	}
	return v1.PodCondition{
		Type:    v1.AllContainersRestarting,
		Status:  v1.ConditionTrue,
		Reason:  RestartAllContainersStarted,
		Message: "container exited with restart policy rule",
	}
}
