/*
Copyright 2021 The Kubernetes Authors.

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

package job

import (
	"fmt"
	"slices"

	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
)

// matchPodFailurePolicy 返回给定失败 Pod 与 Pod 失败策略规则的匹配信息：
//   - 可选的 Job 失败消息（仅当 Pod 命中 'FailJob' 规则时存在）；
//   - 一个布尔值，表示失败是否应计入 backoffLimit（若指定，
//     也计入 backoffLimitPerIndex）。命中 'Ignore' 规则时不应计入；
//   - 指向命中的 Pod 失败策略动作的指针。
func matchPodFailurePolicy(podFailurePolicy *batch.PodFailurePolicy, failedPod *v1.Pod) (*string, bool, *batch.PodFailurePolicyAction) {
	// 未配置策略时沿用默认行为：计入失败次数，不产生失败消息。
	if podFailurePolicy == nil {
		return nil, true, nil
	}
	ignore := batch.PodFailurePolicyActionIgnore
	failJob := batch.PodFailurePolicyActionFailJob
	failIndex := batch.PodFailurePolicyActionFailIndex
	count := batch.PodFailurePolicyActionCount
	for index, podFailurePolicyRule := range podFailurePolicy.Rules {
		// 规则按索引顺序检查，首个命中的规则生效；消息中的 index 便于
		// 用户定位是哪条规则触发了失败。
		if podFailurePolicyRule.OnExitCodes != nil {
			if containerStatus := matchOnExitCodes(&failedPod.Status, podFailurePolicyRule.OnExitCodes); containerStatus != nil {
				switch podFailurePolicyRule.Action {
				case batch.PodFailurePolicyActionIgnore:
					return nil, false, &ignore
				case batch.PodFailurePolicyActionFailIndex:
					return nil, true, &failIndex
				case batch.PodFailurePolicyActionCount:
					return nil, true, &count
				case batch.PodFailurePolicyActionFailJob:
					msg := fmt.Sprintf("Container %s for pod %s/%s failed with exit code %v matching %v rule at index %d",
						containerStatus.Name, failedPod.Namespace, failedPod.Name, containerStatus.State.Terminated.ExitCode, podFailurePolicyRule.Action, index)
					return &msg, true, &failJob
				}
			}
		} else if podFailurePolicyRule.OnPodConditions != nil {
			if podCondition := matchOnPodConditions(&failedPod.Status, podFailurePolicyRule.OnPodConditions); podCondition != nil {
				switch podFailurePolicyRule.Action {
				case batch.PodFailurePolicyActionIgnore:
					return nil, false, &ignore
				case batch.PodFailurePolicyActionFailIndex:
					return nil, true, &failIndex
				case batch.PodFailurePolicyActionCount:
					return nil, true, &count
				case batch.PodFailurePolicyActionFailJob:
					msg := fmt.Sprintf("Pod %s/%s has condition %v matching %v rule at index %d",
						failedPod.Namespace, failedPod.Name, podCondition.Type, podFailurePolicyRule.Action, index)
					return &msg, true, &failJob
				}
			}
		}
	}
	return nil, true, nil
}

// matchOnExitCodes 返回首个匹配退出码要求的已终止容器状态（若存在）。
// 若返回状态非 nil，则其 Terminated 字段必非 nil。
func matchOnExitCodes(podStatus *v1.PodStatus, requirement *batch.PodFailurePolicyOnExitCodesRequirement) *v1.ContainerStatus {
	// 先查普通容器再查 init 容器，保证匹配顺序确定。
	if containerStatus := getMatchingContainerFromList(podStatus.ContainerStatuses, requirement); containerStatus != nil {
		return containerStatus
	}
	return getMatchingContainerFromList(podStatus.InitContainerStatuses, requirement)
}

// matchOnPodConditions 返回首个同时满足类型和状态模式的 Pod 条件（若存在）。
func matchOnPodConditions(podStatus *v1.PodStatus, requirement []batch.PodFailurePolicyOnPodConditionsPattern) *v1.PodCondition {
	for _, podCondition := range podStatus.Conditions {
		for _, pattern := range requirement {
			if podCondition.Type == pattern.Type && podCondition.Status == pattern.Status {
				return &podCondition
			}
		}
	}
	return nil
}

// getMatchingContainerFromList 返回列表中首个匹配退出码要求的已终止容器状态，
// 没有匹配项则返回 nil。若返回状态非 nil，则其 Terminated 字段必非 nil。
func getMatchingContainerFromList(containerStatuses []v1.ContainerStatus, requirement *batch.PodFailurePolicyOnExitCodesRequirement) *v1.ContainerStatus {
	for _, containerStatus := range containerStatuses {
		if containerStatus.State.Terminated == nil {
			// 容器仍在运行中，没有退出码可供匹配，跳过。
			continue
		}
		if requirement.ContainerName == nil || *requirement.ContainerName == containerStatus.Name {
			if containerStatus.State.Terminated.ExitCode != 0 {
				if isOnExitCodesOperatorMatching(containerStatus.State.Terminated.ExitCode, requirement) {
					return &containerStatus
				}
			}
		}
	}
	return nil
}

// isOnExitCodesOperatorMatching 按 In/NotIn 操作符判断退出码是否命中要求的值集合。
func isOnExitCodesOperatorMatching(exitCode int32, requirement *batch.PodFailurePolicyOnExitCodesRequirement) bool {
	switch requirement.Operator {
	case batch.PodFailurePolicyOnExitCodesOpIn:
		return slices.Contains(requirement.Values, exitCode)
	case batch.PodFailurePolicyOnExitCodesOpNotIn:
		return !slices.Contains(requirement.Values, exitCode)
	default:
		return false
	}
}
