/*
Copyright 2019 The Kubernetes Authors.

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

package framework

import (
	"slices"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-helpers/resource"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
)

// 特殊事件标签。
const (
	// ScheduleAttemptFailure 表示一次调度尝试失败的事件。
	ScheduleAttemptFailure = "ScheduleAttemptFailure"
	// BackoffComplete 表示 Pod 退避结束的事件。
	BackoffComplete = "BackoffComplete"
	// PopFromBackoffQ 表示 activeQ 为空时从 backoffQ 中取出 Pod 的事件。
	PopFromBackoffQ = "PopFromBackoffQ"
	// ForceActivate 表示 Pod 从 unschedulableEntities/backoffQ 移到 activeQ 的事件。
	// 通常由插件实现触发。
	ForceActivate = "ForceActivate"
	// UnschedulableTimeout 表示 Pod 因 pod-max-in-unschedulable-pods-duration
	// 指定的超时而从 unschedulableEntities 中移出的事件。
	UnschedulableTimeout = "UnschedulableTimeout"
)

var (
	// EventAssignedPodAdd 表示新增一个已分配 Pod 的事件。
	EventAssignedPodAdd = fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Add}
	// EventAssignedPodUpdate 表示已分配 Pod 被更新的事件。
	EventAssignedPodUpdate = fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Update}
	// EventAssignedPodDelete 表示删除一个已分配 Pod 的事件。
	EventAssignedPodDelete = fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Delete}
	// EventUnscheduledPodAdd 表示新增一个未调度 Pod 的事件。
	EventUnscheduledPodAdd = fwk.ClusterEvent{Resource: fwk.UnscheduledPod, ActionType: fwk.Add}
	// EventUnscheduledPodUpdate 表示未调度 Pod 被更新的事件。
	EventUnscheduledPodUpdate = fwk.ClusterEvent{Resource: fwk.UnscheduledPod, ActionType: fwk.Update}
	// EventUnscheduledPodDelete 表示删除一个未调度 Pod 的事件。
	EventUnscheduledPodDelete = fwk.ClusterEvent{Resource: fwk.UnscheduledPod, ActionType: fwk.Delete}
	// EventTargetPodUpdate 表示目标 Pod 正在被更新的事件。
	EventTargetPodUpdate = fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.Update}
	// EventUnschedulableTimeout 表示 Pod 处于不可调度状态超过超时时间的事件。
	EventUnschedulableTimeout = fwk.ClusterEvent{Resource: fwk.WildCard, ActionType: fwk.All, CustomLabel: UnschedulableTimeout}
	// EventForceActivate 表示 Pod 从 unschedulableEntities/backoffQ 移到 activeQ 的事件。
	EventForceActivate = fwk.ClusterEvent{Resource: fwk.WildCard, ActionType: fwk.All, CustomLabel: ForceActivate}
)

// PodSchedulingPropertiesChange 解析 Pod 的更新并返回对应的 UpdatePodXYZ 事件。
// 后续新增 Pod 更新事件时，也需要在这里同步补充。
// isTargetPod 表示该 Pod 是否是本次更新事件直接作用的对象。
func PodSchedulingPropertiesChange(newPod *v1.Pod, oldPod *v1.Pod, isTargetPod bool) (events []fwk.ClusterEvent) {
	var r fwk.EventResource
	if newPod.Spec.NodeName != "" {
		r = fwk.AssignedPod
	} else if isTargetPod {
		r = fwk.TargetPod
	} else {
		r = fwk.UnscheduledPod
	}

	podChangeExtractors := []podChangeExtractor{
		extractPodLabelsChange,
		extractPodScaleDown,
		extractPodScaleUp,
		extractPodSchedulingGateEliminatedChange,
		extractPodTolerationChange,
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
		podChangeExtractors = append(podChangeExtractors, extractPodGeneratedResourceClaimChange)
	}

	for _, fn := range podChangeExtractors {
		if event := fn(newPod, oldPod); event != fwk.None {
			events = append(events, fwk.ClusterEvent{Resource: r, ActionType: event})
		}
	}

	if len(events) == 0 {
		// 没有找到具体事件时退回到通用的 Update 动作，
		// 这样只会触发注册了通用 Pod/Update 事件的插件。
		events = append(events, fwk.ClusterEvent{Resource: r, ActionType: fwk.Update})
	}

	return
}

// podChangeExtractor 从 Pod 更新中提取对应的调度相关动作类型。
type podChangeExtractor func(newPod *v1.Pod, oldPod *v1.Pod) fwk.ActionType

// extractPodScaleDown 解析 Pod 更新，若有资源请求被下调则返回 PodRequestScaledDown 事件。
func extractPodScaleDown(newPod, oldPod *v1.Pod) fwk.ActionType {
	opt := resource.PodResourcesOptions{
		UseStatusResources: utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling),
		InPlacePodLevelResourcesVerticalScalingEnabled: utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling),
	}
	newPodRequests := resource.PodRequests(newPod, opt)
	oldPodRequests := resource.PodRequests(oldPod, opt)

	for rName, oldReq := range oldPodRequests {
		newReq, ok := newPodRequests[rName]
		if !ok {
			// rName 的资源请求被移除。
			return fwk.UpdatePodScaleDown
		}

		if oldReq.MilliValue() > newReq.MilliValue() {
			// rName 的资源请求被下调。
			return fwk.UpdatePodScaleDown
		}
	}

	return fwk.None
}

// extractPodScaleUp 解析 Pod 更新，若有资源请求被上调则返回 PodRequestScaledUp 事件。
func extractPodScaleUp(newPod, oldPod *v1.Pod) fwk.ActionType {
	opt := resource.PodResourcesOptions{
		UseStatusResources: utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling),
		InPlacePodLevelResourcesVerticalScalingEnabled: utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling),
	}
	newPodRequests := resource.PodRequests(newPod, opt)
	oldPodRequests := resource.PodRequests(oldPod, opt)

	for rName, newReq := range newPodRequests {
		oldReq, ok := oldPodRequests[rName]
		if !ok {
			// 新增了 rName 的资源请求。
			return fwk.UpdatePodScaleUp
		}

		if newReq.MilliValue() > oldReq.MilliValue() {
			// rName 的资源请求被上调。
			return fwk.UpdatePodScaleUp
		}
	}

	return fwk.None
}

// extractPodLabelsChange 在 Pod 标签发生变化时返回 UpdatePodLabel 事件。
func extractPodLabelsChange(newPod *v1.Pod, oldPod *v1.Pod) fwk.ActionType {
	if isLabelChanged(newPod.GetLabels(), oldPod.GetLabels()) {
		return fwk.UpdatePodLabel
	}
	return fwk.None
}

// extractPodTolerationChange 在 Pod 容忍发生变化时返回 UpdatePodToleration 事件。
func extractPodTolerationChange(newPod *v1.Pod, oldPod *v1.Pod) fwk.ActionType {
	if len(newPod.Spec.Tolerations) != len(oldPod.Spec.Tolerations) {
		// Pod 新增了容忍。
		// 受 API 校验限制，用户只能新增容忍，不能修改或删除。
		// 因此只比较数量就足以发现变化。
		// 任何容忍的变化都可能让 Pod 变为可调度。
		return fwk.UpdatePodToleration
	}

	return fwk.None
}

// extractPodSchedulingGateEliminatedChange 在 Pod 的调度门禁被全部移除时返回对应事件。
func extractPodSchedulingGateEliminatedChange(newPod *v1.Pod, oldPod *v1.Pod) fwk.ActionType {
	if len(newPod.Spec.SchedulingGates) == 0 && len(oldPod.Spec.SchedulingGates) != 0 {
		// Pod 上的调度门禁被彻底移除。
		return fwk.UpdatePodSchedulingGatesEliminated
	}

	return fwk.None
}

// extractPodGeneratedResourceClaimChange 在 Pod 生成的 ResourceClaim 状态变化时返回对应事件。
func extractPodGeneratedResourceClaimChange(newPod *v1.Pod, oldPod *v1.Pod) fwk.ActionType {
	if !resourceclaim.PodStatusEqual(newPod.Status.ResourceClaimStatuses, oldPod.Status.ResourceClaimStatuses) ||
		!resourceclaim.PodExtendedStatusEqual(newPod.Status.ExtendedResourceClaimStatus, oldPod.Status.ExtendedResourceClaimStatus) {
		return fwk.UpdatePodGeneratedResourceClaim
	}

	return fwk.None
}

// NodeSchedulingPropertiesChange 解析节点的更新并返回对应的 UpdateNodeXYZ 事件。
func NodeSchedulingPropertiesChange(newNode *v1.Node, oldNode *v1.Node) (events []fwk.ClusterEvent) {
	nodeChangeExtracters := []nodeChangeExtractor{
		extractNodeSpecUnschedulableChange,
		extractNodeAllocatableChange,
		extractNodeLabelsChange,
		extractNodeTaintsChange,
		extractNodeConditionsChange,
		extractNodeAnnotationsChange,
		extractNodePreemptionPolicyChange,
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures) {
		nodeChangeExtracters = append(nodeChangeExtracters, extractNodeFeaturesChange)
	}

	for _, fn := range nodeChangeExtracters {
		if event := fn(newNode, oldNode); event != fwk.None {
			events = append(events, fwk.ClusterEvent{Resource: fwk.Node, ActionType: event})
		}
	}
	return
}

// nodeChangeExtractor 从节点更新中提取对应的调度相关动作类型。
type nodeChangeExtractor func(newNode *v1.Node, oldNode *v1.Node) fwk.ActionType

// extractNodePreemptionPolicyChange 在节点的 Pod 抢占策略变化时返回对应事件。
func extractNodePreemptionPolicyChange(newNode *v1.Node, oldNode *v1.Node) fwk.ActionType {
	if !equality.Semantic.DeepEqual(oldNode.Spec.PodPreemptionPolicy, newNode.Spec.PodPreemptionPolicy) {
		return fwk.UpdateNodePreemptionPolicy
	}
	return fwk.None
}

// extractNodeAllocatableChange 在节点可分配资源变化时返回对应事件。
func extractNodeAllocatableChange(newNode *v1.Node, oldNode *v1.Node) fwk.ActionType {
	if !equality.Semantic.DeepEqual(oldNode.Status.Allocatable, newNode.Status.Allocatable) {
		return fwk.UpdateNodeAllocatable
	}
	return fwk.None
}

// extractNodeLabelsChange 在节点标签变化时返回对应事件。
func extractNodeLabelsChange(newNode *v1.Node, oldNode *v1.Node) fwk.ActionType {
	if isLabelChanged(newNode.GetLabels(), oldNode.GetLabels()) {
		return fwk.UpdateNodeLabel
	}
	return fwk.None
}

// isLabelChanged 判断新旧标签集合是否不同。
func isLabelChanged(newLabels map[string]string, oldLabels map[string]string) bool {
	return !equality.Semantic.DeepEqual(newLabels, oldLabels)
}

// extractNodeTaintsChange 在节点污点变化时返回对应事件。
func extractNodeTaintsChange(newNode *v1.Node, oldNode *v1.Node) fwk.ActionType {
	if !equality.Semantic.DeepEqual(newNode.Spec.Taints, oldNode.Spec.Taints) {
		return fwk.UpdateNodeTaint
	}
	return fwk.None
}

// extractNodeConditionsChange 在节点 Condition 状态变化时返回对应事件。
func extractNodeConditionsChange(newNode *v1.Node, oldNode *v1.Node) fwk.ActionType {
	strip := func(conditions []v1.NodeCondition) map[v1.NodeConditionType]v1.ConditionStatus {
		conditionStatuses := make(map[v1.NodeConditionType]v1.ConditionStatus, len(conditions))
		for i := range conditions {
			conditionStatuses[conditions[i].Type] = conditions[i].Status
		}
		return conditionStatuses
	}
	// 只比较类型到状态的映射，忽略 LastTransitionTime 等无关字段的抖动。
	if !equality.Semantic.DeepEqual(strip(oldNode.Status.Conditions), strip(newNode.Status.Conditions)) {
		return fwk.UpdateNodeCondition
	}
	return fwk.None
}

// extractNodeSpecUnschedulableChange 在节点从不可调度变为可调度时返回污点更新事件。
func extractNodeSpecUnschedulableChange(newNode *v1.Node, oldNode *v1.Node) fwk.ActionType {
	if newNode.Spec.Unschedulable != oldNode.Spec.Unschedulable && !newNode.Spec.Unschedulable {
		// TODO：新增 UpdateNodeSpecUnschedulable ActionType。
		return fwk.UpdateNodeTaint
	}
	return fwk.None
}

// extractNodeAnnotationsChange 在节点注解变化时返回对应事件。
func extractNodeAnnotationsChange(newNode *v1.Node, oldNode *v1.Node) fwk.ActionType {
	if !equality.Semantic.DeepEqual(oldNode.GetAnnotations(), newNode.GetAnnotations()) {
		return fwk.UpdateNodeAnnotation
	}
	return fwk.None
}

// extractNodeFeaturesChange 在节点声明的特性变化时返回对应事件。
func extractNodeFeaturesChange(newNode *v1.Node, oldNode *v1.Node) fwk.ActionType {
	// DeclaredFeatures 保持有序，因此可以直接比较切片。
	if !slices.Equal(oldNode.Status.DeclaredFeatures, newNode.Status.DeclaredFeatures) {
		return fwk.UpdateNodeDeclaredFeature
	}
	return fwk.None
}
