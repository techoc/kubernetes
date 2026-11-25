/*
Copyright 2016 The Kubernetes Authors.

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

package policy

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// PodDisruptionBudgetSpec 描述 PodDisruptionBudget 的期望配置。
type PodDisruptionBudgetSpec struct {
	// An eviction is allowed if at least "minAvailable" pods selected by
	// "selector" will still be available after the eviction, i.e. even in the
	// absence of the evicted pod.  So for example you can prevent all voluntary
	// evictions by specifying "100%".
	// +optional
	MinAvailable *intstr.IntOrString

	// Label query over pods whose evictions are managed by the disruption
	// budget.
	// +optional
	Selector *metav1.LabelSelector

	// An eviction is allowed if at most "maxUnavailable" pods selected by
	// "selector" are unavailable after the eviction, i.e. even in absence of
	// the evicted pod. For example, one can prevent all voluntary evictions
	// by specifying 0. This is a mutually exclusive setting with "minAvailable".
	// +optional
	MaxUnavailable *intstr.IntOrString

	// UnhealthyPodEvictionPolicy 定义了何时应将不健康的 Pod 纳入驱逐考量。
	// 当前实现将健康 Pod 视为 status.conditions 中存在 type="Ready"、
	// status="True" 条目的 Pod。
	//
	// 合法的策略为 IfHealthyBudget 和 AlwaysAllow。
	// 若未指定策略，则使用默认行为，
	// 等同于 IfHealthyBudget 策略。
	//
	// IfHealthyBudget 策略表示：正在运行（status.phase="Running"）但尚未健康的
	// Pod，仅当受保护的应用未处于中断状态（status.currentHealthy 至少等于
	// status.desiredHealthy）时才可被驱逐。
	// 健康的 Pod 会受到 PDB 的驱逐保护。
	//
	// AlwaysAllow 策略表示：所有正在运行（status.phase="Running"）但尚未健康的
	// Pod 都被视为处于中断状态，无论是否满足 PDB 中的条件都可以被驱逐。
	// 这意味着已中断应用的运行中 Pod 可能没有机会恢复健康。
	// 健康的 Pod 会受到 PDB 的驱逐保护。
	//
	// 未来可能添加更多策略。
	// 若在此字段中遇到无法识别的策略，做出驱逐决定的客户端
	// 应禁止驱逐不健康的 Pod。
	// +optional
	UnhealthyPodEvictionPolicy *UnhealthyPodEvictionPolicyType
}

// UnhealthyPodEvictionPolicyType defines the criteria for when unhealthy pods
// should be considered for eviction.
// +enum
type UnhealthyPodEvictionPolicyType string

const (
	// IfHealthyBudget 策略表示：正在运行（status.phase="Running"）但尚未健康的
	// Pod，仅当受保护的应用未处于中断状态（status.currentHealthy 至少等于
	// status.desiredHealthy）时才可被驱逐。
	// 健康的 Pod 会受到 PDB 的驱逐保护。
	IfHealthyBudget UnhealthyPodEvictionPolicyType = "IfHealthyBudget"

	// AlwaysAllow 策略表示：所有正在运行（status.phase="Running"）但尚未健康的
	// Pod 都被视为处于中断状态，无论是否满足 PDB 中的条件都可以被驱逐。
	// 这意味着已中断应用的运行中 Pod 可能没有机会恢复健康。
	// 健康的 Pod 会受到 PDB 的驱逐保护。
	AlwaysAllow UnhealthyPodEvictionPolicyType = "AlwaysAllow"
)

// PodDisruptionBudgetStatus represents information about the status of a
// PodDisruptionBudget. Status may trail the actual state of a system.
type PodDisruptionBudgetStatus struct {
	// Most recent generation observed when updating this PDB status. DisruptionsAllowed and other
	// status information is valid only if observedGeneration equals to PDB's object generation.
	// +optional
	ObservedGeneration int64

	// DisruptedPods contains information about pods whose eviction was
	// processed by the API server eviction subresource handler but has not
	// yet been observed by the PodDisruptionBudget controller.
	// A pod will be in this map from the time when the API server processed the
	// eviction request to the time when the pod is seen by PDB controller
	// as having been marked for deletion (or after a timeout). The key in the map is the name of the pod
	// and the value is the time when the API server processed the eviction request. If
	// the deletion didn't occur and a pod is still there it will be removed from
	// the list automatically by PodDisruptionBudget controller after some time.
	// If everything goes smooth this map should be empty for the most of the time.
	// Large number of entries in the map may indicate problems with pod deletions.
	// +optional
	DisruptedPods map[string]metav1.Time

	// 当前允许的 Pod 中断数量。
	DisruptionsAllowed int32

	// 当前健康 pod 数量。
	CurrentHealthy int32

	// 期望的最少健康 Pod 数量。
	DesiredHealthy int32

	// 本中断预算计入的 Pod 总数。
	ExpectedPods int32

	// Conditions contain conditions for PDB
	// +optional
	Conditions []metav1.Condition
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodDisruptionBudget 用于定义一组 Pod 可承受的最大中断程度。
type PodDisruptionBudget struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Specification of the desired behavior of the PodDisruptionBudget.
	// +optional
	Spec PodDisruptionBudgetSpec
	// Most recently observed status of the PodDisruptionBudget.
	// +optional
	Status PodDisruptionBudgetStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodDisruptionBudgetList 是 PodDisruptionBudget 的集合。
type PodDisruptionBudgetList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta
	Items []PodDisruptionBudget
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Eviction evicts a pod from its node subject to certain policies and safety constraints.
// This is a subresource of Pod.  A request to cause such an eviction is
// created by POSTing to .../pods/<pod name>/eviction.
type Eviction struct {
	metav1.TypeMeta

	// ObjectMeta describes the pod that is being evicted.
	// +optional
	metav1.ObjectMeta

	// DeleteOptions may be provided
	// +optional
	DeleteOptions *metav1.DeleteOptions
}
