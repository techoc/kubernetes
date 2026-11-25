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
	// 仅当驱逐后，"selector" 选出的 Pod 中至少仍有 "minAvailable" 个可用
	// （即即使被驱逐的 Pod 不存在也仍然可用）时，才允许驱逐。
	// 例如，指定 "100%" 即可阻止所有自愿驱逐。
	// +optional
	MinAvailable *intstr.IntOrString

	// 对该中断预算所管理的驱逐涉及的 Pod 进行 label 查询。
	// +optional
	Selector *metav1.LabelSelector

	// 仅当驱逐后，"selector" 选出的 Pod 中不可用的数量至多为 "maxUnavailable" 个
	// （即即使被驱逐的 Pod 不存在也计入）时，才允许驱逐。
	// 例如，指定 0 即可阻止所有自愿驱逐。该设置与 "minAvailable" 互斥。
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

// UnhealthyPodEvictionPolicyType 定义何时应将不健康 Pod 纳入驱逐考量的标准。
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

// PodDisruptionBudgetStatus 表示 PodDisruptionBudget 的状态信息。
// Status 可能滞后于系统的实际状态。
type PodDisruptionBudgetStatus struct {
	// 更新此 PDB status 时观察到的最近 generation。仅当 observedGeneration 等于
	// PDB 对象的 generation 时，DisruptionsAllowed 及其他状态信息才有效。
	// +optional
	ObservedGeneration int64

	// DisruptedPods 包含这样的 Pod 信息：其驱逐请求已被 API 服务器的 eviction
	// 子资源处理器处理，但尚未被 PodDisruptionBudget 控制器观察到。
	// 从 API 服务器处理驱逐请求开始，到 PDB 控制器看到该 Pod 被标记为删除
	// （或超时）为止，该 Pod 会一直存在于这个 map 中。map 的 key 是 Pod 名称，
	// value 是 API 服务器处理驱逐请求的时间。如果删除没有发生、Pod 仍然存在，
	// PodDisruptionBudget 控制器会在一段时间后自动将其从列表中移除。
	// 如果一切顺利，这个 map 在大多数时间应当为空。
	// map 中出现大量条目可能表明 Pod 删除存在问题。
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

	// Conditions 包含 PDB 的条件。
	// +optional
	Conditions []metav1.Condition
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodDisruptionBudget 用于定义一组 Pod 可承受的最大中断程度。
type PodDisruptionBudget struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// PodDisruptionBudget 期望行为的规格。
	// +optional
	Spec PodDisruptionBudgetSpec
	// PodDisruptionBudget 最近观察到的状态。
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

// Eviction 在满足特定策略和安全约束的前提下，将 Pod 从其所在节点驱逐。
// 这是 Pod 的一个子资源。发起此类驱逐请求的方式是向
// .../pods/<pod name>/eviction 发送 POST 请求。
type Eviction struct {
	metav1.TypeMeta

	// ObjectMeta 描述被驱逐的 Pod。
	// +optional
	metav1.ObjectMeta

	// 可以提供 DeleteOptions。
	// +optional
	DeleteOptions *metav1.DeleteOptions
}
