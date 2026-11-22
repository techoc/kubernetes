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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// DisruptionBudgetCause 是因违反 PodDisruptionBudget 而导致驱逐失败时返回的状态原因。
const DisruptionBudgetCause metav1.CauseType = "DisruptionBudget"

// PodDisruptionBudgetSpec 描述 PodDisruptionBudget 的期望配置。
type PodDisruptionBudgetSpec struct {
	// minAvailable 表示驱逐后由 "selector" 选中的 Pod 中至少仍有 "minAvailable" 个可用时才允许驱逐，
	// 即不计被驱逐的 Pod。例如指定 "100%" 可阻止所有自愿驱逐。
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty" protobuf:"bytes,1,opt,name=minAvailable"`

	// selector 是对受该中断预算管理的 Pod 的标签查询。
	// nil 选择器不匹配任何 Pod，空选择器 ({}) 选中命名空间内所有 Pod。
	// +patchStrategy=replace
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" patchStrategy:"replace" protobuf:"bytes,2,opt,name=selector"`

	// maxUnavailable 表示驱逐后由 "selector" 选中的 Pod 中至多有 "maxUnavailable" 个不可用时才允许驱逐，
	// 即包括被驱逐的 Pod。例如指定 0 可阻止所有自愿驱逐。此字段与 "minAvailable" 互斥。
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"bytes,3,opt,name=maxUnavailable"`

	// unhealthyPodEvictionPolicy 定义何时可驱逐不健康 Pod 的判定标准。当前实现将具有
	// type="Ready"、status="True" 的 status.conditions 条目的 Pod 视为健康。
	//
	// 有效策略为 IfHealthyBudget 和 AlwaysAllow。未指定策略时使用默认行为，
	// 即 IfHealthyBudget 策略。
	//
	// IfHealthyBudget 策略下，运行中（status.phase="Running"）但尚未健康的 Pod，
	// 仅在受保护应用未中断（status.currentHealthy 至少等于 status.desiredHealthy）时才可驱逐。
	// 健康 Pod 的驱逐仍受 PDB 约束。
	//
	// AlwaysAllow 策略下，所有运行中但尚未健康的 Pod 均视为已中断，无论 PDB 条件是否满足都可驱逐。
	// 这意味着已中断应用中原本可能转为健康的 Pod 会失去恢复机会。健康 Pod 的驱逐仍受 PDB 约束。
	//
	// 未来可能新增策略。做驱逐决策的客户端遇到无法识别的策略时，应禁止驱逐不健康 Pod。
	// +optional
	UnhealthyPodEvictionPolicy *UnhealthyPodEvictionPolicyType `json:"unhealthyPodEvictionPolicy,omitempty" protobuf:"bytes,4,opt,name=unhealthyPodEvictionPolicy"`
}

// UnhealthyPodEvictionPolicyType 定义何时可将不健康 Pod 视为可驱逐对象的判定标准。
// +enum
type UnhealthyPodEvictionPolicyType string

const (
	// IfHealthyBudget 策略下，运行中但尚未健康的 Pod 仅在受保护应用未中断
	// （status.currentHealthy 至少等于 status.desiredHealthy）时才可驱逐。
	// 健康 Pod 的驱逐仍受 PDB 约束。
	IfHealthyBudget UnhealthyPodEvictionPolicyType = "IfHealthyBudget"

	// AlwaysAllow 策略下，所有运行中但尚未健康的 Pod 均视为已中断，无论 PDB 条件是否满足都可驱逐。
	// 这意味着已中断应用中原本可能转为健康的 Pod 会失去恢复机会。健康 Pod 的驱逐仍受 PDB 约束。
	AlwaysAllow UnhealthyPodEvictionPolicyType = "AlwaysAllow"
)

// PodDisruptionBudgetStatus 表示 PodDisruptionBudget 的状态信息，可能滞后于系统实际状态。
type PodDisruptionBudgetStatus struct {
	// 更新本 PDB 状态时观测到的最新 generation。仅当 observedGeneration 等于 PDB 对象的
	// generation 时，DisruptionsAllowed 等其他状态信息才有效。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// DisruptedPods 记录驱逐请求已被 API server 的 eviction 子资源处理、但 PDB 控制器尚未观测到的 Pod。
	// 条目从 API server 处理驱逐请求起保留，直到 PDB 控制器看到该 Pod 被标记为删除（或超时）。
	// map 的键为 Pod 名称，值为 API server 处理驱逐请求的时间。若删除未发生且 Pod 仍在，
	// PDB 控制器会在一段时间后自动将其从列表中移除。正常情况下此 map 大部分时间应为空，
	// 条目过多可能意味着 Pod 删除出现问题。
	// +optional
	DisruptedPods map[string]metav1.Time `json:"disruptedPods,omitempty" protobuf:"bytes,2,rep,name=disruptedPods"`

	// 当前允许的 Pod 中断数量。
	// +optional
	DisruptionsAllowed int32 `json:"disruptionsAllowed" protobuf:"varint,3,opt,name=disruptionsAllowed"`

	// 当前健康 Pod 的数量。
	// +optional
	CurrentHealthy int32 `json:"currentHealthy" protobuf:"varint,4,opt,name=currentHealthy"`

	// 期望的最少健康 Pod 数量。
	// +optional
	DesiredHealthy int32 `json:"desiredHealthy" protobuf:"varint,5,opt,name=desiredHealthy"`

	// 本中断预算计入的 Pod 总数。
	// +optional
	ExpectedPods int32 `json:"expectedPods" protobuf:"varint,6,opt,name=expectedPods"`

	// Conditions 包含 PDB 的条件，由 disruption 控制器设置 DisruptionAllowed 条件。reason 字段的已知取值
	// （未来可能新增）如下：
	// - SyncFailed：控制器出错，无法计算允许的中断数量，因此不允许中断，条件状态为 False。
	// - InsufficientPods：Pod 数量等于或低于 PodDisruptionBudget 要求，不允许中断，条件状态为 False。
	// - SufficientPods：Pod 数量多于 PodDisruptionBudget 要求，条件状态为 True，
	//                   允许的中断数量由 disruptionsAllowed 给出。
	//
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,7,rep,name=conditions"`
}

const (
	// DisruptionAllowedCondition 是由 disruption 控制器设置的条件，表示 PDB 覆盖的 Pod 是否可被中断。
	DisruptionAllowedCondition = "DisruptionAllowed"

	// SyncFailedReason 在 PDB 调和失败、因而不允许中断 Pod 时设置到 DisruptionAllowed 条件上。
	SyncFailedReason = "SyncFailed"
	// SufficientPodsReason 在 PDB 覆盖的 Pod 多于要求且至少一个可被中断时设置到 DisruptionAllowed 条件上。
	SufficientPodsReason = "SufficientPods"
	// InsufficientPodsReason 在 Pod 数量等于或少于 PDB 要求时设置到 DisruptionAllowed 条件上。
	InsufficientPodsReason = "InsufficientPods"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.21

// PodDisruptionBudget 用于定义一组 Pod 可承受的最大中断程度。
// +k8s:supportsSubresource="/status"
type PodDisruptionBudget struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 定义 PodDisruptionBudget 的期望行为。
	// +optional
	Spec PodDisruptionBudgetSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// status 是 PodDisruptionBudget 最近观测到的状态。
	// +optional
	Status PodDisruptionBudgetStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.21

// PodDisruptionBudgetList 是 PodDisruptionBudget 的集合。
type PodDisruptionBudgetList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// Items 是 PodDisruptionBudget 列表。
	Items []PodDisruptionBudget `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:noVerbs
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.22

// Eviction 在遵循特定策略和安全约束的前提下将 Pod 从其节点驱逐。
// 这是 Pod 的子资源，通过向 .../pods/<pod name>/evictions 发送 POST 请求来触发驱逐。
type Eviction struct {
	metav1.TypeMeta `json:""`

	// metadata 描述被驱逐的 Pod。
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// deleteOptions 可选提供。
	// +optional
	DeleteOptions *metav1.DeleteOptions `json:"deleteOptions,omitempty" protobuf:"bytes,2,opt,name=deleteOptions"`
}
