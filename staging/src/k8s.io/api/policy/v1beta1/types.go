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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// PodDisruptionBudgetSpec 描述 PodDisruptionBudget 的期望行为。
type PodDisruptionBudgetSpec struct {
	// 仅当驱逐后 selector 选中的 pod 中仍至少有 minAvailable 个可用时，才允许驱逐。
	// 例如设为 "100%" 可以阻止所有主动驱逐。
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty" protobuf:"bytes,1,opt,name=minAvailable"`

	// 用于筛选受该 PDB 管理的 pod 的标签查询。
	// nil selector 不选中任何 pod。
	// 空 selector ({}) 同样不选中任何 pod，这与通常选中全部 pod 的语义不同。
	// policy/v1 中空 selector 会选中命名空间内所有 pod。
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,2,opt,name=selector"`

	// 仅当驱逐后 selector 选中的 pod 中不可用的数量不超过 maxUnavailable 时，才允许驱逐。
	// 例如设为 0 可阻止所有主动驱逐。与 minAvailable 互斥。
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"bytes,3,opt,name=maxUnavailable"`

	// 定义不健康 pod 是否可被驱逐的判定标准。当前实现将具备
	// status.conditions 中 type="Ready"、status="True" 的 pod 视为健康。
	//
	// 合法取值为 IfHealthyBudget 和 AlwaysAllow；未指定时默认 IfHealthyBudget。
	//
	// IfHealthyBudget：运行中的 pod（status.phase="Running"）若尚未健康，
	// 仅当受保护应用未中断（currentHealthy >= desiredHealthy）时才可驱逐。
	// 健康 pod 始终受 PDB 约束。
	//
	// AlwaysAllow：所有运行中但尚未健康的 pod 均被视为已中断，
	// 即使不满足 PDB 条件也可驱逐。这可能导致已中断应用的 pod 没有机会恢复健康。
	// 健康 pod 始终受 PDB 约束。
	//
	// 后续可能新增策略。客户端遇到无法识别的取值时，应禁止驱逐不健康 pod。
	// +optional
	UnhealthyPodEvictionPolicy *UnhealthyPodEvictionPolicyType `json:"unhealthyPodEvictionPolicy,omitempty" protobuf:"bytes,4,opt,name=unhealthyPodEvictionPolicy"`
}

// UnhealthyPodEvictionPolicyType 定义不健康 pod 是否可被驱逐的判定标准。
// +enum
type UnhealthyPodEvictionPolicyType string

const (
	// 运行中的 pod 若尚未健康，仅当受保护应用未中断时才可驱逐，
	// 即 currentHealthy >= desiredHealthy；健康 pod 始终受 PDB 约束。
	IfHealthyBudget UnhealthyPodEvictionPolicyType = "IfHealthyBudget"

	// 所有运行中但尚未健康的 pod 均被视为已中断，即使不满足 PDB 条件也可驱逐。
	// 这可能导致已中断应用的 pod 没有机会恢复健康；健康 pod 始终受 PDB 约束。
	AlwaysAllow UnhealthyPodEvictionPolicyType = "AlwaysAllow"
)

// PodDisruptionBudgetStatus 记录 PodDisruptionBudget 的状态，可能滞后于系统实际状态。
type PodDisruptionBudgetStatus struct {
	// 更新本状态时观测到的最新 generation。
	// 仅在 observedGeneration 等于 PDB 对象 generation 时，状态信息才有效。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// 记录已被 API server eviction 子资源处理、但 PDB 控制器尚未观测到的 pod。
	// 条目从 API server 处理驱逐请求起保留，直到控制器看到该 pod 被标记删除（或超时）。
	// key 为 pod 名，value 为处理驱逐请求的时间。若 pod 未被删除，
	// PDB 控制器会在一段时间后自动清除该条目。
	// 正常情况下该 map 基本为空；条目过多通常意味着 pod 删除存在问题。
	// +optional
	DisruptedPods map[string]metav1.Time `json:"disruptedPods,omitempty" protobuf:"bytes,2,rep,name=disruptedPods"`

	// 当前允许的驱逐数量。
	// +optional
	DisruptionsAllowed int32 `json:"disruptionsAllowed" protobuf:"varint,3,opt,name=disruptionsAllowed"`

	// 当前健康 pod 数量。
	// +optional
	CurrentHealthy int32 `json:"currentHealthy" protobuf:"varint,4,opt,name=currentHealthy"`

	// 期望的最小健康 pod 数量。
	// +optional
	DesiredHealthy int32 `json:"desiredHealthy" protobuf:"varint,5,opt,name=desiredHealthy"`

	// 本 PDB 统计的 pod 总数。
	// +optional
	ExpectedPods int32 `json:"expectedPods" protobuf:"varint,6,opt,name=expectedPods"`

	// PDB 的条件列表，由 disruption 控制器设置 DisruptionAllowed 条件。
	// reason 字段已知取值如下（后续可能新增）：
	// - SyncFailed：控制器出错，无法计算允许的驱逐数，条件状态为 False。
	// - InsufficientPods：pod 数量达到或低于 PDB 要求，不允许驱逐，条件状态为 False。
	// - SufficientPods：pod 数量超过 PDB 要求，条件状态为 True，
	//                   允许的驱逐数见 disruptionsAllowed。
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
	// disruption 控制器设置的条件，表示 PDB 覆盖的 pod 是否可被驱逐。
	DisruptionAllowedCondition = "DisruptionAllowed"

	// PDB 协调失败、因而不允许驱逐时设置的 reason。
	SyncFailedReason = "SyncFailed"
	// pod 数量超过 PDB 要求、至少有一个可驱逐时设置的 reason。
	SufficientPodsReason = "SufficientPods"
	// pod 数量达到或低于 PDB 要求时设置的 reason。
	InsufficientPodsReason = "InsufficientPods"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.5
// +k8s:prerelease-lifecycle-gen:deprecated=1.21
// +k8s:prerelease-lifecycle-gen:removed=1.25
// +k8s:prerelease-lifecycle-gen:replacement=policy,v1,PodDisruptionBudget

// PodDisruptionBudget 定义一组 pod 允许承受的最大中断程度。
// +k8s:supportsSubresource="/status"
type PodDisruptionBudget struct {
	metav1.TypeMeta `json:""`

	// 标准对象元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// PodDisruptionBudget 期望行为的规格。
	// +optional
	Spec PodDisruptionBudgetSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// PodDisruptionBudget 最近一次观测到的状态。
	// +optional
	Status PodDisruptionBudgetStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.5
// +k8s:prerelease-lifecycle-gen:deprecated=1.21
// +k8s:prerelease-lifecycle-gen:removed=1.25
// +k8s:prerelease-lifecycle-gen:replacement=policy,v1,PodDisruptionBudgetList

// PodDisruptionBudgetList 是 PodDisruptionBudget 的集合。
type PodDisruptionBudgetList struct {
	metav1.TypeMeta `json:""`

	// 标准对象元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// items 为各个 PodDisruptionBudget 对象。
	Items []PodDisruptionBudget `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:noVerbs
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.5
// +k8s:prerelease-lifecycle-gen:deprecated=1.22

// Eviction 在满足特定策略和安全约束的前提下将 pod 从其节点驱逐。
// 它是 Pod 的子资源，通过 POST .../pods/<pod name>/evictions 发起驱逐请求。
type Eviction struct {
	metav1.TypeMeta `json:""`

	// 描述待驱逐 pod 的元数据。
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 可选的删除参数。
	// +optional
	DeleteOptions *metav1.DeleteOptions `json:"deleteOptions,omitempty" protobuf:"bytes,2,opt,name=deleteOptions"`
}
