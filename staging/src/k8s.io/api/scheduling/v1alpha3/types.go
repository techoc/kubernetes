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

package v1alpha3

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Workload 用于表达从调度视角管理负载生命周期（包括调度、抢占、驱逐等阶段）时
// 应遵循的调度约束。
// Workload API 的启用由 GenericWorkload 特性门控控制。
type Workload struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	//
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 定义 Workload 的期望行为。
	//
	// +required
	Spec WorkloadSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WorkloadList 包含一组 Workload 资源。
type WorkloadList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	//
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 Workload 列表。
	Items []Workload `json:"items" protobuf:"bytes,2,rep,name=items"`
}

const (
	// WorkloadMaxPodGroupTemplates 是每个 Workload 允许的最大 PodGroup 模板数。
	WorkloadMaxPodGroupTemplates = 8
	// WorkloadMaxTreeDepth 是 Workload 中（复合）PodGroup 模板树允许的最大深度。
	WorkloadMaxTreeDepth = 4
)

// WorkloadSpec 定义 Workload 的期望状态。
type WorkloadSpec struct {
	// controllerRef 是指向控制对象（如 Deployment 或 Job）的可选引用，
	// 供 CLI 等工具回溯原始负载定义。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	ControllerRef *TypedLocalObjectReference `json:"controllerRef,omitempty" protobuf:"bytes,1,opt,name=controllerRef"`

	// podGroupTemplates 是构成 Workload 的模板列表。
	// 模板数量上限为 8，Workload 创建后不能增删模板；已有模板在各字段允许时仍可更新。
	// CompositePodGroupTemplates 与 PodGroupTemplates 必须且只能设置其一。
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:maxItems=8
	// +k8s:update=NoAddItem
	// +k8s:update=NoRemoveItem
	PodGroupTemplates []PodGroupTemplate `json:"podGroupTemplates" protobuf:"bytes,2,rep,name=podGroupTemplates"`

	// compositePodGroupTemplates 是构成 Workload 的 CompositePodGroup 模板列表。
	// 模板数量上限为 8，此字段不可变。
	// CompositePodGroupTemplates 与 PodGroupTemplates 必须且只能设置其一。
	//
	// 仅当 CompositePodGroup 特性门控启用时使用此字段。
	//
	// +featureGate=CompositePodGroup
	// +optional
	// +listType=map
	// +listMapKey=name
	// +k8s:ifDisabled("CompositePodGroup")=+k8s:forbidden
	// +k8s:ifEnabled("CompositePodGroup")=+k8s:optional
	// +k8s:unionMember
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:maxItems=8
	// +k8s:update=NoAddItem
	// +k8s:update=NoRemoveItem
	CompositePodGroupTemplates []CompositePodGroupTemplate `json:"compositePodGroupTemplates,omitempty" protobuf:"bytes,3,rep,name=compositePodGroupTemplates"`
}

// TypedLocalObjectReference 用于引用同一命名空间内的类型化对象。
type TypedLocalObjectReference struct {
	// apiGroup 是被引用资源所属的组。
	// APIGroup 为空时，Kind 必须属于核心 API 组；其他第三方类型必须设置 APIGroup。
	// APIGroup 必须是 DNS 子域名。
	//
	// +optional
	// +k8s:optional
	// +k8s:format=k8s-long-name
	APIGroup string `json:"apiGroup,omitempty" protobuf:"bytes,1,opt,name=apiGroup"`
	// kind 是被引用资源的类型，必须是路径片段名（path segment name）。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-path-segment-name
	Kind string `json:"kind" protobuf:"bytes,2,opt,name=kind"`
	// name 是被引用资源的名称，必须是路径片段名。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-path-segment-name
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`
}

// MaxPodGroupResourceClaims 是 PodGroup 或 Workload 的 PodGroupTemplate
// 允许的最大资源申领数。
const MaxPodGroupResourceClaims = 4

// PodGroupTemplate 表示一组带调度策略的 Pod 的模板。
type PodGroupTemplate struct {
	// name 是 PodGroupTemplate 在 Workload 内的唯一标识，必须是 DNS 标签，且不可变。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-short-name
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// schedulingPolicy 定义此 PodGroupTemplate 的调度策略。
	//
	// +required
	SchedulingPolicy PodGroupSchedulingPolicy `json:"schedulingPolicy" protobuf:"bytes,2,opt,name=schedulingPolicy"`

	// schedulingConstraints 定义此 PodGroupTemplate 的可选调度约束（如拓扑）。
	// 仅当 TopologyAwareWorkloadScheduling 特性门控启用时可用，且不可变。
	//
	// +featureGate=TopologyAwareWorkloadScheduling
	// +optional
	// +k8s:ifDisabled(TopologyAwareWorkloadScheduling)=+k8s:forbidden
	// +k8s:ifEnabled(TopologyAwareWorkloadScheduling)=+k8s:optional
	// +k8s:immutable
	SchedulingConstraints *PodGroupSchedulingConstraints `json:"schedulingConstraints" protobuf:"bytes,3,opt,name=schedulingConstraints"`

	// resourceClaims 定义组内 Pod 可共享的 ResourceClaim。Pod 在自己
	// Spec.ResourceClaims 中定义与 PodGroup 的申领名称、所指向的 ResourceClaim 或
	// ResourceClaimTemplate 完全一致的申领，即可消费分配给该 PodGroup 的设备。
	//
	// 这是 beta 级字段，需要启用 DRAWorkloadResourceClaims 特性门控。
	//
	// 此字段不可变。
	//
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge,retainKeys
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:maxItems=4
	// +featureGate=DRAWorkloadResourceClaims
	// +k8s:immutable
	ResourceClaims []PodGroupResourceClaim `json:"resourceClaims,omitempty" patchStrategy:"merge,retainKeys" patchMergeKey:"name" protobuf:"bytes,4,rep,name=resourceClaims"`

	// disruptionMode 定义 PodGroup 可被中断的方式，取值为 Single 或 All。
	// 此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	DisruptionMode *DisruptionMode `json:"disruptionMode,omitempty" protobuf:"bytes,5,opt,name=disruptionMode"`

	// priorityClassName 表示由此模板创建的 PodGroup 调度时应考虑的优先级类名。
	// 此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:format=k8s-long-name
	// +k8s:immutable
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,6,opt,name=priorityClassName"`

	// priority 是自此模板创建的 PodGroup 的优先级值，各系统组件据此判断其优先级。
	// 值越大优先级越高。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:maximum=1000000000 # HighestUserDefinablePriority
	// +k8s:immutable
	Priority *int32 `json:"priority,omitempty" protobuf:"varint,7,opt,name=priority"`

	// preemptionPolicy 是抢占低优先级 Pod/PodGroup 的策略，取值为 Never 或 PreemptLowerPriority。
	// 此字段不可变，且仅当 PodGroupPreemptionPolicy 特性门控启用时可用。
	//
	// +featureGate=PodGroupPreemptionPolicy
	// +optional
	// +k8s:immutable
	// +k8s:ifDisabled("PodGroupPreemptionPolicy")=+k8s:forbidden
	// +k8s:ifEnabled("PodGroupPreemptionPolicy")=+k8s:optional
	PreemptionPolicy *PreemptionPolicy `json:"preemptionPolicy,omitempty" protobuf:"bytes,8,opt,name=preemptionPolicy"`
}

// CompositePodGroupTemplate 表示带调度策略的 CompositePodGroup 模板。
type CompositePodGroupTemplate struct {
	// name 是 CompositePodGroupTemplate 在 Workload 内的唯一标识，必须是 DNS 标签，且为必填项。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-short-name
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// schedulingPolicy 定义此模板的调度策略。
	//
	// +required
	SchedulingPolicy CompositePodGroupSchedulingPolicy `json:"schedulingPolicy" protobuf:"bytes,2,opt,name=schedulingPolicy"`

	// schedulingConstraints 定义此 CompositePodGroupTemplate 的可选调度约束（如拓扑）。
	// 此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	SchedulingConstraints *CompositePodGroupSchedulingConstraints `json:"schedulingConstraints,omitempty" protobuf:"bytes,3,opt,name=schedulingConstraints"`

	// disruptionMode 定义 CompositePodGroup 可被中断的方式，取值为 Single 或 All。
	// 此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	DisruptionMode *CompositeDisruptionMode `json:"disruptionMode,omitempty" protobuf:"bytes,4,opt,name=disruptionMode"`

	// priorityClassName 表示由此模板创建的复合 PodGroup 调度时应考虑的优先级类名。
	// 若未指定，准入控制可将其设为全局默认优先级类（如存在）；否则优先级为零。
	// 此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:format=k8s-long-name
	// +k8s:immutable
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,5,opt,name=priorityClassName"`

	// priority 是自此模板创建的复合 PodGroup 的优先级值，各系统组件据此判断其优先级。
	// 启用 Priority 准入控制器时用户不可设置此字段，其值由控制器根据 PriorityClassName 填充。
	// 值越大优先级越高。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:maximum=1000000000 # HighestUserDefinablePriority
	// +k8s:immutable
	Priority *int32 `json:"priority,omitempty" protobuf:"varint,6,opt,name=priority"`

	// preemptionPolicy 是抢占低优先级 Pod/PodGroup 的策略，取值为 Never 或 PreemptLowerPriority。
	// 此字段不可变，且仅当 PodGroupPreemptionPolicy 特性门控启用时可用。
	//
	// +featureGate=PodGroupPreemptionPolicy
	// +optional
	// +k8s:immutable
	// +k8s:ifDisabled("PodGroupPreemptionPolicy")=+k8s:forbidden
	// +k8s:ifEnabled("PodGroupPreemptionPolicy")=+k8s:optional
	PreemptionPolicy *PreemptionPolicy `json:"preemptionPolicy,omitempty" protobuf:"bytes,7,opt,name=preemptionPolicy"`

	// podGroupTemplates 是子 PodGroup 的模板列表，数量上限为 8。
	// CompositePodGroupTemplates 或 PodGroupTemplates 至少要设置一项。
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:maxItems=8
	// +k8s:update=NoAddItem
	// +k8s:update=NoRemoveItem
	PodGroupTemplates []PodGroupTemplate `json:"podGroupTemplates,omitempty" protobuf:"bytes,8,rep,name=podGroupTemplates"`

	// compositePodGroupTemplates 是子 CompositePodGroup 的模板列表，数量上限为 8。
	// CompositePodGroupTemplates 或 PodGroupTemplates 至少要设置一项。
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:maxItems=8
	// +k8s:update=NoAddItem
	// +k8s:update=NoRemoveItem
	CompositePodGroupTemplates []CompositePodGroupTemplate `json:"compositePodGroupTemplates,omitempty" protobuf:"bytes,9,rep,name=compositePodGroupTemplates"`
}

// PodGroupSchedulingPolicy 定义 PodGroup 的调度配置。
// 必须且只能设置一种策略。在创建时通过设置 Basic 或 Gang 字段选定策略，
// 创建后不可更改；所选策略内部的字段在各字段允许时可更新。
//

// +union
type PodGroupSchedulingPolicy struct {
	// basic 指定组内 Pod 使用标准 Kubernetes 调度行为。创建时设置此字段即选择
	// 基础调度，此后不可更改。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:immutable
	Basic *BasicSchedulingPolicy `json:"basic,omitempty" protobuf:"bytes,1,opt,name=basic"`

	// gang 指定组内 Pod 使用全有或全无（all-or-nothing）语义调度。创建时设置此字段
	// 即选择 gang 调度，此后不可设置或取消；Gang 策略内的 minCount 在创建后仍可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:update=NoSet
	// +k8s:update=NoUnset
	Gang *GangSchedulingPolicy `json:"gang,omitempty" protobuf:"bytes,2,opt,name=gang"`
}

// BasicSchedulingPolicy 表示应使用标准 Kubernetes 调度行为。
type BasicSchedulingPolicy struct {
	// 此结构故意留空。它的存在表示应应用基础调度策略。将来可能新增字段，
	// 用于描述 PodGroup 级别、不涉及“全有或全无”（gang）调度的约束。
}

// GangSchedulingPolicy 定义 gang 调度的参数。
type GangSchedulingPolicy struct {
	// minCount 是调度器接纳整个 PodGroup 前，必须可调度或已同时调度的 Pod 最小数量。
	// 必须为正整数；此字段可变，以支持负载扩缩容。
	//
	// 注意调度器基于最终一致模型运行：受传播延迟影响，minCount 的更新可能不会立即反映在
	// 调度决策中；若更新发生在针对该组的调度周期进行中，新值可能要到下一个周期才生效。
	// 此外 minCount 仅在调度时生效，修改它不会影响已调度的 Pod，只作用于后续周期中待评估的 Pod。
	//
	// +required
	// +k8s:required
	// +k8s:minimum=1
	MinCount int32 `json:"minCount" protobuf:"varint,1,opt,name=minCount"`
}

// PodGroupResourceClaim 直接引用一个 ResourceClaim，或通过指定一个 ResourceClaimTemplate
// 为 PodGroup 生成 ResourceClaim。
//
// 它为申领附加一个在 PodGroup 内唯一标识该 ResourceClaim 的名称。需要访问该 ResourceClaim
// 的 Pod 须在自己的 Spec.ResourceClaims 中定义匹配的引用，且各字段与 PodGroup 的申领完全一致。

type PodGroupResourceClaim struct {
	// name 在 PodGroup 内唯一标识此资源申领，必须是 DNS_LABEL。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-short-name
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// resourceClaimName 是与 PodGroup 同命名空间中 ResourceClaim 对象的名称。
	// 该 ResourceClaim 将为 PodGroup（而非其各个 Pod）预留。
	//
	// ResourceClaimName 与 ResourceClaimTemplateName 必须且只能设置其一。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:format=k8s-long-name
	ResourceClaimName *string `json:"resourceClaimName,omitempty" protobuf:"bytes,2,opt,name=resourceClaimName"`

	// resourceClaimTemplateName 是与 PodGroup 同命名空间中 ResourceClaimTemplate 对象的名称。
	//
	// 模板将用于创建绑定到该 PodGroup 的新 ResourceClaim。PodGroup 删除时，该 ResourceClaim
	// 也会被删除。其唯一名称由 PodGroup 名称、资源名和一个生成后缀拼接而成，记录在
	// podgroup.status.resourceClaimStatuses 中。
	//
	// 此字段不可变，控制面创建 ResourceClaim 后不会再修改它。
	//
	// ResourceClaimName 与 ResourceClaimTemplateName 必须且只能设置其一。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:format=k8s-long-name
	ResourceClaimTemplateName *string `json:"resourceClaimTemplateName,omitempty" protobuf:"bytes,3,opt,name=resourceClaimTemplateName"`
}

// DisruptionMode 定义组内各实体可被中断的方式，且只能设置一种模式。
//

// +union
type DisruptionMode struct {
	// single 指定各子对象可彼此独立地被中断。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	Single *SingleDisruptionMode `json:"single,omitempty" protobuf:"bytes,1,opt,name=single"`

	// all 指定所有子对象只能作为一个整体被中断。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	All *AllDisruptionMode `json:"all,omitempty" protobuf:"bytes,2,opt,name=all"`
}

// SingleDisruptionMode 指定各子对象可被独立中断。
type SingleDisruptionMode struct {
	// 当前故意留空。
}

// AllDisruptionMode 指定各子对象只能作为一个整体被中断。
type AllDisruptionMode struct {
	// 当前故意留空。
}

// PreemptionPolicy 描述是否以及何时抢占 Pod 的策略。
// +enum
// +k8s:enum
type PreemptionPolicy string

const (
	// PreemptLowerPriority 表示 Pod 可抢占优先级更低的 Pod。
	PreemptLowerPriority PreemptionPolicy = "PreemptLowerPriority"
	// PreemptNever 表示 Pod 从不抢占其他 Pod。
	PreemptNever PreemptionPolicy = "Never"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:supportsSubresource="/status"

// PodGroup 表示一组 Pod 的运行时实例。
// PodGroup 由负载控制器（Job、LWS、JobSet 等）根据 Workload.podGroupTemplates 创建。
// PodGroup API 的启用由 GenericWorkload 特性门控控制。
type PodGroup struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	//
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 定义 PodGroup 的期望状态。
	//
	// +required
	Spec PodGroupSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 表示 PodGroup 当前观测到的状态。
	//
	// +optional
	Status PodGroupStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodGroupList 包含一组 PodGroup 资源。
type PodGroupList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	//
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 PodGroup 列表。
	Items []PodGroup `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// PodGroupSpec 定义 PodGroup 的期望状态。
type PodGroupSpec struct {
	// parentCompositePodGroupName 是同一命名空间中父复合 PodGroup 的名称。
	// 为 nil 时，此 PodGroup 是负载层级结构的根。
	// 仅当 CompositePodGroup 特性门控启用时使用此字段，且不可变。
	//
	// +featureGate=CompositePodGroup
	// +optional
	// +k8s:ifDisabled(CompositePodGroup)=+k8s:forbidden
	// +k8s:ifEnabled(CompositePodGroup)=+k8s:optional
	// +k8s:immutable
	// +k8s:format=k8s-long-name
	// +k8s:alpha(since: "1.37")=+k8s:dependentRequired("workloadRef")
	ParentCompositePodGroupName *string `json:"parentCompositePodGroupName,omitempty" protobuf:"bytes,1,opt,name=parentCompositePodGroupName"`

	// workloadRef 引用创建此 PodGroup 时所用 Workload 对象中的 PodGroup 模板。
	// 此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	WorkloadRef *WorkloadReference `json:"workloadRef,omitempty" protobuf:"bytes,2,opt,name=workloadRef"`

	// schedulingPolicy 定义该 PodGroup 实例的调度策略。
	// 控制器应从 PodGroupTemplate 复制此字段。
	//
	// +required
	SchedulingPolicy PodGroupSchedulingPolicy `json:"schedulingPolicy" protobuf:"bytes,3,opt,name=schedulingPolicy"`

	// schedulingConstraints 定义此 PodGroup 的可选调度约束（如拓扑）。
	// 控制器应从 PodGroupTemplate 复制此字段，且不可变。
	// 仅当 TopologyAwareWorkloadScheduling 特性门控启用时可用。
	//
	// +featureGate=TopologyAwareWorkloadScheduling
	// +optional
	// +k8s:ifDisabled(TopologyAwareWorkloadScheduling)=+k8s:forbidden
	// +k8s:ifEnabled(TopologyAwareWorkloadScheduling)=+k8s:optional
	// +k8s:ifEnabled(TopologyAwareWorkloadScheduling)=+k8s:immutable
	SchedulingConstraints *PodGroupSchedulingConstraints `json:"schedulingConstraints,omitempty" protobuf:"bytes,4,opt,name=schedulingConstraints"`

	// resourceClaims 定义组内 Pod 可共享的 ResourceClaim。Pod 在自己
	// Spec.ResourceClaims 中定义与 PodGroup 的申领名称、所指向的 ResourceClaim 或
	// ResourceClaimTemplate 完全一致的申领，即可消费分配给该 PodGroup 的设备。
	//
	// 这是 beta 级字段，需要启用 DRAWorkloadResourceClaims 特性门控。
	//
	// 此字段不可变。
	//
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge,retainKeys
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:maxItems=4
	// +k8s:immutable
	// +featureGate=DRAWorkloadResourceClaims
	ResourceClaims []PodGroupResourceClaim `json:"resourceClaims,omitempty" patchStrategy:"merge,retainKeys" patchMergeKey:"name" protobuf:"bytes,5,rep,name=resourceClaims"`

	// disruptionMode 定义 PodGroup 可被中断的方式。控制器应从 PodGroupTemplate 复制此字段。
	// 取值为 Single 或 All，未设置时默认为 Single。此字段不可变。
	//
	// +default={"single": {}}
	// +optional
	// +k8s:optional
	// +k8s:immutable
	DisruptionMode *DisruptionMode `json:"disruptionMode,omitempty" protobuf:"bytes,6,opt,name=disruptionMode"`

	// priorityClassName 定义调度此 PodGroup 时应考虑的优先级类名。控制器应从
	// PodGroupTemplate 复制此字段；否则其校验和解析方式与 PodGroupTemplate 上的
	// PriorityClassName 相同（即未指定优先级类时，准入控制可设为全局默认优先级类（如存在），
	// 否则 PodGroup 的优先级为零）。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	// +k8s:format=k8s-long-name
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,7,opt,name=priorityClassName"`

	// priority 是此 PodGroup 的优先级值，各系统组件据此判断其优先级。启用 Priority
	// 准入控制器时用户不可设置此字段，其值由控制器根据 PriorityClassName 填充。
	// 值越大优先级越高。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	// +k8s:maximum=1000000000 # HighestUserDefinablePriority
	Priority *int32 `json:"priority,omitempty" protobuf:"varint,8,opt,name=priority"`

	// preemptionPolicy 是抢占低优先级 Pod/PodGroup 的策略，取值为 Never 或
	// PreemptLowerPriority，未设置时默认为 PreemptLowerPriority。启用 Priority 准入控制器时，
	// 其值由控制器根据 PriorityClassName 填充；PriorityClass 中未设置时亦默认为 PreemptLowerPriority。
	// 此字段不可变，且仅当 PodGroupPreemptionPolicy 特性门控启用时可用。
	//
	// +featureGate=PodGroupPreemptionPolicy
	// +optional
	// +k8s:immutable
	// +k8s:ifDisabled("PodGroupPreemptionPolicy")=+k8s:forbidden
	// +k8s:ifEnabled("PodGroupPreemptionPolicy")=+k8s:optional
	PreemptionPolicy *PreemptionPolicy `json:"preemptionPolicy,omitempty" protobuf:"bytes,9,opt,name=preemptionPolicy"`
}

// PodGroupStatus 表示 PodGroup 的状态信息。
type PodGroupStatus struct {
	// conditions 是对 PodGroup 状态的最新观测。
	//
	// 已知条件类型：
	// - "PodGroupInitiallyScheduled"：表示调度要求是否已满足。一旦转为 True 即为终态，
	//   即使随后 Pod 被驱逐、组约束不再满足，也不会回退为 False。
	// - "DisruptionTarget"：表示 PodGroup 即将因抢占等中断而终止。
	//
	// PodGroupInitiallyScheduled 条件的已知原因：
	// - "Unschedulable"：因资源限制、亲和/反亲和规则或 gang 容量不足而无法调度。
	// - "SchedulerError"：因调度过程中的内部错误（如 nodeAffinity 解析错误）而无法调度。
	//
	// DisruptionTarget 条件的已知原因：
	// - "PreemptionByScheduler"：PodGroup 被调度器抢占，为更高优先级的 PodGroup 或 Pod 腾出空间。
	//

	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`

	// resourceClaimStatuses 是资源申领的状态。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge,retainKeys
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:maxItems=4
	// +featureGate=DRAWorkloadResourceClaims
	ResourceClaimStatuses []PodGroupResourceClaimStatus `json:"resourceClaimStatuses,omitempty" patchStrategy:"merge,retainKeys" patchMergeKey:"name" protobuf:"bytes,2,rep,name=resourceClaimStatuses"`
}

// PodGroup 的已知条件类型。
const (
	// PodGroupInitiallyScheduled 表示此 PodGroup 首次成功前的调度过程状态。
	PodGroupInitiallyScheduled string = "PodGroupInitiallyScheduled"
	// DisruptionTarget 表示 PodGroup 即将因抢占等中断而终止。
	DisruptionTarget string = "DisruptionTarget"
)

// PodGroup 的已知条件原因。
const (
	// PodGroupInitiallyScheduled 条件中的 Unschedulable 原因表示 PodGroup 因资源限制、
	// 亲和/反亲和规则或容量不足而无法调度。
	PodGroupReasonUnschedulable string = "Unschedulable"
	// PodGroupInitiallyScheduled 条件中的 SchedulerError 原因表示调度过程中发生了
	// 内部错误，例如 nodeAffinity 解析错误。
	PodGroupReasonSchedulerError string = "SchedulerError"
	// DisruptionTarget 条件中的 PreemptionByScheduler 原因表示 PodGroup 被抢占，
	// 以便为更高优先级的 PodGroup 或 Pod 腾出空间。
	PodGroupReasonPreemptionByScheduler string = "PreemptionByScheduler"
)

// PodGroupResourceClaimStatus 针对每个引用 ResourceClaimTemplate 的 PodGroupResourceClaim
// 存放在 PodGroupStatus 中，记录对应 ResourceClaim 生成后的名称。
type PodGroupResourceClaimStatus struct {
	// name 在 PodGroup 内唯一标识此资源申领，必须与 podgroup.spec.resourceClaims 中
	// 某条目的名称一致，因此该字符串必须是 DNS_LABEL。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// resourceClaimName 是为 PodGroup 在其命名空间中生成的 ResourceClaim 的名称。
	// 未设置时表示无需生成 ResourceClaim，此时可忽略 podgroup.spec.resourceClaims 对应条目。
	//
	// +optional
	// +k8s:optional
	// +k8s:format=k8s-long-name
	ResourceClaimName *string `json:"resourceClaimName,omitempty" protobuf:"bytes,2,opt,name=resourceClaimName"`
}

// WorkloadReference 引用 Workload 对象以及创建特定 PodGroup 时所用的模板。
type WorkloadReference struct {
	// workloadName 是包含创建 PodGroup 所用模板的 Workload 对象名称，必须是 DNS 名称，为必填项。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-long-name
	WorkloadName string `json:"workloadName" protobuf:"bytes,1,opt,name=workloadName"`

	// templateName 是 Workload 对象中用于创建 PodGroup 的模板名称，必须是 DNS 标签，为必填项。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-short-name
	TemplateName string `json:"templateName" protobuf:"bytes,2,opt,name=templateName"`
}

// PodGroupSchedulingConstraints 定义 PodGroup 的调度约束（如拓扑）。
type PodGroupSchedulingConstraints struct {
	// topology 定义 PodGroup 的拓扑约束，目前只能指定一条，将来可能改变。
	//
	// +optional
	// +k8s:optional
	// +k8s:maxItems=1
	// +listType=atomic
	// +k8s:listType=atomic
	Topology []TopologyConstraint `json:"topology,omitempty" protobuf:"bytes,1,rep,name=topology"`
}

// TopologyConstraint 定义 PodGroup 的拓扑约束。
type TopologyConstraint struct {
	// key 指定表示拓扑域的节点标签的键。PodGroup 中所有 Pod 必须位于同一域实例内。
	// 不同 PodGroup 即使源自同一 PodGroupTemplate，也可落在不同域实例上。
	// 例如 "topology.kubernetes.io/rack"。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-label-key
	Key string `json:"key" protobuf:"bytes,1,opt,name=key"`
}

// WorkloadPodGroupSchedulingPolicy 定义由负载控制器管理的 Pod 组的调度策略。
// 必须且只能设置一种策略。
//

// ---
//
// 这是一个可复用的构建块，用于嵌入控制器自己的 API，与其他调度字段（例如 Job 的
// spec.scheduling）并列。建议禁止创建后修改策略，但仍允许 gang.minCount 变更。DV 无法表达
// “只有 gang.minCount 可变而 basic/gang 变体冻结”，因此嵌入方必须用手写校验强制变体不可变。
// 例如：
//
//	type JobSchedulingConfiguration struct {
//		// SchedulingPolicy 定义此 Job 的调度策略。
//		// Basic 或 Gang 必须且只能设置其一。
//		// 创建后此字段不可变：不可增删策略。策略变体（basic/gang）由控制器的
//		// 手写校验冻结；只有 schedulingPolicy.gang.minCount 允许修改。
//		// +optional
//		// +k8s:optional
//		// +k8s:update=NoSet
//		// +k8s:update=NoUnset
//		SchedulingPolicy *schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy `json:"schedulingPolicy,omitempty" protobuf:"bytes,N,opt,name=schedulingPolicy"`
//
//		// 其他调度字段
//	}
//

// +union
type WorkloadPodGroupSchedulingPolicy struct {
	// basic 指定使用标准的、逐 Pod 的 Kubernetes 调度行为。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	Basic *WorkloadPodGroupBasicSchedulingPolicy `json:"basic,omitempty" protobuf:"bytes,1,opt,name=basic"`

	// gang 指定全有或全无的调度语义。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	Gang *WorkloadPodGroupGangSchedulingPolicy `json:"gang,omitempty" protobuf:"bytes,2,opt,name=gang"`
}

// WorkloadPodGroupBasicSchedulingPolicy 表示标准的 Kubernetes 调度行为。
type WorkloadPodGroupBasicSchedulingPolicy struct {
	// 当前故意留空。
}

// WorkloadPodGroupGangSchedulingPolicy 定义 gang（全有或全无）调度的参数。
type WorkloadPodGroupGangSchedulingPolicy struct {
	// minCount 是调度器接纳整个 PodGroup 前必须同时调度的 Pod 最小数量。
	// 此字段可选；未指定时控制器应注入符合上下文的合理默认值（如 Job 的 parallelism）。
	// 若设置，必须为正整数。
	//
	// +optional
	// +k8s:optional
	// +k8s:minimum=1
	MinCount *int32 `json:"minCount,omitempty" protobuf:"varint,1,opt,name=minCount"`
}

// WorkloadPodGroupSchedulingConstraints 定义叶子级别的调度约束（如拓扑）。
//

// ---
//
// 这是一个可复用的构建块，用于嵌入控制器自己的 API，与其他调度字段（例如 Job 的
// spec.scheduling）并列。建议创建后冻结此字段（+k8s:immutable），因为编译后的 Workload 中
// 约束是不可变的。例如：
//
//	type JobSchedulingConfiguration struct {
//		// SchedulingConstraints 定义 Job 的 Pod 的调度约束（如拓扑）。
//		// 创建后此字段不可变。
//		// +optional
//		// +k8s:optional
//		// +k8s:immutable
//		SchedulingConstraints *schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints `json:"schedulingConstraints,omitempty" protobuf:"bytes,N,opt,name=schedulingConstraints"`
//
//		// 其他调度字段
//	}

type WorkloadPodGroupSchedulingConstraints struct {
	// topology 指定 PodGroup 中所有 Pod 期望的拓扑放置位置。
	// 未设置时不请求任何拓扑放置。
	//
	// +optional
	// +k8s:optional
	// +k8s:maxItems=1
	// +listType=atomic
	// +k8s:listType=atomic
	Topology []TopologyConstraint `json:"topology,omitempty" protobuf:"bytes,1,rep,name=topology"`
}

// WorkloadPodGroupResourceClaim 引用组内 Pod 共享的动态资源申领。
//

// ---
//
// 这是一个可复用的构建块，作为列表嵌入控制器自己的 API，与其他调度字段（例如 Job 的
// spec.scheduling）并列。建议创建后冻结整个列表（+k8s:immutable），因为编译后的 Workload 中
// 该列表不可变。例如：
//
//	type JobSchedulingConfiguration struct {
//		// ResourceClaims 列出组内 Pod 共享的 ResourceClaim。
//		// 最多可设置 4 项，与最终 PodGroup 的限制一致。
//		// 创建后此列表不可变：条目不可增删、修改。
//		// +optional
//		// +patchMergeKey=name
//		// +patchStrategy=merge
//		// +listType=map
//		// +listMapKey=name
//		// +k8s:optional
//		// +k8s:listType=map
//		// +k8s:listMapKey=name
//		// +k8s:maxItems=4
//		// +k8s:immutable
//		ResourceClaims []schedulingv1alpha3.WorkloadPodGroupResourceClaim `json:"resourceClaims,omitempty" protobuf:"bytes,N,rep,name=resourceClaims"`
//
//		// 其他调度字段
//	}

type WorkloadPodGroupResourceClaim struct {
	// name 在组内唯一标识此资源申领，为必填项，必须是 DNS_LABEL。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-short-name
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// resourceClaimName 是同一命名空间中 ResourceClaim 对象的名称。
	// 此字段可选；未指定时不使用资源申领。若设置，必须是 DNS 子域名。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:format=k8s-long-name
	ResourceClaimName *string `json:"resourceClaimName,omitempty" protobuf:"bytes,2,opt,name=resourceClaimName"`

	// resourceClaimTemplateName 是同一命名空间中 ResourceClaimTemplate 对象的名称。
	// 此字段可选；未指定时不使用资源申领模板。若设置，必须是 DNS 子域名。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:format=k8s-long-name
	ResourceClaimTemplateName *string `json:"resourceClaimTemplateName,omitempty" protobuf:"bytes,3,opt,name=resourceClaimTemplateName"`
}

// WorkloadPodGroupDisruptionMode 定义组内各 Pod 可被中断的方式。
// 必须且只能设置一种模式。
//

// ---
//
// 这是一个可复用的构建块，用于嵌入控制器自己的 API，与其他调度字段（例如 Job 的
// spec.scheduling）并列。建议创建后冻结此字段（+k8s:immutable），因为编译后的 Workload 中
// 所选模式不可变。例如：
//
//	type JobSchedulingConfiguration struct {
//		// DisruptionMode 定义 Job 的 Pod 可被中断的方式。
//		// 取值为 Single 或 All。
//		// 创建后此字段不可变：不可增删，所选模式也不可修改。
//		// +optional
//		// +k8s:optional
//		// +k8s:immutable
//		DisruptionMode *schedulingv1alpha3.WorkloadPodGroupDisruptionMode `json:"disruptionMode,omitempty" protobuf:"bytes,N,opt,name=disruptionMode"`
//
//		// 其他调度字段
//	}
//

// +union
type WorkloadPodGroupDisruptionMode struct {
	// single 指定各 Pod 可彼此独立地被中断。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	Single *WorkloadPodGroupSingleDisruptionMode `json:"single,omitempty" protobuf:"bytes,1,opt,name=single"`

	// all 指定组内所有 Pod 必须作为一个整体被中断。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	All *WorkloadPodGroupAllDisruptionMode `json:"all,omitempty" protobuf:"bytes,2,opt,name=all"`
}

// WorkloadPodGroupSingleDisruptionMode 表示各 Pod 可被独立中断。
type WorkloadPodGroupSingleDisruptionMode struct {
	// 当前故意留空。
}

// WorkloadPodGroupAllDisruptionMode 表示组内所有 Pod 必须作为一个整体被中断。
type WorkloadPodGroupAllDisruptionMode struct {
	// 当前故意留空。
}

// WorkloadCompositePodGroupSchedulingPolicy 定义由负载控制器管理的 CompositePodGroup
// （组的组）的调度策略。必须且只能设置一种策略。
//

// ---
//
// 这是一个可复用的构建块，用于嵌入复合控制器自己的 API，与其他调度字段并列。
// 它是 WorkloadPodGroupSchedulingPolicy 在“组的组”层面的对应物。复合调度策略不可变，
// 建议创建后冻结整个字段（+k8s:immutable）。例如：
//
//	type JobSetSchedulingConfiguration struct {
//		// SchedulingPolicy 定义此 JobSet 的调度策略。
//		// Basic 或 Gang 必须且只能设置其一。
//		// 创建后此字段不可变。
//		// +optional
//		// +k8s:optional
//		// +k8s:immutable
//		SchedulingPolicy *schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy `json:"schedulingPolicy,omitempty" protobuf:"bytes,N,opt,name=schedulingPolicy"`
//
//		// 其他调度字段
//	}
//

// +union
type WorkloadCompositePodGroupSchedulingPolicy struct {
	// basic 指定此复合组的子组应被独立调度。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	Basic *WorkloadCompositePodGroupBasicSchedulingPolicy `json:"basic,omitempty" protobuf:"bytes,1,opt,name=basic"`

	// gang 指定此复合组的子组应使用全有或全无语义调度。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	Gang *WorkloadCompositePodGroupGangSchedulingPolicy `json:"gang,omitempty" protobuf:"bytes,2,opt,name=gang"`
}

// WorkloadCompositePodGroupBasicSchedulingPolicy 表示复合组的子组应被独立调度。
type WorkloadCompositePodGroupBasicSchedulingPolicy struct {
	// 当前故意留空。
}

// WorkloadCompositePodGroupGangSchedulingPolicy 定义复合组子组 gang 调度的参数。
type WorkloadCompositePodGroupGangSchedulingPolicy struct {
	// minGroupCount 是调度器接纳整个复合组前，必须可同时调度的子组最小数量。
	// 此字段可选；未指定时控制器应注入符合上下文的合理默认值（如子组数量）。
	// 若设置，必须为正整数。
	//
	// +optional
	// +k8s:optional
	// +k8s:minimum=1
	MinGroupCount *int32 `json:"minGroupCount,omitempty" protobuf:"varint,1,opt,name=minGroupCount"`
}

// WorkloadCompositePodGroupSchedulingConstraints 定义由负载控制器管理的 CompositePodGroup
// （组的组）的复合级调度约束（如拓扑）。
//

// ---
//
// 这是一个可复用的构建块，用于嵌入复合控制器自己的 API，与其他调度字段并列。
// 它是 WorkloadPodGroupSchedulingConstraints 在“组的组”层面的对应物。建议创建后冻结此字段
// （+k8s:immutable），因为编译后的 Workload 中约束不可变。例如：
//
//	type JobSetSchedulingConfiguration struct {
//		// SchedulingConstraints 定义此 JobSet 的调度约束（如拓扑）。
//		// 创建后此字段不可变。
//		// +optional
//		// +k8s:optional
//		// +k8s:immutable
//		SchedulingConstraints *schedulingv1alpha3.WorkloadCompositePodGroupSchedulingConstraints `json:"schedulingConstraints,omitempty" protobuf:"bytes,N,opt,name=schedulingConstraints"`
//
//		// 其他调度字段
//	}

type WorkloadCompositePodGroupSchedulingConstraints struct {
	// topology 指定复合组内所有子组期望的拓扑放置位置。
	// 未设置时不请求任何拓扑放置。
	//
	// +optional
	// +k8s:optional
	// +k8s:maxItems=1
	// +listType=atomic
	// +k8s:listType=atomic
	Topology []TopologyConstraint `json:"topology,omitempty" protobuf:"bytes,1,rep,name=topology"`
}

// WorkloadCompositePodGroupDisruptionMode 定义复合组的子组可被中断的方式。
// 必须且只能设置一种模式。
//

// ---
//
// 这是一个可复用的构建块，用于嵌入复合控制器自己的 API，与其他调度字段并列。
// 它是 WorkloadPodGroupDisruptionMode 在“组的组”层面的对应物。建议创建后冻结此字段
// （+k8s:immutable），因为编译后的 Workload 中所选模式不可变。例如：
//
//	type JobSetSchedulingConfiguration struct {
//		// DisruptionMode 定义此 JobSet 的子组可被中断的方式。
//		// 取值为 Single 或 All。
//		// 创建后此字段不可变：不可增删，所选模式也不可修改。
//		// +optional
//		// +k8s:optional
//		// +k8s:immutable
//		DisruptionMode *schedulingv1alpha3.WorkloadCompositePodGroupDisruptionMode `json:"disruptionMode,omitempty" protobuf:"bytes,N,opt,name=disruptionMode"`
//
//		// 其他调度字段
//	}
//

// +union
type WorkloadCompositePodGroupDisruptionMode struct {
	// single 指定各子组可彼此独立地被中断。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	Single *WorkloadCompositePodGroupSingleDisruptionMode `json:"single,omitempty" protobuf:"bytes,1,opt,name=single"`

	// all 指定所有子组必须作为一个整体被中断。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	All *WorkloadCompositePodGroupAllDisruptionMode `json:"all,omitempty" protobuf:"bytes,2,opt,name=all"`
}

// WorkloadCompositePodGroupSingleDisruptionMode 表示各子组可被独立中断。
type WorkloadCompositePodGroupSingleDisruptionMode struct {
	// 当前故意留空。
}

// WorkloadCompositePodGroupAllDisruptionMode 表示所有子组必须作为一个整体被中断。
type WorkloadCompositePodGroupAllDisruptionMode struct {
	// 当前故意留空。
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:supportsSubresource="/status"

// CompositePodGroup 表示多个 PodGroup 组合在一起后的运行时实例。
// CompositePodGroup 由负载控制器（LWS、JobSet 等）根据 Workload.compositePodGroupTemplates 创建。
// CompositePodGroup API 的启用由 CompositePodGroup 特性门控控制。
type CompositePodGroup struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	//
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 定义 CompositePodGroup 的期望状态。
	//
	// +required
	Spec CompositePodGroupSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 表示 CompositePodGroup 当前观测到的状态。
	//
	// +optional
	Status CompositePodGroupStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CompositePodGroupList 包含一组 CompositePodGroup 资源。
type CompositePodGroupList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	//
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 CompositePodGroup 列表。
	Items []CompositePodGroup `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// CompositePodGroupSpec 定义 CompositePodGroup 的期望状态。
type CompositePodGroupSpec struct {
	// parentCompositePodGroupName 是同一命名空间中父复合 PodGroup 的名称，必须是 DNS 名称。
	// 为 nil 时，此复合 PodGroup 是负载层级结构的根。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	// +k8s:format=k8s-long-name
	ParentCompositePodGroupName *string `json:"parentCompositePodGroupName,omitempty" protobuf:"bytes,1,opt,name=parentCompositePodGroupName"`

	// workloadRef 引用创建 CompositePodGroup 时所用 Workload 对象中的 CompositePodGroup 模板。
	// 此字段为必填且不可变。
	//
	// +required
	// +k8s:required
	// +k8s:immutable
	WorkloadRef *WorkloadReference `json:"workloadRef" protobuf:"bytes,2,opt,name=workloadRef"`

	// schedulingPolicy 定义该 CompositePodGroup 实例的调度策略。
	// 控制器应从 CompositePodGroupTemplate 复制此字段，且不可变。
	//
	// +required
	// +k8s:immutable
	SchedulingPolicy CompositePodGroupSchedulingPolicy `json:"schedulingPolicy" protobuf:"bytes,3,opt,name=schedulingPolicy"`

	// schedulingConstraints 定义此 CompositePodGroup 的可选调度约束（如拓扑）。
	// 控制器应从 CompositePodGroupTemplate 复制此字段，且不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	SchedulingConstraints *CompositePodGroupSchedulingConstraints `json:"schedulingConstraints,omitempty" protobuf:"bytes,4,opt,name=schedulingConstraints"`

	// disruptionMode 定义 CompositePodGroup 可被中断的方式。控制器应从
	// CompositePodGroupTemplate 复制此字段。取值为 Single 或 All，未设置时默认为 Single。
	// 此字段不可变。
	//
	// +default={"single": {}}
	// +optional
	// +k8s:optional
	// +k8s:immutable
	DisruptionMode *CompositeDisruptionMode `json:"disruptionMode,omitempty" protobuf:"bytes,5,opt,name=disruptionMode"`

	// priorityClassName 定义调度此 CompositePodGroup 时应考虑的优先级类名。控制器应从
	// CompositePodGroupTemplate 复制此字段。若未指定，其校验和解析方式与 Pod 的 PriorityClassName
	// 相同（即未指定优先级类时，准入控制可设为全局默认优先级类（如存在），否则复合 PodGroup
	// 的优先级为零）。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:format=k8s-long-name
	// +k8s:immutable
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,6,opt,name=priorityClassName"`

	// priority 是此复合 PodGroup 的优先级值，各系统组件据此判断其优先级。启用 Priority
	// 准入控制器时用户不可设置此字段，其值由控制器根据 PriorityClassName 填充。
	// 值越大优先级越高。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	// +k8s:maximum=1000000000 # HighestUserDefinablePriority
	Priority *int32 `json:"priority,omitempty" protobuf:"varint,7,opt,name=priority"`

	// preemptionPolicy 是抢占低优先级 Pod/PodGroup 的策略，取值为 Never 或
	// PreemptLowerPriority，未设置时默认为 PreemptLowerPriority。启用 Priority 准入控制器时，
	// 其值由控制器根据 PriorityClassName 填充；PriorityClass 中未设置时亦默认为 PreemptLowerPriority。
	// 此字段不可变，且仅当 PodGroupPreemptionPolicy 特性门控启用时可用。
	//
	// +featureGate=PodGroupPreemptionPolicy
	// +optional
	// +k8s:immutable
	// +k8s:ifDisabled("PodGroupPreemptionPolicy")=+k8s:forbidden
	// +k8s:ifEnabled("PodGroupPreemptionPolicy")=+k8s:optional
	PreemptionPolicy *PreemptionPolicy `json:"preemptionPolicy,omitempty" protobuf:"bytes,8,opt,name=preemptionPolicy"`
}

// CompositePodGroupSchedulingPolicy 定义 CompositePodGroup 的调度配置。
// 必须且只能设置一种策略。
//

// +union
type CompositePodGroupSchedulingPolicy struct {
	// basic 指定此复合组的各子组应被独立调度，此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	// +k8s:unionMember
	Basic *CompositeBasicSchedulingPolicy `json:"basic,omitempty" protobuf:"bytes,1,opt,name=basic"`

	// gang 指定此复合组的各子组应使用全有或全无语义调度。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:update=NoSet
	// +k8s:update=NoUnset
	Gang *CompositeGangSchedulingPolicy `json:"gang,omitempty" protobuf:"bytes,2,opt,name=gang"`
}

// CompositeBasicSchedulingPolicy 表示复合组中的各子组应被独立调度。
type CompositeBasicSchedulingPolicy struct {
	// 此结构故意留空。它的存在表示应应用基础子组调度策略。将来可能新增字段，
	// 用于描述复合 PodGroup 级别、不涉及“全有或全无”（gang）调度的约束。
}

// CompositeGangSchedulingPolicy 表示复合组中的各子组应使用全有或全无语义调度。
type CompositeGangSchedulingPolicy struct {
	// minGroupCount 是调度器接纳整个组前，必须可同时调度或已调度的子组最小数量。
	// 必须为正整数。
	//
	// +required
	// +k8s:required
	// +k8s:minimum=1
	MinGroupCount int32 `json:"minGroupCount" protobuf:"varint,1,req,name=minGroupCount"`
}

// CompositeDisruptionMode 定义复合 PodGroup 中各实体可被中断的方式。
// 必须且只能设置一种模式。
//

// +union
type CompositeDisruptionMode struct {
	// single 指定各子组可彼此独立地被中断。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	Single *SingleCompositeDisruptionMode `json:"single,omitempty" protobuf:"bytes,1,opt,name=single"`

	// all 指定所有子组只能作为一个整体被中断。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	All *AllCompositeDisruptionMode `json:"all,omitempty" protobuf:"bytes,2,opt,name=all"`
}

// SingleCompositeDisruptionMode 表示 CompositePodGroup 的各子对象可被独立中断或抢占。
type SingleCompositeDisruptionMode struct {
	// 此结构故意留空。
}

// AllCompositeDisruptionMode 表示 CompositePodGroup 的各子对象只能作为一个整体被中断或抢占。
type AllCompositeDisruptionMode struct {
	// 此结构故意留空。
}

// CompositePodGroupStatus 表示复合 PodGroup 的状态信息。
type CompositePodGroupStatus struct {
	// conditions 是对 CompositePodGroup 状态的最新观测。
	//
	// 已知条件类型：
	// - "CompositePodGroupInitiallyScheduled"：表示此 CompositePodGroup 下整棵子树的调度要求
	//   是否已满足。一旦转为 True 即为终态，即使随后 Pod 被删除、组约束不再满足，也不会回退为 False。
	// - "DisruptionTarget"：表示 CompositePodGroup 即将因抢占等中断而终止。
	//
	// CompositePodGroupInitiallyScheduled 条件的已知原因：
	// - "Unschedulable"：因资源限制、亲和/反亲和或拓扑约束而无法放置子树。
	// - "SchedulerError"：因调度过程中发生的内部错误而无法调度。
	// - "Invalid"：kube-scheduler 在运行时校验中检测到无效组布局时置为 True。
	//   `message` 字段详述具体的布局违规（如检测到环、深度超过 4、引用多个不同 Workload）。
	//
	// DisruptionTarget 条件的已知原因：
	// - "PreemptionByScheduler"：CompositePodGroup 被调度器抢占循环选中，为更高优先级的抢占者腾出容量。
	//

	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// CompositePodGroupSchedulingConstraints 定义 CompositePodGroup 的调度约束（如拓扑）。
type CompositePodGroupSchedulingConstraints struct {
	// topology 定义复合 PodGroup 的拓扑约束，目前只能指定一条，将来可能改变。
	//
	// +optional
	// +k8s:optional
	// +k8s:maxItems=1
	// +listType=atomic
	// +k8s:listType=atomic
	Topology []TopologyConstraint `json:"topology,omitempty" protobuf:"bytes,1,rep,name=topology"`
}
