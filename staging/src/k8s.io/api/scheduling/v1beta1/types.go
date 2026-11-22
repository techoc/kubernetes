/*
Copyright 2018 The Kubernetes Authors.

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
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.11
// +k8s:prerelease-lifecycle-gen:deprecated=1.14
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=scheduling.k8s.io,v1,PriorityClass

// 已弃用 - 本组版本的 PriorityClass 已被 scheduling.k8s.io/v1/PriorityClass 取代。
// PriorityClass 定义从优先级类名到优先级整数值的映射，值可以是任意有效整数。
type PriorityClass struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// value 表示此优先级类的整数值。当 Pod 的 spec 中指定该类名时，
	// 这就是 Pod 实际获得的优先级。
	// +optional
	Value int32 `json:"value" protobuf:"bytes,2,opt,name=value"`

	// globalDefault 指定此 PriorityClass 是否作为未指定优先级类的 Pod 的默认优先级。
	// 只能有一个 PriorityClass 被标记为 `globalDefault`。但若存在多个
	// `globalDefault` 为 true 的 PriorityClass，则取其中值最小者作为默认优先级。
	// +optional
	GlobalDefault bool `json:"globalDefault,omitempty" protobuf:"bytes,3,opt,name=globalDefault"`

	// description 是任意字符串，通常说明何时应使用此优先级类。
	// +optional
	Description string `json:"description,omitempty" protobuf:"bytes,4,opt,name=description"`

	// preemptionPolicy 是抢占低优先级 Pod 的策略，取值 Never 或 PreemptLowerPriority。
	// 未设置时默认为 PreemptLowerPriority。
	// +optional
	PreemptionPolicy *apiv1.PreemptionPolicy `json:"preemptionPolicy,omitempty" protobuf:"bytes,5,opt,name=preemptionPolicy"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.11
// +k8s:prerelease-lifecycle-gen:deprecated=1.14
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=scheduling.k8s.io,v1,PriorityClassList

// PriorityClassList 是优先级类的集合。
type PriorityClassList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 PriorityClass 列表。
	Items []PriorityClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37

// Workload 用于表达从调度角度管理工作负载生命周期时应遵循的调度约束，
// 涵盖调度、抢占、驱逐等阶段。Workload API 的启用由 GenericWorkload 特性门控切换。
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
// +k8s:prerelease-lifecycle-gen:introduced=1.37

// WorkloadList 包含 Workload 资源列表。
type WorkloadList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准列表元数据。
	//
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 Workload 列表。
	Items []Workload `json:"items" protobuf:"bytes,2,rep,name=items"`
}

const (
	// WorkloadMaxPodGroupTemplates 是每个 Workload 允许的 Pod 组模板上限。
	WorkloadMaxPodGroupTemplates = 8
	// WorkloadMaxTreeDepth 是 Workload 中（复合）Pod 组模板树的最大允许深度。
	WorkloadMaxTreeDepth = 4
)

// WorkloadSpec 定义 Workload 的期望状态。
type WorkloadSpec struct {
	// controllerRef 是对控制对象（如 Deployment 或 Job）的可选引用，
	// 供 CLI 等工具回链到原始工作负载定义。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	ControllerRef *TypedLocalObjectReference `json:"controllerRef,omitempty" protobuf:"bytes,1,opt,name=controllerRef"`

	// podGroupTemplates 是构成 Workload 的模板列表，最多 8 个。
	// Workload 创建后不能增删模板，但已有模板中允许变更的字段仍可更新。
	// CompositePodGroupTemplates 与 PodGroupTemplates 必须且只能设置一个。
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

	// compositePodGroupTemplates 是构成 Workload 的 CompositePodGroup 模板列表，最多 8 个。
	// 此字段不可变，且与 PodGroupTemplates 必须且只能设置一个。
	//
	// 仅在 CompositePodGroup 特性门控启用时此字段才生效。
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

// TypedLocalObjectReference 允许引用同一命名空间内带类型的对象。
type TypedLocalObjectReference struct {
	// apiGroup 是被引用资源所属的组。若为空，则 Kind 必须属于核心 API 组。
	// 其他第三方类型必须设置 apiGroup，且值必须是 DNS 子域。
	//
	// +optional
	// +k8s:optional
	// +k8s:format=k8s-long-name
	APIGroup string `json:"apiGroup,omitempty" protobuf:"bytes,1,opt,name=apiGroup"`
	// kind 是被引用资源的类型，必须是合法的路径段名称。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-path-segment-name
	Kind string `json:"kind" protobuf:"bytes,2,opt,name=kind"`
	// name 是被引用资源的名称，必须是合法的路径段名称。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-path-segment-name
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`
}

// MaxPodGroupResourceClaims 是 PodGroup 或 Workload 的 PodGroupTemplate 中资源声明的上限。
const MaxPodGroupResourceClaims = 4

// PodGroupTemplate 表示带调度策略的一组 Pod 的模板。
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

	// schedulingConstraints 定义此 PodGroupTemplate 的可选调度约束（如拓扑），
	// 仅在 TopologyAwareWorkloadScheduling 特性门控启用时可用，且此字段不可变。
	//
	// +featureGate=TopologyAwareWorkloadScheduling
	// +optional
	// +k8s:ifDisabled(TopologyAwareWorkloadScheduling)=+k8s:forbidden
	// +k8s:ifEnabled(TopologyAwareWorkloadScheduling)=+k8s:optional
	// +k8s:immutable
	SchedulingConstraints *PodGroupSchedulingConstraints `json:"schedulingConstraints" protobuf:"bytes,3,opt,name=schedulingConstraints"`

	// resourceClaims 定义组内 Pod 可共享的 ResourceClaim。Pod 通过在自身
	// Spec.ResourceClaims 中定义与 PodGroup 的声明完全一致的条目来消费分配给该
	// PodGroup 的设备；名称必须相同，并指向同一个 ResourceClaim 或 ResourceClaimTemplate。
	//
	// 这是 beta 字段，需要启用 DRAWorkloadResourceClaims 特性门控。
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

	// disruptionMode 定义 PodGroup 可被中断的方式，取值 Single 或 All，且不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	DisruptionMode *DisruptionMode `json:"disruptionMode,omitempty" protobuf:"bytes,5,opt,name=disruptionMode"`

	// priorityClassName 指明调度由此模板创建的 Pod 组时应考虑的优先级类，此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:format=k8s-long-name
	// +k8s:immutable
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,6,opt,name=priorityClassName"`

	// priority 是由此模板创建的 Pod 组的优先级值，各系统组件据此判断 Pod 组的优先级。
	// 值越大优先级越高。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:maximum=1000000000 # HighestUserDefinablePriority
	// +k8s:immutable
	Priority *int32 `json:"priority,omitempty" protobuf:"varint,7,opt,name=priority"`

	// preemptionPolicy 是抢占低优先级 Pod/Pod 组的策略，取值 Never 或 PreemptLowerPriority。
	// 此字段不可变，且仅在 PodGroupPreemptionPolicy 特性门控启用时可用。
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
	// name 是 CompositePodGroupTemplate 在 Workload 内的唯一标识，必须是 DNS 标签，为必填项。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-short-name
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// schedulingPolicy 定义此模板的调度策略。
	//
	// +required
	SchedulingPolicy CompositePodGroupSchedulingPolicy `json:"schedulingPolicy" protobuf:"bytes,2,opt,name=schedulingPolicy"`

	// schedulingConstraints 定义此 CompositePodGroupTemplate 的可选调度约束（如拓扑），此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	SchedulingConstraints *CompositePodGroupSchedulingConstraints `json:"schedulingConstraints,omitempty" protobuf:"bytes,3,opt,name=schedulingConstraints"`

	// disruptionMode 定义 CompositePodGroup 可被中断的方式，取值 Single 或 All，且不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	DisruptionMode *CompositeDisruptionMode `json:"disruptionMode,omitempty" protobuf:"bytes,4,opt,name=disruptionMode"`

	// priorityClassName 指明调度由此模板创建的复合 Pod 组时应考虑的优先级类。
	// 未指定优先级类时，若存在全局默认优先级类，准入控制会将其填入；否则
	// 由此模板创建的复合 Pod 组优先级为零。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:format=k8s-long-name
	// +k8s:immutable
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,5,opt,name=priorityClassName"`

	// priority 是由此模板创建的复合 Pod 组的优先级值，各系统组件据此判断其优先级。
	// 启用 Priority 准入控制器时用户无法设置此字段，由准入控制器根据 PriorityClassName 填充。
	// 值越大优先级越高。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:maximum=1000000000 # HighestUserDefinablePriority
	// +k8s:immutable
	Priority *int32 `json:"priority,omitempty" protobuf:"varint,6,opt,name=priority"`

	// preemptionPolicy 是抢占低优先级 Pod/Pod 组的策略，取值 Never 或 PreemptLowerPriority。
	// 此字段不可变，且仅在 PodGroupPreemptionPolicy 特性门控启用时可用。
	//
	// +featureGate=PodGroupPreemptionPolicy
	// +optional
	// +k8s:immutable
	// +k8s:ifDisabled("PodGroupPreemptionPolicy")=+k8s:forbidden
	// +k8s:ifEnabled("PodGroupPreemptionPolicy")=+k8s:optional
	PreemptionPolicy *PreemptionPolicy `json:"preemptionPolicy,omitempty" protobuf:"bytes,7,opt,name=preemptionPolicy"`

	// podGroupTemplates 是子 PodGroup 的模板列表，最多 8 个。
	// CompositePodGroupTemplates 或 PodGroupTemplates 中至少要设置一个条目。
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

	// compositePodGroupTemplates 是子 CompositePodGroup 的模板列表，最多 8 个。
	// CompositePodGroupTemplates 或 PodGroupTemplates 中至少要设置一个条目。
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

// PodGroupSchedulingPolicy 定义 PodGroup 的调度配置，必须且只能设置一种策略。
// 创建时通过设置 Basic 或 Gang 字段选定策略，之后不可更改。
// 所选策略内部的字段若各自允许，则创建后仍可更新。
//
// +union
type PodGroupSchedulingPolicy struct {
	// basic 指定组内 Pod 使用标准 Kubernetes 调度行为。创建组时设置此字段即选用基础调度，
	// 之后不可更改。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:immutable
	Basic *BasicSchedulingPolicy `json:"basic,omitempty" protobuf:"bytes,1,opt,name=basic"`

	// gang 指定组内 Pod 使用全有或全无（all-or-nothing）语义调度。创建组时设置此字段即选用 gang 调度，
	// 之后不可设置或取消。Gang 策略中的 minCount 字段在组创建后仍可修改。
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
	// 此处刻意留空。其存在即表示应应用基础调度策略。将来可能新增字段，
	// 用于描述 Pod 组级别、不涉及全有或全无（gang）调度的约束。
}

// GangSchedulingPolicy 定义 gang 调度的参数。
type GangSchedulingPolicy struct {
	// minCount 是调度器接纳整个组时，必须同时可调度或已调度的最小 Pod 数，必须为正整数。
	// 此字段可变，以支持工作负载扩缩容。
	//
	// 注意调度器基于最终一致模型运行：受传播延迟影响，minCount 的更新可能不会立即
	// 反映到调度决策中。若在该组的调度周期进行期间更新 minCount，新值可能要到下一周期才生效。
	// 此外 minCount 仅在调度时校验，因此修改它不影响已调度的 Pod，只作用于后续周期中待评估的 Pod。
	//
	// +required
	// +k8s:required
	// +k8s:minimum=1
	MinCount int32 `json:"minCount" protobuf:"varint,1,opt,name=minCount"`
}

// PodGroupResourceClaim 恰好引用一个 ResourceClaim，可直接引用，也可命名一个
// ResourceClaimTemplate，由其为 PodGroup 生成 ResourceClaim。
//
// 该声明带有在 PodGroup 内唯一标识此 ResourceClaim 的名称。需要访问该 ResourceClaim 的 Pod
// 需在自己的 Spec.ResourceClaims 中定义匹配的引用，且所有字段必须与 PodGroup 的声明完全一致。
type PodGroupResourceClaim struct {
	// name 在 PodGroup 内唯一标识此资源声明，必须是 DNS_LABEL。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-short-name
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// resourceClaimName 是与 PodGroup 同命名空间下 ResourceClaim 对象的名称。
	// 该 ResourceClaim 将保留给 PodGroup 而非其各个 Pod 使用。
	//
	// ResourceClaimName 与 ResourceClaimTemplateName 必须且只能设置一个。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:format=k8s-long-name
	ResourceClaimName *string `json:"resourceClaimName,omitempty" protobuf:"bytes,2,opt,name=resourceClaimName"`

	// resourceClaimTemplateName 是与 PodGroup 同命名空间下 ResourceClaimTemplate 对象的名称。
	//
	// 该模板用于创建绑定到本 PodGroup 的新 ResourceClaim。PodGroup 删除时该 ResourceClaim
	// 也会被删除。其唯一名称由 PodGroup 名、资源名及一段生成组件拼接而成，并记录在
	// podgroup.status.resourceClaimStatuses 中。
	//
	// 此字段不可变，控制面创建 ResourceClaim 后不会再对其做任何修改。
	//
	// ResourceClaimName 与 ResourceClaimTemplateName 必须且只能设置一个。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:format=k8s-long-name
	ResourceClaimTemplateName *string `json:"resourceClaimTemplateName,omitempty" protobuf:"bytes,3,opt,name=resourceClaimTemplateName"`
}

// DisruptionMode 定义组内各个实体可被中断的方式，必须且只能设置一种。
//
// +union
type DisruptionMode struct {
	// single 指定各子项可以彼此独立地被中断。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	Single *SingleDisruptionMode `json:"single,omitempty" protobuf:"bytes,1,opt,name=single"`

	// all 指定所有子项只能一起被中断。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	All *AllDisruptionMode `json:"all,omitempty" protobuf:"bytes,2,opt,name=all"`
}

// SingleDisruptionMode 指定各子项可被独立中断。
type SingleDisruptionMode struct {
	// 当前刻意留空。
}

// AllDisruptionMode 指定各子项只能一起被中断。
type AllDisruptionMode struct {
	// 当前刻意留空。
}

// PreemptionPolicy 描述是否/何时抢占 Pod 的策略。
// +enum
// +k8s:enum
type PreemptionPolicy string

const (
	// PreemptLowerPriority 表示 Pod 可以抢占优先级更低的其他 Pod。
	PreemptLowerPriority PreemptionPolicy = "PreemptLowerPriority"
	// PreemptNever 表示 Pod 从不抢占其他 Pod。
	PreemptNever PreemptionPolicy = "Never"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37
// +k8s:supportsSubresource="/status"

// PodGroup 表示一组 Pod 的运行时实例，由工作负载控制器（Job、LWS、JobSet 等）
// 根据 Workload.podGroupTemplates 创建。PodGroup API 的启用由 GenericWorkload 特性门控切换。
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
// +k8s:prerelease-lifecycle-gen:introduced=1.37

// PodGroupList 包含 PodGroup 资源列表。
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
	// parentCompositePodGroupName 包含与本 Pod 组同命名空间的父复合 Pod 组的名称。
	// 若为 nil，则本 Pod 组是工作负载层级的根节点。
	// 此字段仅在 CompositePodGroup 特性门控启用时使用，且不可变。
	//
	// +featureGate=CompositePodGroup
	// +optional
	// +k8s:ifDisabled(CompositePodGroup)=+k8s:forbidden
	// +k8s:ifEnabled(CompositePodGroup)=+k8s:optional
	// +k8s:immutable
	// +k8s:format=k8s-long-name
	// +k8s:alpha(since: "1.37")=+k8s:dependentRequired("workloadRef")
	ParentCompositePodGroupName *string `json:"parentCompositePodGroupName,omitempty" protobuf:"bytes,1,opt,name=parentCompositePodGroupName"`

	// workloadRef 指向用于创建此 PodGroup 的 Workload 对象中相应的 PodGroup 模板（可选），此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	WorkloadRef *WorkloadReference `json:"workloadRef,omitempty" protobuf:"bytes,2,opt,name=workloadRef"`

	// schedulingPolicy 定义此 PodGroup 实例的调度策略，控制器应从 PodGroupTemplate 复制填充此字段。
	//
	// +required
	SchedulingPolicy PodGroupSchedulingPolicy `json:"schedulingPolicy" protobuf:"bytes,3,opt,name=schedulingPolicy"`

	// schedulingConstraints 定义此 PodGroup 的可选调度约束（如拓扑），控制器应从 PodGroupTemplate 复制填充。
	// 此字段不可变，且仅在 TopologyAwareWorkloadScheduling 特性门控启用时可用。
	//
	// +featureGate=TopologyAwareWorkloadScheduling
	// +optional
	// +k8s:ifDisabled(TopologyAwareWorkloadScheduling)=+k8s:forbidden
	// +k8s:ifEnabled(TopologyAwareWorkloadScheduling)=+k8s:optional
	// +k8s:ifEnabled(TopologyAwareWorkloadScheduling)=+k8s:immutable
	SchedulingConstraints *PodGroupSchedulingConstraints `json:"schedulingConstraints,omitempty" protobuf:"bytes,4,opt,name=schedulingConstraints"`

	// resourceClaims 定义组内 Pod 可共享的 ResourceClaim。Pod 通过在自身
	// Spec.ResourceClaims 中定义与 PodGroup 的声明完全一致的条目来消费分配给该
	// PodGroup 的设备；名称必须相同，并指向同一个 ResourceClaim 或 ResourceClaimTemplate。
	//
	// 这是 beta 字段，需要启用 DRAWorkloadResourceClaims 特性门控。
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

	// disruptionMode 定义 PodGroup 可被中断的方式，控制器应从 PodGroupTemplate 复制填充。
	// 取值 Single 或 All，未设置时默认为 Single。此字段不可变。
	//
	// +default={"single": {}}
	// +optional
	// +k8s:optional
	// +k8s:immutable
	DisruptionMode *DisruptionMode `json:"disruptionMode,omitempty" protobuf:"bytes,6,opt,name=disruptionMode"`

	// priorityClassName 定义调度此 Pod 组时应考虑的优先级类，控制器应从 PodGroupTemplate 复制填充。
	// 否则其校验和解析方式与 PodGroupTemplate 上的 PriorityClassName 相同（即未指定优先级类时，
	// 若存在全局默认优先级类，准入控制会将其填入；否则 Pod 组的优先级为零）。此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	// +k8s:format=k8s-long-name
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,7,opt,name=priorityClassName"`

	// priority 是此 Pod 组的优先级值，各系统组件据此判断其优先级。启用 Priority 准入控制器时
	// 用户无法设置此字段，由准入控制器根据 PriorityClassName 填充。值越大优先级越高。
	// 此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	// +k8s:maximum=1000000000 # HighestUserDefinablePriority
	Priority *int32 `json:"priority,omitempty" protobuf:"varint,8,opt,name=priority"`

	// preemptionPolicy 是抢占低优先级 Pod/Pod 组的策略，取值 Never 或 PreemptLowerPriority，
	// 未设置时默认为 PreemptLowerPriority。启用 Priority 准入控制器时，此字段由 PriorityClassName
	// 填充；若 PriorityClass 中未设置该值，则默认为 PreemptLowerPriority。
	// 此字段不可变，且仅在 PodGroupPreemptionPolicy 特性门控启用时可用。
	//
	// +featureGate=PodGroupPreemptionPolicy
	// +optional
	// +k8s:immutable
	// +k8s:ifDisabled("PodGroupPreemptionPolicy")=+k8s:forbidden
	// +k8s:ifEnabled("PodGroupPreemptionPolicy")=+k8s:optional
	PreemptionPolicy *PreemptionPolicy `json:"preemptionPolicy,omitempty" protobuf:"bytes,9,opt,name=preemptionPolicy"`
}

// PodGroupStatus 表示 Pod 组的状态信息。
type PodGroupStatus struct {
	// conditions 表示对 PodGroup 状态的最新观测。
	//
	// 已知的条件类型：
	// - "PodGroupInitiallyScheduled"：表示调度需求是否已满足。该条件一旦转为 True 即为终态，
	//   即便后续 Pod 被驱逐、组约束不再满足，也不会回退为 False。
	// - "DisruptionTarget"：表示 PodGroup 即将因抢占等中断而被终止。
	//
	// PodGroupInitiallyScheduled 条件的已知原因：
	// - "Unschedulable"：因资源不足、亲和/反亲和规则或 gang 容量不足而无法调度。
	// - "SchedulerError"：因调度过程中的内部错误（如 nodeAffinity 解析错误）而无法调度。
	//
	// DisruptionTarget 条件的已知原因：
	// - "PreemptionByScheduler"：调度器为高优先级 PodGroup 或 Pod 腾出空间而抢占了本 PodGroup。
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

	// resourceClaimStatuses 是资源声明的状态。
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
	// PodGroupInitiallyScheduled 表示此 PodGroup 从开始到首次成功期间的调度过程状态。
	PodGroupInitiallyScheduled string = "PodGroupInitiallyScheduled"
	// DisruptionTarget 表示 PodGroup 即将因抢占等中断而被终止。
	DisruptionTarget string = "DisruptionTarget"
)

// PodGroup 的已知条件原因。
const (
	// PodGroupInitiallyScheduled 条件中的 Unschedulable 原因表示因资源不足、亲和/反亲和规则
	// 或 PodGroup 容量不足而无法调度。
	PodGroupReasonUnschedulable string = "Unschedulable"
	// PodGroupInitiallyScheduled 条件中的 SchedulerError 原因表示调度期间发生了内部错误，
	// 例如 nodeAffinity 解析错误。
	PodGroupReasonSchedulerError string = "SchedulerError"
	// DisruptionTarget 条件中的 PreemptionByScheduler 原因表示调度器为高优先级 PodGroup 或 Pod
	// 腾出空间而抢占了本 PodGroup。
	PodGroupReasonPreemptionByScheduler string = "PreemptionByScheduler"
)

// PodGroupResourceClaimStatus 存储在 PodGroupStatus 中，对应每个引用了
// ResourceClaimTemplate 的 PodGroupResourceClaim，记录所生成 ResourceClaim 的名称。
type PodGroupResourceClaimStatus struct {
	// name 在 PodGroup 内唯一标识此资源声明，必须与 podgroup.spec.resourceClaims 中
	// 某个条目的名称一致，因此该字符串必须是 DNS_LABEL。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// resourceClaimName 是为 PodGroup 在其命名空间中生成的 ResourceClaim 的名称。
	// 若未设置，则说明无需生成 ResourceClaim，此时可忽略 podgroup.spec.resourceClaims 中的对应条目。
	//
	// +optional
	// +k8s:optional
	// +k8s:format=k8s-long-name
	ResourceClaimName *string `json:"resourceClaimName,omitempty" protobuf:"bytes,2,opt,name=resourceClaimName"`
}

// WorkloadReference 引用创建某个 PodGroup 时所用的 Workload 对象及其模板。
type WorkloadReference struct {
	// workloadName 是包含创建 Pod 组所用模板的 Workload 对象名称，必须是 DNS 名称，为必填项。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-long-name
	WorkloadName string `json:"workloadName" protobuf:"bytes,1,opt,name=workloadName"`

	// templateName 是 Workload 对象中用于创建 Pod 组的模板名称，必须是 DNS 标签，为必填项。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-short-name
	TemplateName string `json:"templateName" protobuf:"bytes,2,opt,name=templateName"`
}

// PodGroupSchedulingConstraints 定义 PodGroup 的调度约束（如拓扑）。
type PodGroupSchedulingConstraints struct {
	// topology 定义 Pod 组的拓扑约束。目前只能指定单个拓扑约束，将来可能改变。
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
	// key 指定表示拓扑域的节点标签的键。PodGroup 内所有 Pod 必须位于同一域实例中。
	// 不同 PodGroup 即使源自同一 PodGroupTemplate，也可落在不同的域实例上。
	// 例如 "topology.kubernetes.io/rack"。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-label-key
	Key string `json:"key" protobuf:"bytes,1,opt,name=key"`
}

// CompositePodGroupSchedulingPolicy 定义 CompositePodGroup 的调度配置，必须且只能设置一种策略。
//
// +union
type CompositePodGroupSchedulingPolicy struct {
	// basic 指定此复合组的各子组应独立调度，此字段不可变。
	//
	// +optional
	// +k8s:optional
	// +k8s:immutable
	// +k8s:unionMember
	Basic *CompositeBasicSchedulingPolicy `json:"basic,omitempty" protobuf:"bytes,1,opt,name=basic"`

	// gang 指定此复合组的各子组使用全有或全无（all-or-nothing）语义调度。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:update=NoSet
	// +k8s:update=NoUnset
	Gang *CompositeGangSchedulingPolicy `json:"gang,omitempty" protobuf:"bytes,2,opt,name=gang"`
}

// CompositeBasicSchedulingPolicy 表示属于复合组的各子组应独立调度。
type CompositeBasicSchedulingPolicy struct {
	// 此处刻意留空。其存在即表示应应用基础组调度策略。将来可能新增字段，
	// 用于描述复合 Pod 组级别、不涉及全有或全无（gang）调度的约束。
}

// CompositeGangSchedulingPolicy 表示属于复合组的各子组使用全有或全无语义调度。
type CompositeGangSchedulingPolicy struct {
	// minGroupCount 是调度器接纳整个复合组时，必须同时可调度或已调度的子组最小数量，必须为正整数。
	//
	// +required
	// +k8s:required
	// +k8s:minimum=1
	MinGroupCount int32 `json:"minGroupCount" protobuf:"varint,1,req,name=minGroupCount"`
}

// CompositeDisruptionMode 定义复合 Pod 组内各实体可被中断的方式，必须且只能设置一种。
//
// +union
type CompositeDisruptionMode struct {
	// single 指定各子组可以彼此独立地被中断。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	Single *SingleCompositeDisruptionMode `json:"single,omitempty" protobuf:"bytes,1,opt,name=single"`

	// all 指定所有子组只能一起被中断。
	//
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	All *AllCompositeDisruptionMode `json:"all,omitempty" protobuf:"bytes,2,opt,name=all"`
}

// SingleCompositeDisruptionMode 表示 CompositePodGroup 的各个子项可被独立中断或抢占。
type SingleCompositeDisruptionMode struct {
	// 此处刻意留空。
}

// AllCompositeDisruptionMode 表示 CompositePodGroup 的各子项只能一起被中断或抢占。
type AllCompositeDisruptionMode struct {
	// 此处刻意留空。
}

// CompositePodGroupSchedulingConstraints 定义 CompositePodGroup 的调度约束（如拓扑）。
type CompositePodGroupSchedulingConstraints struct {
	// topology 定义复合 Pod 组的拓扑约束。目前只能指定单个拓扑约束，将来可能改变。
	//
	// +optional
	// +k8s:optional
	// +k8s:maxItems=1
	// +listType=atomic
	// +k8s:listType=atomic
	Topology []TopologyConstraint `json:"topology,omitempty" protobuf:"bytes,1,rep,name=topology"`
}
