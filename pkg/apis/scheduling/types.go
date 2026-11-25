/*
Copyright 2017 The Kubernetes Authors.

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

package scheduling

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/core"
)

const (
	// DefaultPriorityWhenNoDefaultClassExists 用于为未指定优先级类、
	// 且不存在标记为默认的优先级类的 Pod 设置优先级。
	DefaultPriorityWhenNoDefaultClassExists = 0
	// HighestUserDefinablePriority 是用户定义的优先级类可用的最高优先级值。
	// 大于 10 亿的优先级值保留给 Kubernetes 系统使用。
	HighestUserDefinablePriority = int32(1000000000)
	// SystemCriticalPriority 是关键系统组件优先级值范围的起始值。
	SystemCriticalPriority = 2 * HighestUserDefinablePriority
	// SystemPriorityClassPrefix 是保留给系统优先级类名称的前缀。其他优先级类
	// 不允许以此前缀开头。
	// NOTE: 为避免与用户定义的优先级类名称冲突，所有名称必须
	// 以 SystemPriorityClassPrefix 开头。
	SystemPriorityClassPrefix = "system-"
	// SystemClusterCritical 是表示 cluster-critical 的系统优先级类名称。
	SystemClusterCritical = SystemPriorityClassPrefix + "cluster-critical"
	// SystemNodeCritical 是表示 node-critical 的系统优先级类名称。
	SystemNodeCritical = SystemPriorityClassPrefix + "node-critical"

	// PodGroupProtectionFinalizer 是添加到 PodGroup 上的最终器，用于在
	// Pod 仍引用它们时防止过早删除。
	PodGroupProtectionFinalizer = GroupName + "/podgroup-protection"
)

// PreemptionPolicy 描述是否以及何时抢占 Pod/PodGroup 的策略。
type PreemptionPolicy string

const (
	// PreemptLowerPriority 表示 Pod 可以抢占优先级更低的其他 Pod。
	PreemptLowerPriority PreemptionPolicy = "PreemptLowerPriority"
	// PreemptNever 表示 Pod 从不抢占其他 Pod。
	PreemptNever PreemptionPolicy = "Never"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PriorityClass 定义从优先级类名称到优先级整数值之间的映射。
// value 可以是任意合法整数。
type PriorityClass struct {
	metav1.TypeMeta
	// 标准的对象元数据；更多信息参见: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta

	// value 表示此优先级类的整数值。当 Pod 的 spec 中指定该类名时，
	// 这就是 Pod 实际获得的优先级。
	Value int32

	// globalDefault 指定此 PriorityClass 是否应作为没有任何优先级类的 Pod 的
	// 默认优先级。只能有一个 PriorityClass 被标记为 `globalDefault`。
	// 但如果存在多个 `globalDefault` 字段为 true 的 PriorityClass，
	// 则将其中值最小的全局默认 PriorityClass 的 value 作为默认优先级。
	// +optional
	GlobalDefault bool

	// description 是任意字符串，通常说明何时应使用此优先级类。
	// +optional
	Description string

	// preemptionPolicy 是抢占较低优先级 Pod 的策略。
	// 取值为 Never 或 PreemptLowerPriority。
	// 如果未设置，默认为 PreemptLowerPriority。
	// +optional
	PreemptionPolicy *core.PreemptionPolicy
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PriorityClassList 是优先级类的集合。
type PriorityClassList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息参见: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta

	// items 是 PriorityClass 列表。
	Items []PriorityClass
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Workload 用于表达在调度视角管理工作负载生命周期时应使用的调度约束，
// 涵盖调度、抢占、驱逐及其他各阶段。
// Workload API 的启用由 GenericWorkload 特性门控控制。
type Workload struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	//
	// +optional
	metav1.ObjectMeta

	// spec 定义 Workload 的期望行为。
	// +required
	Spec WorkloadSpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// WorkloadList 包含一组 Workload 资源。
type WorkloadList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	//
	// +optional
	metav1.ListMeta

	// items 是 Workload 列表。
	Items []Workload
}

const (
	// WorkloadMaxPodGroupTemplates 是每个 Workload 允许的 Pod 组模板上限。
	WorkloadMaxPodGroupTemplates = 8
	// WorkloadMaxTemplateDepth 是 Workload 对象中定义的顶层复合 Pod 组
	// 模板允许的最大嵌套深度。
	WorkloadMaxTemplateDepth = 4
)

// WorkloadSpec 定义 Workload 的期望状态。
type WorkloadSpec struct {
	// ControllerRef 是对控制对象（如 Deployment 或 Job）的可选引用。
	// 该字段供 CLI 等工具使用，以提供回链到原始工作负载定义的链接。
	// 该字段不可变。
	//
	// +optional
	ControllerRef *TypedLocalObjectReference

	// PodGroupTemplates 是构成该 Workload 的模板列表。
	// 模板数量最多为 8 个。Workload 创建后不能添加或删除模板。
	// 已存在的模板在其各字段允许的范围内仍可更新。
	// CompositePodGroupTemplates 与 PodGroupTemplates 必须且只能设置其一。
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	PodGroupTemplates []PodGroupTemplate

	// CompositePodGroupTemplates 是构成该 Workload 的 CompositePodGroup 模板列表。
	// 模板数量最多为 8 个。该字段不可变。
	// CompositePodGroupTemplates 与 PodGroupTemplates 必须且只能设置其一。
	//
	// 仅当 CompositePodGroup 特性门控启用时使用此字段。
	//
	// +featureGate=CompositePodGroup
	// +optional
	// +listType=map
	// +listMapKey=name
	CompositePodGroupTemplates []CompositePodGroupTemplate
}

// TypedLocalObjectReference 用于引用同一命名空间内的类型化对象。
type TypedLocalObjectReference struct {
	// APIGroup 是被引用资源所属的组。若 APIGroup 为空，则指定的 Kind
	// 必须位于核心 API 组中。对于任何其他第三方类型，必须设置 APIGroup。
	// 其值必须是 DNS 子域名（DNS subdomain）。
	//
	// +optional
	APIGroup string
	// kind 是被引用资源的类型，必须是合法的路径段名称。
	// +required
	Kind string
	// name 是被引用资源的名称，必须是合法的路径段名称。
	// +required
	Name string
}

// MaxPodGroupResourceClaims 是 PodGroup 或 Workload 的 PodGroupTemplate
// 允许的最大资源申领数。
const MaxPodGroupResourceClaims = 4

// PodGroupTemplate 表示一组带调度策略的 Pod 的模板。
type PodGroupTemplate struct {
	// name 是 PodGroupTemplate 在 Workload 内的唯一标识，必须是 DNS 标签，且不可变。
	// +required
	Name string

	// SchedulingPolicy 定义此 PodGroupTemplate 的调度策略。
	//
	// +required
	SchedulingPolicy PodGroupSchedulingPolicy

	// SchedulingConstraints 为此 PodGroupTemplate 定义可选的调度约束（如拓扑）。
	// 仅当启用 TopologyAwareWorkloadScheduling 特性门控时该字段可用。
	// 该字段不可变。
	//
	// +optional
	// +featureGate=TopologyAwareWorkloadScheduling
	SchedulingConstraints *PodGroupSchedulingConstraints

	// ResourceClaims 定义组内 Pod 之间可以共享哪些 ResourceClaims。
	// Pod 通过在自己的 Spec.ResourceClaims 中定义与 PodGroup 的声明完全匹配的声明，
	// 来消费分配给 PodGroup 声明的设备。声明必须同名，并引用相同的
	// ResourceClaim 或 ResourceClaimTemplate。
	//
	// 此为 beta 级字段，需要启用 DRAWorkloadResourceClaims 特性门控。
	//
	// 该字段不可变。
	//
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge,retainKeys
	// +listType=map
	// +listMapKey=name
	// +featureGate=DRAWorkloadResourceClaims
	ResourceClaims []PodGroupResourceClaim

	// DisruptionMode 定义给定 PodGroup 可被打断（disrupted）的模式。
	// 取值为 Single、All 之一。
	// 该字段不可变。
	//
	// +optional
	DisruptionMode *DisruptionMode

	// PriorityClassName 指示调度由该模板创建的 Pod 组时应考虑的优先级。
	// 该字段不可变。
	//
	// +optional
	PriorityClassName string

	// Priority 是由该模板创建的 Pod 组的优先级数值。
	// 多个系统组件使用该字段查找 Pod 组的优先级。
	// 数值越大，优先级越高。
	// 该字段不可变。
	//
	// +optional
	Priority *int32

	// PreemptionPolicy 定义抢占低优先级 Pod/PodGroup 的策略。
	// 取值为 Never 或 PreemptLowerPriority。
	// 该字段不可变。
	// 仅当 PodGroupPreemptionPolicy 特性开关开启时该字段才可用。
	//
	// +featureGate=PodGroupPreemptionPolicy
	// +optional
	PreemptionPolicy *PreemptionPolicy
}

// CompositePodGroupTemplate 表示带调度策略的 CompositePodGroup 模板。
type CompositePodGroupTemplate struct {
	// name 是 CompositePodGroupTemplate 在 Workload 内的唯一标识，必须是 DNS 标签，为必填项。
	// +required
	Name string

	// SchedulingPolicy 定义此模板的调度策略。
	//
	// +required
	SchedulingPolicy CompositePodGroupSchedulingPolicy

	// SchedulingConstraints 为此 CompositePodGroupTemplate 定义可选的调度约束（如拓扑）。
	// 该字段不可变。
	//
	// +optional
	SchedulingConstraints *CompositePodGroupSchedulingConstraints

	// DisruptionMode 定义给定 CompositePodGroup 可被打断的模式。
	// 取值为 Single、All 之一。
	// 该字段不可变。
	//
	// +optional
	DisruptionMode *CompositeDisruptionMode

	// PriorityClassName 指示调度由该模板创建的复合 Pod 组时应考虑的优先级。
	// 若未指定优先级类，当存在全局默认优先级类时，准入控制可将此字段设置为其。
	// 否则，由该模板创建的复合 Pod 组的优先级将被设为零。
	// 该字段不可变。
	//
	// +optional
	PriorityClassName string

	// Priority 是由此模板创建的复合 Pod 组的优先级数值。
	// 多个系统组件使用该字段查找复合 Pod 组的优先级。
	// 启用 Priority 准入控制器时，会阻止用户设置此字段。
	// 该准入控制器会根据 PriorityClassName 填充此字段。
	// 数值越大，优先级越高。
	// 该字段不可变。
	//
	// +optional
	Priority *int32

	// PreemptionPolicy 定义抢占低优先级 Pod/PodGroup 的策略。
	// 取值为 Never 或 PreemptLowerPriority。
	// 该字段不可变。
	// 仅当 PodGroupPreemptionPolicy 特性开关开启时该字段才可用。
	//
	// +featureGate=PodGroupPreemptionPolicy
	// +optional
	PreemptionPolicy *PreemptionPolicy

	// PodGroupTemplates 是子级 PodGroup 的模板列表。
	// 模板数量最多为 8 个。CompositePodGroupTemplates
	// 或 PodGroupTemplates 中必须至少设置一个条目。
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	PodGroupTemplates []PodGroupTemplate

	// CompositePodGroupTemplates 是子级 CompositePodGroup 的模板列表。
	// 模板数量最多为 8 个。CompositePodGroupTemplates
	// 或 PodGroupTemplates 中必须至少设置一个条目。
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	CompositePodGroupTemplates []CompositePodGroupTemplate
}

// PodGroupSchedulingPolicy 定义 PodGroup 的调度配置。
// 必须且只能设置一种策略。策略在创建时通过设置 Basic 或 Gang 字段来选定。
// PodGroup 创建之后不能更换策略。
// 所选策略内部的字段在其各字段允许的情况下可在创建后更新。
//
// +union
type PodGroupSchedulingPolicy struct {
	// basic 指定组内 Pod 使用标准 Kubernetes 调度行为。创建组时设置此字段即选用基础调度，
	// 之后不可更改。
	// +optional
	Basic *BasicSchedulingPolicy

	// gang 指定组内 Pod 使用全有或全无（all-or-nothing）语义调度。创建组时设置此字段即选用 gang 调度，
	// 之后不可设置或取消。Gang 策略中的 minCount 字段在组创建后仍可修改。
	// +optional
	Gang *GangSchedulingPolicy
}

// BasicSchedulingPolicy 表示应当使用标准 Kubernetes 调度行为。
type BasicSchedulingPolicy struct {
	// 此处有意留空。该类型存在即表示应应用基础调度策略。未来可能出现新字段，
	// 用于在 Pod 组层面描述非“全有或全无”（gang）调度的此类约束。
}

// GangSchedulingPolicy 定义 gang 调度的参数。
type GangSchedulingPolicy struct {
	// MinCount 是让调度器接纳整个组时，必须同时处于可调度或已调度状态的最少 Pod 数。
	// 其值必须是正整数。该字段可变，以支持工作负载扩缩容。
	//
	// 注意，调度器基于最终一致性模型运行。由于传播延迟，对 minCount 的更新
	// 可能不会立即体现在调度决策中。如果在该组的某个调度周期进行期间更新了
	// minCount，新值可能要等到下一个周期才会生效。此外，minCount 仅在调度期间
	// 被强制执行，即对该字段的修改不影响已调度的 Pod，只适用于未来周期中
	// 被评估的 Pod。
	//
	// +required
	MinCount int32
}

// PodGroupResourceClaim 引用恰好一个 ResourceClaim，或直接引用其名称，
// 或命名一个 ResourceClaimTemplate，后者会随之为 PodGroup 转换成
// 一个 ResourceClaim。
//
// 它为后者附加一个名称，用于在 PodGroup 内唯一标识该 ResourceClaim。
// 需要访问该 ResourceClaim 的 Pod 会在自己的 Spec.ResourceClaims 中
// 定义一个匹配的引用。Pod 的声明必须与 PodGroup 声明的所有字段完全匹配。
type PodGroupResourceClaim struct {
	// name 在 PodGroup 内唯一标识此资源声明，必须是 DNS_LABEL。
	// +required
	Name string

	// ResourceClaimName 是与本 PodGroup 处于同一命名空间内的某个
	// ResourceClaim 对象的名称。该 ResourceClaim 将为整个 PodGroup 保留，
	// 而不是为其中的单个 Pod 保留。
	//
	// ResourceClaimName 与 ResourceClaimTemplateName 必须且只能设置其一。
	//
	// +optional
	ResourceClaimName *string

	// ResourceClaimTemplateName 是与本 PodGroup 处于同一命名空间内的某个
	// ResourceClaimTemplate 对象的名称。
	//
	// 该模板将被用于创建一个新的 ResourceClaim，并绑定到此 PodGroup。
	// 当此 PodGroup 被删除时，该 ResourceClaim 也会被删除。PodGroup 的名称
	// 与资源名称，加上一个生成的组件，将共同构成该 ResourceClaim 的唯一名称，
	// 并记录在 podgroup.status.resourceClaimStatuses 中。
	//
	// 该字段不可变，且在创建对应的 ResourceClaim 后，控制平面不会
	// 再对其做任何修改。
	//
	// ResourceClaimName 与 ResourceClaimTemplateName 必须且只能设置其一。
	//
	// +optional
	ResourceClaimTemplateName *string
}

// DisruptionMode 定义组内各实体可被中断的方式。
// 只能设置一种模式。
//
// +union
type DisruptionMode struct {
	// single 指定各子项可以彼此独立地被中断。
	// +optional
	Single *SingleDisruptionMode

	// all 指定所有子项只能一起被中断。
	// +optional
	All *AllDisruptionMode
}

// SingleDisruptionMode 指定各子对象可被独立中断。
type SingleDisruptionMode struct {
	// 此处有意留空。
}

// AllDisruptionMode 指定各子对象只能作为一个整体被中断。
type AllDisruptionMode struct {
	// 此处有意留空。
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodGroup 表示被归为一组的 Pod 的运行时实例。
// PodGroup 由工作负载控制器（Job、LWS、JobSet 等……）根据
// Workload.podGroupTemplates 创建。
// PodGroup API 的启用由 GenericWorkload 特性门控控制。
type PodGroup struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	//
	// +optional
	metav1.ObjectMeta

	// spec 定义 PodGroup 的期望状态。
	// +required
	Spec PodGroupSpec

	// status 表示 PodGroup 当前观测到的状态。
	// +optional
	Status PodGroupStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodGroupList 包含一组 PodGroup 资源。
type PodGroupList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	//
	// +optional
	metav1.ListMeta

	// Items 是 PodGroup 列表。
	Items []PodGroup
}

// PodGroupSpec 定义 PodGroup 的期望状态。
type PodGroupSpec struct {
	// ParentCompositePodGroupName 包含与此 Pod 组同命名空间内的父复合 Pod 组名称。
	// 若为 nil，则此 Pod 组是某个工作负载层级的根。
	// 仅当 CompositePodGroup 特性门控启用时使用此字段。
	// 该字段不可变。
	//
	// +featureGate=CompositePodGroup
	// +optional
	ParentCompositePodGroupName *string

	// WorkloadRef 引用创建该 PodGroup 时所使用的 Workload 对象中的
	// 可选 PodGroup 模板。
	// 该字段不可变。
	//
	// +optional
	WorkloadRef *WorkloadReference

	// SchedulingPolicy 为此 PodGroup 实例定义调度策略。
	// 控制器应从 PodGroupTemplate 复制该字段来填充。
	//
	// +required
	SchedulingPolicy PodGroupSchedulingPolicy

	// SchedulingConstraints 为此 PodGroup 定义可选的调度约束（如拓扑）。
	// 控制器应从 PodGroupTemplate 复制该字段来填充。
	// 该字段不可变。
	// 仅当启用 TopologyAwareWorkloadScheduling 特性门控时该字段可用。
	//
	// +optional
	// +featureGate=TopologyAwareWorkloadScheduling
	SchedulingConstraints *PodGroupSchedulingConstraints

	// ResourceClaims 定义组内 Pod 之间可以共享哪些 ResourceClaims。
	// Pod 必须引用这些声明才能消费已分配的设备。
	//
	// 此为 beta 级字段，需要启用 DRAWorkloadResourceClaims 特性门控。
	//
	// 该字段不可变。
	//
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge,retainKeys
	// +listType=map
	// +listMapKey=name
	// +featureGate=DRAWorkloadResourceClaims
	ResourceClaims []PodGroupResourceClaim

	// DisruptionMode 定义给定 PodGroup 可被打断的模式。
	// 控制器应从 PodGroupTemplate 复制该字段来填充。
	// 取值为 Single、All 之一。若未设置，默认为 Single。
	// 该字段不可变。
	//
	// +optional
	DisruptionMode *DisruptionMode

	// PriorityClassName 定义调度此 Pod 组时应考虑的优先级。
	// 控制器应从 PodGroupTemplate 复制该字段来填充。
	// 否则，其校验与解析方式与 PodGroupTemplate 上的 PriorityClassName 类似
	// （即若未指定优先级类，当存在全局默认优先级类时，准入控制可将此字段设置为其。
	// 否则，Pod 组的优先级将为零）。
	// 该字段不可变。
	//
	// +optional
	PriorityClassName string

	// Priority 是此 Pod 组的优先级数值。多个系统组件
	// 使用该字段查找 Pod 组的优先级。启用 Priority 准入控制器时，
	// 会阻止用户设置此字段。该准入控制器会根据 PriorityClassName 填充此字段。
	// 数值越大，优先级越高。
	// 该字段不可变。
	//
	// +optional
	Priority *int32

	// PreemptionPolicy 定义抢占低优先级 Pod/PodGroup 的策略。
	// 取值为 Never、PreemptLowerPriority 之一。若未设置，默认为 PreemptLowerPriority。
	// 启用 Priority 准入控制器时，该控制器会根据 PriorityClassName 填充此字段，
	// 若 PriorityClass 中该值未设置，则默认为 PreemptLowerPriority。
	// 该字段不可变。
	// 仅当 PodGroupPreemptionPolicy 特性开关开启时该字段才可用。
	//
	// +featureGate=PodGroupPreemptionPolicy
	// +optional

	PreemptionPolicy *PreemptionPolicy
}

// PodGroupStatus 表示 PodGroup 的状态信息。
type PodGroupStatus struct {
	// Conditions 表示对 PodGroup 状态的最新观测结果。
	//
	// 已知条件类型：
	// - "PodGroupInitiallyScheduled"：表示调度需求是否已满足。
	//   该条件一旦转为 True，即为终态，之后绝不会回退为 False，
	//   即使随后 Pod 被驱逐、组约束不再满足也是如此。
	// - "DisruptionTarget"：表示 PodGroup 是否即将因抢占等中断而被终止。
	//
	// PodGroupInitiallyScheduled 条件的已知原因：
	// - "Unschedulable"：由于资源约束、亲和/反亲和规则或 gang 所需
	//   容量不足，PodGroup 无法被调度。
	// - "SchedulerError"：由于调度过程中发生的某种内部错误，
	//   PodGroup 无法被调度，例如 nodeAffinity 解析错误。
	//
	// DisruptionTarget 条件的已知原因：
	// - "PreemptionByScheduler"：调度器为给更高优先级的 PodGroup 或 Pod
	//   腾出空间而抢占了该 PodGroup。
	//
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition

	// 资源声明的状态。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge,retainKeys
	// +listType=map
	// +listMapKey=name
	// +featureGate=DRAWorkloadResourceClaims
	ResourceClaimStatuses []PodGroupResourceClaimStatus
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
	// DisruptionTarget 条件中的 PreemptionByScheduler 原因表示该 PodGroup 被抢占，
	// 以便为更高优先级的 PodGroup 或 Pod 腾出空间。
	PodGroupReasonPreemptionByScheduler string = "PreemptionByScheduler"
)

// PodGroupResourceClaimStatus 为每个引用了 ResourceClaimTemplate 的
// PodGroupResourceClaim 存储于 PodGroupStatus 中，记录对应 ResourceClaim
// 生成的名称。
type PodGroupResourceClaimStatus struct {
	// name 在 PodGroup 内唯一标识此资源声明，必须与 podgroup.spec.resourceClaims 中
	// 某个条目的名称一致，因此该字符串必须是 DNS_LABEL。
	// +required
	Name string

	// ResourceClaimName 是为 PodGroup 在其所属命名空间中生成的
	// ResourceClaim 的名称。若未设置，则说明无需生成 ResourceClaim，
	// 此时可忽略对应的 podgroup.spec.resourceClaims 条目。
	//
	// +optional
	ResourceClaimName *string
}

// WorkloadReference 引用用于创建特定 PodGroup 的 Workload 对象
// 及所用模板。
type WorkloadReference struct {
	// WorkloadName 是包含创建 Pod 组时所用模板的 Workload 对象的名称。
	// 必须是 DNS 名称。
	// 该字段必填。
	//
	// +required
	WorkloadName string

	// TemplateName 是创建 Pod 组时所使用的 Workload 对象内模板的名称。
	// 必须是 DNS 标签。
	// 该字段必填。
	//
	// +required
	TemplateName string
}

// PodGroupSchedulingConstraints 定义 PodGroup 的调度约束（如拓扑）。
type PodGroupSchedulingConstraints struct {
	// topology 定义 Pod 组的拓扑约束。目前只能指定单个拓扑约束，将来可能改变。
	// +optional
	// +listType=atomic
	Topology []TopologyConstraint
}

// TopologyConstraint 定义 PodGroup 的拓扑约束。
type TopologyConstraint struct {
	// Key 指定表示拓扑域的节点标签的键。
	// PodGroup 内的所有 Pod 必须同置于同一个域实例内。
	// 不同的 PodGroup 可以落在不同的域实例上，即使它们源自同一个 PodGroupTemplate。
	// Examples: "topology.kubernetes.io/rack"
	//
	// +required
	Key string
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CompositePodGroup 表示被归为一组的 Pod 组的运行时实例。
// CompositePodGroup 由工作负载控制器（LWS、JobSet 等……）根据
// Workload.compositePodGroupTemplates 创建。
// CompositePodGroup API 的启用由 CompositePodGroup 特性门控控制。
type CompositePodGroup struct {
	metav1.TypeMeta

	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	//
	// +optional
	metav1.ObjectMeta

	// Spec 定义 CompositePodGroup 的期望状态。
	//
	// +required
	Spec CompositePodGroupSpec

	// Status 表示 CompositePodGroup 当前观测到的状态。
	//
	// +optional
	Status CompositePodGroupStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CompositePodGroupList 包含一组 CompositePodGroup 资源。
type CompositePodGroupList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	//
	// +optional
	metav1.ListMeta

	// Items 是 CompositePodGroup 列表。
	Items []CompositePodGroup
}

// CompositePodGroupSpec 定义 CompositePodGroup 的期望状态。
type CompositePodGroupSpec struct {
	// ParentCompositePodGroupName 包含与此复合 Pod 组同命名空间内的父复合 Pod 组名称。
	// 必须是 DNS 名称。
	// 若为 nil，则此复合 Pod 组是某个工作负载层级的根。
	// 该字段不可变。
	//
	// +optional
	ParentCompositePodGroupName *string

	// WorkloadRef 引用创建该 CompositePodGroup 时所使用的、
	// Workload 对象内的可选 CompositePodGroup 模板。
	// 该字段必填。
	// 该字段不可变。
	//
	// +required
	WorkloadRef *WorkloadReference

	// SchedulingPolicy 为此 CompositePodGroup 实例定义调度策略。
	// 控制器应从 CompositePodGroupTemplate 复制该字段来填充。
	// 该字段不可变。
	//
	// +required
	SchedulingPolicy CompositePodGroupSchedulingPolicy

	// SchedulingConstraints 为此 CompositePodGroup 定义可选的调度约束（如拓扑）。
	// 控制器应从 CompositePodGroupTemplate 复制该字段来填充。
	// 该字段不可变。
	//
	// +optional
	SchedulingConstraints *CompositePodGroupSchedulingConstraints

	// DisruptionMode 定义给定 CompositePodGroup 可被打断的模式。
	// 控制器应从 CompositePodGroupTemplate 复制该字段来填充。
	// 取值为 Single、All 之一。若未设置，默认为 Single。
	// 该字段不可变。
	//
	// +optional
	DisruptionMode *CompositeDisruptionMode

	// PriorityClassName 定义调度此 CompositePodGroup 时应考虑的优先级。
	// 控制器应从 CompositePodGroupTemplate 复制该字段来填充。
	// 若未指定，其校验与解析方式与 Pod 的 PriorityClassName 字段类似
	// （即若未指定优先级类，当存在全局默认优先级类时，准入控制可将此字段设置为其。
	// 否则，复合 Pod 组的优先级将为零）。
	// 该字段不可变。
	//
	// +optional
	PriorityClassName string

	// Priority 是此复合 Pod 组的优先级数值。多个系统组件
	// 使用该字段查找复合 Pod 组的优先级。启用 Priority 准入控制器时，
	// 会阻止用户设置此字段。该准入控制器会根据 PriorityClassName 填充此字段。
	// 数值越大，优先级越高。
	// 该字段不可变。
	//
	// +optional
	Priority *int32

	// PreemptionPolicy 定义抢占低优先级 Pod/PodGroup 的策略。
	// 取值为 Never、PreemptLowerPriority 之一。若未设置，默认为 PreemptLowerPriority。
	// 启用 Priority 准入控制器时，该控制器会根据 PriorityClassName 填充此字段，
	// 若 PriorityClass 中该值未设置，则默认为 PreemptLowerPriority。
	// 该字段不可变。
	// 仅当 PodGroupPreemptionPolicy 特性开关开启时该字段才可用。
	//
	// +featureGate=PodGroupPreemptionPolicy
	// +optional
	PreemptionPolicy *PreemptionPolicy
}

// CompositePodGroupSchedulingPolicy 定义 CompositePodGroup 的调度配置。
// 必须且只能设置一种策略。
//
// +union
type CompositePodGroupSchedulingPolicy struct {
	// Basic 表示该复合组内的组应被独立调度。
	// 该字段不可变。
	//
	// +optional
	Basic *CompositeBasicSchedulingPolicy

	// Gang 指定该复合组内的组应使用全有或全无（all-or-nothing）语义调度。
	//
	// +optional
	Gang *CompositeGangSchedulingPolicy
}

// CompositeBasicSchedulingPolicy 表示属于复合组的各分组应当被独立调度。
type CompositeBasicSchedulingPolicy struct {
	// 此处有意留空。该类型存在即表示应应用基础分组调度策略。
	// 未来可能出现新字段，用于在复合 Pod 组层面描述非“全有或
	// 全无”（gang）调度的此类约束。
}

// CompositeGangSchedulingPolicy 表示属于复合组的各分组应当使用
// 全有或全无（all-or-nothing）语义调度。
type CompositeGangSchedulingPolicy struct {
	// MinGroupCount 是让调度器接纳整个组时，必须同时处于可调度或已调度状态的
	// 最小子分组数。其值必须是正整数。
	//
	// +required
	MinGroupCount int32
}

// CompositeDisruptionMode 定义复合 PodGroup 中各实体可被中断的方式。
// 必须且只能设置一种模式。
//
// +union
type CompositeDisruptionMode struct {
	// single 指定各子组可以彼此独立地被中断。
	// +optional
	Single *SingleCompositeDisruptionMode

	// all 指定所有子组只能一起被中断。
	// +optional
	All *AllCompositeDisruptionMode
}

// SingleCompositeDisruptionMode 表示 CompositePodGroup 的各子组
// 可以被独立中断或抢占。
type SingleCompositeDisruptionMode struct {
	// 此处有意留空。
}

// AllCompositeDisruptionMode 表示 CompositePodGroup 的各子组只能
// 一起被中断或抢占。
type AllCompositeDisruptionMode struct {
	// 此处有意留空。
}

// CompositePodGroupStatus 表示复合 PodGroup 的状态信息。
type CompositePodGroupStatus struct {
	// Conditions 表示对 CompositePodGroup 状态的最新观测结果。
	//
	// 已知条件类型：
	// - "CompositePodGroupInitiallyScheduled"：表示该 CompositePodGroup 下
	//   整个子树的整体调度需求是否已满足。该条件一旦转为 True，即为终态，
	//   之后绝不会回退为 False，即使随后 Pod 被删除、组约束不再满足也是如此。
	// - "DisruptionTarget"：表示 CompositePodGroup 是否即将因抢占等中断
	//   而被终止。
	//
	// CompositePodGroupInitiallyScheduled 条件的已知原因：
	// - "Unschedulable"：由于资源约束、亲和/反亲和或拓扑约束，
	//   CompositePodGroup 的子树无法被放置。
	// - "SchedulerError"：由于调度过程中发生的某种内部错误，
	//   CompositePodGroup 无法被调度。
	// - "Invalid"：当 kube-scheduler 在运行时校验期间检测到无效的组布局时，
	//   该原因被置为 True。`message` 字段详述具体的布局违规情形（例如检测到
	//   循环引用、超出最大深度 4、或引用了多个不同的 Workload）。
	//
	// DisruptionTarget 条件的已知原因：
	// - "PreemptionByScheduler"：调度器的抢占循环选中该 CompositePodGroup，
	//   以为更高优先级的抢占者释放容量。
	//
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition
}

// CompositePodGroupSchedulingConstraints 定义 CompositePodGroup 的调度约束（如拓扑）。
type CompositePodGroupSchedulingConstraints struct {
	// topology 定义复合 Pod 组的拓扑约束。目前只能指定单个拓扑约束，将来可能改变。
	// +optional
	// +listType=atomic
	Topology []TopologyConstraint
}
