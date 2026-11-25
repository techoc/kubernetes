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
	// DefaultPriorityWhenNoDefaultClassExists is used to set priority of pods
	// that do not specify any priority class and there is no priority class
	// marked as default.
	DefaultPriorityWhenNoDefaultClassExists = 0
	// HighestUserDefinablePriority is the highest priority for user defined priority classes. Priority values larger than 1 billion are reserved for Kubernetes system use.
	HighestUserDefinablePriority = int32(1000000000)
	// SystemCriticalPriority is the beginning of the range of priority values for critical system components.
	SystemCriticalPriority = 2 * HighestUserDefinablePriority
	// SystemPriorityClassPrefix is the prefix reserved for system priority class names. Other priority
	// classes are not allowed to start with this prefix.
	// NOTE: In order to avoid conflict of names with user-defined priority classes, all the names must
	// start with SystemPriorityClassPrefix.
	SystemPriorityClassPrefix = "system-"
	// SystemClusterCritical is the system priority class name that represents cluster-critical.
	SystemClusterCritical = SystemPriorityClassPrefix + "cluster-critical"
	// SystemNodeCritical is the system priority class name that represents node-critical.
	SystemNodeCritical = SystemPriorityClassPrefix + "node-critical"

	// PodGroupProtectionFinalizer is the finalizer added to PodGroups to prevent
	// premature deletion while pods still reference them.
	PodGroupProtectionFinalizer = GroupName + "/podgroup-protection"
)

// PreemptionPolicy describes a policy for if/when to preempt a pod/podgroup.
type PreemptionPolicy string

const (
	// PreemptLowerPriority 表示 Pod 可以抢占优先级更低的其他 Pod。
	PreemptLowerPriority PreemptionPolicy = "PreemptLowerPriority"
	// PreemptNever 表示 Pod 从不抢占其他 Pod。
	PreemptNever PreemptionPolicy = "Never"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PriorityClass defines the mapping from a priority class name to the priority
// integer value. The value can be any valid integer.
type PriorityClass struct {
	metav1.TypeMeta
	// Standard object metadata; More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata.
	// +optional
	metav1.ObjectMeta

	// value 表示此优先级类的整数值。当 Pod 的 spec 中指定该类名时，
	// 这就是 Pod 实际获得的优先级。
	Value int32

	// globalDefault specifies whether this PriorityClass should be considered as
	// the default priority for pods that do not have any priority class.
	// Only one PriorityClass can be marked as `globalDefault`. However, if more than
	// one PriorityClasses exists with their `globalDefault` field set to true,
	// the smallest value of such global default PriorityClasses will be used as the default priority.
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

	// items is the list of PriorityClasses.
	Items []PriorityClass
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Workload allows for expressing scheduling constraints that should be used
// when managing the lifecycle of workloads from the scheduling perspective,
// including scheduling, preemption, eviction and other phases.
// Workload API enablement is toggled by the GenericWorkload feature gate.
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
	// WorkloadMaxTemplateDepth is the maximum allowed depth of top-level composite pod group
	// templates defined in a Workload object.
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

	// PodGroupTemplates is the list of templates that make up the Workload.
	// The maximum number of templates is 8. Templates cannot be added or removed after the workload is created.
	// Existing templates may still be updated where their individual fields allow it.
	// Exactly one of CompositePodGroupTemplates and PodGroupTemplates must be set.
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
	// APIGroup is the group for the resource being referenced.
	// If APIGroup is empty, the specified Kind must be in the core API group.
	// For any other third-party types, setting APIGroup is required.
	// It must be a DNS subdomain.
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

	// SchedulingPolicy defines the scheduling policy for this PodGroupTemplate.
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

	// SchedulingPolicy defines the scheduling policy for this template.
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

	// PodGroupTemplates is the list of templates for children PodGroups.
	// The maximum number of templates is 8. At least one entry in CompositePodGroupTemplates
	// or PodGroupTemplates must be set.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	PodGroupTemplates []PodGroupTemplate

	// CompositePodGroupTemplates is the list of templates for children CompositePodGroups.
	// The maximum number of templates is 8. At least one entry in CompositePodGroupTemplates
	// or PodGroupTemplates must be set.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	CompositePodGroupTemplates []CompositePodGroupTemplate
}

// PodGroupSchedulingPolicy defines the scheduling configuration for a PodGroup.
// Exactly one policy must be set. The policy is chosen at creation time by setting either the
// Basic or Gang field. The PodGroup may not change policy after creation.
// Fields within chosen policy may be updated after creation when their individual fields allow it.
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

// BasicSchedulingPolicy indicates that standard Kubernetes
// scheduling behavior should be used.
type BasicSchedulingPolicy struct {
	// This is intentionally empty. Its presence indicates that the basic
	// scheduling policy should be applied. In the future, new fields may appear,
	// describing such constraints on a pod group level without "all or nothing"
	// (gang) scheduling.
}

// GangSchedulingPolicy 定义 gang 调度的参数。
type GangSchedulingPolicy struct {
	// MinCount is the minimum number of pods that must be schedulable or scheduled
	// at the same time for the scheduler to admit the entire group.
	// It must be a positive integer. This field is mutable to support workload scaling.
	//
	// Note that the scheduler operates on an eventually consistent model. Updates
	// to minCount may not be immediately reflected in scheduling decisions due to
	// propagation delays. If minCount is updated while a scheduling cycle is in
	// progress for that group, the new value may not take effect until the next
	// cycle. Moreover, minCount is only enforced during scheduling, meaning that
	// modifications to this field do not affect already-scheduled pods, applying
	// only to those evaluated in future cycles.
	//
	// +required
	MinCount int32
}

// PodGroupResourceClaim references exactly one ResourceClaim, either directly
// or by naming a ResourceClaimTemplate which is then turned into a ResourceClaim
// for the PodGroup.
//
// It adds a name to it that uniquely identifies the ResourceClaim inside the PodGroup.
// Pods that need access to the ResourceClaim define a matching reference in its
// own Spec.ResourceClaims. The Pod's claim must match all fields of the
// PodGroup's claim exactly.
type PodGroupResourceClaim struct {
	// name 在 PodGroup 内唯一标识此资源声明，必须是 DNS_LABEL。
	// +required
	Name string

	// ResourceClaimName is the name of a ResourceClaim object in the same
	// namespace as this PodGroup. The ResourceClaim will be reserved for the
	// PodGroup instead of its individual pods.
	//
	// Exactly one of ResourceClaimName and ResourceClaimTemplateName must
	// be set.
	//
	// +optional
	ResourceClaimName *string

	// ResourceClaimTemplateName is the name of a ResourceClaimTemplate
	// object in the same namespace as this PodGroup.
	//
	// The template will be used to create a new ResourceClaim, which will
	// be bound to this PodGroup. When this PodGroup is deleted, the ResourceClaim
	// will also be deleted. The PodGroup name and resource name, along with a
	// generated component, will be used to form a unique name for the
	// ResourceClaim, which will be recorded in podgroup.status.resourceClaimStatuses.
	//
	// This field is immutable and no changes will be made to the
	// corresponding ResourceClaim by the control plane after creating the
	// ResourceClaim.
	//
	// Exactly one of ResourceClaimName and ResourceClaimTemplateName must
	// be set.
	//
	// +optional
	ResourceClaimTemplateName *string
}

// DisruptionMode defines how individual entities within a group can be disrupted.
// Exactly one mode can be set.
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
	// Intentionally empty now.
}

// AllDisruptionMode 指定各子对象只能作为一个整体被中断。
type AllDisruptionMode struct {
	// Intentionally empty now.
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodGroup represents a runtime instance of pods grouped together.
// PodGroups are created by workload controllers (Job, LWS, JobSet, etc...) from
// Workload.podGroupTemplates.
// PodGroup API enablement is toggled by the GenericWorkload feature gate.
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

	// Items is the list of PodGroups.
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
	// Conditions represent the latest observations of the PodGroup's state.
	//
	// Known condition types:
	// - "PodGroupInitiallyScheduled": Indicates whether the scheduling requirement has been satisfied.
	//   Once this condition transitions to True, it serves as a terminal state and will never revert to False,
	//   even if pods are subsequently evicted and group constraints are no longer met.
	// - "DisruptionTarget": Indicates whether the PodGroup is about to be terminated
	//   due to disruption such as preemption.
	//
	// Known reasons for the PodGroupInitiallyScheduled condition:
	// - "Unschedulable": The PodGroup cannot be scheduled due to resource constraints,
	//   affinity/anti-affinity rules, or insufficient capacity for the gang.
	// - "SchedulerError": The PodGroup cannot be scheduled due to some internal error
	//   that happened during scheduling, for example due to nodeAffinity parsing errors.
	//
	// Known reasons for the DisruptionTarget condition:
	// - "PreemptionByScheduler": The PodGroup was preempted by the scheduler to make room for
	//   higher-priority PodGroups or Pods.
	//
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition

	// Status of resource claims.
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
	// PreemptionByScheduler reason in the DisruptionTarget condition indicates the PodGroup was preempted
	// to make room for higher-priority PodGroups or Pods.
	PodGroupReasonPreemptionByScheduler string = "PreemptionByScheduler"
)

// PodGroupResourceClaimStatus is stored in the PodGroupStatus for each
// PodGroupResourceClaim which references a ResourceClaimTemplate. It stores the
// generated name for the corresponding ResourceClaim.
type PodGroupResourceClaimStatus struct {
	// name 在 PodGroup 内唯一标识此资源声明，必须与 podgroup.spec.resourceClaims 中
	// 某个条目的名称一致，因此该字符串必须是 DNS_LABEL。
	// +required
	Name string

	// ResourceClaimName is the name of the ResourceClaim that was generated for
	// the PodGroup in the namespace of the PodGroup. If this is unset, then
	// generating a ResourceClaim was not necessary. The
	// podgroup.spec.resourceClaims entry can be ignored in this case.
	//
	// +optional
	ResourceClaimName *string
}

// WorkloadReference references the Workload object together with the template
// that was used to create a particular PodGroup.
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
	// Key specifies the key of the node label representing the topology domain.
	// All pods within the PodGroup must be colocated within the same domain instance.
	// Different PodGroups can land on different domain instances even if they derive from the same PodGroupTemplate.
	// Examples: "topology.kubernetes.io/rack"
	//
	// +required
	Key string
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CompositePodGroup represents a runtime instance of pod groups grouped together.
// CompositePodGroups are created by workload controllers (LWS, JobSet, etc...) from
// Workload.compositePodGroupTemplates.
// CompositePodGroup API enablement is toggled by the CompositePodGroup feature gate.
type CompositePodGroup struct {
	metav1.TypeMeta

	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	//
	// +optional
	metav1.ObjectMeta

	// Spec defines the desired state of the CompositePodGroup.
	//
	// +required
	Spec CompositePodGroupSpec

	// Status represents the current observed state of the CompositePodGroup.
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

// CompositePodGroupSchedulingPolicy defines the scheduling configuration for a CompositePodGroup.
// Exactly one policy must be set.
//
// +union
type CompositePodGroupSchedulingPolicy struct {
	// Basic 表示该复合组内的组应被独立调度。
	// 该字段不可变。
	//
	// +optional
	Basic *CompositeBasicSchedulingPolicy

	// Gang specifies that the groups of this composite group should be scheduled using
	// all-or-nothing semantics.
	//
	// +optional
	Gang *CompositeGangSchedulingPolicy
}

// CompositeBasicSchedulingPolicy indicates that the groups belonging to the composite group
// should be scheduled independently.
type CompositeBasicSchedulingPolicy struct {
	// This is intentionally empty. Its presence indicates that the basic group
	// scheduling policy should be applied. In the future, new fields may appear,
	// describing such constraints on a composite pod group level without "all or
	// nothing" (gang) scheduling.
}

// CompositeGangSchedulingPolicy indicates that the groups belonging to the composite group
// should be scheduled using all-or-nothing semantics.
type CompositeGangSchedulingPolicy struct {
	// MinGroupCount is the minimum number of child groups that must be schedulable
	// or scheduled at the same time for the scheduler to admit the entire group.
	// It must be a positive integer.
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

// SingleCompositeDisruptionMode means that individual children of a CompositePodGroup
// can be disrupted or preempted independently.
type SingleCompositeDisruptionMode struct {
	// This is intentionally empty.
}

// AllCompositeDisruptionMode means that children of a CompositePodGroup can only be
// disrupted or preempted together.
type AllCompositeDisruptionMode struct {
	// This is intentionally empty.
}

// CompositePodGroupStatus 表示复合 PodGroup 的状态信息。
type CompositePodGroupStatus struct {
	// Conditions represent the latest observations of the CompositePodGroup's state.
	//
	// Known condition types:
	// - "CompositePodGroupInitiallyScheduled": Indicates whether the overall scheduling requirement
	//   for the subtree under this CompositePodGroup has been satisfied. Once this condition
	//   transitions to True, it serves as a terminal state and will never revert to False,
	//   even if pods are subsequently deleted and group constraints are no longer met.
	// - "DisruptionTarget": Indicates whether the CompositePodGroup is about to be terminated
	//   due to disruption such as preemption.
	//
	// Known reasons for the CompositePodGroupInitiallyScheduled condition:
	// - "Unschedulable": The CompositePodGroup's subtree could not be placed due to resource constraints,
	//   affinity/anti-affinity, or topological constraints.
	// - "SchedulerError": The CompositePodGroup cannot be scheduled due to some internal error
	//   that occurred during scheduling.
	// - "Invalid": Set to True when kube-scheduler detects an invalid group layout during
	//   runtime validation. The `message` field details the specific layout violation (such as
	//   a detected cycle, exceeding the maximum depth of 4, or referencing multiple distinct Workloads).
	//
	// Known reasons for the DisruptionTarget condition:
	// - "PreemptionByScheduler": The CompositePodGroup was targeted by the scheduler's preemption loop
	//   to free up capacity for higher-priority preemptors.
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
