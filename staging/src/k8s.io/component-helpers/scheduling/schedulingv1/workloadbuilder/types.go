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

package workloadbuilder

import (
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// SchedulingConfig 是与层级无关的调度配置中间表示（IR），
// 把调度语义从 leaf/composite 的 API 拆分中解耦出来。
//
// leaf 与 composite 在 API 里是两套结构，但编译成 Workload 时关心的调度语义
// （策略、约束、中断模式等）完全一致。引入这层与层级无关的 IR，是为了让默认合并、
// callback 处理和模板编译只需实现一遍；nil 字段一律表示“继承默认值”或“不设置”，
// 而不是空配置。
//
// +k8s:deepcopy-gen=true
type SchedulingConfig struct {
	// Policy 选择调度模式；nil 时解析为 Basic。
	Policy *SchedulingPolicy

	// Constraints 保存拓扑等放置约束；nil 表示该组没有放置约束。
	Constraints *SchedulingConstraints

	// DisruptionMode 选择组内 Pod 的中断方式；nil 表示不设置，由调度器使用默认值。
	DisruptionMode *DisruptionMode

	// ResourceClaims 列出组内共享的动态资源声明。
	ResourceClaims []ResourceClaim

	// PriorityClassName 会被拷贝到编译出的 PodGroupTemplate，
	// 使从该模板物化的 PodGroup 继承本组的优先级。
	PriorityClassName string
}

// SchedulingPolicy 选择调度模式，必须且只能设置其中一个字段。
//
// 用“多个可选字段 + 恰好一个非空”而不是枚举，是为了让每种策略能携带自己的参数
// （比如 gang 的 MinCount），同时让校验器可以用 union 规则统一检查互斥性。
//
// +k8s:deepcopy-gen=true
type SchedulingPolicy struct {
	// Basic 表示使用标准的 Kubernetes 逐个 Pod 调度。
	Basic *BasicSchedulingPolicy

	// Gang 表示使用 all-or-nothing（全有或全无）的成组调度。
	Gang *GangSchedulingPolicy
}

// BasicSchedulingPolicy 表示标准的 Kubernetes 逐个 Pod 调度。
//
// +k8s:deepcopy-gen=true
type BasicSchedulingPolicy struct{}

// GangSchedulingPolicy 定义 all-or-nothing 成组调度的参数。
//
// +k8s:deepcopy-gen=true
type GangSchedulingPolicy struct {
	// MinCount 可以为 nil，留给回调按控制器语义补默认值
	// （例如取 Job 的 parallelism）；但在 Build 编译模板前它必须为正数。
	// 用指针而非 int32 是为了区分“用户没写”（留给 callback 补默认）
	// 与“显式写了 0”（非法值，必须被声明式校验拦下）。
	MinCount *int32
}

// SchedulingConstraints 保存拓扑等放置约束。
//
// +k8s:deepcopy-gen=true
type SchedulingConstraints struct {
	// Topology 列出施加在组内 Pod 上的拓扑约束。
	Topology []schedulingv1beta1.TopologyConstraint
}

// DisruptionMode 选择 Pod 的中断方式，必须且只能设置其中一个字段。
//
// +k8s:deepcopy-gen=true
type DisruptionMode struct {
	// Single 表示各 Pod 可以独立被中断。
	Single *SingleDisruptionMode

	// All 表示所有 Pod 必须一起被中断。
	All *AllDisruptionMode
}

// SingleDisruptionMode 表示各 Pod 可以独立被中断。
//
// +k8s:deepcopy-gen=true
type SingleDisruptionMode struct{}

// AllDisruptionMode 表示所有 Pod 必须一起被中断。
//
// +k8s:deepcopy-gen=true
type AllDisruptionMode struct{}

// ResourceClaim 引用一个共享的动态资源声明。
// ResourceClaimName 与 ResourceClaimTemplateName 必须且只能设置其中一个。
//
// +k8s:deepcopy-gen=true
type ResourceClaim struct {
	// Name 在组内唯一标识这条资源声明。
	Name string

	// ResourceClaimName 按名字引用一个已存在的 ResourceClaim。
	ResourceClaimName *string

	// ResourceClaimTemplateName 按名字引用一个 ResourceClaimTemplate，
	// 由该模板为本组生成对应的 ResourceClaim。
	ResourceClaimTemplateName *string
}

// SchedulingConfigFunc 在默认值与用户输入合并之后，对 SchedulingConfig 做后处理。
//
// 控制器常有一些依赖自身语义的默认值（例如把 gang 的 MinCount 设为 Job 的
// parallelism），这类逻辑无法放进通用的默认配置。用回调钩子把这个差异留给控制器，
// 同时保证它在“默认值 + 用户输入”合并之后运行，不会被后续合并覆盖。
type SchedulingConfigFunc func(*SchedulingConfig)

// WorkloadItem 是控制器逻辑 Workload 中的一个节点。
//
// 树形结构直接映射控制器的组合关系——有 Children 的节点编译成 composite 模板，
// 没有的编译成 leaf 模板。Input 里保留原始版本化对象和字段路径，是为了让声明式校验报错
// 能精确指回控制器的 API 字段，而不是指向 IR 内部。
//
// 当某个 WorkloadItem 作为 ValidationInput.OldRoot 传入时，只会读取它的 Name 与 Input；
// DefaultConfig、Callbacks 以及 Input.*.PathElements 都会被忽略，
// 因为解析后的配置和错误路径都来自新的根（root）。
type WorkloadItem struct {
	// Name 标识该组件，会作为 PodGroupTemplate 的名字，必须非空。
	Name string

	// Path 是该项的调度 building block 在控制器 API 中嵌入位置的字段路径。
	// 例如对 Job 来说是 "spec.scheduling"。
	Path *field.Path

	// DefaultConfig 是控制器为该节点提供的默认配置，用于用户未设置的任何字段。
	DefaultConfig *SchedulingConfig

	// Input 以原始版本化 API 对象的形式保存用户对该节点的意图，
	// 并附带对应的字段路径，用于精确的声明式校验与错误定位。
	// 为 nil 的字段会回退到 DefaultConfig。
	Input WorkloadInput

	// Callbacks 在默认值/用户输入合并之后，按顺序作用于解析后的配置。
	Callbacks []SchedulingConfigFunc

	// Children 保存该节点的逻辑子组件。
	//   - len(Children) > 0 表示这是复合节点，编译成 CompositePodGroupTemplate，
	//     其子节点递归编译。
	//   - len(Children) == 0 表示这是叶子节点，编译成单个 PodGroupTemplate。
	//
	// 复合节点通过自己的 Input/DefaultConfig（即 WorkloadInput.Policy.CompositePodGroupData）
	// 承载 group-of-groups 调度策略，它本身不直接拥有 Pod。
	Children []*WorkloadItem
}

// WorkloadInput 聚合了控制器嵌入到自身 API 中的叶子级 building block。
// 零值表示“什么都没设置”，因此调用方只需填充自己关心的 block。
//
// 每个 building block 都同时携带数据和 PathElements，把“值”与“它在控制器 API 中的位置”
// 绑在一起。这样校验错误信息能返回到用户填写的字段，而不是一个对控制器用户毫无意义的
// 内部路径。
type WorkloadInput struct {
	// Policy 是调度策略 building block 及其字段路径。
	Policy PolicyInput

	// Constraints 是调度约束 building block 及其字段路径。
	Constraints ConstraintsInput

	// DisruptionMode 是中断模式 building block 及其字段路径。
	DisruptionMode DisruptionModeInput

	// ResourceClaims 是资源声明 building block 及其字段路径。
	ResourceClaims ResourceClaimsInput
}

// PolicyInput 把调度策略 building block 连同其字段路径一起封装。
//
// 这里的“至多设置一个”属于输入合法性，与 IR 层 SchedulingPolicy 的“恰好一个”不同——
// 用户可能什么都没填（此时走默认配置）。校验器必须把两者分别检查，不能混为一谈。
//
// 一个节点要么是叶子要么是复合：PodGroupData 与 CompositePodGroupData 至多设置一个。
// 叶子节点（len(WorkloadItem.Children) == 0）使用 PodGroupData，
// 复合节点（len(WorkloadItem.Children) > 0）使用 CompositePodGroupData。
type PolicyInput struct {
	// PodGroupData 是叶子 PodGroup 的 Workload 调度策略信息。
	PodGroupData *schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy

	// CompositePodGroupData 是复合 CompositePodGroup 节点的 group-of-groups 调度策略信息。
	CompositePodGroupData *schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy

	// PathElements 是该 building block 相对于 WorkloadItem.Path 的路径，
	// 表示它在控制器 API 中的嵌入位置。为空时错误直接报告在 item 的 Path 上。
	PathElements []string
}

// ConstraintsInput 把调度约束 building block 连同其字段路径一起封装。
// 一个节点要么是叶子要么是复合，PodGroupData 与 CompositePodGroupData 至多设置一个。
type ConstraintsInput struct {
	// PodGroupData 是叶子 PodGroup 的 Workload 调度约束信息。
	PodGroupData *schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints

	// CompositePodGroupData 是复合 CompositePodGroup 节点的调度约束信息。
	CompositePodGroupData *schedulingv1alpha3.WorkloadCompositePodGroupSchedulingConstraints

	// PathElements 是该 building block 相对于 WorkloadItem.Path 的路径，
	// 表示它在控制器 API 中的嵌入位置；语义同 PolicyInput.PathElements。
	PathElements []string
}

// DisruptionModeInput 把中断模式 building block 连同其字段路径一起封装。
// 一个节点要么是叶子要么是复合，PodGroupData 与 CompositePodGroupData 至多设置一个。
type DisruptionModeInput struct {
	// PodGroupData 是叶子 PodGroup 的 Workload 中断模式信息。
	PodGroupData *schedulingv1alpha3.WorkloadPodGroupDisruptionMode

	// CompositePodGroupData 是复合 CompositePodGroup 节点的中断模式信息。
	CompositePodGroupData *schedulingv1alpha3.WorkloadCompositePodGroupDisruptionMode

	// PathElements 是该 building block 相对于 WorkloadItem.Path 的路径，
	// 表示它在控制器 API 中的嵌入位置。
	PathElements []string
}

// ResourceClaimsInput 把资源声明 building block 连同其字段路径一起封装。
type ResourceClaimsInput struct {
	// PodGroupData 是 PodGroup 的 Workload 资源声明列表。
	PodGroupData []schedulingv1alpha3.WorkloadPodGroupResourceClaim

	// PathElements 是该 building block 相对于 WorkloadItem.Path 的路径，
	// 表示它在控制器 API 中的嵌入位置；语义同 PolicyInput.PathElements。
	PathElements []string
}
