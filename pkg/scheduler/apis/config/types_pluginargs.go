/*
Copyright 2020 The Kubernetes Authors.

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

package config

import (
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DefaultPreemptionArgs 保存 DefaultPreemption 插件的配置参数。
type DefaultPreemptionArgs struct {
	metav1.TypeMeta

	// MinCandidateNodesPercentage 是试运行抢占时，候选节点入围数量占节点总数的百分比下限。
	// 取值范围 [0, 100]。未指定时默认为集群规模的 10%。
	MinCandidateNodesPercentage int32
	// MinCandidateNodesAbsolute 是入围候选节点的绝对数量下限。
	// 试运行抢占时枚举的候选节点数量大致为：
	// numCandidates = max(numNodes * minCandidateNodesPercentage, minCandidateNodesAbsolute)
	// 之所以说「大致」，是因为 PDB 冲突等其他因素也会影响最终入围数量。必须至少为 0。未指定时默认为 100。
	MinCandidateNodesAbsolute int32
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// InterPodAffinityArgs 保存 InterPodAffinity 插件的配置参数。
type InterPodAffinityArgs struct {
	metav1.TypeMeta

	// HardPodAffinityWeight 是对与传入 Pod 硬亲和性匹配的现有 Pod 的打分权重。
	HardPodAffinityWeight int32

	// IgnorePreferredTermsOfExistingPods 让调度器在候选节点打分时忽略现有 Pod 的 preferred 亲和规则，
	// 除非传入的 Pod 自身定义了 Pod 间亲和性。
	IgnorePreferredTermsOfExistingPods bool
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeResourcesFitArgs 保存 NodeResourcesFit 插件的配置参数。
type NodeResourcesFitArgs struct {
	metav1.TypeMeta

	// IgnoredResources 是 NodeResources fit 过滤器应忽略的资源列表。
	IgnoredResources []string
	// IgnoredResourceGroups 是 NodeResources fit 过滤器应忽略的资源组列表。
	// 例如组为 ["example.com"] 时，会忽略所有以 "example.com" 开头的资源名，
	// 如 "example.com/aaa" 和 "example.com/bbb"。资源组名不能包含 '/'。
	IgnoredResourceGroups []string

	// ScoringStrategy 选择节点资源打分策略。
	ScoringStrategy *ScoringStrategy
}

// PodTopologySpreadConstraintsDefaulting 定义 PodTopologySpread 插件设置默认约束的方式。
type PodTopologySpreadConstraintsDefaulting string

const (
	// SystemDefaulting 表示使用 Kubernetes 预定义的默认值。
	SystemDefaulting PodTopologySpreadConstraintsDefaulting = "System"
	// ListDefaulting 表示使用配置中提供的默认值。
	ListDefaulting PodTopologySpreadConstraintsDefaulting = "List"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodTopologySpreadArgs 保存 PodTopologySpread 插件的配置参数。
type PodTopologySpreadArgs struct {
	metav1.TypeMeta

	// DefaultConstraints 定义施加到未在 `pod.spec.topologySpreadConstraints` 中声明任何约束的 Pod 上的拓扑分布约束。
	// `.defaultConstraints[*].labelSelectors` 必须为空，因为它们由 Pod 与 Service、ReplicationController、
	// ReplicaSet 或 StatefulSet 的归属关系推导得出。
	// 非空时，.defaultingType 必须是 "List"。
	DefaultConstraints []v1.TopologySpreadConstraint

	// DefaultingType 决定 .defaultConstraints 的推导方式，可以是 "System" 或 "List"。
	//
	// - "System"：使用 Kubernetes 定义的约束，在节点和可用区间分散 Pod。
	// - "List"：使用 .defaultConstraints 中定义的约束。
	//
	// 默认为 "System"。
	// +optional
	DefaultingType PodTopologySpreadConstraintsDefaulting
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeResourcesBalancedAllocationArgs 保存 NodeResourcesBalancedAllocation 插件的配置参数。
type NodeResourcesBalancedAllocationArgs struct {
	metav1.TypeMeta

	// 打分时考虑的资源列表。
	// 默认资源集包含 "cpu" 和 "memory"，唯一合法的权重是 1。
	Resources []ResourceSpec
}

// UtilizationShapePoint 表示优先级函数形状上的一个点。
type UtilizationShapePoint struct {
	// 利用率（x 轴）。取值 0 到 100，100 表示节点完全用满。
	Utilization int32
	// 给定利用率对应的分数（y 轴）。取值 0 到 10。
	Score int32
}

// ResourceSpec 表示单个资源。
type ResourceSpec struct {
	// 资源名称。
	Name string
	// 资源权重。
	Weight int64
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeBindingArgs 保存 VolumeBinding 插件的配置参数。
type VolumeBindingArgs struct {
	metav1.TypeMeta

	// BindTimeoutSeconds 是卷绑定操作的超时秒数。
	// 必须是非负整数，0 表示不等待。nil 时使用默认值。
	BindTimeoutSeconds int64

	// Shape 指定打分函数形状的点集，用于根据静态制备 PV 的利用率给节点打分。
	// 利用率等于 Pod 请求的总存储量除以每个节点上可行 PV 的总容量。
	// 每个点包含利用率（0 到 100）和对应分数（0 到 10）。
	// 通过给不同利用率指定不同分数，可以反转优先级方向。
	// 默认形状点为：
	// 1) 利用率 0 对应分数 0
	// 2) 利用率 100 对应分数 10
	// 所有点必须按利用率递增排序。
	// +featureGate=StorageCapacityScoring
	// +optional
	Shape []UtilizationShapePoint
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeAffinityArgs 保存 NodeAffinity 插件的配置参数。
type NodeAffinityArgs struct {
	metav1.TypeMeta

	// AddedAffinity 在 PodSpec 指定的 NodeAffinity 之外额外施加到所有 Pod 上。
	// 也就是说，节点必须同时满足 AddedAffinity 与 .spec.NodeAffinity。
	// AddedAffinity 默认为空（所有节点都匹配）。
	// 使用 AddedAffinity 时，某些亲和性要求匹配特定节点的 Pod（如 DaemonSet Pod）可能保持不可调度。
	AddedAffinity *v1.NodeAffinity
}

// ScoringStrategyType 是 NodeResourcesFit 插件使用的打分策略类型。
type ScoringStrategyType string

const (
	// LeastAllocated 策略优先选择已分配资源最少的节点。
	LeastAllocated ScoringStrategyType = "LeastAllocated"
	// MostAllocated 策略优先选择已分配资源最多的节点。
	MostAllocated ScoringStrategyType = "MostAllocated"
	// RequestedToCapacityRatio 策略允许指定自定义形状函数，按请求量与容量之比给节点打分。
	RequestedToCapacityRatio ScoringStrategyType = "RequestedToCapacityRatio"
)

// ScoringStrategy 为节点资源插件定义 ScoringStrategyType。
type ScoringStrategy struct {
	// Type 选择运行哪种策略。
	Type ScoringStrategyType

	// 打分时考虑的资源。
	// 默认资源集包含 "cpu" 和 "memory"，权重相等。
	// 允许的权重范围是 1 到 100。
	// 未指定或显式设为 0 时，权重默认为 1。
	Resources []ResourceSpec

	// RequestedToCapacityRatio 策略特有的参数。
	RequestedToCapacityRatio *RequestedToCapacityRatioParam
}

// RequestedToCapacityRatioParam 定义 RequestedToCapacityRatio 的参数。
type RequestedToCapacityRatioParam struct {
	// Shape 是定义打分函数形状的点列表。
	Shape []UtilizationShapePoint
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DynamicResourcesArgs 保存 DynamicResources 插件的配置参数。
type DynamicResourcesArgs struct {
	metav1.TypeMeta

	// FilterTimeout 限制 filter 操作在每个节点上查找可分配给 Pod 的设备时所花的时间。
	//
	// 典型情况下该操作在 10 到 200 毫秒内完成，但也可能更久，取决于每个 ResourceClaim 的请求数、
	// ResourceClaim 数量、ResourceSlice 中发布的设备数量以及请求的复杂度。
	// CEL 求值之外的检查（使用检查、属性匹配等）同样耗时。
	//
	// 因此该插件施加此超时。超时后，Pod 对该节点被视为不可调度。
	// 如果其他节点的过滤成功，就选择那些节点；如果全部失败，Pod 进入不可调度队列。
	// 当 ResourceSlice 或 ResourceClaim 等变化表明再次调度可能成功时会重新检查。
	// 若反复失败，指数退避会减慢后续尝试。
	//
	// 默认 10 秒。
	// 这足以防止最坏情况，同时不影响 DRA 的正常使用。但过滤变慢也会拖慢不使用 DRA 的 Pod 的调度。
	// 管理员可在查看
	// `scheduler_plugin_execution_duration_seconds` 指标后调低该值，
	// 该指标记录每次 Filter 操作的耗时。
	// 另有 `scheduler_framework_extension_point_duration_seconds` 记录整体过滤阶段耗时。
	//
	// 设为 0 会完全禁用超时。
	FilterTimeout *metav1.Duration

	// BindingTimeout 限制 PreBind 扩展点等待 ResourceClaim 设备 BindingConditions 满足的时长。
	// 等待期间调度器周期性检查设备状态。若超时前所有必需条件未变为 true
	// （或任一 bindingFailureConditions 变为 true），则清除分配，Pod 重新进入调度队列。
	// 注意：若仍可行，可能选择同一节点或其他节点；否则 Pod 进入不可调度队列，按集群变化和退避重试。
	//
	// 默认值与特性门控：
	//   - DRADeviceBindingConditions 启用时默认 10 分钟。
	//   - 仅当 DRADeviceBindingConditions 与 DRAResourceClaimDeviceStatus 同时启用时才生效；否则省略该字段。
	//   - DRADeviceBindingConditions 禁用时设置该字段视为错误。
	//
	// 合法取值：
	//   - >=1s（非零），不设上限。
	//
	// 调优建议：
	//   - 较小的值可缩短设备未就绪时的重试间隔，但如果驱动通常需要更长时间报告就绪，可能增加抖动。
	//   - 收紧该超时前，先查看调度器延迟指标（如 `scheduler_framework_extension_point_duration_seconds`
	//     中的 PreBind 耗时）以及驱动的就绪行为。
	BindingTimeout *metav1.Duration
}

const DynamicResourcesFilterTimeoutDefault = 10 * time.Second
const DynamicResourcesBindingTimeoutDefault = 600 * time.Second
