/*
Copyright 2022 The Kubernetes Authors.

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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DefaultPreemptionArgs holds arguments used to configure the
// DefaultPreemption plugin.
//
// 抢占的“试运行”阶段需要遍历候选节点，逐个模拟驱逐受害者，代价随节点数线性增长。
// 这两个参数一起限制候选集的大小：百分比在大集群里避免固定数值过小，
// 绝对值在小集群里避免百分比算出零个候选。指针类型是为了区分“用户没填”和“显式填了 0”，
// 只有前者才能套用默认值，后者是合法配置。
type DefaultPreemptionArgs struct {
	metav1.TypeMeta `json:""`

	// MinCandidateNodesPercentage is the minimum number of candidates to
	// shortlist when dry running preemption as a percentage of number of nodes.
	// Must be in the range [0, 100]. Defaults to 10% of the cluster size if
	// unspecified.
	// 用百分比表达下限，让候选集规模随集群大小伸缩，而不是在小集群和大集群之间二选一。
	MinCandidateNodesPercentage *int32 `json:"minCandidateNodesPercentage,omitempty"`
	// MinCandidateNodesAbsolute is the absolute minimum number of candidates to
	// shortlist. The likely number of candidates enumerated for dry running
	// preemption is given by the formula:
	// numCandidates = max(numNodes * minCandidateNodesPercentage, minCandidateNodesAbsolute)
	// We say "likely" because there are other factors such as PDB violations
	// that play a role in the number of candidates shortlisted. Must be at least
	// 0 nodes. Defaults to 100 nodes if unspecified.
	// 绝对值下限保证百分比在小集群里不会算出太少候选，甚至一个都没有。
	MinCandidateNodesAbsolute *int32 `json:"minCandidateNodesAbsolute,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// InterPodAffinityArgs holds arguments used to configure the InterPodAffinity plugin.
//
// 反亲和在 Score 阶段默认只看硬约束，因为把软约束也计入会让分数受集群里已有 Pod
// 的偏好影响过大，且难以预测。Boolean 开关而非权重，是为了让关闭该行为不引入额外分值。
type InterPodAffinityArgs struct {
	metav1.TypeMeta `json:""`

	// HardPodAffinityWeight is the scoring weight for existing pods with a
	// matching hard affinity to the incoming pod.
	// 指针类型让“未配置”沿用默认值，同时允许显式配置为 0，即完全不计入这部分得分。
	HardPodAffinityWeight *int32 `json:"hardPodAffinityWeight,omitempty"`

	// IgnorePreferredTermsOfExistingPods configures the scheduler to ignore existing pods' preferred affinity
	// rules when scoring candidate nodes, unless the incoming pod has inter-pod affinities.
	// 软亲和对新 Pod 只是偏好；若新 Pod 自己没有亲和诉求，却让老 Pod 的偏好反向影响候选节点，
	// 打分就会随集群历史状态漂移。开关可用于关闭这部分计算，代价是旧行为不再保留。
	IgnorePreferredTermsOfExistingPods bool `json:"ignorePreferredTermsOfExistingPods"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeResourcesFitArgs holds arguments used to configure the NodeResourcesFit plugin.
//
// Filter 和 Score 由同一个插件负责，但关注点不同：Filter 要判断资源够不够，某些自定义资源
// 可能并不适合参与过滤；Score 则决定节点优先级，需要另一套策略。因此忽略列表只作用于
// Filter，打分策略单独放在 ScoringStrategy 里，互不干扰。
type NodeResourcesFitArgs struct {
	metav1.TypeMeta `json:""`

	// IgnoredResources is the list of resources that NodeResources fit filter
	// should ignore. This doesn't apply to scoring.
	// 整名匹配，用于精确排除个别资源；与下面的资源组互补。
	// +listType=atomic
	IgnoredResources []string `json:"ignoredResources,omitempty"`
	// IgnoredResourceGroups defines the list of resource groups that NodeResources fit filter should ignore.
	// e.g. if group is ["example.com"], it will ignore all resource names that begin
	// with "example.com", such as "example.com/aaa" and "example.com/bbb".
	// A resource group name can't contain '/'. This doesn't apply to scoring.
	// 按前缀成组忽略，避免为同一扩展资源的许多子类逐个列名；同样只影响过滤。
	// +listType=atomic
	IgnoredResourceGroups []string `json:"ignoredResourceGroups,omitempty"`

	// ScoringStrategy selects the node resource scoring strategy.
	// The default strategy is LeastAllocated with an equal "cpu" and "memory" weight.
	// 把打分策略做成可选结构，是为了让“只改过滤行为”的配置不必重复声明一整套打分参数。
	ScoringStrategy *ScoringStrategy `json:"scoringStrategy,omitempty"`
}

// PodTopologySpreadConstraintsDefaulting defines how to set default constraints
// for the PodTopologySpread plugin.
//
// 拓扑分布的默认约束有“内置”和“管理员自定义”两种来源。
// 用字符串枚举而不是布尔开关，便于以后增加第三种来源而不破坏现有配置。
type PodTopologySpreadConstraintsDefaulting string

const (
	// SystemDefaulting instructs to use the kubernetes defined default.
	// 使用内置约束，随 Kubernetes 版本演进而更新，不依赖管理员维护。
	SystemDefaulting PodTopologySpreadConstraintsDefaulting = "System"
	// ListDefaulting instructs to use the config provided default.
	// 使用配置中显式给出的约束列表，便于集群按自身拓扑定制。
	ListDefaulting PodTopologySpreadConstraintsDefaulting = "List"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodTopologySpreadArgs holds arguments used to configure the PodTopologySpread plugin.
//
// 默认约束用于给没有声明拓扑分布的 Pod 补一层打散策略。
// 这里禁止 labelSelectors 而由 Pod 所属工作负载推导，是为了约束只作用于对应 Pod 集合，
// 避免管理员写出过宽的选择器把不相关 Pod 一并打散。
type PodTopologySpreadArgs struct {
	metav1.TypeMeta `json:""`

	// DefaultConstraints defines topology spread constraints to be applied to
	// Pods that don't define any in `pod.spec.topologySpreadConstraints`.
	// `.defaultConstraints[*].labelSelectors` must be empty, as they are
	// deduced from the Pod's membership to Services, ReplicationControllers,
	// ReplicaSets or StatefulSets.
	// When not empty, .defaultingType must be "List".
	// 约束列表与类型必须配套，否则自定义约束会被忽略，配置看起来生效但实际没有。
	// +optional
	// +listType=atomic
	DefaultConstraints []corev1.TopologySpreadConstraint `json:"defaultConstraints,omitempty"`

	// DefaultingType determines how .defaultConstraints are deduced. Can be one
	// of "System" or "List".
	//
	// - "System": Use kubernetes defined constraints that spread Pods among
	//   Nodes and Zones.
	// - "List": Use constraints defined in .defaultConstraints.
	//
	// Defaults to "System".
	// 默认值选 System，让未配置的集群自动获得合理打散，而不用先写一段约束。
	// +optional
	DefaultingType PodTopologySpreadConstraintsDefaulting `json:"defaultingType,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeResourcesBalancedAllocationArgs holds arguments used to configure NodeResourcesBalancedAllocation plugin.
//
// 均衡分配的目标是让各维度资源的使用率接近，因此“参与比较的资源”和“各自权重”
// 必须放在一起定义：少了权重就无法体现资源重要性差异，少了资源列表就无法确定比较哪些维度。
type NodeResourcesBalancedAllocationArgs struct {
	metav1.TypeMeta `json:""`

	// Resources to be managed, the default is "cpu" and "memory" if not specified.
	// 用 map 语义而非普通列表，让同名资源只能配置一次，避免权重被重复项覆盖。
	// +listType=map
	// +listMapKey=name
	Resources []ResourceSpec `json:"resources,omitempty"`
}

// UtilizationShapePoint represents single point of priority function shape.
//
// 分段线性插值的基本单元。把横纵坐标都归一化到固定区间（0-100 / 0-10），
// 使同一套形状定义能用于不同资源，而不用为每种资源写一份打分函数。
type UtilizationShapePoint struct {
	// Utilization (x axis). Valid values are 0 to 100. Fully utilized node maps to 100.
	// 横轴统一用百分比，避免不同资源的绝对量纲混在一起。
	Utilization int32 `json:"utilization"`
	// Score assigned to given utilization (y axis). Valid values are 0 to 10.
	// 纵轴沿用调度框架的打分范围，插值后直接得到节点分数。
	Score int32 `json:"score"`
}

// ResourceSpec represents a single resource.
//
// 把资源名和权重绑定成一个结构，而不是两个并行列表，可避免两表长度不一致导致配置歧义。
type ResourceSpec struct {
	// Name of the resource.
	Name string `json:"name"`
	// Weight of the resource.
	// 省略时由各使用方填默认值（通常为 1），这样只需声明与默认值不同的资源。
	Weight int64 `json:"weight,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeBindingArgs holds arguments used to configure the VolumeBinding plugin.
//
// 卷绑定涉及等待存储系统完成 Provision，可能阻塞调度周期，因此需要超时上限。
// 打分形状则是独立特性，由特性门控控制，避免在未开启该特性时影响默认打分。
type VolumeBindingArgs struct {
	metav1.TypeMeta `json:""`

	// BindTimeoutSeconds is the timeout in seconds in volume binding operation.
	// Value must be non-negative integer. The value zero indicates no waiting.
	// If this value is nil, the default value (600) will be used.
	// 指针类型区分“未配置”与“显式为 0”：后者表示完全不等待，前者才回退到默认值。
	BindTimeoutSeconds *int64 `json:"bindTimeoutSeconds,omitempty"`

	// Shape specifies the points defining the score function shape, which is
	// used to score nodes based on the utilization of provisioned PVs.
	// The utilization is calculated by dividing the total requested
	// storage of the pod by the total capacity of feasible PVs on each node.
	// Each point contains utilization (ranges from 0 to 100) and its
	// associated score (ranges from 0 to 10). You can turn the priority by
	// specifying different scores for different utilization numbers.
	// The default shape points are:
	// 1) 10 for 0 utilization
	// 2) 0 for 100 utilization
	// All points must be sorted in increasing order by utilization.
	// 要求递增是因为插值依赖有序点查找，运行时不做排序以省去每次打分的开销。
	// +featureGate=StorageCapacityScoring
	// +optional
	// +listType=atomic
	Shape []UtilizationShapePoint `json:"shape,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeAffinityArgs holds arguments to configure the NodeAffinity plugin.
//
// 额外亲和约束作用于全部 Pod，等价于给集群加一层统一的节点准入条件。
// 它不是修改 PodSpec，因此不会影响 API 中看到的 Pod 定义，只影响调度结果。
type NodeAffinityArgs struct {
	metav1.TypeMeta `json:""`

	// AddedAffinity is applied to all Pods additionally to the NodeAffinity
	// specified in the PodSpec. That is, Nodes need to satisfy AddedAffinity
	// AND .spec.NodeAffinity. AddedAffinity is empty by default (all Nodes
	// match).
	// When AddedAffinity is used, some Pods with affinity requirements that match
	// a specific Node (such as Daemonset Pods) might remain unschedulable.
	// 这是叠加约束而非替换，与 Pod 自带条件取交集；指针类型让不启用该功能时无需构造空对象。
	// +optional
	AddedAffinity *corev1.NodeAffinity `json:"addedAffinity,omitempty"`
}

// ScoringStrategyType the type of scoring strategy used in NodeResourcesFit plugin.
//
// 不同策略的数学形式差别较大，放进同一个枚举便于配置校验时提前发现不支持的值，
// 而不是在打分阶段才失败。
type ScoringStrategyType string

const (
	// LeastAllocated strategy prioritizes nodes with least allocated resources.
	// 偏向分散放置，保留更多空余资源给未来的大 Pod。
	LeastAllocated ScoringStrategyType = "LeastAllocated"
	// MostAllocated strategy prioritizes nodes with most allocated resources.
	// 偏向紧凑放置，尽量腾出整节点供大任务使用。
	MostAllocated ScoringStrategyType = "MostAllocated"
	// RequestedToCapacityRatio strategy allows specifying a custom shape function
	// to score nodes based on the request to capacity ratio.
	// 当线性策略不够用时，用自定义形状表达非线性偏好（例如中段收益最高）。
	RequestedToCapacityRatio ScoringStrategyType = "RequestedToCapacityRatio"
)

// ScoringStrategy define ScoringStrategyType for node resource plugin
//
// 一个策略需要同时回答三个问题：用哪种算法、比较哪些资源、算法需要哪些专属参数。
// 专属参数单独放一个可选结构，让那些不需要形状函数的策略不必填无用字段。
type ScoringStrategy struct {
	// Type selects which strategy to run.
	Type ScoringStrategyType `json:"type,omitempty"`

	// Resources to consider when scoring.
	// The default resource set includes "cpu" and "memory" with an equal weight.
	// Allowed weights go from 1 to 100.
	// Weight defaults to 1 if not specified or explicitly set to 0.
	// 权重缺省时统一按 1 处理，避免权重为 0 时该资源完全不参与比较。
	// +listType=map
	// +listMapKey=topologyKey
	Resources []ResourceSpec `json:"resources,omitempty"`

	// Arguments specific to RequestedToCapacityRatio strategy.
	// 只有选了该策略才会读取，因此用指针表示“未配置”，让其他策略的配置保持简洁。
	RequestedToCapacityRatio *RequestedToCapacityRatioParam `json:"requestedToCapacityRatio,omitempty"`
}

// RequestedToCapacityRatioParam define RequestedToCapacityRatio parameters
//
// 策略的自定义能力全部收敛到“形状点列表”，避免为每个可能的可调参数都往顶层配置加字段。
type RequestedToCapacityRatioParam struct {
	// Shape is a list of points defining the scoring function shape.
	// 声明为 atomic，因为点之间有顺序依赖，局部合并会破坏插值所需的整体形状。
	// +listType=atomic
	Shape []UtilizationShapePoint `json:"shape,omitempty"`
}
