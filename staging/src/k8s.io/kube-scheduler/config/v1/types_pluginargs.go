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

// 抢占的“试运行”阶段需要遍历候选节点，逐个模拟驱逐受害者，代价随节点数线性增长。
// 这两个参数一起限制候选集的大小：百分比在大集群里避免固定数值过小，
// 绝对值在小集群里避免百分比算出零个候选。指针类型是为了区分“用户没填”和“显式填了 0”，
// 只有前者才能套用默认值，后者是合法配置。
type DefaultPreemptionArgs struct {
	metav1.TypeMeta `json:""`

	// MinCandidateNodesPercentage 是抢占 dry running 时候选节点短名单的最小数量，
	// 以节点总数的百分比表示，必须落在 [0, 100] 区间；未指定时默认为集群规模的 10%。
	// 用百分比表达下限，让候选集规模随集群大小伸缩，而不是在小集群和大集群之间二选一。
	MinCandidateNodesPercentage *int32 `json:"minCandidateNodesPercentage,omitempty"`
	// MinCandidateNodesAbsolute 是候选短名单的最小数量绝对值。抢占 dry running 实际枚举的
	// 候选数大概率由公式给出：
	// numCandidates = max(numNodes * minCandidateNodesPercentage, minCandidateNodesAbsolute)
	// 之所以说"大概率"，是因为还有 PDB 违规等其他因素影响最终短名单。该值至少为 0 个节点；
	// 未指定时默认为 100 个节点。
	// 绝对值下限保证百分比在小集群里不会算出太少候选，甚至一个都没有。
	MinCandidateNodesAbsolute *int32 `json:"minCandidateNodesAbsolute,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// InterPodAffinityArgs 用于配置 InterPodAffinity 插件。
// 反亲和在 Score 阶段默认只看硬约束，因为把软约束也计入会让分数受集群里已有 Pod
// 的偏好影响过大，且难以预测。Boolean 开关而非权重，是为了让关闭该行为不引入额外分值。
type InterPodAffinityArgs struct {
	metav1.TypeMeta `json:""`

	// HardPodAffinityWeight 是针对“已有 Pod 与 incoming Pod 存在匹配的硬亲和”这一部分的打分权重。
	// 指针类型让“未配置”沿用默认值，同时允许显式配置为 0，即完全不计入这部分得分。
	HardPodAffinityWeight *int32 `json:"hardPodAffinityWeight,omitempty"`

	// IgnorePreferredTermsOfExistingPods 配置调度器在给候选节点打分时忽略已有 Pod 的软亲和规则，
	// 除非 incoming Pod 自身带有 Pod 间亲和性。
	// 软亲和对新 Pod 只是偏好；若新 Pod 自己没有亲和诉求，却让老 Pod 的偏好反向影响候选节点，
	// 打分就会随集群历史状态漂移。开关可用于关闭这部分计算，代价是旧行为不再保留。
	IgnorePreferredTermsOfExistingPods bool `json:"ignorePreferredTermsOfExistingPods"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeResourcesFitArgs 用于配置 NodeResourcesFit 插件。
// Filter 和 Score 由同一个插件负责，但关注点不同：Filter 要判断资源够不够，某些自定义资源
// 可能并不适合参与过滤；Score 则决定节点优先级，需要另一套策略。因此忽略列表只作用于
// Filter，打分策略单独放在 ScoringStrategy 里，互不干扰。
type NodeResourcesFitArgs struct {
	metav1.TypeMeta `json:""`

	// IgnoredResources 是 NodeResources fit 过滤器应忽略的资源列表，不适用于打分阶段。
	// 整名匹配，用于精确排除个别资源；与下面的资源组互补。
	// +listType=atomic
	IgnoredResources []string `json:"ignoredResources,omitempty"`
	// IgnoredResourceGroups 定义 NodeResources fit 过滤器应忽略的资源组列表。
	// 例如组为 ["example.com"] 时，所有以 "example.com" 开头的资源名都会被忽略，
	// 如 "example.com/aaa" 与 "example.com/bbb"。资源组名不能包含 '/'；不适用于打分阶段。
	// 按前缀成组忽略，避免为同一扩展资源的许多子类逐个列名；同样只影响过滤。
	// +listType=atomic
	IgnoredResourceGroups []string `json:"ignoredResourceGroups,omitempty"`

	// ScoringStrategy 选择节点资源打分策略。默认策略是 LeastAllocated，"cpu" 与 "memory" 权重相同。
	// 把打分策略做成可选结构，是为了让“只改过滤行为”的配置不必重复声明一整套打分参数。
	ScoringStrategy *ScoringStrategy `json:"scoringStrategy,omitempty"`
}

// PodTopologySpreadConstraintsDefaulting 定义 PodTopologySpread 插件如何设置默认约束：
// 拓扑分布的默认约束有“内置”和“管理员自定义”两种来源。
// 用字符串枚举而不是布尔开关，便于以后增加第三种来源而不破坏现有配置。
type PodTopologySpreadConstraintsDefaulting string

const (
	// SystemDefaulting 表示使用 Kubernetes 定义的默认约束。
	// 使用内置约束，随 Kubernetes 版本演进而更新，不依赖管理员维护。
	SystemDefaulting PodTopologySpreadConstraintsDefaulting = "System"
	// ListDefaulting 表示使用配置中提供的默认约束。
	// 使用配置中显式给出的约束列表，便于集群按自身拓扑定制。
	ListDefaulting PodTopologySpreadConstraintsDefaulting = "List"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodTopologySpreadArgs 用于配置 PodTopologySpread 插件。
// 默认约束用于给没有声明拓扑分布的 Pod 补一层打散策略。
// 这里禁止 labelSelectors 而由 Pod 所属工作负载推导，是为了约束只作用于对应 Pod 集合，
// 避免管理员写出过宽的选择器把不相关 Pod 一并打散。
type PodTopologySpreadArgs struct {
	metav1.TypeMeta `json:""`

	// DefaultConstraints 定义要施加给那些没有在 `pod.spec.topologySpreadConstraints` 中声明
	// 拓扑分布约束的 Pod 上的默认约束。`.defaultConstraints[*].labelSelectors` 必须为空，
	// 它们会由 Pod 所属的 Services、ReplicationControllers、ReplicaSets 或 StatefulSets 推导得出。
	// 该列表非空时，.defaultingType 必须为 "List"。
	// 约束列表与类型必须配套，否则自定义约束会被忽略，配置看起来生效但实际没有。
	// +optional
	// +listType=atomic
	DefaultConstraints []corev1.TopologySpreadConstraint `json:"defaultConstraints,omitempty"`

	// DefaultingType 决定 .defaultConstraints 如何推导，可取 "System" 或 "List"。
	// - "System": 使用 Kubernetes 定义的、把 Pod 打散到各 Node 和 Zone 的约束。
	// - "List": 使用 .defaultConstraints 中定义的约束。
	// 默认为 "System"。
	// 默认值选 System，让未配置的集群自动获得合理打散，而不用先写一段约束。
	// +optional
	DefaultingType PodTopologySpreadConstraintsDefaulting `json:"defaultingType,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeResourcesBalancedAllocationArgs 用于配置 NodeResourcesBalancedAllocation 插件。
// 均衡分配的目标是让各维度资源的使用率接近，因此“参与比较的资源”和“各自权重”
// 必须放在一起定义：少了权重就无法体现资源重要性差异，少了资源列表就无法确定比较哪些维度。
type NodeResourcesBalancedAllocationArgs struct {
	metav1.TypeMeta `json:""`

	// 要管理的资源列表；未指定时默认为 "cpu" 和 "memory"。
	// 用 map 语义而非普通列表，让同名资源只能配置一次，避免权重被重复项覆盖。
	// +listType=map
	// +listMapKey=name
	Resources []ResourceSpec `json:"resources,omitempty"`
}

// UtilizationShapePoint 表示打分函数形状中的单个点。
// 分段线性插值的基本单元。把横纵坐标都归一化到固定区间（0-100 / 0-10），
// 使同一套形状定义能用于不同资源，而不用为每种资源写一份打分函数。
type UtilizationShapePoint struct {
	// 利用率（x 轴），有效取值 0 到 100，完全利用的节点对应 100。
	// 横轴统一用百分比，避免不同资源的绝对量纲混在一起。
	Utilization int32 `json:"utilization"`
	// 分配给定利用率的分数（y 轴），有效取值 0 到 10。
	// 纵轴沿用调度框架的打分范围，插值后直接得到节点分数。
	Score int32 `json:"score"`
}

// ResourceSpec 表示单个资源。
// 把资源名和权重绑定成一个结构，而不是两个并行列表，可避免两表长度不一致导致配置歧义。
type ResourceSpec struct {
	// Name 是资源名称。
	Name string `json:"name"`
	// 该资源的权重。省略时由各使用方填默认值（通常为 1），这样只需声明与默认值不同的资源。
	Weight int64 `json:"weight,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeBindingArgs 用于配置 VolumeBinding 插件。
// 卷绑定涉及等待存储系统完成 Provision，可能阻塞调度周期，因此需要超时上限。
// 打分形状则是独立特性，由特性门控控制，避免在未开启该特性时影响默认打分。
type VolumeBindingArgs struct {
	metav1.TypeMeta `json:""`

	// BindTimeoutSeconds 是卷绑定操作的超时秒数，必须为非负整数；0 表示不等待；
	// 为 nil 时使用默认值（600）。
	// 指针类型区分“未配置”与“显式为 0”：后者表示完全不等待，前者才回退到默认值。
	BindTimeoutSeconds *int64 `json:"bindTimeoutSeconds,omitempty"`

	// Shape 指定定义打分函数形状的点集，用于按已供给 PV 的利用率给节点打分。
	// 利用率 = Pod 请求的存储总量除以每个节点上可行 PV 的总容量。
	// 每个点包含利用率（0 到 100）及其对应分数（0 到 10）；
	// 通过给不同利用率指定不同分数即可调节优先级。默认形状点为：
	// 1) 利用率 0 对应 10 分
	// 2) 利用率 100 对应 0 分
	// 所有点必须按利用率递增排序。
	// 要求递增是因为插值依赖有序点查找，运行时不做排序以省去每次打分的开销。
	// +featureGate=StorageCapacityScoring
	// +optional
	// +listType=atomic
	Shape []UtilizationShapePoint `json:"shape,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeAffinityArgs 用于配置 NodeAffinity 插件。
// 额外亲和约束作用于全部 Pod，等价于给集群加一层统一的节点准入条件。
// 它不是修改 PodSpec，因此不会影响 API 中看到的 Pod 定义，只影响调度结果。
type NodeAffinityArgs struct {
	metav1.TypeMeta `json:""`

	// AddedAffinity 在 PodSpec 中的 NodeAffinity 之外额外施加到所有 Pod 上：
	// 节点必须同时满足 AddedAffinity 与 .spec.NodeAffinity。
	// AddedAffinity 默认为空（匹配所有节点）。
	// 使用 AddedAffinity 时，某些亲和性要求恰好只匹配特定节点的 Pod（如 DaemonSet Pod）
	// 可能因此一直无法调度。
	// 这是叠加约束而非替换，与 Pod 自带条件取交集；指针类型让不启用该功能时无需构造空对象。
	// +optional
	AddedAffinity *corev1.NodeAffinity `json:"addedAffinity,omitempty"`
}

// ScoringStrategyType 是 NodeResourcesFit 插件所使用的打分策略的类型。
// 不同策略的数学形式差别较大，放进同一个枚举便于配置校验时提前发现不支持的值，
// 而不是在打分阶段才失败。
type ScoringStrategyType string

const (
	// LeastAllocated 策略优先选择已分配资源最少的节点。
	// 偏向分散放置，保留更多空余资源给未来的大 Pod。
	LeastAllocated ScoringStrategyType = "LeastAllocated"
	// MostAllocated 策略优先选择已分配资源最多的节点。
	// 偏向紧凑放置，尽量腾出整节点供大任务使用。
	MostAllocated ScoringStrategyType = "MostAllocated"
	// RequestedToCapacityRatio 策略允许指定自定义形状函数，按请求与容量的比值给节点打分。
	// 当线性策略不够用时，用自定义形状表达非线性偏好（例如中段收益最高）。
	RequestedToCapacityRatio ScoringStrategyType = "RequestedToCapacityRatio"
)

// ScoringStrategy 定义节点资源插件使用的 ScoringStrategyType。
// 一个策略需要同时回答三个问题：用哪种算法、比较哪些资源、算法需要哪些专属参数。
// 专属参数单独放一个可选结构，让那些不需要形状函数的策略不必填无用字段。
type ScoringStrategy struct {
	// Type 选择要运行的打分策略。
	Type ScoringStrategyType `json:"type,omitempty"`

	// 打分时要考虑的资源。默认资源集为 "cpu" 和 "memory"，权重相同。
	// 允许的权重范围为 1 到 100；未指定或显式设为 0 时权重默认为 1。
	// 权重缺省时统一按 1 处理，避免权重为 0 时该资源完全不参与比较。
	// +listType=map
	// +listMapKey=topologyKey
	Resources []ResourceSpec `json:"resources,omitempty"`

	// RequestedToCapacityRatio 策略的专属参数。
	// 只有选了该策略才会读取，因此用指针表示“未配置”，让其他策略的配置保持简洁。
	RequestedToCapacityRatio *RequestedToCapacityRatioParam `json:"requestedToCapacityRatio,omitempty"`
}

// RequestedToCapacityRatioParam 定义 RequestedToCapacityRatio 策略的参数。
// 策略的自定义能力全部收敛到“形状点列表”，避免为每个可能的可调参数都往顶层配置加字段。
type RequestedToCapacityRatioParam struct {
	// Shape 是定义打分函数形状的点列表。
	// 声明为 atomic，因为点之间有顺序依赖，局部合并会破坏插值所需的整体形状。
	// +listType=atomic
	Shape []UtilizationShapePoint `json:"shape,omitempty"`
}
