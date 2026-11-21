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

package autoscaling

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Scale 表示针对某个资源的伸缩请求。
type Scale struct {
	metav1.TypeMeta
	// 标准对象元数据。详见：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta

	// spec 定义 scale 的行为。详见：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status。
	// +optional
	Spec ScaleSpec

	// status 表示 scale 的当前状态。详见：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status。只读。
	// +optional
	Status ScaleStatus
}

// ScaleSpec 描述 scale 子资源的属性。
type ScaleSpec struct {
	// replicas 是被伸缩对象期望的实例数。
	// +optional
	Replicas int32
}

// ScaleStatus 表示 scale 子资源的当前状态。
type ScaleStatus struct {
	// replicas 是被伸缩对象实际观测到的实例数。
	Replicas int32

	// 用于筛选与副本数对应的 Pod 的标签查询。它与 label selector 含义相同，
	// 但以字符串形式提供，以避免客户端对其做自省解析。字符串格式与 query-param 语法一致。
	// 详见：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	Selector string
}

// CrossVersionObjectReference 包含定位被引用资源所需的足够信息。
type CrossVersionObjectReference struct {
	// kind 是被引用对象的 kind。详见：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds"
	Kind string

	// name 是被引用对象的名称。详见：https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	Name string

	// apiVersion 是被引用对象的 API 版本。
	// +optional
	APIVersion string
}

// HorizontalPodAutoscalerSpec 描述 HorizontalPodAutoscaler 期望实现的功能。
type HorizontalPodAutoscalerSpec struct {
	// scaleTargetRef 指向要伸缩的目标资源，既用于确定采集哪些 Pod 的指标，
	// 也用于实际修改副本数。
	ScaleTargetRef CrossVersionObjectReference

	// minReplicas 是自动伸缩器可以缩到的最小副本数，默认为 1。
	// 如果启用了 alpha 特性门控 HPAScaleToZero，且至少配置了一个 Object 或 External 指标，
	// 则允许设为 0。只要至少有一个指标值可用，伸缩就会生效。
	// +optional
	MinReplicas *int32

	// maxReplicas 是自动伸缩器可以扩到的最大副本数，不能小于 minReplicas。
	MaxReplicas int32

	// metrics 包含用于计算期望副本数的指标规格（取所有指标计算结果中的最大值）。
	// 期望副本数按目标值与当前值之比乘以当前 Pod 数计算。因此所用指标必须
	// 随 Pod 数增加而下降，反之亦然。各指标源类型的说明中给出了指标应如何响应的细节。
	// +optional
	Metrics []MetricSpec

	// behavior 配置目标在扩容和缩容两个方向上的伸缩行为
	// （分别对应 scaleUp 和 scaleDown 字段）。未设置时使用默认的 HPAScalingRules。
	// +optional
	Behavior *HorizontalPodAutoscalerBehavior
}

// HorizontalPodAutoscalerBehavior 分别通过 scaleUp 和 scaleDown 字段
// 配置扩容和缩容两个方向上的伸缩行为。
type HorizontalPodAutoscalerBehavior struct {
	// scaleUp 是扩容方向上的伸缩策略。
	// 未设置时默认取以下两者中较大的值：
	//   * 每 60 秒最多增加 4 个 Pod
	//   * 每 60 秒 Pod 数量翻倍
	// 不使用稳定窗口。
	// +optional
	ScaleUp *HPAScalingRules
	// scaleDown 是缩容方向上的伸缩策略。
	// 未设置时默认允许缩到 minReplicas 个 Pod，并使用 300 秒稳定窗口
	// （即取最近 300 秒内最高的推荐值）。
	// +optional
	ScaleDown *HPAScalingRules
}

// ScalingPolicySelect 用于指定某个伸缩方向上应采用哪条策略。
type ScalingPolicySelect string

const (
	// MaxPolicySelect 选择变化幅度最大的策略。
	MaxPolicySelect ScalingPolicySelect = "Max"
	// MinPolicySelect 选择变化幅度最小的策略。
	MinPolicySelect ScalingPolicySelect = "Min"
	// DisabledPolicySelect 禁用该方向上的伸缩。
	DisabledPolicySelect ScalingPolicySelect = "Disabled"
)

// HPAScalingRules 通过伸缩策略规则和可配置的指标容差，
// 配置某一个方向上的伸缩行为。
//
// 策略规则在 HPA 根据指标算出 DesiredReplicas 之后生效：
// 通过限制伸缩策略可以控制伸缩速度；通过指定稳定窗口可以避免抖动，
// 即不立即采用最新的推荐值，而是取窗口内最保守的值。
//
// 容差作用于指标值，避免指标有小幅波动时就过早触发伸缩。
type HPAScalingRules struct {
	// StabilizationWindowSeconds 是扩容或缩容时考虑历史推荐值的秒数。
	// 取值必须大于等于 0 且小于等于 3600（一小时）。未设置时使用默认值：
	// - 扩容：0（即不做稳定处理）。
	// - 缩容：300（即稳定窗口为 300 秒）。
	// +optional
	StabilizationWindowSeconds *int32
	// selectPolicy 用于指定采用哪条策略。未设置时默认使用 MaxPolicySelect。
	// +optional
	SelectPolicy *ScalingPolicySelect
	// policies 是伸缩时可用的候选策略列表。未设置时使用默认值：
	// - 扩容：允许在 15 秒窗口内 Pod 数翻倍，或绝对增加 4 个 Pod。
	// - 缩容：允许在 15 秒窗口内移除所有 Pod。
	// +optional
	Policies []HPAScalingPolicy
	// tolerance 是当前指标值与期望指标值之比的容差，在此范围内不更新期望副本数
	// （例如 0.01 表示 1%）。取值必须大于等于零。未设置时应用集群范围的默认容差（默认 10%）。
	//
	// 例如，若自动伸缩配置的内存消耗目标为 100Mi，缩容和扩容容差分别为 5% 和 1%，
	// 则当实际消耗低于 95Mi 或超过 101Mi 时才会触发伸缩。
	//
	// +featureGate=HPAConfigurableTolerance
	// +optional
	Tolerance *resource.Quantity
}

// HPAScalingPolicyType 是伸缩决策时可以使用的策略类型。
type HPAScalingPolicyType string

const (
	// PodsScalingPolicy 用于指定 Pod 绝对数量的变化。
	PodsScalingPolicy HPAScalingPolicyType = "Pods"
	// PercentScalingPolicy 用于指定相对于当前 Pod 数量的变化比例。
	PercentScalingPolicy HPAScalingPolicyType = "Percent"
)

// HPAScalingPolicy 表示一条策略，且必须在指定的历史时间窗口内一直成立。
type HPAScalingPolicy struct {
	// Type 用于指定伸缩策略类型。
	Type HPAScalingPolicyType
	// Value 是该策略允许的变化量，必须大于零。
	Value int32
	// PeriodSeconds 指定该策略必须成立的时间窗口，必须大于零且小于等于 1800（30 分钟）。
	PeriodSeconds int32
}

// MetricSourceType 表示指标的类型。
type MetricSourceType string

const (
	// ObjectMetricSourceType 是描述某个 Kubernetes 对象的指标
	// （例如 Ingress 对象上的每秒请求数）。
	ObjectMetricSourceType MetricSourceType = "Object"
	// PodsMetricSourceType 是描述当前伸缩目标中每个 Pod 的指标
	// （例如每秒处理的事务数）。各 Pod 的值会先求平均值再与目标值比较。
	PodsMetricSourceType MetricSourceType = "Pods"
	// ResourceMetricSourceType 是 Kubernetes 已知的资源指标（在 requests 和 limits 中指定，
	// 如 CPU 或内存），描述当前伸缩目标中的每个 Pod。这类指标是 Kubernetes 内建的，
	// 除普通 Pod 指标（"pods" 来源）的伸缩选项外还有专门的选项。
	ResourceMetricSourceType MetricSourceType = "Resource"
	// ExternalMetricSourceType 是不与任何 Kubernetes 对象关联的全局指标。
	// 它允许基于集群外组件的信息进行自动伸缩
	// （例如云消息服务中的队列长度，或集群外负载均衡器的 QPS）。
	ExternalMetricSourceType MetricSourceType = "External"
	// ContainerResourceMetricSourceType 是 Kubernetes 已知的资源指标
	// （在 requests 和 limits 中指定，如 CPU 或内存），描述当前伸缩目标中
	// 每个 Pod 内的单个容器。这类指标是 Kubernetes 内建的，除普通 Pod 指标
	// （"pods" 来源）的伸缩选项外还有专门的选项。
	ContainerResourceMetricSourceType MetricSourceType = "ContainerResource"
)

// MetricSpec 指定如何基于单个指标进行伸缩
// （只能同时设置 `type` 和一个与之匹配的字段）。
type MetricSpec struct {
	// Type 是指标源类型，取值为 "Object"、"Pods" 或 "Resource" 之一，
	// 分别对应对象中的同名匹配字段。
	Type MetricSourceType

	// Object 指向描述单个 Kubernetes 对象的指标
	// （例如 Ingress 对象上的每秒请求数）。
	// +optional
	Object *ObjectMetricSource
	// Pods 指向描述当前伸缩目标中每个 Pod 的指标
	// （例如每秒处理的事务数）。各 Pod 的取值会先求平均值再与目标值比较。
	// +optional
	Pods *PodsMetricSource
	// Resource 指向 Kubernetes 已知的资源指标（如 requests 和 limits 中指定的指标），
	// 描述当前伸缩目标中的每个 Pod（如 CPU 或内存）。这类指标是 Kubernetes 内建的，
	// 除普通 Pod 指标（"pods" 来源）的伸缩选项外还有专门的选项。
	// +optional
	Resource *ResourceMetricSource
	// ContainerResource 指向 Kubernetes 已知的资源指标（如 requests 和 limits 中指定的指标），
	// 描述当前伸缩目标中每个 Pod 内的单个容器（如 CPU 或内存）。这类指标是 Kubernetes 内建的，
	// 除普通 Pod 指标（"pods" 来源）的伸缩选项外还有专门的选项。
	// +optional
	ContainerResource *ContainerResourceMetricSource
	// External 指向不与任何 Kubernetes 对象关联的全局指标。
	// 它允许基于集群外组件的信息进行自动伸缩
	// （例如云消息服务中的队列长度，或集群外负载均衡器的 QPS）。
	// +optional
	External *ExternalMetricSource
}

// ObjectMetricSource 表示基于描述某个 Kubernetes 对象的指标进行伸缩
// （例如 Ingress 对象上的每秒请求数）。
type ObjectMetricSource struct {
	DescribedObject CrossVersionObjectReference
	Target          MetricTarget
	Metric          MetricIdentifier
}

// PodsMetricSource 表示基于描述当前伸缩目标中每个 Pod 的指标进行伸缩
// （例如每秒处理的事务数）。各 Pod 的取值会先求平均值，再与目标值比较。
type PodsMetricSource struct {
	// metric 通过名称和选择器标识目标指标。
	Metric MetricIdentifier
	// target 指定该指标的目标值。
	Target MetricTarget
}

// ResourceMetricSource 表示基于 Kubernetes 已知的资源指标（在 requests 和 limits 中指定，
// 如 CPU 或内存）描述当前伸缩目标中的每个 Pod 进行伸缩。各 Pod 的取值会先求平均值，
// 再与目标值比较。这类指标是 Kubernetes 内建的，除普通 Pod 指标（"pods" 来源）
// 的伸缩选项外还有专门的选项。只能设置一种 "target" 类型。
type ResourceMetricSource struct {
	// Name 是所讨论资源的名称。
	Name api.ResourceName
	// Target 指定该指标的目标值。
	Target MetricTarget
}

// ContainerResourceMetricSource 表示基于 Kubernetes 已知的资源指标
// （在 requests 和 limits 中指定，如 CPU 或内存）描述当前伸缩目标中
// 每个 Pod 内的单个容器进行伸缩。各容器的取值会先求平均值，再与目标值比较。
// 这类指标是 Kubernetes 内建的，除普通 Pod 指标（"pods" 来源）的伸缩选项外
// 还有专门的选项。只能设置一种 "target" 类型。
type ContainerResourceMetricSource struct {
	// name 是资源的名称。
	Name api.ResourceName
	// container 是伸缩目标 Pod 内容器的名称。
	Container string
	// target 指定该指标的目标值。
	Target MetricTarget
}

// ExternalMetricSource 表示基于不与任何 Kubernetes 对象关联的指标进行伸缩
// （例如云消息服务中的队列长度，或集群外负载均衡器的 QPS）。
type ExternalMetricSource struct {
	// Metric 通过名称和选择器标识目标指标。
	Metric MetricIdentifier
	// Target 指定该指标的目标值。
	Target MetricTarget
}

// MetricIdentifier 定义指标的名称以及可选的选择器。
type MetricIdentifier struct {
	// Name 是该指标的名称。
	Name string
	// Selector 是该指标的选择器，采用标准 Kubernetes 标签选择器的字符串编码形式。
	// +optional
	Selector *metav1.LabelSelector
}

// MetricTarget 定义某个指标的目标值、平均值或平均利用率。
type MetricTarget struct {
	// Type 表示指标类型是 Utilization、Value 还是 AverageValue。
	Type MetricTargetType
	// Value 是该指标的目标值（以 quantity 表示）。
	Value *resource.Quantity
	// TargetAverageValue 是该指标在所有相关 Pod 上的平均目标值（以 quantity 表示）。
	AverageValue *resource.Quantity

	// AverageUtilization 是资源指标在所有相关 Pod 上的平均目标值，
	// 以 Pod 请求的资源量的百分比表示。目前仅对 Resource 指标源类型有效。
	AverageUtilization *int32
}

// MetricTargetType 指定目标指标的类型，取值为
// "Value"、"AverageValue" 或 "Utilization" 之一。
type MetricTargetType string

const (
	// UtilizationMetricType 是 MetricTarget.Type 的可能取值之一。
	UtilizationMetricType MetricTargetType = "Utilization"
	// ValueMetricType 是 MetricTarget.Type 的可能取值之一。
	ValueMetricType MetricTargetType = "Value"
	// AverageValueMetricType 是 MetricTarget.Type 的可能取值之一。
	AverageValueMetricType MetricTargetType = "AverageValue"
)

// HorizontalPodAutoscalerStatus 描述 horizontal pod autoscaler 的当前状态。
type HorizontalPodAutoscalerStatus struct {
	// ObservedGeneration 是该自动伸缩器观测到的最新 generation。
	// +optional
	ObservedGeneration *int64

	// LastScaleTime 是 HorizontalPodAutoscaler 上一次伸缩 Pod 数量的时间，
	// 自动伸缩器用它来控制 Pod 数量的变更频率。
	// +optional
	LastScaleTime *metav1.Time

	// CurrentReplicas 是该自动伸缩器管理的 Pod 当前副本数，即其最近一次观测到的值。
	CurrentReplicas int32

	// DesiredReplicas 是该自动伸缩器管理的 Pod 期望副本数，即其最近一次计算出的值。
	DesiredReplicas int32

	// CurrentMetrics 是该自动伸缩器所用指标最近一次读取到的状态。
	// +optional
	CurrentMetrics []MetricStatus

	// Conditions 是该自动伸缩器伸缩目标所需满足的 condition 集合，
	// 并指示这些条件是否已满足。
	Conditions []HorizontalPodAutoscalerCondition
}

// ConditionStatus 表示某个 condition 的状态（true、false 或 unknown）。
type ConditionStatus string

// 以下是合法的 condition 状态。"ConditionTrue" 表示资源满足该条件；
// "ConditionFalse" 表示资源不满足该条件；"ConditionUnknown" 表示 Kubernetes
// 无法判断资源是否满足该条件。将来可能会增加其他中间状态，例如 ConditionDegraded。
const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// HorizontalPodAutoscalerConditionType 是 HorizontalPodAutoscaler 的合法 condition 类型。
type HorizontalPodAutoscalerConditionType string

const (
	// ScalingActive 表示 HPA controller 在必要时能够执行伸缩：
	// 配置正确、能获取到期望的指标，且未被禁用。
	ScalingActive HorizontalPodAutoscalerConditionType = "ScalingActive"
	// AbleToScale 表示不存在阻碍伸缩的临时性问题，
	// 例如处于退避窗口，或无法访问/更新目标的 scale 对象。
	AbleToScale HorizontalPodAutoscalerConditionType = "AbleToScale"
	// ScalingLimited 表示根据指标算出的伸缩值超出 HPA 的允许范围，因而被限制（截断）。
	ScalingLimited HorizontalPodAutoscalerConditionType = "ScalingLimited"
	// ScaledToZero 表示 HPA controller 已将工作负载缩容到零。
	ScaledToZero HorizontalPodAutoscalerConditionType = "ScaledToZero"
)

// HorizontalPodAutoscalerCondition 描述 HorizontalPodAutoscaler 在某一时刻的状态。
type HorizontalPodAutoscalerCondition struct {
	// Type 描述当前的 condition 类型。
	Type HorizontalPodAutoscalerConditionType
	// Status 是该 condition 的状态（True、False、Unknown）。
	Status ConditionStatus
	// LastTransitionTime 是该 condition 上一次发生状态转换的时间。
	// +optional
	LastTransitionTime metav1.Time
	// Reason 是该 condition 上一次状态转换的原因。
	// +optional
	Reason string
	// Message 是对该状态转换的可读详细说明。
	// +optional
	Message string

	// observedGeneration 表示该 condition 是基于哪个 .metadata.generation 设置的。
	// 例如，若 .metadata.generation 当前为 12，而 .status.conditions[x].observedGeneration 为 9，
	// 则说明该 condition 相对于实例当前状态已经过期。
	// +optional
	ObservedGeneration *int64
}

// MetricStatus 描述单个指标最近一次读取到的状态。
type MetricStatus struct {
	// Type 是指标源类型，取值为 "Object"、"Pods" 或 "Resource" 之一，
	// 分别对应对象中的匹配字段。
	Type MetricSourceType

	// Object 指向描述单个 Kubernetes 对象的指标
	// （例如 Ingress 对象上的每秒请求数）。
	// +optional
	Object *ObjectMetricStatus
	// Pods 指向描述当前伸缩目标中每个 Pod 的指标
	// （例如每秒处理的事务数）。各 Pod 的取值会先求平均值再与目标值比较。
	// +optional
	Pods *PodsMetricStatus
	// Resource 指向 Kubernetes 已知的资源指标（如 requests 和 limits 中指定的指标），
	// 描述当前伸缩目标中的每个 Pod（如 CPU 或内存）。这类指标是 Kubernetes 内建的，
	// 除普通 Pod 指标（"pods" 来源）的伸缩选项外还有专门的选项。
	// +optional
	Resource *ResourceMetricStatus
	// ContainerResource 指向 Kubernetes 已知的资源指标（如 requests 和 limits 中指定的指标），
	// 描述当前伸缩目标中每个 Pod 内的单个容器（如 CPU 或内存）。这类指标是 Kubernetes 内建的，
	// 除普通 Pod 指标（"pods" 来源）的伸缩选项外还有专门的选项。
	// +optional
	ContainerResource *ContainerResourceMetricStatus
	// External 指向不与任何 Kubernetes 对象关联的全局指标。
	// 它允许基于集群外组件的信息进行自动伸缩
	// （例如云消息服务中的队列长度，或集群外负载均衡器的 QPS）。
	// +optional
	External *ExternalMetricStatus
}

// ObjectMetricStatus 表示描述某个 Kubernetes 对象的指标的当前值
// （例如 Ingress 对象上的每秒请求数）。
type ObjectMetricStatus struct {
	Metric  MetricIdentifier
	Current MetricValueStatus

	DescribedObject CrossVersionObjectReference
}

// PodsMetricStatus 表示描述当前伸缩目标中每个 Pod 的指标的当前值
// （例如每秒处理的事务数）。
type PodsMetricStatus struct {
	Metric  MetricIdentifier
	Current MetricValueStatus
}

// ResourceMetricStatus 表示 Kubernetes 已知的资源指标（在 requests 和 limits 中指定，
// 如 CPU 或内存）描述当前伸缩目标中每个 Pod 的当前值。这类指标是 Kubernetes 内建的，
// 除普通 Pod 指标（"pods" 来源）的伸缩选项外还有专门的选项。
type ResourceMetricStatus struct {
	// name 是该资源的名称。
	Name    api.ResourceName
	Current MetricValueStatus
}

// ContainerResourceMetricStatus 表示 Kubernetes 已知的资源指标
// （在 requests 和 limits 中指定，如 CPU 或内存）描述当前伸缩目标中
// 每个 Pod 内容器的当前值。这类指标是 Kubernetes 内建的，除普通 Pod 指标
// （"pods" 来源）的伸缩选项外还有专门的选项。
type ContainerResourceMetricStatus struct {
	// name 是该资源的名称。
	Name      api.ResourceName
	Container string
	Current   MetricValueStatus
}

// ExternalMetricStatus 表示不与任何 Kubernetes 对象关联的全局指标的当前值。
type ExternalMetricStatus struct {
	Metric  MetricIdentifier
	Current MetricValueStatus
}

// MetricValueStatus 表示某个指标的当前值。
type MetricValueStatus struct {
	Value              *resource.Quantity
	AverageValue       *resource.Quantity
	AverageUtilization *int32
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HorizontalPodAutoscaler 是 horizontal pod autoscaler 的配置，
// 它根据指定的指标自动管理任何实现了 scale 子资源的对象的副本数。
type HorizontalPodAutoscaler struct {
	metav1.TypeMeta
	// Metadata 是标准对象元数据。
	// 详见：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// spec 是自动伸缩器行为的规格说明。
	// 详见：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status。
	// +optional
	Spec HorizontalPodAutoscalerSpec

	// status 是自动伸缩器的当前信息。
	// +optional
	Status HorizontalPodAutoscalerStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// HorizontalPodAutoscalerList 是 horizontal pod autoscaler 对象的列表。
type HorizontalPodAutoscalerList struct {
	metav1.TypeMeta
	// Metadata 是标准的列表元数据。
	// +optional
	metav1.ListMeta

	// items 是 horizontal pod autoscaler 对象的列表。
	Items []HorizontalPodAutoscaler
}
