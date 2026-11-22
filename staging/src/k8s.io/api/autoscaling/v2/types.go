/*
Copyright 2021 The Kubernetes Authors.

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

// +k8s:openapi-gen=true

package v2

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.23

// HorizontalPodAutoscaler 是水平 Pod 自动扩缩器的配置，它根据指定的指标自动管理
// 任何实现 scale 子资源的资源的副本数。
// +k8s:supportsSubresource="/status"
type HorizontalPodAutoscaler struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 是自动扩缩器行为的规格说明。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status.
	// +required
	Spec HorizontalPodAutoscalerSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// status 是自动扩缩器的当前信息。
	// +optional
	Status HorizontalPodAutoscalerStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// HorizontalPodAutoscalerSpec 描述 HorizontalPodAutoscaler 的期望功能。
type HorizontalPodAutoscalerSpec struct {
	// scaleTargetRef 指向要扩缩的目标资源，既用于确定应收集指标的 Pod，
	// 也用于实际修改副本数。
	ScaleTargetRef CrossVersionObjectReference `json:"scaleTargetRef" protobuf:"bytes,1,opt,name=scaleTargetRef"`
	// minReplicas 是自动扩缩器可缩容到的副本数下限，默认为 1 个 Pod。
	// 当 alpha 特性门控 HPAScaleToZero 启用且至少配置了一个 Object 或 External 指标时，
	// minReplicas 允许为 0。只要至少有一个指标值可用，扩缩就保持激活。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifEnabled(HPAScaleToZero)=+k8s:minimum=0
	// +k8s:beta(since: "1.37")=+k8s:ifDisabled(HPAScaleToZero)=+k8s:minimum=1
	MinReplicas *int32 `json:"minReplicas,omitempty" protobuf:"varint,2,opt,name=minReplicas"`

	// maxReplicas 是自动扩缩器可扩容到的副本数上限，不得小于 minReplicas。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:minimum=1
	MaxReplicas int32 `json:"maxReplicas" protobuf:"varint,3,opt,name=maxReplicas"`

	// metrics 包含用于计算期望副本数的指标规格（取所有指标算出的最大副本数）。
	// 期望副本数的计算方式为：当前 Pod 数乘以目标值与当前值之比。因此所用指标必须随
	// Pod 数增加而下降，反之亦然。各指标源类型详情参见其各自说明。
	// 未设置时，默认指标为 80% 的平均 CPU 利用率。
	// +listType=atomic
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	Metrics []MetricSpec `json:"metrics,omitempty" protobuf:"bytes,4,rep,name=metrics"`

	// behavior 配置目标在扩容和缩容两个方向上的扩缩行为（分别对应 scaleUp 和 scaleDown 字段）。
	// 未设置时使用默认的 scaleUp 和 scaleDown HPAScalingRules。
	// +optional
	Behavior *HorizontalPodAutoscalerBehavior `json:"behavior,omitempty" protobuf:"bytes,5,opt,name=behavior"`
}

// CrossVersionObjectReference 包含足以识别被引用对象的信息。
type CrossVersionObjectReference struct {
	// kind 是被引用对象的 Kind；更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +k8s:alpha(since: "1.37")=+k8s:required
	Kind string `json:"kind" protobuf:"bytes,1,opt,name=kind"`

	// name 是被引用对象的名称；更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	// +k8s:alpha(since: "1.37")=+k8s:required
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`

	// apiVersion 是被引用对象的 API 版本。
	// +optional
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,3,opt,name=apiVersion"`
}

// MetricSpec 指定如何基于单个指标进行扩缩
// （只能同时设置 `type` 和一个与之匹配的字段）。
type MetricSpec struct {
	// type 是指标源类型，必须是 "ContainerResource"、"External"、"Object"、
	// "Pods" 或 "Resource" 之一，各自对应对象中的一个匹配字段。
	Type MetricSourceType `json:"type" protobuf:"bytes,1,name=type"`

	// object 指描述单个 Kubernetes 对象的指标（例如 Ingress 对象上的每秒请求数）。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:opaqueType
	Object *ObjectMetricSource `json:"object,omitempty" protobuf:"bytes,2,opt,name=object"`

	// pods 指描述当前扩缩目标中每个 Pod 的指标（例如每秒处理的事务数）。
	// 各值会先取平均值，再与目标值比较。
	// +optional
	Pods *PodsMetricSource `json:"pods,omitempty" protobuf:"bytes,3,opt,name=pods"`

	// resource 指 Kubernetes 已知的资源指标（如 requests 和 limits 中指定的资源），
	// 描述当前扩缩目标中的每个 Pod（如 CPU 或内存）。这类指标是 Kubernetes 内置的，
	// 相比使用 "pods" 源的普通单 Pod 指标有额外的扩缩选项。
	// +optional
	Resource *ResourceMetricSource `json:"resource,omitempty" protobuf:"bytes,4,opt,name=resource"`

	// containerResource 指 Kubernetes 已知的资源指标（如 requests 和 limits 中指定的资源），
	// 描述当前扩缩目标中每个 Pod 内的单个容器（如 CPU 或内存）。这类指标是 Kubernetes 内置的，
	// 相比使用 "pods" 源的普通单 Pod 指标有额外的扩缩选项。
	// +optional
	ContainerResource *ContainerResourceMetricSource `json:"containerResource,omitempty" protobuf:"bytes,7,opt,name=containerResource"`

	// external 指不与任何 Kubernetes 对象关联的全局指标。它支持基于集群外部组件的信息
	// 进行自动扩缩（例如云消息服务中的队列长度，或集群外部负载均衡器的 QPS）。
	// +optional
	External *ExternalMetricSource `json:"external,omitempty" protobuf:"bytes,5,opt,name=external"`
}

// HorizontalPodAutoscalerBehavior 配置目标在扩容和缩容两个方向上的扩缩行为
// （分别对应 scaleUp 和 scaleDown 字段）。
type HorizontalPodAutoscalerBehavior struct {
	// scaleUp 是扩容方向的扩缩策略。未设置时默认取以下两者中的较大值：
	//   * 每 60 秒最多增加 4 个 Pod
	//   * 每 60 秒 Pod 数翻倍
	// 不使用稳定窗口。
	// +optional
	ScaleUp *HPAScalingRules `json:"scaleUp,omitempty" protobuf:"bytes,1,opt,name=scaleUp"`

	// scaleDown 是缩容方向的扩缩策略。未设置时默认允许缩容至 minReplicas 个 Pod，
	// 并使用 300 秒的稳定窗口（即取最近 300 秒内的最高建议值）。
	// +optional
	ScaleDown *HPAScalingRules `json:"scaleDown,omitempty" protobuf:"bytes,2,opt,name=scaleDown"`
}

// ScalingPolicySelect 用于指定在某个方向上扩缩时应采用哪种策略。
type ScalingPolicySelect string

const (
	// MaxChangePolicySelect 选择变化幅度最大的策略。
	MaxChangePolicySelect ScalingPolicySelect = "Max"
	// MinChangePolicySelect 选择变化幅度最小的策略。
	MinChangePolicySelect ScalingPolicySelect = "Min"
	// DisabledPolicySelect 禁用此方向的扩缩。
	DisabledPolicySelect ScalingPolicySelect = "Disabled"
)

// HPAScalingRules 通过扩缩策略规则和一个可配置的指标容差来配置某个方向的扩缩行为。
//
// 扩缩策略规则在 HPA 根据指标算出 DesiredReplicas 之后应用。它们可通过指定扩缩策略限制
// 扩缩速度，也可通过指定稳定窗口防止抖动：副本数不会立即设定，而是选取稳定窗口内最安全的值。
//
// 容差作用于指标值，避免因指标小幅波动而过度急切地扩缩。
type HPAScalingRules struct {
	// stabilizationWindowSeconds 是扩缩容时考虑历史建议值的秒数。
	// StabilizationWindowSeconds 必须在 0 到 3600（一小时）之间（含两端）。
	// 未设置时使用默认值：
	// - 扩容：0（即不做稳定化处理）。
	// - 缩容：300（即稳定窗口为 300 秒）。
	// +optional
	StabilizationWindowSeconds *int32 `json:"stabilizationWindowSeconds,omitempty" protobuf:"varint,3,opt,name=stabilizationWindowSeconds"`

	// selectPolicy 指定要使用的策略，未设置时默认使用 Max。
	// +optional
	SelectPolicy *ScalingPolicySelect `json:"selectPolicy,omitempty" protobuf:"bytes,1,opt,name=selectPolicy"`

	// policies 是扩缩时可用的候选扩缩策略列表。未设置时使用默认值：
	// - 扩容：允许 Pod 数翻倍，或在 15 秒窗口内绝对增加 4 个 Pod。
	// - 缩容：允许在 15 秒窗口内移除所有 Pod。
	// +listType=atomic
	// +optional
	Policies []HPAScalingPolicy `json:"policies,omitempty" listType:"atomic" protobuf:"bytes,2,rep,name=policies"`

	// tolerance 是当前指标值与期望指标值之比的容差，比值变化不超过该容差时不更新期望副本数
	// （例如 0.01 表示 1%）。必须大于等于零。未设置时应用集群范围的默认容差（默认 10%）。
	//
	// 例如，若自动扩缩配置的内存消耗目标为 100Mi，缩容和扩容容差分别为 5% 和 1%，
	// 则实际消耗低于 95Mi 或超过 101Mi 时才会触发扩缩。
	//
	// +featureGate=HPAConfigurableTolerance
	// +optional
	Tolerance *resource.Quantity `json:"tolerance,omitempty" protobuf:"bytes,4,opt,name=tolerance"`
}

// HPAScalingPolicyType 是做出扩缩决策时可使用策略的类型。
type HPAScalingPolicyType string

const (
	// PodsScalingPolicy 是以 Pod 绝对数量变化来指定的策略。
	PodsScalingPolicy HPAScalingPolicyType = "Pods"
	// PercentScalingPolicy 是相对于当前 Pod 数指定变化幅度的策略。
	PercentScalingPolicy HPAScalingPolicyType = "Percent"
)

// HPAScalingPolicy 是必须在指定历史时间窗口内成立的单个策略。
type HPAScalingPolicy struct {
	// type 用于指定扩缩策略。
	Type HPAScalingPolicyType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=HPAScalingPolicyType"`

	// value 是此策略允许的变化量，必须大于零。
	Value int32 `json:"value" protobuf:"varint,2,opt,name=value"`

	// periodSeconds 指定策略必须成立的时间窗口。PeriodSeconds 必须大于零且不超过 1800（30 分钟）。
	PeriodSeconds int32 `json:"periodSeconds" protobuf:"varint,3,opt,name=periodSeconds"`
}

// MetricSourceType 表示指标的类型。
type MetricSourceType string

const (
	// ObjectMetricSourceType 是描述 Kubernetes 对象的指标
	// （例如 Ingress 对象上的每秒请求数）。
	ObjectMetricSourceType MetricSourceType = "Object"
	// PodsMetricSourceType 是描述当前扩缩目标中每个 Pod 的指标（例如每秒处理的事务数）。
	// 各值会在与目标值比较前取平均值。
	PodsMetricSourceType MetricSourceType = "Pods"
	// ResourceMetricSourceType 是 Kubernetes 已知的资源指标，如 requests 和 limits 中指定的，
	// 描述当前扩缩目标中的每个 Pod（如 CPU 或内存）。这类指标是 Kubernetes 内置的，
	// 相比普通单 Pod 指标（"pods" 源）有额外的扩缩选项。
	ResourceMetricSourceType MetricSourceType = "Resource"
	// ContainerResourceMetricSourceType 是 Kubernetes 已知的资源指标，如 requests 和 limits 中
	// 指定的，描述当前扩缩目标中每个 Pod 内的单个容器（如 CPU 或内存）。这类指标是
	// Kubernetes 内置的，相比普通单 Pod 指标（"pods" 源）有额外的扩缩选项。
	ContainerResourceMetricSourceType MetricSourceType = "ContainerResource"
	// ExternalMetricSourceType 是不与任何 Kubernetes 对象关联的全局指标。它支持基于
	// 集群外部组件的信息进行自动扩缩（例如云消息服务中的队列长度，或集群外部负载均衡器的 QPS）。
	ExternalMetricSourceType MetricSourceType = "External"
)

// ObjectMetricSource 指示如何基于描述 Kubernetes 对象的指标进行扩缩
// （例如 Ingress 对象上的每秒请求数）。
type ObjectMetricSource struct {
	// describedObject 指定对象的描述信息，如 kind、name、apiVersion。
	DescribedObject CrossVersionObjectReference `json:"describedObject" protobuf:"bytes,1,name=describedObject"`

	// target 指定该指标的目标值。
	Target MetricTarget `json:"target" protobuf:"bytes,2,name=target"`

	// metric 通过名称和选择器标识目标指标。
	Metric MetricIdentifier `json:"metric" protobuf:"bytes,3,name=metric"`
}

// PodsMetricSource 指示如何基于描述当前扩缩目标中每个 Pod 的指标进行扩缩
// （例如每秒处理的事务数）。各值会在与目标值比较前取平均值。
type PodsMetricSource struct {
	// metric 通过名称和选择器标识目标指标。
	Metric MetricIdentifier `json:"metric" protobuf:"bytes,1,name=metric"`

	// target 指定该指标的目标值。
	Target MetricTarget `json:"target" protobuf:"bytes,2,name=target"`
}

// ResourceMetricSource 指示如何基于 Kubernetes 已知的资源指标（如 requests 和 limits 中
// 指定的资源）进行扩缩，该指标描述当前扩缩目标中的每个 Pod（如 CPU 或内存）。各值会在与
// 目标比较前取平均值。这类指标是 Kubernetes 内置的，相比使用 "pods" 源的普通单 Pod 指标
// 有额外的扩缩选项。只能设置一种 "target" 类型。
type ResourceMetricSource struct {
	// name 是所讨论资源的名称。
	Name v1.ResourceName `json:"name" protobuf:"bytes,1,name=name"`

	// target 指定该指标的目标值。
	Target MetricTarget `json:"target" protobuf:"bytes,2,name=target"`
}

// ContainerResourceMetricSource 指示如何基于 Kubernetes 已知的资源指标（如 requests 和
// limits 中指定的资源）进行扩缩，该指标描述当前扩缩目标中每个 Pod 内的单个容器
// （如 CPU 或内存）。各值会在与目标比较前取平均值。这类指标是 Kubernetes 内置的，
// 相比使用 "pods" 源的普通单 Pod 指标有额外的扩缩选项。只能设置一种 "target" 类型。
type ContainerResourceMetricSource struct {
	// name 是所讨论资源的名称。
	Name v1.ResourceName `json:"name" protobuf:"bytes,1,name=name"`

	// target 指定该指标的目标值。
	Target MetricTarget `json:"target" protobuf:"bytes,2,name=target"`

	// container 是扩缩目标各 Pod 中容器的名称。
	Container string `json:"container" protobuf:"bytes,3,opt,name=container"`
}

// ExternalMetricSource 指示如何基于不与任何 Kubernetes 对象关联的指标进行扩缩
// （例如云消息服务中的队列长度，或集群外部负载均衡器的 QPS）。
type ExternalMetricSource struct {
	// metric 通过名称和选择器标识目标指标。
	Metric MetricIdentifier `json:"metric" protobuf:"bytes,1,name=metric"`

	// target 指定该指标的目标值。
	Target MetricTarget `json:"target" protobuf:"bytes,2,name=target"`
}

// MetricIdentifier 定义指标的名称以及可选的选择器。
type MetricIdentifier struct {
	// name 是给定指标的名称。
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// selector 是给定指标的标准 Kubernetes 标签选择器的字符串编码形式。
	// 设置后会作为附加参数传给指标服务器，以更精确地限定指标范围。
	// 未设置时仅使用 metricName 收集指标。
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,2,name=selector"`
}

// MetricTarget 定义特定指标的目标值、平均值或平均利用率。
type MetricTarget struct {
	// type 表示指标类型是 Utilization、Value 还是 AverageValue。
	Type MetricTargetType `json:"type" protobuf:"bytes,1,name=type"`

	// value 是指标的目标值（以 Quantity 表示）。
	// +optional
	Value *resource.Quantity `json:"value,omitempty" protobuf:"bytes,2,opt,name=value"`

	// averageValue 是所有相关 Pod 上该指标平均值的目标值（以 Quantity 表示）。
	// +optional
	AverageValue *resource.Quantity `json:"averageValue,omitempty" protobuf:"bytes,3,opt,name=averageValue"`

	// averageUtilization 是所有相关 Pod 上该资源指标平均值的目标值，
	// 表示为 Pod 请求资源值的百分比。目前仅对 Resource 指标源类型有效。
	// +optional
	AverageUtilization *int32 `json:"averageUtilization,omitempty" protobuf:"bytes,4,opt,name=averageUtilization"`
}

// MetricTargetType 指定所针对指标的类型，应为 "Value"、"AverageValue" 或
// "Utilization" 之一。
type MetricTargetType string

const (
	// UtilizationMetricType 声明 MetricTarget 是 AverageUtilization 值。
	UtilizationMetricType MetricTargetType = "Utilization"
	// ValueMetricType 声明 MetricTarget 是原始值。
	ValueMetricType MetricTargetType = "Value"
	// AverageValueMetricType 声明 MetricTarget 是
	AverageValueMetricType MetricTargetType = "AverageValue"
)

// HorizontalPodAutoscalerStatus 描述水平 Pod 自动扩缩器的当前状态。
type HorizontalPodAutoscalerStatus struct {
	// observedGeneration 是此自动扩缩器观测到的最新 generation。
	// +optional
	ObservedGeneration *int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// lastScaleTime 是 HorizontalPodAutoscaler 上次扩缩 Pod 数量的时间，
	// 自动扩缩器据此控制 Pod 数量变更的频率。
	// +optional
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty" protobuf:"bytes,2,opt,name=lastScaleTime"`

	// currentReplicas 是此自动扩缩器管理的 Pod 当前副本数（自动扩缩器最后观测到的值）。
	// +optional
	CurrentReplicas int32 `json:"currentReplicas,omitempty" protobuf:"varint,3,opt,name=currentReplicas"`

	// desiredReplicas 是此自动扩缩器管理的 Pod 期望副本数（自动扩缩器最后计算出的值）。
	DesiredReplicas int32 `json:"desiredReplicas" protobuf:"varint,4,opt,name=desiredReplicas"`

	// currentMetrics 是此自动扩缩器所用指标的最后读取状态。
	// +listType=atomic
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	CurrentMetrics []MetricStatus `json:"currentMetrics" protobuf:"bytes,5,rep,name=currentMetrics"`

	// conditions 是此自动扩缩器扩缩目标所需满足的条件集合，并指示这些条件是否达成。
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []HorizontalPodAutoscalerCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" listType:"map" protobuf:"bytes,6,rep,name=conditions"`
}

// HorizontalPodAutoscalerConditionType 是 HorizontalPodAutoscaler 的有效条件类型。
type HorizontalPodAutoscalerConditionType string

const (
	// ScalingActive 表示 HPA 控制器在需要时能够扩缩：配置正确、能获取所需指标且未被禁用。
	ScalingActive HorizontalPodAutoscalerConditionType = "ScalingActive"
	// AbleToScale 表示不存在阻碍扩缩的瞬时问题，例如处于退避窗口，或无法访问/更新目标 scale。
	AbleToScale HorizontalPodAutoscalerConditionType = "AbleToScale"
	// ScalingLimited 表示根据指标计算出的扩缩值超出 HPA 的范围，因而已被限制。
	ScalingLimited HorizontalPodAutoscalerConditionType = "ScalingLimited"
	// ScaledToZero 表示 HPA 控制器已将工作负载缩容至零。
	ScaledToZero HorizontalPodAutoscalerConditionType = "ScaledToZero"
)

// HorizontalPodAutoscalerCondition 描述某时刻 HorizontalPodAutoscaler 的状态。
type HorizontalPodAutoscalerCondition struct {
	// type 描述当前条件。
	Type HorizontalPodAutoscalerConditionType `json:"type" protobuf:"bytes,1,name=type"`

	// status 是条件的状态（True、False、Unknown）。
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,name=status"`

	// lastTransitionTime 是条件上次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`

	// reason 是条件上次状态转变的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`

	// message 是对该转变详情的人类可读说明。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`

	// observedGeneration 表示设置此条件所依据的 .metadata.generation。
	// 例如当前 .metadata.generation 为 12，而 .status.conditions[x].observedGeneration 为 9，
	// 则说明该条件相对于实例的当前状态已过期。
	// +optional
	ObservedGeneration *int64 `json:"observedGeneration,omitempty" protobuf:"varint,6,opt,name=observedGeneration"`
}

// MetricStatus 描述单个指标的最后读取状态。
type MetricStatus struct {
	// type 是指标源类型，取值为 "ContainerResource"、"External"、"Object"、
	// "Pods" 或 "Resource" 之一，各自对应对象中的一个字段。
	Type MetricSourceType `json:"type" protobuf:"bytes,1,name=type"`

	// object 指描述单个 Kubernetes 对象的指标（例如 Ingress 对象上的每秒请求数）。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:opaqueType
	Object *ObjectMetricStatus `json:"object,omitempty" protobuf:"bytes,2,opt,name=object"`

	// pods 指描述当前扩缩目标中每个 Pod 的指标（例如每秒处理的事务数）。
	// 各值会先取平均值，再与目标值比较。
	// +optional
	Pods *PodsMetricStatus `json:"pods,omitempty" protobuf:"bytes,3,opt,name=pods"`

	// resource 指 Kubernetes 已知的资源指标（如 requests 和 limits 中指定的资源），
	// 描述当前扩缩目标中的每个 Pod（如 CPU 或内存）。这类指标是 Kubernetes 内置的，
	// 相比使用 "pods" 源的普通单 Pod 指标有额外的扩缩选项。
	// +optional
	Resource *ResourceMetricStatus `json:"resource,omitempty" protobuf:"bytes,4,opt,name=resource"`

	// containerResource 指 Kubernetes 已知的资源指标（如 requests 和 limits 中指定的），
	// 描述当前扩缩目标中每个 Pod 内的单个容器（如 CPU 或内存）。这类指标是 Kubernetes 内置的，
	// 相比使用 "pods" 源的普通单 Pod 指标有额外的扩缩选项。
	// +optional
	ContainerResource *ContainerResourceMetricStatus `json:"containerResource,omitempty" protobuf:"bytes,7,opt,name=containerResource"`

	// external 指不与任何 Kubernetes 对象关联的全局指标。它支持基于集群外部组件的信息
	// 进行自动扩缩（例如云消息服务中的队列长度，或集群外部负载均衡器的 QPS）。
	// +optional
	External *ExternalMetricStatus `json:"external,omitempty" protobuf:"bytes,5,opt,name=external"`
}

// ObjectMetricStatus 指示描述 Kubernetes 对象的指标的当前值
// （例如 Ingress 对象上的每秒请求数）。
type ObjectMetricStatus struct {
	// metric 通过名称和选择器标识目标指标。
	Metric MetricIdentifier `json:"metric" protobuf:"bytes,1,name=metric"`

	// current 包含该指标的当前值。
	Current MetricValueStatus `json:"current" protobuf:"bytes,2,name=current"`

	// describedObject 指定对象的描述信息，如 kind、name、apiVersion。
	DescribedObject CrossVersionObjectReference `json:"describedObject" protobuf:"bytes,3,name=describedObject"`
}

// PodsMetricStatus 指示描述当前扩缩目标中每个 Pod 的指标的当前值
// （例如每秒处理的事务数）。
type PodsMetricStatus struct {
	// metric 通过名称和选择器标识目标指标。
	Metric MetricIdentifier `json:"metric" protobuf:"bytes,1,name=metric"`

	// current 包含该指标的当前值。
	Current MetricValueStatus `json:"current" protobuf:"bytes,2,name=current"`
}

// ResourceMetricStatus 指示 Kubernetes 已知的资源指标的当前值，如 requests 和 limits 中
// 指定的，描述当前扩缩目标中的每个 Pod（如 CPU 或内存）。这类指标是 Kubernetes 内置的，
// 相比使用 "pods" 源的普通单 Pod 指标有额外的扩缩选项。
type ResourceMetricStatus struct {
	// name 是所讨论资源的名称。
	Name v1.ResourceName `json:"name" protobuf:"bytes,1,name=name"`

	// current 包含该指标的当前值。
	Current MetricValueStatus `json:"current" protobuf:"bytes,2,name=current"`
}

// ContainerResourceMetricStatus 指示 Kubernetes 已知的资源指标的当前值，如 requests 和
// limits 中指定的，描述当前扩缩目标中每个 Pod 内的单个容器（如 CPU 或内存）。这类指标是
// Kubernetes 内置的，相比使用 "pods" 源的普通单 Pod 指标有额外的扩缩选项。
type ContainerResourceMetricStatus struct {
	// name 是所讨论资源的名称。
	Name v1.ResourceName `json:"name" protobuf:"bytes,1,name=name"`

	// current 包含该指标的当前值。
	Current MetricValueStatus `json:"current" protobuf:"bytes,2,name=current"`

	// container 是扩缩目标各 Pod 中容器的名称。
	Container string `json:"container" protobuf:"bytes,3,opt,name=container"`
}

// ExternalMetricStatus 指示不与任何 Kubernetes 对象关联的全局指标的当前值。
type ExternalMetricStatus struct {
	// metric 通过名称和选择器标识目标指标。
	Metric MetricIdentifier `json:"metric" protobuf:"bytes,1,name=metric"`

	// current 包含该指标的当前值。
	Current MetricValueStatus `json:"current" protobuf:"bytes,2,name=current"`
}

// MetricValueStatus 保存指标的当前值。
type MetricValueStatus struct {
	// value 是指标的当前值（以 Quantity 表示）。
	// +optional
	Value *resource.Quantity `json:"value,omitempty" protobuf:"bytes,1,opt,name=value"`

	// averageValue 是所有相关 Pod 上该指标平均值的当前值（以 Quantity 表示）。
	// +optional
	AverageValue *resource.Quantity `json:"averageValue,omitempty" protobuf:"bytes,2,opt,name=averageValue"`

	// averageUtilization 是所有相关 Pod 上该资源指标平均值的当前值，
	// 表示为 Pod 请求资源值的百分比。
	// +optional
	AverageUtilization *int32 `json:"averageUtilization,omitempty" protobuf:"bytes,3,opt,name=averageUtilization"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.23

// HorizontalPodAutoscalerList 是水平 Pod 自动扩缩器对象的列表。
type HorizontalPodAutoscalerList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是水平 Pod 自动扩缩器对象的列表。
	Items []HorizontalPodAutoscaler `json:"items" protobuf:"bytes,2,rep,name=items"`
}
