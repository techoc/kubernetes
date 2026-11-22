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

package v1

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CrossVersionObjectReference 提供定位被引用对象所需的完整信息（kind、name、apiVersion）。
// +structType=atomic
type CrossVersionObjectReference struct {
	// 被引对象的 kind。详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +k8s:alpha(since: "1.37")=+k8s:required
	Kind string `json:"kind" protobuf:"bytes,1,opt,name=kind"`

	// 被引对象的 name。详见 https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	// +k8s:alpha(since: "1.37")=+k8s:required
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`

	// 被引对象的 API 版本。
	// +optional
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,3,opt,name=apiVersion"`
}

// HorizontalPodAutoscalerSpec 定义水平 Pod 自动扩缩容的期望行为。
type HorizontalPodAutoscalerSpec struct {
	// 扩缩容目标对象的引用。HPA 读取其资源用量，并通过 Scale 子资源设置期望副本数。
	ScaleTargetRef CrossVersionObjectReference `json:"scaleTargetRef" protobuf:"bytes,1,opt,name=scaleTargetRef"`
	// 自动缩容的副本数下限，默认 1。
	// 开启 alpha 特性门控 HPAScaleToZero 且配置了至少一个 Object 或 External 指标时可为 0；
	// 只要有一个指标可用，扩缩容就会生效。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:ifEnabled(HPAScaleToZero)=+k8s:minimum=0
	// +k8s:beta(since: "1.37")=+k8s:ifDisabled(HPAScaleToZero)=+k8s:minimum=1
	MinReplicas *int32 `json:"minReplicas,omitempty" protobuf:"varint,2,opt,name=minReplicas"`

	// 自动扩容的副本数上限，不得小于 minReplicas。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:minimum=1
	MaxReplicas int32 `json:"maxReplicas" protobuf:"varint,3,opt,name=maxReplicas"`

	// 所有 pod 的平均 CPU 利用率目标（占请求 CPU 的百分比）；未指定时使用默认扩缩容策略。
	// +optional
	TargetCPUUtilizationPercentage *int32 `json:"targetCPUUtilizationPercentage,omitempty" protobuf:"varint,4,opt,name=targetCPUUtilizationPercentage"`
}

// HorizontalPodAutoscalerStatus 记录水平 Pod 自动扩缩容的当前状态。
type HorizontalPodAutoscalerStatus struct {
	// 该自动扩缩容器观测到的最新 generation。
	// +optional
	ObservedGeneration *int64 `json:"observedGeneration,omitempty" protobuf:"varint,1,opt,name=observedGeneration"`

	// 上次扩缩容 pod 数量的时间，用于控制变更频率（冷却）。
	// +optional
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty" protobuf:"bytes,2,opt,name=lastScaleTime"`

	// 当前实际管理的 pod 副本数。
	CurrentReplicas int32 `json:"currentReplicas" protobuf:"varint,3,opt,name=currentReplicas"`

	// 当前期望的 pod 副本数。
	DesiredReplicas int32 `json:"desiredReplicas" protobuf:"varint,4,opt,name=desiredReplicas"`

	// 当前所有 pod 的平均 CPU 利用率（占请求 CPU 的百分比），
	// 例如 70 表示平均每个 pod 使用了其请求 CPU 的 70%。
	// +optional
	CurrentCPUUtilizationPercentage *int32 `json:"currentCPUUtilizationPercentage,omitempty" protobuf:"varint,5,opt,name=currentCPUUtilizationPercentage"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2

// HorizontalPodAutoscaler 定义水平 Pod 自动扩缩容的配置。
// +k8s:supportsSubresource="/status"
type HorizontalPodAutoscaler struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 自动扩缩容的行为规格。详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec HorizontalPodAutoscalerSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 自动扩缩容器当前状态信息。
	// +optional
	Status HorizontalPodAutoscalerStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2

// HorizontalPodAutoscalerList 是 HorizontalPodAutoscaler 对象的列表。
type HorizontalPodAutoscalerList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 为 HorizontalPodAutoscaler 对象列表。
	Items []HorizontalPodAutoscaler `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2
// +k8s:isSubresource="/scale"

// Scale 表示对某一资源的扩缩容请求。
type Scale struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 扩缩容的行为规格。详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec ScaleSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 扩缩容的当前状态，只读。详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status ScaleStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ScaleSpec 描述 scale 子资源的属性。
type ScaleSpec struct {
	// 被扩缩容对象的期望实例数。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +default=0
	// +k8s:beta(since: "1.37")=+k8s:minimum=0
	Replicas int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`
}

// ScaleStatus 表示 scale 子资源的当前状态。
type ScaleStatus struct {
	// 观测到的被扩缩容对象的实际实例数。
	Replicas int32 `json:"replicas" protobuf:"varint,1,opt,name=replicas"`

	// 匹配副本数的 pod 标签查询。以字符串形式表示，避免客户端解析内省，
	// 格式与查询参数语法一致。
	// 详见 https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
	// +optional
	Selector string `json:"selector,omitempty" protobuf:"bytes,2,opt,name=selector"`
}

// 以下类型用于 alpha 版本的指标注解。

// MetricSourceType 表示指标来源类型。
// +enum
type MetricSourceType string

const (
	// 描述某个 Kubernetes 对象的指标（如 Ingress 的每秒请求数）。
	ObjectMetricSourceType MetricSourceType = "Object"
	// 描述扩缩容目标中每个 pod 的指标（如每秒处理事务数）。
	// 各 pod 的值先取平均再与目标值比较。
	PodsMetricSourceType MetricSourceType = "Pods"
	// Kubernetes 内置的资源指标（如 CPU、内存），取自 requests/limits，描述目标中的每个 pod。
	// 除普通 pod 指标（"pods" 来源）的选项外，还有专门的扩缩容配置。
	ResourceMetricSourceType MetricSourceType = "Resource"
	// Kubernetes 内置的资源指标（如 CPU、内存），取自 requests/limits，描述目标中每个 pod 的单个容器。
	// 除普通 pod 指标（"pods" 来源）的选项外，还有专门的扩缩容配置。
	ContainerResourceMetricSourceType MetricSourceType = "ContainerResource"
	// 不关联任何 Kubernetes 对象的全局指标，可基于集群外部组件的信息扩缩容
	// （如云消息服务的队列长度、集群外负载均衡器的 QPS）。
	ExternalMetricSourceType MetricSourceType = "External"
)

// MetricSpec 定义基于单个指标的扩缩容方式
// （只能同时设置 type 和一个与之匹配的字段）。
type MetricSpec struct {
	// 指标来源类型，取值为 "ContainerResource"、"External"、"Object"、"Pods" 或 "Resource"，
	// 分别对应对象中的一个字段。
	Type MetricSourceType `json:"type" protobuf:"bytes,1,name=type"`

	// 描述单个 Kubernetes 对象的指标（如 Ingress 每秒请求数）。
	// +optional
	Object *ObjectMetricSource `json:"object,omitempty" protobuf:"bytes,2,opt,name=object"`

	// 描述扩缩容目标中每个 pod 的指标，各值先取平均再与目标值比较。
	// +optional
	Pods *PodsMetricSource `json:"pods,omitempty" protobuf:"bytes,3,opt,name=pods"`

	// Kubernetes 内置的资源指标（见 requests/limits），描述目标中的每个 pod。
	// 除普通 pod 指标（"pods" 来源）外，还有专门的扩缩容选项。
	// +optional
	Resource *ResourceMetricSource `json:"resource,omitempty" protobuf:"bytes,4,opt,name=resource"`

	// Kubernetes 内置的资源指标（见 requests/limits），描述目标中每个 pod 的单个容器。
	// 除普通 pod 指标（"pods" 来源）外，还有专门的扩缩容选项。
	// +optional
	ContainerResource *ContainerResourceMetricSource `json:"containerResource,omitempty" protobuf:"bytes,7,opt,name=containerResource"`

	// 不关联任何 Kubernetes 对象的全局指标，可基于集群外部组件的信息扩缩容。
	// +optional
	External *ExternalMetricSource `json:"external,omitempty" protobuf:"bytes,5,opt,name=external"`
}

// ObjectMetricSource 定义如何基于描述 Kubernetes 对象的指标扩缩容。
type ObjectMetricSource struct {
	// 被描述的 Kubernetes 对象。
	Target CrossVersionObjectReference `json:"target" protobuf:"bytes,1,name=target"`

	// 指标名称。
	MetricName string `json:"metricName" protobuf:"bytes,2,name=metricName"`

	// 指标的目标值。
	TargetValue resource.Quantity `json:"targetValue" protobuf:"bytes,3,name=targetValue"`

	// 该指标的标准标签选择器字符串形式。设置后作为附加参数传给 metrics server 以缩小范围；
	// 未设置时仅按 metricName 采集指标。
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,4,name=selector"`

	// 所有相关 pod 该指标平均值的目标值。
	// +optional
	AverageValue *resource.Quantity `json:"averageValue,omitempty" protobuf:"bytes,5,name=averageValue"`
}

// PodsMetricSource 定义如何基于描述扩缩容目标中每个 pod 的指标扩缩容，
// 各 pod 的值先取平均再与目标值比较。
type PodsMetricSource struct {
	// 指标名称。
	MetricName string `json:"metricName" protobuf:"bytes,1,name=metricName"`

	// 所有相关 pod 该指标平均值的目标值。
	TargetAverageValue resource.Quantity `json:"targetAverageValue" protobuf:"bytes,2,name=targetAverageValue"`

	// 指标标签选择器的字符串形式。设置后传给 metrics server 缩小采集范围；
	// 未设置时仅按 metricName 采集。
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,3,name=selector"`
}

// ResourceMetricSource 定义如何基于 Kubernetes 内置资源指标（见 requests/limits）扩缩容，
// 各 pod 的值先取平均再与目标值比较。除普通 pod 指标外还有专用扩缩容选项。
// 只能设置一种目标类型。
type ResourceMetricSource struct {
	// 资源名称。
	Name v1.ResourceName `json:"name" protobuf:"bytes,1,name=name"`

	// 所有相关 pod 的资源指标平均值目标，表示为资源请求量的百分比。
	// +optional
	TargetAverageUtilization *int32 `json:"targetAverageUtilization,omitempty" protobuf:"varint,2,opt,name=targetAverageUtilization"`

	// 所有相关 pod 的资源指标平均值目标，为原始值而非请求量百分比，类似 "pods" 指标来源。
	// +optional
	TargetAverageValue *resource.Quantity `json:"targetAverageValue,omitempty" protobuf:"bytes,3,opt,name=targetAverageValue"`
}

// ContainerResourceMetricSource 定义如何基于内置资源指标扩缩容，
// 指标取自 requests/limits，描述目标中每个 pod 的单个容器。
// 各值先取平均再与目标值比较，只能设置一种目标类型。
type ContainerResourceMetricSource struct {
	// 资源名称。
	Name v1.ResourceName `json:"name" protobuf:"bytes,1,name=name"`

	// 所有相关 pod 的资源指标平均值目标，表示为资源请求量的百分比。
	// +optional
	TargetAverageUtilization *int32 `json:"targetAverageUtilization,omitempty" protobuf:"varint,2,opt,name=targetAverageUtilization"`

	// 所有相关 pod 的资源指标平均值目标，为原始值而非请求量百分比，类似 "pods" 指标来源。
	// +optional
	TargetAverageValue *resource.Quantity `json:"targetAverageValue,omitempty" protobuf:"bytes,3,opt,name=targetAverageValue"`

	// 扩缩容目标中 pod 内的容器名。
	Container string `json:"container" protobuf:"bytes,5,opt,name=container"`
}

// ExternalMetricSource 定义如何基于不关联 Kubernetes 对象的全局指标扩缩容。
type ExternalMetricSource struct {
	// 指标名称。
	MetricName string `json:"metricName" protobuf:"bytes,1,name=metricName"`

	// 用于在给定指标中定位特定时间序列。
	// +optional
	MetricSelector *metav1.LabelSelector `json:"metricSelector,omitempty" protobuf:"bytes,2,opt,name=metricSelector"`

	// 指标的目标值，与 targetAverageValue 互斥。
	// +optional
	TargetValue *resource.Quantity `json:"targetValue,omitempty" protobuf:"bytes,3,opt,name=targetValue"`

	// 全局指标的每 pod 目标平均值，与 targetValue 互斥。
	// +optional
	TargetAverageValue *resource.Quantity `json:"targetAverageValue,omitempty" protobuf:"bytes,4,opt,name=targetAverageValue"`
}

// MetricStatus 描述单个指标最近一次读取的状态。
type MetricStatus struct {
	// 指标来源类型，取值同上，分别对应对象中的一个字段。
	Type MetricSourceType `json:"type" protobuf:"bytes,1,name=type"`

	// 描述单个 Kubernetes 对象的指标（如 Ingress 每秒请求数）。
	// +optional
	Object *ObjectMetricStatus `json:"object,omitempty" protobuf:"bytes,2,opt,name=object"`

	// 描述扩缩容目标中每个 pod 的指标，各值先取平均再与目标值比较。
	// +optional
	Pods *PodsMetricStatus `json:"pods,omitempty" protobuf:"bytes,3,opt,name=pods"`

	// Kubernetes 内置的资源指标（见 requests/limits），描述目标中的每个 pod。
	// 除普通 pod 指标（"pods" 来源）外，还有专门的扩缩容选项。
	// +optional
	Resource *ResourceMetricStatus `json:"resource,omitempty" protobuf:"bytes,4,opt,name=resource"`

	// Kubernetes 内置的资源指标（见 requests/limits），描述目标中每个 pod 的单个容器。
	// 除普通 pod 指标（"pods" 来源）外，还有专门的扩缩容选项。
	// +optional
	ContainerResource *ContainerResourceMetricStatus `json:"containerResource,omitempty" protobuf:"bytes,7,opt,name=containerResource"`

	// 不关联任何 Kubernetes 对象的全局指标，可基于集群外部组件的信息扩缩容。
	// +optional
	External *ExternalMetricStatus `json:"external,omitempty" protobuf:"bytes,5,opt,name=external"`
}

// HorizontalPodAutoscalerConditionType 是 HPA 的合法条件类型。
type HorizontalPodAutoscalerConditionType string

const (
	// HPA 控制器配置正确、可获取所需指标且未被禁用，必要时可执行扩缩容。
	ScalingActive HorizontalPodAutoscalerConditionType = "ScalingActive"
	// 不存在阻碍扩缩容的临时问题（如处于退避窗口、无法访问或更新目标 Scale）。
	AbleToScale HorizontalPodAutoscalerConditionType = "AbleToScale"
	// 按指标计算出的副本数超出 HPA 范围，已被限制到边界值。
	ScalingLimited HorizontalPodAutoscalerConditionType = "ScalingLimited"
	// HPA 控制器已将工作负载缩容至零。
	ScaledToZero HorizontalPodAutoscalerConditionType = "ScaledToZero"
)

// HorizontalPodAutoscalerCondition 描述某一时刻 HPA 的状态。
type HorizontalPodAutoscalerCondition struct {
	// 当前条件类型。
	Type HorizontalPodAutoscalerConditionType `json:"type" protobuf:"bytes,1,name=type"`

	// 条件状态，取值为 True、False 或 Unknown。
	Status v1.ConditionStatus `json:"status" protobuf:"bytes,2,name=status"`

	// 条件上次发生状态变更的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`

	// 条件上次状态变更的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`

	// 可读的变更详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`

	// 设置该条件时依据的 .metadata.generation。
	// 例如当前 generation 为 12 而条件中的 observedGeneration 为 9，说明该条件已过期。
	// +optional
	ObservedGeneration *int64 `json:"observedGeneration,omitempty" protobuf:"varint,6,opt,name=observedGeneration"`
}

// ObjectMetricStatus 描述 Kubernetes 对象指标（如 Ingress 每秒请求数）的当前值。
type ObjectMetricStatus struct {
	// 被描述的 Kubernetes 对象。
	Target CrossVersionObjectReference `json:"target" protobuf:"bytes,1,name=target"`

	// 指标名称。
	MetricName string `json:"metricName" protobuf:"bytes,2,name=metricName"`

	// 指标当前值。
	CurrentValue resource.Quantity `json:"currentValue" protobuf:"bytes,3,name=currentValue"`

	// 指标标签选择器的字符串形式。在 ObjectMetricSource 中设置时会传给 metrics server 缩小采集范围，
	// 未设置时仅按 metricName 采集。
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,4,name=selector"`

	// 所有相关 pod 该指标平均值的当前值。
	// +optional
	AverageValue *resource.Quantity `json:"averageValue,omitempty" protobuf:"bytes,5,name=averageValue"`
}

// PodsMetricStatus 描述目标中每个 pod 指标（如每秒处理事务数）的当前值。
type PodsMetricStatus struct {
	// 指标名称。
	MetricName string `json:"metricName" protobuf:"bytes,1,name=metricName"`

	// 所有相关 pod 该指标平均值的当前值。
	CurrentAverageValue resource.Quantity `json:"currentAverageValue" protobuf:"bytes,2,name=currentAverageValue"`

	// 指标标签选择器的字符串形式。在 PodsMetricSource 中设置时会传给 metrics server 缩小采集范围，
	// 未设置时仅按 metricName 采集。
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,3,name=selector"`
}

// ResourceMetricStatus 描述内置资源指标（见 requests/limits）的当前值，针对目标中的每个 pod。
type ResourceMetricStatus struct {
	// 资源名称。
	Name v1.ResourceName `json:"name" protobuf:"bytes,1,name=name"`

	// 所有相关 pod 的平均资源利用率，表示为资源请求量的百分比。
	// 仅当对应指标规格设置了 targetAverageValue 时才出现。
	// +optional
	CurrentAverageUtilization *int32 `json:"currentAverageUtilization,omitempty" protobuf:"bytes,2,opt,name=currentAverageUtilization"`

	// 所有相关 pod 的平均资源指标值，为原始值而非请求量百分比，类似 "pods" 指标来源。
	// 无论指标规格如何，该字段始终存在。
	CurrentAverageValue resource.Quantity `json:"currentAverageValue" protobuf:"bytes,3,name=currentAverageValue"`
}

// ContainerResourceMetricStatus 描述目标中每个 pod 单个容器的内置资源指标当前值。
type ContainerResourceMetricStatus struct {
	// 资源名称。
	Name v1.ResourceName `json:"name" protobuf:"bytes,1,name=name"`

	// 所有相关 pod 的平均资源利用率，表示为资源请求量的百分比。
	// 仅当对应指标规格设置了 targetAverageValue 时才出现。
	// +optional
	CurrentAverageUtilization *int32 `json:"currentAverageUtilization,omitempty" protobuf:"bytes,2,opt,name=currentAverageUtilization"`

	// 所有相关 pod 的平均资源指标值，为原始值而非请求量百分比，类似 "pods" 指标来源。
	// 无论指标规格如何，该字段始终存在。
	CurrentAverageValue resource.Quantity `json:"currentAverageValue" protobuf:"bytes,3,name=currentAverageValue"`

	// 扩缩容目标中 pod 内的容器名。
	Container string `json:"container" protobuf:"bytes,4,opt,name=container"`
}

// ExternalMetricStatus 描述不关联 Kubernetes 对象的全局指标的当前值。
type ExternalMetricStatus struct {
	// 指标系统中用于自动扩缩容的指标名称。
	MetricName string `json:"metricName" protobuf:"bytes,1,name=metricName"`

	// 用于在给定指标中定位特定时间序列。
	// +optional
	MetricSelector *metav1.LabelSelector `json:"metricSelector,omitempty" protobuf:"bytes,2,opt,name=metricSelector"`
	// 指标当前值。
	CurrentValue resource.Quantity `json:"currentValue" protobuf:"bytes,3,name=currentValue"`

	// 在所有被扩缩容 pod 上取平均后的指标当前值。
	// +optional
	CurrentAverageValue *resource.Quantity `json:"currentAverageValue,omitempty" protobuf:"bytes,4,opt,name=currentAverageValue"`
}
