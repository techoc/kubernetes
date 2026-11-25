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

package custom_metrics

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// MetricIdentifier 通过名称和可选标签选择器标识一个指标。
type MetricIdentifier struct {
	// 指标名称。
	// 名字是查询主键，同名指标可以有多组标签对应的时间序列。
	Name string
	// 用于选择该指标的标签选择器，通常就是查询该指标时传入的选择器。
	// 省略标签时只按名字取值，提供后才能区分同名指标下的不同序列。
	// +optional
	Selector *metav1.LabelSelector
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricValueList 是一组对象上同一指标的取值列表。
// 每个元素对应一个对象，空列表表示没有匹配项，不视为错误。
type MetricValueList struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	metav1.ListMeta `json:"metadata,omitempty"`

	// 所描述对象的指标取值。
	Items []MetricValue `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricValue 是某个对象的一条指标记录。
type MetricValue struct {
	// API 对象的类型元数据。
	metav1.TypeMeta

	// 所描述对象的引用。
	// 引用类型在本仓库内定义，避免把内部 API 包拖进来形成依赖环。
	DescribedObject ObjectReference

	// 名字加可选标签才能确定一条具体的时间序列。
	Metric MetricIdentifier

	// 指标产生的时间。
	// 取服务端采样时刻，而非客户端收到响应的时间。
	Timestamp metav1.Time

	// 指标计算所使用的时间区间 [Timestamp-Window, Timestamp]。
	// 返回由累计指标计算出的速率时提供该窗口；非计算的瞬时指标为零。
	// 速率值只有配上窗口才能还原原始累积量，否则无法二次聚合。
	WindowSeconds *int64

	// 该对象的指标值。
	// 用 Quantity 保留精度和单位，避免后端上报值在客户端被提前舍入。
	Value resource.Quantity
}

// AllObjects 是用于选择指标的通配符，匹配给定标签选择器下的所有对象。
// 用显式的 * 表示“全部对象”，比空选择器语义更明确。
const AllObjects = "*"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricListOptions 用于通过标签选择器筛选指标。
// 两个选择器分别作用于被查询对象和指标自身的标签，混用会查不到预期结果。
type MetricListOptions struct {
	// API 对象的类型元数据。
	metav1.TypeMeta

	// 用于按标签限制返回对象列表的选择器，默认匹配所有对象。
	// 这里匹配的是 Kubernetes 对象标签，不是指标标签。
	// +optional
	LabelSelector string

	// 用于按标签限制返回指标列表的选择器。
	// 对应指标自身的标签维度，用于区分同名指标的多条序列。
	// +optional
	MetricLabelSelector string
}

// ObjectReference 的定义复制自 k8s.io/kubernetes/pkg/api/types.go。
// 不能依赖 k8s.io/kubernetes/pkg/api，否则会造成 k8s.io/metrics 与
// k8s.io/kubernetes 之间的循环依赖；也不能依赖即将弃用的
// k8s.io/client-go/pkg/api。各仓库可独立定义所需字段，无需保持完全一致。

// ObjectReference 包含足以查看或修改所引用对象的信息。
type ObjectReference struct {
	// 对象类型。
	Kind string
	// 对象所在的命名空间。
	Namespace string
	// 对象名称。
	Name string
	// 对象唯一标识。
	UID types.UID
	// 对象的 API 版本。
	APIVersion string
	// 对象资源版本，可用于并发控制。
	ResourceVersion string
	// 对象内字段的路径，用于引用子字段。
	FieldPath string
}
