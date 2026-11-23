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

type MetricIdentifier struct {
	// name is the name of the given metric
	// 名字是查询主键；同名指标可以有多组标签对应的时间序列。
	Name string
	// selector represents the label selector that could be used to select
	// this metric, and will generally just be the selector passed in to
	// the query used to fetch this metric.
	// 省略标签时只按名字取值，提供后才能区分同名指标下的不同序列。
	// +optional
	Selector *metav1.LabelSelector
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// a list of values for a given metric for some set of objects
// 每个元素对应一个对象；空列表表示没有匹配项，不视为错误。
type MetricValueList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty"`

	// the value of the metric across the described objects
	Items []MetricValue `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// a metric value for some object
type MetricValue struct {
	metav1.TypeMeta

	// a reference to the described object
	// 引用类型在本仓库内定义，避免把内部 API 包拖进来形成依赖环。
	DescribedObject ObjectReference

	// 名字加可选标签才能确定一条具体的时间序列。
	Metric MetricIdentifier

	// indicates the time at which the metrics were produced
	// 取服务端采样时刻，而非客户端收到响应的时间。
	Timestamp metav1.Time

	// indicates the window ([Timestamp-Window, Timestamp]) from
	// which these metrics were calculated, when returning rate
	// metrics calculated from cumulative metrics (or zero for
	// non-calculated instantaneous metrics).
	// 速率值只有配上窗口才能还原原始累积量，否则无法二次聚合。
	WindowSeconds *int64

	// the value of the metric for this
	// 用 Quantity 保留精度和单位，避免后端上报值在客户端被提前舍入。
	Value resource.Quantity
}

// allObjects is a wildcard used to select metrics
// for all objects matching the given label selector
// 用显式的 * 表示「全部对象」，比空选择器语义更明确。
const AllObjects = "*"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricListOptions is used to select metrics by their label selectors
// 两个选择器分别作用于被查询对象和指标自身的标签，混用会查不到预期结果。
type MetricListOptions struct {
	metav1.TypeMeta

	// A selector to restrict the list of returned objects by their labels.
	// Defaults to everything.
	// 这里匹配的是 Kubernetes 对象标签，不是指标标签。
	// +optional
	LabelSelector string

	// A selector to restrict the list of returned metrics by their labels
	// 对应指标自身的标签维度，用于区分同名指标的多条序列。
	// +optional
	MetricLabelSelector string
}

// NOTE: ObjectReference is copied from k8s.io/kubernetes/pkg/api/types.go. We
// cannot depend on k8s.io/kubernetes/pkg/api because that creates cyclic
// dependency between k8s.io/metrics and k8s.io/kubernetes. We cannot depend on
// k8s.io/client-go/pkg/api because the package is going to be deprecated soon.
// There is no need to keep it an exact copy. Each repo can define its own
// internal objects.

// ObjectReference contains enough information to let you inspect or modify the referred object.
// 这是一份本地副本，字段不必与上游逐字一致；只保留定位对象所需部分，随本仓库独立演进。
type ObjectReference struct {
	Kind            string
	Namespace       string
	Name            string
	UID             types.UID
	APIVersion      string
	ResourceVersion string
	FieldPath       string
}
