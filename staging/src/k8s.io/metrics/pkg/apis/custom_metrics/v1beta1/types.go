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

package v1beta1

import (
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricValueList is a list of values for a given metric for some set of objects
// 中文说明：同一指标在一组对象上的取值集合，批量查询时每项对应一个对象。
type MetricValueList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// the value of the metric across the described objects
	// 中文说明：列表项通过 DescribedObject 指回具体对象，调用方据此对齐标签与用量。
	Items []MetricValue `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricValue is a metric value for some object
// 中文说明：一个对象的一条指标记录，自带对象引用、时间和标签选择器以便回溯来源。
type MetricValue struct {
	metav1.TypeMeta `json:""`

	// a reference to the described object
	// 中文说明：ObjectReference 比裸名字信息更全，可区分同名但不同 Kind 的对象。
	DescribedObject v1.ObjectReference `json:"describedObject" protobuf:"bytes,1,name=describedObject"`

	// the name of the metric
	// 中文说明：与 Selector 配合唯一确定一条指标，仅靠名字可能命中多条时间序列。
	MetricName string `json:"metricName" protobuf:"bytes,2,name=metricName"`

	// indicates the time at which the metrics were produced
	// 中文说明：这是采样时刻，不等于查询时刻；HPA 等消费者据此判断数据新鲜度。
	Timestamp metav1.Time `json:"timestamp" protobuf:"bytes,3,name=timestamp"`

	// indicates the window ([Timestamp-Window, Timestamp]) from
	// which these metrics were calculated, when returning rate
	// metrics calculated from cumulative metrics (or zero for
	// non-calculated instantaneous metrics).
	// 中文说明：只有速率类指标才需要窗口；瞬时指标为零，故用指针区分“未设置”和零值。
	WindowSeconds *int64 `json:"window,omitempty" protobuf:"bytes,4,opt,name=window"`

	// the value of the metric for this
	// 中文说明：Quantity 保留精度，避免浮点误差影响阈值比较。
	Value resource.Quantity `json:"value" protobuf:"bytes,5,name=value"`

	// selector represents the label selector that could be used to select
	// this metric, and will generally just be the selector passed in to
	// the query used to fetch this metric.
	// When left blank, only the metric's Name will be used to gather metrics.
	// 中文说明：回填查询时使用的选择器，便于消费方确认这条数据对应哪组标签维度。
	// 指令 +optional 表示该字段可省略，空值时按仅用 MetricName 匹配处理。
	// +optional
	Selector *metav1.LabelSelector `json:"selector" protobuf:"bytes,6,opt,name=selector"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricListOptions is used to select metrics by their label selectors
// 中文说明：两个选择器各管一件事——对象标签 vs. 指标标签，混淆会导致查错数据。
type MetricListOptions struct {
	metav1.TypeMeta `json:""`

	// A selector to restrict the list of returned objects by their labels.
	// Defaults to everything.
	// 中文说明：筛选被查询对象的标签，空值表示所有对象。
	// 指令 +optional 表示该查询参数可省略。
	// +optional
	LabelSelector string `json:"labelSelector,omitempty" protobuf:"bytes,1,opt,name=labelSelector"`

	// A selector to restrict the list of returned metrics by their labels
	// 中文说明：筛选指标自身的标签维度，用于同名指标下的多时间序列。
	// 指令 +optional 表示该查询参数可省略。
	// +optional
	MetricLabelSelector string `json:"metricLabelSelector,omitempty" protobuf:"bytes,2,opt,name=metricLabelSelector"`
}

// AllObjects is a wildcard used to select metrics
// for all objects matching the given label selector
// 中文说明：作为 URL 里的 Name 段传给 API，表示“按 selector 批量取”，而非字面量对象名。
const AllObjects = "*"
