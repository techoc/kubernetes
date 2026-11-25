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

// MetricValueList 是一组对象上同一指标的取值列表。
// 批量查询时每个元素对应一个对象。
type MetricValueList struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 所描述对象的指标取值。
	// 列表项通过 DescribedObject 指回具体对象，调用方据此对齐标签与用量。
	Items []MetricValue `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricValue 是某个对象的一条指标记录。
// 记录自带对象引用、采样时间和标签选择器，便于回溯来源。
type MetricValue struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`

	// 所描述对象的引用。
	// ObjectReference 比裸名字信息更全，可区分同名但不同 Kind 的对象。
	DescribedObject v1.ObjectReference `json:"describedObject" protobuf:"bytes,1,name=describedObject"`

	// 指标名称。
	// 与 Selector 配合唯一确定一条指标，仅靠名字可能命中多条时间序列。
	MetricName string `json:"metricName" protobuf:"bytes,2,name=metricName"`

	// 指标产生的时间。
	// 这是采样时刻，不等于查询时刻；HPA 等消费者据此判断数据新鲜度。
	Timestamp metav1.Time `json:"timestamp" protobuf:"bytes,3,name=timestamp"`

	// 指标计算所使用的时间区间 [Timestamp-Window, Timestamp]。
	// 返回由累计指标计算出的速率时提供该窗口；非计算的瞬时指标为零。
	// 只有速率类指标才需要窗口，指针用于区分“未设置”和零值。
	WindowSeconds *int64 `json:"window,omitempty" protobuf:"bytes,4,opt,name=window"`

	// 该对象的指标值。
	// Quantity 保留精度，避免浮点误差影响阈值比较。
	Value resource.Quantity `json:"value" protobuf:"bytes,5,name=value"`

	// 用于选择该指标的标签选择器，通常就是查询该指标时传入的选择器。
	// 留空时仅使用 MetricName 收集指标；回填该字段便于消费方确认对应的标签维度。
	// +optional
	Selector *metav1.LabelSelector `json:"selector" protobuf:"bytes,6,opt,name=selector"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricListOptions 用于通过标签选择器筛选指标。
// 两个选择器分别作用于被查询对象和指标自身的标签，混用会查不到预期结果。
type MetricListOptions struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`

	// 用于按标签限制返回对象列表的选择器，默认匹配所有对象。
	// 这里匹配的是 Kubernetes 对象标签，不是指标标签。
	// +optional
	LabelSelector string `json:"labelSelector,omitempty" protobuf:"bytes,1,opt,name=labelSelector"`

	// 用于按标签限制返回指标列表的选择器。
	// 对应指标自身的标签维度，用于区分同名指标的多条序列。
	// +optional
	MetricLabelSelector string `json:"metricLabelSelector,omitempty" protobuf:"bytes,2,opt,name=metricLabelSelector"`
}

// AllObjects 是用于选择指标的通配符，匹配给定标签选择器下的所有对象。
// 作为 API 请求中的 Name 段传递，表示批量获取，而非字面量对象名。
const AllObjects = "*"
