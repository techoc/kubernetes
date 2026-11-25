/*
Copyright 2018 The Kubernetes Authors.

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

package v1beta2

import (
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MetricIdentifier 通过名称和可选标签选择器标识一个指标。
type MetricIdentifier struct {
	// 指标名称。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// 用于选择该指标的标签选择器，通常就是查询该指标时传入的选择器。
	// 标签留空时只按名字取数，与内部版本保持一致。
	// +optional
	Selector *metav1.LabelSelector `json:"selector" protobuf:"bytes,2,opt,name=selector"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricValueList 是一组对象上同一指标的取值列表。
// 每个元素对应一个对象，空列表表示无匹配项。
type MetricValueList struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 所描述对象的指标取值。
	Items []MetricValue `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricValue 是某个对象的指标值。
type MetricValue struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`

	// 所描述对象的引用。
	// 直接复用 core/v1 的引用类型，线上格式与 Kubernetes 对象引用保持一致。
	DescribedObject v1.ObjectReference `json:"describedObject" protobuf:"bytes,1,name=describedObject"`

	// 通过名称和可选标签选择器标识该指标。
	Metric MetricIdentifier `json:"metric" protobuf:"bytes,2,name=metric"`

	// 指标产生的时间。
	Timestamp metav1.Time `json:"timestamp" protobuf:"bytes,3,name=timestamp"`

	// 指标计算所使用的时间区间 [Timestamp-Window, Timestamp]。
	// 返回由累计指标计算出的速率时提供该窗口；非计算的瞬时指标为零。
	// 速率值依赖窗口才能还原累积量。
	WindowSeconds *int64 `json:"windowSeconds,omitempty" protobuf:"bytes,4,opt,name=windowSeconds"`

	// 该对象的指标值。
	// Quantity 保留精度和单位，避免客户端提前舍入。
	Value resource.Quantity `json:"value" protobuf:"bytes,5,name=value"`
}

// AllObjects 是用于选择指标的通配符，匹配给定标签选择器下的所有对象。
// 显式通配符让“全部对象”的语义不依赖空选择器。
const AllObjects = "*"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MetricListOptions 用于通过标签选择器筛选指标。
// 对象标签与指标标签是两个不同维度，分别由下面两个选择器控制。
type MetricListOptions struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`

	// 用于按标签限制返回对象列表的选择器，默认匹配所有对象。
	// 这里匹配对象标签，留空即不过滤对象。
	// +optional
	LabelSelector string `json:"labelSelector,omitempty" protobuf:"bytes,1,opt,name=labelSelector"`

	// 用于按标签限制返回指标列表的选择器。
	// 匹配指标自身的标签，用于区分同名指标的多条序列。
	// +optional
	MetricLabelSelector string `json:"metricLabelSelector,omitempty" protobuf:"bytes,2,opt,name=metricLabelSelector"`
}
