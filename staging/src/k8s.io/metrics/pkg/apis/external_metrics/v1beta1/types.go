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

package v1beta1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ExternalMetricValueList 是同一指标在一组标签下的取值列表。
// 一次查询可返回多条时间序列，每条对应一组不同的标签取值。
type ExternalMetricValueList struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 与给定标签集合匹配的指标值。
	// 列表顺序无语义，消费方应按 MetricLabels 匹配需要的序列。
	Items []ExternalMetricValue `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ExternalMetricValue 是外部指标的一条取值记录。
// 单条记录由指标名称和一组字符串标签共同标识，同一指标可以有不同的标签组合。
// 本结构对应一条时间序列在某个时刻的采样，而非该指标的全部取值。
type ExternalMetricValue struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`

	// 指标名称。
	// 名字由外部指标适配器定义，Kubernetes 侧不校验其命名规范。
	MetricName string `json:"metricName" protobuf:"bytes,1,name=metricName"`

	// 标识该指标下单一时间序列的一组标签。
	// 标签集合相当于时间序列的主键，同一指标名下不允许出现重复的标签组合。
	MetricLabels map[string]string `json:"metricLabels" protobuf:"bytes,2,rep,name=metricLabels"`

	// 指标产生的时间，而非查询时间，避免轮询间隔影响指标判定。
	Timestamp metav1.Time `json:"timestamp" protobuf:"bytes,3,name=timestamp"`

	// 指标计算所使用的时间区间 [Timestamp-Window, Timestamp]。
	// 返回由累计指标计算出的速率时提供该窗口；非计算的瞬时指标为零。
	// 指针类型用于区分“未提供窗口”和“窗口为零”的瞬时指标。
	WindowSeconds *int64 `json:"window,omitempty" protobuf:"bytes,4,opt,name=window"`

	// 指标值。
	// 用 Quantity 而非 float64，避免十进制指标值在 JSON 编解码中丢失精度。
	Value resource.Quantity `json:"value" protobuf:"bytes,5,name=value"`
}
