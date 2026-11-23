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

// ExternalMetricValueList is a list of values for a given metric for some set labels
// 一次查询可返回多条时间序列，每条对应一组不同的标签取值。
type ExternalMetricValueList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// value of the metric matching a given set of labels
	// 列表顺序无语义，消费方应按 MetricLabels 匹配需要的序列。
	Items []ExternalMetricValue `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ExternalMetricValue is a metric value for external metric
// A single metric value is identified by metric name and a set of string labels.
// For one metric there can be multiple values with different sets of labels.
// 本结构对应一条时间序列在某个时刻的采样，而非该指标的全部取值。
type ExternalMetricValue struct {
	metav1.TypeMeta `json:""`

	// the name of the metric
	// 名字由外部指标适配器定义，Kubernetes 侧不校验其命名规范。
	MetricName string `json:"metricName" protobuf:"bytes,1,name=metricName"`

	// a set of labels that identify a single time series for the metric
	// 标签集合相当于时间序列的主键，同一指标名下不允许出现重复的标签组合。
	MetricLabels map[string]string `json:"metricLabels" protobuf:"bytes,2,rep,name=metricLabels"`

	// indicates the time at which the metrics were produced
	// 记录的是产生时间，不是查询时间，避免轮询间隔影响指标判定。
	Timestamp metav1.Time `json:"timestamp" protobuf:"bytes,3,name=timestamp"`

	// indicates the window ([Timestamp-Window, Timestamp]) from
	// which these metrics were calculated, when returning rate
	// metrics calculated from cumulative metrics (or zero for
	// non-calculated instantaneous metrics).
	// 指针类型用于区分“未提供窗口”和“窗口为零”的瞬时指标。
	WindowSeconds *int64 `json:"window,omitempty" protobuf:"bytes,4,opt,name=window"`

	// the value of the metric
	// 用 Quantity 而非 float64，避免十进制指标值在 JSON 编解码中丢失精度。
	Value resource.Quantity `json:"value" protobuf:"bytes,5,name=value"`
}
