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

package external_metrics

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ExternalMetricValueList 是同一指标在一组标签下的取值列表。
// 同一个指标名可以返回多条不同标签组合的值。
type ExternalMetricValueList struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	metav1.ListMeta `json:"metadata,omitempty"`

	// 与给定标签集合匹配的指标值。
	Items []ExternalMetricValue `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ExternalMetricValue 是外部指标的一条取值记录。
// 单条记录由指标名称和一组字符串标签共同标识，同一指标可以有不同的标签组合。
type ExternalMetricValue struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`

	// 指标名称。
	MetricName string `json:"metricName"`

	// 标识该指标下单一时间序列的一组标签。
	// 标签直接映射后端时间序列的维度，不做额外归并。
	MetricLabels map[string]string `json:"metricLabels"`

	// 指标产生的时间。
	Timestamp metav1.Time `json:"timestamp"`

	// 指标计算所使用的时间区间 [Timestamp-Window, Timestamp]。
	// 返回由累计指标计算出的速率时提供该窗口；非计算的瞬时指标为零。
	// 速率值需要窗口才能还原累积量。
	WindowSeconds *int64 `json:"window,omitempty"`

	// 指标值。
	// 用 Quantity 保留单位与精度，避免客户端提前舍入。
	Value resource.Quantity `json:"value"`
}
