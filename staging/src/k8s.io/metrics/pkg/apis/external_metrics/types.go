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

// a list of values for a given metric for some set labels
// 同一个指标名可以返回多条不同标签组合的值，列表即这些时间序列的集合。
type ExternalMetricValueList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty"`

	// value of the metric matching a given set of labels
	Items []ExternalMetricValue `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// a metric value for external metric
// A single metric value is identified by metric name and a set of string labels.
// 名字加标签共同确定一条序列，只按名字查询会拿到全部标签组合。
// For one metric there can be multiple values with different sets of labels.
type ExternalMetricValue struct {
	metav1.TypeMeta `json:""`

	// the name of the metric
	MetricName string `json:"metricName"`

	// a set of labels that identify a single time series for the metric
	// 标签直接映射后端时间序列的维度，不做额外归并。
	MetricLabels map[string]string `json:"metricLabels"`

	// indicates the time at which the metrics were produced
	Timestamp metav1.Time `json:"timestamp"`

	// indicates the window ([Timestamp-Window, Timestamp]) from
	// which these metrics were calculated, when returning rate
	// metrics calculated from cumulative metrics (or zero for
	// non-calculated instantaneous metrics).
	// 速率值需要窗口才能还原累积量；瞬时值该字段为零。
	WindowSeconds *int64 `json:"window,omitempty"`

	// the value of the metric
	// 用 Quantity 保留单位与精度，避免客户端提前舍入。
	Value resource.Quantity `json:"value"`
}
