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
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/metrics/pkg/apis/custom_metrics"
)

// Convert_v1beta1_MetricValue_To_custom_metrics_MetricValue 补全自动生成的转换：
// v1beta1 把指标名和标签平铺在顶层，内部版本则收进 Metric 字段。
func Convert_v1beta1_MetricValue_To_custom_metrics_MetricValue(in *MetricValue, out *custom_metrics.MetricValue, s conversion.Scope) error {
	// 自动转换只覆盖同名字段，指标标识需要手工重排。
	if err := autoConvert_v1beta1_MetricValue_To_custom_metrics_MetricValue(in, out, s); err != nil {
		return err
	}
	out.Metric.Name = in.MetricName
	out.Metric.Selector = in.Selector
	return nil
}

// Convert_custom_metrics_MetricValue_To_v1beta1_MetricValue 做反向转换，
// 把内部版本的 Metric 拆回顶层的 MetricName 和 Selector。
func Convert_custom_metrics_MetricValue_To_v1beta1_MetricValue(in *custom_metrics.MetricValue, out *MetricValue, s conversion.Scope) error {
	if err := autoConvert_custom_metrics_MetricValue_To_v1beta1_MetricValue(in, out, s); err != nil {
		return err
	}
	out.MetricName = in.Metric.Name
	out.Selector = in.Metric.Selector
	return nil
}
