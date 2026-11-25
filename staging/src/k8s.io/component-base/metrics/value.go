/*
Copyright 2019 The Kubernetes Authors.

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

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ValueType 是表示简单值的指标类型枚举。
type ValueType int

// 取值刻意与 prometheus.ValueType 对齐，以便直接做类型转换。
const (
	_ ValueType = iota
	CounterValue
	GaugeValue
	UntypedValue
)

// toPromValueType 转换为 prometheus 的枚举，两者数值一一对应。
func (vt *ValueType) toPromValueType() prometheus.ValueType {
	return prometheus.ValueType(*vt)
}

// NewLazyConstMetric 是 MustNewConstMetric 的辅助函数。
// 隐藏指标返回 nil，调用方需要容忍 nil，避免把已弃用指标重新暴露出来。
func NewLazyConstMetric(desc *Desc, valueType ValueType, value float64, labelValues ...string) Metric {
	if desc.IsHidden() {
		return nil
	}
	return prometheus.MustNewConstMetric(desc.toPrometheusDesc(), valueType.toPromValueType(), value, labelValues...)
}

// NewConstMetric 是 prometheus.NewConstMetric 的辅助函数。
// 隐藏指标返回 (nil, nil)，表示“按策略不创建”，而不是错误。
func NewConstMetric(desc *Desc, valueType ValueType, value float64, labelValues ...string) (Metric, error) {
	if desc.IsHidden() {
		return nil, nil
	}
	return prometheus.NewConstMetric(desc.toPrometheusDesc(), valueType.toPromValueType(), value, labelValues...)
}

// NewLazyMetricWithTimestamp 是 NewMetricWithTimestamp 的辅助函数。
// 注意：Metric 'm' 必须由 NewLazyConstMetric() 创建，否则不提供稳定性保证。
// 因为 NewLazyConstMetric 可能返回 nil，这里显式短路，避免把 nil 传给 prometheus。
func NewLazyMetricWithTimestamp(t time.Time, m Metric) Metric {
	if m == nil {
		return nil
	}

	return prometheus.NewMetricWithTimestamp(t, m)
}
