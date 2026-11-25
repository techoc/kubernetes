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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
)

// CustomMetricsClient 获取描述集群级资源和命名空间资源的指标。
// 把两类作用域拆成独立 getter，调用方按对象作用域选择视图。
type CustomMetricsClient interface {
	RootScopedMetricsGetter
	NamespacedMetricsGetter
}

// RootScopedMetricsGetter 提供获取集群级对象指标的接口。
// Namespace 的指标是集群级指标的特殊情况，也通过此接口获取。
type RootScopedMetricsGetter interface {
	// RootScopedMetrics 返回集群级指标查询接口。
	RootScopedMetrics() MetricsInterface
}

// NamespacedMetricsGetter 提供获取指定命名空间内资源指标的接口。
// 返回的接口已绑定命名空间，调用方无需在每次查询里重复携带它。
type NamespacedMetricsGetter interface {
	// NamespacedMetrics 返回绑定指定命名空间的指标查询接口。
	NamespacedMetrics(namespace string) MetricsInterface
}

// MetricsInterface 提供获取 Kubernetes 对象指标的接口。
// 这是单个作用域内的查询入口，区分单对象与批量对象两种取法。
type MetricsInterface interface {
	// GetForObject 获取描述指定对象的指定指标。
	// metricSelector 用来在同名的多条时间序列中进一步过滤。
	GetForObject(groupKind schema.GroupKind, name string, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValue, error)

	// GetForObjects 获取描述指定类型中符合标签选择器的所有对象的指定指标；
	// selector 为 nil 时获取该类型的全部对象。
	// 对象选择器与指标选择器分离，分别决定“查哪些对象”和“查哪条指标”。
	GetForObjects(groupKind schema.GroupKind, selector labels.Selector, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValueList, error)
}
