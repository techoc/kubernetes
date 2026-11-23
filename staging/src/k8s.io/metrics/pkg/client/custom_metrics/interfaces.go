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

// CustomMetricsClient is a client for fetching metrics
// describing both root-scoped and namespaced resources.
// 中文说明：把两类作用域拆成独立 getter，调用方按对象作用域选择视图，接口语义更清楚。
type CustomMetricsClient interface {
	RootScopedMetricsGetter
	NamespacedMetricsGetter
}

// RootScopedMetricsGetter provides access to an interface for fetching
// metrics describing root-scoped objects.  Note that metrics describing
// a namespace are simply considered a special case of root-scoped metrics.
// 中文说明：Namespace 本身是集群级对象，因此查询单个 Namespace 的指标也走这里。
type RootScopedMetricsGetter interface {
	RootScopedMetrics() MetricsInterface
}

// NamespacedMetricsGetter provides access to an interface for fetching
// metrics describing resources in a particular namespace.
// 中文说明：返回的接口已绑定命名空间，调用方无需在每次查询里重复携带它。
type NamespacedMetricsGetter interface {
	NamespacedMetrics(namespace string) MetricsInterface
}

// MetricsInterface provides access to metrics describing Kubernetes objects.
// 中文说明：这是单个作用域内的查询入口，区分单对象与批量对象两种取法。
type MetricsInterface interface {
	// GetForObject fetchs the given metric describing the given object.
	// 中文说明：metricSelector 用来在同名的多条时间序列中进一步过滤。
	GetForObject(groupKind schema.GroupKind, name string, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValue, error)

	// GetForObjects fetches the given metric describing all objects of the given
	// type matching the given label selector (or simply all objects of the given type
	// if the selector is nil).
	// 中文说明：对象选择器与指标选择器分离，分别决定“查哪些对象”和“查哪条指标”。
	GetForObjects(groupKind schema.GroupKind, selector labels.Selector, metricName string, metricSelector labels.Selector) (*v1beta2.MetricValueList, error)
}
