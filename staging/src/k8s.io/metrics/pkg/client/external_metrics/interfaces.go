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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics/v1beta1"
)

// ExternalMetricsClient 是获取外部指标的客户端。
// 只暴露必要的窄接口，调用方无法依赖具体实现，便于替换后端。
type ExternalMetricsClient interface {
	NamespacedMetricsGetter
}

// NamespacedMetricsGetter 提供获取指定命名空间指标的接口。
// 外部指标总是带命名空间上下文，接口层面就强制调用方提供。
type NamespacedMetricsGetter interface {
	// NamespacedMetrics 返回绑定指定命名空间的指标查询接口。
	NamespacedMetrics(namespace string) MetricsInterface
}

// MetricsInterface 提供获取外部指标的接口。
// 命名空间已由上一层绑定，这里只需指标名和标签选择器。
type MetricsInterface interface {
	// List 获取当前命名空间中符合 metricSelector 的指定指标。
	// 选择器由调用方构造，服务端按标签精确匹配时间序列；同名不同标签的值会分别返回。
	List(metricName string, metricSelector labels.Selector) (*v1beta1.ExternalMetricValueList, error)
}
