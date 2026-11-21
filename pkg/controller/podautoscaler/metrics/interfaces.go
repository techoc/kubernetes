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

package metrics

import (
	"context"
	"time"

	autoscaling "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// PodMetric 保存单个 Pod 的指标值；指标值按毫单位（milli-value）表示，
// 以便在不损失精度的情况下用整数承载小数指标。
type PodMetric struct {
	Timestamp time.Time     // Timestamp 是该指标的采集时间。
	Window    time.Duration // Window 是指标统计所覆盖的时间窗口。
	Value     int64         // Value 是毫单位的指标数值。
}

// PodMetricsInfo 以 Pod 名称为键保存 Pod 指标，便于按名称快速查找。
type PodMetricsInfo map[string]PodMetric

// MetricsClient 封装远程指标接口的查询能力，既能获取容器级别的资源指标，
// 也能获取 Pod 级别的任意指标。
type MetricsClient interface {
	// GetResourceMetric 返回给定命名空间中、匹配指定 selector 的所有 Pod 里，
	// 指定容器的某项资源指标及对应的最早时间戳。
	// container 为空字符串时返回该 Pod 所有容器指标之和。
	// 指标缺失不会报错；调用方应依据 Pod 状态自行过滤本接口返回的指标信息。
	GetResourceMetric(ctx context.Context, resource v1.ResourceName, namespace string, selector labels.Selector, container string) (PodMetricsInfo, time.Time, error)

	// GetRawMetric 返回给定命名空间中、匹配指定 selector 的所有 Pod 的某项指标
	// 及对应的最早时间戳；metricSelector 用于进一步筛选指标自身标签。
	GetRawMetric(metricName string, namespace string, selector labels.Selector, metricSelector labels.Selector) (PodMetricsInfo, time.Time, error)

	// GetObjectMetric 返回给定命名空间中指定对象的某项指标及对应的时间戳。
	GetObjectMetric(metricName string, namespace string, objectRef *autoscaling.CrossVersionObjectReference, metricSelector labels.Selector) (int64, time.Time, error)

	// GetExternalMetric 返回匹配指定 selector 的某项外部指标的所有取值。
	GetExternalMetric(metricName string, namespace string, selector labels.Selector) ([]int64, time.Time, error)
}
