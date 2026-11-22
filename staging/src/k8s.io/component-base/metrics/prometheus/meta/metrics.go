/*
Copyright 2023 The Kubernetes Authors.

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

// Package meta 定义描述指标系统自身行为的元指标：
// 已注册/已禁用/已隐藏指标的数量，用于监控指标配置是否符合预期。
package meta

import (
	k8smetrics "k8s.io/component-base/metrics"
)

var (
	// RegisteredMetrics 按稳定性级别与弃用版本统计已注册的指标数。
	RegisteredMetrics = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Name:           "registered_metrics_total",
			Help:           "The count of registered metrics broken by stability level and deprecation version.",
			StabilityLevel: k8smetrics.BETA,
		},
		[]string{"stability_level", "deprecated_version"},
	)

	// DisabledMetricsTotal 统计被 --disabled-metrics 关闭的指标数。
	DisabledMetricsTotal = k8smetrics.NewCounter(
		&k8smetrics.CounterOpts{
			Name:           "disabled_metrics_total",
			Help:           "The count of disabled metrics.",
			StabilityLevel: k8smetrics.BETA,
		},
	)

	// HiddenMetricsTotal 统计因弃用而隐藏的指标数。
	HiddenMetricsTotal = k8smetrics.NewCounter(
		&k8smetrics.CounterOpts{
			Name:           "hidden_metrics_total",
			Help:           "The count of hidden metrics.",
			StabilityLevel: k8smetrics.BETA,
		},
	)
)
