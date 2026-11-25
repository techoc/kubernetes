/*
Copyright 2022 The Kubernetes Authors.

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

// Package feature 暴露 feature gate 的启用状态与阶段指标。
package feature

import (
	"context"

	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	// featureInfo 是记录 Kubernetes feature 启用状态的 Prometheus Gauge。
	// 每条时间序列以 name/stage 为标签，值为 0 或 1。
	featureInfo = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace:      "kubernetes",
			Name:           "feature_enabled",
			Help:           "This metric records the data about the stage and enablement of a k8s feature.",
			StabilityLevel: k8smetrics.BETA,
		},
		[]string{"name", "stage"},
	)
)

func init() {
	legacyregistry.MustRegister(featureInfo)
}

// ResetFeatureInfoMetric 清空所有 feature 指标，主要用于测试。
func ResetFeatureInfoMetric() {
	featureInfo.Reset()
}

// RecordFeatureInfo 记录某 feature 在给定阶段的启用状态。
// 把 bool 转换为 gauge 值 0/1，方便直接做比率计算。
func RecordFeatureInfo(ctx context.Context, name string, stage string, enabled bool) {
	value := 0.0
	if enabled {
		value = 1.0
	}
	featureInfo.WithContext(ctx).WithLabelValues(name, stage).Set(value)
}
