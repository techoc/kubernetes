/*
Copyright 2025 The Kubernetes Authors.

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

// Package compatversion 暴露组件的版本兼容信息指标，
// 便于运维同时观测二进制版本、模拟版本与最低兼容版本。
package compatversion

import (
	"context"

	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	// compatVersionInfo 是以恒为 1 的 gauge 携带组件的模拟版本与二进制版本标签，
	// 便于用 PromQL 做版本分布统计。
	compatVersionInfo = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Name: "version_info",
			Help: "Provides the compatibility version info of the component. " +
				"The component label is the name of the component, usually kube, " +
				"but is relevant for aggregated-apiservers.",
			StabilityLevel: k8smetrics.ALPHA,
		},
		[]string{"component", "binary", "emulation", "min_compat"},
	)
)

// ResetCompatVersionInfoMetric 清空所有版本指标，主要用于测试。
func ResetCompatVersionInfoMetric() {
	compatVersionInfo.Reset()
}

// RecordCompatVersionInfo 记录某个组件的版本信息。
// 使用 WithContext 以在 tracing 开启时把 trace 信息作为 exemplar 带上。
func RecordCompatVersionInfo(ctx context.Context, component, binary, emulation, minCompat string) {
	compatVersionInfo.WithContext(ctx).WithLabelValues(component, binary, emulation, minCompat).Set(1)
}

func init() {
	legacyregistry.MustRegister(compatVersionInfo)
}
