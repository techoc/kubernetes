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

// Package features 定义 component-base/metrics 使用的 feature gate。
// 这些 gate 由各组件在启动时注册并应用，metrics 子系统据此切换实验性行为。
package features

import (
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/featuregate"
	internalmetrics "k8s.io/component-base/metrics/internal"
)

const (
	// owner: @richabanker
	// kep: https://kep.k8s.io/5808
	// alpha: v1.36
	// beta: v1.37
	//
	// NativeHistograms 启用 Kubernetes 指标的原生直方图支持，开启后同时输出经典桶与原生直方图格式。
	// 开启后同时输出经典桶与原生直方图，便于抓取端逐步迁移而不破坏旧消费者。
	NativeHistograms featuregate.Feature = "NativeHistograms"
)

// featureGates 返回本包 gate 的版本化定义：
// 1.36 引入为 alpha（默认关闭），1.37 转为 beta 并默认开启。
func featureGates() map[featuregate.Feature]featuregate.VersionedSpecs {
	return map[featuregate.Feature]featuregate.VersionedSpecs{
		NativeHistograms: {
			{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
			{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
		},
	}
}

// AddFeatureGates 添加本包使用的全部特性门控。
func AddFeatureGates(mutableFeatureGate featuregate.MutableVersionedFeatureGate) error {
	return mutableFeatureGate.AddVersioned(featureGates())
}

// ApplyFeatureGates 将当前特性门控状态传递到 metrics 子系统，必须在特性门控确定后、任何直方图指标注册前调用。
// 把 gate 状态写入 metrics 内部的原子开关；因此调用时机必须早于直方图注册，
// 否则已按旧状态创建的指标不会补发原生直方图。
func ApplyFeatureGates(featureGate featuregate.FeatureGate) {
	internalmetrics.SetNativeHistogramsEnabled(featureGate.Enabled(NativeHistograms))
}
