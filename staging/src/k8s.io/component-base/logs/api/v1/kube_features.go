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

package v1

import (
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/featuregate"
)

const (
	// owner: @pohly
	// kep: https://kep.k8s.io/3077
	// alpha: v1.24
	// beta: v1.30
	//
	// Enables looking up a logger from a context.Context instead of using
	// the global fallback logger and manipulating the logger that is
	// used by a call chain.
	// beta 起默认开启；关闭后组件退回 klog 全局 logger 的旧行为。
	ContextualLogging featuregate.Feature = "ContextualLogging"

	// Allow fine-tuning of experimental, alpha-quality logging options.
	//
	// Per https://groups.google.com/g/kubernetes-sig-architecture/c/Nxsc7pfe5rw/m/vF2djJh0BAAJ
	// we want to avoid a proliferation of feature gates. This feature gate:
	// - will guard *a group* of logging options whose quality level is alpha.
	// - will never graduate to beta or stable.
	//
	// IMPORTANT: Unlike typical feature gates, LoggingAlphaOptions is NOT affected by
	// emulation version changes. Its behavior remains constant regardless of the
	// emulation version being used.
	// 注意：它不受 emulation-version 影响，因为调用的配置 API 本身不随版本变化。
	LoggingAlphaOptions featuregate.Feature = "LoggingAlphaOptions"

	// Allow fine-tuning of experimental, beta-quality logging options.
	//
	// Per https://groups.google.com/g/kubernetes-sig-architecture/c/Nxsc7pfe5rw/m/vF2djJh0BAAJ
	// we want to avoid a proliferation of feature gates. This feature gate:
	// - will guard *a group* of logging options whose quality level is beta.
	// - is thus *introduced* as beta
	// - will never graduate to stable.
	//
	// IMPORTANT: Unlike typical feature gates, LoggingBetaOptions is NOT affected by
	// emulation version changes. Its behavior remains constant regardless of the
	// emulation version being used.
	// 与 LoggingAlphaOptions 相同，配置 API 不参与版本模拟。
	LoggingBetaOptions featuregate.Feature = "LoggingBetaOptions"

	// Stable logging options. Always enabled.
	// 稳定选项不再受开关控制，保留名称只为让组件注册逻辑保持一致。
	LoggingStableOptions featuregate.Feature = "LoggingStableOptions"
)

// featureGates 返回本包 gate 的版本化定义。
// ContextualLogging 在 1.24 alpha、1.30 beta；Logging*Options 永久停留在各自的预发布级别。
func featureGates() map[featuregate.Feature]featuregate.VersionedSpecs {
	return map[featuregate.Feature]featuregate.VersionedSpecs{
		ContextualLogging: {
			{Version: version.MustParse("1.24"), Default: false, PreRelease: featuregate.Alpha},
			{Version: version.MustParse("1.30"), Default: true, PreRelease: featuregate.Beta},
		},
		LoggingAlphaOptions: {
			{Version: version.MustParse("1.24"), Default: false, PreRelease: featuregate.Alpha},
		},
		LoggingBetaOptions: {
			{Version: version.MustParse("1.24"), Default: true, PreRelease: featuregate.Beta},
		},
	}
}

// AddFeatureGates adds all feature gates used by this package.
// 组件在构造 FeatureGate 后需调用本函数，否则 AddFlags 与 ValidateAndApply 无法识别这些开关。
func AddFeatureGates(mutableFeatureGate featuregate.MutableVersionedFeatureGate) error {
	return mutableFeatureGate.AddVersioned(featureGates())
}
