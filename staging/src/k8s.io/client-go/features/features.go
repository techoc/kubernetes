/*
Copyright 2024 The Kubernetes Authors.

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

package features

import (
	"context"
	"sync/atomic"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/version"
)

// NOTE: types Feature, FeatureSpec, prerelease (and its values)
// were duplicated from the component-base repository
//
// for more information please refer to https://docs.google.com/document/d/1g9BGCRw-7ucUxO6OtCWbb3lfzUGA_uU9178wLdXAIfs

const (
	// Values for PreRelease.
	// 这些取值与 component-base 中的定义保持字符串一致，便于注册表之间互相接线。
	Alpha = prerelease("ALPHA")
	Beta  = prerelease("BETA")
	GA    = prerelease("")

	// Deprecated
	// 已弃用：仍可被显式启用，但表示该特性计划在未来移除。
	Deprecated = prerelease("DEPRECATED")
)

// prerelease 表示特性的成熟度阶段，未导出以免使用方依赖具体实现。
type prerelease string

// Feature 是特性开关的名称，例如 "ClientsAllowCBOR"。
type Feature string

// FeatureSpec 描述某个特性的默认值和生命周期元数据。
type FeatureSpec struct {
	// Default is the default enablement state for the feature
	Default bool
	// LockToDefault indicates that the feature is locked to its default and cannot be changed
	LockToDefault bool
	// PreRelease indicates the maturity level of the feature
	PreRelease prerelease
	// Version indicates the earliest version from which this FeatureSpec is valid.
	// If multiple FeatureSpecs exist for a Feature, the one with the highest version that is less
	// than or equal to the effective version of the component is used.
	Version *version.Version
}

// VersionedSpecs 是按版本排序的特性定义列表，由支持版本化门控的注册表使用。
type VersionedSpecs []FeatureSpec

// Gates indicates whether a given feature is enabled or not.
type Gates interface {
	// Enabled returns true if the key is enabled.
	Enabled(key Feature) bool
}

// Registry represents an external feature gates registry.
type Registry interface {
	// Add adds existing feature gates to the provided registry.
	//
	// As of today, this method is used by AddFeaturesToExistingFeatureGates and
	// ReplaceFeatureGates to take control of the features exposed by this library.
	Add(map[Feature]FeatureSpec) error
}

// VersionedRegistry represents an external versioned feature gates registry.
type VersionedRegistry interface {
	// AddVersioned adds existing versioned feature gates to the provided registry.
	//
	// As of today, this method is used by AddVersionedFeaturesToExistingFeatureGates and
	// ReplaceFeatureGates to take control of the features exposed by this library.
	AddVersioned(in map[Feature]VersionedSpecs) error
}

// FeatureGates returns the feature gates exposed by this library.
//
// By default, only the default features gates will be returned.
// The default implementation allows controlling the features
// via environmental variables.
// For example, if you have a feature named "MyFeature"
// setting an environmental variable "KUBE_FEATURE_MyFeature"
// will allow you to configure the state of that feature.
//
// Please note that the actual set of the feature gates
// might be overwritten by calling ReplaceFeatureGates method.
//
// 每次调用都从原子变量读取当前实现，因此可以在启动后安全地看到 ReplaceFeatureGates 的结果；
// 但调用方应在初始化阶段完成替换，避免运行中行为突变。
func FeatureGates() Gates {
	return featureGates.Load().(*featureGatesWrapper).Gates
}

// AddFeaturesToExistingFeatureGates adds the default feature gates to the provided registry.
// Usually this function is combined with ReplaceFeatureGates to take control of the
// features exposed by this library.
//
// 注册时传入的是“非版本化”视图：每个特性只保留最新一条规格并清空版本，
// 因为外部注册表通常只理解单一的布尔默认值。
func AddFeaturesToExistingFeatureGates(registry Registry) error {
	return registry.Add(unversionedFeatureGates(defaultVersionedKubernetesFeatureGates))
}

// AddFeaturesToExistingFeatureGates adds the default versioned feature gates to the provided registry.
// Usually this function is combined with ReplaceFeatureGates to take control of the
// features exposed by this library.
// Generally only used by k/k.
//
// 与上一个函数不同，这里保留完整的版本化规格，供支持按版本解析的门控实现使用。
func AddVersionedFeaturesToExistingFeatureGates(registry VersionedRegistry) error {
	return registry.AddVersioned(defaultVersionedKubernetesFeatureGates)
}

// ReplaceFeatureGates overwrites the default implementation of the feature gates
// used by this library.
//
// Useful for binaries that would like to have full control of the features
// exposed by this library, such as allowing consumers of a binary
// to interact with the features via a command line flag.
//
// For example:
//
//	// first, register client-go's features to your registry.
//	clientgofeaturegate.AddFeaturesToExistingFeatureGates(utilfeature.DefaultMutableFeatureGate)
//	// then replace client-go's feature gates implementation with your implementation
//	clientgofeaturegate.ReplaceFeatureGates(utilfeature.DefaultMutableFeatureGate)
func ReplaceFeatureGates(newFeatureGates Gates) {
	if replaceFeatureGatesWithWarningIndicator(newFeatureGates) {
		// TODO (?): A new API would be needed where callers pass in a context or logger.
		// Probably not worth it.
		// 默认实现已被读取过再替换，可能让同一进程内不同组件看到不一致的门控状态。
		utilruntime.HandleErrorWithContext(context.TODO(), nil, "The default feature gates implementation has already been used and now it's being overwritten. This might lead to unexpected behaviour. Check your initialization order.")
	}
}

// replaceFeatureGatesWithWarningIndicator 原子地替换全局门控实现。
// 返回 true 表示旧实现（若仍是环境变量实现）此前已经读取过环境变量，
// 调用方据此决定是否发出“替换过晚”的告警。
func replaceFeatureGatesWithWarningIndicator(newFeatureGates Gates) bool {
	shouldProduceWarning := false

	if defaultFeatureGates, ok := FeatureGates().(*envVarFeatureGates); ok {
		if defaultFeatureGates.hasAlreadyReadEnvVar() {
			shouldProduceWarning = true
		}
	}
	// atomic.Value 要求所有 Store 使用相同的具体类型，故统一包一层 featureGatesWrapper。
	wrappedFeatureGates := &featureGatesWrapper{newFeatureGates}
	featureGates.Store(wrappedFeatureGates)

	return shouldProduceWarning
}

// unversionedFeatureGates takes the latest entry from the VersionedSpecs of each feature, and clears out the version information,
// so that the result can be used with an unversioned feature gate.
//
// 这里假设 VersionedSpecs 已按版本升序排列，因此取末尾元素即最新的规格。
func unversionedFeatureGates(featureGates map[Feature]VersionedSpecs) map[Feature]FeatureSpec {
	unversioned := map[Feature]FeatureSpec{}
	for feature, specs := range featureGates {
		if len(specs) == 0 {
			continue
		}
		latestSpec := specs[len(specs)-1]
		latestSpec.Version = nil // Clear version information.
		unversioned[feature] = latestSpec
	}
	return unversioned
}

// init 安装默认实现：基于环境变量的门控。后续可通过 ReplaceFeatureGates 覆盖。
func init() {
	envVarGates := newEnvVarFeatureGates(unversionedFeatureGates(defaultVersionedKubernetesFeatureGates))

	wrappedFeatureGates := &featureGatesWrapper{envVarGates}
	featureGates.Store(wrappedFeatureGates)
}

// featureGatesWrapper a thin wrapper to satisfy featureGates variable (atomic.Value).
// That is, all calls to Store for a given Value must use values of the same concrete type.
type featureGatesWrapper struct {
	Gates
}

var (
	// featureGates is a shared global FeatureGates.
	//
	// Top-level commands/options setup that needs to modify this feature gates
	// should use AddFeaturesToExistingFeatureGates followed by ReplaceFeatureGates.
	//
	// 使用 atomic.Value 而非普通变量，是为了让无锁读取在并发下安全：
	// Enabled 可能在多个 worker goroutine 中高频调用。
	featureGates = &atomic.Value{}
)
