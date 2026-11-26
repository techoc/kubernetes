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

// 注意：Feature、FeatureSpec、prerelease（及其取值）等类型复制自 component-base 仓库。
//
// 更多信息参见 https://docs.google.com/document/d/1g9BGCRw-7ucUxO6OtCWbb3lfzUGA_uU9178wLdXAIfs

const (
	// prerelease 的取值。
	// 这些取值与 component-base 中的定义保持字符串一致，便于注册表之间互相接线。
	Alpha = prerelease("ALPHA")
	Beta  = prerelease("BETA")
	GA    = prerelease("")

	// 已弃用。
	// 已弃用：仍可被显式启用，但表示该特性计划在未来移除。
	Deprecated = prerelease("DEPRECATED")
)

// prerelease 表示特性的成熟度阶段，未导出以免使用方依赖具体实现。
type prerelease string

// Feature 是特性开关的名称，例如 "ClientsAllowCBOR"。
type Feature string

// FeatureSpec 描述某个特性的默认值和生命周期元数据。
type FeatureSpec struct {
	// Default 是该特性的默认启用状态。
	Default bool
	// LockToDefault 表示特性锁定在默认值上，不可更改。
	LockToDefault bool
	// PreRelease 表示特性的成熟度阶段。
	PreRelease prerelease
	// Version 是该规格生效的最早版本。同一特性有多条规格时，取不高于组件实际版本的
	// 最高版本那条。
	Version *version.Version
}

// VersionedSpecs 是按版本排序的特性定义列表，由支持版本化门控的注册表使用。
type VersionedSpecs []FeatureSpec

// Gates 用于查询某个特性是否启用。
type Gates interface {
	// Enabled 在特性启用时返回 true。
	Enabled(key Feature) bool
}

// Registry 表示外部特性门控注册表。
type Registry interface {
	// Add 把已有门控加入给定注册表。
	//
	// 目前 AddFeaturesToExistingFeatureGates 和 ReplaceFeatureGates 通过它接管本库
	// 暴露的特性。
	Add(map[Feature]FeatureSpec) error
}

// VersionedRegistry 表示支持版本的外部特性门控注册表。
type VersionedRegistry interface {
	// AddVersioned 把已有的版本化门控加入给定注册表。
	//
	// 目前 AddVersionedFeaturesToExistingFeatureGates 和 ReplaceFeatureGates 通过它
	// 接管本库暴露的特性。
	AddVersioned(in map[Feature]VersionedSpecs) error
}

// FeatureGates 返回本库暴露的门控。
//
// 默认返回默认门控实现，该实现允许通过环境变量控制特性。例如特性名为 "MyFeature" 时，
// 设置环境变量 "KUBE_FEATURE_MyFeature" 即可配置其状态。
//
// 注意实际门控集合可能被 ReplaceFeatureGates 替换。
//
// 每次调用都从原子变量读取当前实现，因此可以在启动后安全地看到 ReplaceFeatureGates 的结果；
// 但调用方应在初始化阶段完成替换，避免运行中行为突变。
func FeatureGates() Gates {
	return featureGates.Load().(*featureGatesWrapper).Gates
}

// AddFeaturesToExistingFeatureGates 把默认门控加入给定注册表。
// 通常与 ReplaceFeatureGates 配合使用，以接管本库暴露的特性。
//
// 注册时传入的是“非版本化”视图：每个特性只保留最新一条规格并清空版本，
// 因为外部注册表通常只理解单一的布尔默认值。
func AddFeaturesToExistingFeatureGates(registry Registry) error {
	return registry.Add(unversionedFeatureGates(defaultVersionedKubernetesFeatureGates))
}

// AddVersionedFeaturesToExistingFeatureGates 把默认版本化门控加入给定注册表。
// 通常与 ReplaceFeatureGates 配合使用以接管本库特性，一般只由 k/k 使用。
//
// 与上一个函数不同，这里保留完整的版本化规格，供支持按版本解析的门控实现使用。
func AddVersionedFeaturesToExistingFeatureGates(registry VersionedRegistry) error {
	return registry.AddVersioned(defaultVersionedKubernetesFeatureGates)
}

// ReplaceFeatureGates 替换本库默认使用的门控实现。
//
// 适合需要完全掌控本库特性的二进制文件，例如让使用者通过命令行开关配置特性。
//
// 例如：
//
//	// first, register client-go's features to your registry.
//	clientgofeaturegate.AddFeaturesToExistingFeatureGates(utilfeature.DefaultMutableFeatureGate)
//	// then replace client-go's feature gates implementation with your implementation
//	clientgofeaturegate.ReplaceFeatureGates(utilfeature.DefaultMutableFeatureGate)
func ReplaceFeatureGates(newFeatureGates Gates) {
	if replaceFeatureGatesWithWarningIndicator(newFeatureGates) {
		// TODO (?): 需要新 API 才能把 context 或 logger 传给调用方，但多半不值得。
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

// unversionedFeatureGates 取每个特性 VersionedSpecs 中的最新条目并清空版本信息，
// 以便结果能传给非版本化门控。
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

// featureGatesWrapper 只是为满足 featureGates（atomic.Value）的类型约束而包的一层：
// 同一个 atomic.Value 的 Store 必须使用相同的具体类型。
type featureGatesWrapper struct {
	Gates
}

var (
	// featureGates 是全局共享的 FeatureGates。
	//
	// 顶层命令/选项若需修改门控，应先调用 AddFeaturesToExistingFeatureGates，
	// 再调用 ReplaceFeatureGates。
	//
	// 使用 atomic.Value 而非普通变量，是为了让无锁读取在并发下安全：
	// Enabled 可能在多个 worker goroutine 中高频调用。
	featureGates = &atomic.Value{}
)
