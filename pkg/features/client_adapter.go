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
	"fmt"

	clientfeatures "k8s.io/client-go/features"
	"k8s.io/component-base/featuregate"
)

// clientAdapter 将 k8s.io/component-base/featuregate.MutableFeatureGate 适配为 client-go 的
// Gate 与 Registry 接口。component-base 的 Feature、FeatureSpec、prerelease 类型及常量
// 在 client-go 中有一套平行定义。之所以重复定义，是为了让 client-go 的功能门控也能
// 复用这套机制，同时避免 component-base 与 client-go 之间产生循环依赖。
type clientAdapter struct {
	// mfg 是底层可变的 feature gate，所有操作都委托给它。
	mfg featuregate.MutableFeatureGate
}

// 编译期断言：clientAdapter 实现了 client-go 的 Gates 接口。
var _ clientfeatures.Gates = &clientAdapter{}

// Enabled 判断 client-go 功能是否启用。
// 将 clientfeatures.Feature 直接转为 component-base 的 Feature，委托底层门控判断。
func (a *clientAdapter) Enabled(name clientfeatures.Feature) bool {
	return a.mfg.Enabled(featuregate.Feature(name))
}

// 编译期断言：clientAdapter 实现了 client-go 的 VersionedRegistry 接口。
var _ clientfeatures.VersionedRegistry = &clientAdapter{}

// Add 批量注册 client-go 功能门控到 component-base。
// 逐项把 client-go 的 FeatureSpec 转换为 component-base 的 FeatureSpec。
func (a *clientAdapter) Add(in map[clientfeatures.Feature]clientfeatures.FeatureSpec) error {
	out := map[featuregate.Feature]featuregate.FeatureSpec{}
	for name, spec := range in {
		// 先复制与版本无关的基础字段。
		converted := featuregate.FeatureSpec{
			Default:       spec.Default,
			LockToDefault: spec.LockToDefault,
		}
		// 两套 prerelease 常量定义相同但类型不同，必须逐值映射。
		switch spec.PreRelease {
		case clientfeatures.Alpha:
			converted.PreRelease = featuregate.Alpha
		case clientfeatures.Beta:
			converted.PreRelease = featuregate.Beta
		case clientfeatures.GA:
			converted.PreRelease = featuregate.GA
		case clientfeatures.Deprecated:
			converted.PreRelease = featuregate.Deprecated
		default:
			// 走到 default 说明出现了预期外的稳定性等级。
			// 两套常量必须一一对应，若新加等级就会在此暴露，因此直接 panic。
			panic(fmt.Sprintf("unrecognized prerelease %q of feature %q", spec.PreRelease, name))
		}
		out[featuregate.Feature(name)] = converted
	}
	// 一次性提交全部定义，由底层门控保证原子性。
	return a.mfg.Add(out) //nolint:forbidigo
}

// AddVersioned 批量注册带版本信息的 client-go 功能门控。
func (a *clientAdapter) AddVersioned(in map[clientfeatures.Feature]clientfeatures.VersionedSpecs) error {
	// 版本化注册要求底层门控实现特定接口；否则报错而不是静默降级。
	mvfg, ok := a.mfg.(featuregate.MutableVersionedFeatureGate)
	if !ok {
		return fmt.Errorf("feature gate does not support versioning")
	}

	out := make(map[featuregate.Feature]featuregate.VersionedSpecs)
	for name, specs := range in {
		convertedSpecs := make(featuregate.VersionedSpecs, len(specs))
		for i, spec := range specs {
			// 版本化场景额外携带 Version 字段，其余与 Add 的转换一致。
			converted := featuregate.FeatureSpec{
				Default:       spec.Default,
				LockToDefault: spec.LockToDefault,
				Version:       spec.Version,
			}
			switch spec.PreRelease {
			case clientfeatures.Alpha:
				converted.PreRelease = featuregate.Alpha
			case clientfeatures.Beta:
				converted.PreRelease = featuregate.Beta
			case clientfeatures.GA:
				converted.PreRelease = featuregate.GA
			case clientfeatures.Deprecated:
				converted.PreRelease = featuregate.Deprecated
			default:
				// 同 Add：两套 prerelease 常量必须一一映射，出现未知值说明代码遗漏。
				panic(fmt.Sprintf("unrecognized prerelease %q of feature %q", spec.PreRelease, name))
			}
			convertedSpecs[i] = converted
		}
		out[featuregate.Feature(name)] = convertedSpecs
	}
	// 委托给支持版本化的底层门控，完成一次性批量注册。
	return mvfg.AddVersioned(out)
}

// Set 实现 client-go 测试包所要求的非导出接口
// k8s.io/client-go/features/testing.SetFeatureDuringTest，使集成测试能够临时覆盖 client-go 功能门控。
func (a *clientAdapter) Set(name clientfeatures.Feature, enabled bool) error {
	return a.mfg.SetFromMap(map[string]bool{string(name): enabled})
}
