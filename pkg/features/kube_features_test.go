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
	"maps"
	"slices"
	"testing"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/component-base/featuregate"
)

// TestKubeFeaturesRegistered 校验 defaultVersionedKubernetesFeatureGates 中每一项都已注册到 DefaultFeatureGate。
func TestKubeFeaturesRegistered(t *testing.T) {
	// 取深拷贝避免测试期间修改全局默认门控。
	registeredFeatures := utilfeature.DefaultFeatureGate.DeepCopy().GetAll()

	for featureName := range defaultVersionedKubernetesFeatureGates {
		if _, ok := registeredFeatures[featureName]; !ok {
			t.Errorf("The feature gate %q is not registered in the DefaultFeatureGate", featureName)
		}
	}
}

// TestClientFeaturesRegistered 校验 client-go 的所有功能也注册到了 kube 的 DefaultFeatureGate。
func TestClientFeaturesRegistered(t *testing.T) {
	// 用独立门控 + 适配器枚举 client-go 注册的全部功能名。
	onlyClientFg := featuregate.NewFeatureGate()
	if err := clientfeatures.AddFeaturesToExistingFeatureGates(&clientAdapter{onlyClientFg}); err != nil {
		t.Fatal(err)
	}
	// 与默认门控的注册集合比对，确保没有遗漏。
	registeredFeatures := utilfeature.DefaultFeatureGate.DeepCopy().GetAll()

	for featureName := range onlyClientFg.GetAll() {
		if _, ok := registeredFeatures[featureName]; !ok {
			t.Errorf("The client-go's feature gate %q is not registered in the DefaultFeatureGate", featureName)
		}
	}
}

// TestAllRegisteredFeaturesExpected 反向校验：默认门控里不应出现本文件或 client-go 之外的功能。
func TestAllRegisteredFeaturesExpected(t *testing.T) {
	registeredFeatures := utilfeature.DefaultFeatureGate.DeepCopy().GetAll()
	// 用干净门控重建两份数据源：client-go 功能 + 本包的版本化定义。
	knownFeatureGates := featuregate.NewFeatureGate()
	if err := clientfeatures.AddFeaturesToExistingFeatureGates(&clientAdapter{knownFeatureGates}); err != nil {
		t.Fatal(err)
	}
	// 叠加本包定义的全部 Kubernetes 功能，得到白名单。
	if err := knownFeatureGates.AddVersioned(defaultVersionedKubernetesFeatureGates); err != nil {
		t.Fatal(err)
	}
	knownFeatures := knownFeatureGates.GetAll()

	for registeredFeature := range registeredFeatures {
		if _, ok := knownFeatures[registeredFeature]; !ok {
			t.Errorf("The feature gate %q is not from known feature gates", registeredFeature)
		}
	}
}

// TestEnsureAlphaGatesAreNotSwitchedOnByDefault 保证 Alpha 功能默认不开启且不能锁定到默认值。
func TestEnsureAlphaGatesAreNotSwitchedOnByDefault(t *testing.T) {
	checkAlphaGates := func(feature featuregate.Feature, spec featuregate.FeatureSpec) {
		// FIXME(dims): WindowsHostNetwork 修复或移除后删除此例外。
		// 不要再添加其他豁免项，否则会削弱对 Alpha 门控的约束。
		if feature == "WindowsHostNetwork" {
			return
		}
		if spec.PreRelease == featuregate.Alpha && spec.Default {
			t.Errorf("The alpha feature gate %q is switched on by default", feature)
		}
		if spec.PreRelease == featuregate.Alpha && spec.LockToDefault {
			t.Errorf("The alpha feature gate %q is locked to default", feature)
		}
	}

	// 逐版本检查，历史版本里的 Alpha 定义同样受约束。
	for feature, specs := range defaultVersionedKubernetesFeatureGates {
		for _, spec := range specs {
			checkAlphaGates(feature, spec)
		}
	}
}

// TestAllDependenciesRegistered 保证每个功能都显式登记了依赖（即使为空列表）。
// 空列表表示已确认没有依赖；缺项则无法区分遗漏登记与确实无依赖。
func TestAllDependenciesRegistered(t *testing.T) {
	// 从默认门控读取实际登记的依赖关系。
	registeredDependencies := utilfeature.DefaultFeatureGate.Dependencies()
	// 排序只为错误信息稳定，便于定位缺失项。
	for _, f := range slices.Sorted(maps.Keys(defaultVersionedKubernetesFeatureGates)) {
		if _, depsRegistered := registeredDependencies[f]; !depsRegistered {
			t.Errorf("Feature %s did not register dependencies. All features must record explicit feature dependencies, even if there are none.", f)
		}
	}
}
