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
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/featuregate"
)

const (
	// owner: @richabanker
	// kep: https://kep.k8s.io/4828
	//
	// ComponentFlagz 控制 /flagz 端点：暴露组件运行时生效的命令行标志。
	ComponentFlagz featuregate.Feature = "ComponentFlagz"

	// owner: @richabanker
	// kep: https://kep.k8s.io/4827
	// alpha: v1.32
	// beta: v1.36
	//
	// Enables /statusz endpoint for a component making it accessible to
	// users with the system:monitoring cluster role.
	//
	// 1.36 起默认开启，但仅授予 system:monitoring 角色只读访问。
	ComponentStatusz featuregate.Feature = "ComponentStatusz"
)

// featureGates 给出各门控的版本化阶段定义；
// 使用版本化门控而非普通布尔门控，便于按模拟版本控制 zpages 可用性。
func featureGates() map[featuregate.Feature]featuregate.VersionedSpecs {
	return map[featuregate.Feature]featuregate.VersionedSpecs{
		ComponentFlagz: {
			{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
			{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
		},
		ComponentStatusz: {
			{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
			{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
		},
	}
}

// AddFeatureGates adds all feature gates used by this package.
//
// 由组件在初始化时调用；AddVersioned 会按当前模拟版本解析各阶段。
func AddFeatureGates(mutableFeatureGate featuregate.MutableVersionedFeatureGate) error {
	return mutableFeatureGate.AddVersioned(featureGates())
}
