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
	"k8s.io/component-base/featuregate"
)

var (
	// 预定义本包 feature gate 的默认、全部启用和全部禁用三种状态。
	// 预先构造三种状态，供测试验证“某特性关闭时行为不同”而无需各自搭建 gate。
	defaultFeatureGate, enabledFeatureGate, disabledFeatureGate featuregate.FeatureGate
)

func init() {
	// 先注册本包声明的全部 feature gate，得到默认状态的实例。
	mutable := featuregate.NewFeatureGate()
	if err := AddFeatureGates(mutable); err != nil {
		panic(err)
	}
	defaultFeatureGate = mutable
	// DeepCopy 保证后续对 enabled/disabled 的修改不会串到默认实例上。
	enabled := mutable.DeepCopy()
	disabled := mutable.DeepCopy()
	for feature := range mutable.GetAll() {
		// 逐项设置，避免硬编码 feature 名称导致新增 gate 后测试覆盖不全。
		if err := enabled.SetFromMap(map[string]bool{string(feature): true}); err != nil {
			panic(err)
		}
		if err := disabled.SetFromMap(map[string]bool{string(feature): false}); err != nil {
			panic(err)
		}
	}
	enabledFeatureGate = enabled
	disabledFeatureGate = disabled
}
