/*
Copyright 2016 The Kubernetes Authors.

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

package feature

import (
	"k8s.io/component-base/featuregate"
)

// 特性门控的全局实例。apiserver 侧组件统一从这里读取开关，避免各自维护状态。
var (
	// DefaultMutableFeatureGate is a mutable version of DefaultFeatureGate.
	// Only top-level commands/options setup and the k8s.io/component-base/featuregate/testing package should make use of this.
	// Tests that need to modify feature gates for the duration of their test should use:
	//   featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.<FeatureName>, <value>)
	DefaultMutableFeatureGate featuregate.MutableVersionedFeatureGate = featuregate.NewFeatureGate()

	// DefaultFeatureGate is a shared global FeatureGate.
	// Top-level commands/options setup that needs to modify this feature gate should use DefaultMutableFeatureGate.
	//
	// 该变量暴露只读接口，防止业务代码在运行时偷偷改开关；仅命令/选项初始化阶段
	// 通过 DefaultMutableFeatureGate 修改，测试用 featuregatetesting 在用例内临时切换。
	DefaultFeatureGate featuregate.FeatureGate = DefaultMutableFeatureGate
)
