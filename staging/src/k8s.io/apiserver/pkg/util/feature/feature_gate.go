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

// Package feature 包提供 apiserver 使用的全局特性门控实例。
// 特性门控（Feature Gate）用于控制 Kubernetes 中某项功能是否启用，
// 通常由命令行参数 --feature-gates 或组件配置设置。
package feature

import (
	// featuregate 包定义了特性门控接口、可变接口以及具体实现。
	// 本文件不重新实现开关管理逻辑，而是创建并导出供 apiserver 共享的实例。
	"k8s.io/component-base/featuregate"
)

// 这里定义 apiserver 使用的两组全局特性门控实例：
// 1. DefaultMutableFeatureGate：可变接口，供程序初始化阶段注册或设置特性。
// 2. DefaultFeatureGate：只读接口，供其他代码查询特性是否启用。
// 两个变量底层指向同一个对象，因此通过可变接口修改后，
// 通过只读接口查询时也能观察到修改结果。
var (
	// DefaultMutableFeatureGate is a mutable version of DefaultFeatureGate.
	// Only top-level commands/options setup and the k8s.io/component-base/featuregate/testing package should make use of this.
	// Tests that need to modify feature gates for the duration of their test should use:
	//   featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.<FeatureName>, <value>)
	//
	// DefaultMutableFeatureGate 是默认特性门控的可变版本。
	// MutableVersionedFeatureGate 在只读查询能力之外，还提供注册特性、设置特性开关等修改能力。
	//
	// 主要使用场景：
	// 1. 顶层命令或选项初始化阶段：注册组件支持的特性，并根据启动参数设置开关。
	// 2. 测试环境：通过 featuregate/testing 提供的测试辅助方法，在单个测试用例期间临时修改开关。
	//
	// 注意：不要让普通业务逻辑随意依赖这个可变变量，否则运行期间可能出现特性状态被意外修改的问题。
	DefaultMutableFeatureGate featuregate.MutableVersionedFeatureGate = featuregate.NewFeatureGate()

	// DefaultFeatureGate is a shared global FeatureGate.
	// Top-level commands/options setup that needs to modify this feature gate should use DefaultMutableFeatureGate.
	//
	// DefaultFeatureGate 是 apiserver 共享的默认特性门控对象，但这里只暴露 FeatureGate 只读接口。
	// 其他组件可以通过 Enabled() 查询某个特性是否启用，而不能通过该接口直接修改开关。
	//
	// 它与 DefaultMutableFeatureGate 指向同一个底层实例：
	// - 初始化阶段：通过 DefaultMutableFeatureGate 注册或设置特性。
	// - 运行阶段：通过 DefaultFeatureGate.Enabled(feature) 查询特性状态。
	//
	// 这种“可变接口负责配置、只读接口负责消费”的设计，能够缩小普通业务代码的修改权限，
	// 避免特性开关在组件运行过程中被任意更改。
	DefaultFeatureGate featuregate.FeatureGate = DefaultMutableFeatureGate
)
