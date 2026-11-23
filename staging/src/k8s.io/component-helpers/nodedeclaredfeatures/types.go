/*
Copyright 2025 The Kubernetes Authors.

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

package nodedeclaredfeatures

import (
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// 以下类型别名是为了方便框架使用者：只需 import 本包即可拿到整套 API，
// 而实现仍放在 types 子包里，子包的内部组织方式可以自由调整，
// 不必让所有使用方跟着改 import path。别名（而非新类型）还保证
// 两边的值和接口可以直接互传，不需要任何转换。
//
// 每个别名都直接转发到 types 子包中的同名类型，语义与实现完全一致。

// PodInfo 是对 Pod 对象的可扩展封装，转发自 types.PodInfo。
type PodInfo = types.PodInfo

// Feature 是单个可声明特性的接口，转发自 types.Feature。
type Feature = types.Feature

// FeatureRequirements 描述特性依赖的开关与运行时能力，转发自 types.FeatureRequirements。
type FeatureRequirements = types.FeatureRequirements

// RuntimeFeatures 汇总 CRI 运行时能力，转发自 types.RuntimeFeatures。
type RuntimeFeatures = types.RuntimeFeatures

// FeatureGate 抽象 feature gate 查询，转发自 types.FeatureGate。
type FeatureGate = types.FeatureGate

// StaticConfiguration 是节点静态配置视图，转发自 types.StaticConfiguration。
type StaticConfiguration = types.StaticConfiguration

// NodeConfiguration 汇总节点侧配置，转发自 types.NodeConfiguration。
type NodeConfiguration = types.NodeConfiguration
