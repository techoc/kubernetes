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

// Package draoptionalnodeoperations 实现 DRA 可选节点操作这一节点声明特性：
// 判断节点是否支持在 prebind 阶段由 DRA 驱动执行可选的节点操作。
package draoptionalnodeoperations

import (
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// 编译期检查“指针实现了 Feature”，接口方法漏写时能立刻发现，
// 否则要等到某个节点没声明该特性才暴露，排查成本高得多。
var _ types.Feature = &draOptionalNodeOperationsFeature{}

const (
	// DRAOptionalNodeOperationsFeatureGate 是 DRA 可选节点操作能力的特性门名，
	// 同时作为本特性对外声明的名字，保证开关与声明共用同一字符串。
	DRAOptionalNodeOperationsFeatureGate = "DRAOptionalNodeOperations"
)

// Feature 是供全局注册表引用的单例。实现没有状态，共享一个实例即可；
// 同时对外只暴露接口类型，避免调用方依赖实现细节。
var Feature = &draOptionalNodeOperationsFeature{}

// 具体实现刻意不导出：这个包对外只提供 Feature 单例和特性门名字，
// 减少可直接构造的入口。
type draOptionalNodeOperationsFeature struct{}

// Name 返回对外声明的特性名，与同名 feature gate 保持一致。
func (f *draOptionalNodeOperationsFeature) Name() string {
	return DRAOptionalNodeOperationsFeatureGate
}

// Requirements 只声明特性门这一个前提：DRA 的可选节点操作由控制面配置决定，
// 节点只要开启对应的门就具备该能力，不依赖额外的运行时探测。
func (f *draOptionalNodeOperationsFeature) Requirements() *types.FeatureRequirements {
	return &types.FeatureRequirements{
		EnabledFeatureGates: []string{DRAOptionalNodeOperationsFeatureGate},
	}
}

// Discover 直接读特性门状态，不嗅探节点硬件或驱动版本：
// 该特性的启用与否完全由部署配置决定，没有更可靠的运行时信号。
func (f *draOptionalNodeOperationsFeature) Discover(cfg *types.NodeConfiguration) bool {
	return cfg.FeatureGates.Enabled(DRAOptionalNodeOperationsFeatureGate)
}

// InferForScheduling 始终返回 false，因为调度阶段无法判断：
// 资源声明的分配结果要到 prebind 才写入，PreFilter 时看不到。
// 相关过滤留给 dynamicresources 调度插件直接处理，这里硬推断只会产生错误结论。
func (f *draOptionalNodeOperationsFeature) InferForScheduling(podInfo *types.PodInfo) bool {
	// ResourceClaim 的 status.allocation 要到 prebind 阶段才会写入，
	// 因此 NDF（节点声明特性）插件在 PreFilter 阶段看不到分配结果，无法推断该需求。
	// 需要可选节点操作的 ResourceClaim 由 dynamicresources 调度插件直接过滤，
	// 这里若强行推断只会得出错误结论。
	return false
}

// InferForUpdate 同样返回 false：更新流程里没有能可靠推断该需求的字段，
// 与其误判不如不声明，交由专门处理 DRA 的组件负责。
func (f *draOptionalNodeOperationsFeature) InferForUpdate(oldPodInfo, newPodInfo *types.PodInfo) bool {
	return false
}

// MaxVersion 返回 nil 表示没有版本上限：该特性由特性门控制，
// 一旦启用就与 Kubernetes 的具体版本无关。
func (f *draOptionalNodeOperationsFeature) MaxVersion() *version.Version {
	return nil
}
