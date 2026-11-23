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

// Package types 定义节点声明特性框架使用的公共数据类型。
// 这些类型是各 Feature 实现与 Framework 之间的契约，避免各特性包互相直接依赖。
package types

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/version"
)

// PodInfo 是对 Pod 对象的可扩展封装，后续可按需加入 ResourceClaims、PVC 等辅助资源。
// 这样特性推断逻辑拿到的是一个稳定入口，不必关心辅助资源具体怎么取。
type PodInfo struct {
	// Spec 是 Pod 的期望规格（pod.Spec），特性推断主要读取这里的内容。
	Spec *v1.PodSpec
	// Status 是 Pod 的当前状态，可以为 nil，例如对象尚未创建（调度阶段）时。
	Status *v1.PodStatus
	// 后续如需辅助资源可在此扩展，例如：
	// ResourceClaims []*v1.ResourceClaim
}

// Feature 封装了某个"可声明特性"的全部判定逻辑。
//
// 这个接口刻意把"节点是否具备该能力"（Discover）和"某个 Pod 是否需要该能力"
// （InferForScheduling/InferForUpdate）分开。前者决定节点声明什么，后者决定调度和升级时
// 如何筛选节点；两者解耦后，同一份特性定义可以服务于节点状态上报和调度决策两条链路。
type Feature interface {
	// Name 返回该特性的公认名称，也是注册表中的唯一标识和位图下标的依据。
	Name() string

	// Discover 根据节点配置判断该节点是否提供此特性。
	Discover(cfg *NodeConfiguration) bool

	// Requirements 返回该特性依赖的 feature gate 与静态配置条件。
	Requirements() *FeatureRequirements

	// InferForScheduling 判断调度该 Pod 是否需要此特性。
	InferForScheduling(podInfo *PodInfo) bool

	// InferForUpdate 判断从 oldPodInfo 更新到 newPodInfo 是否需要此特性。
	InferForUpdate(oldPodInfo, newPodInfo *PodInfo) bool

	// MaxVersion 给出该特性作为调度因素仍然有意义的最高 Kubernetes 版本（含）。
	// 一般按特性 GA 版本和集群版本偏差策略设定；nil 表示没有上限。
	// 比较采用完整语义化版本，包含预发布与构建元数据。
	MaxVersion() *version.Version
}

// FeatureRequirements 列出某个特性声明生效所依赖的全部条件。
type FeatureRequirements struct {
	// EnabledFeatureGates 是该特性依赖的 feature gate 名称列表。
	EnabledFeatureGates []string
	// RequiredRuntimeFeatures 是该特性必须满足的运行时能力；nil 表示无运行时依赖。
	RequiredRuntimeFeatures *RuntimeFeatures
}

// FeatureGate 抽象了"查询某个开关是否打开"的能力。
//
// 只用接口而不是依赖具体的 featuregate 实现，是为了让测试能提供一个极简的
// map 实现，也避免把整个组件的配置体系拖进这个包。
type FeatureGate interface {
	// Enabled 在名为 key 的 feature gate 已打开时返回 true。
	Enabled(key string) bool
}

// RuntimeFeatures 汇总从 CRI 探测到的运行时能力。
type RuntimeFeatures struct {
	// UserNamespacesHostNetwork 表示运行时是否支持 user namespace 与 host network 同时使用。
	UserNamespacesHostNetwork bool
	// MountOptions 表示运行时是否支持容器挂载上的额外 bind mount 选项。
	MountOptions bool
}

// StaticConfiguration 为特性发现提供节点静态配置的视图。
type StaticConfiguration struct {
	// 后续按已注册特性的需要在此添加配置字段。
}

// NodeConfiguration 汇总节点侧供特性发现使用的静态配置。
type NodeConfiguration struct {
	// FeatureGates 是 FeatureGate 接口的实现，用于查询开关状态。
	FeatureGates FeatureGate
	// StaticConfig 保存节点的静态配置。
	StaticConfig StaticConfiguration
	// Version 是当前节点版本，用于与 Feature.MaxVersion() 做语义化版本比较，
	// 判断某个特性是否还需要对外声明。
	Version *version.Version
	// RuntimeFeatures 是从 CRI 探测到的运行时能力。
	RuntimeFeatures RuntimeFeatures
}

// FeatureGateMap 是 FeatureGate 的便捷 map 实现。
//
// map 中不存在的键返回零值 false，正好对应"未知特性默认关闭"，
// 因此不需要额外的存在性检查。
type FeatureGateMap map[string]bool

// Enabled 返回 map 中 key 对应的开关值；未配置的键返回 false。
func (m FeatureGateMap) Enabled(key string) bool {
	return m[key]
}

var _ FeatureGate = FeatureGateMap{}
