/*
Copyright The Kubernetes Authors.

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

// Package dranodeallocatableresources 实现 DRA 节点可分配资源这一节点声明特性：
// 根据节点上已分配的设备资源，推断调度与更新阶段对节点能力的要求。
package dranodeallocatableresources

import (
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// 编译期断言：确保这个实现没有漏掉 types.Feature 接口中的任何方法。
// 接口以后加方法时，改动会直接在这里报错，而不是等到运行时才发现。
var _ types.Feature = &draNodeAllocatableResourcesFeature{}

const (
	// DRANodeAllocatableResourcesFeature 是 DRA 可分配节点资源能力的唯一标识，
	// 同时用作 feature gate 名和对外声明的特性名，避免两处字符串漂移。
	DRANodeAllocatableResourcesFeature = "DRANodeAllocatableResources"
)

// Feature 以无状态单例导出。特性实现本身不保存任何节点或 Pod 数据，
// 重复构造实例没有意义，单例也方便各组件共享同一份判定逻辑。
var Feature = &draNodeAllocatableResourcesFeature{}

// draNodeAllocatableResourcesFeature 用私有结构体实现，外部只能通过导出的 Feature 单例访问，
// 防止调用方依赖具体实现类型。
type draNodeAllocatableResourcesFeature struct{}

// Name 返回该特性的对外标识，与同名 feature gate 一致，便于日志和声明互相对照。
func (f *draNodeAllocatableResourcesFeature) Name() string {
	return DRANodeAllocatableResourcesFeature
}

// Discover 判断节点是否声明了该特性。cfg 携带节点的 feature gate 与运行时能力；
// 这个能力完全由特性门控决定：只要门控打开就认为节点声明了该特性，
// 不依赖 CRI 运行时能力或静态配置。
func (f *draNodeAllocatableResourcesFeature) Discover(cfg *types.NodeConfiguration) bool {
	return cfg.FeatureGates.Enabled(DRANodeAllocatableResourcesFeature)
}

// Requirements 返回节点的必要前提。与 Discover 保持一致：
// 只声明门控依赖，运行时能力列表留空，避免给调度侧附加不存在的约束。
func (f *draNodeAllocatableResourcesFeature) Requirements() *types.FeatureRequirements {
	return &types.FeatureRequirements{
		EnabledFeatureGates: []string{DRANodeAllocatableResourcesFeature},
	}
}

// InferForScheduling 恒返回 false：该能力描述节点本身的资源，
// Pod spec 中没有可据以推断的字段，不会影响 pod 调度。
func (f *draNodeAllocatableResourcesFeature) InferForScheduling(podInfo *types.PodInfo) bool {
	return false
}

// InferForUpdate 恒返回 false：资源能力在调度时已确定，
// Pod 更新不会产生新的“节点是否支持 DRA 可分配资源”的需求。
func (f *draNodeAllocatableResourcesFeature) InferForUpdate(oldPodInfo, newPodInfo *types.PodInfo) bool {
	return false
}

// MaxVersion 返回 nil 表示这个特性没有版本上限：即便 GA 之后，
// 它描述的节点资源能力仍然有效，不应因为集群版本升高就从节点声明中消失。
func (f *draNodeAllocatableResourcesFeature) MaxVersion() *version.Version {
	return nil
}
