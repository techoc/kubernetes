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

package inplacepodresize

import (
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// 编译期断言：漏实现 Feature 的接口方法应当在构建时暴露，而不是运行到框架里才失败。
var _ types.Feature = &podLevelResourcesResizeFeature{}

// IPPRPodLevelResourcesFeatureGate 是 pod 级资源原地扩缩容对应的 feature gate。
// 暴露门控名供节点侧与测试共用，避免字符串在两处各写一遍后悄悄不一致。
const IPPRPodLevelResourcesFeatureGate = "InPlacePodLevelResourcesVerticalScaling"

// PodLevelResourcesResizeFeature 是 pod 级资源原地扩缩容特性的实现。
// 导出单例供框架注册；实现类型刻意不导出，防止外部绕过单例自行构造。
var PodLevelResourcesResizeFeature = &podLevelResourcesResizeFeature{}

// podLevelResourcesResizeFeature 为空结构体，所有判断只依赖传入的 PodInfo。
type podLevelResourcesResizeFeature struct{}

// Name 返回该特性的声明名，与 feature gate 常量一致。
func (f *podLevelResourcesResizeFeature) Name() string {
	return IPPRPodLevelResourcesFeatureGate
}

// Discover 直接读取 feature gate 状态：该能力完全由部署开关决定。
func (f *podLevelResourcesResizeFeature) Discover(cfg *types.NodeConfiguration) bool {
	return cfg.FeatureGates.Enabled(IPPRPodLevelResourcesFeatureGate)
}

// Requirements 把 feature gate 列为节点的必要条件，供调度侧判断节点是否声明该能力。
func (f *podLevelResourcesResizeFeature) Requirements() *types.FeatureRequirements {
	return &types.FeatureRequirements{
		EnabledFeatureGates: []string{IPPRPodLevelResourcesFeatureGate},
	}
}

// InferForScheduling 恒返回 false：该特性只在 Pod 更新时才有意义，
// 调度新 Pod 时没有“变更前”的 pod 级资源可供比较。
func (f *podLevelResourcesResizeFeature) InferForScheduling(podInfo *types.PodInfo) bool {
	return false
}

// InferForUpdate 比较新旧 PodInfo 的 Spec.Resources：两边都为 nil 时短路；
// 否则只要语义上不相等就认为发生了原地调整，需要该特性。
func (f *podLevelResourcesResizeFeature) InferForUpdate(oldPodInfo, newPodInfo *types.PodInfo) bool {
	// 两边都没配置 pod 级资源时直接短路。
	// 用语义等价比较而非地址比较，是因为 field 顺序或默认零值差异
	// 不应被误判为一次资源调整，从而引来多余的节点约束。
	if oldPodInfo.Spec.Resources == nil && newPodInfo.Spec.Resources == nil {
		return false
	}
	return !apiequality.Semantic.DeepEqual(oldPodInfo.Spec.Resources, newPodInfo.Spec.Resources)
}

// 返回 nil 表示没有版本上限：该能力不受 kubelet 版本淘汰约束。
func (f *podLevelResourcesResizeFeature) MaxVersion() *version.Version {
	return nil
}
