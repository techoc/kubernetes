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

package extendwebsocketstokubelet

import (
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// 编译期断言确保特性实现没有偏离 Feature 接口；
// 接口是框架统一调度各特性的前提，漏实现方法会导致运行时才暴露问题。
var _ types.Feature = &extendWebSocketsToKubeletFeature{}

const (
	// ExtendWebSocketsToKubeletFeatureGate 是“kubelet WebSocket 传输增强”的开关名，
	// 同时作为对外声明的特性名；导出后供节点侧与测试共用，避免字符串各写一遍。
	ExtendWebSocketsToKubeletFeatureGate = "ExtendWebSocketsToKubelet"
)

// Feature 是 ExtendWebSocketsToKubelet 特性的实现单例。
// 导出单例供框架注册使用；实现类型不导出，外部只能通过这个实例访问。
var Feature = &extendWebSocketsToKubeletFeature{}

// extendWebSocketsToKubeletFeature 为空结构体：所有判断都只依赖传入参数，无需保存状态。
type extendWebSocketsToKubeletFeature struct{}

// Name 返回该特性的声明名，与 feature gate 常量一致。
func (f *extendWebSocketsToKubeletFeature) Name() string {
	return ExtendWebSocketsToKubeletFeatureGate
}

// Requirements 返回节点前提。节点要提供该能力，必须显式打开同名 feature gate。
// Requirements 与 Discover 共用同一个常量，避免开关名出现两处定义后不一致。
func (f *extendWebSocketsToKubeletFeature) Requirements() *types.FeatureRequirements {
	return &types.FeatureRequirements{
		EnabledFeatureGates: []string{ExtendWebSocketsToKubeletFeatureGate},
	}
}

// Discover 直接读取 feature gate 状态：该能力完全由部署开关决定，
// 没有额外的运行时探测信号，开关打开即认为节点支持。
func (f *extendWebSocketsToKubeletFeature) Discover(cfg *types.NodeConfiguration) bool {
	return cfg.FeatureGates.Enabled(ExtendWebSocketsToKubeletFeatureGate)
}

// InferForScheduling 恒返回 false：WebSocket 是传输层能力，
// 对 Pod 透明，不影响 Pod 应该落在哪个节点上。
func (f *extendWebSocketsToKubeletFeature) InferForScheduling(podInfo *types.PodInfo) bool {
	return false
}

// InferForUpdate 恒返回 false：该传输能力与 Pod 更新无关，
// 不会因 PodSpec 变化而产生新的节点能力需求。
func (f *extendWebSocketsToKubeletFeature) InferForUpdate(oldPodInfo, newPodInfo *types.PodInfo) bool {
	return false
}

// MaxVersion 返回 nil，表示该特性没有版本上限：不受 kubelet 版本淘汰约束，
// 只要开关打开就一直声明，框架无需做版本裁剪。
func (f *extendWebSocketsToKubeletFeature) MaxVersion() *version.Version {
	return nil
}
