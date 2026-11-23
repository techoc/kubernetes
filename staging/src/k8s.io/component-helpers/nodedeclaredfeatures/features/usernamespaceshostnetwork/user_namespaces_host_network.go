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

// 本文件声明 UserNamespacesHostNetworkSupport 特性：允许 pod 同时使用
// hostNetwork 与 user namespace。它同时受 kubelet feature gate 和 runtime 能力约束，
// 且只在调度推断（而非更新推断）中体现，因为相关字段不可变。
package usernamespaceshostnetwork

import (
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// 编译期断言：确保实现满足统一的 Feature 接口，
// 接口一旦新增方法就会在编译期暴露，不必等到运行时才发现。
var _ types.Feature = &userNamespacesHostNetworkFeature{}

const (
	// UserNamespacesHostNetworkSupportFeatureGate 是 kubelet 侧的特性开关名。
	// 分成 gate 名与特性名两个常量，是为了表达二者语义不同：
	// 一个是 kubelet 的开关，一个是节点对外声明的能力名称。
	UserNamespacesHostNetworkSupportFeatureGate = "UserNamespacesHostNetworkSupport"
	// UserNamespacesHostNetworkSupport 是节点对外声明的能力名称。
	UserNamespacesHostNetworkSupport = "UserNamespacesHostNetworkSupport"
)

// Feature 是 UserNamespacesHostNetworkSupport 特性的实现单例。
//
// 无状态单例：该特性只根据传入的节点配置与 pod spec 做判断，无需保存状态。
var Feature = &userNamespacesHostNetworkFeature{}

// 所有状态都来自方法参数（NodeConfiguration / PodInfo），结构体本身保持空。
type userNamespacesHostNetworkFeature struct{}

// Requirements 同时声明 gate 与 runtime 能力：
// 节点必须“开关打开且 runtime 支持”才算具备该能力，缺一不可。
func (f *userNamespacesHostNetworkFeature) Requirements() *types.FeatureRequirements {
	return &types.FeatureRequirements{
		EnabledFeatureGates: []string{UserNamespacesHostNetworkSupportFeatureGate},
		RequiredRuntimeFeatures: &types.RuntimeFeatures{
			UserNamespacesHostNetwork: true,
		},
	}
}

// Name 返回对外声明的特性名，与 Requirements 中声明的能力一一对应。
func (f *userNamespacesHostNetworkFeature) Name() string {
	return UserNamespacesHostNetworkSupport
}

// Discover 判断节点是否声明该能力。cfg 携带节点的开关与运行时能力。
// 该特性要求“特性门打开”且“runtime 支持 hostNetwork + user namespace”，缺一不可：
// 先查 gate，gate 关闭时 runtime 探测结果无意义，可提前返回。
func (f *userNamespacesHostNetworkFeature) Discover(cfg *types.NodeConfiguration) bool {
	if !cfg.FeatureGates.Enabled(UserNamespacesHostNetworkSupportFeatureGate) {
		return false
	}

	return cfg.RuntimeFeatures.UserNamespacesHostNetwork
}

// InferForScheduling 判断 Pod 是否依赖该能力：
// 只有同时使用 hostNetwork 且显式关闭 HostUsers（即启用 user namespace）时才需要。
// HostUsers=false 表示不使用宿主机 user namespace；
// nil 或 true 都表示沿用宿主机命名空间，不需要该能力。
func (f *userNamespacesHostNetworkFeature) InferForScheduling(podInfo *types.PodInfo) bool {
	if podInfo.Spec.HostNetwork && podInfo.Spec.HostUsers != nil && !*podInfo.Spec.HostUsers {
		return true
	}
	return false
}

// InferForUpdate 恒返回 false：HostNetwork 与 HostUsers 都是不可变字段，
// 更新中不会变化，推断为 true 只会造成无意义的调度约束。
func (f *userNamespacesHostNetworkFeature) InferForUpdate(oldPodInfo, newPodInfo *types.PodInfo) bool {
	return false
}

// MaxVersion 返回 nil：该能力与具体 kubelet 版本没有绑定上限。
func (f *userNamespacesHostNetworkFeature) MaxVersion() *version.Version {
	return nil
}
