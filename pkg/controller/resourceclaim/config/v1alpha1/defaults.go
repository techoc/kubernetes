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

package v1alpha1

import (
	kubectrlmgrconfigv1alpha1 "k8s.io/kube-controller-manager/config/v1alpha1"
)

// RecommendedDefaultResourceClaimControllerConfiguration 为 ResourceClaimControllerConfiguration
// 设置推荐默认值，但这些默认值可能在不同 API 版本间发生变化。
// 该函数刻意没有作为普通的 `SetDefaults_Foo` 注册到 scheme 中，因为 scheme 的默认值设置
// 是转换过程的一部分，强制使用者接受这些默认值会让他们无法为嵌入的配置自行设定默认值、
// 也没有简单的退出途径。需要使用这些默认值时，应在包装该类型的结构的 `SetDefaults_` 方法中调用它。
func RecommendedDefaultResourceClaimControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.ResourceClaimControllerConfiguration) {
	if obj.ConcurrentSyncs == 0 {
		// 选 50 个 worker 是为了通过 1000 个模拟节点的 DRA 测试（#133113）；
		// 没有实际工作时这些 worker 基本处于空闲状态，因此代价很小。
		obj.ConcurrentSyncs = 50
	}
}
