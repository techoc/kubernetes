/*
Copyright 2019 The Kubernetes Authors.

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

// RecommendedDefaultEndpointSliceControllerConfiguration 为 EndpointSliceControllerConfiguration
// 结构体指针设置推荐默认值。这些默认值可能随 API 版本变化。
// 该函数刻意没有以常规的 `SetDefaults_Foo` 形式注册到 scheme 中，
// 以便该类型的使用者能为自身嵌入的配置自由设定默认值。
// 强制使用这套默认值会带来问题：scheme 中的默认值设定是转换过程的一部分，
// 使用者将无法便捷地退出。若想采用此默认逻辑，请在外层包装结构体的
// `SetDefaults_` 方法中调用它。
func RecommendedDefaultEndpointSliceControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.EndpointSliceControllerConfiguration) {
	if obj.ConcurrentServiceEndpointSyncs == 0 {
		obj.ConcurrentServiceEndpointSyncs = 5
	}

	if obj.MaxEndpointsPerSlice == 0 {
		obj.MaxEndpointsPerSlice = 100
	}
}
