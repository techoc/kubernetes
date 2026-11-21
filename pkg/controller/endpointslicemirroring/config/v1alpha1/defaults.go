/*
Copyright 2020 The Kubernetes Authors.

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

// RecommendedDefaultEndpointSliceMirroringControllerConfiguration 为
// EndpointSliceMirroringControllerConfiguration 设置推荐默认值；这些值可能随 API 版本变化。
// 该函数特意没有作为普通的 `SetDefaults_Foo` 注册到 scheme 中：scheme 的默认值设置
// 发生在转换过程中，若强制使用这里的值，消费方无法为内嵌配置自定义默认值或选择退出。
// 需要使用这些默认值时，应在包装结构体的 `SetDefaults_` 方法中调用本函数。
func RecommendedDefaultEndpointSliceMirroringControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.EndpointSliceMirroringControllerConfiguration) {
	// 零值表示用户未指定，填入推荐的镜像同步并发度。
	if obj.MirroringConcurrentServiceEndpointSyncs == 0 {
		obj.MirroringConcurrentServiceEndpointSyncs = 5
	}

	// 限制单个 subset 的 endpoint 数，避免一次同步产生过大的 EndpointSlice。
	if obj.MirroringMaxEndpointsPerSubset == 0 {
		obj.MirroringMaxEndpointsPerSubset = 1000
	}
}
