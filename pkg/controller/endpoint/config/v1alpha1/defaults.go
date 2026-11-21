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

// RecommendedDefaultEndpointControllerConfiguration 为 EndpointControllerConfiguration
// 指针填充推荐默认值。这些默认值可能随 API 版本变化。
// 本函数有意不注册为 scheme 中普通的 `SetDefaults_Foo`，以允许该类型的使用方
// 为嵌入的配置自行设置默认值：scheme 中的默认值填充是转换的一部分，强制使用
// 这些默认值会让使用方难以退出。若需要这些默认值，请在外层结构的
// `SetDefaults_` 方法中调用本函数。
func RecommendedDefaultEndpointControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.EndpointControllerConfiguration) {
	// 仅当未显式配置（零值）时才填充默认值，避免覆盖使用方的设置。
	if obj.ConcurrentEndpointSyncs == 0 {
		obj.ConcurrentEndpointSyncs = 5
	}
}
