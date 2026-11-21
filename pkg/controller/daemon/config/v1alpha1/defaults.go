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

// RecommendedDefaultDaemonSetControllerConfiguration 为 DaemonSetControllerConfiguration
// 设置推荐的默认值（这些值可能随 API 版本变化）。该函数有意不注册为 scheme 中普通的
// `SetDefaults_Foo` 函数，以便使用方为嵌入的配置自行设定默认值：scheme 中的默认值
// 处理是转换过程的一部分，强制使用这些默认值会让使用方难以退出。若需要应用此默认值，
// 请在使用方包装结构体的 `SetDefaults_` 方法中调用本函数。
func RecommendedDefaultDaemonSetControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.DaemonSetControllerConfiguration) {
	// 仅在未配置时填默认值，避免覆盖使用方显式传入的 0 以外的值。
	if obj.ConcurrentDaemonSetSyncs == 0 {
		obj.ConcurrentDaemonSetSyncs = 2
	}
}
