/*
Copyright 2021 The Kubernetes Authors.

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

// RecommendedDefaultEphemeralVolumeControllerConfiguration 为
// EphemeralVolumeControllerConfiguration 填充推荐的默认值；这些默认值可能随 API 版本变化。
// 该函数刻意没有以普通的 `SetDefaults_Foo` 形式注册到 scheme 中，
// 以便使用此类型的调用方能为内嵌配置自由设置自己的默认值。
// 之所以不强制使用这些默认值，是因为 scheme 的默认值填充发生在转换过程中，
// 调用方没有便捷的退出方式。想要使用此默认值逻辑时，
// 应在包裹了该类型的外层结构的 `SetDefaults_` 方法中调用它。
func RecommendedDefaultEphemeralVolumeControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.EphemeralVolumeControllerConfiguration) {
	// 仅在未配置（值为 0）时填充默认值，避免覆盖调用方的显式设置。
	if obj.ConcurrentEphemeralVolumeSyncs == 0 {
		obj.ConcurrentEphemeralVolumeSyncs = 5
	}
}
