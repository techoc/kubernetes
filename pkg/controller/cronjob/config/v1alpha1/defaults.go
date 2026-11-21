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

// RecommendedDefaultCronJobControllerConfiguration 为 CronJobControllerConfiguration
// 指针设置推荐默认值；这些默认值可能随 API 版本变化。
// 该函数有意不被注册为 scheme 中普通的 `SetDefaults_Foo` 函数，
// 原因是：scheme 的默认值设置发生在转换过程中，若强制使用这些默认值，
// 引用该类型的上层结构将无法为自己的嵌入配置指定默认值，也没有便捷的关闭方式。
// 因此，若需要使用该默认值逻辑，请在包装该类型的结构的 `SetDefaults_` 方法中显式调用。
func RecommendedDefaultCronJobControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.CronJobControllerConfiguration) {
	// 仅在未显式配置（零值）时应用默认值，避免覆盖用户的自定义设置。
	if obj.ConcurrentCronJobSyncs == 0 {
		obj.ConcurrentCronJobSyncs = 5
	}
}
