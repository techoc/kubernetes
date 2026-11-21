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

// RecommendedDefaultJobControllerConfiguration 为 JobControllerConfiguration 设置推荐默认值，
// 这些值可能随 API 版本变化。
// 这里刻意不把它注册为 scheme 中普通的 `SetDefaults_Foo` 函数：scheme 的默认值处理
// 是转换流程的一部分，一旦强制调用就没有便捷的退出方式，会剥夺调用方为嵌入配置
// 自行设定默认值的能力。需要的调用方应在自己的包装结构上，于 `SetDefaults_` 方法中显式调用它。
func RecommendedDefaultJobControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.JobControllerConfiguration) {
	// 仅在零值时填充，保留调用方显式指定的配置。
	if obj.ConcurrentJobSyncs == 0 {
		obj.ConcurrentJobSyncs = 5
	}
}
