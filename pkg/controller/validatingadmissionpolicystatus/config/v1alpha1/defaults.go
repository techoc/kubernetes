/*
Copyright 2023 The Kubernetes Authors.

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

// RecommendedDefaultValidatingAdmissionPolicyStatusControllerConfiguration 为
// ValidatingAdmissionPolicyStatusControllerConfiguration 填充推荐默认值（当前为 5 个并发同步）。
// 这些默认值可能随 API 版本变化。
// 刻意不将其注册为 scheme 中普通的 `SetDefaults_Foo` 函数：scheme 的默认值处理是转换的一部分，
// 一旦强制应用，消费者就无法为自己的嵌入配置设置不同的默认值，也没有简单的退出方式。
// 需要这些默认值的消费者应在自己的包装结构体的 `SetDefaults_` 方法中显式调用它。
func RecommendedDefaultValidatingAdmissionPolicyStatusControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.ValidatingAdmissionPolicyStatusControllerConfiguration) {
	if obj.ConcurrentPolicySyncs == 0 {
		obj.ConcurrentPolicySyncs = 5
	}
}
