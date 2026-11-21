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

// RecommendedDefaultDeploymentControllerConfiguration 为 DeploymentControllerConfiguration
// 填充推荐默认值（可能会随 API 版本演进而变化）：并发 Deployment 同步数默认 5。
//
// 该函数刻意不注册为 scheme 中普通的 `SetDefaults_Foo`：scheme 的默认值设置是
// 转换的一部分，一旦强制使用便无法退出。这样使用方可以在自己的包装类型
// `SetDefaults_` 方法中按需调用或改用自己的默认值。
func RecommendedDefaultDeploymentControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.DeploymentControllerConfiguration) {
	// 仅补齐零值，避免覆盖调用方显式配置的并发数。
	if obj.ConcurrentDeploymentSyncs == 0 {
		obj.ConcurrentDeploymentSyncs = 5
	}
}
