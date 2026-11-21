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

// RecommendedDefaultReplicationControllerConfiguration 为
// ReplicationControllerConfiguration 设置推荐默认值，值可能随 API 版本变化。
// 该函数故意不注册为 scheme 中普通的 `SetDefaults_Foo`：默认值处理发生在转换过程中，
// 强制套用会使用户无法为自己的嵌入配置选择不同默认值。需要时用调用方应在包装
// 结构的 `SetDefaults_` 方法中显式调用。
func RecommendedDefaultReplicationControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.ReplicationControllerConfiguration) {
	if obj.ConcurrentRCSyncs == 0 {
		obj.ConcurrentRCSyncs = 5
	}
}
