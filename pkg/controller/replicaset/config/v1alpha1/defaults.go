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

// RecommendedDefaultReplicaSetControllerConfiguration 为 ReplicaSetControllerConfiguration
// 指针设置推荐的默认值。推荐值可能随 API 版本变化。
// 该函数刻意没有作为普通的 `SetDefaults_Foo` 注册到 scheme 中，
// 这样该类型的使用者可以为自己嵌入的配置设置任意默认值：
// scheme 中的默认值设置是转换过程的一部分，若强制使用这些默认值，
// 使用者将无法选择退出。需要这些默认值时，应在包含该类型的包装结构的
// `SetDefaults_` 方法中调用本函数。
func RecommendedDefaultReplicaSetControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.ReplicaSetControllerConfiguration) {
	if obj.ConcurrentRSSyncs == 0 {
		obj.ConcurrentRSSyncs = 5
	}
}
