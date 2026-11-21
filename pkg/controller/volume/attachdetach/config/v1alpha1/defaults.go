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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubectrlmgrconfigv1alpha1 "k8s.io/kube-controller-manager/config/v1alpha1"
)

// RecommendedDefaultAttachDetachControllerConfiguration 为
// AttachDetachControllerConfiguration 指针设置推荐默认值。默认值可能随 API 版本变化。
// 该函数有意不注册为 scheme 中的普通 `SetDefaults_Foo` 函数，
// 以便使用该类型的调用方为内嵌配置自行设置默认值。
// 强制使用这些默认值会有问题：scheme 中的默认值设置是转换的一部分，调用方难以选择退出。
// 因此，若想使用此默认逻辑，应在包装该类型的结构的 `SetDefaults_` 方法中调用它。
func RecommendedDefaultAttachDetachControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.AttachDetachControllerConfiguration) {
	zero := metav1.Duration{}
	// 仅当未显式配置时才填充默认值，避免覆盖用户设置。
	if obj.ReconcilerSyncLoopPeriod == zero {
		obj.ReconcilerSyncLoopPeriod = metav1.Duration{Duration: 60 * time.Second}
	}
}
