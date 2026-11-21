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
	"k8s.io/utils/ptr"
)

// RecommendedDefaultPersistentVolumeBinderControllerConfiguration 为
// PersistentVolumeBinderControllerConfiguration 结构体指针设置推荐默认值。
// 默认值可能随 API 版本变化。该函数有意不注册到 scheme 中作为普通的
// `SetDefaults_Foo` 函数，以便该类型的使用者可以为嵌入的配置设置自己的默认值。
// 强制使用者采用这些默认值会带来问题：scheme 中的默认化是转换过程的一部分，
// 使用者没有便捷的退出方式。因此若需要使用此默认化逻辑，
// 请在包装该类型的结构的 `SetDefaults_` 方法中调用它。
func RecommendedDefaultPersistentVolumeBinderControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.PersistentVolumeBinderControllerConfiguration) {
	zero := metav1.Duration{}
	if obj.PVClaimBinderSyncPeriod == zero {
		obj.PVClaimBinderSyncPeriod = metav1.Duration{Duration: 15 * time.Second}
	}

	// 使用 VolumeConfiguration 的默认选项。
	RecommendedDefaultVolumeConfiguration(&obj.VolumeConfiguration)
}

// RecommendedDefaultVolumeConfiguration 为 VolumeConfiguration 结构体指针设置推荐默认值。
// 默认值可能随 API 版本变化。该函数有意不注册到 scheme 中作为普通的
// `SetDefaults_Foo` 函数，以便该类型的使用者可以为嵌入的配置设置自己的默认值。
// 强制使用者采用这些默认值会带来问题：scheme 中的默认化是转换过程的一部分，
// 使用者没有便捷的退出方式。因此若需要使用此默认化逻辑，
// 请在包装该类型的结构的 `SetDefaults_` 方法中调用它。
func RecommendedDefaultVolumeConfiguration(obj *kubectrlmgrconfigv1alpha1.VolumeConfiguration) {
	if obj.EnableHostPathProvisioning == nil {
		obj.EnableHostPathProvisioning = ptr.To(false)
	}
	if obj.EnableDynamicProvisioning == nil {
		obj.EnableDynamicProvisioning = ptr.To(true)
	}
	if obj.FlexVolumePluginDir == "" {
		obj.FlexVolumePluginDir = "/usr/libexec/kubernetes/kubelet-plugins/volume/exec/"
	}
	// 使用 PersistentVolumeRecyclerConfiguration 的默认选项。
	RecommendedDefaultPersistentVolumeRecyclerConfiguration(&obj.PersistentVolumeRecyclerConfiguration)
}

// RecommendedDefaultPersistentVolumeRecyclerConfiguration 为
// PersistentVolumeRecyclerConfiguration 结构体指针设置推荐默认值。
// 默认值可能随 API 版本变化。该函数有意不注册到 scheme 中作为普通的
// `SetDefaults_Foo` 函数，以便该类型的使用者可以为嵌入的配置设置自己的默认值。
// 强制使用者采用这些默认值会带来问题：scheme 中的默认化是转换过程的一部分，
// 使用者没有便捷的退出方式。因此若需要使用此默认化逻辑，
// 请在包装该类型的结构的 `SetDefaults_` 方法中调用它。
func RecommendedDefaultPersistentVolumeRecyclerConfiguration(obj *kubectrlmgrconfigv1alpha1.PersistentVolumeRecyclerConfiguration) {
	if obj.MaximumRetry == 0 {
		obj.MaximumRetry = 3
	}
	if obj.MinimumTimeoutNFS == 0 {
		obj.MinimumTimeoutNFS = 300
	}
	if obj.IncrementTimeoutNFS == 0 {
		obj.IncrementTimeoutNFS = 30
	}
	if obj.MinimumTimeoutHostPath == 0 {
		obj.MinimumTimeoutHostPath = 60
	}
	if obj.IncrementTimeoutHostPath == 0 {
		obj.IncrementTimeoutHostPath = 30
	}
}
