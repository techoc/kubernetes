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
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kube-controller-manager/config/v1alpha1"
	"k8s.io/kubernetes/pkg/controller/volume/persistentvolume/config"
)

// 重要！本包类型与 PersistentVolumeBinderControllerConfiguration 之间的双向公开转换函数
// 必须像这样手动暴露，其他引用本包的包才能在自动生成的转换代码中调用它们。
// TODO: 修复 conversion-gen 的缺陷，使其也能在生成代码中自动发现这些 Convert_* 函数。

// Convert_v1alpha1_PersistentVolumeBinderControllerConfiguration_To_config_PersistentVolumeBinderControllerConfiguration 是自动生成的转换函数，
// 将外部 v1alpha1 配置转换为内部 PersistentVolumeBinderControllerConfiguration。
func Convert_v1alpha1_PersistentVolumeBinderControllerConfiguration_To_config_PersistentVolumeBinderControllerConfiguration(in *v1alpha1.PersistentVolumeBinderControllerConfiguration, out *config.PersistentVolumeBinderControllerConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_PersistentVolumeBinderControllerConfiguration_To_config_PersistentVolumeBinderControllerConfiguration(in, out, s)
}

// Convert_config_PersistentVolumeBinderControllerConfiguration_To_v1alpha1_PersistentVolumeBinderControllerConfiguration 是自动生成的转换函数，
// 将内部 PersistentVolumeBinderControllerConfiguration 转换为外部 v1alpha1 配置。
func Convert_config_PersistentVolumeBinderControllerConfiguration_To_v1alpha1_PersistentVolumeBinderControllerConfiguration(in *config.PersistentVolumeBinderControllerConfiguration, out *v1alpha1.PersistentVolumeBinderControllerConfiguration, s conversion.Scope) error {
	return autoConvert_config_PersistentVolumeBinderControllerConfiguration_To_v1alpha1_PersistentVolumeBinderControllerConfiguration(in, out, s)
}
