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
	"k8s.io/apimachinery/pkg/conversion"
	cpconfig "k8s.io/cloud-provider/config"
)

// 重要！本包这些带有 ComponentConfig 类型的公开双向转换函数，必须像下面这样手动暴露
// 出来，引用本包的其他包才能以自动生成的方式调用它们。
// 以下手写的 Convert_* 只是对 autoConvert_* 的公开包装。conversion-gen
// 目前无法自动发现生成的转换函数，因此需要在此显式暴露，供其他包在 scheme
// 中注册并完成内部/外部版本互转。
// TODO: 修复 conversion-gen 的缺陷，让它也能在自动生成的代码中发现这些 Convert_* 函数。

// Convert_v1alpha1_KubeCloudSharedConfiguration_To_config_KubeCloudSharedConfiguration 将外部 v1alpha1 的 KubeCloudSharedConfiguration 转换为内部版本。
func Convert_v1alpha1_KubeCloudSharedConfiguration_To_config_KubeCloudSharedConfiguration(in *KubeCloudSharedConfiguration, out *cpconfig.KubeCloudSharedConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_KubeCloudSharedConfiguration_To_config_KubeCloudSharedConfiguration(in, out, s)
}

// Convert_config_KubeCloudSharedConfiguration_To_v1alpha1_KubeCloudSharedConfiguration 将内部 KubeCloudSharedConfiguration 转换回 v1alpha1，用于序列化与输出。
func Convert_config_KubeCloudSharedConfiguration_To_v1alpha1_KubeCloudSharedConfiguration(in *cpconfig.KubeCloudSharedConfiguration, out *KubeCloudSharedConfiguration, s conversion.Scope) error {
	return autoConvert_config_KubeCloudSharedConfiguration_To_v1alpha1_KubeCloudSharedConfiguration(in, out, s)
}

// Convert_v1alpha1_CloudProviderConfiguration_To_config_CloudProviderConfiguration 将外部 v1alpha1 的 CloudProviderConfiguration 转换为内部版本。
func Convert_v1alpha1_CloudProviderConfiguration_To_config_CloudProviderConfiguration(in *CloudProviderConfiguration, out *cpconfig.CloudProviderConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_CloudProviderConfiguration_To_config_CloudProviderConfiguration(in, out, s)
}

// Convert_config_CloudProviderConfiguration_To_v1alpha1_CloudProviderConfiguration 将内部 CloudProviderConfiguration 转换回 v1alpha1。
func Convert_config_CloudProviderConfiguration_To_v1alpha1_CloudProviderConfiguration(in *cpconfig.CloudProviderConfiguration, out *CloudProviderConfiguration, s conversion.Scope) error {
	return autoConvert_config_CloudProviderConfiguration_To_v1alpha1_CloudProviderConfiguration(in, out, s)
}
