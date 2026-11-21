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
	"k8s.io/kubernetes/pkg/controller/serviceaccount/config"
)

// 重要！本包中的类型与 SAControllerConfiguration 类型之间公开的双向转换函数
// 必须像这样手动暴露出来，这样引用本包的其他包才能以自动生成的方式调用这些转换函数。
// TODO: 修复 conversion-gen 的缺陷，使其在生成的代码中也能自动发现这些 Convert_* 函数。

// Convert_v1alpha1_SAControllerConfiguration_To_config_SAControllerConfiguration 是自动生成的转换函数。
func Convert_v1alpha1_SAControllerConfiguration_To_config_SAControllerConfiguration(in *v1alpha1.SAControllerConfiguration, out *config.SAControllerConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_SAControllerConfiguration_To_config_SAControllerConfiguration(in, out, s)
}

// Convert_config_SAControllerConfiguration_To_v1alpha1_SAControllerConfiguration 是自动生成的转换函数。
func Convert_config_SAControllerConfiguration_To_v1alpha1_SAControllerConfiguration(in *config.SAControllerConfiguration, out *v1alpha1.SAControllerConfiguration, s conversion.Scope) error {
	return autoConvert_config_SAControllerConfiguration_To_v1alpha1_SAControllerConfiguration(in, out, s)
}

// Convert_v1alpha1_LegacySATokenCleanerConfiguration_To_config_LegacySATokenCleanerConfiguration 是自动生成的转换函数。
func Convert_v1alpha1_LegacySATokenCleanerConfiguration_To_config_LegacySATokenCleanerConfiguration(in *v1alpha1.LegacySATokenCleanerConfiguration, out *config.LegacySATokenCleanerConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_LegacySATokenCleanerConfiguration_To_config_LegacySATokenCleanerConfiguration(in, out, s)
}

// Convert_config_LegacySATokenCleanerConfiguration_To_v1alpha1_LegacySATokenCleanerConfiguration 是自动生成的转换函数。
func Convert_config_LegacySATokenCleanerConfiguration_To_v1alpha1_LegacySATokenCleanerConfiguration(in *config.LegacySATokenCleanerConfiguration, out *v1alpha1.LegacySATokenCleanerConfiguration, s conversion.Scope) error {
	return autoConvert_config_LegacySATokenCleanerConfiguration_To_v1alpha1_LegacySATokenCleanerConfiguration(in, out, s)
}
