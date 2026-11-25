/*
Copyright The Kubernetes Authors.

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
	disruptionconfig "k8s.io/kubernetes/pkg/controller/disruption/config"
)

// 重要！本包类型与 DisruptionControllerConfiguration 之间的双向转换函数
// 必须以这种方式手动暴露，引用本包的其他包才能在自动生成的转换代码中调用它们。
// TODO: 修复 conversion-gen 的缺陷，使其也能在自动生成的代码中发现这些 Convert_* 函数。

// Convert_v1alpha1_DisruptionControllerConfiguration_To_config_DisruptionControllerConfiguration 是自动生成的转换函数，将外部 v1alpha1 配置转换为内部配置。
func Convert_v1alpha1_DisruptionControllerConfiguration_To_config_DisruptionControllerConfiguration(in *v1alpha1.DisruptionControllerConfiguration, out *disruptionconfig.DisruptionControllerConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_DisruptionControllerConfiguration_To_config_DisruptionControllerConfiguration(in, out, s)
}

// Convert_config_DisruptionControllerConfiguration_To_v1alpha1_DisruptionControllerConfiguration 是自动生成的转换函数，将内部配置转换为外部 v1alpha1 配置。
func Convert_config_DisruptionControllerConfiguration_To_v1alpha1_DisruptionControllerConfiguration(in *disruptionconfig.DisruptionControllerConfiguration, out *v1alpha1.DisruptionControllerConfiguration, s conversion.Scope) error {
	return autoConvert_config_DisruptionControllerConfiguration_To_v1alpha1_DisruptionControllerConfiguration(in, out, s)
}
