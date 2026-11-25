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
	endpointconfig "k8s.io/kubernetes/pkg/controller/endpoint/config"
)

// 重要！本包类型与 EndpointControllerConfiguration 类型之间公开的双向转换函数
// 必须像这样手动暴露出来，其他引用本包的包才能以自动生成的方式调用这些转换函数。
// TODO: 修复 conversion-gen 的缺陷，使其也能在自动生成的代码中发现这些 Convert_* 函数。

// Convert_v1alpha1_EndpointControllerConfiguration_To_config_EndpointControllerConfiguration
// 是由 conversion-gen 生成的转换函数，将外部 v1alpha1 配置转换为内部配置。
// 之所以手动包装，是因为生成代码中的 autoConvert 不会被外部包自动发现（见上方 TODO）。
func Convert_v1alpha1_EndpointControllerConfiguration_To_config_EndpointControllerConfiguration(in *v1alpha1.EndpointControllerConfiguration, out *endpointconfig.EndpointControllerConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_EndpointControllerConfiguration_To_config_EndpointControllerConfiguration(in, out, s)
}

// Convert_config_EndpointControllerConfiguration_To_v1alpha1_EndpointControllerConfiguration
// 是由 conversion-gen 生成的反向转换函数，将内部配置转换回外部 v1alpha1 配置。
// 手动暴露的原因同上：外部包需要能直接调用到生成的转换逻辑。
func Convert_config_EndpointControllerConfiguration_To_v1alpha1_EndpointControllerConfiguration(in *endpointconfig.EndpointControllerConfiguration, out *v1alpha1.EndpointControllerConfiguration, s conversion.Scope) error {
	return autoConvert_config_EndpointControllerConfiguration_To_v1alpha1_EndpointControllerConfiguration(in, out, s)
}
