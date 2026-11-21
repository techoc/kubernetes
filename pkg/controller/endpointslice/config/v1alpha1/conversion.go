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
	endpointsliceconfig "k8s.io/kubernetes/pkg/controller/endpointslice/config"
)

// 注意！本包类型与 EndpointSliceControllerConfiguration 之间的双向公开转换函数
// 必须像这样手动暴露，其他引用本包的包才能在自动生成的代码中调用这些转换函数。
// TODO: 修复 conversion-gen 的缺陷，使其也能在生成的代码中自动发现这些 Convert_* 函数。

// Convert_v1alpha1_EndpointSliceControllerConfiguration_To_config_EndpointSliceControllerConfiguration 是自动生成的转换函数，
// 将 v1alpha1 外部配置转换为内部 EndpointSliceControllerConfiguration。
func Convert_v1alpha1_EndpointSliceControllerConfiguration_To_config_EndpointSliceControllerConfiguration(in *v1alpha1.EndpointSliceControllerConfiguration, out *endpointsliceconfig.EndpointSliceControllerConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_EndpointSliceControllerConfiguration_To_config_EndpointSliceControllerConfiguration(in, out, s)
}

// Convert_config_EndpointSliceControllerConfiguration_To_v1alpha1_EndpointSliceControllerConfiguration 是自动生成的转换函数，
// 将内部 EndpointSliceControllerConfiguration 转换回 v1alpha1 外部配置。
func Convert_config_EndpointSliceControllerConfiguration_To_v1alpha1_EndpointSliceControllerConfiguration(in *endpointsliceconfig.EndpointSliceControllerConfiguration, out *v1alpha1.EndpointSliceControllerConfiguration, s conversion.Scope) error {
	return autoConvert_config_EndpointSliceControllerConfiguration_To_v1alpha1_EndpointSliceControllerConfiguration(in, out, s)
}
