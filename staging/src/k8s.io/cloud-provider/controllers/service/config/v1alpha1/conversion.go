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
	serviceconfig "k8s.io/cloud-provider/controllers/service/config"
)

// 重要！本包与 ComponentConfig 类型之间的双向转换函数必须这样显式暴露，
// 引用本包的其他包才能在生成的代码中调用它们。
// TODO: 修复 conversion-gen 的 bug，让它也能自动发现这些 Convert_* 函数。

// Convert_v1alpha1_ServiceControllerConfiguration_To_config_ServiceControllerConfiguration 是自动生成的转换函数。
//
// 显式包装生成函数是本包的约定，见上方待办注释；不要绕过包装直接调用
// autoConvert，否则转换注册表无法发现该入口。
func Convert_v1alpha1_ServiceControllerConfiguration_To_config_ServiceControllerConfiguration(in *ServiceControllerConfiguration, out *serviceconfig.ServiceControllerConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_ServiceControllerConfiguration_To_config_ServiceControllerConfiguration(in, out, s)
}

// Convert_config_ServiceControllerConfiguration_To_v1alpha1_ServiceControllerConfiguration 是自动生成的转换函数。
//
// 内部配置转为外部版本，用于 /configz 等场景的序列化。
func Convert_config_ServiceControllerConfiguration_To_v1alpha1_ServiceControllerConfiguration(in *serviceconfig.ServiceControllerConfiguration, out *ServiceControllerConfiguration, s conversion.Scope) error {
	return autoConvert_config_ServiceControllerConfiguration_To_v1alpha1_ServiceControllerConfiguration(in, out, s)
}
