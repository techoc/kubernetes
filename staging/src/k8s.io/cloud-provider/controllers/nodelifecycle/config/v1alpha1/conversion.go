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
	"k8s.io/cloud-provider/controllers/nodelifecycle/config"
)

// 重要！本包这些带有 ComponentConfig 类型的公开双向转换函数，必须像下面这样手动暴露
// 出来，引用本包的其他包才能以自动生成的方式调用它们。
// 这些公开的 Convert_* 函数包装自动生成的 autoConvert_*，因为
// conversion-gen 尚不能自动发现生成代码中的转换函数。
// TODO: 修复 conversion-gen 的缺陷，让它也能在自动生成的代码中发现这些 Convert_* 函数。

// Convert_config_NodeLifecycleControllerConfiguration_To_v1alpha1_NodeLifecycleControllerConfiguration 是供生成代码调用的公开转换函数。
// 内部配置转外部版本，用于序列化组件配置。
func Convert_config_NodeLifecycleControllerConfiguration_To_v1alpha1_NodeLifecycleControllerConfiguration(in *config.NodeLifecycleControllerConfiguration, out *NodeLifecycleControllerConfiguration, s conversion.Scope) error {
	return autoConvert_config_NodeLifecycleControllerConfiguration_To_v1alpha1_NodeLifecycleControllerConfiguration(in, out, s)
}

// Convert_v1alpha1_NodeLifecycleControllerConfiguration_To_config_NodeLifecycleControllerConfiguration 是供生成代码调用的公开转换函数。
// 外部版本转内部配置，组件启动时走此路径。
func Convert_v1alpha1_NodeLifecycleControllerConfiguration_To_config_NodeLifecycleControllerConfiguration(in *NodeLifecycleControllerConfiguration, out *config.NodeLifecycleControllerConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_NodeLifecycleControllerConfiguration_To_config_NodeLifecycleControllerConfiguration(in, out, s)
}
