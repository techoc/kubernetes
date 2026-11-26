/*
Copyright 2022 The Kubernetes Authors.

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
	"k8s.io/cloud-provider/controllers/node/config"
)

// 重要！本包与 ComponentConfig 类型之间的双向转换函数必须这样显式暴露，
// 引用本包的其他包才能在生成的代码中调用它们。
// TODO: 修复 conversion-gen 的 bug，让它也能自动发现这些 Convert_* 函数。

// Convert_config_NodeControllerConfiguration_To_v1alpha1_NodeControllerConfiguration 是自动生成的转换函数。
//
// 显式暴露包装函数是本包约定，见上方待办注释；转换注册表依赖这些
// 固定名称的入口来发现转换实现。
func Convert_config_NodeControllerConfiguration_To_v1alpha1_NodeControllerConfiguration(in *config.NodeControllerConfiguration, out *NodeControllerConfiguration, s conversion.Scope) error {
	return autoConvert_config_NodeControllerConfiguration_To_v1alpha1_NodeControllerConfiguration(in, out, s)
}

// Convert_v1alpha1_NodeControllerConfiguration_To_config_NodeControllerConfiguration 是自动生成的转换函数。
//
// 外部 v1alpha1 配置转为内部表示，是启动时配置加载的主路径。
func Convert_v1alpha1_NodeControllerConfiguration_To_config_NodeControllerConfiguration(in *NodeControllerConfiguration, out *config.NodeControllerConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_NodeControllerConfiguration_To_config_NodeControllerConfiguration(in, out, s)
}
