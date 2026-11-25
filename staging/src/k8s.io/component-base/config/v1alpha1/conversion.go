/*
Copyright 2018 The Kubernetes Authors.

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
	"k8s.io/component-base/config"
)

// 重要：本通用包中 ComponentConfig 类型的公开双向转换函数必须像这样手动暴露，
// 引用本包的其他包才能以自动生成的方式调用它们。
// TODO：修复 conversion-gen，使其在自动生成代码中也能发现这些 Convert_* 函数。

func Convert_v1alpha1_ClientConnectionConfiguration_To_config_ClientConnectionConfiguration(in *ClientConnectionConfiguration, out *config.ClientConnectionConfiguration, s conversion.Scope) error {
	// 包装自动生成函数，使其可被 conversion-gen 在其它包中作为已知转换发现；
	// 逻辑本身完全由 autoConvert_* 承担。
	return autoConvert_v1alpha1_ClientConnectionConfiguration_To_config_ClientConnectionConfiguration(in, out, s)
}

// Convert_config_ClientConnectionConfiguration_To_v1alpha1_ClientConnectionConfiguration 将内部客户端连接配置转回 v1alpha1。
func Convert_config_ClientConnectionConfiguration_To_v1alpha1_ClientConnectionConfiguration(in *config.ClientConnectionConfiguration, out *ClientConnectionConfiguration, s conversion.Scope) error {
	return autoConvert_config_ClientConnectionConfiguration_To_v1alpha1_ClientConnectionConfiguration(in, out, s)
}

// Convert_v1alpha1_DebuggingConfiguration_To_config_DebuggingConfiguration 将 v1alpha1 调试配置转换为内部表示。
func Convert_v1alpha1_DebuggingConfiguration_To_config_DebuggingConfiguration(in *DebuggingConfiguration, out *config.DebuggingConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_DebuggingConfiguration_To_config_DebuggingConfiguration(in, out, s)
}

// Convert_config_DebuggingConfiguration_To_v1alpha1_DebuggingConfiguration 将内部调试配置转换为 v1alpha1。
func Convert_config_DebuggingConfiguration_To_v1alpha1_DebuggingConfiguration(in *config.DebuggingConfiguration, out *DebuggingConfiguration, s conversion.Scope) error {
	return autoConvert_config_DebuggingConfiguration_To_v1alpha1_DebuggingConfiguration(in, out, s)
}

// Convert_v1alpha1_LeaderElectionConfiguration_To_config_LeaderElectionConfiguration 将 v1alpha1 leader election 配置转换为内部表示。
func Convert_v1alpha1_LeaderElectionConfiguration_To_config_LeaderElectionConfiguration(in *LeaderElectionConfiguration, out *config.LeaderElectionConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_LeaderElectionConfiguration_To_config_LeaderElectionConfiguration(in, out, s)
}

// Convert_config_LeaderElectionConfiguration_To_v1alpha1_LeaderElectionConfiguration 将内部 leader election 配置转换为 v1alpha1。
func Convert_config_LeaderElectionConfiguration_To_v1alpha1_LeaderElectionConfiguration(in *config.LeaderElectionConfiguration, out *LeaderElectionConfiguration, s conversion.Scope) error {
	return autoConvert_config_LeaderElectionConfiguration_To_v1alpha1_LeaderElectionConfiguration(in, out, s)
}
