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
	"k8s.io/kubernetes/pkg/controller/replication/config"
)

// 重要：本包类型与 ReplicationControllerConfiguration 之间的双向转换函数必须像这样手动暴露，
// 否则其他包无法以自动生成的方式调用它们。
// TODO: 修复 conversion-gen 的缺陷，使其也能在自动生成的代码中发现这些 Convert_* 函数。

// Convert_v1alpha1_ReplicationControllerConfiguration_To_config_ReplicationControllerConfiguration 是自动生成的转换函数，
// 将 v1alpha1 外部版本配置转换为内部 ReplicationControllerConfiguration。
// 手动暴露它是为了让生成代码中的调用方能找到该符号（见文件头说明）。
func Convert_v1alpha1_ReplicationControllerConfiguration_To_config_ReplicationControllerConfiguration(in *v1alpha1.ReplicationControllerConfiguration, out *config.ReplicationControllerConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_ReplicationControllerConfiguration_To_config_ReplicationControllerConfiguration(in, out, s)
}

// Convert_config_ReplicationControllerConfiguration_To_v1alpha1_ReplicationControllerConfiguration 是自动生成的转换函数，
// 将内部配置转换回 v1alpha1 外部版本，与上面的函数共同构成往返转换。
func Convert_config_ReplicationControllerConfiguration_To_v1alpha1_ReplicationControllerConfiguration(in *config.ReplicationControllerConfiguration, out *v1alpha1.ReplicationControllerConfiguration, s conversion.Scope) error {
	return autoConvert_config_ReplicationControllerConfiguration_To_v1alpha1_ReplicationControllerConfiguration(in, out, s)
}
