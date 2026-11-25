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
	"k8s.io/kubernetes/pkg/controller/ttlafterfinished/config"
)

// 重要！本包类型与 TTLAfterFinishedControllerConfiguration 之间的双向转换函数
// 必须像这样手动公开，其他引用本包的包才能在生成的转换代码中调用它们。
// TODO: 修复 conversion-gen 的缺陷，使其也能在自动生成的代码中发现这些 Convert_* 函数。

// Convert_v1alpha1_TTLAfterFinishedControllerConfiguration_To_config_TTLAfterFinishedControllerConfiguration 是生成的外部版本到内部版本的转换函数；
// 在此手动公开，供生成代码调用。
func Convert_v1alpha1_TTLAfterFinishedControllerConfiguration_To_config_TTLAfterFinishedControllerConfiguration(in *v1alpha1.TTLAfterFinishedControllerConfiguration, out *config.TTLAfterFinishedControllerConfiguration, s conversion.Scope) error {
	return autoConvert_v1alpha1_TTLAfterFinishedControllerConfiguration_To_config_TTLAfterFinishedControllerConfiguration(in, out, s)
}

// Convert_config_TTLAfterFinishedControllerConfiguration_To_v1alpha1_TTLAfterFinishedControllerConfiguration 是生成的内部版本到外部版本的转换函数；
// 在此手动公开，供生成代码调用。
func Convert_config_TTLAfterFinishedControllerConfiguration_To_v1alpha1_TTLAfterFinishedControllerConfiguration(in *config.TTLAfterFinishedControllerConfiguration, out *v1alpha1.TTLAfterFinishedControllerConfiguration, s conversion.Scope) error {
	return autoConvert_config_TTLAfterFinishedControllerConfiguration_To_v1alpha1_TTLAfterFinishedControllerConfiguration(in, out, s)
}
