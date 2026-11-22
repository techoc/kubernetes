/*
Copyright 2021 The Kubernetes Authors.

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
	conversion "k8s.io/apimachinery/pkg/conversion"
	apiserver "k8s.io/apiserver/pkg/apis/apiserver"
)

// Convert_v1alpha1_EgressSelection_To_apiserver_EgressSelection 在自动生成的
// 转换之上修正了出站选择器的名称。
//
// v1alpha1 早于 "controlplane" 这个命名，历史上用 "master" 表示控制面流量。
// 转换时把旧名称改写为新名称，使得内部类型只处理规范化的取值；反向转换
// （internal -> v1alpha1）不做还原，避免把已弃用的名字重新写回配置。
func Convert_v1alpha1_EgressSelection_To_apiserver_EgressSelection(in *EgressSelection, out *apiserver.EgressSelection, s conversion.Scope) error {
	if err := autoConvert_v1alpha1_EgressSelection_To_apiserver_EgressSelection(in, out, s); err != nil {
		return err
	}
	if out.Name == "master" {
		out.Name = "controlplane"
	}
	return nil
}
