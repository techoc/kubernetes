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

package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ValidatingAdmissionPolicyConfiguration 提供 validating admission policy 控制器的配置。
//
// 这是组件配置文件（--admission-control-config-file）中的片段，
// 不是可通过 API 创建的资源；使用内部版本 + v1 转换以保持向后兼容。
type ValidatingAdmissionPolicyConfiguration struct {
	metav1.TypeMeta `json:""`

	// StaticManifestsDir 指向一个目录，其中存放启动时加载的静态
	// ValidatingAdmissionPolicy 与 ValidatingAdmissionPolicyBinding 资源。
	// 读取扩展名为 .yaml、.yml 和 .json 的文件，仅支持
	// admissionregistration.k8s.io/v1 资源。
	// 使用该字段需要启用 ManifestBasedAdmissionControlConfig 特性门禁。
	// +optional
	//
	// 仅支持 admissionregistration.k8s.io/v1 是为了避免为静态清单
	// 维护多版本兼容矩阵；配置对象名必须以 .static.k8s.io 结尾。
	StaticManifestsDir string `json:"staticManifestsDir,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MutatingAdmissionPolicyConfiguration 提供 mutating admission policy 控制器的配置。
//
// 与 validating 版本结构相同但类型独立，便于两者将来独立演进。
type MutatingAdmissionPolicyConfiguration struct {
	metav1.TypeMeta `json:""`

	// StaticManifestsDir 指向一个目录，其中存放启动时加载的静态
	// MutatingAdmissionPolicy 与 MutatingAdmissionPolicyBinding 资源。
	// 读取扩展名为 .yaml、.yml 和 .json 的文件，仅支持
	// admissionregistration.k8s.io/v1 资源。
	// 使用该字段需要启用 ManifestBasedAdmissionControlConfig 特性门禁。
	// +optional
	//
	// mutating 策略可改变对象内容，因此该目录的写入权限等同于
	// 对集群内所有匹配对象的修改权限，部署时须严格限制。
	StaticManifestsDir string `json:"staticManifestsDir,omitempty"`
}
