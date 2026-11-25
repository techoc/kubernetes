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

package policyconfig

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ValidatingAdmissionPolicyConfiguration 提供 validating admission policy 控制器的配置。
// 该结构体是 apiserver 内部配置 API（非版本化），由 v1 转换而来；
// 字段变更需同步修改 v1 类型与转换函数。
type ValidatingAdmissionPolicyConfiguration struct {
	metav1.TypeMeta

	// StaticManifestsDir 指向一个目录，其中存放启动时加载的静态
	// ValidatingAdmissionPolicy 与 ValidatingAdmissionPolicyBinding 资源。
	// 读取扩展名为 .yaml、.yml 和 .json 的文件，仅支持
	// admissionregistration.k8s.io/v1 资源。
	// 使用该字段需要启用 ManifestBasedAdmissionControlConfig 特性门禁。
	// +optional
	// 该目录在启动时一次性加载，用于在控制面不可用时也能提供准入策略；
	// 由于绕过 API 写入路径，加载内容不做准入校验，只接受 v1 版本以缩小解析面。
	StaticManifestsDir string
}

// GetStaticManifestsDir 返回静态清单目录的路径。
func (c *ValidatingAdmissionPolicyConfiguration) GetStaticManifestsDir() string {
	return c.StaticManifestsDir
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MutatingAdmissionPolicyConfiguration 提供 mutating admission policy 控制器的配置。
// 与 ValidatingAdmissionPolicyConfiguration 结构对称，二者分别对应两类策略插件。
type MutatingAdmissionPolicyConfiguration struct {
	metav1.TypeMeta

	// StaticManifestsDir 指向一个目录，其中存放启动时加载的静态
	// MutatingAdmissionPolicy 与 MutatingAdmissionPolicyBinding 资源。
	// 读取扩展名为 .yaml、.yml 和 .json 的文件，仅支持
	// admissionregistration.k8s.io/v1 资源。
	// 使用该字段需要启用 ManifestBasedAdmissionControlConfig 特性门禁。
	// +optional
	StaticManifestsDir string
}

// GetStaticManifestsDir 返回静态清单目录的路径。
func (c *MutatingAdmissionPolicyConfiguration) GetStaticManifestsDir() string {
	return c.StaticManifestsDir
}
