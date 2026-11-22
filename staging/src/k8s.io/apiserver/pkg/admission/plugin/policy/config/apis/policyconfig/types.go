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

// ValidatingAdmissionPolicyConfiguration provides configuration for the validating admission policy controller.
// 该结构体是 apiserver 内部配置 API（非版本化），由 v1 转换而来；
// 字段变更需同步修改 v1 类型与转换函数。
type ValidatingAdmissionPolicyConfiguration struct {
	metav1.TypeMeta

	// StaticManifestsDir is the path to a directory containing static
	// ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding
	// resources to be loaded at startup. Files with extensions .yaml,
	// .yml, and .json are read. Only admissionregistration.k8s.io/v1
	// resources are supported.
	// Using this field requires the ManifestBasedAdmissionControlConfig
	// feature gate to be enabled.
	// +optional
	// 该目录在启动时一次性加载，用于在控制面不可用时也能提供准入策略；
	// 由于绕过 API 写入路径，加载内容不做准入校验，只接受 v1 版本以缩小解析面。
	StaticManifestsDir string
}

// GetStaticManifestsDir returns the static manifests directory path.
func (c *ValidatingAdmissionPolicyConfiguration) GetStaticManifestsDir() string {
	return c.StaticManifestsDir
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// MutatingAdmissionPolicyConfiguration provides configuration for the mutating admission policy controller.
// 与 ValidatingAdmissionPolicyConfiguration 结构对称，二者分别对应两类策略插件。
type MutatingAdmissionPolicyConfiguration struct {
	metav1.TypeMeta

	// StaticManifestsDir is the path to a directory containing static
	// MutatingAdmissionPolicy and MutatingAdmissionPolicyBinding
	// resources to be loaded at startup. Files with extensions .yaml,
	// .yml, and .json are read. Only admissionregistration.k8s.io/v1
	// resources are supported.
	// Using this field requires the ManifestBasedAdmissionControlConfig
	// feature gate to be enabled.
	// +optional
	StaticManifestsDir string
}

// GetStaticManifestsDir returns the static manifests directory path.
func (c *MutatingAdmissionPolicyConfiguration) GetStaticManifestsDir() string {
	return c.StaticManifestsDir
}
