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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// deepcopy/codegen 标签说明：该接口标签指向 runtime.Object 使生成代码为该类型实现 DeepCopyObject，
// 从而满足 runtime.Object 接口并参与 scheme 编解码。
// 本文件保留旧版配置结构，仅供读取历史配置并转换到内部表示；新代码应使用 v1。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodSecurityConfiguration 是 v1alpha1 配置根对象，用于读取历史配置并转换到内部表示。
type PodSecurityConfiguration struct {
	metav1.TypeMeta `json:""`

	Defaults   PodSecurityDefaults   `json:"defaults"`
	Exemptions PodSecurityExemptions `json:"exemptions"`
}

// PodSecurityDefaults 保存未打标签命名空间所采用的默认级别与版本。
type PodSecurityDefaults struct {
	Enforce        string `json:"enforce,omitempty"`
	EnforceVersion string `json:"enforce-version,omitempty"`
	Audit          string `json:"audit,omitempty"`
	AuditVersion   string `json:"audit-version,omitempty"`
	Warn           string `json:"warn,omitempty"`
	WarnVersion    string `json:"warn-version,omitempty"`
}

// PodSecurityExemptions 列出跳过策略检查的用户、命名空间与 RuntimeClass。
type PodSecurityExemptions struct {
	Usernames      []string `json:"usernames,omitempty"`
	Namespaces     []string `json:"namespaces,omitempty"`
	RuntimeClasses []string `json:"runtimeClasses,omitempty"`
}
