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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// deepcopy/codegen 标签说明：该接口标签指向 runtime.Object 使生成代码为本类型实现 DeepCopyObject，
// 从而满足 runtime.Object 接口参与 scheme 编解码；本文件保留旧版配置结构，供读取历史配置并转换到内部表示。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodSecurityConfiguration 是 v1beta1 配置根对象，加载后转换为内部表示。
type PodSecurityConfiguration struct {
	metav1.TypeMeta `json:""`

	// 默认策略用于未打标签的命名空间；三种模式各自带版本。
	// 豁免名单按用户、命名空间与运行时分类，命中即跳过策略评估。
	Defaults   PodSecurityDefaults   `json:"defaults"`
	Exemptions PodSecurityExemptions `json:"exemptions"`
}

// PodSecurityDefaults 保存未打标签命名空间所采用的默认级别与版本。
// 等级与版本分开存放，便于分别校验字符串合法性与版本可行性。
type PodSecurityDefaults struct {
	Enforce        string `json:"enforce,omitempty"`
	EnforceVersion string `json:"enforce-version,omitempty"`
	Audit          string `json:"audit,omitempty"`
	AuditVersion   string `json:"audit-version,omitempty"`
	Warn           string `json:"warn,omitempty"`
	WarnVersion    string `json:"warn-version,omitempty"`
}

// PodSecurityExemptions 列出无需执行策略的用户、命名空间与 RuntimeClass。
// 任一命中即可跳过检查；切片可省略，nil 与空列表等价。
type PodSecurityExemptions struct {
	Usernames      []string `json:"usernames,omitempty"`
	Namespaces     []string `json:"namespaces,omitempty"`
	RuntimeClasses []string `json:"runtimeClasses,omitempty"`
}
