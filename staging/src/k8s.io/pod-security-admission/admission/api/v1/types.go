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

// 本包定义 PodSecurity Admission 配置文件（v1）的 API 类型。
// 这些类型描述准入控制器从配置文件读取的策略默认值与豁免清单，
// 与命名空间标签上的运行时配置是两套入口：此处是集群管理员的静态配置。
package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// deepcopy/codegen 标签说明：该接口标签指向 runtime.Object
// 要求生成器为该类型实现 runtime.Object 接口，使其能作为 scheme 中的注册对象被序列化。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodSecurityConfiguration 是配置文件的顶层结构，
// 把“默认策略”与“豁免名单”分开：前者定义对普通 Pod 的要求，后者定义哪些主体不受约束。
type PodSecurityConfiguration struct {
	metav1.TypeMeta `json:""`

	Defaults   PodSecurityDefaults   `json:"defaults"`
	Exemptions PodSecurityExemptions `json:"exemptions"`
}

// PodSecurityDefaults 描述三个动作各自的默认级别与策略版本。
// 级别与版本成对出现；固定版本可避免策略语义随集群升级变化。
type PodSecurityDefaults struct {
	Enforce        string `json:"enforce,omitempty"`
	EnforceVersion string `json:"enforce-version,omitempty"`
	Audit          string `json:"audit,omitempty"`
	AuditVersion   string `json:"audit-version,omitempty"`
	Warn           string `json:"warn,omitempty"`
	WarnVersion    string `json:"warn-version,omitempty"`
}

// PodSecurityExemptions 列出完全豁免策略的用户、命名空间与 RuntimeClass。
// 豁免会跳过所有检查，名单应保持精简并显式记录，避免策略被大范围架空。
type PodSecurityExemptions struct {
	Usernames      []string `json:"usernames,omitempty"`
	Namespaces     []string `json:"namespaces,omitempty"`
	RuntimeClasses []string `json:"runtimeClasses,omitempty"`
}
