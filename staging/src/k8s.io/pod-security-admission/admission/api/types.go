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

package api

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// 指令 +k8s:deepcopy-gen:interfaces 让生成器为它实现 runtime.Object 接口，便于放进 scheme 中解码。

// PodSecurityConfiguration 是配置文件的内部表示，各外部版本先转换成它再供准入控制器使用。
type PodSecurityConfiguration struct {
	metav1.TypeMeta
	Defaults   PodSecurityDefaults
	Exemptions PodSecurityExemptions
}

// PodSecurityDefaults 保存三个动作各自的默认级别与版本。
// enforce 真正拒绝 Pod，warn/audit 只记录，互不共用取值以免互相影响。
type PodSecurityDefaults struct {
	Enforce        string
	EnforceVersion string
	Audit          string
	AuditVersion   string
	Warn           string
	WarnVersion    string
}

// PodSecurityExemptions 列出完全跳过检查的对象维度。
// 豁免等于关闭策略，通常只用于系统组件等无法改造的工作负载。
type PodSecurityExemptions struct {
	Usernames      []string
	Namespaces     []string
	RuntimeClasses []string
}
