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

// +k8s:deepcopy-gen=package
// +k8s:conversion-gen=k8s.io/pod-security-admission/admission/api
// +k8s:defaulter-gen=TypeMeta
// +k8s:validation-gen=false
// +groupName=pod-security.admission.config.k8s.io
// 生成器按 deepcopy-gen 与 conversion-gen 指令生成深拷贝和跨版本转换，
// 并按 groupName 指令确定配置文件使用的 API 组名。

// Package v1 包含 PodSecurity 准入配置文件稳定版本的外部类型。
package v1
