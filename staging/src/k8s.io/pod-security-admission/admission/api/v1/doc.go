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
// 指令 +k8s:deepcopy-gen 与 +k8s:conversion-gen 负责生成深拷贝和跨版本转换，
// 指令 +groupName 指定了配置文件使用的 API 组名。

// Package v1 contains PodSecurity admission configuration file types
// 本包是 PodSecurity 准入配置文件的稳定版本，定义用户写入磁盘的外部类型。
package v1
