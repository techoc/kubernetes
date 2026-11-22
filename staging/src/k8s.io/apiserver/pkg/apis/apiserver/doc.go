/*
Copyright 2017 The Kubernetes Authors.

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
// +groupName=apiserver.k8s.io

// Package apiserver is the internal version of the API.
//
// 这里是 apiserver 组件配置（admission、认证、授权、tracing 等）的内部
// （未版本化）API 类型。内部版本只用于组件内部处理，不对外暴露：外部 YAML
// 先经 v1/v1alpha1/v1beta1 解码，再由 conversion 转成这里的类型，因此字段
// 可以是 Go 原生类型，而不必遵守 API 版本兼容规则。
package apiserver
