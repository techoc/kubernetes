/*
Copyright 2024 The Kubernetes Authors.

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
// +k8s:defaulter-gen=TypeMeta
// +groupName=extensions.test.crd.code-generator.k8s.io
// +groupGoName=ExtensionsExample

// Package v1 是 code-generator 的 CRD 示例 API 包。
// 同时开启 deepcopy 与 defaulter 生成：defaulter 以 TypeMeta 字段为筛选条件。
package v1
