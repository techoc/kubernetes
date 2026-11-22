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
// +k8s:protobuf-gen=package
// +k8s:conversion-gen=k8s.io/apiextensions-apiserver/pkg/apis/apiextensions
// +k8s:defaulter-gen=TypeMeta
// +k8s:openapi-gen=true
// +k8s:prerelease-lifecycle-gen=true
// +k8s:openapi-model-package=io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1beta1

// +groupName=apiextensions.k8s.io

// Package v1beta1 定义 CRD API 的 v1beta1 版本（已废弃）。
// v1beta1 在 1.16 版本后被 v1 取代，但仍保留以支持向后兼容。
// 新创建的 CRD 应使用 v1 版本。
//
// 与 v1 的主要差异：
//   - v1beta1 不支持 per-version schema、subresources 和 printer columns
//   - v1beta1 使用顶层 spec.validation 而不是 per-version schema
//   - v1beta1 的 conversion webhook 结构与 v1 不同
package v1beta1
