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
// +k8s:openapi-gen=true
// +k8s:defaulter-gen=TypeMeta
// +k8s:openapi-model-package=io.k8s.apimachinery.pkg.apis.meta.v1beta1

// +groupName=meta.k8s.io

// Package v1beta1 提供 Kubernetes 元数据的 v1beta1 版本类型，
// 用于兼容仍依赖该版本 API 的客户端。
// 上方生成器指令声明了 deepcopy、OpenAPI 与默认值的生成配置。
package v1beta1
