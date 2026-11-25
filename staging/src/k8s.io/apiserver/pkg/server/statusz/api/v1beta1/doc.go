/*
Copyright The Kubernetes Authors.

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
// +k8s:openapi-model-package=io.k8s.apiserver.pkg.server.statusz.api.v1beta1

// Package v1beta1 包含 statusz v1beta1 API 组的 API Schema 定义。
// statusz 是 API Server 的运行时状态自省端点，其响应体在此处定义为
// 版本化 API 类型。单独建包（而不是复用 metrics）是为了让 statusz 输出拥有稳定的
// schema 与 OpenAPI 描述，从而能像普通 API 一样被客户端消费和演进；
// v1beta1 表示字段仍可能在不通知的情况下调整。
package v1beta1
