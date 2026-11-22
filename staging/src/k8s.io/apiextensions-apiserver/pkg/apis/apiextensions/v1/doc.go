/*
Copyright 2019 The Kubernetes Authors.

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
// +k8s:openapi-model-package=io.k8s.apiextensions-apiserver.pkg.apis.apiextensions.v1

// +groupName=apiextensions.k8s.io

// Package v1 定义 CRD API 的 v1 版本（2019 年成为稳定版本）。
// v1 版本是 CRD 的主要版本，取代了已废弃的 v1beta1。
//
// 与内部版本的关系：
//   - 客户端发送 v1 版本的 CRD 请求 → API Server 转换为内部版本 → 存储到 etcd
//   - 从 etcd 读取内部版本 → 转换为客户端请求的版本 → 返回响应
//
// 主要类型：
//   - CustomResourceDefinition：CRD 的 v1 表示
//   - JSONSchemaProps：OpenAPI v3.0 schema 的 Go 表示
//   - CustomResourceValidation：验证配置，包括 OpenAPI schema 和 CEL 表达式
//   - CustomResourceSubresources：子资源（status、scale）配置
package v1
