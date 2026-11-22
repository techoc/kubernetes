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
// +groupName=apiextensions.k8s.io

// Package apiextensions 定义 CRD API 的内部版本类型。
// 内部版本是 API Server 内部使用的"真实"表示，不直接暴露给外部 API。
// 所有外部版本（v1、v1beta1）在接收到请求后，会转换为内部版本进行处理。
//
// 主要类型：
//   - CustomResourceDefinition：CRD 的核心类型，定义 CR 的组、版本、种类和 schema
//   - JSONSchemaProps：OpenAPI v3.0 schema 的 Go 表示，用于 CR 的验证
//   - CustomResourceColumnDefinition：kubectl get 的打印列定义
package apiextensions
