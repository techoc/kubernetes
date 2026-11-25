/*
Copyright 2018 The Kubernetes Authors.

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

// Package managedfields 提供服务端 apply（SSA）所需的字段管理能力。
//
// 该包是 internal 实现之上的稳定门面：对外暴露 TypeConverter 与 FieldManager
// 等入口，避免调用方直接依赖 internal 包。
package managedfields

import (
	"sigs.k8s.io/structured-merge-diff/v6/typed"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/managedfields/internal"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// TypeConverter 用于在 runtime.Object 与 typed.TypedValue 之间双向转换。
type TypeConverter = internal.TypeConverter

// NewDeducedTypeConverter 为没有 schema 的 CRD 创建 TypeConverter；
// 它实现相同接口并创建相同类型的对象，因此行为保持一致。
// CRD 的所有字段（包括列表）均按 "atomic" 合并。
func NewDeducedTypeConverter() TypeConverter {
	return internal.NewDeducedTypeConverter()
}

// NewTypeConverter 根据 OpenAPIV3 schema 映射构建 TypeConverter。
// 它会自动查找对象的正确版本及对应的 schema 信息。
// 映射的键必须与 schema 中 Refs 使用的名称一致。
// schema 应符合 Kubernetes Structural Schema 的 OpenAPI 限制，
// 详见文档：
// https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#specifying-a-structural-schema
func NewTypeConverter(openapiSpec map[string]*spec.Schema, preserveUnknownFields bool) (TypeConverter, error) {
	return internal.NewTypeConverter(openapiSpec, preserveUnknownFields)
}

// NewSchemeTypeConverter 创建使用给定 scheme 的 TypeConverter，
// 用于在 runtime.Object 与 TypedValue 之间转换。
//
// 与基于 OpenAPI 的转换器不同，它直接从 Go 类型结构推导字段模型，
// 适合内置类型；parser 决定如何处理未知字段与重复字段。
func NewSchemeTypeConverter(scheme *runtime.Scheme, parser *typed.Parser) TypeConverter {
	return internal.NewSchemeTypeConverter(scheme, parser)
}
