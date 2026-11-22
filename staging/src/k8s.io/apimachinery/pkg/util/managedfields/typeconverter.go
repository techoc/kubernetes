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

// TypeConverter allows you to convert from runtime.Object to
// typed.TypedValue and the other way around.
type TypeConverter = internal.TypeConverter

// NewDeducedTypeConverter creates a TypeConverter for CRDs that don't
// have a schema. It does implement the same interface though (and
// create the same types of objects), so that everything can still work
// the same. CRDs are merged with all their fields being "atomic" (lists
// included).
func NewDeducedTypeConverter() TypeConverter {
	return internal.NewDeducedTypeConverter()
}

// NewTypeConverter builds a TypeConverter from a map of OpenAPIV3 schemas.
// This will automatically find the proper version of the object, and the
// corresponding schema information.
// The keys to the map must be consistent with the names
// used by Refs within the schemas.
// The schemas should conform to the Kubernetes Structural Schema OpenAPI
// restrictions found in docs:
// https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/#specifying-a-structural-schema
func NewTypeConverter(openapiSpec map[string]*spec.Schema, preserveUnknownFields bool) (TypeConverter, error) {
	return internal.NewTypeConverter(openapiSpec, preserveUnknownFields)
}

// NewSchemeTypeConverter creates a TypeConverter that uses the provided scheme to
// convert between runtime.Objects and TypedValues.
//
// 与基于 OpenAPI 的转换器不同，它直接从 Go 类型结构推导字段模型，
// 适合内置类型；parser 决定如何处理未知字段与重复字段。
func NewSchemeTypeConverter(scheme *runtime.Scheme, parser *typed.Parser) TypeConverter {
	return internal.NewSchemeTypeConverter(scheme, parser)
}
