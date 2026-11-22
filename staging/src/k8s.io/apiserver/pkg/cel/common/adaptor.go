/*
Copyright 2023 The Kubernetes Authors.

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

package common

// Schema is the adapted type for an OpenAPI schema that CEL uses.
// This schema does not cover all OpenAPI fields but only these CEL requires
// are exposed as getters.
//
// 中文说明：这是一个抽象适配层，屏蔽内部 OpenAPI 与结构化 schema 的差异，
// 使 CEL 类型推导只依赖只读 getter。所有方法都应视作纯读：实现方可能被多个
// 编译/校验 goroutine 共享。
type Schema interface {
	// Type returns the OpenAPI type.
	// Multiple types are not supported. It should return
	// empty string if no type is specified.
	Type() string

	// Format returns the OpenAPI format. May be empty
	Format() string

	// Items returns the OpenAPI items. or nil of this field does not exist or
	// contains no schema.
	Items() Schema

	// Properties returns the OpenAPI properties, or nil if this field does not
	// exist.
	// The values of the returned map are of the adapted type.
	Properties() map[string]Schema

	// AdditionalProperties returns the OpenAPI additional properties field,
	// or nil if this field does not exist.
	AdditionalProperties() SchemaOrBool

	// Default returns the OpenAPI default field, or nil if this field does not exist.
	Default() any

	Validations
	KubeExtensions

	// WithTypeAndObjectMeta returns a schema that has the type and object meta set.
	// the type includes "kind", "apiVersion" field
	// the "metadata" field requires "name" and "generateName" to be set
	// The original schema must not be mutated. Make a copy if necessary.
	//
	// 中文说明：这是“不可变”契约的关键：调用方可能在共享 schema 上调用本方法，
	// 实现必须返回副本。
	WithTypeAndObjectMeta() Schema
}

// Validations contains OpenAPI validation that the CEL library uses.
// 中文说明：这些 getter 直接映射 OpenAPI 校验字段，CEL 用它推导默认值与约束，
// 因此缺省语义（如是否设置、exclusive 的默认值）必须与 OpenAPI 一致。
type Validations interface {
	Pattern() string
	Minimum() *float64
	IsExclusiveMinimum() bool
	Maximum() *float64
	IsExclusiveMaximum() bool
	MultipleOf() *float64
	MinItems() *int64
	MaxItems() *int64
	MinLength() *int64
	MaxLength() *int64
	MinProperties() *int64
	MaxProperties() *int64
	Required() []string
	Enum() []any
	Nullable() bool
	UniqueItems() bool

	AllOf() []Schema
	OneOf() []Schema
	AnyOf() []Schema
	Not() Schema
}

// KubeExtensions contains Kubernetes-specific extensions to the OpenAPI schema.
// 中文说明：x-kubernetes-* 扩展决定了 CEL 对列表/映射的语义（merge key、原子性等），
// 与 API 的 server-side apply 行为保持一致。
type KubeExtensions interface {
	IsXIntOrString() bool
	IsXEmbeddedResource() bool
	IsXPreserveUnknownFields() bool
	XListType() string
	XListMapKeys() []string
	XMapType() string
	XValidations() []ValidationRule
}

// ValidationRule represents a single x-kubernetes-validations rule.
// 中文说明：只暴露规则原文与其元数据，不求值；求值由 CEL 运行时负责，
// 以便编译结果可缓存。
type ValidationRule interface {
	Rule() string
	Message() string
	MessageExpression() string
	FieldPath() string
}

// SchemaOrBool contains either a schema or a boolean indicating if the object
// can contain any fields.
// 中文说明：对应 OpenAPI 的 additionalProperties，可为 schema 或布尔值两种形态。
type SchemaOrBool interface {
	Schema() Schema
	Allows() bool
}
