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

// Schema 是 CEL 使用的 OpenAPI schema 适配类型。它不覆盖全部 OpenAPI 字段，
// 只把 CEL 需要的字段暴露为 getter。
// 这是一个抽象适配层，屏蔽内部 OpenAPI 与结构化 schema 的差异，
// 使 CEL 类型推导只依赖只读 getter。所有方法都应视作纯读：实现方可能被多个
// 编译/校验 goroutine 共享。
type Schema interface {
	// 返回 OpenAPI 类型；不支持多类型，未指定时返回空字符串。
	Type() string

	// 返回 OpenAPI format，可为空。
	Format() string

	// 返回 OpenAPI items；字段不存在或不含 schema 时返回 nil。
	Items() Schema

	// 返回 OpenAPI properties，字段不存在时返回 nil；
	// map 的值应同样经过适配。
	Properties() map[string]Schema

	// 返回 OpenAPI additionalProperties，字段不存在时返回 nil。
	AdditionalProperties() SchemaOrBool

	// 返回 OpenAPI default，字段不存在时返回 nil。
	Default() any

	Validations
	KubeExtensions

	// WithTypeAndObjectMeta 返回设置了 type 与 object meta 的 schema。
	// type 包含 "kind"、"apiVersion" 字段；
	// "metadata" 字段要求 "name" 与 "generateName" 已设置。
	// 原始 schema 不得被修改，必要时先复制副本。
	// 这是“不可变”契约的关键：调用方可能在共享 schema 上调用本方法，
	// 实现必须返回副本。
	WithTypeAndObjectMeta() Schema
}

// Validations 包含 CEL 库使用的 OpenAPI 校验信息。
// 这些 getter 直接映射 OpenAPI 校验字段，CEL 用它推导默认值与约束，
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

// KubeExtensions 包含 Kubernetes 对 OpenAPI schema 的专有扩展。
// x-kubernetes-* 扩展决定了 CEL 对列表/映射的语义（merge key、原子性等），
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

// ValidationRule 表示单条 x-kubernetes-validations 规则。
// 只暴露规则原文与其元数据，不求值；求值由 CEL 运行时负责，
// 以便编译结果可缓存。
type ValidationRule interface {
	Rule() string
	Message() string
	MessageExpression() string
	FieldPath() string
}

// SchemaOrBool 要么包含一个 schema，要么是一个布尔值，指示对象
// 能否包含任意字段。对应 OpenAPI 的 additionalProperties，可为 schema 或布尔值两种形态。
type SchemaOrBool interface {
	Schema() Schema
	Allows() bool
}
