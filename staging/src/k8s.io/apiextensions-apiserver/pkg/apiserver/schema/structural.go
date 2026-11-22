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

package schema

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +k8s:deepcopy-gen=true

// Structural 表示结构化 schema。
type Structural struct {
	Items                *Structural
	Properties           map[string]Structural
	AdditionalProperties *StructuralOrBool

	Generic
	Extensions
	ValidationExtensions

	ValueValidation *ValueValidation
}

// +k8s:deepcopy-gen=true

// StructuralOrBool 要么是结构化 schema，要么是布尔值。
type StructuralOrBool struct {
	Structural *Structural
	Bool       bool
}

// +k8s:deepcopy-gen=true

// ExternalDocumentation 允许引用外部资源以提供扩展文档。
type ExternalDocumentation struct {
	Description string
	URL         string
}

// +k8s:deepcopy-gen=true

// Generic 包含不允许出现在 value validation 中的通用 schema 字段。
type Generic struct {
	Description string
	// type 指定值的类型。
	// 可以是 object、array、number、integer、boolean、string。
	// 仅当 x-kubernetes-preserve-unknown-fields
	// 或 x-kubernetes-int-or-string 为 true 时才可省略。
	Type         string
	Title        string
	Default      JSON
	Nullable     bool
	ExternalDocs *ExternalDocumentation
	Example      JSON
}

// +k8s:deepcopy-gen=true

// Extensions 包含 Kubernetes OpenAPI v3 的 vendor 扩展。
type Extensions struct {
	// x-kubernetes-preserve-unknown-fields 阻止 API server
	// 在解码阶段裁剪校验 schema 中未声明的字段。该行为
	// 递归生效，但若 schema 中指定了嵌套的 properties 或
	// additionalProperties，则恢复为常规裁剪行为。
	// 为 false 表示裁剪行为继承自父级；
	// false 并不意味着启用裁剪。
	XPreserveUnknownFields bool

	// x-kubernetes-embedded-resource 表示该值是一个内嵌的
	// Kubernetes runtime.Object，带有 TypeMeta 与 ObjectMeta。
	// 其类型必须为 object，且允许进一步限制内嵌对象。
	// ObjectMeta 与 TypeMeta 会自动校验。
	// x-kubernetes-preserve-unknown-fields 必须为 true。
	XEmbeddedResource bool

	// x-kubernetes-int-or-string 表示该值要么是整数，要么是字符串。
	// 若为 true，则允许空 type，并且允许 anyOf 下的 type，
	// 只要符合以下模式之一：
	//
	// 1) anyOf:
	//    - type: integer
	//    - type: string
	// 2) allOf:
	//    - anyOf:
	//      - type: integer
	//      - type: string
	//    - ... 零个或多个
	XIntOrString bool

	// x-kubernetes-list-map-keys 通过指定用作 map 索引的键，
	// 为 x-kubernetes-list-type 为 `map` 的列表添加注解。
	//
	// 该标签只能用于设置了 "x-kubernetes-list-type" 为 "map"
	// 的列表。此外，为此属性指定的值必须是子结构中的
	// 标量类型字段（不支持嵌套）。
	XListMapKeys []string

	// x-kubernetes-list-type 为列表添加注解，以进一步描述其拓扑结构。
	// 该扩展只能用于列表，且可能有 3 种取值：
	//
	// 1) `atomic`：列表被当作单一实体，类似标量。
	//     原子列表在更新时会被整体替换。该扩展可用于
	//     任意类型的列表（struct、scalar 等）。
	// 2) `set`：
	//     集合是不允许出现重复值的列表。每个值必须是
	//     标量（或其他原子类型）。
	// 3) `map`：
	//     这类列表类似 map，其元素带有用于标识的非索引键。
	//     合并时保持顺序。map 标签只能用于元素类型为 object
	//     的列表。
	XListType *string

	// x-kubernetes-map-type 为对象添加注解，以进一步描述其拓扑结构。
	// 该扩展只能用于 type 为 object 的情况，且可能有 2 种取值：
	//
	// 1) `granular`：
	//     这些 map 是真正的 map（键值对），各字段相互独立
	//     （可由不同的操作者分别修改）。这是所有 map 的默认行为。
	// 2) `atomic`：该列表被当作单一实体，类似标量。
	//     原子 map 在更新时会被整体替换。
	// +optional
	XMapType *string
}

// +k8s:deepcopy-gen=true

// ValidationExtensions 包含用于校验而非结构的 Kubernetes OpenAPI v3 扩展。
type ValidationExtensions struct {
	// x-kubernetes-validations 描述用于表达式校验的校验规则列表。
	// 使用 v1 结构体，因为它会被序列化为扩展。
	XValidations apiextensionsv1.ValidationRules
}

// +k8s:deepcopy-gen=true

// ValueValidation 包含不参与 schema 结构的所有 schema 字段。
type ValueValidation struct {
	Format           string
	Maximum          *float64
	ExclusiveMaximum bool
	Minimum          *float64
	ExclusiveMinimum bool
	MaxLength        *int64
	MinLength        *int64
	Pattern          string
	MaxItems         *int64
	MinItems         *int64
	UniqueItems      bool
	MultipleOf       *float64
	Enum             []JSON
	MaxProperties    *int64
	MinProperties    *int64
	Required         []string
	AllOf            []NestedValueValidation
	OneOf            []NestedValueValidation
	AnyOf            []NestedValueValidation
	Not              *NestedValueValidation
}

// +k8s:deepcopy-gen=true

// NestedValueValidation 包含可在逻辑连接词下嵌套使用的 value validation、items 与 properties，
// 以及通用与 vendor 扩展 schema 字段的兜底结构。
type NestedValueValidation struct {
	ValueValidation
	ValidationExtensions

	Items                *NestedValueValidation
	Properties           map[string]NestedValueValidation
	AdditionalProperties *NestedValueValidation

	// 以下任一字段被设置都会使 schema
	// 变为非结构化，但当 x-kubernetes-int-or-string 为 true 时，
	// 以下两种模式除外：
	//
	// 1) anyOf:
	//    - type: integer
	//    - type: string
	// 2) allOf:
	//    - anyOf:
	//      - type: integer
	//      - type: string
	//    - ... 零个或多个
	ForbiddenGenerics   Generic
	ForbiddenExtensions Extensions
}

// JSON 包装任意 JSON 值，以便实现 deepcopy。
type JSON struct {
	Object interface{}
}

// DeepCopy 创建被包装 JSON 值的深拷贝。
func (j JSON) DeepCopy() JSON {
	return JSON{runtime.DeepCopyJSONValue(j.Object)}
}

// DeepCopyInto 创建被包装 JSON 值的深拷贝并写入 into。
func (j JSON) DeepCopyInto(into *JSON) {
	into.Object = runtime.DeepCopyJSONValue(j.Object)
}
