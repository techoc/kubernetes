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

package apiextensions

// FieldValueErrorReason 是机器可读的值，用于说明字段未通过校验的具体原因。
// +enum
type FieldValueErrorReason string

const (
	// FieldValueRequired 用于上报必填值缺失的情形
	// （例如空字符串、null 值或空数组）。
	FieldValueRequired FieldValueErrorReason = "FieldValueRequired"
	// FieldValueDuplicate 用于上报本应唯一但发生冲突的值
	// （例如唯一 ID 重复）。
	FieldValueDuplicate FieldValueErrorReason = "FieldValueDuplicate"
	// FieldValueInvalid 用于上报格式不正确的值
	// （例如正则不匹配、过长、越界）。
	FieldValueInvalid FieldValueErrorReason = "FieldValueInvalid"
	// FieldValueForbidden 用于上报格式正确、在某些条件下可接受，
	// 但被当前条件（如安全策略）禁止的值。
	FieldValueForbidden FieldValueErrorReason = "FieldValueForbidden"
)

// JSONSchemaProps 是遵循 JSON Schema Draft 4 规范（http://json-schema.org/）的 schema。
type JSONSchemaProps struct {
	ID                   string
	Schema               JSONSchemaURL
	Ref                  *string
	Description          string
	Type                 string
	Nullable             bool
	Format               string
	Title                string
	Default              *JSON
	Maximum              *float64
	ExclusiveMaximum     bool
	Minimum              *float64
	ExclusiveMinimum     bool
	MaxLength            *int64
	MinLength            *int64
	Pattern              string
	MaxItems             *int64
	MinItems             *int64
	UniqueItems          bool
	MultipleOf           *float64
	Enum                 []JSON
	MaxProperties        *int64
	MinProperties        *int64
	Required             []string
	Items                *JSONSchemaPropsOrArray
	AllOf                []JSONSchemaProps
	OneOf                []JSONSchemaProps
	AnyOf                []JSONSchemaProps
	Not                  *JSONSchemaProps
	Properties           map[string]JSONSchemaProps
	AdditionalProperties *JSONSchemaPropsOrBool
	PatternProperties    map[string]JSONSchemaProps
	Dependencies         JSONSchemaDependencies
	AdditionalItems      *JSONSchemaPropsOrBool
	Definitions          JSONSchemaDefinitions
	ExternalDocs         *ExternalDocumentation
	Example              *JSON

	// x-kubernetes-preserve-unknown-fields 阻止 API server
	// 在解码阶段剪除校验 schema 中未声明的字段。该设置会递归生效，
	// 但若 schema 中嵌套声明了 properties 或 additionalProperties，
	// 则回到正常的剪枝行为。只能为 true 或不设置；false 不被允许。
	XPreserveUnknownFields *bool

	// x-kubernetes-embedded-resource 表示该值是内嵌的 Kubernetes
	// runtime.Object，带有 TypeMeta 与 ObjectMeta。类型必须为 object，
	// 允许对内嵌对象进一步限制。ObjectMeta 与 TypeMeta 会被自动校验。
	// 此时 x-kubernetes-preserve-unknown-fields 必须为 true。
	XEmbeddedResource bool

	// x-kubernetes-int-or-string 表示该值可以是整数或字符串。
	// 为 true 时允许类型为空，并且当满足以下任一模式时，
	// 允许在 anyOf 下使用 type：
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

	// x-kubernetes-list-map-keys 为 x-kubernetes-list-type 为 `map` 的数组
	// 标注用作 map 索引的键。
	//
	// 该标记只能用于 x-kubernetes-list-type 扩展为 "map" 的列表；
	// 且所指定的值必须是子结构中的标量类型字段（不支持嵌套）。
	XListMapKeys []string

	// x-kubernetes-list-type 为数组标注更细的拓扑结构。
	// 该扩展只能用于列表，可取 3 个值：
	//
	// 1) `atomic`：列表被当作单个实体，类似标量。
	//      原子列表在更新时会被整体替换。可用于任意类型的列表（结构体、标量等）。
	// 2) `set`：
	//      set 是任意两个元素值都不得重复的列表。每个值必须是标量、
	//      带 x-kubernetes-map-type `atomic` 的对象，或带 x-kubernetes-list-type `atomic` 的数组。
	// 3) `map`：
	//      这类列表类似 map，元素带有用于标识自身的非索引键。
	//      合并时保持顺序。map 标记只能用于元素类型为 object 的列表。
	XListType *string

	// x-kubernetes-map-type 为对象标注更细的拓扑结构。
	// 该扩展只能用于 type 为 object 时，可取 2 个值：
	//
	// 1) `granular`：
	//      这类 map 是真正的 map（键值对），各字段相互独立
	//      （可由不同主体分别操作）。这是所有 map 的默认行为。
	// 2) `atomic`：列表被当作单个实体，类似标量。
	//      原子 map 在更新时会被整体替换。
	// +optional
	XMapType *string

	// x-kubernetes-validations -kubernetes-validations 描述一组用 CEL 表达式语言编写的校验规则。
	// +patchMergeKey=rule
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=rule
	XValidations ValidationRules
}

// ValidationRules 描述一组用 CEL 表达式语言编写的校验规则。
type ValidationRules []ValidationRule

// ValidationRule 描述一条用 CEL 表达式语言编写的校验规则。
type ValidationRule struct {
	// Rule 表示由 CEL 求值的表达式。
	// ref: https://github.com/google/cel-spec
	// Rule 的作用域为 schema 中 x-kubernetes-validations 扩展所在的位置，
	// CEL 表达式中的 `self` 变量绑定到该作用域的值。
	// 示例：
	// - 规则作用于带 status 子资源的资源根：{"rule": "self.status.actual <= self.spec.maxDesired"}
	//
	// 若规则作用于带 properties 的对象，可通过 `self.field` 访问字段，
	// 并通过 `has(self.field)` 检查字段是否存在。在 CEL 表达式中，
	// 值为 null 的字段视为不存在。
	// 若规则作用于带 additionalProperties（即 map）的对象，可通过 `self[mapKey]`
	// 访问 map 的值，通过 `mapKey in self` 检查包含关系，并可借助
	// `self.all(...)` 等 CEL 宏与函数访问所有条目。
	// 若规则作用于数组，可通过 `self[i]` 以及宏和函数访问数组元素。
	// 若规则作用于标量，`self` 绑定到该标量值。
	// 示例：
	// - 规则作用于对象 map：{"rule": "self.components['Widget'].priority < 10"}
	// - 规则作用于整数列表：{"rule": "self.values.all(value, value >= 0 && value < 100)"}
	// - 规则作用于字符串：{"rule": "self.startsWith('kube')"}
	//
	// `apiVersion`、`kind`、`metadata.name` 与 `metadata.generateName` 始终可从
	// 对象根以及任何带 x-kubernetes-embedded-resource 标注的对象访问，其他
	// metadata 属性不可访问。
	//
	// 通过 x-kubernetes-preserve-unknown-fields 保留在自定义资源中的未知数据
	// 在 CEL 表达式中不可访问，包括：
	// - 由带 x-kubernetes-preserve-unknown-fields 的对象 schema 保留的未知字段值。
	// - 属性 schema 为“未知类型”的对象属性。“未知类型”递归定义如下：
	//   - 无 type 且 x-kubernetes-preserve-unknown-fields 为 true 的 schema
	//   - items schema 为“未知类型”的数组
	//   - additionalProperties schema 为“未知类型”的对象
	//
	// 仅形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名可访问。
	// 在表达式中访问时，可访问的属性名按以下规则转义：
	// - '__' 转义为 '__underscores__'
	// - '.' 转义为 '__dot__'
	// - '-' 转义为 '__dash__'
	// - '/' 转义为 '__slash__'
	// - 与 CEL 保留关键字完全同名的属性名转义为 '__{keyword}__'。关键字包括：
	//	  "true", "false", "null", "in", "as", "break", "const", "continue", "else", "for", "function", "if",
	//	  "import", "let", "loop", "package", "namespace", "return".
	// 示例：
	//   - 访问名为 "namespace" 的属性：{"rule": "self.__namespace__ > 0"}
	//   - 访问名为 "x-prop" 的属性：{"rule": "self.x__dash__prop > 0"}
	//   - 访问名为 "redact__d" 的属性：{"rule": "self.redact__underscores__d > 0"}
	//
	// 对 x-kubernetes-list-type 为 'set' 或 'map' 的数组做相等比较时忽略元素顺序，
	// 即 [1, 2] == [2, 1]。
	// 对带 x-kubernetes-list-type 的数组做拼接时遵循列表类型语义：
	//   - 'set'：`X + Y` 执行并集，保留 `X` 中所有元素的数组位置，
	//     并将 `Y` 中不相交的元素按原有相对顺序追加。
	//   - 'map'：`X + Y` 执行合并，保留 `X` 中所有键的数组位置，
	//     但当 `X` 与 `Y` 的键集合相交时，值以 `Y` 为准覆盖。`Y` 中
	//     键不相交的元素按原有相对顺序追加。
	//
	// 若 `rule` 使用了 `oldSelf` 变量，则它隐式成为一条
	// `transition rule`（转换规则）。
	//
	// 默认情况下 `oldSelf` 与 `self` 类型相同。
	// 当 `optionalOldSelf` 为 true 时，`oldSelf` 是 CEL optional
	//  变量，其 value() 与 `self` 类型相同。
	// 详见 `optionalOldSelf` 字段的文档。
	//
	// 转换规则默认仅作用于 UPDATE 请求，且在找不到旧值时跳过。
	// 可通过将 `optionalOldSelf` 设为 true，使转换规则无条件求值。
	//
	Rule string
	// Message 表示校验失败时展示的消息。若 Rule 含换行则 Message 必填。
	// Message 本身不得包含换行。
	// 未设置时消息为 "failed rule: {Rule}"。
	// 例如 "must be a URL with the host matching spec.host"
	Message string
	// MessageExpression 声明一个 CEL 表达式，用于在本条规则失败时求值出校验失败消息。
	// 由于 messageExpression 用作失败消息，它必须求值为字符串。
	// 若规则同时设置了 message 与 messageExpression，则校验失败时优先使用 messageExpression。
	// 若 messageExpression 运行时报错，会记录该错误，并按 messageExpression 未设置的情形
	// 生成失败消息。若 messageExpression 求值为空串、仅含空格的串或含换行的串，
	// 同样按 messageExpression 未设置的情形生成失败消息，并记录该结果。
	// messageExpression 可访问与 rule 相同的全部变量，唯一区别是返回类型。
	// 示例：
	// "x must be less than max ("+string(self.max)+")"
	// +optional
	MessageExpression string
	// reason 提供机器可读的校验失败原因，在请求未通过本规则时返回给调用方。
	// 返回给调用方的 HTTP 状态码与第一条失败规则的 reason 一致。
	// 目前支持的原因有："FieldValueInvalid"、"FieldValueForbidden"、"FieldValueRequired"、"FieldValueDuplicate"。
	// 未设置时默认使用 "FieldValueInvalid"。
	// 后续新增的原因在客户端读取时必须被接受，未知原因应按 FieldValueInvalid 处理。
	// +optional
	Reason *FieldValueErrorReason
	// fieldPath 表示校验失败时返回的字段路径。
	// 它必须是相对于 schema 中该 x-kubernetes-validations 扩展所在位置的 JSON 路径
	// （即使用数组记法），并指向一个已存在的字段。
	// 例如当校验检查 map `testMap` 下的属性 `foo` 时，fieldPath 可设为 `.testMap.foo`
	// 若校验要求两个列表的属性唯一，fieldPath 可设为其中任一列表，例如 `.testList`
	// 它不支持列表的数字索引。
	// 目前支持子操作来引用已存在字段，详见 [JSONPath support in Kubernetes](https://kubernetes.io/docs/reference/kubectl/jsonpath/)。
	// 不支持数组的数字索引。
	// 对于含特殊字符的字段名，使用 `['specialName']` 引用。
	// 例如属性 `foo.34$` 出现在列表 `testList` 中时，fieldPath 可设为 `.testList['foo.34$']`
	// +optional
	FieldPath string

	// optionalOldSelf 用于让转换规则在对象首次创建、或旧对象缺少该值时
	// 也参与求值。
	//
	// 启用后 `oldSelf` 将成为 CEL optional：当没有旧值或对象初次创建时，
	// 其值为 `None`。
	//
	// 可用 `oldSelf.hasValue()` 检查 oldSelf 是否存在，检查后再用
	// `oldSelf.value()` 解包。Optional 类型的更多信息见 CEL 文档：
	// https://pkg.go.dev/github.com/google/cel-go/cel#OptionalTypes
	//
	// 除非 `rule` 中使用了 `oldSelf`，否则不得设置。
	//
	// +featureGate=CRDValidationRatcheting
	// +optional
	OptionalOldSelf *bool
}

// JSON 表示任意合法的 JSON 值。
// 支持的类型：bool、int64、float64、string、[]interface{}、map[string]interface{} 和 nil。
type JSON interface{}

// JSONSchemaURL 表示 schema 的 URL。
type JSONSchemaURL string

// JSONSchemaPropsOrArray 表示取值可以是单个 JSONSchemaProps，
// 也可以是 JSONSchemaProps 数组。主要用于序列化。
type JSONSchemaPropsOrArray struct {
	Schema      *JSONSchemaProps
	JSONSchemas []JSONSchemaProps
}

// JSONSchemaPropsOrBool 表示取值为 JSONSchemaProps 或布尔值。
// 布尔属性缺省为 true。
type JSONSchemaPropsOrBool struct {
	Allows bool
	Schema *JSONSchemaProps
}

// JSONSchemaDependencies 表示 dependencies 属性。
type JSONSchemaDependencies map[string]JSONSchemaPropsOrStringArray

// JSONSchemaPropsOrStringArray 表示取值为 JSONSchemaProps 或字符串数组。
type JSONSchemaPropsOrStringArray struct {
	Schema   *JSONSchemaProps
	Property []string
}

// JSONSchemaDefinitions 保存本 spec 中显式定义的模型。
type JSONSchemaDefinitions map[string]JSONSchemaProps

// ExternalDocumentation 允许引用外部资源以提供扩展文档。
type ExternalDocumentation struct {
	Description string
	URL         string
}
