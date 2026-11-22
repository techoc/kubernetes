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

package v1

// FieldValueErrorReason 是机器可读的值，用于进一步说明字段为何未通过校验。
// +enum
type FieldValueErrorReason string

const (
	// FieldValueRequired 用于报告未提供必填值
	//（例如空字符串、null 值或空数组）。
	FieldValueRequired FieldValueErrorReason = "FieldValueRequired"
	// FieldValueDuplicate 用于报告必须唯一的值发生冲突
	//（例如唯一 ID）。
	FieldValueDuplicate FieldValueErrorReason = "FieldValueDuplicate"
	// FieldValueInvalid 用于报告格式错误的值
	//（例如正则不匹配、过长、越界）。
	FieldValueInvalid FieldValueErrorReason = "FieldValueInvalid"
	// FieldValueForbidden 用于报告格式合法、在某些条件下可接受，
	// 但被当前条件（如安全策略）禁止的值。
	FieldValueForbidden FieldValueErrorReason = "FieldValueForbidden"
)

// JSONSchemaProps 是遵循 Specification Draft 4 的 JSON Schema（http://json-schema.org/）。
type JSONSchemaProps struct {
	ID          string        `json:"id,omitempty" protobuf:"bytes,1,opt,name=id"`
	Schema      JSONSchemaURL `json:"$schema,omitempty" protobuf:"bytes,2,opt,name=schema"`
	Ref         *string       `json:"$ref,omitempty" protobuf:"bytes,3,opt,name=ref"`
	Description string        `json:"description,omitempty" protobuf:"bytes,4,opt,name=description"`
	Type        string        `json:"type,omitempty" protobuf:"bytes,5,opt,name=type"`

	// format 是 OpenAPI v3 的格式字符串。未知格式会被忽略。以下格式会被校验：
	//
	// - bsonobjectid：bson 对象 ID，即 24 个字符的十六进制字符串
	// - uri：由 Golang net/url.ParseRequestURI 解析的 URI
	// - email：由 Golang net/mail.ParseAddress 解析的邮箱地址
	// - hostname：符合 RFC 1034 第 3.1 节 [RFC1034] 的互联网主机名合法表示
	// - ipv4：由 Golang net.ParseIP 解析的 IPv4 地址
	// - ipv6：由 Golang net.ParseIP 解析的 IPv6 地址
	// - cidr：由 Golang net.ParseCIDR 解析的 CIDR
	// - mac：由 Golang net.ParseMAC 解析的 MAC 地址
	// - uuid：允许大写的 UUID，由正则 (?i)^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$ 定义
	// - uuid3：允许大写的 UUID3，由正则 (?i)^[0-9a-f]{8}-?[0-9a-f]{4}-?3[0-9a-f]{3}-?[0-9a-f]{4}-?[0-9a-f]{12}$ 定义
	// - uuid4：允许大写的 UUID4，由正则 (?i)^[0-9a-f]{8}-?[0-9a-f]{4}-?4[0-9a-f]{3}-?[89ab][0-9a-f]{3}-?[0-9a-f]{12}$ 定义
	// - uuid5：允许大写的 UUID5，由正则 (?i)^[0-9a-f]{8}-?[0-9a-f]{4}-?5[0-9a-f]{3}-?[89ab][0-9a-f]{3}-?[0-9a-f]{12}$ 定义
	// - isbn：ISBN10 或 ISBN13 数字字符串，如 "0321751043" 或 "978-0321751041"
	// - isbn10：ISBN10 数字字符串，如 "0321751043"
	// - isbn13：ISBN13 数字字符串，如 "978-0321751041"
	// - creditcard：由正则 ^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|6(?:011|5[0-9][0-9])[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\\d{3})\\d{11})$ 定义、可混入任意非数字字符的信用卡号
	// - ssn：符合正则 ^\\d{3}[- ]?\\d{2}[- ]?\\d{4}$ 的美国社会安全号
	// - hexcolor：符合正则 ^#?([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$ 的十六进制颜色码，如 "#FFFFFF"
	// - rgbcolor：RGB 颜色码，如 "rgb(255,255,2559"
	// - byte：base64 编码的二进制数据
	// - password：任意字符串
	// - date：符合 RFC3339 中 full-date 定义的日期字符串，如 "2006-01-02"
	// - duration：由 Golang time.ParseDuration 解析、或兼容 Scala duration 格式的时长字符串，如 "22 ns"
	// - datetime：符合 RFC3339 中 date-time 定义的日期时间字符串，如 "2014-12-15T19:30:20.000Z"。
	Format string `json:"format,omitempty" protobuf:"bytes,6,opt,name=format"`

	Title string `json:"title,omitempty" protobuf:"bytes,7,opt,name=title"`
	// default 是未定义对象字段的默认值。
	// 默认值填充是 CustomResourceDefaulting feature gate 下的 beta 特性。
	// 默认值填充要求 spec.preserveUnknownFields 为 false。
	Default          *JSON    `json:"default,omitempty" protobuf:"bytes,8,opt,name=default"`
	Maximum          *float64 `json:"maximum,omitempty" protobuf:"bytes,9,opt,name=maximum"`
	ExclusiveMaximum bool     `json:"exclusiveMaximum,omitempty" protobuf:"bytes,10,opt,name=exclusiveMaximum"`
	Minimum          *float64 `json:"minimum,omitempty" protobuf:"bytes,11,opt,name=minimum"`
	ExclusiveMinimum bool     `json:"exclusiveMinimum,omitempty" protobuf:"bytes,12,opt,name=exclusiveMinimum"`
	MaxLength        *int64   `json:"maxLength,omitempty" protobuf:"bytes,13,opt,name=maxLength"`
	MinLength        *int64   `json:"minLength,omitempty" protobuf:"bytes,14,opt,name=minLength"`
	Pattern          string   `json:"pattern,omitempty" protobuf:"bytes,15,opt,name=pattern"`
	MaxItems         *int64   `json:"maxItems,omitempty" protobuf:"bytes,16,opt,name=maxItems"`
	MinItems         *int64   `json:"minItems,omitempty" protobuf:"bytes,17,opt,name=minItems"`
	UniqueItems      bool     `json:"uniqueItems,omitempty" protobuf:"bytes,18,opt,name=uniqueItems"`
	MultipleOf       *float64 `json:"multipleOf,omitempty" protobuf:"bytes,19,opt,name=multipleOf"`
	// +listType=atomic
	Enum          []JSON `json:"enum,omitempty" protobuf:"bytes,20,rep,name=enum"`
	MaxProperties *int64 `json:"maxProperties,omitempty" protobuf:"bytes,21,opt,name=maxProperties"`
	MinProperties *int64 `json:"minProperties,omitempty" protobuf:"bytes,22,opt,name=minProperties"`
	// +listType=atomic
	Required []string                `json:"required,omitempty" protobuf:"bytes,23,rep,name=required"`
	Items    *JSONSchemaPropsOrArray `json:"items,omitempty" protobuf:"bytes,24,opt,name=items"`
	// +listType=atomic
	AllOf []JSONSchemaProps `json:"allOf,omitempty" protobuf:"bytes,25,rep,name=allOf"`
	// +listType=atomic
	OneOf []JSONSchemaProps `json:"oneOf,omitempty" protobuf:"bytes,26,rep,name=oneOf"`
	// +listType=atomic
	AnyOf                []JSONSchemaProps          `json:"anyOf,omitempty" protobuf:"bytes,27,rep,name=anyOf"`
	Not                  *JSONSchemaProps           `json:"not,omitempty" protobuf:"bytes,28,opt,name=not"`
	Properties           map[string]JSONSchemaProps `json:"properties,omitempty" protobuf:"bytes,29,rep,name=properties"`
	AdditionalProperties *JSONSchemaPropsOrBool     `json:"additionalProperties,omitempty" protobuf:"bytes,30,opt,name=additionalProperties"`
	PatternProperties    map[string]JSONSchemaProps `json:"patternProperties,omitempty" protobuf:"bytes,31,rep,name=patternProperties"`
	Dependencies         JSONSchemaDependencies     `json:"dependencies,omitempty" protobuf:"bytes,32,opt,name=dependencies"`
	AdditionalItems      *JSONSchemaPropsOrBool     `json:"additionalItems,omitempty" protobuf:"bytes,33,opt,name=additionalItems"`
	Definitions          JSONSchemaDefinitions      `json:"definitions,omitempty" protobuf:"bytes,34,opt,name=definitions"`
	ExternalDocs         *ExternalDocumentation     `json:"externalDocs,omitempty" protobuf:"bytes,35,opt,name=externalDocs"`
	Example              *JSON                      `json:"example,omitempty" protobuf:"bytes,36,opt,name=example"`
	Nullable             bool                       `json:"nullable,omitempty" protobuf:"bytes,37,opt,name=nullable"`

	// x-kubernetes-preserve-unknown-fields 阻止 API server
	// 在解码阶段裁剪校验 schema 中未声明的字段。该效果递归作用于字段，
	// 但如果 schema 中指定了嵌套的 properties 或 additionalProperties，
	// 则恢复为普通裁剪行为。
	// 该值可为 true 或未定义；不允许为 false。
	XPreserveUnknownFields *bool `json:"x-kubernetes-preserve-unknown-fields,omitempty" protobuf:"bytes,38,opt,name=xKubernetesPreserveUnknownFields"`

	// x-kubernetes-embedded-resource 表示该值是一个内嵌的 Kubernetes runtime.Object，
	// 带有 TypeMeta 和 ObjectMeta。类型必须为 object。可以进一步限制内嵌对象。
	// kind、apiVersion 和 metadata 会被自动校验。
	// x-kubernetes-preserve-unknown-fields 可以设为 true，但若对象已完整指定
	//（直到 kind、apiVersion、metadata）则不必设置。
	XEmbeddedResource bool `json:"x-kubernetes-embedded-resource,omitempty" protobuf:"bytes,39,opt,name=xKubernetesEmbeddedResource"`

	// x-kubernetes-int-or-string 表示该值是整数或字符串。若为 true，
	// 允许空 type，并且 anyOf 下的 type 在符合以下模式之一时被允许：
	//
	// 1) anyOf:
	//    - type: integer
	//    - type: string
	// 2) allOf:
	//    - anyOf:
	//      - type: integer
	//      - type: string
	//    - ... 零个或多个
	XIntOrString bool `json:"x-kubernetes-int-or-string,omitempty" protobuf:"bytes,40,opt,name=xKubernetesIntOrString"`

	// x-kubernetes-list-map-keys 通过指定用作 map 索引的键，
	// 为带有 x-kubernetes-list-type `map` 的数组添加注解。
	//
	// 该标签只能用于设置了 "x-kubernetes-list-type" 扩展为 "map" 的列表。
	// 同时，为此属性指定的值必须是子结构中标量类型的字段（不支持嵌套）。
	//
	// 指定的属性必须为必填或带有默认值，以确保所有列表项都包含这些属性。
	//
	// +optional
	// +listType=atomic
	XListMapKeys []string `json:"x-kubernetes-list-map-keys,omitempty" protobuf:"bytes,41,rep,name=xKubernetesListMapKeys"`

	// x-kubernetes-list-type 为数组添加注解，进一步描述其拓扑结构。
	// 该扩展只能用于列表，可能取 3 个值：
	//
	// 1) `atomic`：列表被视为单个实体，类似标量。
	//      原子列表在更新时会被整体替换。该扩展可用于任何类型的列表（结构、标量等）。
	// 2) `set`：
	//      set 列表不得包含多个相同值的项。每个值必须是标量、
	//      带有 x-kubernetes-map-type `atomic` 的对象，或带有
	//      x-kubernetes-list-type `atomic` 的数组。
	// 3) `map`：
	//      此类列表类似 map，其元素带有非索引键用于标识。合并时保持顺序。
	//      map 标签只能用于元素类型为 object 的列表。
	// 数组默认值为 atomic。
	// +optional
	XListType *string `json:"x-kubernetes-list-type,omitempty" protobuf:"bytes,42,opt,name=xKubernetesListType"`

	// x-kubernetes-map-type 为对象添加注解，进一步描述其拓扑结构。
	// 该扩展只能在 type 为 object 时使用，可能取 2 个值：
	//
	// 1) `granular`：
	//      这些 map 是真正的 map（键值对），各字段彼此独立
	//      （可由不同的操作者分别修改）。这是所有 map 的默认行为。
	// 2) `atomic`：列表被视为单个实体，类似标量。
	//      原子 map 在更新时会被整体替换。
	// +optional
	XMapType *string `json:"x-kubernetes-map-type,omitempty" protobuf:"bytes,43,opt,name=xKubernetesMapType"`

	// x-kubernetes-validations 描述用 CEL 表达式语言编写的校验规则列表。
	// +patchMergeKey=rule
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=rule
	XValidations ValidationRules `json:"x-kubernetes-validations,omitempty" patchStrategy:"merge" patchMergeKey:"rule" protobuf:"bytes,44,rep,name=xKubernetesValidations"`
}

// ValidationRules 描述用 CEL 表达式语言编写的校验规则列表。
type ValidationRules []ValidationRule

// ValidationRule 描述用 CEL 表达式语言编写的校验规则。
type ValidationRule struct {
	// Rule 表示将由 CEL 求值的表达式。
	// 参考：https://github.com/google/cel-spec
	// Rule 的作用域是 schema 中 x-kubernetes-validations 扩展所在的位置。
	// CEL 表达式中的 `self` 变量绑定到该作用域的值。
	// 示例：
	// - 作用于带 status 子资源的资源根部的规则：{"rule": "self.status.actual <= self.spec.maxDesired"}
	//
	// 若 Rule 作用于带属性的对象，可通过 `self.field` 选择字段，
	// 并通过 `has(self.field)` 检查字段是否存在。在 CEL 表达式中，
	// 值为 null 的字段视为不存在。
	// 若 Rule 作用于带 additionalProperties 的对象（即 map），
	// 可通过 `self[mapKey]` 访问 map 的值，通过 `mapKey in self` 检查键是否存在，
	// 并通过 CEL 宏和函数（如 `self.all(...)`）访问 map 的所有条目。
	// 若 Rule 作用于数组，可通过 `self[i]` 以及宏和函数访问数组元素。
	// 若 Rule 作用于标量，`self` 绑定到该标量值。
	// 示例：
	// - 作用于对象 map 的规则：{"rule": "self.components['Widget'].priority < 10"}
	// - 作用于整数列表的规则：{"rule": "self.values.all(value, value >= 0 && value < 100)"}
	// - 作用于字符串值的规则：{"rule": "self.startsWith('kube')"}
	//
	// `apiVersion`、`kind`、`metadata.name` 和 `metadata.generateName` 始终可从
	// 对象根部以及任何带 x-kubernetes-embedded-resource 注解的对象访问。
	// 其他 metadata 属性不可访问。
	//
	// 通过 x-kubernetes-preserve-unknown-fields 保留在自定义资源中的未知数据
	// 在 CEL 表达式中不可访问，包括：
	// - 由带 x-kubernetes-preserve-unknown-fields 的对象 schema 保留的未知字段值。
	// - 属性 schema 为 "unknown type" 的对象属性。"unknown type" 递归定义为：
	//   - 没有 type 且 x-kubernetes-preserve-unknown-fields 为 true 的 schema
	//   - items schema 为 "unknown type" 的数组
	//   - additionalProperties schema 为 "unknown type" 的对象
	//
	// 仅形如 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 的属性名可访问。
	// 在表达式中访问可访问的属性名时，按以下规则转义：
	// - '__' 转义为 '__underscores__'
	// - '.' 转义为 '__dot__'
	// - '-' 转义为 '__dash__'
	// - '/' 转义为 '__slash__'
	// - 与 CEL RESERVED 关键字完全匹配的属性名转义为 '__{keyword}__'。关键字包括：
	//	  "true", "false", "null", "in", "as", "break", "const", "continue", "else", "for", "function", "if",
	//	  "import", "let", "loop", "package", "namespace", "return".
	// 示例：
	//   - 访问名为 "namespace" 的属性：{"rule": "self.__namespace__ > 0"}
	//   - 访问名为 "x-prop" 的属性：{"rule": "self.x__dash__prop > 0"}
	//   - 访问名为 "redact__d" 的属性：{"rule": "self.redact__underscores__d > 0"}
	//
	// 对 x-kubernetes-list-type 为 'set' 或 'map' 的数组做相等比较时忽略元素顺序，
	// 即 [1, 2] == [2, 1]。
	// 对 x-kubernetes-list-type 数组做拼接时遵循该 list type 的语义：
	//   - 'set'：`X + Y` 执行并集，保留 `X` 中所有元素的数组位置，
	//     并追加 `Y` 中不交叠的元素，保持其相对顺序。
	//   - 'map'：`X + Y` 执行合并，保留 `X` 中所有键的数组位置，
	//     但当 `X` 与 `Y` 的键集合相交时，值会被 `Y` 中的值覆盖。
	//     `Y` 中键不交叠的元素被追加，保持其相对顺序。
	//
	// 若 `rule` 使用了 `oldSelf` 变量，则它隐式是一条
	// `transition rule`（转换规则）。
	//
	// 默认情况下，`oldSelf` 变量与 `self` 类型相同。
	// 当 `optionalOldSelf` 为 true 时，`oldSelf` 是 CEL optional
	//  变量，其 value() 与 `self` 类型相同。
	// 详见 `optionalOldSelf` 字段的文档。
	//
	// 转换规则默认只在 UPDATE 请求上应用；若找不到旧值则跳过。
	// 将 `optionalOldSelf` 设为 true 可让转换规则无条件求值。
	//
	Rule string `json:"rule" protobuf:"bytes,1,opt,name=rule"`
	// Message 表示校验失败时显示的消息。若 Rule 含换行，则 Message 必填。
	// 消息本身不得包含换行。
	// 若未设置，消息为 "failed rule: {Rule}"。
	// 例如 "must be a URL with the host matching spec.host"
	Message string `json:"message,omitempty" protobuf:"bytes,2,opt,name=message"`
	// MessageExpression 声明一个 CEL 表达式，其求值结果作为该规则失败时返回的校验失败消息。
	// 由于 messageExpression 用作失败消息，它必须求值为字符串。
	// 若规则同时设置 message 和 messageExpression，则校验失败时使用 messageExpression。
	// 若 messageExpression 产生运行时错误，会记录该运行时错误，并按未设置
	// messageExpression 的方式生成校验失败消息。若 messageExpression 求值为空字符串、
	// 仅含空格的字符串或包含换行的字符串，同样按未设置 messageExpression 的方式生成
	// 校验失败消息，并记录其产生了空字符串/仅空格字符串/含换行字符串。
	// messageExpression 可访问与 rule 相同的全部变量，区别仅在返回类型。
	// 示例：
	// "x must be less than max ("+string(self.max)+")"
	// +optional
	MessageExpression string `json:"messageExpression,omitempty" protobuf:"bytes,3,opt,name=messageExpression"`
	// reason 提供机器可读的校验失败原因，在该请求未通过此校验规则时返回给调用方。
	// 返回给调用方的 HTTP 状态码与第一条失败校验规则的 reason 一致。
	// 当前支持的原因有："FieldValueInvalid"、"FieldValueForbidden"、"FieldValueRequired"、"FieldValueDuplicate"。
	// 若未设置，默认为 "FieldValueInvalid"。
	// 未来新增的原因必须能被客户端读取，未知原因应按 FieldValueInvalid 处理。
	// +optional
	Reason *FieldValueErrorReason `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// fieldPath 表示校验失败时返回的字段路径。
	// 它必须是相对于本 x-kubernetes-validations 扩展在 schema 中位置的相对 JSON 路径
	//（即使用数组记法），并指向一个已存在的字段。
	// 例如，当校验检查 map `testMap` 下的特定属性 `foo` 时，fieldPath 可设为 `.testMap.foo`。
	// 若校验要求两个列表具有唯一属性，fieldPath 可设为其中任一列表，例如 `.testList`。
	// 它不支持列表数字索引。
	// 目前支持子操作来引用已存在的字段。详情参见 [Kubernetes 中的 JSONPath 支持](https://kubernetes.io/docs/reference/kubectl/jsonpath/)。
	// 不支持数组的数字索引。
	// 对包含特殊字符的字段名，使用 `['specialName']` 引用该字段名。
	// 例如，属性 `foo.34$` 出现在列表 `testList` 中时，fieldPath 可设为 `.testList['foo.34$']`。
	// +optional
	FieldPath string `json:"fieldPath,omitempty" protobuf:"bytes,5,opt,name=fieldPath"`

	// optionalOldSelf 用于让转换规则在对象首次创建、或旧对象缺少该值时
	// 也参与求值。
	//
	// 启用后，`oldSelf` 是一个 CEL optional；没有旧值或对象初次创建时，
	// 其值为 `None`。
	//
	// 可用 `oldSelf.hasValue()` 检查 oldSelf 是否存在，检查后用
	// `oldSelf.value()` 解包。Optional 类型的更多信息见 CEL 文档：
	// https://pkg.go.dev/github.com/google/cel-go/cel#OptionalTypes
	//
	// 除非 `rule` 中使用了 `oldSelf`，否则不得设置该字段。
	//
	// +featureGate=CRDValidationRatcheting
	// +optional
	OptionalOldSelf *bool `json:"optionalOldSelf,omitempty" protobuf:"bytes,6,opt,name=optionalOldSelf"`
}

// JSON 表示任意合法的 JSON 值。
// 支持的类型：bool、int64、float64、string、[]interface{}、map[string]interface{} 和 nil。
type JSON struct {
	Raw []byte `json:"-" protobuf:"bytes,1,opt,name=raw"`
}

// OpenAPISchemaType 供 kube-openapi 生成器在构建该类型的 OpenAPI spec 时使用。
//
// 参见：https://github.com/kubernetes/kube-openapi/tree/master/pkg/generators
func (_ JSON) OpenAPISchemaType() []string {
	// TODO: 当支持 anyOf 时返回实际类型
	return nil
}

// OpenAPISchemaFormat 供 kube-openapi 生成器在构建该类型的 OpenAPI spec 时使用。
func (_ JSON) OpenAPISchemaFormat() string { return "" }

// JSONSchemaURL 表示 schema URL。
type JSONSchemaURL string

// JSONSchemaPropsOrArray 表示一个值，它可以是 JSONSchemaProps
// 或 JSONSchemaProps 数组。主要出于序列化目的。
type JSONSchemaPropsOrArray struct {
	Schema *JSONSchemaProps `protobuf:"bytes,1,opt,name=schema"`
	// +listType=atomic
	JSONSchemas []JSONSchemaProps `protobuf:"bytes,2,rep,name=jSONSchemas"`
}

// OpenAPISchemaType 供 kube-openapi 生成器在构建该类型的 OpenAPI spec 时使用。
//
// 参见：https://github.com/kubernetes/kube-openapi/tree/master/pkg/generators
func (_ JSONSchemaPropsOrArray) OpenAPISchemaType() []string {
	// TODO: 当支持 anyOf 时返回实际类型
	return nil
}

// OpenAPISchemaFormat 供 kube-openapi 生成器在构建该类型的 OpenAPI spec 时使用。
func (_ JSONSchemaPropsOrArray) OpenAPISchemaFormat() string { return "" }

// JSONSchemaPropsOrBool 表示 JSONSchemaProps 或布尔值。
// 布尔属性默认为 true。
type JSONSchemaPropsOrBool struct {
	Allows bool             `protobuf:"varint,1,opt,name=allows"`
	Schema *JSONSchemaProps `protobuf:"bytes,2,opt,name=schema"`
}

// OpenAPISchemaType 供 kube-openapi 生成器在构建该类型的 OpenAPI spec 时使用。
//
// 参见：https://github.com/kubernetes/kube-openapi/tree/master/pkg/generators
func (_ JSONSchemaPropsOrBool) OpenAPISchemaType() []string {
	// TODO: 当支持 anyOf 时返回实际类型
	return nil
}

// OpenAPISchemaFormat 供 kube-openapi 生成器在构建该类型的 OpenAPI spec 时使用。
func (_ JSONSchemaPropsOrBool) OpenAPISchemaFormat() string { return "" }

// JSONSchemaDependencies 表示 dependencies 属性。
type JSONSchemaDependencies map[string]JSONSchemaPropsOrStringArray

// JSONSchemaPropsOrStringArray 表示 JSONSchemaProps 或字符串数组。
type JSONSchemaPropsOrStringArray struct {
	Schema *JSONSchemaProps `protobuf:"bytes,1,opt,name=schema"`
	// +listType=atomic
	Property []string `protobuf:"bytes,2,rep,name=property"`
}

// OpenAPISchemaType 供 kube-openapi 生成器在构建该类型的 OpenAPI spec 时使用。
//
// 参见：https://github.com/kubernetes/kube-openapi/tree/master/pkg/generators
func (_ JSONSchemaPropsOrStringArray) OpenAPISchemaType() []string {
	// TODO: 当支持 anyOf 时返回实际类型
	return nil
}

// OpenAPISchemaFormat 供 kube-openapi 生成器在构建该类型的 OpenAPI spec 时使用。
func (_ JSONSchemaPropsOrStringArray) OpenAPISchemaFormat() string { return "" }

// JSONSchemaDefinitions 包含本 spec 中显式定义的模型。
type JSONSchemaDefinitions map[string]JSONSchemaProps

// ExternalDocumentation 允许引用外部资源以提供扩展文档。
type ExternalDocumentation struct {
	Description string `json:"description,omitempty" protobuf:"bytes,1,opt,name=description"`
	URL         string `json:"url,omitempty" protobuf:"bytes,2,opt,name=url"`
}
