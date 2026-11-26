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

// Package openapi 把 kube-openapi 的 spec.Schema 适配为 CEL 类型系统使用的 common.Schema。
// 这一层是纯适配器：所有语义（类型推导、默认值、x-kubernetes-* 扩展）都委托给 pkg/cel/common，
// 因此这里不缓存、不复制状态，只做接口转换。
package openapi

import (
	"github.com/google/cel-go/common/types/ref"

	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

var _ common.Schema = (*Schema)(nil)
var _ common.SchemaOrBool = (*SchemaOrBool)(nil)

// Schema 包装单个 OpenAPI schema，使其满足 common.Schema 接口。
type Schema struct {
	Schema *spec.Schema
}

// SchemaOrBool 对应 additionalProperties 这类“schema 或布尔值”的联合结构。
type SchemaOrBool struct {
	SchemaOrBool *spec.SchemaOrBool
}

// Schema 返回内部 schema；为 nil 表示该位置没有 schema 约束（如 additionalProperties: true）。
func (sb *SchemaOrBool) Schema() common.Schema {
	if sb.SchemaOrBool.Schema == nil {
		return nil
	}
	return &Schema{Schema: sb.SchemaOrBool.Schema}
}

// Allows 对应 additionalProperties 的布尔形式，true 表示允许任意额外字段。
func (sb *SchemaOrBool) Allows() bool {
	return sb.SchemaOrBool.Allows
}

// Type 返回主类型；OpenAPI 允许类型数组，但 Kubernetes schema 只会有一个，取首元素即可。
func (s *Schema) Type() string {
	if len(s.Schema.Type) == 0 {
		return ""
	}
	return s.Schema.Type[0]
}

// Format 返回 format 字符串（如 int32、date-time），CEL 用它选择对应的扩展类型。
func (s *Schema) Format() string {
	return s.Schema.Format
}

// Pattern 返回正则约束，用于字符串类型的运行时校验。
func (s *Schema) Pattern() string {
	return s.Schema.Pattern
}

// Items 返回数组元素 schema；OpenAPI 的 Items 可能是 schema 也可能是 schema 数组，
// 后者在 Kubernetes 中不使用，因此只处理单 schema 形式。
func (s *Schema) Items() common.Schema {
	if s.Schema.Items == nil || s.Schema.Items.Schema == nil {
		return nil
	}
	return &Schema{Schema: s.Schema.Items.Schema}
}

// Properties 返回字段名到 schema 的映射。
// 必须返回新的 map 和每个值的浅拷贝：map 的值不可寻址，
// 直接取 &prop 会指向同一个循环变量，导致所有条目共享地址。
func (s *Schema) Properties() map[string]common.Schema {
	if s.Schema.Properties == nil {
		return nil
	}
	res := make(map[string]common.Schema, len(s.Schema.Properties))
	for n, prop := range s.Schema.Properties {
		// map value 不可寻址，创建浅拷贝
		// 这只是浅层的非递归拷贝
		s := prop
		res[n] = &Schema{Schema: &s}
	}
	return res
}

// AdditionalProperties 返回额外字段约束，nil 表示未声明。
func (s *Schema) AdditionalProperties() common.SchemaOrBool {
	if s.Schema.AdditionalProperties == nil {
		return nil
	}
	return &SchemaOrBool{SchemaOrBool: s.Schema.AdditionalProperties}
}

// Default 返回 schema 声明的默认值，CEL 求值时会用它填充缺失字段。
func (s *Schema) Default() any {
	return s.Schema.Default
}

// Minimum 返回数值下界。
func (s *Schema) Minimum() *float64 {
	return s.Schema.Minimum
}

// IsExclusiveMinimum 指示 Minimum 是否为开区间边界。
func (s *Schema) IsExclusiveMinimum() bool {
	return s.Schema.ExclusiveMinimum
}

// Maximum 返回数值上界。
func (s *Schema) Maximum() *float64 {
	return s.Schema.Maximum
}

// IsExclusiveMaximum 指示 Maximum 是否为开区间边界。
func (s *Schema) IsExclusiveMaximum() bool {
	return s.Schema.ExclusiveMaximum
}

// MultipleOf 返回数值必须整除的因子。
func (s *Schema) MultipleOf() *float64 {
	return s.Schema.MultipleOf
}

// UniqueItems 指示数组元素是否必须唯一。
func (s *Schema) UniqueItems() bool {
	return s.Schema.UniqueItems
}

// MinItems 返回数组最少元素数。
func (s *Schema) MinItems() *int64 {
	return s.Schema.MinItems
}

// MaxItems 返回数组最多元素数。
func (s *Schema) MaxItems() *int64 {
	return s.Schema.MaxItems
}

// MinLength 返回字符串最小长度。
func (s *Schema) MinLength() *int64 {
	return s.Schema.MinLength
}

// MaxLength 返回字符串最大长度。
func (s *Schema) MaxLength() *int64 {
	return s.Schema.MaxLength
}

// MinProperties 返回 map 最少键数。
func (s *Schema) MinProperties() *int64 {
	return s.Schema.MinProperties
}

// MaxProperties 返回 map 最多键数。
func (s *Schema) MaxProperties() *int64 {
	return s.Schema.MaxProperties
}

// Required 返回必填字段名列表。
func (s *Schema) Required() []string {
	return s.Schema.Required
}

// Enum 返回允许的取值集合。
func (s *Schema) Enum() []any {
	return s.Schema.Enum
}

// Nullable 指示字段是否允许为 null。
func (s *Schema) Nullable() bool {
	return s.Schema.Nullable
}

// AllOf 返回全部子 schema；注意这里取循环变量的地址会共享存储，
// 因此显式复制到新变量。Go 1.22 起循环变量每轮独立，但保持显式复制更清晰。
func (s *Schema) AllOf() []common.Schema {
	var res []common.Schema
	for _, nestedSchema := range s.Schema.AllOf {
		res = append(res, &Schema{&nestedSchema})
	}
	return res
}

// AnyOf 返回任一满足即可的子 schema。
func (s *Schema) AnyOf() []common.Schema {
	var res []common.Schema
	for _, nestedSchema := range s.Schema.AnyOf {
		res = append(res, &Schema{&nestedSchema})
	}
	return res
}

// OneOf 返回恰好满足其一的子 schema。
func (s *Schema) OneOf() []common.Schema {
	var res []common.Schema
	for _, nestedSchema := range s.Schema.OneOf {
		res = append(res, &Schema{&nestedSchema})
	}
	return res
}

// Not 返回取反约束的子 schema。
func (s *Schema) Not() common.Schema {
	if s.Schema.Not == nil {
		return nil
	}
	return &Schema{s.Schema.Not}
}

// IsXIntOrString 对应 x-kubernetes-int-or-string 扩展。
func (s *Schema) IsXIntOrString() bool {
	return isXIntOrString(s.Schema)
}

// IsXEmbeddedResource 对应 x-kubernetes-embedded-resource 扩展。
func (s *Schema) IsXEmbeddedResource() bool {
	return isXEmbeddedResource(s.Schema)
}

// IsXPreserveUnknownFields 对应 x-kubernetes-preserve-unknown-fields 扩展。
func (s *Schema) IsXPreserveUnknownFields() bool {
	return isXPreserveUnknownFields(s.Schema)
}

// XListType 返回 listType（map/set/atomic），决定列表的合并语义。
func (s *Schema) XListType() string {
	return getXListType(s.Schema)
}

// XMapType 返回 mapType（granular/atomic）。
func (s *Schema) XMapType() string {
	return getXMapType(s.Schema)
}

// XListMapKeys 返回 listType=map 时用于标识条目的键字段。
func (s *Schema) XListMapKeys() []string {
	return getXListMapKeys(s.Schema)
}

// XValidations 返回 x-kubernetes-validations 声明，是 CEL 校验规则的来源。
func (s *Schema) XValidations() []common.ValidationRule {
	return getXValidations(s.Schema)
}

// WithTypeAndObjectMeta 返回补全了 type 与 metadata 描述的 schema 副本，
// 用于把根对象包装成 CEL 可识别的资源类型。
func (s *Schema) WithTypeAndObjectMeta() common.Schema {
	return &Schema{common.WithTypeAndObjectMeta(s.Schema)}
}

// UnstructuredToVal 把非结构化对象按 schema 转换为 CEL 值。
func UnstructuredToVal(unstructured any, schema *spec.Schema) ref.Val {
	return common.UnstructuredToVal(unstructured, &Schema{schema})
}

// SchemaDeclType 由 OpenAPI schema 推导 CEL 声明类型；isResourceRoot 决定是否附带 apiVersion/kind 等根字段。
func SchemaDeclType(s *spec.Schema, isResourceRoot bool) *apiservercel.DeclType {
	return common.SchemaDeclType(&Schema{Schema: s}, isResourceRoot)
}

// MakeMapList 为 listType=map 的列表提供按键查找的包装视图。
func MakeMapList(sts *spec.Schema, items []interface{}) (rv common.MapList) {
	return common.MakeMapList(&Schema{Schema: sts}, items)
}
