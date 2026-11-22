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

package model

import (
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiserver/pkg/cel/common"
)

var _ common.Schema = (*Structural)(nil)
var _ common.SchemaOrBool = (*StructuralOrBool)(nil)

// Structural 把 schema.Structural 适配为 CEL 使用的 common.Schema 接口。
type Structural struct {
	Structural *schema.Structural
}

// StructuralOrBool 把 schema.StructuralOrBool 适配为 CEL 使用的 common.SchemaOrBool 接口。
type StructuralOrBool struct {
	StructuralOrBool *schema.StructuralOrBool
}

// Schema 返回 underlying 的结构 schema，未定义时返回 nil。
func (sb *StructuralOrBool) Schema() common.Schema {
	if sb.StructuralOrBool.Structural == nil {
		return nil
	}
	return &Structural{Structural: sb.StructuralOrBool.Structural}
}

// Allows 报告 schema 或布尔值中布尔值为 true 的情况。
func (sb *StructuralOrBool) Allows() bool {
	return sb.StructuralOrBool.Bool
}

// Type 返回 schema 声明的类型。
func (s *Structural) Type() string {
	return s.Structural.Type
}

// Format 返回值校验中声明的 format。
func (s *Structural) Format() string {
	if s.Structural.ValueValidation == nil {
		return ""
	}
	return s.Structural.ValueValidation.Format
}

// Pattern 返回值校验中声明的 pattern。
func (s *Structural) Pattern() string {
	if s.Structural.ValueValidation == nil {
		return ""
	}
	return s.Structural.ValueValidation.Pattern
}

// Items 返回数组元素的结构 schema。
func (s *Structural) Items() common.Schema {
	if s.Structural.Items == nil {
		return nil
	}
	return &Structural{Structural: s.Structural.Items}
}

// Properties 返回对象属性的结构 schema 映射。
func (s *Structural) Properties() map[string]common.Schema {
	if s.Structural.Properties == nil {
		return nil
	}
	res := make(map[string]common.Schema, len(s.Structural.Properties))
	for n, prop := range s.Structural.Properties {
		s := prop
		res[n] = &Structural{Structural: &s}
	}
	return res
}

// AdditionalProperties 返回对象 additionalProperties 的结构 schema。
func (s *Structural) AdditionalProperties() common.SchemaOrBool {
	if s.Structural.AdditionalProperties == nil {
		return nil
	}
	return &StructuralOrBool{StructuralOrBool: s.Structural.AdditionalProperties}
}

// Default 返回 schema 声明的默认值。
func (s *Structural) Default() any {
	return s.Structural.Default.Object
}

// Minimum 返回值校验的最小值约束。
func (s *Structural) Minimum() *float64 {
	if s.Structural.ValueValidation == nil {
		return nil
	}
	return s.Structural.ValueValidation.Minimum
}

// IsExclusiveMinimum 报告最小值约束是否为排他约束。
func (s *Structural) IsExclusiveMinimum() bool {
	if s.Structural.ValueValidation == nil {
		return false
	}
	return s.Structural.ValueValidation.ExclusiveMinimum
}

// Maximum 返回值校验的最大值约束。
func (s *Structural) Maximum() *float64 {
	if s.Structural.ValueValidation == nil {
		return nil
	}
	return s.Structural.ValueValidation.Maximum
}

// IsExclusiveMaximum 报告最大值约束是否为排他约束。
func (s *Structural) IsExclusiveMaximum() bool {
	if s.Structural.ValueValidation == nil {
		return false
	}
	return s.Structural.ValueValidation.ExclusiveMaximum
}

// MultipleOf 返回值校验的 multipleOf 约束。
func (s *Structural) MultipleOf() *float64 {
	if s.Structural.ValueValidation == nil {
		return nil
	}
	return s.Structural.ValueValidation.MultipleOf
}

// MinItems 返回数组最小元素数约束。
func (s *Structural) MinItems() *int64 {
	if s.Structural.ValueValidation == nil {
		return nil
	}
	return s.Structural.ValueValidation.MinItems
}

// MaxItems 返回数组最大元素数约束。
func (s *Structural) MaxItems() *int64 {
	if s.Structural.ValueValidation == nil {
		return nil
	}
	return s.Structural.ValueValidation.MaxItems
}

// MinLength 返回字符串最小长度约束。
func (s *Structural) MinLength() *int64 {
	if s.Structural.ValueValidation == nil {
		return nil
	}
	return s.Structural.ValueValidation.MinLength
}

// MaxLength 返回字符串最大长度约束。
func (s *Structural) MaxLength() *int64 {
	if s.Structural.ValueValidation == nil {
		return nil
	}
	return s.Structural.ValueValidation.MaxLength
}

// MinProperties 返回对象最少属性数约束。
func (s *Structural) MinProperties() *int64 {
	if s.Structural.ValueValidation == nil {
		return nil
	}
	return s.Structural.ValueValidation.MinProperties
}

// MaxProperties 返回对象最多属性数约束。
func (s *Structural) MaxProperties() *int64 {
	if s.Structural.ValueValidation == nil {
		return nil
	}
	return s.Structural.ValueValidation.MaxProperties
}

// Required 返回必需属性列表。
func (s *Structural) Required() []string {
	if s.Structural.ValueValidation == nil {
		return nil
	}
	return s.Structural.ValueValidation.Required
}

// UniqueItems 返回元素是否唯一。结构化 schema 中禁止该字段，因此恒为 false。
func (s *Structural) UniqueItems() bool {
	// 该字段在结构化 schema 中被禁止。
	// 但可以用 x-kubernetes-list-type:set 达到同样效果 :)
	return false
}

// Enum 返回枚举取值列表。
func (s *Structural) Enum() []any {
	if s.Structural.ValueValidation == nil {
		return nil
	}
	ret := make([]any, 0, len(s.Structural.ValueValidation.Enum))
	for _, e := range s.Structural.ValueValidation.Enum {
		ret = append(ret, e.Object)
	}
	return ret
}

// Nullable 报告 schema 是否允许 null。
func (s *Structural) Nullable() bool {
	return s.Structural.Nullable
}

// IsXIntOrString 报告 schema 是否标记为 x-kubernetes-int-or-string。
func (s *Structural) IsXIntOrString() bool {
	return s.Structural.XIntOrString
}

// IsXEmbeddedResource 报告 schema 是否标记为 x-kubernetes-embedded-resource。
func (s *Structural) IsXEmbeddedResource() bool {
	return s.Structural.XEmbeddedResource
}

// IsXPreserveUnknownFields 报告 schema 是否标记为 x-kubernetes-preserve-unknown-fields。
func (s *Structural) IsXPreserveUnknownFields() bool {
	return s.Structural.XPreserveUnknownFields
}

// XListType 返回 x-kubernetes-list-type 值。
func (s *Structural) XListType() string {
	if s.Structural.XListType == nil {
		return ""
	}
	return *s.Structural.XListType
}

// XMapType 返回 x-kubernetes-map-type 值。
func (s *Structural) XMapType() string {
	if s.Structural.XMapType == nil {
		return ""
	}
	return *s.Structural.XMapType
}

// XListMapKeys 返回 x-kubernetes-list-map-keys 值。
func (s *Structural) XListMapKeys() []string {
	return s.Structural.XListMapKeys
}

// AllOf 返回 allOf 子 schema。
func (s *Structural) AllOf() []common.Schema {
	var res []common.Schema
	for _, subSchema := range s.Structural.ValueValidation.AllOf {
		res = append(res, nestedValueValidationToStructural(&subSchema))
	}
	return res
}

// AnyOf 返回 anyOf 子 schema。
func (s *Structural) AnyOf() []common.Schema {
	var res []common.Schema
	for _, subSchema := range s.Structural.ValueValidation.AnyOf {
		res = append(res, nestedValueValidationToStructural(&subSchema))
	}
	return res
}

// OneOf 返回 oneOf 子 schema。
func (s *Structural) OneOf() []common.Schema {
	var res []common.Schema
	for _, subSchema := range s.Structural.ValueValidation.OneOf {
		res = append(res, nestedValueValidationToStructural(&subSchema))
	}
	return res
}

// Not 返回 not 子 schema。
func (s *Structural) Not() common.Schema {
	if s.Structural.ValueValidation.Not == nil {
		return nil
	}
	return nestedValueValidationToStructural(s.Structural.ValueValidation.Not)
}

// nestedValueValidationToStructural 把嵌套值校验转换为等价的结构化 schema 实例。
//
// 这样就不必为嵌套值校验单独实现适配器，而且开销不大：通常我们本来就要遍历整个 schema。
func nestedValueValidationToStructural(nvv *schema.NestedValueValidation) *Structural {
	var newItems *schema.Structural
	if nvv.Items != nil {
		newItems = nestedValueValidationToStructural(nvv.Items).Structural
	}

	var newProperties map[string]schema.Structural
	for k, v := range nvv.Properties {
		if newProperties == nil {
			newProperties = make(map[string]schema.Structural)
		}

		v := v
		newProperties[k] = *nestedValueValidationToStructural(&v).Structural
	}

	var newAdditionalProperties *schema.StructuralOrBool
	if nvv.AdditionalProperties != nil {
		newAdditionalProperties = &schema.StructuralOrBool{Structural: nestedValueValidationToStructural(nvv.AdditionalProperties).Structural}
	}

	return &Structural{
		Structural: &schema.Structural{
			Items:                newItems,
			Properties:           newProperties,
			AdditionalProperties: newAdditionalProperties,
			ValueValidation:      &nvv.ValueValidation,
			ValidationExtensions: nvv.ValidationExtensions,
		},
	}
}

// StructuralValidationRule 实现 common.ValidationRule，携带单条 x-kubernetes-validations 规则的信息。
type StructuralValidationRule struct {
	rule, message, messageExpression, fieldPath string
}

// Rule 返回规则表达式。
func (s *StructuralValidationRule) Rule() string {
	return s.rule
}

// Message 返回规则不满足时的静态消息。
func (s *StructuralValidationRule) Message() string {
	return s.message
}

// FieldPath 返回规则报告错误时使用的字段路径。
func (s *StructuralValidationRule) FieldPath() string {
	return s.fieldPath
}

// MessageExpression 返回规则不满足时用于生成消息的 CEL 表达式。
func (s *StructuralValidationRule) MessageExpression() string {
	return s.messageExpression
}

// XValidations 返回该节点的 x-kubernetes-validations 规则列表。
func (s *Structural) XValidations() []common.ValidationRule {
	if len(s.Structural.XValidations) == 0 {
		return nil
	}
	result := make([]common.ValidationRule, len(s.Structural.XValidations))
	for i, v := range s.Structural.XValidations {
		result[i] = &StructuralValidationRule{rule: v.Rule, message: v.Message, messageExpression: v.MessageExpression, fieldPath: v.FieldPath}
	}
	return result
}

// WithTypeAndObjectMeta 返回附加了 apiVersion、kind、metadata 属性的结构 schema。
func (s *Structural) WithTypeAndObjectMeta() common.Schema {
	return &Structural{Structural: WithTypeAndObjectMeta(s.Structural)}
}
