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
	"fmt"
	"reflect"
	"regexp"
	"sort"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// intOrStringAnyOf 是 int-or-string 这种例外结构中 anyOf/allOf 允许的两种类型。
var intOrStringAnyOf = []NestedValueValidation{
	{ForbiddenGenerics: Generic{
		Type: "integer",
	}},
	{ForbiddenGenerics: Generic{
		Type: "string",
	}},
}

// level 标识结构化 schema 校验所处的位置层级。
type level int

const (
	// rootLevel 表示 schema 的根节点。
	rootLevel level = iota
	// itemLevel 表示数组元素的 schema。
	itemLevel
	// fieldLevel 表示对象字段或 additionalProperties 的 schema。
	fieldLevel
)

// ValidationOptions 控制结构性校验的放宽开关，
// 用于在全量校验与特性化校验之间切换。
type ValidationOptions struct {
	// AllowNestedAdditionalProperties 允许在嵌套上下文
	// （allOf、anyOf、oneOf、not）中指定 additionalProperties。
	AllowNestedAdditionalProperties bool

	// AllowNestedXValidations 允许在嵌套上下文
	// （allOf、anyOf、oneOf、not）中指定 x-kubernetes-validations。
	AllowNestedXValidations bool

	// AllowValidationPropertiesWithAdditionalProperties 允许在由
	// additionalProperties 结构定义的属性上指定取值校验，
	// 例如主结构 schema 的 additionalProperties 在 allOf 内的属性。
	AllowValidationPropertiesWithAdditionalProperties bool
}

// ValidateStructural 校验 s 是否为一个满足下列不变性的结构化 schema：
//
//   - 结构性：ForbiddenGenerics 和 ForbiddenExtensions 只能为零值，
//     但 IntOrString 有两种例外。
//
//   - RawExtension：凡设置了 `x-kubernetes-embedded-resource: true` 的 schema，
//     必须同时设置 `x-kubernetes-preserve-unknown-fields: true` 且 `type: object`。
//
//   - IntOrString：对于 `x-kubernetes-int-or-string: true`，anyOf/allOf 下的 `type`
//     必须为空，或其结构为下列之一：
//
//     1. anyOf:
//
//   - type: integer
//
//   - type: string
//     2. allOf:
//
//   - anyOf:
//
//   - type: integer
//
//   - type: string
//
//   - ... 可再有零个或多个
//
//   - s 中指定的每个字段或数组，也必须在取值校验之外被指定。
//
//   - 根处的 metadata 只能约束 name 和 generateName，
//     且在嵌套上下文中完全不允许指定。
//
//   - 根处不允许 additionalProperties。
func ValidateStructural(fldPath *field.Path, s *Structural) field.ErrorList {
	return ValidateStructuralWithOptions(fldPath, s, ValidationOptions{
		// 置为 true 会放宽 CRD 的 schema，因此最初几个版本仍不接受任何放宽；
		// 但本地库和声明式校验原生类型时仍可使用。
		AllowNestedAdditionalProperties:                   false,
		AllowNestedXValidations:                           false,
		AllowValidationPropertiesWithAdditionalProperties: false,
	})
}

// ValidateStructuralWithOptions 在给定选项下校验结构化 schema，
// 返回全部错误。
func ValidateStructuralWithOptions(fldPath *field.Path, s *Structural, opts ValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, validateStructuralInvariants(s, rootLevel, fldPath, opts)...)
	allErrs = append(allErrs, validateStructuralCompleteness(s, fldPath, opts)...)

	// 对错误消息排序，否则由于类型中的 map 以及随机迭代顺序，
	// 每次返回的切片都会不同。
	sort.Slice(allErrs, func(i, j int) bool {
		return allErrs[i].Error() < allErrs[j].Error()
	})

	return allErrs
}

// validateStructuralInvariants 校验结构化 schema 的不变性。
func validateStructuralInvariants(s *Structural, lvl level, fldPath *field.Path, opts ValidationOptions) field.ErrorList {
	if s == nil {
		return nil
	}

	allErrs := field.ErrorList{}

	if s.Type == "array" && s.Items == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("items"), "must be specified"))
	}
	allErrs = append(allErrs, validateStructuralInvariants(s.Items, itemLevel, fldPath.Child("items"), opts)...)

	for k, v := range s.Properties {
		allErrs = append(allErrs, validateStructuralInvariants(&v, fieldLevel, fldPath.Child("properties").Key(k), opts)...)
	}

	if s.AdditionalProperties != nil {
		if lvl == rootLevel {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("additionalProperties"), "must not be used at the root"))
		}
		if s.AdditionalProperties.Structural != nil {
			allErrs = append(allErrs, validateStructuralInvariants(s.AdditionalProperties.Structural, fieldLevel, fldPath.Child("additionalProperties"), opts)...)
		}
	}

	allErrs = append(allErrs, validateGeneric(&s.Generic, lvl, fldPath)...)
	allErrs = append(allErrs, validateExtensions(&s.Extensions, fldPath)...)

	// 识别两种 IntOrString 例外结构：
	// 1) anyOf:
	//    - type: integer
	//    - type: string
	// 2) allOf:
	//    - anyOf:
	//      - type: integer
	//      - type: string
	//    - ... 可再有零个或多个
	skipAnyOf := isIntOrStringAnyOfPattern(s)
	skipFirstAllOfAnyOf := isIntOrStringAllOfPattern(s)

	allErrs = append(allErrs, validateValueValidation(s.ValueValidation, skipAnyOf, skipFirstAllOfAnyOf, lvl, fldPath, opts)...)

	checkMetadata := (lvl == rootLevel) || s.XEmbeddedResource

	if s.XEmbeddedResource && s.Type != "object" {
		if len(s.Type) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("type"), "must be object if x-kubernetes-embedded-resource is true"))
		} else {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("type"), s.Type, "must be object if x-kubernetes-embedded-resource is true"))
		}
	} else if len(s.Type) == 0 && !s.Extensions.XIntOrString && !s.Extensions.XPreserveUnknownFields {
		switch lvl {
		case rootLevel:
			allErrs = append(allErrs, field.Required(fldPath.Child("type"), "must not be empty at the root"))
		case itemLevel:
			allErrs = append(allErrs, field.Required(fldPath.Child("type"), "must not be empty for specified array items"))
		case fieldLevel:
			allErrs = append(allErrs, field.Required(fldPath.Child("type"), "must not be empty for specified object fields"))
		}
	}
	if s.XEmbeddedResource && s.AdditionalProperties != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("additionalProperties"), "must not be used if x-kubernetes-embedded-resource is set"))
	}

	if lvl == rootLevel && len(s.Type) > 0 && s.Type != "object" {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("type"), s.Type, "must be object at the root"))
	}

	// 仅把 metadata schema 限制在 name 和 generateName 上。
	if kind, found := s.Properties["kind"]; found && checkMetadata {
		if kind.Type != "string" {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("properties").Key("kind").Child("type"), kind.Type, "must be string"))
		}
	}
	if apiVersion, found := s.Properties["apiVersion"]; found && checkMetadata {
		if apiVersion.Type != "string" {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("properties").Key("apiVersion").Child("type"), apiVersion.Type, "must be string"))
		}
	}

	if metadata, found := s.Properties["metadata"]; found {
		allErrs = append(allErrs, validateStructuralMetadataInvariants(&metadata, checkMetadata, lvl, fldPath.Child("properties").Key("metadata"))...)
	}

	if s.XEmbeddedResource && !s.XPreserveUnknownFields && len(s.Properties) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("properties"), "must not be empty if x-kubernetes-embedded-resource is true without x-kubernetes-preserve-unknown-fields"))
	}

	return allErrs
}

// validateStructuralMetadataInvariants 校验 metadata 字段的结构性约束，
// 并就地修正根处的 metadata（仅保留 name/generateName）。
func validateStructuralMetadataInvariants(s *Structural, checkMetadata bool, lvl level, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if checkMetadata && s.Type != "object" {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("type"), s.Type, "must be object"))
	}

	if lvl == rootLevel {
		// metadata 是浅拷贝，可以直接就地修改。
		_, foundName := s.Properties["name"]
		_, foundGenerateName := s.Properties["generateName"]
		if foundName && foundGenerateName && len(s.Properties) == 2 {
			s.Properties = nil
		} else if (foundName || foundGenerateName) && len(s.Properties) == 1 {
			s.Properties = nil
		}
		s.Type = ""
		s.Default.Object = nil // 该默认值在 API 校验中处理（且已有测试覆盖）
		if s.ValueValidation == nil {
			s.ValueValidation = &ValueValidation{}
		}
		if !reflect.DeepEqual(*s, Structural{ValueValidation: &ValueValidation{}}) {
			// TODO: 这实际上是一个 field.Invalid 错误，但此处无法对 metadata 做 JSON 序列化以获得恰当消息
			allErrs = append(allErrs, field.Forbidden(fldPath, "must not specify anything other than name and generateName, but metadata is implicitly specified"))
		}
	}

	return allErrs
}

// isIntOrStringAnyOfPattern 判断 schema 的 anyOf 是否为 integer/string 的 IntOrString 结构。
func isIntOrStringAnyOfPattern(s *Structural) bool {
	if s == nil || s.ValueValidation == nil {
		return false
	}
	return len(s.ValueValidation.AnyOf) == 2 && reflect.DeepEqual(s.ValueValidation.AnyOf, intOrStringAnyOf)
}

// isIntOrStringAllOfPattern 判断 schema 的 allOf[0].anyOf 是否为 integer/string 的 IntOrString 结构。
func isIntOrStringAllOfPattern(s *Structural) bool {
	if s == nil || s.ValueValidation == nil {
		return false
	}
	return len(s.ValueValidation.AllOf) >= 1 && len(s.ValueValidation.AllOf[0].AnyOf) == 2 && reflect.DeepEqual(s.ValueValidation.AllOf[0].AnyOf, intOrStringAnyOf)
}

// validateGeneric 校验结构化 schema 的通用字段。
func validateGeneric(g *Generic, lvl level, fldPath *field.Path) field.ErrorList {
	if g == nil {
		return nil
	}

	return nil
}

// validateExtensions 校验结构化 schema 的 Kubernetes 扩展字段。
func validateExtensions(x *Extensions, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if x.XIntOrString && x.XPreserveUnknownFields {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("x-kubernetes-preserve-unknown-fields"), x.XPreserveUnknownFields, "must be false if x-kubernetes-int-or-string is true"))
	}
	if x.XIntOrString && x.XEmbeddedResource {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("x-kubernetes-embedded-resource"), x.XEmbeddedResource, "must be false if x-kubernetes-int-or-string is true"))
	}

	return allErrs
}

// validateValueValidation 校验结构化 schema 中的取值校验。
func validateValueValidation(v *ValueValidation, skipAnyOf, skipFirstAllOfAnyOf bool, lvl level, fldPath *field.Path, opts ValidationOptions) field.ErrorList {
	if v == nil {
		return nil
	}

	allErrs := field.ErrorList{}

	// 仍无条件禁止在量词中使用 XValidations；当前唯一允许的量词是 AllOf。
	// 这是实现层面的约束——SchemaValidator 需要感知 CEL 才能正确实现其他量词。
	optsWithCELDisabled := opts
	optsWithCELDisabled.AllowNestedXValidations = false

	if !skipAnyOf {
		for i := range v.AnyOf {
			allErrs = append(allErrs, validateNestedValueValidation(&v.AnyOf[i], false, false, lvl, fldPath.Child("anyOf").Index(i), optsWithCELDisabled)...)
		}
	}

	for i := range v.AllOf {
		skipAnyOf := false
		if skipFirstAllOfAnyOf && i == 0 {
			skipAnyOf = true
		}
		allErrs = append(allErrs, validateNestedValueValidation(&v.AllOf[i], skipAnyOf, false, lvl, fldPath.Child("allOf").Index(i), opts)...)
	}

	for i := range v.OneOf {
		allErrs = append(allErrs, validateNestedValueValidation(&v.OneOf[i], false, false, lvl, fldPath.Child("oneOf").Index(i), optsWithCELDisabled)...)
	}

	allErrs = append(allErrs, validateNestedValueValidation(v.Not, false, false, lvl, fldPath.Child("not"), optsWithCELDisabled)...)

	if len(v.Pattern) > 0 {
		if _, err := regexp.Compile(v.Pattern); err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("pattern"), v.Pattern, fmt.Sprintf("must be a valid regular expression, but isn't: %v", err)))
		}
	}

	return allErrs
}

// validateNestedValueValidation 校验结构化 schema 中逻辑连接词下嵌套的取值校验。
func validateNestedValueValidation(v *NestedValueValidation, skipAnyOf, skipAllOfAnyOf bool, lvl level, fldPath *field.Path, opts ValidationOptions) field.ErrorList {
	if v == nil {
		return nil
	}

	allErrs := field.ErrorList{}

	allErrs = append(allErrs, validateValueValidation(&v.ValueValidation, skipAnyOf, skipAllOfAnyOf, lvl, fldPath, opts)...)
	allErrs = append(allErrs, validateNestedValueValidation(v.Items, false, false, lvl, fldPath.Child("items"), opts)...)

	for k, fld := range v.Properties {
		allErrs = append(allErrs, validateNestedValueValidation(&fld, false, false, fieldLevel, fldPath.Child("properties").Key(k), opts)...)
	}

	if len(v.ForbiddenGenerics.Type) > 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("type"), "must be empty to be structural"))
	}
	if v.AdditionalProperties != nil && !opts.AllowNestedAdditionalProperties {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("additionalProperties"), "must be undefined to be structural"))
	} else {
		allErrs = append(allErrs, validateNestedValueValidation(v.AdditionalProperties, false, false, lvl, fldPath.Child("additionalProperties"), opts)...)
	}
	if v.ForbiddenGenerics.Default.Object != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("default"), "must be undefined to be structural"))
	}
	if len(v.ForbiddenGenerics.Title) > 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("title"), "must be empty to be structural"))
	}
	if len(v.ForbiddenGenerics.Description) > 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("description"), "must be empty to be structural"))
	}
	if v.ForbiddenGenerics.Nullable {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("nullable"), "must be false to be structural"))
	}

	if v.ForbiddenExtensions.XPreserveUnknownFields {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("x-kubernetes-preserve-unknown-fields"), "must be false to be structural"))
	}
	if v.ForbiddenExtensions.XEmbeddedResource {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("x-kubernetes-embedded-resource"), "must be false to be structural"))
	}
	if v.ForbiddenExtensions.XIntOrString {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("x-kubernetes-int-or-string"), "must be false to be structural"))
	}
	if len(v.ForbiddenExtensions.XListMapKeys) > 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("x-kubernetes-list-map-keys"), "must be empty to be structural"))
	}
	if v.ForbiddenExtensions.XListType != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("x-kubernetes-list-type"), "must be undefined to be structural"))
	}
	if v.ForbiddenExtensions.XMapType != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("x-kubernetes-map-type"), "must be undefined to be structural"))
	}
	if len(v.ValidationExtensions.XValidations) > 0 && !opts.AllowNestedXValidations {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("x-kubernetes-validations"), "must be empty to be structural"))
	}

	// 禁止在嵌套上下文中对 metadata 做推理，
	// 否则会引入我们不希望的 metadata 限制。
	if _, found := v.Properties["metadata"]; found {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("properties").Key("metadata"), "must not be specified in a nested context"))
	}

	return allErrs
}
