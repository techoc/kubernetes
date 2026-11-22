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

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// validateStructuralCompleteness 检查 s 中的每个值校验是否都有对应的结构位置，
// 从而保证每个值校验都作用于 schema 已知的值：
//   - 针对特定属性的校验必须具有该属性（或某个选项下的 additionalProperties）的结构定义
//   - additionalProperties 校验必须在对应节点的 schema 结构部分中定义了 additionalProperties
//   - items 校验也必须具有对应的结构 items
//
// schema 的“结构”部分指不经过任何 NestedValueValidations 即可遍历到的所有节点。
func validateStructuralCompleteness(s *Structural, fldPath *field.Path, opts ValidationOptions) field.ErrorList {
	if s == nil {
		return nil
	}

	return validateValueValidationCompleteness(s.ValueValidation, s, fldPath, fldPath, opts)
}

// validateValueValidationCompleteness 校验自身以及所有嵌套的 ValueValidation 是否完整。
func validateValueValidationCompleteness(v *ValueValidation, s *Structural, sPath, vPath *field.Path, opts ValidationOptions) field.ErrorList {
	if v == nil {
		return nil
	}
	if s == nil {
		return field.ErrorList{field.Required(sPath, fmt.Sprintf("because it is defined in %s", vPath.String()))}
	}

	allErrs := field.ErrorList{}

	allErrs = append(allErrs, validateNestedValueValidationCompleteness(v.Not, s, sPath, vPath.Child("not"), opts)...)
	for i := range v.AllOf {
		allErrs = append(allErrs, validateNestedValueValidationCompleteness(&v.AllOf[i], s, sPath, vPath.Child("allOf").Index(i), opts)...)
	}
	for i := range v.AnyOf {
		allErrs = append(allErrs, validateNestedValueValidationCompleteness(&v.AnyOf[i], s, sPath, vPath.Child("anyOf").Index(i), opts)...)
	}
	for i := range v.OneOf {
		allErrs = append(allErrs, validateNestedValueValidationCompleteness(&v.OneOf[i], s, sPath, vPath.Child("oneOf").Index(i), opts)...)
	}

	return allErrs
}

// validateNestedValueValidationCompleteness 递归校验嵌套值校验所引用的结构路径都存在，
// 对缺失的结构位置给出 Required 错误。
func validateNestedValueValidationCompleteness(v *NestedValueValidation, s *Structural, sPath, vPath *field.Path, opts ValidationOptions) field.ErrorList {
	if v == nil {
		return nil
	}
	if s == nil {
		return field.ErrorList{field.Required(sPath, fmt.Sprintf("because it is defined in %s", vPath.String()))}
	}

	allErrs := field.ErrorList{}

	allErrs = append(allErrs, validateValueValidationCompleteness(&v.ValueValidation, s, sPath, vPath, opts)...)
	allErrs = append(allErrs, validateNestedValueValidationCompleteness(v.Items, s.Items, sPath.Child("items"), vPath.Child("items"), opts)...)

	var sAdditionalPropertiesSchema *Structural
	if s.AdditionalProperties != nil {
		sAdditionalPropertiesSchema = s.AdditionalProperties.Structural
	}

	for k, vFld := range v.Properties {
		if sFld, ok := s.Properties[k]; !ok {
			if sAdditionalPropertiesSchema == nil || !opts.AllowValidationPropertiesWithAdditionalProperties {
				allErrs = append(allErrs, field.Required(sPath.Child("properties").Key(k), fmt.Sprintf("because it is defined in %s", vPath.Child("properties").Key(k))))
			} else {
				// 若结构 schema 中指定的是 additionalProperties（而非直接 properties），
				// 则允许对具体属性添加校验。
				// 注意：这不允许把 `additionalProperties: true` 的结构 schema
				// 与具体属性校验组合使用。
				allErrs = append(allErrs, validateNestedValueValidationCompleteness(&vFld, sAdditionalPropertiesSchema, sPath.Child("additionalProperties"), vPath.Child("properties").Key(k), opts)...)
			}
		} else {
			allErrs = append(allErrs, validateNestedValueValidationCompleteness(&vFld, &sFld, sPath.Child("properties").Key(k), vPath.Child("properties").Key(k), opts)...)
		}
	}

	if v.AdditionalProperties != nil && opts.AllowNestedAdditionalProperties {
		allErrs = append(allErrs, validateNestedValueValidationCompleteness(v.AdditionalProperties, sAdditionalPropertiesSchema, sPath.Child("additionalProperties"), vPath.Child("additionalProperties"), opts)...)
	}

	return allErrs
}
