/*
Copyright 2016 The Kubernetes Authors.

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

package validation

import (
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/sample-apiserver/pkg/apis/wardle"
)

// ValidateFlunder validates a Flunder.
// 校验逻辑放在内部版本，各版本 API 通过转换汇合到这里，避免规则散落。
func ValidateFlunder(f *wardle.Flunder) field.ErrorList {
	allErrs := field.ErrorList{}

	// 目前只有 spec 需要校验；保留聚合列表便于以后追加字段级错误。
	allErrs = append(allErrs, ValidateFlunderSpec(&f.Spec, field.NewPath("spec"))...)

	return allErrs
}

// ValidateFlunderSpec validates a FlunderSpec.
// 用 else-if 串联保证同一组字段只报一个最相关的错误，避免刷屏。
func ValidateFlunderSpec(s *wardle.FlunderSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 两个引用字段互斥，且必须与 referenceType 对应；类型为空视为未指定，不强制报错。
	if len(s.FlunderReference) != 0 && len(s.FischerReference) != 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("fischerReference"), s.FischerReference, "cannot be set with flunderReference at the same time"))
	} else if len(s.FlunderReference) != 0 && s.ReferenceType != wardle.FlunderReferenceType {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("flunderReference"), s.FlunderReference, "cannot be set if referenceType is not Flunder"))
	} else if len(s.FischerReference) != 0 && s.ReferenceType != wardle.FischerReferenceType {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("fischerReference"), s.FischerReference, "cannot be set if referenceType is not Fischer"))
	} else if len(s.FischerReference) == 0 && s.ReferenceType == wardle.FischerReferenceType {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("fischerReference"), s.FischerReference, "cannot be empty if referenceType is Fischer"))
	} else if len(s.FlunderReference) == 0 && s.ReferenceType == wardle.FlunderReferenceType {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("flunderReference"), s.FlunderReference, "cannot be empty if referenceType is Flunder"))
	}

	// 单独校验取值合法性：即使引用字段都为空，非法的 referenceType 也不能放过。
	if len(s.ReferenceType) != 0 && s.ReferenceType != wardle.FischerReferenceType && s.ReferenceType != wardle.FlunderReferenceType {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("referenceType"), s.ReferenceType, "must be Flunder or Fischer"))
	}

	return allErrs
}
