/*
Copyright 2015 The Kubernetes Authors.

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
	"context"
	"fmt"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/operation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	authorizationapi "k8s.io/kubernetes/pkg/apis/authorization"
)

// ValidateSubjectAccessReviewSpec 校验 SubjectAccessReviewSpec，返回包含所有错误的 ErrorList。
func ValidateSubjectAccessReviewSpec(spec authorizationapi.SubjectAccessReviewSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// resourceAttributes 与 nonResourceAttributes 是互斥联合字段，同时指定会让请求的含义产生歧义。
	if spec.ResourceAttributes != nil && spec.NonResourceAttributes != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, spec.NonResourceAttributes, `exactly one of nonResourceAttributes or resourceAttributes must be specified`).WithOrigin("union").MarkCoveredByDeclarative())
	}
	// 两者都不指定则无法判断请求针对资源还是非资源，同样非法。
	if spec.ResourceAttributes == nil && spec.NonResourceAttributes == nil {
		allErrs = append(allErrs, field.Invalid(fldPath, spec.NonResourceAttributes, `exactly one of nonResourceAttributes or resourceAttributes must be specified`).WithOrigin("union").MarkCoveredByDeclarative())
	}
	// 至少要有主体（user 或 group）之一，否则无法判断要检查谁的权限。
	if len(spec.User) == 0 && len(spec.Groups) == 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("user"), spec.User, `at least one of user or group must be specified`))
	}
	allErrs = append(allErrs, validateResourceAttributes(spec.ResourceAttributes, field.NewPath("spec.resourceAttributes"))...)

	return allErrs
}

// ValidateSelfSubjectAccessReviewSpec 校验 SelfSubjectAccessReviewSpec，返回包含所有错误的 ErrorList。
// 与 SubjectAccessReviewSpec 不同，此处主体隐含为当前用户，因此不要求显式指定 user 或 group。
func ValidateSelfSubjectAccessReviewSpec(spec authorizationapi.SelfSubjectAccessReviewSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// 与上面相同：资源与非资源属性必须二选一，不能同时出现，也不能都不出现。
	if spec.ResourceAttributes != nil && spec.NonResourceAttributes != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, spec.NonResourceAttributes, `exactly one of nonResourceAttributes or resourceAttributes must be specified`).WithOrigin("union").MarkCoveredByDeclarative())
	}
	if spec.ResourceAttributes == nil && spec.NonResourceAttributes == nil {
		allErrs = append(allErrs, field.Invalid(fldPath, spec.NonResourceAttributes, `exactly one of nonResourceAttributes or resourceAttributes must be specified`).WithOrigin("union").MarkCoveredByDeclarative())
	}
	allErrs = append(allErrs, validateResourceAttributes(spec.ResourceAttributes, field.NewPath("spec.resourceAttributes"))...)

	return allErrs
}

// ValidateSubjectAccessReview 校验 SubjectAccessReview，返回包含所有错误的 ErrorList。
// 除 spec 外，这类请求对象不允许携带任何元数据（metadata 必须为空）。
func ValidateSubjectAccessReview(sar *authorizationapi.SubjectAccessReview) field.ErrorList {
	allErrs := ValidateSubjectAccessReviewSpec(sar.Spec, field.NewPath("spec"))
	// 对 ObjectMeta 做浅拷贝后忽略 ManagedFields：它由服务端自动维护，不应被视为客户端填写的元数据。
	objectMetaShallowCopy := sar.ObjectMeta
	objectMetaShallowCopy.ManagedFields = nil
	if !apiequality.Semantic.DeepEqual(metav1.ObjectMeta{}, objectMetaShallowCopy) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("metadata"), sar.ObjectMeta, `must be empty`))
	}
	return allErrs
}

// ValidateSelfSubjectAccessReview 校验 SelfSubjectAccessReview，返回包含所有错误的 ErrorList。
// 与 SubjectAccessReview 一样，metadata 必须为空。
func ValidateSelfSubjectAccessReview(sar *authorizationapi.SelfSubjectAccessReview) field.ErrorList {
	allErrs := ValidateSelfSubjectAccessReviewSpec(sar.Spec, field.NewPath("spec"))
	// ManagedFields 由服务端填充，比对时剔除，避免误判为客户端非法携带的元数据。
	objectMetaShallowCopy := sar.ObjectMeta
	objectMetaShallowCopy.ManagedFields = nil
	if !apiequality.Semantic.DeepEqual(metav1.ObjectMeta{}, objectMetaShallowCopy) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("metadata"), sar.ObjectMeta, `must be empty`))
	}
	return allErrs
}

// ValidateLocalSubjectAccessReview 校验 LocalSubjectAccessReview，返回包含所有错误的 ErrorList。
// 此类请求只允许在 metadata 中携带 namespace，且该 namespace 必须与 spec 中的一致。
func ValidateLocalSubjectAccessReview(sar *authorizationapi.LocalSubjectAccessReview) field.ErrorList {
	allErrs := ValidateSubjectAccessReviewSpec(sar.Spec, field.NewPath("spec"))

	// 允许携带 namespace（用于请求作用域），其余元数据必须为空；ManagedFields 同样由服务端维护，忽略之。
	objectMetaShallowCopy := sar.ObjectMeta
	objectMetaShallowCopy.Namespace = ""
	objectMetaShallowCopy.ManagedFields = nil
	if !apiequality.Semantic.DeepEqual(metav1.ObjectMeta{}, objectMetaShallowCopy) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("metadata"), sar.ObjectMeta, `must be empty except for namespace`))
	}

	// 本地请求的命名空间作用域由 URL 决定，若 spec 中另填了不同的 namespace，会造成作用域混淆。
	if sar.Spec.ResourceAttributes != nil && sar.Spec.ResourceAttributes.Namespace != sar.Namespace {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec.resourceAttributes.namespace"), sar.Spec.ResourceAttributes.Namespace, `must match metadata.namespace`))
	}
	// 非资源请求不属于任何命名空间，因此不允许出现在 LocalSubjectAccessReview 中。
	if sar.Spec.NonResourceAttributes != nil {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec.nonResourceAttributes"), sar.Spec.NonResourceAttributes, `disallowed on this kind of request`))
	}

	return allErrs
}

// validateResourceAttributes 校验资源请求属性中的选择器字段；属性为空时直接跳过。
func validateResourceAttributes(resourceAttributes *authorizationapi.ResourceAttributes, fldPath *field.Path) field.ErrorList {
	if resourceAttributes == nil {
		return nil
	}
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, validateFieldSelectorAttributes(resourceAttributes.FieldSelector, fldPath.Child("fieldSelector"))...)
	allErrs = append(allErrs, validateLabelSelectorAttributes(resourceAttributes.LabelSelector, fldPath.Child("labelSelector"))...)

	return allErrs
}

// validateFieldSelectorAttributes 校验字段选择器属性。
// rawSelector 与 requirements 互斥且必须提供其一：前者在服务端解析，后者是已解析的结构化形式，
// 两者共存会导致不同组件对同一请求产生不同理解。
func validateFieldSelectorAttributes(selector *authorizationapi.FieldSelectorAttributes, fldPath *field.Path) field.ErrorList {
	if selector == nil {
		return nil
	}
	allErrs := field.ErrorList{}

	if len(selector.RawSelector) > 0 && len(selector.Requirements) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("rawSelector"), selector.RawSelector, "may not specified at the same time as requirements"))
	}
	// 若两个字段都为空，则选择器形同虚设，调用者很可能遗漏了必填内容。
	if len(selector.RawSelector) == 0 && len(selector.Requirements) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("requirements"), fmt.Sprintf("when %s is specified, requirements or rawSelector is required", fldPath)))
	}

	// AllowUnknownOperatorInRequirement 允许来自版本较新的客户端的 *SubjectAccessReview 请求通过校验，
	// 即便这些客户端理解 kube-apiserver 尚不认识的 operator。
	validationOptions := metav1validation.FieldSelectorValidationOptions{AllowUnknownOperatorInRequirement: true}
	for i, requirement := range selector.Requirements {
		allErrs = append(allErrs, metav1validation.ValidateFieldSelectorRequirement(requirement, validationOptions, fldPath.Child("requirements").Index(i))...)
	}

	return allErrs
}

// validateLabelSelectorAttributes 校验标签选择器属性。
// 与字段选择器一样，rawSelector 和 requirements 必须二选一，避免解析歧义。
func validateLabelSelectorAttributes(selector *authorizationapi.LabelSelectorAttributes, fldPath *field.Path) field.ErrorList {
	if selector == nil {
		return nil
	}
	allErrs := field.ErrorList{}

	if len(selector.RawSelector) > 0 && len(selector.Requirements) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("rawSelector"), selector.RawSelector, "may not specified at the same time as requirements"))
	}
	// 两个字段都为空时，无法表达任何实际的选择条件。
	if len(selector.RawSelector) == 0 && len(selector.Requirements) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("requirements"), fmt.Sprintf("when %s is specified, requirements or rawSelector is required", fldPath)))
	}

	// AllowUnknownOperatorInRequirement 允许来自版本较新的客户端的 *SubjectAccessReview 请求通过校验，
	// 即便这些客户端理解 kube-apiserver 尚不认识的 operator。
	// 对标签选择器放宽 operator 校验，同样是为了容忍客户端与服务端的版本差异。
	validationOptions := metav1validation.LabelSelectorValidationOptions{AllowUnknownOperatorInRequirement: true}
	for i, requirement := range selector.Requirements {
		allErrs = append(allErrs, metav1validation.ValidateLabelSelectorRequirement(requirement, validationOptions, fldPath.Child("requirements").Index(i))...)
	}

	return allErrs
}

// ValidateSubjectAccessReviewCreate 把手写的 SubjectAccessReview 校验与声明式校验组合为单一入口，
// 供创建（Create）操作调用。声明式部分通过 rest.DeclarativeValidation 执行。
func ValidateSubjectAccessReviewCreate(ctx context.Context, scheme *runtime.Scheme, sar *authorizationapi.SubjectAccessReview) field.ErrorList {
	errs := ValidateSubjectAccessReview(sar)
	dv := rest.DeclarativeValidation{Scheme: scheme}
	return dv.ValidateDeclaratively(ctx, sar, nil, errs, operation.Create, rest.DeclarativeValidationConfig{})
}

// ValidateSelfSubjectAccessReviewCreate 把手写的 SelfSubjectAccessReview 校验与声明式校验组合为单一入口，
// 供创建（Create）操作调用。
func ValidateSelfSubjectAccessReviewCreate(ctx context.Context, scheme *runtime.Scheme, sar *authorizationapi.SelfSubjectAccessReview) field.ErrorList {
	errs := ValidateSelfSubjectAccessReview(sar)
	dv := rest.DeclarativeValidation{Scheme: scheme}
	return dv.ValidateDeclaratively(ctx, sar, nil, errs, operation.Create, rest.DeclarativeValidationConfig{})
}

// ValidateLocalSubjectAccessReviewCreate 把手写的 LocalSubjectAccessReview 校验与声明式校验组合为单一入口，
// 供创建（Create）操作调用。
func ValidateLocalSubjectAccessReviewCreate(ctx context.Context, scheme *runtime.Scheme, sar *authorizationapi.LocalSubjectAccessReview) field.ErrorList {
	errs := ValidateLocalSubjectAccessReview(sar)
	dv := rest.DeclarativeValidation{Scheme: scheme}
	return dv.ValidateDeclaratively(ctx, sar, nil, errs, operation.Create, rest.DeclarativeValidationConfig{})
}
