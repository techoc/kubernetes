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
	"k8s.io/apimachinery/pkg/api/validate/content"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unversionedvalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/rbac"
)

// ValidateRBACName 导出以供 RBAC API 组之外的类型复用同一套名称校验逻辑。
// 对 Role 和 Binding 的名称只做最小限度校验（与 OpenShift 一致），
// 因为名称会用于 URL 路径段；参考实现见：
// * https://github.com/kubernetes/kubernetes/blob/60db507b279ce45bd16ea3db49bf181f2aeb3c3d/pkg/api/validation/name.go
// * https://github.com/openshift/origin/blob/388478c40e751c4295dcb9a44dd69e5ac65d0e3b/pkg/api/helpers.go
func ValidateRBACName(name string, prefix bool) []string {
	return content.IsPathSegmentName(name)
}

// ValidateRole 校验 Role 的元数据和每条规则：
// Role 是命名空间级资源，规则中的非资源 URL 会被拒绝。
func ValidateRole(role *rbac.Role) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, validation.ValidateObjectMeta(&role.ObjectMeta, true, ValidateRBACName, field.NewPath("metadata"))...)

	// 逐条校验规则，错误路径带上索引以便定位到具体规则。
	for i, rule := range role.Rules {
		if err := ValidatePolicyRule(rule, true, field.NewPath("rules").Index(i)); err != nil {
			allErrs = append(allErrs, err...)
		}
	}
	if len(allErrs) != 0 {
		return allErrs
	}
	return nil
}

// ValidateRoleUpdate 在 ValidateRole 的基础上校验元数据更新是否合法
// （如命名空间、名称、resourceVersion 不可变）。
func ValidateRoleUpdate(role *rbac.Role, oldRole *rbac.Role) field.ErrorList {
	allErrs := ValidateRole(role)
	allErrs = append(allErrs, validation.ValidateObjectMetaUpdate(&role.ObjectMeta, &oldRole.ObjectMeta, field.NewPath("metadata"))...)

	return allErrs
}

// ClusterRoleValidationOptions 控制 ClusterRole 校验的可选行为，
// 例如是否允许标签选择器中出现非法标签值（兼容旧数据）。
type ClusterRoleValidationOptions struct {
	AllowInvalidLabelValueInSelector bool
}

// ValidateClusterRole 校验 ClusterRole 的元数据、规则以及聚合规则：
// 聚合规则非空时至少要有一个 selector，且每个 selector 必须合法且可转换。
func ValidateClusterRole(role *rbac.ClusterRole, opts ClusterRoleValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, validation.ValidateObjectMeta(&role.ObjectMeta, false, ValidateRBACName, field.NewPath("metadata"))...)

	for i, rule := range role.Rules {
		if err := ValidatePolicyRule(rule, false, field.NewPath("rules").Index(i)); err != nil {
			allErrs = append(allErrs, err...)
		}
	}

	labelSelectorValidationOptions := unversionedvalidation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: opts.AllowInvalidLabelValueInSelector}

	// 聚合规则一旦出现就不能是空的，否则无法明确聚合到哪些 ClusterRole。
	if role.AggregationRule != nil {
		if len(role.AggregationRule.ClusterRoleSelectors) == 0 {
			allErrs = append(allErrs, field.Required(field.NewPath("aggregationRule", "clusterRoleSelectors"), "at least one clusterRoleSelector required if aggregationRule is non-nil"))
		}
		for i, selector := range role.AggregationRule.ClusterRoleSelectors {
			fieldPath := field.NewPath("aggregationRule", "clusterRoleSelectors").Index(i)
			allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(&selector, labelSelectorValidationOptions, fieldPath)...)

			// 除结构校验外还需确保选择器能真正转换为运行时选择器。
			selector, err := metav1.LabelSelectorAsSelector(&selector)
			if err != nil {
				allErrs = append(allErrs, field.Invalid(fieldPath, selector, "invalid label selector."))
			}
		}
	}

	if len(allErrs) != 0 {
		return allErrs
	}
	return nil
}

// ValidateClusterRoleUpdate 在 ValidateClusterRole 的基础上校验元数据更新是否合法。
func ValidateClusterRoleUpdate(role *rbac.ClusterRole, oldRole *rbac.ClusterRole, opts ClusterRoleValidationOptions) field.ErrorList {
	allErrs := ValidateClusterRole(role, opts)
	allErrs = append(allErrs, validation.ValidateObjectMetaUpdate(&role.ObjectMeta, &oldRole.ObjectMeta, field.NewPath("metadata"))...)

	return allErrs
}

// ValidatePolicyRule 导出以供 RBAC API 组之外的类型复用规则校验。
// verbs 必须非空；非资源 URL 规则不能出现在命名空间作用域内，
// 也不能与资源规则混用；普通资源规则必须指定 apiGroups 和 resources。
func ValidatePolicyRule(rule rbac.PolicyRule, isNamespaced bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(rule.Verbs) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("verbs"), "verbs must contain at least one value").MarkCoveredByDeclarative())
	}

	// 非资源 URL（如 /healthz）与资源规则语义互斥，且不受命名空间约束。
	if len(rule.NonResourceURLs) > 0 {
		if isNamespaced {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nonResourceURLs"), rule.NonResourceURLs, "namespaced rules cannot apply to non-resource URLs"))
		}
		if len(rule.APIGroups) > 0 || len(rule.Resources) > 0 || len(rule.ResourceNames) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nonResourceURLs"), rule.NonResourceURLs, "rules cannot apply to both regular resources and non-resource URLs"))
		}
		return allErrs
	}

	if len(rule.APIGroups) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("apiGroups"), "resource rules must supply at least one api group"))
	}
	if len(rule.Resources) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("resources"), "resource rules must supply at least one resource"))
	}
	return allErrs
}

// ValidateRoleBinding 校验 RoleBinding 的元数据、roleRef 合法性
// 以及每个 subject 的字段；roleRef 目前只允许 RBAC 组的 Role/ClusterRole。
func ValidateRoleBinding(roleBinding *rbac.RoleBinding) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, validation.ValidateObjectMeta(&roleBinding.ObjectMeta, true, ValidateRBACName, field.NewPath("metadata"))...)

	// TODO allow multiple API groups.  For now, restrict to one, but I can envision other experimental roles in other groups taking
	// advantage of the binding infrastructure
	// 目前只允许 RBAC 组的角色引用，为将来其他 API 组复用绑定机制留有余地。
	if roleBinding.RoleRef.APIGroup != rbac.GroupName {
		allErrs = append(allErrs, field.NotSupported(field.NewPath("roleRef", "apiGroup"), roleBinding.RoleRef.APIGroup, []string{rbac.GroupName}))
	}

	// RoleBinding 可引用命名空间内的 Role 或集群范围的 ClusterRole。
	switch roleBinding.RoleRef.Kind {
	case "Role", "ClusterRole":
	default:
		allErrs = append(allErrs, field.NotSupported(field.NewPath("roleRef", "kind"), roleBinding.RoleRef.Kind, []string{"Role", "ClusterRole"}))
	}

	// roleRef.name 是必填项，且必须是合法的路径段名称。
	if len(roleBinding.RoleRef.Name) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("roleRef", "name"), "").MarkCoveredByDeclarative())
	} else {
		for _, msg := range ValidateRBACName(roleBinding.RoleRef.Name, false) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("roleRef", "name"), roleBinding.RoleRef.Name, msg))
		}
	}

	// 逐条校验 subject；isNamespaced 决定 ServiceAccount 是否必须带命名空间。
	subjectsPath := field.NewPath("subjects")
	for i, subject := range roleBinding.Subjects {
		allErrs = append(allErrs, ValidateRoleBindingSubject(subject, true, subjectsPath.Index(i))...)
	}

	return allErrs
}

// ValidateRoleBindingUpdate 在 ValidateRoleBinding 的基础上校验元数据更新，
// 并强制 roleRef 不可变：变更授权对象必须删除并重建绑定，防止权限被就地改写。
func ValidateRoleBindingUpdate(roleBinding *rbac.RoleBinding, oldRoleBinding *rbac.RoleBinding) field.ErrorList {
	allErrs := ValidateRoleBinding(roleBinding)
	allErrs = append(allErrs, validation.ValidateObjectMetaUpdate(&roleBinding.ObjectMeta, &oldRoleBinding.ObjectMeta, field.NewPath("metadata"))...)

	allErrs = append(allErrs, validation.ValidateImmutableField(roleBinding.RoleRef, oldRoleBinding.RoleRef, field.NewPath("roleRef")).WithOrigin("immutable").MarkAlpha().MarkCoveredByDeclarative()...)

	return allErrs
}

// ValidateClusterRoleBinding 校验 ClusterRoleBinding 的元数据、roleRef
// 以及每个 subject；集群级绑定只能引用 ClusterRole。
func ValidateClusterRoleBinding(roleBinding *rbac.ClusterRoleBinding) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, validation.ValidateObjectMeta(&roleBinding.ObjectMeta, false, ValidateRBACName, field.NewPath("metadata"))...)

	// TODO allow multiple API groups.  For now, restrict to one, but I can envision other experimental roles in other groups taking
	// advantage of the binding infrastructure
	// 目前只允许 RBAC 组的角色引用，为将来其他 API 组复用绑定机制留有余地。
	if roleBinding.RoleRef.APIGroup != rbac.GroupName {
		allErrs = append(allErrs, field.NotSupported(field.NewPath("roleRef", "apiGroup"), roleBinding.RoleRef.APIGroup, []string{rbac.GroupName}))
	}

	// ClusterRoleBinding 的授权范围是集群级，不能引用命名空间级的 Role。
	switch roleBinding.RoleRef.Kind {
	case "ClusterRole":
	default:
		allErrs = append(allErrs, field.NotSupported(field.NewPath("roleRef", "kind"), roleBinding.RoleRef.Kind, []string{"ClusterRole"}))
	}

	// roleRef.name 是必填项，且必须是合法的路径段名称。
	if len(roleBinding.RoleRef.Name) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("roleRef", "name"), "").MarkCoveredByDeclarative())
	} else {
		for _, msg := range ValidateRBACName(roleBinding.RoleRef.Name, false) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("roleRef", "name"), roleBinding.RoleRef.Name, msg))
		}
	}

	// 逐条校验 subject；isNamespaced 决定 ServiceAccount 是否必须带命名空间。
	subjectsPath := field.NewPath("subjects")
	for i, subject := range roleBinding.Subjects {
		allErrs = append(allErrs, ValidateRoleBindingSubject(subject, false, subjectsPath.Index(i))...)
	}

	return allErrs
}

// ValidateClusterRoleBindingUpdate 在 ValidateClusterRoleBinding 的基础上校验元数据更新，
// 并强制 roleRef 不可变，语义与 RoleBindingUpdate 一致。
func ValidateClusterRoleBindingUpdate(roleBinding *rbac.ClusterRoleBinding, oldRoleBinding *rbac.ClusterRoleBinding) field.ErrorList {
	allErrs := ValidateClusterRoleBinding(roleBinding)
	allErrs = append(allErrs, validation.ValidateObjectMetaUpdate(&roleBinding.ObjectMeta, &oldRoleBinding.ObjectMeta, field.NewPath("metadata"))...)

	allErrs = append(allErrs, apivalidation.ValidateImmutableField(roleBinding.RoleRef, oldRoleBinding.RoleRef, field.NewPath("roleRef")).WithOrigin("immutable").MarkCoveredByDeclarative()...)

	return allErrs
}

// ValidateRoleBindingSubject 导出以供 RBAC API 组之外的类型复用 subject 校验：
// name 必填；ServiceAccount 的 apiGroup 必须为空且在集群级绑定中必须指定命名空间；
// User 和 Group 的 apiGroup 必须是 RBAC 组名；kind 只能是三种已知类型。
func ValidateRoleBindingSubject(subject rbac.Subject, isNamespaced bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(subject.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), "").MarkCoveredByDeclarative())
	}

	// 不同 subject 类型的 apiGroup 和 namespace 约束不同，需分别处理。
	switch subject.Kind {
	case rbac.ServiceAccountKind:
		if len(subject.Name) > 0 {
			for _, msg := range validation.ValidateServiceAccountName(subject.Name, false) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), subject.Name, msg))
			}
		}
		// ServiceAccount 主体不使用 apiGroup，设置后将无法正确解析。
		if len(subject.APIGroup) > 0 {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("apiGroup"), subject.APIGroup, []string{""}))
		}
		// 集群级绑定没有默认命名空间，ServiceAccount 必须显式指明来自哪个命名空间。
		if !isNamespaced && len(subject.Namespace) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("namespace"), ""))
		}

	case rbac.UserKind:
		// TODO(ericchiang): What other restrictions on user name are there?
		if subject.APIGroup != rbac.GroupName {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("apiGroup"), subject.APIGroup, []string{rbac.GroupName}))
		}

	case rbac.GroupKind:
		// TODO(ericchiang): What other restrictions on group name are there?
		if subject.APIGroup != rbac.GroupName {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("apiGroup"), subject.APIGroup, []string{rbac.GroupName}))
		}

	default:
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("kind"), subject.Kind, []string{rbac.ServiceAccountKind, rbac.UserKind, rbac.GroupKind}))
	}

	return allErrs
}
