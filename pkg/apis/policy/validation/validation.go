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
	"regexp"

	policyapiv1beta1 "k8s.io/api/policy/v1beta1"
	unversionedvalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	appsvalidation "k8s.io/kubernetes/pkg/apis/apps/validation"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/policy"
)

// supportedUnhealthyPodEvictionPolicies 列出不健康 Pod 允许逐出策略的合法取值集合。
var supportedUnhealthyPodEvictionPolicies = sets.NewString(
	string(policy.IfHealthyBudget),
	string(policy.AlwaysAllow),
)

// PodDisruptionBudgetValidationOptions 控制 PDB 校验的宽松程度。
type PodDisruptionBudgetValidationOptions struct {
	// AllowInvalidLabelValueInSelector 允许 selector 中出现不合法的标签值，
	// 用于兼容历史遗留对象；新建对象应始终使用严格校验。
	AllowInvalidLabelValueInSelector bool
}

// ValidatePodDisruptionBudget 校验 PodDisruptionBudget 并返回所有校验错误。
func ValidatePodDisruptionBudget(pdb *policy.PodDisruptionBudget, opts PodDisruptionBudgetValidationOptions) field.ErrorList {
	allErrs := ValidatePodDisruptionBudgetSpec(pdb.Spec, opts, field.NewPath("spec"))
	return allErrs
}

// ValidatePodDisruptionBudgetSpec 校验 PodDisruptionBudgetSpec 并返回所有校验错误。
func ValidatePodDisruptionBudgetSpec(spec policy.PodDisruptionBudgetSpec, opts PodDisruptionBudgetValidationOptions, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// minAvailable 与 maxUnavailable 语义互斥，同时设置会让预期副本数无法确定。
	if spec.MinAvailable != nil && spec.MaxUnavailable != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, spec, "minAvailable and maxUnavailable cannot be both set"))
	}

	// 两种阈值都必须是正整数或百分比，且不能超过 100%。
	if spec.MinAvailable != nil {
		allErrs = append(allErrs, appsvalidation.ValidatePositiveIntOrPercent(*spec.MinAvailable, fldPath.Child("minAvailable"))...)
		allErrs = append(allErrs, appsvalidation.IsNotMoreThan100Percent(*spec.MinAvailable, fldPath.Child("minAvailable"))...)
	}

	if spec.MaxUnavailable != nil {
		allErrs = append(allErrs, appsvalidation.ValidatePositiveIntOrPercent(*spec.MaxUnavailable, fldPath.Child("maxUnavailable"))...)
		allErrs = append(allErrs, appsvalidation.IsNotMoreThan100Percent(*spec.MaxUnavailable, fldPath.Child("maxUnavailable"))...)
	}

	labelSelectorValidationOptions := unversionedvalidation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: opts.AllowInvalidLabelValueInSelector}

	allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(spec.Selector, labelSelectorValidationOptions, fldPath.Child("selector"))...)

	// 逐出策略若非空，必须是已知枚举值，否则 disruption controller 无法解释其语义。
	if spec.UnhealthyPodEvictionPolicy != nil && !supportedUnhealthyPodEvictionPolicies.Has(string(*spec.UnhealthyPodEvictionPolicy)) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("unhealthyPodEvictionPolicy"), *spec.UnhealthyPodEvictionPolicy, supportedUnhealthyPodEvictionPolicies.List()))
	}

	return allErrs
}

// ValidatePodDisruptionBudgetStatusUpdate 校验 PDB 状态更新并返回所有校验错误。
func ValidatePodDisruptionBudgetStatusUpdate(status, oldStatus policy.PodDisruptionBudgetStatus, fldPath *field.Path, apiVersion schema.GroupVersion) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, unversionedvalidation.ValidateConditions(status.Conditions, fldPath.Child("conditions"))...)
	// 不对 v1beta1 追加其他校验：版本已冻结，追溯性引入新约束会拒绝过去合法的对象。
	if apiVersion == policyapiv1beta1.SchemeGroupVersion {
		return allErrs
	}
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.DisruptionsAllowed), fldPath.Child("disruptionsAllowed"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.CurrentHealthy), fldPath.Child("currentHealthy"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.DesiredHealthy), fldPath.Child("desiredHealthy"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.ExpectedPods), fldPath.Child("expectedPods"))...)
	return allErrs
}

// sysctlPatternSegmentFmt 定义 sysctl 模式中单个路径段的合法格式，
// 允许小写字母、数字、连字符、下划线，以及末尾的通配符 *。
const sysctlPatternSegmentFmt string = "([a-z0-9][-_a-z0-9]*)?[a-z0-9*]"

// SysctlContainSlashPatternFmt 是用于匹配合法 sysctl 模式的正则表达式片段，允许名称中包含斜杠。
const SysctlContainSlashPatternFmt string = "(" + apivalidation.SysctlSegmentFmt + "[\\./])*" + sysctlPatternSegmentFmt

// sysctlContainSlashPatternRegexp 是完整匹配 sysctl 模式的预编译正则。
var sysctlContainSlashPatternRegexp = regexp.MustCompile("^" + SysctlContainSlashPatternFmt + "$")

// IsValidSysctlPattern 检查 name 是否为合法的 sysctl 模式，
// 即是否匹配 sysctlContainSlashPatternRegexp。
// 更多信息参见：
//
//	https://man7.org/linux/man-pages/man8/sysctl.8.html
//	https://man7.org/linux/man-pages/man5/sysctl.d.5.html
func IsValidSysctlPattern(name string) bool {
	if len(name) > apivalidation.SysctlMaxLength {
		return false
	}
	return sysctlContainSlashPatternRegexp.MatchString(name)
}
