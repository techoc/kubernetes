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

package validation

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/validate/content"
	genericvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	plugincel "k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/admission/plugin/policy/mutating/patch"
	validatingadmissionpolicy "k8s.io/apiserver/pkg/admission/plugin/policy/validating"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/matchconditions"
	"k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/util/jsonpath"

	"k8s.io/kubernetes/pkg/apis/admissionregistration"
	admissionregistrationv1 "k8s.io/kubernetes/pkg/apis/admissionregistration/v1"
	admissionregistrationv1beta1 "k8s.io/kubernetes/pkg/apis/admissionregistration/v1beta1"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
)

// hasWildcard 判断 slice 中是否包含 "*" 通配符。
// 通配符与具体取值同时出现时匹配范围会重叠，语义无法明确定义，因此需要被拒绝。
func hasWildcard(slice []string) bool {
	for _, s := range slice {
		if s == "*" {
			return true
		}
	}
	return false
}

// validateResources 校验允许子资源的 resources 列表，支持 "resource/subresource" 及 "*" / "*/*"。
// 通配符必须是唯一取值：否则不同条目之间会发生重叠匹配，webhook 的匹配行为难以预测。
func validateResources(resources []string, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	if len(resources) == 0 {
		allErrors = append(allErrors, field.Required(fldPath, ""))
	}

	// 记录形如 x/* 的资源，用于检测后续 x/y 是否与之重叠
	resourcesWithWildcardSubresoures := sets.String{}
	// 记录形如 */x 的子资源，用于检测后续 y/x 是否与之重叠
	subResourcesWithWildcardResource := sets.String{}
	// 是否出现 */*（匹配所有资源及子资源）
	hasDoubleWildcard := false
	// 是否出现 *（匹配所有资源，但不含子资源）
	hasSingleWildcard := false
	// 是否出现不带子资源的具体资源名（如 x）
	hasResourceWithoutSubresource := false

	for i, resSub := range resources {
		if resSub == "" {
			allErrors = append(allErrors, field.Required(fldPath.Index(i), ""))
			continue
		}
		if resSub == "*/*" {
			hasDoubleWildcard = true
		}
		if resSub == "*" {
			hasSingleWildcard = true
		}
		parts := strings.SplitN(resSub, "/", 2)
		if len(parts) == 1 {
			hasResourceWithoutSubresource = resSub != "*"
			continue
		}
		res, sub := parts[0], parts[1]
		if _, ok := resourcesWithWildcardSubresoures[res]; ok {
			allErrors = append(allErrors, field.Invalid(fldPath.Index(i), resSub, fmt.Sprintf("if '%s/*' is present, must not specify %s", res, resSub)))
		}
		if _, ok := subResourcesWithWildcardResource[sub]; ok {
			allErrors = append(allErrors, field.Invalid(fldPath.Index(i), resSub, fmt.Sprintf("if '*/%s' is present, must not specify %s", sub, resSub)))
		}
		if sub == "*" {
			resourcesWithWildcardSubresoures[res] = struct{}{}
		}
		if res == "*" {
			subResourcesWithWildcardResource[sub] = struct{}{}
		}
	}
	if len(resources) > 1 && hasDoubleWildcard {
		allErrors = append(allErrors, field.Invalid(fldPath, resources, "if '*/*' is present, must not specify other resources"))
	}
	if hasSingleWildcard && hasResourceWithoutSubresource {
		allErrors = append(allErrors, field.Invalid(fldPath, resources, "if '*' is present, must not specify other resources without subresources"))
	}
	return allErrors
}

// validateResourcesNoSubResources 校验不允许子资源的 resources 列表。
// 与 validateResources 相比禁止出现 "/"，通配符同样必须独占，理由同上。
func validateResourcesNoSubResources(resources []string, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	if len(resources) == 0 {
		allErrors = append(allErrors, field.Required(fldPath, ""))
	}
	for i, resource := range resources {
		if resource == "" {
			allErrors = append(allErrors, field.Required(fldPath.Index(i), ""))
		}
		if strings.Contains(resource, "/") {
			allErrors = append(allErrors, field.Invalid(fldPath.Index(i), resource, "must not specify subresources"))
		}
	}
	if len(resources) > 1 && hasWildcard(resources) {
		allErrors = append(allErrors, field.Invalid(fldPath, resources, "if '*' is present, must not specify other resources"))
	}
	return allErrors
}

// validScopes 是 scope 字段的合法取值：集群级、命名空间级或两者皆可。
var validScopes = sets.NewString(
	string(admissionregistration.ClusterScope),
	string(admissionregistration.NamespacedScope),
	string(admissionregistration.AllScopes),
)

// validateRule 校验 Rule 的 apiGroups / apiVersions / resources / scope。
// apiGroups 与 apiVersions 中的 "*" 必须独占；resources 的校验方式由 allowSubResource 决定。
func validateRule(rule *admissionregistration.Rule, fldPath *field.Path, allowSubResource bool) field.ErrorList {
	var allErrors field.ErrorList
	if len(rule.APIGroups) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("apiGroups"), ""))
	}
	if len(rule.APIGroups) > 1 && hasWildcard(rule.APIGroups) {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("apiGroups"), rule.APIGroups, "if '*' is present, must not specify other API groups"))
	}
	// group 允许为空，例如遗留的核心 API（"v1"），因此此处不校验 apiGroups 元素非空
	if len(rule.APIVersions) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("apiVersions"), ""))
	}
	if len(rule.APIVersions) > 1 && hasWildcard(rule.APIVersions) {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("apiVersions"), rule.APIVersions, "if '*' is present, must not specify other API versions"))
	}
	for i, version := range rule.APIVersions {
		if version == "" {
			allErrors = append(allErrors, field.Required(fldPath.Child("apiVersions").Index(i), ""))
		}
	}
	if allowSubResource {
		allErrors = append(allErrors, validateResources(rule.Resources, fldPath.Child("resources"))...)
	} else {
		allErrors = append(allErrors, validateResourcesNoSubResources(rule.Resources, fldPath.Child("resources"))...)
	}
	if rule.Scope != nil && !validScopes.Has(string(*rule.Scope)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("scope"), *rule.Scope, validScopes.List()))
	}
	return allErrors
}

// AcceptedAdmissionReviewVersions 列出"上一个版本"的 API server 能理解的 AdmissionReview 版本。
// 保留旧版本是为了滚动升级期间不破坏仍运行旧代码的 apiserver：webhook 必须至少声明其中一个版本。
// 1.15：服务端理解 v1beta1；可接受版本为 ["v1beta1"]
// 1.16：服务端理解 v1、v1beta1；出于兼容仍只接受 ["v1beta1"]
// 1.17+：服务端理解 v1、v1beta1；可接受版本为 ["v1","v1beta1"]
var AcceptedAdmissionReviewVersions = []string{admissionregistrationv1.SchemeGroupVersion.Version, admissionregistrationv1beta1.SchemeGroupVersion.Version}

// isAcceptedAdmissionReviewVersion 判断 v 是否在 AcceptedAdmissionReviewVersions 中。
func isAcceptedAdmissionReviewVersion(v string) bool {
	for _, version := range AcceptedAdmissionReviewVersions {
		if v == version {
			return true
		}
	}
	return false
}

// validateAdmissionReviewVersions 校验 admissionReviewVersions 非空、无重复、符合 DNS1035 标签格式，
// 并在 requireRecognizedAdmissionReviewVersion 为真时要求至少包含一个可识别版本。
func validateAdmissionReviewVersions(versions []string, requireRecognizedAdmissionReviewVersion bool, fldPath *field.Path) field.ErrorList {
	allErrors := field.ErrorList{}

	// 要求至少声明一个当前（或上一版本）apiserver 可接受的 AdmissionReview 版本
	if len(versions) < 1 {
		allErrors = append(allErrors, field.Required(fldPath, fmt.Sprintf("must specify one of %v", strings.Join(AcceptedAdmissionReviewVersions, ", "))))
	} else {
		seen := map[string]bool{}
		hasAcceptedVersion := false
		for i, v := range versions {
			if seen[v] {
				allErrors = append(allErrors, field.Invalid(fldPath.Index(i), v, "duplicate version"))
				continue
			}
			seen[v] = true
			for _, errString := range utilvalidation.IsDNS1035Label(v) {
				allErrors = append(allErrors, field.Invalid(fldPath.Index(i), v, errString))
			}
			if isAcceptedAdmissionReviewVersion(v) {
				hasAcceptedVersion = true
			}
		}
		if requireRecognizedAdmissionReviewVersion && !hasAcceptedVersion {
			allErrors = append(allErrors, field.Invalid(
				fldPath, versions,
				fmt.Sprintf("must include at least one of %v",
					strings.Join(AcceptedAdmissionReviewVersions, ", "))))
		}
	}
	return allErrors
}

// ValidateValidatingWebhookConfiguration 在创建前校验 ValidatingWebhookConfiguration，使用最严格的校验选项。
func ValidateValidatingWebhookConfiguration(e *admissionregistration.ValidatingWebhookConfiguration) field.ErrorList {
	return validateValidatingWebhookConfiguration(e, validationOptions{
		ignoreMatchConditions:                   false,
		allowParamsInMatchConditions:            false,
		requireNoSideEffects:                    true,
		requireRecognizedAdmissionReviewVersion: true,
		requireUniqueWebhookNames:               true,
		allowInvalidLabelValueInSelector:        false,
	})
}

// validateValidatingWebhookConfiguration 校验元数据及每个 webhook；
// 各 webhook 的错误会累积到同一个 ErrorList，便于调用方一次性看到全部问题。
func validateValidatingWebhookConfiguration(e *admissionregistration.ValidatingWebhookConfiguration, opts validationOptions) field.ErrorList {
	allErrors := genericvalidation.ValidateObjectMeta(&e.ObjectMeta, false, genericvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	hookNames := sets.NewString()
	for i, hook := range e.Webhooks {
		allErrors = append(allErrors, validateValidatingWebhook(&hook, opts, field.NewPath("webhooks").Index(i))...)
		allErrors = append(allErrors, validateAdmissionReviewVersions(hook.AdmissionReviewVersions, opts.requireRecognizedAdmissionReviewVersion, field.NewPath("webhooks").Index(i).Child("admissionReviewVersions"))...)
		if opts.requireUniqueWebhookNames && len(hook.Name) > 0 {
			if hookNames.Has(hook.Name) {
				allErrors = append(allErrors, field.Duplicate(field.NewPath("webhooks").Index(i).Child("name"), hook.Name))
			} else {
				hookNames.Insert(hook.Name)
			}
		}
	}
	return allErrors
}

// ValidateMutatingWebhookConfiguration 在创建前校验 MutatingWebhookConfiguration，使用最严格的校验选项。
func ValidateMutatingWebhookConfiguration(e *admissionregistration.MutatingWebhookConfiguration) field.ErrorList {
	return validateMutatingWebhookConfiguration(e, validationOptions{
		ignoreMatchConditions:                   false,
		allowParamsInMatchConditions:            false,
		requireNoSideEffects:                    true,
		requireRecognizedAdmissionReviewVersion: true,
		requireUniqueWebhookNames:               true,
		allowInvalidLabelValueInSelector:        false,
	})
}

// validationOptions 控制校验的严格程度。
// 更新场景会依据旧对象放宽部分约束，避免历史遗留的合法状态在升级后被拒绝。
type validationOptions struct {
	// 旧对象的 matchConditions 已校验过时，更新可跳过，避免新编译器拒绝旧表达式
	ignoreMatchConditions bool
	// matchConditions 中是否允许引用 params（仅当 spec 定义了 paramKind 时才有意义）
	allowParamsInMatchConditions bool
	// 是否强制 sideEffects 为 None/NoneOnDryRun
	requireNoSideEffects bool
	// 是否要求声明至少一个当前 apiserver 可识别的 AdmissionReview 版本
	requireRecognizedAdmissionReviewVersion bool
	// 是否要求同一配置内的 webhook 名称唯一
	requireUniqueWebhookNames bool
	// 是否容忍 selector 中非法的 label value（兼容旧对象）
	allowInvalidLabelValueInSelector bool
	// 旧对象中已存在的表达式，用于选择兼容性编译环境
	preexistingExpressions preexistingExpressions
}

// preexistingExpressions 按用途收集旧对象中已有的 CEL 表达式。
// 校验更新时使用 StoredExpressions 环境编译这些表达式，防止新增限制破坏既有对象。
type preexistingExpressions struct {
	matchConditionExpressions        sets.Set[string]
	validationExpressions            sets.Set[string]
	validationMessageExpressions     sets.Set[string]
	auditAnnotationValuesExpressions sets.Set[string]
	applyConfigurationExpressions    sets.Set[string]
	jsonPatchExpressions             sets.Set[string]
}

// newPreexistingExpressions 创建各用途均为空集合的 preexistingExpressions。
func newPreexistingExpressions() preexistingExpressions {
	return preexistingExpressions{
		matchConditionExpressions:        sets.New[string](),
		validationExpressions:            sets.New[string](),
		validationMessageExpressions:     sets.New[string](),
		auditAnnotationValuesExpressions: sets.New[string](),
		applyConfigurationExpressions:    sets.New[string](),
		jsonPatchExpressions:             sets.New[string](),
	}
}

// findMutatingPreexistingExpressions 收集 MutatingWebhookConfiguration 中已有的表达式。
// 目前 webhook 只可能在 matchConditions 中使用 CEL。
func findMutatingPreexistingExpressions(mutating *admissionregistration.MutatingWebhookConfiguration) preexistingExpressions {
	preexisting := newPreexistingExpressions()
	for _, wh := range mutating.Webhooks {
		for _, mc := range wh.MatchConditions {
			preexisting.matchConditionExpressions.Insert(mc.Expression)
		}
	}
	return preexisting
}

// findValidatingPreexistingExpressions 收集 ValidatingWebhookConfiguration 中已有的表达式。
// 目前 webhook 只可能在 matchConditions 中使用 CEL。
func findValidatingPreexistingExpressions(validating *admissionregistration.ValidatingWebhookConfiguration) preexistingExpressions {
	preexisting := newPreexistingExpressions()
	for _, wh := range validating.Webhooks {
		for _, mc := range wh.MatchConditions {
			preexisting.matchConditionExpressions.Insert(mc.Expression)
		}
	}
	return preexisting
}

// findValidatingPolicyPreexistingExpressions 收集 ValidatingAdmissionPolicy 中各类已存在的表达式，
// 用于更新校验时按表达式类别选择兼容性编译环境。
func findValidatingPolicyPreexistingExpressions(validatingPolicy *admissionregistration.ValidatingAdmissionPolicy) preexistingExpressions {
	preexisting := newPreexistingExpressions()
	for _, mc := range validatingPolicy.Spec.MatchConditions {
		preexisting.matchConditionExpressions.Insert(mc.Expression)
	}
	for _, v := range validatingPolicy.Spec.Validations {
		preexisting.validationExpressions.Insert(v.Expression)
		if len(v.MessageExpression) > 0 {
			preexisting.validationMessageExpressions.Insert(v.MessageExpression)
		}
	}
	for _, a := range validatingPolicy.Spec.AuditAnnotations {
		preexisting.auditAnnotationValuesExpressions.Insert(a.ValueExpression)
	}
	return preexisting
}

// findMutatingPolicyPreexistingExpressions 收集 MutatingAdmissionPolicy 中各类已存在的表达式，
// 包括 matchConditions、applyConfiguration 与 jsonPatch。
func findMutatingPolicyPreexistingExpressions(mutatingPolicy *admissionregistration.MutatingAdmissionPolicy) preexistingExpressions {
	preexisting := newPreexistingExpressions()
	for _, mc := range mutatingPolicy.Spec.MatchConditions {
		preexisting.matchConditionExpressions.Insert(mc.Expression)
	}
	for _, m := range mutatingPolicy.Spec.Mutations {
		if m.ApplyConfiguration != nil {
			preexisting.applyConfigurationExpressions.Insert(m.ApplyConfiguration.Expression)
		}
		if m.JSONPatch != nil {
			preexisting.jsonPatchExpressions.Insert(m.JSONPatch.Expression)
		}
	}
	return preexisting
}

// validateMutatingWebhookConfiguration 校验元数据及每个 mutating webhook；
// 同时检查同一配置内 webhook 名称是否重复。
func validateMutatingWebhookConfiguration(e *admissionregistration.MutatingWebhookConfiguration, opts validationOptions) field.ErrorList {
	allErrors := genericvalidation.ValidateObjectMeta(&e.ObjectMeta, false, genericvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))

	hookNames := sets.NewString()
	for i, hook := range e.Webhooks {
		allErrors = append(allErrors, validateMutatingWebhook(&hook, opts, field.NewPath("webhooks").Index(i))...)
		allErrors = append(allErrors, validateAdmissionReviewVersions(hook.AdmissionReviewVersions, opts.requireRecognizedAdmissionReviewVersion, field.NewPath("webhooks").Index(i).Child("admissionReviewVersions"))...)
		if opts.requireUniqueWebhookNames && len(hook.Name) > 0 {
			if hookNames.Has(hook.Name) {
				allErrors = append(allErrors, field.Duplicate(field.NewPath("webhooks").Index(i).Child("name"), hook.Name))
			} else {
				hookNames.Insert(hook.Name)
			}
		}
	}
	return allErrors
}

// validateValidatingWebhook 校验单个 validating webhook 的各字段及 clientConfig。
// 错误被聚合而非提前返回，以便一次请求反馈全部问题。
func validateValidatingWebhook(hook *admissionregistration.ValidatingWebhook, opts validationOptions, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	// webhook 名称必须是带 DNS 域名的完全限定名，便于与审计、指标等外部系统关联
	allErrors = append(allErrors, utilvalidation.IsFullyQualifiedName(fldPath.Child("name"), hook.Name)...)
	labelSelectorValidationOpts := metav1validation.LabelSelectorValidationOptions{
		AllowInvalidLabelValueInSelector: opts.allowInvalidLabelValueInSelector,
	}

	for i, rule := range hook.Rules {
		allErrors = append(allErrors, validateRuleWithOperations(&rule, fldPath.Child("rules").Index(i))...)
	}
	if hook.FailurePolicy != nil && !supportedFailurePolicies.Has(string(*hook.FailurePolicy)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("failurePolicy"), *hook.FailurePolicy, supportedFailurePolicies.List()))
	}
	if hook.MatchPolicy != nil && !supportedMatchPolicies.Has(string(*hook.MatchPolicy)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("matchPolicy"), *hook.MatchPolicy, supportedMatchPolicies.List()))
	}
	allowedSideEffects := supportedSideEffectClasses
	if opts.requireNoSideEffects {
		allowedSideEffects = noSideEffectClasses
	}
	if hook.SideEffects == nil {
		allErrors = append(allErrors, field.Required(fldPath.Child("sideEffects"), fmt.Sprintf("must specify one of %v", strings.Join(allowedSideEffects.List(), ", "))))
	}
	if hook.SideEffects != nil && !allowedSideEffects.Has(string(*hook.SideEffects)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("sideEffects"), *hook.SideEffects, allowedSideEffects.List()))
	}
	if hook.TimeoutSeconds != nil && (*hook.TimeoutSeconds > 30 || *hook.TimeoutSeconds < 1) {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("timeoutSeconds"), *hook.TimeoutSeconds, "the timeout value must be between 1 and 30 seconds"))
	}

	if hook.NamespaceSelector != nil {
		allErrors = append(allErrors, metav1validation.ValidateLabelSelector(hook.NamespaceSelector, labelSelectorValidationOpts, fldPath.Child("namespaceSelector"))...)
	}

	if hook.ObjectSelector != nil {
		allErrors = append(allErrors, metav1validation.ValidateLabelSelector(hook.ObjectSelector, labelSelectorValidationOpts, fldPath.Child("objectSelector"))...)
	}

	cc := hook.ClientConfig
	switch {
	case (cc.URL == nil) == (cc.Service == nil):
		allErrors = append(allErrors, field.Required(fldPath.Child("clientConfig"), "exactly one of url or service is required"))
	case cc.URL != nil:
		allErrors = append(allErrors, webhook.ValidateWebhookURL(fldPath.Child("clientConfig").Child("url"), *cc.URL, true)...)
	case cc.Service != nil:
		allErrors = append(allErrors, webhook.ValidateWebhookService(fldPath.Child("clientConfig").Child("service"), cc.Service.Name, cc.Service.Namespace, cc.Service.Path, cc.Service.Port)...)
	}

	if !opts.ignoreMatchConditions {
		allErrors = append(allErrors, validateMatchConditions(hook.MatchConditions, opts, fldPath.Child("matchConditions"))...)
	}

	return allErrors
}

// validateMutatingWebhook 校验单个 mutating webhook 的各字段及 clientConfig。
// 相比 validating webhook 额外校验 reinvocationPolicy。
func validateMutatingWebhook(hook *admissionregistration.MutatingWebhook, opts validationOptions, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	// webhook 名称必须是带 DNS 域名的完全限定名，便于与审计、指标等外部系统关联
	allErrors = append(allErrors, utilvalidation.IsFullyQualifiedName(fldPath.Child("name"), hook.Name)...)
	labelSelectorValidationOpts := metav1validation.LabelSelectorValidationOptions{
		AllowInvalidLabelValueInSelector: opts.allowInvalidLabelValueInSelector,
	}

	for i, rule := range hook.Rules {
		allErrors = append(allErrors, validateRuleWithOperations(&rule, fldPath.Child("rules").Index(i))...)
	}
	if hook.FailurePolicy != nil && !supportedFailurePolicies.Has(string(*hook.FailurePolicy)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("failurePolicy"), *hook.FailurePolicy, supportedFailurePolicies.List()))
	}
	if hook.MatchPolicy != nil && !supportedMatchPolicies.Has(string(*hook.MatchPolicy)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("matchPolicy"), *hook.MatchPolicy, supportedMatchPolicies.List()))
	}
	allowedSideEffects := supportedSideEffectClasses
	if opts.requireNoSideEffects {
		allowedSideEffects = noSideEffectClasses
	}
	if hook.SideEffects == nil {
		allErrors = append(allErrors, field.Required(fldPath.Child("sideEffects"), fmt.Sprintf("must specify one of %v", strings.Join(allowedSideEffects.List(), ", "))))
	}
	if hook.SideEffects != nil && !allowedSideEffects.Has(string(*hook.SideEffects)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("sideEffects"), *hook.SideEffects, allowedSideEffects.List()))
	}
	if hook.TimeoutSeconds != nil && (*hook.TimeoutSeconds > 30 || *hook.TimeoutSeconds < 1) {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("timeoutSeconds"), *hook.TimeoutSeconds, "the timeout value must be between 1 and 30 seconds"))
	}

	if hook.NamespaceSelector != nil {
		allErrors = append(allErrors, metav1validation.ValidateLabelSelector(hook.NamespaceSelector, labelSelectorValidationOpts, fldPath.Child("namespaceSelector"))...)
	}
	if hook.ObjectSelector != nil {
		allErrors = append(allErrors, metav1validation.ValidateLabelSelector(hook.ObjectSelector, labelSelectorValidationOpts, fldPath.Child("objectSelector"))...)
	}
	if hook.ReinvocationPolicy != nil && !supportedReinvocationPolicies.Has(string(*hook.ReinvocationPolicy)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("reinvocationPolicy"), *hook.ReinvocationPolicy, supportedReinvocationPolicies.List()))
	}

	cc := hook.ClientConfig
	switch {
	case (cc.URL == nil) == (cc.Service == nil):
		allErrors = append(allErrors, field.Required(fldPath.Child("clientConfig"), "exactly one of url or service is required"))
	case cc.URL != nil:
		allErrors = append(allErrors, webhook.ValidateWebhookURL(fldPath.Child("clientConfig").Child("url"), *cc.URL, true)...)
	case cc.Service != nil:
		allErrors = append(allErrors, webhook.ValidateWebhookService(fldPath.Child("clientConfig").Child("service"), cc.Service.Name, cc.Service.Namespace, cc.Service.Path, cc.Service.Port)...)
	}

	if !opts.ignoreMatchConditions {
		allErrors = append(allErrors, validateMatchConditions(hook.MatchConditions, opts, fldPath.Child("matchConditions"))...)
	}

	return allErrors
}

// supportedFailurePolicies 是 failurePolicy（webhook 调用失败时的行为）的合法取值。
var supportedFailurePolicies = sets.NewString(
	string(admissionregistration.Ignore),
	string(admissionregistration.Fail),
)

// supportedMatchPolicies 是 matchPolicy（请求如何被改写以匹配规则）的合法取值。
var supportedMatchPolicies = sets.NewString(
	string(admissionregistration.Exact),
	string(admissionregistration.Equivalent),
)

// supportedSideEffectClasses 是 sideEffects 的合法取值，描述 dry-run 之外的副作用级别。
var supportedSideEffectClasses = sets.NewString(
	string(admissionregistration.SideEffectClassUnknown),
	string(admissionregistration.SideEffectClassNone),
	string(admissionregistration.SideEffectClassSome),
	string(admissionregistration.SideEffectClassNoneOnDryRun),
)

// noSideEffectClasses 是无副作用的取值集合；更新时若旧对象已声明无副作用则继续强制该约束。
var noSideEffectClasses = sets.NewString(
	string(admissionregistration.SideEffectClassNone),
	string(admissionregistration.SideEffectClassNoneOnDryRun),
)

// supportedOperations 是 RuleWithOperations 中合法的操作类型。
var supportedOperations = sets.NewString(
	string(admissionregistration.OperationAll),
	string(admissionregistration.Create),
	string(admissionregistration.Update),
	string(admissionregistration.Delete),
	string(admissionregistration.Connect),
)

// supportedReinvocationPolicies 是 mutating webhook 重入策略的合法取值；
// 重入可让多个 webhook 依次修改同一请求，但可能带来无限循环风险，因此需要显式配置。
var supportedReinvocationPolicies = sets.NewString(
	string(admissionregistration.NeverReinvocationPolicy),
	string(admissionregistration.IfNeededReinvocationPolicy),
)

// supportedValidationPolicyReason 是 Validation.Reason 的合法取值，受 StatusReason 子集限制。
var supportedValidationPolicyReason = sets.NewString(
	string(metav1.StatusReasonForbidden),
	string(metav1.StatusReasonInvalid),
	string(metav1.StatusReasonRequestEntityTooLarge),
)

// supportedPatchType 是 Mutation.patchType 的合法取值，决定使用哪种补丁语义。
var supportedPatchType = sets.NewString(
	string(admissionregistration.PatchTypeApplyConfiguration),
	string(admissionregistration.PatchTypeJSONPatch),
)

// MutatingAdmissionPolicy 不支持 DELETE（补丁需要基于对象当前状态进行修改），因此不允许 DELETE 操作。
var supportedMutatingOperations = sets.NewString(
	string(admissionregistration.OperationAll),
	string(admissionregistration.Create),
	string(admissionregistration.Update),
	string(admissionregistration.Connect),
)

// hasWildcardOperation 判断操作列表中是否包含 "*"；通配符与具体取值不能并存。
func hasWildcardOperation(operations []admissionregistration.OperationType) bool {
	for _, o := range operations {
		if o == admissionregistration.OperationAll {
			return true
		}
	}
	return false
}

// validateRuleWithOperations 校验 operations 及内嵌的 Rule。
// 此处允许 resources 使用子资源；不支持子资源的场景由调用方另行把关。
func validateRuleWithOperations(ruleWithOperations *admissionregistration.RuleWithOperations, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	if len(ruleWithOperations.Operations) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("operations"), ""))
	}
	if len(ruleWithOperations.Operations) > 1 && hasWildcardOperation(ruleWithOperations.Operations) {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("operations"), ruleWithOperations.Operations, "if '*' is present, must not specify other operations"))
	}
	for i, operation := range ruleWithOperations.Operations {
		if !supportedOperations.Has(string(operation)) {
			allErrors = append(allErrors, field.NotSupported(fldPath.Child("operations").Index(i), operation, supportedOperations.List()))
		}
	}
	allowSubResource := true
	allErrors = append(allErrors, validateRule(&ruleWithOperations.Rule, fldPath, allowSubResource)...)
	return allErrors
}

// mutatingHasAcceptedAdmissionReviewVersions 判断所有 mutating webhook 是否都声明了至少一个
// 当前 apiserver 可接受的 AdmissionReview 版本；用于更新时决定是否放宽版本约束。
// 版本列表为空的旧对象视为无需强制（向后兼容）。
func mutatingHasAcceptedAdmissionReviewVersions(webhooks []admissionregistration.MutatingWebhook) bool {
	for _, hook := range webhooks {
		hasRecognizedVersion := false
		for _, version := range hook.AdmissionReviewVersions {
			if isAcceptedAdmissionReviewVersion(version) {
				hasRecognizedVersion = true
				break
			}
		}
		if !hasRecognizedVersion && len(hook.AdmissionReviewVersions) > 0 {
			return false
		}
	}
	return true
}

// validatingHasAcceptedAdmissionReviewVersions 判断所有 validating webhook 是否都声明了至少一个
// 当前 apiserver 可接受的 AdmissionReview 版本；用途同上。
func validatingHasAcceptedAdmissionReviewVersions(webhooks []admissionregistration.ValidatingWebhook) bool {
	for _, hook := range webhooks {
		hasRecognizedVersion := false
		for _, version := range hook.AdmissionReviewVersions {
			if isAcceptedAdmissionReviewVersion(version) {
				hasRecognizedVersion = true
				break
			}
		}
		if !hasRecognizedVersion && len(hook.AdmissionReviewVersions) > 0 {
			return false
		}
	}
	return true
}

// ignoreMutatingWebhookMatchConditions 判断 matchConditions 是否未发生变化，
// 未变化则更新时可跳过重新校验；数量变化也视为发生变化。
func ignoreMutatingWebhookMatchConditions(new, old []admissionregistration.MutatingWebhook) bool {
	if len(new) != len(old) {
		return false
	}
	for i := range old {
		if !reflect.DeepEqual(new[i].MatchConditions, old[i].MatchConditions) {
			return false
		}
	}

	return true
}

// ignoreValidatingWebhookMatchConditions 判断 matchConditions 是否未发生变化，
// 未变化则更新时可跳过重新校验，避免新版本编译器拒绝旧表达式。
func ignoreValidatingWebhookMatchConditions(new, old []admissionregistration.ValidatingWebhook) bool {
	if len(new) != len(old) {
		return false
	}
	for i := range old {
		if !reflect.DeepEqual(new[i].MatchConditions, old[i].MatchConditions) {
			return false
		}
	}

	return true
}

// ignoreValidatingAdmissionPolicyMatchConditions 判断是否存在会使原有 matchConditions 失效的变更。
// ParamKind 或 matchConditions 本身变化都可能影响 params 的类型信息，此时必须重新校验。
func ignoreValidatingAdmissionPolicyMatchConditions(new, old *admissionregistration.ValidatingAdmissionPolicy) bool {
	if !equality.Semantic.DeepEqual(new.Spec.ParamKind, old.Spec.ParamKind) {
		return false
	}
	if !equality.Semantic.DeepEqual(new.Spec.MatchConditions, old.Spec.MatchConditions) {
		return false
	}
	return true
}

// ignoreMutatingAdmissionPolicyMatchConditions 判断是否存在会使原有 matchConditions 失效的变更。
// ParamKind 或 matchConditions 本身变化都可能影响 params 的类型信息，此时必须重新校验。
func ignoreMutatingAdmissionPolicyMatchConditions(new, old *admissionregistration.MutatingAdmissionPolicy) bool {
	if !equality.Semantic.DeepEqual(new.Spec.ParamKind, old.Spec.ParamKind) {
		return false
	}
	if !equality.Semantic.DeepEqual(new.Spec.MatchConditions, old.Spec.MatchConditions) {
		return false
	}
	return true
}

// mutatingHasUniqueWebhookNames 判断所有 mutating webhook 名称是否唯一；
// 名称用于区分调用结果和审计记录，重复会导致语义混淆。
func mutatingHasUniqueWebhookNames(webhooks []admissionregistration.MutatingWebhook) bool {
	names := sets.NewString()
	for _, hook := range webhooks {
		if names.Has(hook.Name) {
			return false
		}
		names.Insert(hook.Name)
	}
	return true
}

// validatingHasUniqueWebhookNames 判断所有 validating webhook 名称是否唯一，理由同上。
func validatingHasUniqueWebhookNames(webhooks []admissionregistration.ValidatingWebhook) bool {
	names := sets.NewString()
	for _, hook := range webhooks {
		if names.Has(hook.Name) {
			return false
		}
		names.Insert(hook.Name)
	}
	return true
}

// mutatingHasNoSideEffects 判断所有 mutating webhook 是否都声明了无副作用；
// 决定更新时是否继续强制无副作用约束。
func mutatingHasNoSideEffects(webhooks []admissionregistration.MutatingWebhook) bool {
	for _, hook := range webhooks {
		if hook.SideEffects == nil || !noSideEffectClasses.Has(string(*hook.SideEffects)) {
			return false
		}
	}
	return true
}

// validatingHasNoSideEffects 判断所有 validating webhook 是否都声明了无副作用，用途同上。
func validatingHasNoSideEffects(webhooks []admissionregistration.ValidatingWebhook) bool {
	for _, hook := range webhooks {
		if hook.SideEffects == nil || !noSideEffectClasses.Has(string(*hook.SideEffects)) {
			return false
		}
	}
	return true
}

// validatingWebhookHasInvalidLabelValueInSelector 判断是否存在 selector 中含非法 label value 的 webhook。
// 历史遗留对象可能包含这类值，更新时需放宽校验以免升级后无法保存。
func validatingWebhookHasInvalidLabelValueInSelector(webhooks []admissionregistration.ValidatingWebhook) bool {
	labelSelectorValidationOpts := metav1validation.LabelSelectorValidationOptions{
		AllowInvalidLabelValueInSelector: false,
	}

	for _, hook := range webhooks {
		if hook.NamespaceSelector != nil {
			if len(metav1validation.ValidateLabelSelector(hook.NamespaceSelector, labelSelectorValidationOpts, nil)) > 0 {
				return true
			}
		}
		if hook.ObjectSelector != nil {
			if len(metav1validation.ValidateLabelSelector(hook.ObjectSelector, labelSelectorValidationOpts, nil)) > 0 {
				return true
			}
		}
	}
	return false
}

// mutatingWebhookHasInvalidLabelValueInSelector 判断是否存在 selector 中含非法 label value 的 mutating webhook，
// 用途同上。
func mutatingWebhookHasInvalidLabelValueInSelector(webhooks []admissionregistration.MutatingWebhook) bool {
	labelSelectorValidationOpts := metav1validation.LabelSelectorValidationOptions{
		AllowInvalidLabelValueInSelector: false,
	}

	for _, hook := range webhooks {
		if hook.NamespaceSelector != nil {
			if len(metav1validation.ValidateLabelSelector(hook.NamespaceSelector, labelSelectorValidationOpts, nil)) > 0 {
				return true
			}
		}
		if hook.ObjectSelector != nil {
			if len(metav1validation.ValidateLabelSelector(hook.ObjectSelector, labelSelectorValidationOpts, nil)) > 0 {
				return true
			}
		}
	}
	return false
}

// ValidateValidatingWebhookConfigurationUpdate 校验 ValidatingWebhookConfiguration 的更新。
// 根据旧对象放宽部分约束，使历史对象可继续更新而不被新规则拒绝。
func ValidateValidatingWebhookConfigurationUpdate(newC, oldC *admissionregistration.ValidatingWebhookConfiguration) field.ErrorList {
	return validateValidatingWebhookConfiguration(newC, validationOptions{
		ignoreMatchConditions:                   ignoreValidatingWebhookMatchConditions(newC.Webhooks, oldC.Webhooks),
		allowParamsInMatchConditions:            false,
		requireNoSideEffects:                    validatingHasNoSideEffects(oldC.Webhooks),
		requireRecognizedAdmissionReviewVersion: validatingHasAcceptedAdmissionReviewVersions(oldC.Webhooks),
		requireUniqueWebhookNames:               validatingHasUniqueWebhookNames(oldC.Webhooks),
		allowInvalidLabelValueInSelector:        validatingWebhookHasInvalidLabelValueInSelector(oldC.Webhooks),
		preexistingExpressions:                  findValidatingPreexistingExpressions(oldC),
	})
}

// ValidateMutatingWebhookConfigurationUpdate 校验 MutatingWebhookConfiguration 的更新，
// 放宽约束的逻辑与 validating 版本一致。
func ValidateMutatingWebhookConfigurationUpdate(newC, oldC *admissionregistration.MutatingWebhookConfiguration) field.ErrorList {
	return validateMutatingWebhookConfiguration(newC, validationOptions{
		ignoreMatchConditions:                   ignoreMutatingWebhookMatchConditions(newC.Webhooks, oldC.Webhooks),
		allowParamsInMatchConditions:            false,
		requireNoSideEffects:                    mutatingHasNoSideEffects(oldC.Webhooks),
		requireRecognizedAdmissionReviewVersion: mutatingHasAcceptedAdmissionReviewVersions(oldC.Webhooks),
		requireUniqueWebhookNames:               mutatingHasUniqueWebhookNames(oldC.Webhooks),
		allowInvalidLabelValueInSelector:        mutatingWebhookHasInvalidLabelValueInSelector(oldC.Webhooks),
		preexistingExpressions:                  findMutatingPreexistingExpressions(oldC),
	})
}

const (
	// 限制审计注解数量，避免单个策略产生过多注解、放大 API 响应与审计日志
	maxAuditAnnotations = 20
	// CEL 表达式长度上限为 5KB，小于审计值本身的 10KB 上限：
	// 拼接字符串的表达式通常会产生比表达式更长的运行结果，需要留出余量
	maxAuditAnnotationValueExpressionLength = 5 * 1024
)

// ValidateValidatingAdmissionPolicy 在创建前校验 ValidatingAdmissionPolicy，使用最严格的校验选项。
func ValidateValidatingAdmissionPolicy(p *admissionregistration.ValidatingAdmissionPolicy) field.ErrorList {
	return validateValidatingAdmissionPolicy(p, validationOptions{ignoreMatchConditions: false})
}

// validateValidatingAdmissionPolicy 校验元数据与 spec；所有字段错误统一聚合返回。
func validateValidatingAdmissionPolicy(p *admissionregistration.ValidatingAdmissionPolicy, opts validationOptions) field.ErrorList {
	allErrors := genericvalidation.ValidateObjectMeta(&p.ObjectMeta, false, genericvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrors = append(allErrors, validateValidatingAdmissionPolicySpec(p.ObjectMeta, &p.Spec, opts, field.NewPath("spec"))...)
	return allErrors
}

// validateValidatingAdmissionPolicySpec 校验策略的 failurePolicy、ParamKind、matchConstraints、
// matchConditions、variables、validations 与 auditAnnotations。
// 需要传入 meta 是因为审计注解的 key 必须带有策略名前缀。
func validateValidatingAdmissionPolicySpec(meta metav1.ObjectMeta, spec *admissionregistration.ValidatingAdmissionPolicySpec, opts validationOptions, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	var compiler plugincel.Compiler // 组合编译器有状态，每个策略按需创建一个，使变量可被后续表达式引用
	getCompiler := func() plugincel.Compiler {
		if compiler == nil {
			needsComposition := len(spec.Variables) > 0
			compiler = createCompiler(needsComposition)
		}
		return compiler
	}
	if spec.FailurePolicy == nil {
		allErrors = append(allErrors, field.Required(fldPath.Child("failurePolicy"), ""))
	} else if !supportedFailurePolicies.Has(string(*spec.FailurePolicy)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("failurePolicy"), *spec.FailurePolicy, supportedFailurePolicies.List()))
	}
	if spec.ParamKind != nil {
		opts.allowParamsInMatchConditions = true
		allErrors = append(allErrors, validateParamKind(*spec.ParamKind, fldPath.Child("paramKind"))...)
	}
	if spec.MatchConstraints == nil {
		allErrors = append(allErrors, field.Required(fldPath.Child("matchConstraints"), ""))
	} else {
		allErrors = append(allErrors, validateMatchResources(spec.MatchConstraints, fldPath.Child("matchConstraints"))...)
		// 必须至少有一个 resourceRule：CEL 环境需要 resource 类型信息才能完成类型检查
		if len(spec.MatchConstraints.ResourceRules) == 0 {
			allErrors = append(allErrors, field.Required(fldPath.Child("matchConstraints", "resourceRules"), ""))
		}
	}
	if !opts.ignoreMatchConditions {
		allErrors = append(allErrors, validateMatchConditions(spec.MatchConditions, opts, fldPath.Child("matchConditions"))...)
	}
	if len(spec.Variables) > 0 {
		for i, variable := range spec.Variables {
			allErrors = append(allErrors, validateVariable(getCompiler(), &variable, spec.ParamKind, opts, fldPath.Child("variables").Index(i))...)
		}
	}
	// validations 与 auditAnnotations 至少有一个：策略必须能产生某种效果，
	// 否则策略没有任何可观察行为，属于配置错误。两个字段都报错便于调用方任选其一修正。
	if len(spec.Validations) == 0 && len(spec.AuditAnnotations) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("validations"), "validations or auditAnnotations must contain at least one item"))
		allErrors = append(allErrors, field.Required(fldPath.Child("auditAnnotations"), "validations or auditAnnotations must contain at least one item"))
	} else {
		for i, validation := range spec.Validations {
			allErrors = append(allErrors, validateValidation(getCompiler(), &validation, spec.ParamKind, opts, fldPath.Child("validations").Index(i))...)
		}
		if spec.AuditAnnotations != nil {
			keys := sets.NewString()
			if len(spec.AuditAnnotations) > maxAuditAnnotations {
				allErrors = append(allErrors, field.Invalid(fldPath.Child("auditAnnotations"), spec.AuditAnnotations, fmt.Sprintf("must not have more than %d auditAnnotations", maxAuditAnnotations)))
			}
			for i, auditAnnotation := range spec.AuditAnnotations {
				allErrors = append(allErrors, validateAuditAnnotation(getCompiler(), meta, &auditAnnotation, spec.ParamKind, opts, fldPath.Child("auditAnnotations").Index(i))...)
				if keys.Has(auditAnnotation.Key) {
					allErrors = append(allErrors, field.Duplicate(fldPath.Child("auditAnnotations").Index(i).Child("key"), auditAnnotation.Key))
				}
				keys.Insert(auditAnnotation.Key)
			}
		}
	}
	return allErrors
}

// validateParamKind 校验 ParamKind 的 apiVersion 与 kind。
// 规则与 APIService 的 group/version 校验保持一致，避免因格式不同导致参数引用失败。
func validateParamKind(gvk admissionregistration.ParamKind, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	if len(gvk.APIVersion) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("apiVersion"), ""))
	} else if gv, err := parseGroupVersion(gvk.APIVersion); err != nil {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("apiVersion"), gvk.APIVersion, err.Error()))
	} else {
		// 与 APIService 的 group 字段校验保持一致
		if len(gv.Group) > 0 {
			if errs := utilvalidation.IsDNS1123Subdomain(gv.Group); len(errs) > 0 {
				allErrors = append(allErrors, field.Invalid(fldPath.Child("apiVersion"), gv.Group, strings.Join(errs, ",")))
			}
		}
		// 与 APIService 的 version 字段校验保持一致
		if len(gv.Version) == 0 {
			allErrors = append(allErrors, field.Invalid(fldPath.Child("apiVersion"), gvk.APIVersion, "version must be specified"))
		} else {
			if errs := utilvalidation.IsDNS1035Label(gv.Version); len(errs) > 0 {
				allErrors = append(allErrors, field.Invalid(fldPath.Child("apiVersion"), gv.Version, strings.Join(errs, ",")))
			}
		}
	}
	if len(gvk.Kind) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("kind"), ""))
	} else if errs := utilvalidation.IsDNS1035Label(strings.ToLower(gvk.Kind)); len(errs) > 0 {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("kind"), gvk.Kind, "may have mixed case, but should otherwise match: "+strings.Join(errs, ",")))
	}

	return allErrors
}

// groupVersion 是 "group/version" 字符串拆分后的结果。
type groupVersion struct {
	Group   string
	Version string
}

// parseGroupVersion 将 "group/version" 字符串拆分为 groupVersion。
// 空字符串或 "/" 表示核心组且版本为空；斜杠数量超过一个则视为格式错误。
func parseGroupVersion(gv string) (groupVersion, error) {
	if (len(gv) == 0) || (gv == "/") {
		return groupVersion{}, nil
	}

	switch strings.Count(gv, "/") {
	case 0:
		return groupVersion{"", gv}, nil
	case 1:
		i := strings.Index(gv, "/")
		return groupVersion{gv[:i], gv[i+1:]}, nil
	default:
		return groupVersion{}, fmt.Errorf("unexpected GroupVersion string: %v", gv)
	}
}

// validateMatchResources 校验 matchConstraints/matchResources 的 matchPolicy、两个 selector 及资源规则。
// 两个 selector 均必填：若省略，策略的匹配范围将不明确。
func validateMatchResources(mc *admissionregistration.MatchResources, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	if mc == nil {
		return allErrors
	}
	if mc.MatchPolicy == nil {
		allErrors = append(allErrors, field.Required(fldPath.Child("matchPolicy"), ""))
	} else if !supportedMatchPolicies.Has(string(*mc.MatchPolicy)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("matchPolicy"), *mc.MatchPolicy, supportedMatchPolicies.List()))
	}
	if mc.NamespaceSelector == nil {
		allErrors = append(allErrors, field.Required(fldPath.Child("namespaceSelector"), ""))
	} else {
		// 严格校验 selector：该类型发布时 issue #99139 已修复，无需放宽非法 label value
		allErrors = append(allErrors, metav1validation.ValidateLabelSelector(mc.NamespaceSelector, metav1validation.LabelSelectorValidationOptions{}, fldPath.Child("namespaceSelector"))...)
	}

	if mc.ObjectSelector == nil {
		allErrors = append(allErrors, field.Required(fldPath.Child("objectSelector"), ""))
	} else {
		// 严格校验 selector：该类型发布时 issue #99139 已修复，无需放宽非法 label value
		allErrors = append(allErrors, metav1validation.ValidateLabelSelector(mc.ObjectSelector, metav1validation.LabelSelectorValidationOptions{}, fldPath.Child("objectSelector"))...)
	}

	for i, namedRuleWithOperations := range mc.ResourceRules {
		allErrors = append(allErrors, validateNamedRuleWithOperations(&namedRuleWithOperations, fldPath.Child("resourceRules").Index(i))...)
	}

	for i, namedRuleWithOperations := range mc.ExcludeResourceRules {
		allErrors = append(allErrors, validateNamedRuleWithOperations(&namedRuleWithOperations, fldPath.Child("excludeResourceRules").Index(i))...)
	}
	return allErrors
}

// validValidationActions 是 validationActions 的合法取值：拒绝、告警、审计。
var validValidationActions = sets.NewString(
	string(admissionregistration.Deny),
	string(admissionregistration.Warn),
	string(admissionregistration.Audit),
)

// validateValidationActions 校验动作取值合法、不重复、非空，并禁止 Deny 与 Warn 同时出现。
// Deny 与 Warn 并用会在 API 响应和响应头中重复同一失败信息，没有意义。
func validateValidationActions(va []admissionregistration.ValidationAction, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	actions := sets.NewString()
	for i, action := range va {
		if !validValidationActions.Has(string(action)) {
			allErrors = append(allErrors, field.NotSupported(fldPath.Index(i), action, validValidationActions.List()))
		}
		if actions.Has(string(action)) {
			allErrors = append(allErrors, field.Duplicate(fldPath.Index(i), action))
		}
		actions.Insert(string(action))
	}
	if actions.Has(string(admissionregistration.Deny)) && actions.Has(string(admissionregistration.Warn)) {
		allErrors = append(allErrors, field.Invalid(fldPath, va, "must not contain both Deny and Warn (repeating the same validation failure information in the API response and headers serves no purpose)"))
	}
	if len(actions) == 0 {
		allErrors = append(allErrors, field.Required(fldPath, "at least one validation action is required").MarkCoveredByDeclarative())
	}
	return allErrors
}

// validateNamedRuleWithOperations 校验 resourceNames（须为合法路径段名称且不重复）及内嵌规则。
func validateNamedRuleWithOperations(n *admissionregistration.NamedRuleWithOperations, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	resourceNames := sets.NewString()
	for i, rName := range n.ResourceNames {
		for _, msg := range content.IsPathSegmentName(rName) {
			allErrors = append(allErrors, field.Invalid(fldPath.Child("resourceNames").Index(i), rName, msg))
		}
		if resourceNames.Has(rName) {
			allErrors = append(allErrors, field.Duplicate(fldPath.Child("resourceNames").Index(i), rName))
		} else {
			resourceNames.Insert(rName)
		}
	}
	allErrors = append(allErrors, validateRuleWithOperations(&n.RuleWithOperations, fldPath)...)
	return allErrors
}

// validateMatchConditions 校验 matchConditions 数量上限（64）、每项内容以及名称唯一性。
// 名称是表达式结果的引用标识，重复会导致引用歧义。
func validateMatchConditions(m []admissionregistration.MatchCondition, opts validationOptions, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	conditionNames := sets.NewString()
	if len(m) > 64 {
		allErrors = append(allErrors, field.TooMany(fldPath, len(m), 64))
	}
	for i, matchCondition := range m {
		allErrors = append(allErrors, validateMatchCondition(&matchCondition, opts, fldPath.Index(i))...)
		if len(matchCondition.Name) > 0 {
			if conditionNames.Has(matchCondition.Name) {
				allErrors = append(allErrors, field.Duplicate(fldPath.Index(i).Child("name"), matchCondition.Name))
			} else {
				conditionNames.Insert(matchCondition.Name)
			}
		}
	}
	return allErrors
}

// validateMatchCondition 校验单个 matchCondition：表达式非空、名称非空且为合法限定名。
func validateMatchCondition(v *admissionregistration.MatchCondition, opts validationOptions, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	trimmedExpression := strings.TrimSpace(v.Expression)
	if len(trimmedExpression) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("expression"), ""))
	} else {
		allErrors = append(allErrors, validateMatchConditionsExpression(trimmedExpression, opts, fldPath.Child("expression"))...)
	}
	if len(v.Name) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("name"), ""))
	} else {
		allErrors = append(allErrors, apivalidation.ValidateQualifiedName(v.Name, fldPath.Child("name"))...)
	}
	return allErrors
}

// validateVariable 校验变量名是否为合法 CEL 标识符（且非保留字），并编译其表达式。
// 变量依赖之前的变量，必须使用支持组合的编译器；旧表达式用 StoredExpressions 环境以保证兼容性。
func validateVariable(compiler plugincel.Compiler, v *admissionregistration.Variable, paramKind *admissionregistration.ParamKind, opts validationOptions, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	if len(v.Name) == 0 || strings.TrimSpace(v.Name) == "" {
		allErrors = append(allErrors, field.Required(fldPath.Child("name"), "name is not specified"))
	} else {
		if !isCELIdentifier(v.Name) {
			allErrors = append(allErrors, field.Invalid(fldPath.Child("name"), v.Name, "must be a valid CEL identifier"))
		}
	}
	if len(v.Expression) == 0 || strings.TrimSpace(v.Expression) == "" {
		allErrors = append(allErrors, field.Required(fldPath.Child("expression"), "expression is not specified"))
	} else {
		if compiler, ok := compiler.(*plugincel.CompositedCompiler); ok {
			envType := environment.NewExpressions
			if opts.preexistingExpressions.validationExpressions.Has(v.Expression) {
				envType = environment.StoredExpressions
			}
			variable := &validatingadmissionpolicy.Variable{
				Name:       v.Name,
				Expression: v.Expression,
			}
			result := compiler.CompileAndStoreVariable(variable, plugincel.OptionalVariableDeclarations{
				HasParams:     paramKind != nil,
				HasAuthorizer: true,
			}, envType)
			if result.Error != nil {
				allErrors = append(allErrors, convertCELErrorToValidationError(fldPath.Child("expression"), variable, result.Error))
			}
		} else {
			allErrors = append(allErrors, field.InternalError(fldPath, fmt.Errorf("variable composition is not allowed")))
		}
	}
	return allErrors
}

// validateValidation 校验 Validation 的表达式、message/messageExpression、reason。
// message 与 messageExpression 均可选，但一旦填写就不能为空白。
func validateValidation(compiler plugincel.Compiler, v *admissionregistration.Validation, paramKind *admissionregistration.ParamKind, opts validationOptions, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	trimmedExpression := strings.TrimSpace(v.Expression)
	trimmedMsg := strings.TrimSpace(v.Message)
	trimmedMessageExpression := strings.TrimSpace(v.MessageExpression)
	if len(trimmedExpression) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("expression"), "expression is not specified"))
	} else {
		allErrors = append(allErrors, validateValidationExpression(compiler, v.Expression, paramKind != nil, opts, fldPath.Child("expression"))...)
	}
	if len(v.MessageExpression) > 0 && len(trimmedMessageExpression) == 0 {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("messageExpression"), v.MessageExpression, "must be non-empty if specified"))
	} else if len(trimmedMessageExpression) != 0 {
		// 使用未裁剪的原表达式，确保编译器报告的错误列号与用户书写位置一致
		allErrors = append(allErrors, validateMessageExpression(compiler, v.MessageExpression, opts, fldPath.Child("messageExpression"))...)
	}
	if len(v.Message) > 0 && len(trimmedMsg) == 0 {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("message"), v.Message, "must be non-empty if specified"))
	} else if hasNewlines(trimmedMsg) {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("message"), v.Message, "must not contain line breaks"))
	} else if hasNewlines(trimmedMsg) && trimmedMsg == "" {
		allErrors = append(allErrors, field.Required(fldPath.Child("message"), "message must be specified if expression contains line breaks"))
	}
	if v.Reason != nil && !supportedValidationPolicyReason.Has(string(*v.Reason)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("reason"), *v.Reason, supportedValidationPolicyReason.List()))
	}
	return allErrors
}

// validateCELCondition 调用编译器编译表达式，并将编译错误转换为 field.Error。
// 表达式的错误聚合交给调用方完成；此处只处理单个表达式。
func validateCELCondition(compiler plugincel.Compiler, expression plugincel.ExpressionAccessor, variables plugincel.OptionalVariableDeclarations, envType environment.Type, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	result := compiler.CompileCELExpression(expression, variables, envType)
	if result.Error != nil {
		allErrors = append(allErrors, convertCELErrorToValidationError(fldPath, expression, result.Error))
	}
	return allErrors
}

// convertCELErrorToValidationError 将 CEL 编译错误按类型映射为必填、非法或内部错误。
// 未知类型按内部错误上报，避免吞掉异常。
func convertCELErrorToValidationError(fldPath *field.Path, expression plugincel.ExpressionAccessor, err error) *field.Error {
	if celErr, ok := err.(*cel.Error); ok {
		switch celErr.Type {
		case cel.ErrorTypeRequired:
			return field.Required(fldPath, celErr.Detail)
		case cel.ErrorTypeInvalid:
			return field.Invalid(fldPath, expression.GetExpression(), celErr.Detail)
		case cel.ErrorTypeInternal:
			return field.InternalError(fldPath, celErr)
		}
	}
	return field.InternalError(fldPath, fmt.Errorf("unsupported error type: %w", err))
}

// validateValidationExpression 编译校验表达式；已有表达式使用 StoredExpressions 环境避免兼容性回退，
// 新表达式使用 NewExpressions 环境以享受最新类型检查与成本限制。
func validateValidationExpression(compiler plugincel.Compiler, expression string, hasParams bool, opts validationOptions, fldPath *field.Path) field.ErrorList {
	envType := environment.NewExpressions
	if opts.preexistingExpressions.validationExpressions.Has(expression) {
		envType = environment.StoredExpressions
	}
	return validateCELCondition(compiler, &validatingadmissionpolicy.ValidationCondition{
		Expression: expression,
	}, plugincel.OptionalVariableDeclarations{
		HasParams:     hasParams,
		HasAuthorizer: true,
	}, envType, fldPath)
}

// validateMatchConditionsExpression 使用无状态编译器校验 matchConditions 表达式：
// 该上下文不允许引用 variables，因此不需要（也不应）具备变量组合能力。
func validateMatchConditionsExpression(expression string, opts validationOptions, fldPath *field.Path) field.ErrorList {
	envType := environment.NewExpressions
	if opts.preexistingExpressions.matchConditionExpressions.Has(expression) {
		envType = environment.StoredExpressions
	}
	var compiler plugincel.Compiler
	compiler = getStrictStatelessCELCompiler()
	return validateCELCondition(compiler, &matchconditions.MatchCondition{
		Expression: expression,
	}, plugincel.OptionalVariableDeclarations{
		HasParams:     opts.allowParamsInMatchConditions,
		HasAuthorizer: true,
	}, envType, fldPath)
}

// validateMessageExpression 编译校验失败时用于生成自定义消息的 CEL 表达式。
// 该表达式无需 authorizer 变量，因为消息生成不应触发授权检查。
func validateMessageExpression(compiler plugincel.Compiler, expression string, opts validationOptions, fldPath *field.Path) field.ErrorList {
	envType := environment.NewExpressions
	if opts.preexistingExpressions.validationMessageExpressions.Has(expression) {
		envType = environment.StoredExpressions
	}
	return validateCELCondition(compiler, &validatingadmissionpolicy.MessageExpressionCondition{
		MessageExpression: expression,
	}, plugincel.OptionalVariableDeclarations{
		HasParams:     opts.allowParamsInMatchConditions,
		HasAuthorizer: false,
	}, envType, fldPath)
}

// validateAuditAnnotation 校验审计注解的 key（需带策略名前缀）、valueExpression 非空且长度受限，并编译表达式。
// 长度限制避免表达式生成远超预期的审计值。
func validateAuditAnnotation(compiler plugincel.Compiler, meta metav1.ObjectMeta, v *admissionregistration.AuditAnnotation, paramKind *admissionregistration.ParamKind, opts validationOptions, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	if len(meta.GetName()) != 0 {
		name := meta.GetName()
		allErrors = append(allErrors, apivalidation.ValidateQualifiedName(name+"/"+v.Key, fldPath.Child("key"))...)
	} else {
		allErrors = append(allErrors, field.Invalid(fldPath.Child("key"), v.Key, "requires metadata.name be non-empty"))
	}

	trimmedValueExpression := strings.TrimSpace(v.ValueExpression)
	if len(trimmedValueExpression) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("valueExpression"), "valueExpression is not specified"))
	} else if len(trimmedValueExpression) > maxAuditAnnotationValueExpressionLength {
		allErrors = append(allErrors, field.Required(fldPath.Child("valueExpression"), fmt.Sprintf("must not exceed %d bytes in length", maxAuditAnnotationValueExpressionLength)))
	} else {
		envType := environment.NewExpressions
		if opts.preexistingExpressions.auditAnnotationValuesExpressions.Has(v.ValueExpression) {
			envType = environment.StoredExpressions
		}
		result := compiler.CompileCELExpression(&validatingadmissionpolicy.AuditAnnotationCondition{
			ValueExpression: trimmedValueExpression,
		}, plugincel.OptionalVariableDeclarations{HasParams: paramKind != nil, HasAuthorizer: true}, envType)
		if result.Error != nil {
			switch result.Error.Type {
			case cel.ErrorTypeRequired:
				allErrors = append(allErrors, field.Required(fldPath.Child("valueExpression"), result.Error.Detail))
			case cel.ErrorTypeInvalid:
				allErrors = append(allErrors, field.Invalid(fldPath.Child("valueExpression"), v.ValueExpression, result.Error.Detail))
			default:
				allErrors = append(allErrors, field.InternalError(fldPath.Child("valueExpression"), result.Error))
			}
		}
	}
	return allErrors
}

// CEL 语法认可的换行字符；用于禁止 message 中出现换行。
var newlineMatcher = regexp.MustCompile(`[\n\r]+`)

// hasNewlines 判断字符串是否包含换行符（message 不允许换行，以免破坏单行展示）。
func hasNewlines(s string) bool {
	return newlineMatcher.MatchString(s)
}

// ValidateValidatingAdmissionPolicyBinding 在创建前校验 ValidatingAdmissionPolicyBinding。
func ValidateValidatingAdmissionPolicyBinding(pb *admissionregistration.ValidatingAdmissionPolicyBinding) field.ErrorList {
	return validateValidatingAdmissionPolicyBinding(pb)
}

// validateValidatingAdmissionPolicyBinding 校验元数据与 spec；
// 所有字段错误累积返回，便于一次请求看到全部问题。
func validateValidatingAdmissionPolicyBinding(pb *admissionregistration.ValidatingAdmissionPolicyBinding) field.ErrorList {
	allErrors := genericvalidation.ValidateObjectMeta(&pb.ObjectMeta, false, genericvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrors = append(allErrors, validateValidatingAdmissionPolicyBindingSpec(&pb.Spec, field.NewPath("spec"))...)

	return allErrors
}

// validateValidatingAdmissionPolicyBindingSpec 校验 policyName、paramRef、matchResources 与 validationActions。
func validateValidatingAdmissionPolicyBindingSpec(spec *admissionregistration.ValidatingAdmissionPolicyBindingSpec, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList

	if len(spec.PolicyName) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("policyName"), "").MarkCoveredByDeclarative())
	} else {
		for _, msg := range genericvalidation.NameIsDNSSubdomain(spec.PolicyName, false) {
			allErrors = append(allErrors, field.Invalid(fldPath.Child("policyName"), spec.PolicyName, msg))
		}
	}
	allErrors = append(allErrors, validateParamRef(spec.ParamRef, fldPath.Child("paramRef"))...)
	allErrors = append(allErrors, validateMatchResources(spec.MatchResources, fldPath.Child("matchResources"))...)
	allErrors = append(allErrors, validateValidationActions(spec.ValidationActions, fldPath.Child("validationActions"))...)

	return allErrors
}

// validateParamRef 校验参数引用：name 与 selector 互斥且必须提供其一，
// parameterNotFoundAction 必填且只能为 Deny 或 Allow；未指定动作会让参数缺失时的行为不确定。
func validateParamRef(pr *admissionregistration.ParamRef, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	if pr == nil {
		return allErrors
	}

	if len(pr.Name) > 0 {
		for _, msg := range content.IsPathSegmentName(pr.Name) {
			allErrors = append(allErrors, field.Invalid(fldPath.Child("name"), pr.Name, msg))
		}

		if pr.Selector != nil {
			allErrors = append(allErrors, field.Forbidden(fldPath.Child("name"), `name and selector are mutually exclusive`))
		}
	}

	if pr.Selector != nil {
		labelSelectorValidationOpts := metav1validation.LabelSelectorValidationOptions{}
		allErrors = append(allErrors, metav1validation.ValidateLabelSelector(pr.Selector, labelSelectorValidationOpts, fldPath.Child("selector"))...)

		if len(pr.Name) > 0 {
			allErrors = append(allErrors, field.Forbidden(fldPath.Child("selector"), `name and selector are mutually exclusive`))
		}
	}

	if len(pr.Name) == 0 && pr.Selector == nil {
		allErrors = append(allErrors, field.Required(fldPath, `one of name or selector must be specified`))
	}

	if pr.ParameterNotFoundAction == nil || len(*pr.ParameterNotFoundAction) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("parameterNotFoundAction"), ""))
	} else {
		if *pr.ParameterNotFoundAction != admissionregistration.DenyAction && *pr.ParameterNotFoundAction != admissionregistration.AllowAction {
			allErrors = append(allErrors, field.NotSupported(fldPath.Child("parameterNotFoundAction"), pr.ParameterNotFoundAction, []string{string(admissionregistration.DenyAction), string(admissionregistration.AllowAction)}))
		}
	}

	return allErrors
}

// ValidateValidatingAdmissionPolicyUpdate 校验 ValidatingAdmissionPolicy 的更新。
// 依据旧对象放宽 matchConditions 校验并使用兼容性编译环境，避免旧表达式被新规则拒绝。
func ValidateValidatingAdmissionPolicyUpdate(newC, oldC *admissionregistration.ValidatingAdmissionPolicy) field.ErrorList {
	return validateValidatingAdmissionPolicy(newC, validationOptions{
		ignoreMatchConditions:  ignoreValidatingAdmissionPolicyMatchConditions(newC, oldC),
		preexistingExpressions: findValidatingPolicyPreexistingExpressions(oldC),
	})
}

// ValidateValidatingAdmissionPolicyStatusUpdate 校验策略 status 的更新；
// status 由控制器写入，此处校验类型检查结果与条件列表的格式。
func ValidateValidatingAdmissionPolicyStatusUpdate(newC, oldC *admissionregistration.ValidatingAdmissionPolicy) field.ErrorList {
	return validateValidatingAdmissionPolicyStatus(&newC.Status, field.NewPath("status"))
}

// ValidateValidatingAdmissionPolicyBindingUpdate 校验 ValidatingAdmissionPolicyBinding 的更新，
// 规则与创建时一致。
func ValidateValidatingAdmissionPolicyBindingUpdate(newC, oldC *admissionregistration.ValidatingAdmissionPolicyBinding) field.ErrorList {
	return validateValidatingAdmissionPolicyBinding(newC)
}

// validateValidatingAdmissionPolicyStatus 校验 status 中的 typeChecking 信息及通用 conditions。
func validateValidatingAdmissionPolicyStatus(status *admissionregistration.ValidatingAdmissionPolicyStatus, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	allErrors = append(allErrors, validateTypeChecking(status.TypeChecking, fldPath.Child("typeChecking"))...)
	allErrors = append(allErrors, metav1validation.ValidateConditions(status.Conditions, fldPath.Child("conditions"))...)
	return allErrors
}

// validateTypeChecking 校验类型检查结果；typeChecking 为 nil 时表示尚未计算，直接放行。
func validateTypeChecking(typeChecking *admissionregistration.TypeChecking, fldPath *field.Path) field.ErrorList {
	if typeChecking == nil {
		return nil
	}
	return validateExpressionWarnings(typeChecking.ExpressionWarnings, fldPath.Child("expressionWarnings"))
}

// validateExpressionWarnings 逐项校验表达式警告列表。
func validateExpressionWarnings(expressionWarnings []admissionregistration.ExpressionWarning, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	for i, warning := range expressionWarnings {
		allErrors = append(allErrors, validateExpressionWarning(&warning, fldPath.Index(i))...)
	}
	return allErrors
}

// validateExpressionWarning 校验警告文本非空，且 fieldRef 指向合法的字段路径。
func validateExpressionWarning(expressionWarning *admissionregistration.ExpressionWarning, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	if expressionWarning.Warning == "" {
		allErrors = append(allErrors, field.Required(fldPath.Child("warning"), ""))
	}
	allErrors = append(allErrors, validateFieldRef(expressionWarning.FieldRef, fldPath.Child("fieldRef"))...)
	return allErrors
}

// validateFieldRef 校验字段引用是否为可解析的 JSONPath。
// 只做语法解析不再做语义限制，方便未来新增字段时无需同步修改此处，保证升级/回滚顺畅。
func validateFieldRef(fieldRef string, fldPath *field.Path) field.ErrorList {
	fieldRef = strings.TrimSpace(fieldRef)
	if fieldRef == "" {
		return field.ErrorList{field.Required(fldPath, "")}
	}
	jsonPath := jsonpath.New("spec")
	if err := jsonPath.Parse(fmt.Sprintf("{%s}", fieldRef)); err != nil {
		return field.ErrorList{field.Invalid(fldPath, fieldRef, fmt.Sprintf("invalid JSONPath: %v", err))}
	}
	// 不再做进一步检查，以便新字段加入时无需同步放宽校验，保证升级/回滚顺畅
	return nil
}

// 无状态 CEL 编译器不支持变量组合，适用于不允许引用变量的场景（如 MatchConditions）。
// 此处使用启用严格成本限制的编译器，防止表达式消耗过多资源。
// 编译器懒加载并全局复用，避免每次校验重复创建环境。
var (
	lazyStrictStatelessCELCompilerInit sync.Once
	lazyStrictStatelessCELCompiler     plugincel.Compiler
)

// getStrictStatelessCELCompiler 返回全局共享的严格成本限制无状态编译器，使用 sync.Once 懒加载。
func getStrictStatelessCELCompiler() plugincel.Compiler {
	lazyStrictStatelessCELCompilerInit.Do(func() {
		lazyStrictStatelessCELCompiler = plugincel.NewCompiler(environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()))
	})
	return lazyStrictStatelessCELCompiler
}

// createCompiler 创建 CEL 编译器：需要变量组合时使用有状态的 CompositedCompiler，
// 否则返回无状态编译器。组合编译器创建失败时记录错误并退回无状态编译器，避免 panic。
func createCompiler(allowComposition bool) plugincel.Compiler {
	if !allowComposition {
		return getStrictStatelessCELCompiler()
	}
	compiler, err := plugincel.NewCompositedCompiler(environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()))
	if err != nil {
		// 理论上不应发生，但校验路径不能 panic，记录错误后降级处理
		utilruntime.HandleError(err)
		return plugincel.NewCompiler(environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()))
	}
	return compiler
}

// CEL 标识符的正则与保留字集合；保留字不能作为变量名，否则表达式语法会产生歧义。
var celIdentRegex = regexp.MustCompile("^[_a-zA-Z][_a-zA-Z0-9]*$")
var celReserved = sets.NewString("true", "false", "null", "in",
	"as", "break", "const", "continue", "else",
	"for", "function", "if", "import", "let",
	"loop", "package", "namespace", "return",
	"var", "void", "while")

// isCELIdentifier 判断 name 是否是合法 CEL 标识符：匹配正则且不在保留字集合中。
func isCELIdentifier(name string) bool {
	// 以下规则摘自 CEL 语法：标识符不能与布尔字面量、null、in 等保留字冲突
	// BOOL_LIT       ::= "true" | "false"
	// NULL_LIT       ::= "null"
	// RESERVED       ::= BOOL_LIT | NULL_LIT | "in"
	// 	 | "as" | "break" | "const" | "continue" | "else"
	// 	 | "for" | "function" | "if" | "import" | "let"
	// 	 | "loop" | "package" | "namespace" | "return"
	// 	 | "var" | "void" | "while"
	return celIdentRegex.MatchString(name) && !celReserved.Has(name)
}

// ValidateMutatingAdmissionPolicyUpdate 校验 MutatingAdmissionPolicy 的更新；
// 依据旧对象放宽 matchConditions 校验，并识别已有的补丁表达式。
func ValidateMutatingAdmissionPolicyUpdate(newC, oldC *admissionregistration.MutatingAdmissionPolicy) field.ErrorList {
	return validateMutatingAdmissionPolicy(newC, validationOptions{
		ignoreMatchConditions:  ignoreMutatingAdmissionPolicyMatchConditions(newC, oldC),
		preexistingExpressions: findMutatingPolicyPreexistingExpressions(oldC),
	})
}

// ValidateMutatingAdmissionPolicyBindingUpdate 校验 MutatingAdmissionPolicyBinding 的更新，
// 规则与创建时一致。
func ValidateMutatingAdmissionPolicyBindingUpdate(newC, oldC *admissionregistration.MutatingAdmissionPolicyBinding) field.ErrorList {
	return validateMutatingAdmissionPolicyBinding(newC)
}

// ValidateMutatingAdmissionPolicy 在创建前校验 MutatingAdmissionPolicy，使用最严格的校验选项。
func ValidateMutatingAdmissionPolicy(p *admissionregistration.MutatingAdmissionPolicy) field.ErrorList {
	return validateMutatingAdmissionPolicy(p, validationOptions{ignoreMatchConditions: false})
}

// validateMutatingAdmissionPolicy 校验元数据与 spec；所有字段错误统一聚合返回。
func validateMutatingAdmissionPolicy(p *admissionregistration.MutatingAdmissionPolicy, opts validationOptions) field.ErrorList {
	allErrors := genericvalidation.ValidateObjectMeta(&p.ObjectMeta, false, genericvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrors = append(allErrors, validateMutatingAdmissionPolicySpec(p.ObjectMeta, &p.Spec, opts, field.NewPath("spec"))...)
	return allErrors
}

// validateMutatingAdmissionPolicySpec 校验 failurePolicy、ParamKind、matchConstraints、
// matchConditions、variables、mutations 与 reinvocationPolicy。
// 与 validating 策略不同，补丁表达式需要组合编译器以支持变量引用。
func validateMutatingAdmissionPolicySpec(meta metav1.ObjectMeta, spec *admissionregistration.MutatingAdmissionPolicySpec, opts validationOptions, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList

	compiler := createCompiler(true)

	if spec.FailurePolicy == nil {
		allErrors = append(allErrors, field.Required(fldPath.Child("failurePolicy"), ""))
	} else if !supportedFailurePolicies.Has(string(*spec.FailurePolicy)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("failurePolicy"), *spec.FailurePolicy, supportedFailurePolicies.List()))
	}
	if spec.ParamKind != nil {
		opts.allowParamsInMatchConditions = true
		allErrors = append(allErrors, validateParamKind(*spec.ParamKind, fldPath.Child("paramKind"))...)
	}
	if spec.MatchConstraints == nil {
		allErrors = append(allErrors, field.Required(fldPath.Child("matchConstraints"), ""))
	} else {
		allErrors = append(allErrors, validateMatchResources(spec.MatchConstraints, fldPath.Child("matchConstraints"))...)
		// 必须至少有一个 resourceRule：CEL 环境需要 resource 类型信息才能完成类型检查
		if len(spec.MatchConstraints.ResourceRules) == 0 {
			allErrors = append(allErrors, field.Required(fldPath.Child("matchConstraints", "resourceRules"), ""))
		}

		// 只对 CONNECT/CREATE/UPDATE 请求做变更才有意义：DELETE 请求的对象即将被移除，无法在其上应用补丁
		for _, rule := range spec.MatchConstraints.ResourceRules {
			for _, op := range rule.RuleWithOperations.Operations {
				if !supportedMutatingOperations.Has(string(op)) {
					allErrors = append(allErrors, field.NotSupported(fldPath.Child("matchConstraints", "resourceRules", "operations"), op, supportedMutatingOperations.List()))
				}
			}
		}
	}
	if !opts.ignoreMatchConditions {
		allErrors = append(allErrors, validateMatchConditions(spec.MatchConditions, opts, fldPath.Child("matchConditions"))...)
	}
	for i, variable := range spec.Variables {
		allErrors = append(allErrors, validateVariable(compiler, &variable, spec.ParamKind, opts, fldPath.Child("variables").Index(i))...)
	}
	if len(spec.Mutations) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("mutations"), "mutations must contain at least one item"))
	} else {
		for i, mutation := range spec.Mutations {
			allErrors = append(allErrors, validateMutation(compiler, &mutation, spec.ParamKind, opts, fldPath.Child("mutations").Index(i))...)
		}
	}
	if len(spec.ReinvocationPolicy) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("reinvocationPolicy"), ""))
	} else if !supportedReinvocationPolicies.Has(string(spec.ReinvocationPolicy)) {
		allErrors = append(allErrors, field.NotSupported(fldPath.Child("reinvocationPolicy"), spec.ReinvocationPolicy, supportedReinvocationPolicies.List()))
	}
	return allErrors
}

// validateMutation 校验 patchType 与对应补丁字段的搭配。
// JSONPatch 与 ApplyConfiguration 互斥：同时指定会使执行顺序和最终对象状态不确定。
func validateMutation(compiler plugincel.Compiler, m *admissionregistration.Mutation, paramKind *admissionregistration.ParamKind, opts validationOptions, fldPath *field.Path) (allErrors field.ErrorList) {
	if len(m.PatchType) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("patchType"), ""))
	} else {
		switch m.PatchType {
		case admissionregistration.PatchTypeJSONPatch:
			if m.JSONPatch == nil {
				allErrors = append(allErrors, field.Required(fldPath.Child("jsonPatch"), "must be specified when patchType is JSONPatch"))
			} else {
				allErrors = append(allErrors, validateJSONPatch(compiler, m.JSONPatch, paramKind, opts, fldPath.Child("jsonPatch"))...)
			}
			if m.ApplyConfiguration != nil {
				allErrors = append(allErrors, field.Invalid(fldPath.Child("applyConfiguration"), "{applyConfiguration}", "must not be specified when patchType is JSONPatch"))
			}
		case admissionregistration.PatchTypeApplyConfiguration:
			if m.ApplyConfiguration == nil {
				allErrors = append(allErrors, field.Required(fldPath.Child("applyConfiguration"), "must be specified when patchType is ApplyConfiguration"))
			} else {
				allErrors = append(allErrors, validateApplyConfiguration(compiler, m.ApplyConfiguration, paramKind, opts, fldPath.Child("applyConfiguration"))...)
			}
			if m.JSONPatch != nil {
				allErrors = append(allErrors, field.Invalid(fldPath.Child("jsonPatch"), "{jsonPatch}", "must not be specified when patchType is ApplyConfiguration"))
			}
		default:
			allErrors = append(allErrors, field.NotSupported(fldPath.Child("patchType"), m.PatchType, supportedPatchType.List()))
		}
	}
	return allErrors
}

// validateApplyConfiguration 编译校验 ApplyConfiguration 表达式。
// HasPatchTypes 为 true 使表达式可引用 patch 类型信息；已有表达式使用兼容性环境。
func validateApplyConfiguration(compiler plugincel.Compiler, applyConfig *admissionregistration.ApplyConfiguration, paramKind *admissionregistration.ParamKind, opts validationOptions, fldPath *field.Path) (allErrors field.ErrorList) {
	trimmedExpression := strings.TrimSpace(applyConfig.Expression)
	if len(trimmedExpression) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("expression"), ""))
	} else {

		envType := environment.NewExpressions
		if opts.preexistingExpressions.applyConfigurationExpressions.Has(applyConfig.Expression) {
			envType = environment.StoredExpressions
		}
		accessor := &patch.ApplyConfigurationCondition{
			Expression: trimmedExpression,
		}
		opts := plugincel.OptionalVariableDeclarations{HasParams: paramKind != nil, HasAuthorizer: true, HasPatchTypes: true}
		result := compiler.CompileCELExpression(accessor, opts, envType)

		if result.Error != nil {
			allErrors = append(allErrors, convertCELErrorToValidationError(fldPath.Child("expression"), accessor, result.Error))
		}
	}
	return allErrors
}

// validateJSONPatch 编译校验 JSONPatch 表达式；与 applyConfiguration 类似开启 patch 类型信息。
func validateJSONPatch(compiler plugincel.Compiler, jsonPatch *admissionregistration.JSONPatch, paramKind *admissionregistration.ParamKind, opts validationOptions, fldPath *field.Path) (allErrors field.ErrorList) {
	trimmedExpression := strings.TrimSpace(jsonPatch.Expression)
	if len(trimmedExpression) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("expression"), ""))
	} else {

		envType := environment.NewExpressions
		if opts.preexistingExpressions.applyConfigurationExpressions.Has(jsonPatch.Expression) {
			envType = environment.StoredExpressions
		}
		accessor := &patch.JSONPatchCondition{
			Expression: trimmedExpression,
		}
		opts := plugincel.OptionalVariableDeclarations{HasParams: paramKind != nil, HasAuthorizer: true, HasPatchTypes: true}
		result := compiler.CompileCELExpression(accessor, opts, envType)

		if result.Error != nil {
			allErrors = append(allErrors, convertCELErrorToValidationError(fldPath.Child("expression"), accessor, result.Error))
		}
	}
	return allErrors
}

// ValidateMutatingAdmissionPolicyBinding 在创建前校验 MutatingAdmissionPolicyBinding。
func ValidateMutatingAdmissionPolicyBinding(pb *admissionregistration.MutatingAdmissionPolicyBinding) field.ErrorList {
	return validateMutatingAdmissionPolicyBinding(pb)
}

// validateMutatingAdmissionPolicyBinding 校验元数据与 spec；所有字段错误累积返回。
func validateMutatingAdmissionPolicyBinding(pb *admissionregistration.MutatingAdmissionPolicyBinding) field.ErrorList {
	allErrors := genericvalidation.ValidateObjectMeta(&pb.ObjectMeta, false, genericvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrors = append(allErrors, validateMutatingAdmissionPolicyBindingSpec(&pb.Spec, field.NewPath("spec"))...)

	return allErrors
}

// validateMutatingAdmissionPolicyBindingSpec 校验 policyName、paramRef、matchResources，
// 并额外禁止 DELETE 操作（只有 create/update 请求才能被修改）。
func validateMutatingAdmissionPolicyBindingSpec(spec *admissionregistration.MutatingAdmissionPolicyBindingSpec, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList

	if len(spec.PolicyName) == 0 {
		allErrors = append(allErrors, field.Required(fldPath.Child("policyName"), ""))
	} else {
		for _, msg := range genericvalidation.NameIsDNSSubdomain(spec.PolicyName, false) {
			allErrors = append(allErrors, field.Invalid(fldPath.Child("policyName"), spec.PolicyName, msg))
		}
	}
	allErrors = append(allErrors, validateParamRef(spec.ParamRef, fldPath.Child("paramRef"))...)
	allErrors = append(allErrors, validateMatchResources(spec.MatchResources, fldPath.Child("matchResources"))...)
	if spec.MatchResources != nil {
		// 只对 CONNECT/CREATE/UPDATE 请求做变更才有意义：DELETE 请求的对象即将被移除，无法在其上应用补丁
		for _, rule := range spec.MatchResources.ResourceRules {
			for _, op := range rule.RuleWithOperations.Operations {
				if !supportedMutatingOperations.Has(string(op)) {
					allErrors = append(allErrors, field.NotSupported(fldPath.Child("matchResources", "resourceRules", "operations"), op, supportedMutatingOperations.List()))
				}
			}
		}
	}

	return allErrors
}
