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

package validation

import (
	"fmt"
	"strings"

	flowcontrolv1beta1 "k8s.io/api/flowcontrol/v1beta1"
	flowcontrolv1beta2 "k8s.io/api/flowcontrol/v1beta2"
	flowcontrolv1beta3 "k8s.io/api/flowcontrol/v1beta3"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/util/shufflesharding"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/flowcontrol"
	"k8s.io/kubernetes/pkg/apis/flowcontrol/internalbootstrap"
)

// ValidateFlowSchemaName 校验 FlowSchema 的名称。
var ValidateFlowSchemaName = apimachineryvalidation.NameIsDNSSubdomain

// ValidatePriorityLevelConfigurationName 校验 PriorityLevelConfiguration 的名称。
var ValidatePriorityLevelConfigurationName = apimachineryvalidation.NameIsDNSSubdomain

var supportedDistinguisherMethods = sets.NewString(
	string(flowcontrol.FlowDistinguisherMethodByNamespaceType),
	string(flowcontrol.FlowDistinguisherMethodByUserType),
)

var priorityLevelConfigurationQueuingMaxQueues int32 = 10 * 1000 * 1000 // 10^7

var supportedVerbs = sets.NewString(
	"get",
	"list",
	"create",
	"update",
	"delete",
	"deletecollection",
	"patch",
	"watch",
	"proxy",
)

var supportedSubjectKinds = sets.NewString(
	string(flowcontrol.SubjectKindServiceAccount),
	string(flowcontrol.SubjectKindGroup),
	string(flowcontrol.SubjectKindUser),
)

var supportedPriorityLevelEnablement = sets.NewString(
	string(flowcontrol.PriorityLevelEnablementExempt),
	string(flowcontrol.PriorityLevelEnablementLimited),
)

var supportedLimitResponseType = sets.NewString(
	string(flowcontrol.LimitResponseTypeQueue),
	string(flowcontrol.LimitResponseTypeReject),
)

// PriorityLevelValidationOptions 保存优先级级别对象的校验选项。
// 目前为空，预留给后续按版本切换的校验开关。
type PriorityLevelValidationOptions struct{}

// ValidateFlowSchema 校验 FlowSchema 对象的整体内容，包括元数据、spec 和 status。
func ValidateFlowSchema(fs *flowcontrol.FlowSchema) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&fs.ObjectMeta, false, ValidateFlowSchemaName, field.NewPath("metadata"))
	specPath := field.NewPath("spec")
	allErrs = append(allErrs, ValidateFlowSchemaSpec(fs.Name, &fs.Spec, specPath)...)
	if mand, ok := internalbootstrap.MandatoryFlowSchemas[fs.Name]; ok {
		// 强制内置的 FlowSchema 与引导代码中的固定 spec 完全一致。
		// 这里采用严格的深度比较是可行的，因为比较双方最终都来自同一份引导代码。
		if !apiequality.Semantic.DeepEqual(fs.Spec, mand.Spec) {
			allErrs = append(allErrs, field.Invalid(specPath, fs.Spec, fmt.Sprintf("spec of '%s' must equal the fixed value", fs.Name)))
		}
	}
	// status 由控制器写入，但也必须满足结构约束。
	allErrs = append(allErrs, ValidateFlowSchemaStatus(&fs.Status, field.NewPath("status"))...)
	return allErrs
}

// ValidateFlowSchemaUpdate 校验 FlowSchema 的更新。
// 当前更新校验与创建校验一致：所有字段都按全量规则重新校验。
func ValidateFlowSchemaUpdate(old, fs *flowcontrol.FlowSchema) field.ErrorList {
	return ValidateFlowSchema(fs)
}

// ValidateFlowSchemaSpec 校验 FlowSchema 的 spec：匹配优先级、区分方法、引用的优先级级别和规则。
func ValidateFlowSchemaSpec(fsName string, spec *flowcontrol.FlowSchemaSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	// 匹配优先级必须是正数，数值越小越先匹配。
	if spec.MatchingPrecedence <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("matchingPrecedence"), spec.MatchingPrecedence, "must be a positive value"))
	}
	if spec.MatchingPrecedence > flowcontrol.FlowSchemaMaxMatchingPrecedence {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("matchingPrecedence"), spec.MatchingPrecedence, fmt.Sprintf("must not be greater than %v", flowcontrol.FlowSchemaMaxMatchingPrecedence)))
	}
	// 优先级 1 是保留给内置的 exempt schema 的，防止用户请求绕过豁免逻辑。
	if (spec.MatchingPrecedence == 1) && (fsName != flowcontrol.FlowSchemaNameExempt) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("matchingPrecedence"), spec.MatchingPrecedence, "only the schema named 'exempt' may have matchingPrecedence 1"))
	}
	if spec.DistinguisherMethod != nil {
		if !supportedDistinguisherMethods.Has(string(spec.DistinguisherMethod.Type)) {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("distinguisherMethod").Child("type"), spec.DistinguisherMethod, supportedDistinguisherMethods.List()))
		}
	}
	if len(spec.PriorityLevelConfiguration.Name) > 0 {
		for _, msg := range ValidatePriorityLevelConfigurationName(spec.PriorityLevelConfiguration.Name, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("priorityLevelConfiguration").Child("name"), spec.PriorityLevelConfiguration.Name, msg))
		}
	} else {
		allErrs = append(allErrs, field.Required(fldPath.Child("priorityLevelConfiguration").Child("name"), "must reference a priority level"))
	}
	for i, rule := range spec.Rules {
		allErrs = append(allErrs, ValidateFlowSchemaPolicyRulesWithSubjects(&rule, fldPath.Child("rules").Index(i))...)
	}
	return allErrs
}

// ValidateFlowSchemaPolicyRulesWithSubjects 校验带 subject 的策略规则：subject 列表与资源/非资源规则不能同时为空。
func ValidateFlowSchemaPolicyRulesWithSubjects(rule *flowcontrol.PolicyRulesWithSubjects, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(rule.Subjects) > 0 {
		for i, subject := range rule.Subjects {
			allErrs = append(allErrs, ValidateFlowSchemaSubject(&subject, fldPath.Child("subjects").Index(i))...)
		}
	} else {
		allErrs = append(allErrs, field.Required(fldPath.Child("subjects"), "subjects must contain at least one value"))
	}

	// 两类规则必须至少有一类非空，否则该 FlowSchema 永远匹配不到请求。
	if len(rule.ResourceRules) == 0 && len(rule.NonResourceRules) == 0 {
		allErrs = append(allErrs, field.Required(fldPath, "at least one of resourceRules and nonResourceRules has to be non-empty"))
	}
	for i, resourceRule := range rule.ResourceRules {
		allErrs = append(allErrs, ValidateFlowSchemaResourcePolicyRule(&resourceRule, fldPath.Child("resourceRules").Index(i))...)
	}
	for i, nonResourceRule := range rule.NonResourceRules {
		allErrs = append(allErrs, ValidateFlowSchemaNonResourcePolicyRule(&nonResourceRule, fldPath.Child("nonResourceRules").Index(i))...)
	}
	return allErrs
}

// ValidateFlowSchemaSubject 校验 subject，并确保只填写与 kind 相匹配的那一组身份字段。
func ValidateFlowSchemaSubject(subject *flowcontrol.Subject, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	switch subject.Kind {
	case flowcontrol.SubjectKindServiceAccount:
		allErrs = append(allErrs, ValidateServiceAccountSubject(subject.ServiceAccount, fldPath.Child("serviceAccount"))...)
		if subject.User != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("user"), "user is forbidden when subject kind is not 'User'"))
		}
		if subject.Group != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("group"), "group is forbidden when subject kind is not 'Group'"))
		}
	case flowcontrol.SubjectKindUser:
		allErrs = append(allErrs, ValidateUserSubject(subject.User, fldPath.Child("user"))...)
		if subject.ServiceAccount != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("serviceAccount"), "serviceAccount is forbidden when subject kind is not 'ServiceAccount'"))
		}
		if subject.Group != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("group"), "group is forbidden when subject kind is not 'Group'"))
		}
	case flowcontrol.SubjectKindGroup:
		allErrs = append(allErrs, ValidateGroupSubject(subject.Group, fldPath.Child("group"))...)
		if subject.ServiceAccount != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("serviceAccount"), "serviceAccount is forbidden when subject kind is not 'ServiceAccount'"))
		}
		if subject.User != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("user"), "user is forbidden when subject kind is not 'User'"))
		}
	default:
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("kind"), subject.Kind, supportedSubjectKinds.List()))
	}
	return allErrs
}

// ValidateServiceAccountSubject 校验 "ServiceAccount" 类型的 subject，name 和 namespace 都是必填项。
func ValidateServiceAccountSubject(subject *flowcontrol.ServiceAccountSubject, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if subject == nil {
		return append(allErrs, field.Required(fldPath, "serviceAccount is required when subject kind is 'ServiceAccount'"))
	}
	if len(subject.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	} else if subject.Name != flowcontrol.NameAll {
		for _, msg := range apimachineryvalidation.ValidateServiceAccountName(subject.Name, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), subject.Name, msg))
		}
	}

	if len(subject.Namespace) > 0 {
		for _, msg := range apimachineryvalidation.ValidateNamespaceName(subject.Namespace, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("namespace"), subject.Namespace, msg))
		}
	} else {
		allErrs = append(allErrs, field.Required(fldPath.Child("namespace"), "must specify namespace for service account"))
	}

	return allErrs
}

// ValidateUserSubject 校验 "User" 类型的 subject。
func ValidateUserSubject(subject *flowcontrol.UserSubject, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if subject == nil {
		return append(allErrs, field.Required(fldPath, "user is required when subject kind is 'User'"))
	}
	if len(subject.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	}
	return allErrs
}

// ValidateGroupSubject 校验 "Group" 类型的 subject。
func ValidateGroupSubject(subject *flowcontrol.GroupSubject, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if subject == nil {
		return append(allErrs, field.Required(fldPath, "group is required when subject kind is 'Group'"))
	}
	if len(subject.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	}
	return allErrs
}

// ValidateFlowSchemaNonResourcePolicyRule 校验非资源策略规则；通配符 '*' 只能单独出现，不能与其他值混用。
func ValidateFlowSchemaNonResourcePolicyRule(rule *flowcontrol.NonResourcePolicyRule, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if len(rule.Verbs) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("verbs"), "verbs must contain at least one value"))
	} else if hasWildcard(rule.Verbs) {
		if len(rule.Verbs) > 1 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("verbs"), rule.Verbs, "if '*' is present, must not specify other verbs"))
		}
	} else if !supportedVerbs.IsSuperset(sets.NewString(rule.Verbs...)) {
		// 只允许使用已知的请求动词，避免拼写错误导致规则意外失效。
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("verbs"), rule.Verbs, supportedVerbs.List()))
	}

	if len(rule.NonResourceURLs) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("nonResourceURLs"), "nonResourceURLs must contain at least one value"))
	} else if hasWildcard(rule.NonResourceURLs) {
		if len(rule.NonResourceURLs) > 1 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nonResourceURLs"), rule.NonResourceURLs, "if '*' is present, must not specify other non-resource URLs"))
		}
	} else {
		for i, nonResourceURL := range rule.NonResourceURLs {
			if err := ValidateNonResourceURLPath(nonResourceURL, fldPath.Child("nonResourceURLs").Index(i)); err != nil {
				allErrs = append(allErrs, err)
			}
		}
	}

	return allErrs
}

// ValidateFlowSchemaResourcePolicyRule 校验资源策略规则，包括 verbs、apiGroups、resources 和 namespaces。
func ValidateFlowSchemaResourcePolicyRule(rule *flowcontrol.ResourcePolicyRule, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if len(rule.Verbs) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("verbs"), "verbs must contain at least one value"))
	} else if hasWildcard(rule.Verbs) {
		if len(rule.Verbs) > 1 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("verbs"), rule.Verbs, "if '*' is present, must not specify other verbs"))
		}
	} else if !supportedVerbs.IsSuperset(sets.NewString(rule.Verbs...)) {
		// 只允许使用已知的请求动词，避免拼写错误导致规则意外失效。
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("verbs"), rule.Verbs, supportedVerbs.List()))
	}

	if len(rule.APIGroups) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("apiGroups"), "resource rules must supply at least one api group"))
	} else if len(rule.APIGroups) > 1 && hasWildcard(rule.APIGroups) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("apiGroups"), rule.APIGroups, "if '*' is present, must not specify other api groups"))
	}

	if len(rule.Resources) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("resources"), "resource rules must supply at least one resource"))
	} else if len(rule.Resources) > 1 && hasWildcard(rule.Resources) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("resources"), rule.Resources, "if '*' is present, must not specify other resources"))
	}

	// 非集群作用域的规则至少要指定一个 namespace，否则匹配范围不明确。
	if len(rule.Namespaces) == 0 && !rule.ClusterScope {
		allErrs = append(allErrs, field.Required(fldPath.Child("namespaces"), "resource rules that are not cluster scoped must supply at least one namespace"))
	} else if hasWildcard(rule.Namespaces) {
		if len(rule.Namespaces) > 1 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("namespaces"), rule.Namespaces, "if '*' is present, must not specify other namespaces"))
		}
	} else {
		for idx, tgtNS := range rule.Namespaces {
			for _, msg := range apimachineryvalidation.ValidateNamespaceName(tgtNS, false) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("namespaces").Index(idx), tgtNS, nsErrIntro+msg))
			}
		}
	}

	return allErrs
}

const nsErrIntro = "each member of this list must be '*' or a DNS-1123 label; "

// ValidateFlowSchemaStatus 校验 FlowSchema 的 status，condition 的 type 不允许重复。
func ValidateFlowSchemaStatus(status *flowcontrol.FlowSchemaStatus, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	keys := sets.NewString()
	for i, condition := range status.Conditions {
		// 同一类型的 condition 出现多次会让状态含义产生歧义。
		if keys.Has(string(condition.Type)) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Child("conditions").Index(i).Child("type"), condition.Type))
		}
		keys.Insert(string(condition.Type))
		allErrs = append(allErrs, ValidateFlowSchemaCondition(&condition, fldPath.Child("conditions").Index(i))...)
	}
	return allErrs
}

// ValidateFlowSchemaStatusUpdate 校验 FlowSchema status 的更新，只处理 status 部分。
func ValidateFlowSchemaStatusUpdate(old, fs *flowcontrol.FlowSchema) field.ErrorList {
	return ValidateFlowSchemaStatus(&fs.Status, field.NewPath("status"))
}

// ValidateFlowSchemaCondition 校验 FlowSchema status 中的单个 condition。
func ValidateFlowSchemaCondition(condition *flowcontrol.FlowSchemaCondition, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(condition.Type) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("type"), ""))
	}
	return allErrs
}

// ValidatePriorityLevelConfiguration 校验优先级级别配置对象，包括元数据、spec、内置对象约束和 status。
func ValidatePriorityLevelConfiguration(pl *flowcontrol.PriorityLevelConfiguration, requestGV schema.GroupVersion, opts PriorityLevelValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&pl.ObjectMeta, false, ValidatePriorityLevelConfigurationName, field.NewPath("metadata"))

	// roundtrip 注解仅供 v1beta3 转换过程使用；转换完成后内部对象不应保留它，
	// 因此这里显式禁止用户设置，避免内部表示被外部数据污染。
	if _, ok := pl.ObjectMeta.Annotations[flowcontrolv1beta3.PriorityLevelPreserveZeroConcurrencySharesKey]; ok {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("metadata").Child("annotations"), fmt.Sprintf("annotation '%s' is forbidden", flowcontrolv1beta3.PriorityLevelPreserveZeroConcurrencySharesKey)))
	}

	specPath := field.NewPath("spec")
	allErrs = append(allErrs, ValidatePriorityLevelConfigurationSpec(&pl.Spec, requestGV, pl.Name, specPath, opts)...)
	allErrs = append(allErrs, ValidateIfMandatoryPriorityLevelConfigurationObject(pl, specPath)...)
	allErrs = append(allErrs, ValidatePriorityLevelConfigurationStatus(&pl.Status, field.NewPath("status"))...)
	return allErrs
}

// ValidateIfMandatoryPriorityLevelConfigurationObject 校验内置优先级级别配置未被随意篡改。
// 内置对象是 API Priority and Fairness 正确运行的基础，允许修改的范围被严格限制。
func ValidateIfMandatoryPriorityLevelConfigurationObject(pl *flowcontrol.PriorityLevelConfiguration, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	mand, ok := internalbootstrap.MandatoryPriorityLevelConfigurations[pl.Name]
	if !ok {
		return allErrs
	}

	if pl.Name == flowcontrol.PriorityLevelConfigurationNameExempt {
		// exempt 这个单例对象允许管理员调整 spec.exempt（放行请求的范围），
		// 但其余字段属于系统运行的关键参数，不允许修改。
		want := &mand.Spec
		have := pl.Spec.DeepCopy()
		have.Exempt = want.Exempt
		if !apiequality.Semantic.DeepEqual(want, have) {
			allErrs = append(allErrs, field.Invalid(fldPath, pl.Spec, fmt.Sprintf("spec of '%s' except the 'spec.exempt' field must equal the fixed value", pl.Name)))
		}
		return allErrs
	}

	// 其余内置对象要求 spec 与引导代码中的固定值完全一致。
	// 比较双方最终都来自同一份引导代码，因此严格比较是安全的。
	if !apiequality.Semantic.DeepEqual(pl.Spec, mand.Spec) {
		allErrs = append(allErrs, field.Invalid(fldPath, pl.Spec, fmt.Sprintf("spec of '%s' must equal the fixed value", pl.Name)))
	}
	return allErrs
}

// ValidatePriorityLevelConfigurationSpec 校验 spec.type 与对象名称是否匹配，并按 Exempt/Limited 分类校验其余字段。
func ValidatePriorityLevelConfigurationSpec(spec *flowcontrol.PriorityLevelConfigurationSpec, requestGV schema.GroupVersion, name string, fldPath *field.Path, opts PriorityLevelValidationOptions) field.ErrorList {
	var allErrs field.ErrorList
	// 名为 exempt 的对象必须是 Exempt 类型，其他对象则不能是 Exempt 类型。
	if (name == flowcontrol.PriorityLevelConfigurationNameExempt) != (spec.Type == flowcontrol.PriorityLevelEnablementExempt) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("type"), spec.Type, "must be 'Exempt' if and only if `name` is 'exempt'"))
	}
	switch spec.Type {
	case flowcontrol.PriorityLevelEnablementExempt:
		if spec.Limited != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("limited"), "must be nil if the type is not Limited").MarkCoveredByDeclarative())
		}
		if spec.Exempt != nil {
			allErrs = append(allErrs, ValidateExemptPriorityLevelConfiguration(spec.Exempt, fldPath.Child("exempt"))...)
		}
	case flowcontrol.PriorityLevelEnablementLimited:
		if spec.Exempt != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("exempt"), "must be nil if the type is Limited").MarkCoveredByDeclarative())
		}

		if spec.Limited == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("limited"), "must not be empty when type is Limited").MarkCoveredByDeclarative())
		} else {
			allErrs = append(allErrs, ValidateLimitedPriorityLevelConfiguration(spec.Limited, requestGV, fldPath.Child("limited"), opts)...)
		}
	default:
		if len(spec.Type) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("type"), "").MarkCoveredByDeclarative())
		} else {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("type"), spec.Type, supportedPriorityLevelEnablement.List()))
		}
	}
	return allErrs
}

// ValidateLimitedPriorityLevelConfiguration 校验 Limited 类型的优先级级别配置，包括并发份额、限流响应和借出/借入比例。
func ValidateLimitedPriorityLevelConfiguration(lplc *flowcontrol.LimitedPriorityLevelConfiguration, requestGV schema.GroupVersion, fldPath *field.Path, opts PriorityLevelValidationOptions) field.ErrorList {
	var allErrs field.ErrorList
	if lplc.NominalConcurrencyShares < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child(getVersionedFieldNameForConcurrencyShares(requestGV)), lplc.NominalConcurrencyShares, "must be a non-negative integer"))
	}
	allErrs = append(allErrs, ValidateLimitResponse(lplc.LimitResponse, fldPath.Child("limitResponse"))...)

	if lplc.LendablePercent != nil && !(*lplc.LendablePercent >= 0 && *lplc.LendablePercent <= 100) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("lendablePercent"), *lplc.LendablePercent, "must be between 0 and 100, inclusive"))
	}
	if lplc.BorrowingLimitPercent != nil && *lplc.BorrowingLimitPercent < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("borrowingLimitPercent"), *lplc.BorrowingLimitPercent, "if specified, must be a non-negative integer"))
	}

	return allErrs
}

// ValidateExemptPriorityLevelConfiguration 校验 Exempt 类型的优先级级别配置。
// Exempt 请求不受排队限流约束，但仍需保证并发份额和借出比例是合法数值。
func ValidateExemptPriorityLevelConfiguration(eplc *flowcontrol.ExemptPriorityLevelConfiguration, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if eplc.NominalConcurrencyShares != nil && *eplc.NominalConcurrencyShares < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("nominalConcurrencyShares"), *eplc.NominalConcurrencyShares, "must be a non-negative integer"))
	}
	if eplc.LendablePercent != nil && !(*eplc.LendablePercent >= 0 && *eplc.LendablePercent <= 100) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("lendablePercent"), *eplc.LendablePercent, "must be between 0 and 100, inclusive"))
	}
	return allErrs
}

// getVersionedFieldNameForConcurrencyShares 按请求的版本返回并发份额字段的名称。
// 老版本中该字段叫 assuredConcurrencyShares，新版本改名为 nominalConcurrencyShares，
// 错误信息必须使用请求version对应的字段，否则用户无法定位问题字段。
func getVersionedFieldNameForConcurrencyShares(requestGV schema.GroupVersion) string {
	switch {
	case requestGV == flowcontrolv1beta1.SchemeGroupVersion ||
		requestGV == flowcontrolv1beta2.SchemeGroupVersion:
		return "assuredConcurrencyShares"
	default:
		return "nominalConcurrencyShares"
	}
}

// ValidateLimitResponse 校验 LimitResponse：Reject 不允许带 queuing，Queue 则必须带合法的 queuing 配置。
func ValidateLimitResponse(lr flowcontrol.LimitResponse, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	switch lr.Type {
	case flowcontrol.LimitResponseTypeReject:
		if lr.Queuing != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("queuing"), "must be nil if limited.limitResponse.type is not Limited").MarkCoveredByDeclarative())
		}
	case flowcontrol.LimitResponseTypeQueue:
		if lr.Queuing == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("queuing"), "must not be empty if limited.limitResponse.type is Limited").MarkCoveredByDeclarative())
		} else {
			allErrs = append(allErrs, ValidatePriorityLevelQueuingConfiguration(lr.Queuing, fldPath.Child("queuing"))...)
		}
	default:
		if len(lr.Type) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("type"), "").MarkCoveredByDeclarative())
		} else {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("type"), lr.Type, supportedLimitResponseType.List()))
		}
	}
	return allErrs
}

// ValidatePriorityLevelQueuingConfiguration 校验排队参数，包括队列长度、队列数和 shuffle sharding 的手牌大小。
func ValidatePriorityLevelQueuingConfiguration(queuing *flowcontrol.QueuingConfiguration, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if queuing.QueueLengthLimit <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("queueLengthLimit"), queuing.QueueLengthLimit, "must be positive"))
	}

	// 这些参数会作为 shuffle sharding 的输入，必须落在算法可接受的范围内。
	if queuing.Queues <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("queues"), queuing.Queues, "must be positive"))
	} else if queuing.Queues > priorityLevelConfigurationQueuingMaxQueues {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("queues"), queuing.Queues,
			fmt.Sprintf("must not be greater than %d", priorityLevelConfigurationQueuingMaxQueues)))
	}

	if queuing.HandSize <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("handSize"), queuing.HandSize, "must be positive"))
	} else if queuing.HandSize > queuing.Queues {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("handSize"), queuing.HandSize,
			fmt.Sprintf("should not be greater than queues (%d)", queuing.Queues)))
	} else if entropy := shufflesharding.RequiredEntropyBits(int(queuing.Queues), int(queuing.HandSize)); entropy > shufflesharding.MaxHashBits {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("handSize"), queuing.HandSize,
			fmt.Sprintf("required entropy bits of deckSize %d and handSize %d should not be greater than %d", queuing.Queues, queuing.HandSize, shufflesharding.MaxHashBits)))
	}
	return allErrs
}

// ValidatePriorityLevelConfigurationStatus 校验优先级级别配置的 status，condition 的 type 不允许重复。
func ValidatePriorityLevelConfigurationStatus(status *flowcontrol.PriorityLevelConfigurationStatus, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	keys := sets.NewString()
	for i, condition := range status.Conditions {
		if keys.Has(string(condition.Type)) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Child("conditions").Index(i).Child("type"), condition.Type))
		}
		keys.Insert(string(condition.Type))
		allErrs = append(allErrs, ValidatePriorityLevelConfigurationCondition(&condition, fldPath.Child("conditions").Index(i))...)
	}
	return allErrs
}

// ValidatePriorityLevelConfigurationStatusUpdate 校验优先级级别配置 status 的更新，只处理 status 部分。
func ValidatePriorityLevelConfigurationStatusUpdate(old, pl *flowcontrol.PriorityLevelConfiguration) field.ErrorList {
	return ValidatePriorityLevelConfigurationStatus(&pl.Status, field.NewPath("status"))
}

// ValidatePriorityLevelConfigurationCondition 校验优先级级别配置 status 中的单个 condition。
func ValidatePriorityLevelConfigurationCondition(condition *flowcontrol.PriorityLevelConfigurationCondition, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(condition.Type) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("type"), ""))
	}
	return allErrs
}

// ValidateNonResourceURLPath 按以下规则校验非资源 URL 路径：
//  1. 路径必须以斜杠开头
//  2. 路径中不允许出现空白字符
//  3. 路径中不允许出现连续的斜杠
//  4. 通配符 "*" 只能用于后缀匹配，且它本身也能匹配斜杠
func ValidateNonResourceURLPath(path string, fldPath *field.Path) *field.Error {
	if len(path) == 0 {
		return field.Invalid(fldPath, path, "must not be empty")
	}
	if path == "/" { // 根路径是唯一合法的特例，直接放行
		return nil
	}

	if !strings.HasPrefix(path, "/") {
		return field.Invalid(fldPath, path, "must start with slash")
	}
	if strings.Contains(path, " ") {
		return field.Invalid(fldPath, path, "must not contain white-space")
	}
	if strings.Contains(path, "//") {
		return field.Invalid(fldPath, path, "must not contain double slash")
	}
	// 通配符只能单独出现在末尾，防止出现难以推理的前缀/中间匹配语义。
	wildcardCount := strings.Count(path, "*")
	if wildcardCount > 1 || (wildcardCount == 1 && path[len(path)-2:] != "/*") {
		return field.Invalid(fldPath, path, "wildcard can only do suffix matching")
	}
	return nil
}

// hasWildcard 判断操作列表中是否包含通配符 "*"。
func hasWildcard(operations []string) bool {
	return memberInList("*", operations...)
}

// memberInList 判断目标值是否出现在给定列表中。
func memberInList(seek string, a ...string) bool {
	for _, ai := range a {
		if ai == seek {
			return true
		}
	}
	return false
}
