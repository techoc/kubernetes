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

// Package validation 校验审计策略（audit.k8s.io Policy）的字段取值与组合约束。
// 校验失败时返回 field.ErrorList，调用方据此给出精确到字段路径的错误信息。
package validation

import (
	"strings"

	"k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/apis/audit"
)

// ValidatePolicy 校验审计策略
//
// 先校验策略级的 omitStages，再逐条校验规则；所有错误会累积返回，而不是遇到
// 第一条就停止，便于用户一次修正全部问题。
func ValidatePolicy(policy *audit.Policy) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateOmitStages(policy.OmitStages, field.NewPath("omitStages"))...)
	rulePath := field.NewPath("rules")
	for i, rule := range policy.Rules {
		allErrs = append(allErrs, validatePolicyRule(rule, rulePath.Index(i))...)
	}
	return allErrs
}

// validatePolicyRule 校验单条规则：level、URL、资源与 omitStages，
// 并检查“资源型/非资源型不能混用”的互斥约束。
func validatePolicyRule(rule audit.PolicyRule, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateLevel(rule.Level, fldPath.Child("level"))...)
	allErrs = append(allErrs, validateNonResourceURLs(rule.NonResourceURLs, fldPath.Child("nonResourceURLs"))...)
	allErrs = append(allErrs, validateResources(rule.Resources, fldPath.Child("resources"))...)
	allErrs = append(allErrs, validateOmitStages(rule.OmitStages, fldPath.Child("omitStages"))...)

	if len(rule.NonResourceURLs) > 0 {
		// 一条规则只能描述资源请求或非资源请求；混用会让匹配语义含糊。
		if len(rule.Resources) > 0 || len(rule.Namespaces) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("nonResourceURLs"), rule.NonResourceURLs, "rules cannot apply to both regular resources and non-resource URLs"))
		}
	}

	return allErrs
}

var validLevels = []string{
	string(audit.LevelNone),
	string(audit.LevelMetadata),
	string(audit.LevelRequest),
	string(audit.LevelRequestResponse),
}

var validOmitStages = []string{
	string(audit.StageRequestReceived),
	string(audit.StageResponseStarted),
	string(audit.StageResponseComplete),
	string(audit.StagePanic),
}

// validateLevel 校验审计等级；level 是必填字段，空值按 Required 报错。
func validateLevel(level audit.Level, fldPath *field.Path) field.ErrorList {
	switch level {
	case audit.LevelNone, audit.LevelMetadata, audit.LevelRequest, audit.LevelRequestResponse:
		return nil
	case "":
		return field.ErrorList{field.Required(fldPath, "")}
	default:
		return field.ErrorList{field.NotSupported(fldPath, level, validLevels)}
	}
}

// validateNonResourceURLs 要求 URL 以 '/' 开头，且 '*' 只能作为最后一步，
// 否则前缀匹配会退化为任意位置的模式匹配，难以预测命中范围。
func validateNonResourceURLs(urls []string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	for i, url := range urls {
		if url == "*" {
			continue
		}

		if !strings.HasPrefix(url, "/") {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i), url, "non-resource URL rules must begin with a '/' character"))
		}

		if url != "" && strings.ContainsRune(url[:len(url)-1], '*') {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i), url, "non-resource URL wildcards '*' must be the final character of the rule"))
		}
	}
	return allErrs
}

// validateResources 校验 API 组名是否为合法 DNS 子域，并检查 resourceNames
// 不能脱离 resources 单独使用（否则无法确定要进行名称匹配的资源类型）。
func validateResources(groupResources []audit.GroupResources, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	for _, groupResource := range groupResources {
		// 空字符串代表 core API 组。
		if len(groupResource.Group) != 0 && groupResource.Group != "*" {
			// 组名必须为小写且是合法的 DNS 子域。
			// 参考：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md
			// 像 rbac.authorization.k8s.io/v1beta1 这样的组名会被拒绝，
			// 其中 rbac.authorization.k8s.io 才是合法部分。
			if msgs := validation.NameIsDNSSubdomain(groupResource.Group, false); len(msgs) != 0 {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("group"), groupResource.Group, strings.Join(msgs, ",")))
			}
		}

		if len(groupResource.ResourceNames) > 0 && len(groupResource.Resources) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("resourceNames"), groupResource.ResourceNames, "using resourceNames requires at least one resource"))
		}
	}
	return allErrs
}

// validateOmitStages 只允许四个已知阶段；拼写错误会让“省略”静默失效，因此必须拒绝。
func validateOmitStages(omitStages []audit.Stage, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	for i, stage := range omitStages {
		valid := false
		for _, validOmitStage := range validOmitStages {
			if string(stage) == validOmitStage {
				valid = true
				break
			}
		}
		if !valid {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i), string(stage), "allowed stages are "+strings.Join(validOmitStages, ",")))
		}
	}
	return allErrs
}
