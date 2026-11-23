/*
Copyright 2021 The Kubernetes Authors.

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
	"strings"

	machinery "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	admissionapi "k8s.io/pod-security-admission/admission/api"
	"k8s.io/pod-security-admission/api"
)

// ValidatePodSecurityConfiguration validates a given PodSecurityConfiguration.
// 一次性收集所有字段的错误，方便用户一次改完，而不是逐条报错反复提交。
func ValidatePodSecurityConfiguration(configuration *admissionapi.PodSecurityConfiguration) field.ErrorList {
	allErrs := field.ErrorList{}

	// validate defaults
	// 默认值决定未打标签命名空间的行为，必须先确认等级和版本号都能被解析。
	allErrs = append(allErrs, validateLevel(field.NewPath("defaults", "enforce"), configuration.Defaults.Enforce)...)
	allErrs = append(allErrs, validateVersion(field.NewPath("defaults", "enforce-version"), configuration.Defaults.EnforceVersion)...)
	allErrs = append(allErrs, validateLevel(field.NewPath("defaults", "warn"), configuration.Defaults.Warn)...)
	allErrs = append(allErrs, validateVersion(field.NewPath("defaults", "warn-version"), configuration.Defaults.WarnVersion)...)
	allErrs = append(allErrs, validateLevel(field.NewPath("defaults", "audit"), configuration.Defaults.Audit)...)
	allErrs = append(allErrs, validateVersion(field.NewPath("defaults", "audit-version"), configuration.Defaults.AuditVersion)...)

	// validate exemptions
	// 豁免名单直接放宽准入边界，名字格式和重复项都要挡住，避免配置写错却静默生效。
	allErrs = append(allErrs, validateNamespaces(configuration)...)
	allErrs = append(allErrs, validateRuntimeClasses(configuration)...)
	allErrs = append(allErrs, validateUsernames(configuration)...)

	return allErrs
}

// validateLevel validates a level
// 复用解析函数做校验，保证「配置能通过」与「准入时能识别」是同一个判断标准。
func validateLevel(p *field.Path, value string) field.ErrorList {
	errs := field.ErrorList{}
	_, err := api.ParseLevel(value)
	if err != nil {
		errs = append(errs, field.Invalid(p, value, err.Error()))
	}
	return errs
}

// validateVersion validates a version
// 版本字符串写错会导致策略回退到意外版本，所以在加载配置时就要拦下来。
func validateVersion(p *field.Path, value string) field.ErrorList {
	errs := field.ErrorList{}
	_, err := api.ParseVersion(value)
	if err != nil {
		errs = append(errs, field.Invalid(p, value, err.Error()))
	}
	return errs
}

// 豁免命名空间必须是合法名字，且不能重复——重复项通常意味着配置写错，而不是有意多加一条。
func validateNamespaces(configuration *admissionapi.PodSecurityConfiguration) field.ErrorList {
	errs := field.ErrorList{}
	validSet := sets.NewString()
	for i, ns := range configuration.Exemptions.Namespaces {
		err := machinery.ValidateNamespaceName(ns, false)
		if len(err) > 0 {
			path := field.NewPath("exemptions", "namespaces").Index(i)
			errs = append(errs, field.Invalid(path, ns, strings.Join(err, ", ")))
			continue
		}
		if validSet.Has(ns) {
			path := field.NewPath("exemptions", "namespaces").Index(i)
			errs = append(errs, field.Duplicate(path, ns))
			continue
		}
		validSet.Insert(ns)
	}
	return errs
}

// RuntimeClass 名字按 DNS 子域校验，因为集群里实际引用的就是这个名字，写错等于豁免失效。
func validateRuntimeClasses(configuration *admissionapi.PodSecurityConfiguration) field.ErrorList {
	errs := field.ErrorList{}
	validSet := sets.NewString()
	for i, rc := range configuration.Exemptions.RuntimeClasses {
		err := machinery.NameIsDNSSubdomain(rc, false)
		if len(err) > 0 {
			path := field.NewPath("exemptions", "runtimeClasses").Index(i)
			errs = append(errs, field.Invalid(path, rc, strings.Join(err, ", ")))
			continue
		}
		if validSet.Has(rc) {
			path := field.NewPath("exemptions", "runtimeClasses").Index(i)
			errs = append(errs, field.Duplicate(path, rc))
			continue
		}
		validSet.Insert(rc)
	}
	return errs
}

// 用户名只有「非空」这一条硬约束，格式由身份提供方决定，这里不替它们做判断。
func validateUsernames(configuration *admissionapi.PodSecurityConfiguration) field.ErrorList {
	errs := field.ErrorList{}
	validSet := sets.NewString()
	for i, uname := range configuration.Exemptions.Usernames {
		if uname == "" {
			path := field.NewPath("exemptions", "usernames").Index(i)
			errs = append(errs, field.Invalid(path, uname, "username must not be empty"))
			continue
		}
		if validSet.Has(uname) {
			path := field.NewPath("exemptions", "usernames").Index(i)
			errs = append(errs, field.Duplicate(path, uname))
			continue
		}
		validSet.Insert(uname)
	}

	return errs
}
