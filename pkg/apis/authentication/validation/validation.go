/*
Copyright 2018 The Kubernetes Authors.

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

// Package validation 包含对 authentication.k8s.io API 组中各类资源进行校验的方法。
package validation

import (
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	admissionregistrationv1 "k8s.io/kubernetes/pkg/apis/admissionregistration/v1"
	"k8s.io/kubernetes/pkg/apis/authentication"
)

// MinTokenAgeSec 定义 TokenRequest 允许的最短有效期（10 分钟），
// 防止请求方创建几乎立即过期、无法实际使用的令牌。
const MinTokenAgeSec = 10 * 60 // 10 minutes

// ValidateTokenRequest 校验 TokenRequest 的合法性。
// 主要约束过期时间范围，并在绑定 webhook 配置时强制指定唯一的 audience
// 与 admissionReviewAPIGroups 证明信息。
func ValidateTokenRequest(tr *authentication.TokenRequest) field.ErrorList {
	allErrs := field.ErrorList{}
	specPath := field.NewPath("spec")

	if tr.Spec.ExpirationSeconds < MinTokenAgeSec {
		allErrs = append(allErrs, field.Invalid(specPath.Child("expirationSeconds"), tr.Spec.ExpirationSeconds, "may not specify a duration less than 10 minutes"))
	}
	if tr.Spec.ExpirationSeconds > 1<<32 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("expirationSeconds"), tr.Spec.ExpirationSeconds, "may not specify a duration larger than 2^32 seconds"))
	}

	switch {
	case isWebhookRef(tr.Spec.BoundObjectRef):
		// 绑定到 webhook 时只允许一个 audience，确保令牌用途唯一。
		if len(tr.Spec.Audiences) != 1 {
			allErrs = append(allErrs, field.Invalid(specPath.Child("audiences"), tr.Spec.Audiences, "must be length 1 when bound to a webhook config"))
		}
		allErrs = append(allErrs, validateWebhookAttestations(tr.Spec.Attestations, specPath.Child("attestations"))...)
	default:
		// 非 webhook 绑定场景禁止携带证明信息，避免无意义或被误用的字段。
		if len(tr.Spec.Attestations) != 0 {
			return field.ErrorList{field.Invalid(specPath.Child("attestations"), tr.Spec.Attestations, "attestations may only be specified with a webhook bound object reference")}
		}
	}

	return allErrs
}

// validateWebhookAttestations 校验 webhook 绑定请求中的证明信息：
// 必须恰好包含一个 admissionReviewAPIGroups 证明，再逐项校验键和值。
func validateWebhookAttestations(attestations map[string]authentication.AttestationValue, attestationsPath *field.Path) field.ErrorList {
	if len(attestations) != 1 {
		return field.ErrorList{field.Invalid(attestationsPath, attestations, "webhook bound requests require exactly one admissionReviewAPIGroups attestation")}
	}

	// 逐个键检查内容是否正确。
	errs := field.ErrorList{}
	for key, values := range attestations {
		errs = append(errs, validateWebhookKeysAndValues(key, values, attestationsPath.Key(key))...)
	}

	return errs
}

// validateWebhookKeysAndValues 校验证明信息的键必须为受支持的键，且值符合对应格式。
// 目前仅支持 admissionReviewAPIGroups，并且只能有一个值。
func validateWebhookKeysAndValues(key string, values authentication.AttestationValue, path *field.Path) field.ErrorList {
	switch key {
	case authentication.AttestationAdmissionReviewAPIGroups:
		if len(values) != 1 {
			return field.ErrorList{field.Invalid(path, values, "must specify a single value")}
		}
		return validateAdmissionReviewAPIGroupsValue(values[0], path.Index(0))
	default:
		return field.ErrorList{field.NotSupported(path, key, []string{authentication.AttestationAdmissionReviewAPIGroups})}
	}
}

// validateAdmissionReviewAPIGroupsValue 校验 admissionReviewAPIGroups 证明值是否合法。
// 允许 "*" 表示核心 API 组，其他值必须是合法的 DNS 子域名；
// 空字符串被拒绝，因为它在资源名和 API 组两种语境下含义不同，容易引发歧义。
func validateAdmissionReviewAPIGroupsValue(group string, path *field.Path) field.ErrorList {
	var errs field.ErrorList

	// "" 不能作为 "admissionReviewAPIGroups" 的值，因为在不同上下文中 "" 含义不同：
	// 在授权请求的资源名语境中它表示“所有资源名”，在 API 组语境中表示“核心 API 组”。
	//
	// 对核心 API 组而言，唯一合法的 admission review 令牌请求方是 Kubernetes API Server 自身。
	// API Server 在需要向 webhook 认证时总会使用 "admissionReviewAPIGroups": "*"，
	// 因此没有任何场景需要放行该证明的空字符串值。
	if len(group) == 0 {
		errs = append(errs, field.Invalid(path, group, "may not be an empty string"))
		return errs
	}

	// "*" 对 kube-apiserver 是合法值。
	if group == "*" {
		return nil
	}

	// 其他值必须是合法的 DNS 子域名，例如 "admissionregistration.k8s.io" 或 "apps"。
	for _, reason := range validation.IsDNS1123Subdomain(group) {
		errs = append(errs, field.Invalid(path, group, "must specify a valid API Group: "+reason))
	}

	return errs
}

// isWebhookRef 判断绑定对象引用是否指向受支持的 webhook 配置类型
// （ValidatingWebhookConfiguration 或 MutatingWebhookConfiguration），且 API 版本匹配。
func isWebhookRef(ref *authentication.BoundObjectReference) bool {
	if ref == nil {
		return false
	}
	switch ref.Kind {
	case "ValidatingWebhookConfiguration", "MutatingWebhookConfiguration":
		return ref.APIVersion == admissionregistrationv1.SchemeGroupVersion.String()
	default:
		return false
	}
}
