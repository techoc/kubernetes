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
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/api/validation"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"

	"k8s.io/kube-aggregator/pkg/apis/apiregistration"
)

// ValidateAPIService 校验 APIService 的定义是否正确。
// 这里不只是逐字段检错：APIService 有两条隐含约束必须在对象级别才能检查——
// 名字必须等于 version.group，以及本地服务和远程服务的合法配置互斥。
// 校验器返回错误列表而不是首个错误，是为了让调用方一次看到所有问题。
func ValidateAPIService(ctx context.Context, apiService *apiregistration.APIService) field.ErrorList {
	// 名字和 spec 的对应关系是硬约束：聚合层靠名字定位 GroupVersion，
	// 允许两者不一致会让路由和对象身份脱节。
	requiredName := apiService.Spec.Version + "." + apiService.Spec.Group

	allErrs := validation.ValidateObjectMetaDeclaratively(ctx, operation.Create, &apiService.ObjectMeta, nil, false,
		func(name string, prefix bool) []string {
			if minimalFailures := content.IsPathSegmentName(name); len(minimalFailures) > 0 {
				return minimalFailures
			}
			// the name *must* be version.group
			if name != requiredName {
				return []string{fmt.Sprintf("must be `spec.version+\".\"+spec.group`: %q", requiredName)}
			}

			return []string{}
		},
		field.NewPath("metadata"),
		utilfeature.DefaultFeatureGate.Enabled(features.DeclarativeValidationBeta))

	// 此处允许空 group：核心 API 组历史上没有组名，它的 APIService 只能写成 v1.；
	// 其他情况留空组名会让对象落不到任何 GroupVersion 上，所以必须拒绝。
	if len(apiService.Spec.Group) == 0 && apiService.Spec.Version != "v1" {
		allErrs = append(allErrs, field.Required(field.NewPath("spec", "group"), "only v1 may have an empty group and it better be legacy kube"))
	}
	if len(apiService.Spec.Group) > 0 {
		for _, errString := range utilvalidation.IsDNS1123Subdomain(apiService.Spec.Group) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "group"), apiService.Spec.Group, errString))
		}
	}

	for _, errString := range utilvalidation.IsDNS1035Label(apiService.Spec.Version) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "version"), apiService.Spec.Version, errString))
	}

	if apiService.Spec.GroupPriorityMinimum <= 0 || apiService.Spec.GroupPriorityMinimum > 20000 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "groupPriorityMinimum"), apiService.Spec.GroupPriorityMinimum, "must be positive and less than 20000"))
	}
	if apiService.Spec.VersionPriority <= 0 || apiService.Spec.VersionPriority > 1000 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "versionPriority"), apiService.Spec.VersionPriority, "must be positive and less than 1000"))
	}

	// Service 为空代表这是"本地"APIService：请求直接转给普通 handler 链处理，
	// 根本不走 TLS 连接。因此任何和远端 TLS 有关的配置在这里都没有意义，
	// 留着只会让人误以为它生效，必须显式报错。
	if apiService.Spec.Service == nil {
		if len(apiService.Spec.CABundle) != 0 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "caBundle"), fmt.Sprintf("%d bytes", len(apiService.Spec.CABundle)), "local APIServices may not have a caBundle"))
		}
		if apiService.Spec.InsecureSkipTLSVerify {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "insecureSkipTLSVerify"), apiService.Spec.InsecureSkipTLSVerify, "local APIServices may not have insecureSkipTLSVerify"))
		}
		return allErrs
	}

	if len(apiService.Spec.Service.Namespace) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("spec", "service", "namespace"), ""))
	}
	if len(apiService.Spec.Service.Name) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("spec", "service", "name"), ""))
	}
	if errs := utilvalidation.IsValidPortNum(int(apiService.Spec.Service.Port)); errs != nil {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "service", "port"), apiService.Spec.Service.Port, "port is not valid: "+strings.Join(errs, ", ")))
	}
	// 两者同时出现时语义自相矛盾：一个说"验证证书"，一个说"别验证证书"。
	// 与其选定一个优先级悄悄生效，不如直接拒绝，让配置意图保持清晰。
	if apiService.Spec.InsecureSkipTLSVerify && len(apiService.Spec.CABundle) > 0 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "insecureSkipTLSVerify"), apiService.Spec.InsecureSkipTLSVerify, "may not be true if caBundle is present"))
	}

	return allErrs
}

// ValidateAPIServiceUpdate 校验 APIService 的更新。
// 更新校验 = 元数据更新校验 + 完整的创建校验。Spec 里各字段之间的约束
// （比如名字必须匹配、TLS 配置互斥）在更新后同样要成立，
// 因此这里老老实实复用创建校验，没有"只校验变化字段"的捷径。
func ValidateAPIServiceUpdate(ctx context.Context, newAPIService *apiregistration.APIService, oldAPIService *apiregistration.APIService) field.ErrorList {
	allErrs := validation.ValidateObjectMetaDeclaratively(ctx, operation.Update, &newAPIService.ObjectMeta, &oldAPIService.ObjectMeta, false, nil, field.NewPath("metadata"), utilfeature.DefaultFeatureGate.Enabled(features.DeclarativeValidationBeta))
	allErrs = append(allErrs, ValidateAPIService(ctx, newAPIService)...)

	return allErrs
}

// ValidateAPIServiceStatus 校验 APIService status 的取值必须是 'True'、'False' 或 'Unknown' 之一。
// 状态由控制器写入，但依然要校验：一个拼错的状态值会让依赖"True/False/Unknown"
// 三态判断的调用方失去依据。这里只管状态取值，条件的 Type、Reason 等交给别的层。
func ValidateAPIServiceStatus(status *apiregistration.APIServiceStatus, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	for i, condition := range status.Conditions {
		if condition.Status != apiregistration.ConditionTrue &&
			condition.Status != apiregistration.ConditionFalse &&
			condition.Status != apiregistration.ConditionUnknown {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("conditions").Index(i).Child("status"), condition.Status, []string{
				string(apiregistration.ConditionTrue), string(apiregistration.ConditionFalse), string(apiregistration.ConditionUnknown)}))
		}
	}

	return allErrs
}

// ValidateAPIServiceStatusUpdate 校验 APIService status 字段的更新。
// 状态更新走的是独立子资源，因此不校验 Spec：控制器只应被允许回写状态，
// 用户也不该因为 Spec 里有错就改不了状态。
func ValidateAPIServiceStatusUpdate(ctx context.Context, newAPIService *apiregistration.APIService, oldAPIService *apiregistration.APIService) field.ErrorList {
	allErrs := validation.ValidateObjectMetaDeclaratively(ctx, operation.Update, &newAPIService.ObjectMeta, &oldAPIService.ObjectMeta, false, nil, field.NewPath("metadata"), utilfeature.DefaultFeatureGate.Enabled(features.DeclarativeValidationBeta))
	allErrs = append(allErrs, ValidateAPIServiceStatus(&newAPIService.Status, field.NewPath("status"))...)
	return allErrs
}
