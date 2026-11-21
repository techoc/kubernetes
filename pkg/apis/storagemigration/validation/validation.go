/*
Copyright 2024 The Kubernetes Authors.

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

	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/storagemigration"

	metaconditions "k8s.io/apimachinery/pkg/api/meta"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
)

// ValidateStorageVersionMigration 校验 StorageVersionMigration 对象的合法性：
// 先按通用规则校验 metadata（名称必须是 DNS 子域名且不能是命名空间级对象），
// 再校验 spec.resource 中的 group/resource 字段是否符合 API 资源命名的约定。
// 返回累积的 field.ErrorList，便于一次调用报告所有错误，而不是遇到第一个错误就中断。
func ValidateStorageVersionMigration(svm *storagemigration.StorageVersionMigration) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, apivalidation.ValidateObjectMeta(&svm.ObjectMeta, false, apimachineryvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))...)

	if len(svm.Spec.Resource.Resource) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("spec", "resource", "resource"), "resource is required to be set"))
	} else {
		// 与 APIService 的校验规则保持一致：Group 必须是 DNS1123 子域名，Resource 必须是 DNS1035 标签。
		// 这样可保证迁移目标资源名与 API 发现文档中公布的名字格式一致。
		if errs := utilvalidation.IsDNS1035Label(svm.Spec.Resource.Resource); len(errs) > 0 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "resource", "resource"), svm.Spec.Resource.Resource, strings.Join(errs, ",")))
		}
	}

	if len(svm.Spec.Resource.Group) != 0 {
		if errs := utilvalidation.IsDNS1123Subdomain(svm.Spec.Resource.Group); len(errs) > 0 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec", "resource", "group"), svm.Spec.Resource.Group, strings.Join(errs, ",")))
		}
	}

	return allErrs
}

// ValidateStorageVersionMigrationUpdate 在对象更新时校验 StorageVersionMigration：
// 除了复用创建时的全量校验外，还要求 metadata 的更新符合常规规则（如 ResourceVersion 不变），
// 并且 spec 整体不可变，因为迁移任务一旦创建就不允许改变迁移目标。
func ValidateStorageVersionMigrationUpdate(newSVMBundle, oldSVMBundle *storagemigration.StorageVersionMigration) field.ErrorList {
	allErrs := ValidateStorageVersionMigration(newSVMBundle)
	allErrs = append(allErrs, apivalidation.ValidateObjectMetaUpdate(&newSVMBundle.ObjectMeta, &oldSVMBundle.ObjectMeta, field.NewPath("metadata"))...)

	// 禁止修改 spec：迁移的源和目标是任务的核心定义，变更会让已执行的迁移状态失去意义。
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(newSVMBundle.Spec, oldSVMBundle.Spec, field.NewPath("spec"))...)

	return allErrs
}

// ValidateStorageVersionMigrationStatusUpdate 只针对 status 子资源更新做校验：
// 校验 metadata 更新是否合法、resourceVersion 字段的格式与不可变性、conditions 的合法性，
// 以及成功/失败条件之间的互斥和单向转换约束，防止控制器写出自相矛盾的状态。
func ValidateStorageVersionMigrationStatusUpdate(newSVMBundle, oldSVMBundle *storagemigration.StorageVersionMigration) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&newSVMBundle.ObjectMeta, &oldSVMBundle.ObjectMeta, field.NewPath("metadata"))

	fldPath := field.NewPath("status")

	// resourceVersion 必须是合法的（非负整数）资源版本号；这里用与自身比较的方式
	// 来解析并校验其格式——若解析失败或比较结果不为相等，则说明它不是合法的版本号。
	if len(newSVMBundle.Status.ResourceVersion) != 0 {
		cmp, err := resourceversion.CompareResourceVersion(newSVMBundle.Status.ResourceVersion, newSVMBundle.Status.ResourceVersion)
		if err != nil || cmp != 0 {
			if err == nil {
				err = fmt.Errorf("unable to compare resource versions, %s is not equal to %s", newSVMBundle.Status.ResourceVersion, newSVMBundle.Status.ResourceVersion)
			}
			allErrs = append(allErrs, field.Invalid(fldPath.Child("resourceVersion"), newSVMBundle.Status.ResourceVersion, err.Error()))
		}
	}

	allErrs = append(allErrs, metav1validation.ValidateConditions(newSVMBundle.Status.Conditions, fldPath.Child("conditions"))...)

	// resourceVersion 一旦写入就不允许再修改：它是迁移实际发生的存储版本，
	// 记录的是既定事实，改变它会让历史状态与真实迁移结果不符。
	if len(oldSVMBundle.Status.ResourceVersion) != 0 && oldSVMBundle.Status.ResourceVersion != newSVMBundle.Status.ResourceVersion {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("resourceVersion"), newSVMBundle.Status.ResourceVersion, "field is immutable"))
	}

	// 成功和失败是互斥的终态，不允许同时为 true：一次迁移不可能既成功又失败。
	if isSuccessful(newSVMBundle) && isFailed(newSVMBundle) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("conditions"), newSVMBundle.Status.Conditions, "Both success and failed conditions cannot be true at the same time"))
	}

	// 成功和失败条件一旦变成 true 就不允许再回退为 false（终态不可逆），
	// 否则会出现迁移结果"翻案"，导致控制器或用户读到不一致的结论。
	isOldSuccessful := isSuccessful(oldSVMBundle)
	if isOldSuccessful && !isSuccessful(newSVMBundle) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("conditions"), newSVMBundle.Status.Conditions, "Success condition cannot be set to false once it is true"))
	}
	isOldFailed := isFailed(oldSVMBundle)
	if isOldFailed && !isFailed(newSVMBundle) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("conditions"), newSVMBundle.Status.Conditions, "Failed condition cannot be set to false once it is true"))
	}

	return allErrs
}

// isSuccessful 判断 MigrationSucceeded 条件是否被置为 True，
// 即该迁移任务的 status 中是否记录了"迁移成功"这一终态。
func isSuccessful(svm *storagemigration.StorageVersionMigration) bool {
	successCondition := metaconditions.FindStatusCondition(svm.Status.Conditions, string(storagemigration.MigrationSucceeded))
	if successCondition != nil && successCondition.Status == metav1.ConditionTrue {
		return true
	}
	return false
}

// isFailed 判断 MigrationFailed 条件是否被置为 True，
// 即该迁移任务的 status 中是否记录了"迁移失败"这一终态。
func isFailed(svm *storagemigration.StorageVersionMigration) bool {
	failedCondition := metaconditions.FindStatusCondition(svm.Status.Conditions, string(storagemigration.MigrationFailed))
	if failedCondition != nil && failedCondition.Status == metav1.ConditionTrue {
		return true
	}
	return false
}
