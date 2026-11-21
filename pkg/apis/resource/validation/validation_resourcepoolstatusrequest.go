/*
Copyright The Kubernetes Authors.

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
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	corevalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/resource"
)

// ValidateResourcePoolStatusRequest 校验 ResourcePoolStatusRequest 的对象元数据。
func ValidateResourcePoolStatusRequest(request *resource.ResourcePoolStatusRequest) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMeta(&request.ObjectMeta, false, apimachineryvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	return allErrs
}

// ValidateResourcePoolStatusRequestUpdate 校验 ResourcePoolStatusRequest 的更新是否合法。
func ValidateResourcePoolStatusRequestUpdate(request, oldRequest *resource.ResourcePoolStatusRequest) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMetaUpdate(&request.ObjectMeta, &oldRequest.ObjectMeta, field.NewPath("metadata"))
	return allErrs
}

// ValidateResourcePoolStatusRequestStatusUpdate 校验状态更新是否合法。
func ValidateResourcePoolStatusRequestStatusUpdate(request, oldRequest *resource.ResourcePoolStatusRequest) field.ErrorList {
	allErrs := corevalidation.ValidateObjectMetaUpdate(&request.ObjectMeta, &oldRequest.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, validateResourcePoolStatusRequestStatusUpdate(request.Status, oldRequest.Status, field.NewPath("status"))...)
	return allErrs
}

// validateResourcePoolStatusRequestStatusUpdate 校验状态的不可变性与 conditions 约束。
func validateResourcePoolStatusRequestStatusUpdate(status, oldStatus *resource.ResourcePoolStatusRequestStatus, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	// 状态一旦设置即视为请求完成，此后不可变（未被 DV 覆盖）。
	if oldStatus != nil {
		allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(status, oldStatus, fldPath)...)
		return allErrs
	}

	// 新状态为空时没有需要校验的内容。
	if status == nil {
		return allErrs
	}

	// conditions 需要满足通用条件格式约束（未被 DV 覆盖）。
	allErrs = append(allErrs, metav1validation.ValidateConditions(status.Conditions, fldPath.Child("conditions"))...)

	return allErrs
}
