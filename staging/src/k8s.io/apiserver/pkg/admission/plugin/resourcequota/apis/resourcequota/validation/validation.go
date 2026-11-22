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

// Package validation 校验 ResourceQuota 准入插件的配置。
package validation

import (
	resourcequotaapi "k8s.io/apiserver/pkg/admission/plugin/resourcequota/apis/resourcequota"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidateConfiguration validates the configuration.
// 逐项检查 limitedResources，resource 字段为空时返回必填错误；
// 其余字段（如 quota 名称）允许为空，由运行时回退到默认行为。
func ValidateConfiguration(config *resourcequotaapi.Configuration) field.ErrorList {
	allErrs := field.ErrorList{}
	fldPath := field.NewPath("limitedResources")
	for i, limitedResource := range config.LimitedResources {
		idxPath := fldPath.Index(i)
		if len(limitedResource.Resource) == 0 {
			allErrs = append(allErrs, field.Required(idxPath.Child("resource"), ""))
		}
	}
	return allErrs
}
