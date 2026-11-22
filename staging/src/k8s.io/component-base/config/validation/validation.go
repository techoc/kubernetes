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

// Package validation 校验 component-base 配置类型的字段约束。
// 这些函数只做纯字段校验，供各组件在启动阶段统一复用。
package validation

import (
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/component-base/config"
)

// ValidateClientConnectionConfiguration 校验 ClientConnectionConfiguration，
// 当前只要求 burst 非负（负值会让限流器行为失去意义）。
func ValidateClientConnectionConfiguration(cc *config.ClientConnectionConfiguration, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if cc.Burst < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("burst"), cc.Burst, "must be non-negative"))
	}
	return allErrs
}

// ValidateLeaderElectionConfiguration 校验 LeaderElectionConfiguration。
// 未启用 leader election 时其余字段无意义，直接放行；启用时要求时间参数
// 为正且 leaseDuration 必须大于 renewDeadline，并显式指定锁资源信息。
func ValidateLeaderElectionConfiguration(cc *config.LeaderElectionConfiguration, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if !cc.LeaderElect {
		return allErrs
	}
	if cc.LeaseDuration.Duration <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("leaseDuration"), cc.LeaseDuration, "must be greater than zero"))
	}
	if cc.RenewDeadline.Duration <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("renewDeadline"), cc.RenewDeadline, "must be greater than zero"))
	}
	if cc.RetryPeriod.Duration <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("retryPeriod"), cc.RetryPeriod, "must be greater than zero"))
	}
	// 续约期限必须短于租约时长，否则锁可能在续约前就已过期。
	if cc.LeaseDuration.Duration <= cc.RenewDeadline.Duration {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("leaseDuration"), cc.RenewDeadline, "must be greater than `RenewDeadline`"))
	}
	// 锁的定位信息是启用 leader election 的必要条件。
	if len(cc.ResourceLock) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("resourceLock"), ""))
	}
	if len(cc.ResourceNamespace) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("resourceNamespace"), ""))
	}
	if len(cc.ResourceName) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("resourceName"), ""))
	}
	return allErrs
}
