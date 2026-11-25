/*
Copyright 2020 The Kubernetes Authors.

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

// Package validation 校验内部版本 metav1.ListOptions 的字段组合。
//
// 内部版本不直接对外暴露，但 list/watch 请求最终都会转换到它，因此这里的
// 规则是 API 行为的最终防线。
package validation

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// watch 与普通 list 对 resourceVersionMatch / sendInitialEvents 的规则完全不同，
// 因此先分流到 validateWatchOptions。
func ValidateListOptions(options *internalversion.ListOptions, isWatchListFeatureEnabled bool) field.ErrorList {
	if options.Watch {
		return validateWatchOptions(options, isWatchListFeatureEnabled)
	}
	allErrs := field.ErrorList{}
	if match := options.ResourceVersionMatch; len(match) > 0 {
		// resourceVersionMatch 只在指定 resourceVersion 时才有意义。
		if len(options.ResourceVersion) == 0 {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("resourceVersionMatch"), "resourceVersionMatch is forbidden unless resourceVersion is provided"))
		}
		// 分页（continue）与一致性匹配语义冲突，不允许同时使用。
		if len(options.Continue) > 0 {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("resourceVersionMatch"), "resourceVersionMatch is forbidden when continue is provided"))
		}
		if match != metav1.ResourceVersionMatchExact && match != metav1.ResourceVersionMatchNotOlderThan {
			allErrs = append(allErrs, field.NotSupported(field.NewPath("resourceVersionMatch"), match, []string{string(metav1.ResourceVersionMatchExact), string(metav1.ResourceVersionMatchNotOlderThan), ""}))
		}
		// resourceVersion "0" 表示任意版本，与 exact 的“精确匹配”矛盾。
		if match == metav1.ResourceVersionMatchExact && options.ResourceVersion == "0" {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("resourceVersionMatch"), "resourceVersionMatch \"exact\" is forbidden for resourceVersion \"0\""))
		}
	}
	// sendInitialEvents 仅对 watch 有意义，list 请求中必须为空。
	if options.SendInitialEvents != nil {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("sendInitialEvents"), "sendInitialEvents is forbidden for list"))
	}
	return allErrs
}

// validateWatchOptions 校验 watch 场景下的选项组合：sendInitialEvents 依赖
// NotOlderThan 语义与 WatchList 特性门控；resourceVersionMatch 又依赖
// sendInitialEvents，两者互为前提。
func validateWatchOptions(options *internalversion.ListOptions, isWatchListFeatureEnabled bool) field.ErrorList {
	allErrs := field.ErrorList{}
	match := options.ResourceVersionMatch
	if options.SendInitialEvents != nil {
		// 初始事件列表要求“不早于”语义，其他匹配模式没有定义。
		if match != metav1.ResourceVersionMatchNotOlderThan {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("resourceVersionMatch"), fmt.Sprintf("sendInitialEvents requires setting resourceVersionMatch to %s", metav1.ResourceVersionMatchNotOlderThan)))
		}
		// 功能门控关闭时即使组合合法也要拒绝，避免客户端提前依赖。
		if !isWatchListFeatureEnabled {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("sendInitialEvents"), "sendInitialEvents is forbidden for watch unless the WatchList feature gate is enabled"))
		}
	}
	if len(match) > 0 {
		// watch 中只有配合 sendInitialEvents 才允许设置 resourceVersionMatch。
		if options.SendInitialEvents == nil {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("resourceVersionMatch"), "resourceVersionMatch is forbidden for watch unless sendInitialEvents is provided"))
		}
		if match != metav1.ResourceVersionMatchNotOlderThan {
			allErrs = append(allErrs, field.NotSupported(field.NewPath("resourceVersionMatch"), match, []string{string(metav1.ResourceVersionMatchNotOlderThan)}))
		}
		if len(options.Continue) > 0 {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("resourceVersionMatch"), "resourceVersionMatch is forbidden when continue is provided"))
		}
	}
	return allErrs
}
