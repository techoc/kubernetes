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
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	unversionedvalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/core"
	corevalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/node"
	"regexp"
)

// 定义归一化规则，用来处理不同 API 版本之间的字段名差异：
// v1alpha1 把字段嵌套在 "spec" 下（如 "spec.runtimeHandler"、"spec.scheduling"），
// 而 v1/v1beta1 把它们暴露在顶层（"handler"、"scheduling"），
// 与手写校验使用的内部类型保持一致。这样同一套声明式校验规则可以复用于各版本。
var NodeNormalizationRules = []field.NormalizationRule{
	{
		Regexp:      regexp.MustCompile(`^spec\.runtimeHandler(.*)$`),
		Replacement: "handler$1",
	},
	{
		Regexp:      regexp.MustCompile(`^spec\.scheduling(.*)$`),
		Replacement: "scheduling$1",
	},
}

// ValidateRuntimeClass 校验 RuntimeClass 对象：
// 包括元数据、必填且符合 DNS Label 格式的 handler、可选的 overhead 与 scheduling。
func ValidateRuntimeClass(rc *node.RuntimeClass) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&rc.ObjectMeta, false, apivalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	// handler 必填，且必须是合法的 DNS Label（小写短名称），
	// 因为它会作为 CRI 运行时句柄使用，命名约束比对象名更严格。
	if rc.Handler == "" {
		allErrs = append(allErrs, field.Required(field.NewPath("handler"), "").MarkCoveredByDeclarative())
	} else {
		for _, msg := range apivalidation.NameIsDNSLabel(rc.Handler, false) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("handler"), rc.Handler, msg).MarkCoveredByDeclarative().WithOrigin("format=k8s-short-name"))
		}
	}

	// overhead/scheduling 是可选字段，只有非空时才做进一步校验。
	if rc.Overhead != nil {
		allErrs = append(allErrs, validateOverhead(rc.Overhead, field.NewPath("overhead"))...)
	}
	if rc.Scheduling != nil {
		allErrs = append(allErrs, validateScheduling(rc.Scheduling, field.NewPath("scheduling"))...)
	}

	return allErrs
}

// ValidateRuntimeClassUpdate 校验 RuntimeClass 的更新：
// 元数据按常规规则更新，但 handler 一旦创建便不可变更，
// 因为运行中 Pod 的运行时由 handler 决定，变更会导致语义不一致。
func ValidateRuntimeClassUpdate(new, old *node.RuntimeClass) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&new.ObjectMeta, &old.ObjectMeta, field.NewPath("metadata"))

	allErrs = append(allErrs, apivalidation.ValidateImmutableField(new.Handler, old.Handler, field.NewPath("handler")).MarkCoveredByDeclarative().WithOrigin("immutable")...)

	return allErrs
}

// validateOverhead 校验 Pod 运行时的固定资源开销。
func validateOverhead(overhead *node.Overhead, fldPath *field.Path) field.ErrorList {
	// 复用 ResourceRequirements 的校验逻辑，
	// 把 PodFixed 当作 Limits 校验，保证资源名称与数量格式与容器资源一致。
	return corevalidation.ValidateContainerResourceRequirements(&core.ResourceRequirements{Limits: overhead.PodFixed}, nil, fldPath,
		corevalidation.PodValidationOptions{})
}

// validateScheduling 校验调度约束：nodeSelector 的标签格式与 tolerations 的合法性。
func validateScheduling(s *node.Scheduling, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if s.NodeSelector != nil {
		allErrs = append(allErrs, unversionedvalidation.ValidateLabels(s.NodeSelector, fldPath.Child("nodeSelector"))...)
	}
	allErrs = append(allErrs, validateTolerations(s.Tolerations, fldPath.Child("tolerations"))...)
	return allErrs
}

// validateTolerations 校验 toleration 列表本身，并额外检查重复项：
// 准入阶段追加 RuntimeClass 的 toleration 时会去重，
// 存储层也要求列表键唯一，避免歧义。
func validateTolerations(tolerations []core.Toleration, fldPath *field.Path) field.ErrorList {
	allErrs := corevalidation.ValidateTolerations(tolerations, fldPath, corevalidation.PodValidationOptions{}, corevalidation.KeyFormatCovered)
	// 确保 toleration 唯一，重复项会导致准入阶段合并时语义不确定。
	tolerationSet := map[core.Toleration]bool{}
	for i, t := range tolerations {
		// listKey 只包含 API 中定义为列表键（listKeys）的字段，
		// TolerationSeconds 不参与重复性判断，因此两个仅时长不同的 toleration 仍视为重复。
		listKey := core.Toleration{
			Key:      t.Key,
			Operator: t.Operator,
			Value:    t.Value,
			Effect:   t.Effect,
		}
		if tolerationSet[listKey] {
			allErrs = append(allErrs, field.Duplicate(fldPath.Index(i), t))
		} else {
			tolerationSet[listKey] = true
		}
	}
	return allErrs
}
