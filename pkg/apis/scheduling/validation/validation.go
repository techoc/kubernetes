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

package validation

import (
	"fmt"
	"slices"
	"strings"

	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	schedulingapiv1 "k8s.io/kubernetes/pkg/apis/scheduling/v1"
)

// validateWorkloadName 用于检查给定的 Workload 名称是否合法。
// 复用 DNS 子域名规则，与集群中其他命名对象保持一致。
var validateWorkloadName = apimachineryvalidation.NameIsDNSSubdomain

// validateCompositePodGroupName 用于检查给定的 CompositePodGroup 名称是否合法。
// 同样采用 DNS 子域名规则。
var validateCompositePodGroupName = apimachineryvalidation.NameIsDNSSubdomain

// ValidatePriorityClass 校验 PriorityClass 中必填字段是否设置正确。
// 除了对象元数据的通用校验外，还限制系统前缀名称只能用于预定义系统优先级，
// 并约束用户自定义优先级不得越过最高可定义值。
func ValidatePriorityClass(pc *scheduling.PriorityClass) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, apivalidation.ValidateObjectMeta(&pc.ObjectMeta, false, apimachineryvalidation.NameIsDNSSubdomain, field.NewPath("metadata"))...)
	// 名称若带有系统前缀，必须是预定义的系统优先级类之一，防止用户伪造系统级别对象。
	if strings.HasPrefix(pc.Name, scheduling.SystemPriorityClassPrefix) {
		if is, err := schedulingapiv1.IsKnownSystemPriorityClass(pc.Name, pc.Value, pc.GlobalDefault); !is {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("metadata", "name"), "priority class names with '"+scheduling.SystemPriorityClassPrefix+"' prefix are reserved for system use only. error: "+err.Error()))
		}
	} else if pc.Value > scheduling.HighestUserDefinablePriority {
		// 非系统的关键优先级类不允许使用超过 HighestUserDefinablePriority 的值，
		// 以免普通用户抢占系统关键负载的调度次序。
		allErrs = append(allErrs, field.Forbidden(field.NewPath("value"), fmt.Sprintf("maximum allowed value of a user defined priority is %v", scheduling.HighestUserDefinablePriority)))
	}
	if pc.PreemptionPolicy != nil {
		allErrs = append(allErrs, apivalidation.ValidatePreemptionPolicy(pc.PreemptionPolicy, field.NewPath("preemptionPolicy"))...)
	}
	return allErrs
}

// ValidatePriorityClassUpdate 校验更新后的 PriorityClass 必填字段是否合法。
// PriorityClass 不允许修改 name、value 和 preemptionPolicy。
func ValidatePriorityClassUpdate(pc, oldPc *scheduling.PriorityClass) field.ErrorList {
	// name 不可变，由 ObjectMeta 校验器负责检查。
	allErrs := apivalidation.ValidateObjectMetaUpdate(&pc.ObjectMeta, &oldPc.ObjectMeta, field.NewPath("metadata"))
	// value 不可变，避免运行中改变已调度 Pod 的优先级语义。
	if pc.Value != oldPc.Value {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("value"), "may not be changed in an update."))
	}
	// preemptionPolicy 不可变。
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(pc.PreemptionPolicy, oldPc.PreemptionPolicy, field.NewPath("preemptionPolicy"))...)
	return allErrs
}

// ValidatePodGroup 校验 PodGroup 中所有字段是否设置正确。
func ValidatePodGroup(podGroup *scheduling.PodGroup) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&podGroup.ObjectMeta, true, apivalidation.ValidatePodGroupName, field.NewPath("metadata"))
	// 在 workloadRef 上的 +k8s:dependentRequired 标记从 Alpha 毕业到 Standard
	// 并被声明式规则完整执行之前，需要手工校验这一依赖关系。
	if podGroup.Spec.ParentCompositePodGroupName != nil && podGroup.Spec.WorkloadRef == nil {
		allErrs = append(allErrs, field.Required(field.NewPath("spec", "workloadRef"), "must be set when parentCompositePodGroupName is set").WithOrigin("dependentRequired").MarkCoveredByDeclarative())
	}
	return allErrs
}

// ValidatePodGroupUpdate 校验对 PodGroup 的更新是否合法。
func ValidatePodGroupUpdate(podGroup, oldPodGroup *scheduling.PodGroup) field.ErrorList {
	return apivalidation.ValidateObjectMetaUpdate(&podGroup.ObjectMeta, &oldPodGroup.ObjectMeta, field.NewPath("metadata"))
}

// ValidateWorkload 校验 Workload 中所有字段是否设置正确。
func ValidateWorkload(workload *scheduling.Workload) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&workload.ObjectMeta, true, validateWorkloadName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateWorkloadSpec(&workload.Spec, field.NewPath("spec"))...)
	return allErrs
}

// ValidateWorkloadUpdate 校验对 Workload 的更新是否合法。
// spec 在更新时同样要重新校验，防止绕过创建期的约束。
func ValidateWorkloadUpdate(workload, oldWorkload *scheduling.Workload) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&workload.ObjectMeta, &oldWorkload.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateWorkloadSpec(&workload.Spec, field.NewPath("spec"))...)
	return allErrs
}

// ValidateWorkloadSpec 校验以下条件是否全部满足：
// - 所有模板名称唯一。
// - CompositePodGroupTemplates 树的深度不超过 4。
// - 层级中所有模板共享相同的 priority 和 PriorityClassName。
// - 层级中所有 composite pod group 模板至少有一个子模板。
func ValidateWorkloadSpec(spec *scheduling.WorkloadSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, validateWorkloadTemplateNamesUniqueness(spec, fldPath)...)
	allErrs = append(allErrs, validateWorkloadTemplatesDepth(spec, fldPath.Child("compositePodGroupTemplates"))...)
	allErrs = append(allErrs, validateWorkloadPriority(spec, fldPath)...)
	for i, cpgt := range spec.CompositePodGroupTemplates {
		allErrs = append(allErrs, validateCompositePodGroupTemplateHasAtLeastOneChild(&cpgt, fldPath.Child("compositePodGroupTemplates").Index(i))...)
	}
	return allErrs
}

// validateWorkloadTemplateNamesUniqueness 检查 Workload 中所有模板名称是否唯一。
// 这里必须小心：声明式校验已经会对同一列表中的重名报错，不能重复产生同类错误，
// 因此本函数只负责跨列表、跨层级的名称冲突。
func validateWorkloadTemplateNamesUniqueness(spec *scheduling.WorkloadSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	names := sets.New[string]()
	for _, pg := range spec.PodGroupTemplates {
		names.Insert(pg.Name)
	}

	allErrs = append(allErrs, validateCompositePodGroupTemplateListNames(fldPath, spec.CompositePodGroupTemplates, names)...)
	return allErrs
}

// validateCompositePodGroupTemplateNames 校验单个 CompositePodGroupTemplate 内子模板名称，
// 并检查这些名称是否与已收集到的其他模板名称冲突，然后递归处理更深层级的模板。
func validateCompositePodGroupTemplateNames(cpg *scheduling.CompositePodGroupTemplate, fldPath *field.Path, names sets.Set[string]) field.ErrorList {
	var allErrs field.ErrorList
	listNames := sets.New[string]()

	for i, child := range cpg.PodGroupTemplates {
		childPath := fldPath.Child("podGroupTemplates").Index(i)
		if names.Has(child.Name) {
			allErrs = append(allErrs, field.Duplicate(childPath.Child("name"), child.Name))
		}
		listNames.Insert(child.Name)
	}
	for name := range listNames {
		names.Insert(name)
	}

	allErrs = append(allErrs, validateCompositePodGroupTemplateListNames(fldPath, cpg.CompositePodGroupTemplates, names)...)
	return allErrs
}

// validateCompositePodGroupTemplateListNames 校验一个 CompositePodGroupTemplate 列表中名称的唯一性。
// 先做一轮检查确认 CPG 名称没有跨列表重复，再递归进入每个 CPG 的子模板。
func validateCompositePodGroupTemplateListNames(fldPath *field.Path, templates []scheduling.CompositePodGroupTemplate, names sets.Set[string]) field.ErrorList {
	var allErrs field.ErrorList
	listNames := sets.New[string]()

	// 第一轮：检查 CPG 名称是否跨列表重复，避免同一名称在不同分支被复用。
	for i, child := range templates {
		childPath := fldPath.Child("compositePodGroupTemplates").Index(i)
		if names.Has(child.Name) {
			allErrs = append(allErrs, field.Duplicate(childPath.Child("name"), child.Name))
		}
		listNames.Insert(child.Name)
	}
	for name := range listNames {
		names.Insert(name)
	}

	for i, child := range templates {
		allErrs = append(allErrs, validateCompositePodGroupTemplateNames(&child, fldPath.Child("compositePodGroupTemplates").Index(i), names)...)
	}
	return allErrs
}

// validateWorkloadTemplatesDepth 递归检测模板层级深度是否超过 WorkloadMaxTemplateDepth。
// walk 返回 false 表示已经越界，调用方据此生成错误。
func validateWorkloadTemplatesDepth(spec *scheduling.WorkloadSpec, fldPath *field.Path) field.ErrorList {
	var walk func(*scheduling.CompositePodGroupTemplate, int) bool
	walk = func(cpg *scheduling.CompositePodGroupTemplate, depth int) bool {
		if depth == scheduling.WorkloadMaxTemplateDepth {
			return false
		}
		for _, child := range cpg.CompositePodGroupTemplates {
			if !walk(&child, depth+1) {
				return false
			}
		}
		return true
	}

	var allErrs field.ErrorList
	for i, cpg := range spec.CompositePodGroupTemplates {
		if !walk(&cpg, 1) {
			allErrs = append(allErrs, field.Invalid(fldPath.Index(i), cpg.CompositePodGroupTemplates, fmt.Sprintf("maximum template hierarchy depth is %d", scheduling.WorkloadMaxTemplateDepth)))
		}
	}
	return allErrs
}

// validateWorkloadPriority 确保 Workload 层级中所有模板使用相同的优先级配置。
// checkPriority 以第一个模板为基准，后续只要名称、优先级其中之一不一致就报错；
// 这样可避免同一工作负载内出现相互矛盾的调度优先级。
func validateWorkloadPriority(spec *scheduling.WorkloadSpec, fldPath *field.Path) field.ErrorList {
	var firstClassName *string
	var firstPriority *int32
	var hasFirst bool

	checkPriority := func(className string, priority *int32) bool {
		if !hasFirst {
			firstClassName = &className
			firstPriority = priority
			hasFirst = true
			return true
		}
		if *firstClassName != className {
			return false
		}
		if (firstPriority == nil) != (priority == nil) {
			return false
		}
		if firstPriority != nil && priority != nil && *firstPriority != *priority {
			return false
		}
		return true
	}

	for _, pg := range spec.PodGroupTemplates {
		if !checkPriority(pg.PriorityClassName, pg.Priority) {
			return field.ErrorList{field.Invalid(fldPath, nil, "detected multiple priority configurations")}
		}
	}

	var checkCpg func(cpg *scheduling.CompositePodGroupTemplate) bool
	checkCpg = func(cpg *scheduling.CompositePodGroupTemplate) bool {
		if !checkPriority(cpg.PriorityClassName, cpg.Priority) {
			return false
		}
		for _, child := range cpg.CompositePodGroupTemplates {
			if !checkCpg(&child) {
				return false
			}
		}
		for _, child := range cpg.PodGroupTemplates {
			if !checkPriority(child.PriorityClassName, child.Priority) {
				return false
			}
		}
		return true
	}

	for _, cpg := range spec.CompositePodGroupTemplates {
		if !checkCpg(&cpg) {
			return field.ErrorList{field.Invalid(fldPath, nil, "all priority and priorityClassName values must be identical, detected multiple values")}
		}
	}

	return nil
}

// validateCompositePodGroupTemplateHasAtLeastOneChild 校验每个 CompositePodGroupTemplate
// 至少包含一个子 PodGroupTemplate 或 CompositePodGroupTemplate，并递归检查所有子 CPG。
func validateCompositePodGroupTemplateHasAtLeastOneChild(cpgt *scheduling.CompositePodGroupTemplate, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(cpgt.PodGroupTemplates) == 0 && len(cpgt.CompositePodGroupTemplates) == 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, cpgt.Name, "must have at least one child PodGroupTemplate or CompositePodGroupTemplate"))
	}
	for i, child := range cpgt.CompositePodGroupTemplates {
		allErrs = append(allErrs, validateCompositePodGroupTemplateHasAtLeastOneChild(&child, fldPath.Child("compositePodGroupTemplates").Index(i))...)
	}
	return allErrs
}

// ValidatePodGroupStatusUpdate 校验对 PodGroup 状态的更新是否合法。
// 状态更新同样要校验 metadata，并检查 conditions 与 resourceClaimStatuses。
func ValidatePodGroupStatusUpdate(podGroup, oldPodGroup *scheduling.PodGroup) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&podGroup.ObjectMeta, &oldPodGroup.ObjectMeta, field.NewPath("metadata"))
	fldPath := field.NewPath("status")
	allErrs = append(allErrs, metav1validation.ValidateConditions(podGroup.Status.Conditions, fldPath.Child("conditions"))...)
	allErrs = append(allErrs, validatePodGroupResourceClaimStatuses(podGroup.Status.ResourceClaimStatuses, podGroup.Spec.ResourceClaims, fldPath.Child("resourceClaimStatuses"))...)
	return allErrs
}

// validatePodGroupResourceClaimStatuses 校验资源声明状态中的名称必须对应 spec.resourceClaims 中的条目。
// 这里无需校验名称内容本身，只要能与声明条目匹配即为合法，否则直接拒绝。
func validatePodGroupResourceClaimStatuses(statuses []scheduling.PodGroupResourceClaimStatus, podGroupClaims []scheduling.PodGroupResourceClaim, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	for i, status := range statuses {
		idxPath := fldPath.Index(i)
		// 名称内容无须单独校验：能匹配到条目即合法，否则在这里拒绝。
		if !havePodGroupClaim(podGroupClaims, status.Name) {
			allErrs = append(allErrs, field.Invalid(idxPath.Child("name"), status.Name, "must match the name of an entry in `spec.resourceClaims`"))
		}
	}

	return allErrs
}

// havePodGroupClaim 判断给定名称是否存在于 PodGroup 的资源声明列表中。
func havePodGroupClaim(podGroupClaims []scheduling.PodGroupResourceClaim, name string) bool {
	return slices.ContainsFunc(podGroupClaims, func(podGroupClaim scheduling.PodGroupResourceClaim) bool {
		return podGroupClaim.Name == name
	})
}

// ValidateCompositePodGroup 校验 CompositePodGroup 中所有字段是否设置正确。
func ValidateCompositePodGroup(compositePodGroup *scheduling.CompositePodGroup) field.ErrorList {
	return apivalidation.ValidateObjectMeta(&compositePodGroup.ObjectMeta, true, validateCompositePodGroupName, field.NewPath("metadata"))
}

// ValidateCompositePodGroupUpdate 校验对 CompositePodGroup 的更新是否合法。
func ValidateCompositePodGroupUpdate(compositePodGroup, oldCompositePodGroup *scheduling.CompositePodGroup) field.ErrorList {
	return apivalidation.ValidateObjectMetaUpdate(&compositePodGroup.ObjectMeta, &oldCompositePodGroup.ObjectMeta, field.NewPath("metadata"))
}

// ValidateCompositePodGroupStatusUpdate 校验对 CompositePodGroup 状态的更新是否合法。
func ValidateCompositePodGroupStatusUpdate(compositePodGroup, oldCompositePodGroup *scheduling.CompositePodGroup) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&compositePodGroup.ObjectMeta, &oldCompositePodGroup.ObjectMeta, field.NewPath("metadata"))
	fldPath := field.NewPath("status")
	allErrs = append(allErrs, metav1validation.ValidateConditions(compositePodGroup.Status.Conditions, fldPath.Child("conditions"))...)
	return allErrs
}
