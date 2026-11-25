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
	"encoding/json"
	"fmt"
	"strconv"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unversionedvalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/apps"
	api "k8s.io/kubernetes/pkg/apis/core"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
)

// StatefulSetValidationOptions 是可以传入 ValidateStatefulSetSpec 的结构体，用于记录校验选项
type StatefulSetValidationOptions struct {
	// 允许非合法的 DNS1123 ServiceName
	AllowInvalidServiceName bool
	// 跳过对 Pod 模板规格的校验，用于 StatefulSet 更新场景
	SkipValidatePodTemplateSpec bool
	// 跳过对 volume claim 模板的校验，用于 StatefulSet 更新场景
	SkipValidateVolumeClaimTemplates bool
	// 允许将 StatefulSet 的更新策略设置为 Recreate
	AllowStatefulSetRecreateStrategy bool
}

// ValidateStatefulSetName 可用于检查给定的 StatefulSet 名称是否合法。
// Prefix 表示该名称将作为生成名字的一部分使用，此时
// 允许结尾的连字符。
func ValidateStatefulSetName(name string, prefix bool) []string {
	// TODO: 校验是否有空间容纳 Pod 插入的后缀。
	// 目前这个后缀只是 "-index"。未来我们可能允许用户
	// 指定一组后缀列表，届时需要校验最长的那个。
	return apimachineryvalidation.NameIsDNSLabel(name, prefix)
}

// ValidatePodTemplateSpecForStatefulSet 校验给定的模板，确保其与期望的 selector 一致。
func ValidatePodTemplateSpecForStatefulSet(template *api.PodTemplateSpec, selector labels.Selector, fldPath *field.Path, opts apivalidation.PodValidationOptions,
	setOpts StatefulSetValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if template == nil {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	} else {
		if !selector.Empty() {
			// 校验 StatefulSet 的 selector 与模板中的 labels 相匹配。
			labels := labels.Set(template.Labels)
			if !selector.Matches(labels) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("metadata", "labels"), template.Labels, "`selector` does not match template `labels`"))
			}
		}
		if !setOpts.SkipValidatePodTemplateSpec {
			allErrs = append(allErrs, apivalidation.ValidatePodTemplateSpec(template, fldPath, opts)...)
		}
		allErrs = append(allErrs, unversionedvalidation.ValidateLabels(template.Labels, fldPath.Child("labels"))...)
		allErrs = append(allErrs, apivalidation.ValidateAnnotations(template.Annotations, fldPath.Child("annotations"))...)
		allErrs = append(allErrs, apivalidation.ValidatePodSpecificAnnotations(template.Annotations, &template.Spec, fldPath.Child("annotations"), opts)...)
	}
	return allErrs
}

func ValidatePersistentVolumeClaimRetentionPolicyType(policy apps.PersistentVolumeClaimRetentionPolicyType, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	switch policy {
	case apps.RetainPersistentVolumeClaimRetentionPolicyType:
	case apps.DeletePersistentVolumeClaimRetentionPolicyType:
	default:
		allErrs = append(allErrs, field.NotSupported(fldPath, policy, []string{string(apps.RetainPersistentVolumeClaimRetentionPolicyType), string(apps.DeletePersistentVolumeClaimRetentionPolicyType)}))
	}
	return allErrs
}

func ValidatePersistentVolumeClaimRetentionPolicy(policy *apps.StatefulSetPersistentVolumeClaimRetentionPolicy, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if policy != nil {
		allErrs = append(allErrs, ValidatePersistentVolumeClaimRetentionPolicyType(policy.WhenDeleted, fldPath.Child("whenDeleted"))...)
		allErrs = append(allErrs, ValidatePersistentVolumeClaimRetentionPolicyType(policy.WhenScaled, fldPath.Child("whenScaled"))...)
	}
	return allErrs
}

func volumesToAddForTemplates(spec *apps.StatefulSetSpec) map[string]api.Volume {
	volumes := make(map[string]api.Volume)
	templates := spec.VolumeClaimTemplates
	for i := range templates {
		volumes[templates[i].Name] = api.Volume{
			Name: templates[i].Name,
			VolumeSource: api.VolumeSource{
				PersistentVolumeClaim: &api.PersistentVolumeClaimVolumeSource{
					ClaimName: templates[i].Name,
				},
			},
		}
	}
	return volumes
}

func validateVolumeClaimTemplates(volumeClaimTemplates []api.PersistentVolumeClaim, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	opts := apivalidation.ValidationOptionsForPersistentVolumeClaimCreate()
	for i, pvc := range volumeClaimTemplates {
		allErrs = append(allErrs, apivalidation.ValidatePersistentVolumeClaimSpec(&pvc.Spec, fldPath.Index(i).Child("spec"), opts)...)
	}
	return allErrs
}

// ValidateStatefulSetSpec 测试 StatefulSet spec 中的必填字段是否已设置。
func ValidateStatefulSetSpec(spec *apps.StatefulSetSpec, fldPath *field.Path, opts apivalidation.PodValidationOptions, setOpts StatefulSetValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	switch spec.PodManagementPolicy {
	case "":
		allErrs = append(allErrs, field.Required(fldPath.Child("podManagementPolicy"), ""))
	case apps.OrderedReadyPodManagement, apps.ParallelPodManagement:
	default:
		allErrs = append(allErrs, field.Invalid(fldPath.Child("podManagementPolicy"), spec.PodManagementPolicy, fmt.Sprintf("must be '%s' or '%s'", apps.OrderedReadyPodManagement, apps.ParallelPodManagement)))
	}

	switch {
	case spec.UpdateStrategy.Type == "":
		allErrs = append(allErrs, field.Required(fldPath.Child("updateStrategy"), ""))
	case spec.UpdateStrategy.Type == apps.OnDeleteStatefulSetStrategyType:
		if spec.UpdateStrategy.RollingUpdate != nil {
			allErrs = append(
				allErrs,
				field.Invalid(
					fldPath.Child("updateStrategy").Child("rollingUpdate"),
					spec.UpdateStrategy.RollingUpdate,
					fmt.Sprintf("only allowed for updateStrategy '%s'", apps.RollingUpdateStatefulSetStrategyType)))
		}
	case spec.UpdateStrategy.Type == apps.RollingUpdateStatefulSetStrategyType:
		if spec.UpdateStrategy.RollingUpdate != nil {
			allErrs = append(allErrs, validateRollingUpdateStatefulSet(spec.UpdateStrategy.RollingUpdate, fldPath.Child("updateStrategy", "rollingUpdate"))...)
		}
	case spec.UpdateStrategy.Type == apps.RecreateStatefulSetStrategyType && setOpts.AllowStatefulSetRecreateStrategy:
		if spec.UpdateStrategy.RollingUpdate != nil {
			allErrs = append(
				allErrs,
				field.Invalid(
					fldPath.Child("updateStrategy").Child("rollingUpdate"),
					spec.UpdateStrategy.RollingUpdate,
					fmt.Sprintf("only allowed for updateStrategy '%s'", apps.RollingUpdateStatefulSetStrategyType)))
		}
	default:
		validValues := fmt.Sprintf("must be '%s' or '%s'", apps.RollingUpdateStatefulSetStrategyType, apps.OnDeleteStatefulSetStrategyType)
		if setOpts.AllowStatefulSetRecreateStrategy {
			validValues = fmt.Sprintf("must be '%s', '%s', or '%s'", apps.RollingUpdateStatefulSetStrategyType, apps.OnDeleteStatefulSetStrategyType, apps.RecreateStatefulSetStrategyType)
		}
		allErrs = append(allErrs, field.Invalid(fldPath.Child("updateStrategy"), spec.UpdateStrategy, validValues))
	}

	allErrs = append(allErrs, ValidatePersistentVolumeClaimRetentionPolicy(spec.PersistentVolumeClaimRetentionPolicy, fldPath.Child("persistentVolumeClaimRetentionPolicy"))...)
	if !setOpts.SkipValidateVolumeClaimTemplates {
		allErrs = append(allErrs, validateVolumeClaimTemplates(spec.VolumeClaimTemplates, fldPath.Child("volumeClaimTemplates"))...)
	}

	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(spec.Replicas), fldPath.Child("replicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(spec.MinReadySeconds), fldPath.Child("minReadySeconds"))...)
	if spec.Ordinals != nil {
		replicaStartOrdinal := spec.Ordinals.Start
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(replicaStartOrdinal), fldPath.Child("ordinals.start"))...)
	}

	if !setOpts.AllowInvalidServiceName && len(spec.ServiceName) > 0 {
		allErrs = append(allErrs, apivalidation.ValidateDNS1123Label(spec.ServiceName, fldPath.Child("serviceName"))...)
	}

	if spec.Selector == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("selector"), "").MarkCoveredByDeclarative())
	} else {
		// 严格校验 selector，spec.selector 一直是必填项，才能通过下面的 LabelSelectorAsSelector
		allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(spec.Selector, unversionedvalidation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: false}, fldPath.Child("selector"))...)
		if len(spec.Selector.MatchLabels)+len(spec.Selector.MatchExpressions) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("selector"), spec.Selector, "empty selector is invalid for statefulset"))
		}
	}

	selector, err := metav1.LabelSelectorAsSelector(spec.Selector)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("selector"), spec.Selector, ""))
	} else {
		templateToValidate := &spec.Template
		if len(spec.Template.Spec.Subdomain) > 0 || len(spec.Template.Spec.Hostname) > 0 || len(spec.VolumeClaimTemplates) > 0 {
			templateToValidate = templateToValidate.DeepCopy()
			// 在 initIdentity 中会被控制器覆盖为 spec.ServiceName，因此不校验
			templateToValidate.Spec.Subdomain = ""
			// 在 initIdentity 中会被控制器覆盖为 pod.Name，因此不校验
			templateToValidate.Spec.Hostname = ""
			if len(spec.VolumeClaimTemplates) > 0 {
				templateVolumes := volumesToAddForTemplates(spec)
				newVolumes := make([]api.Volume, 0, len(templateVolumes))
				for _, v := range templateVolumes {
					newVolumes = append(newVolumes, v)
				}
				for _, v := range templateToValidate.Spec.Volumes {
					if _, ok := templateVolumes[v.Name]; !ok {
						newVolumes = append(newVolumes, v)
					}
				}
				templateToValidate.Spec.Volumes = newVolumes
			}
		}
		allErrs = append(allErrs, ValidatePodTemplateSpecForStatefulSet(templateToValidate, selector, fldPath.Child("template"), opts, setOpts)...)
	}

	if spec.Template.Spec.RestartPolicy != api.RestartPolicyAlways {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("template", "spec", "restartPolicy"), spec.Template.Spec.RestartPolicy, []string{string(api.RestartPolicyAlways)}))
	}
	if spec.Template.Spec.ActiveDeadlineSeconds != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("template", "spec", "activeDeadlineSeconds"), "activeDeadlineSeconds in StatefulSet is not Supported"))
	}

	return allErrs
}

// ValidateStatefulSet 校验一个 StatefulSet。
func ValidateStatefulSet(statefulSet *apps.StatefulSet, setOpts StatefulSetValidationOptions, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&statefulSet.ObjectMeta, true, ValidateStatefulSetName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateStatefulSetSpec(&statefulSet.Spec, field.NewPath("spec"), opts, setOpts)...)
	return allErrs
}

// ValidateStatefulSetUpdate 测试 StatefulSet 中的必填字段是否已设置。
func ValidateStatefulSetUpdate(statefulSet, oldStatefulSet *apps.StatefulSet, setOpts StatefulSetValidationOptions, opts apivalidation.PodValidationOptions) field.ErrorList {
	// 首先校验新的 statefulset 是合法的。不要调用
	// ValidateStatefulSet()，因为我们不希望在更新时重新校验名称。
	// 这里很重要，因为我们过去允许名称使用 DNS subdomain，
	// 但这种名称实际上无法创建 Pod。唯一合理的做法是删除这样的实例，
	// 但如果存在 finalizer，它需要通过更新校验。而且名称本来就不能更改。
	allErrs := apivalidation.ValidateObjectMetaUpdate(&statefulSet.ObjectMeta, &oldStatefulSet.ObjectMeta, field.NewPath("metadata"))

	// 为了兼容已存在的 sts，我们选择跳过旧 sts 的 podTemplateSpec 的校验错误。
	if len(ValidateStatefulSetSpec(&oldStatefulSet.Spec, nil, opts, setOpts)) > 0 {
		setOpts.SkipValidatePodTemplateSpec = true
	}
	specPath := field.NewPath("spec")
	allErrs = append(allErrs, ValidateStatefulSetSpec(&statefulSet.Spec, specPath, opts, setOpts)...)

	allErrs = append(allErrs, apivalidation.ValidateImmutableField(statefulSet.Spec.Selector, oldStatefulSet.Spec.Selector, specPath.Child("selector")).WithOrigin("immutable").MarkCoveredByDeclarative()...)
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(statefulSet.Spec.VolumeClaimTemplates, oldStatefulSet.Spec.VolumeClaimTemplates, specPath.Child("volumeClaimTemplates")).WithOrigin("immutable").MarkCoveredByDeclarative()...)
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(statefulSet.Spec.ServiceName, oldStatefulSet.Spec.ServiceName, specPath.Child("serviceName")).WithOrigin("immutable").MarkCoveredByDeclarative()...)
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(statefulSet.Spec.PodManagementPolicy, oldStatefulSet.Spec.PodManagementPolicy, specPath.Child("podManagementPolicy")).WithOrigin("immutable").MarkCoveredByDeclarative()...)

	return allErrs
}

// ValidateStatefulSetStatus 校验一个 StatefulSetStatus。
func ValidateStatefulSetStatus(status *apps.StatefulSetStatus, fieldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.Replicas), fieldPath.Child("replicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.ReadyReplicas), fieldPath.Child("readyReplicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.CurrentReplicas), fieldPath.Child("currentReplicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.UpdatedReplicas), fieldPath.Child("updatedReplicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.AvailableReplicas), fieldPath.Child("availableReplicas"))...)
	if status.ObservedGeneration != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*status.ObservedGeneration), fieldPath.Child("observedGeneration"))...)
	}
	if status.CollisionCount != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*status.CollisionCount), fieldPath.Child("collisionCount"))...)
	}

	msg := "cannot be greater than status.replicas"
	if status.ReadyReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(fieldPath.Child("readyReplicas"), status.ReadyReplicas, msg))
	}
	if status.CurrentReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(fieldPath.Child("currentReplicas"), status.CurrentReplicas, msg))
	}
	if status.UpdatedReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(fieldPath.Child("updatedReplicas"), status.UpdatedReplicas, msg))
	}
	if status.AvailableReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(fieldPath.Child("availableReplicas"), status.AvailableReplicas, msg))
	}
	if status.AvailableReplicas > status.ReadyReplicas {
		allErrs = append(allErrs, field.Invalid(fieldPath.Child("availableReplicas"), status.AvailableReplicas, "cannot be greater than status.readyReplicas"))
	}
	return allErrs
}

// ValidateStatefulSetStatusUpdate 测试 StatefulSet 中的必填字段是否已设置。
func ValidateStatefulSetStatusUpdate(statefulSet, oldStatefulSet *apps.StatefulSet) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateStatefulSetStatus(&statefulSet.Status, field.NewPath("status"))...)
	allErrs = append(allErrs, apivalidation.ValidateObjectMetaUpdate(&statefulSet.ObjectMeta, &oldStatefulSet.ObjectMeta, field.NewPath("metadata"))...)
	// TODO: 校验 status。
	if apivalidation.IsDecremented(statefulSet.Status.CollisionCount, oldStatefulSet.Status.CollisionCount) {
		value := int32(0)
		if statefulSet.Status.CollisionCount != nil {
			value = *statefulSet.Status.CollisionCount
		}
		allErrs = append(allErrs, field.Invalid(field.NewPath("status").Child("collisionCount"), value, "cannot be decremented"))
	}
	return allErrs
}

// ValidateControllerRevisionName 可用于检查给定的 ControllerRevision 名称是否合法。
// Prefix 表示该名称将作为生成名字的一部分使用，此时
// 允许结尾的连字符。
var ValidateControllerRevisionName = apimachineryvalidation.NameIsDNSSubdomain

// validateControllerRevision 收集 state 各字段的错误，并以 ErrorList 形式返回这些错误。如果返回的
// 列表为空，则 state 合法。校验内容包括：state 是一个合法的 ObjectMeta、其名称
// 合法，且不超过 MaxControllerRevisionSize。
func validateControllerRevision(revision *apps.ControllerRevision) field.ErrorList {
	errs := field.ErrorList{}
	errs = append(errs, apivalidation.ValidateObjectMeta(&revision.ObjectMeta, true, ValidateControllerRevisionName, field.NewPath("metadata"))...)
	errs = append(errs, apivalidation.ValidateNonnegativeField(revision.Revision, field.NewPath("revision"))...)
	return errs
}

func ValidateControllerRevisionCreate(revision *apps.ControllerRevision) field.ErrorList {
	errs := field.ErrorList{}
	errs = append(errs, validateControllerRevision(revision)...)

	var v any
	if revision.Data.Raw == nil {
		errs = append(errs, field.Required(field.NewPath("data"), "data is mandatory"))
	} else if err := json.Unmarshal(revision.Data.Raw, &v); err != nil {
		errs = append(errs, field.Invalid(field.NewPath("data"), "<value omitted>", fmt.Sprintf("error parsing data: %v", err.Error())))
	} else if v == nil {
		errs = append(errs, field.Required(field.NewPath("data"), "data is mandatory"))
	} else if _, isObject := v.(map[string]any); !isObject {
		errs = append(errs, field.Required(field.NewPath("data"), "data must be a valid JSON object"))
	}

	return errs
}

// ValidateControllerRevisionUpdate 收集与 ControllerRevision 对象变更相关的错误。如果返回的
// ErrorList 为空，则更新操作合法。对 ControllerRevision 的 Data 或 Revision
// 的任何变更都被视为非法。
func ValidateControllerRevisionUpdate(newHistory, oldHistory *apps.ControllerRevision) field.ErrorList {
	errs := field.ErrorList{}

	errs = append(errs, apivalidation.ValidateObjectMetaUpdate(&newHistory.ObjectMeta, &oldHistory.ObjectMeta, field.NewPath("metadata"))...)
	errs = append(errs, validateControllerRevision(newHistory)...)
	errs = append(errs, apivalidation.ValidateImmutableField(newHistory.Data, oldHistory.Data, field.NewPath("data"))...)
	return errs
}

// ValidateDaemonSet 测试 DaemonSet 中的必填字段是否已设置。
func ValidateDaemonSet(ds *apps.DaemonSet, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&ds.ObjectMeta, true, ValidateDaemonSetName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateDaemonSetSpec(&ds.Spec, field.NewPath("spec"), opts)...)
	return allErrs
}

// ValidateDaemonSetUpdate 测试 DaemonSet 中的必填字段是否已设置。
func ValidateDaemonSetUpdate(ds, oldDS *apps.DaemonSet, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&ds.ObjectMeta, &oldDS.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateDaemonSetSpecUpdate(&ds.Spec, &oldDS.Spec, field.NewPath("spec"))...)
	allErrs = append(allErrs, ValidateDaemonSetSpec(&ds.Spec, field.NewPath("spec"), opts)...)
	return allErrs
}

// ValidateDaemonSetSpecUpdate 测试对 DaemonSetSpec 的更新是否合法。
func ValidateDaemonSetSpecUpdate(newSpec, oldSpec *apps.DaemonSetSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// TemplateGeneration 不应当被递减
	if newSpec.TemplateGeneration < oldSpec.TemplateGeneration {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("templateGeneration"), newSpec.TemplateGeneration, "must not be decremented"))
	}

	// TemplateGeneration 应当且仅当模板发生变更时才递增
	templateUpdated := !apiequality.Semantic.DeepEqual(newSpec.Template, oldSpec.Template)
	if newSpec.TemplateGeneration == oldSpec.TemplateGeneration && templateUpdated {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("templateGeneration"), newSpec.TemplateGeneration, "must be incremented upon template update"))
	} else if newSpec.TemplateGeneration > oldSpec.TemplateGeneration && !templateUpdated {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("templateGeneration"), newSpec.TemplateGeneration, "must not be incremented without template update"))
	}

	// Spec.Selector 不可变
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(newSpec.Selector, oldSpec.Selector, field.NewPath("spec").Child("selector"))...)

	return allErrs
}

// validateDaemonSetStatus 校验一个 DaemonSetStatus
func validateDaemonSetStatus(status *apps.DaemonSetStatus, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.CurrentNumberScheduled), fldPath.Child("currentNumberScheduled"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.NumberMisscheduled), fldPath.Child("numberMisscheduled"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.DesiredNumberScheduled), fldPath.Child("desiredNumberScheduled"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.NumberReady), fldPath.Child("numberReady"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(status.ObservedGeneration, fldPath.Child("observedGeneration"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.UpdatedNumberScheduled), fldPath.Child("updatedNumberScheduled"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.NumberAvailable), fldPath.Child("numberAvailable"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.NumberUnavailable), fldPath.Child("numberUnavailable"))...)
	if status.CollisionCount != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*status.CollisionCount), fldPath.Child("collisionCount"))...)
	}
	return allErrs
}

// ValidateDaemonSetStatusUpdate 测试 DaemonSet Status 部分的必填字段是否已设置
func ValidateDaemonSetStatusUpdate(ds, oldDS *apps.DaemonSet) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&ds.ObjectMeta, &oldDS.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, validateDaemonSetStatus(&ds.Status, field.NewPath("status"))...)
	if apivalidation.IsDecremented(ds.Status.CollisionCount, oldDS.Status.CollisionCount) {
		value := int32(0)
		if ds.Status.CollisionCount != nil {
			value = *ds.Status.CollisionCount
		}
		allErrs = append(allErrs, field.Invalid(field.NewPath("status").Child("collisionCount"), value, "cannot be decremented"))
	}
	return allErrs
}

// ValidateDaemonSetSpec 测试 DaemonSetSpec 中的必填字段是否已设置。
func ValidateDaemonSetSpec(spec *apps.DaemonSetSpec, fldPath *field.Path, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	labelSelectorValidationOpts := unversionedvalidation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: opts.AllowInvalidLabelValueInSelector}

	allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(spec.Selector, labelSelectorValidationOpts, fldPath.Child("selector"))...)

	selector, err := metav1.LabelSelectorAsSelector(spec.Selector)
	if err == nil && !selector.Matches(labels.Set(spec.Template.Labels)) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("template", "metadata", "labels"), spec.Template.Labels, "`selector` does not match template `labels`"))
	}
	if spec.Selector != nil && len(spec.Selector.MatchLabels)+len(spec.Selector.MatchExpressions) == 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("selector"), spec.Selector, "empty selector is invalid for daemonset"))
	}

	allErrs = append(allErrs, apivalidation.ValidatePodTemplateSpec(&spec.Template, fldPath.Child("template"), opts)...)
	// RestartPolicy 已按照 ValidatePodTemplateSpec() 完成了一阶校验。
	if spec.Template.Spec.RestartPolicy != api.RestartPolicyAlways {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("template", "spec", "restartPolicy"), spec.Template.Spec.RestartPolicy, []string{string(api.RestartPolicyAlways)}))
	}
	if spec.Template.Spec.ActiveDeadlineSeconds != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("template", "spec", "activeDeadlineSeconds"), "activeDeadlineSeconds in DaemonSet is not Supported"))
	}
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(spec.MinReadySeconds), fldPath.Child("minReadySeconds"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(spec.TemplateGeneration), fldPath.Child("templateGeneration"))...)

	allErrs = append(allErrs, ValidateDaemonSetUpdateStrategy(&spec.UpdateStrategy, fldPath.Child("updateStrategy"))...)
	if spec.RevisionHistoryLimit != nil {
		// 零是合法的 RevisionHistoryLimit
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.RevisionHistoryLimit), fldPath.Child("revisionHistoryLimit"))...)
	}
	return allErrs
}

// ValidateRollingUpdateDaemonSet 校验给定的 RollingUpdateDaemonSet。
func ValidateRollingUpdateDaemonSet(rollingUpdate *apps.RollingUpdateDaemonSet, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	// 校验两个字段的值都是正整数或百分比值
	allErrs = append(allErrs, ValidatePositiveIntOrPercent(rollingUpdate.MaxUnavailable, fldPath.Child("maxUnavailable"))...)
	allErrs = append(allErrs, ValidatePositiveIntOrPercent(rollingUpdate.MaxSurge, fldPath.Child("maxSurge"))...)

	// 校验 MaxUnavailable 和 MaxSurge 不超过 100%。
	allErrs = append(allErrs, IsNotMoreThan100Percent(rollingUpdate.MaxUnavailable, fldPath.Child("maxUnavailable"))...)
	allErrs = append(allErrs, IsNotMoreThan100Percent(rollingUpdate.MaxSurge, fldPath.Child("maxSurge"))...)

	// 校验 MaxSurge 和 MaxUnavailable 中恰好有一个非零
	hasUnavailable := getIntOrPercentValue(rollingUpdate.MaxUnavailable) != 0
	hasSurge := getIntOrPercentValue(rollingUpdate.MaxSurge) != 0
	switch {
	case hasUnavailable && hasSurge:
		allErrs = append(allErrs, field.Invalid(fldPath.Child("maxSurge"), rollingUpdate.MaxSurge, "may not be set when maxUnavailable is non-zero"))
	case !hasUnavailable && !hasSurge:
		allErrs = append(allErrs, field.Required(fldPath.Child("maxUnavailable"), "cannot be 0 when maxSurge is 0"))
	}

	return allErrs
}

// validateRollingUpdateStatefulSet 校验给定的 RollingUpdateStatefulSet。
func validateRollingUpdateStatefulSet(rollingUpdate *apps.RollingUpdateStatefulSetStrategy, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	fldPathMaxUn := fldPath.Child("maxUnavailable")
	allErrs = append(allErrs,
		apivalidation.ValidateNonnegativeField(
			int64(rollingUpdate.Partition),
			fldPath.Child("partition"))...)
	if rollingUpdate.MaxUnavailable != nil {
		allErrs = append(allErrs, ValidatePositiveIntOrPercent(*rollingUpdate.MaxUnavailable, fldPathMaxUn)...)
		if getIntOrPercentValue(*rollingUpdate.MaxUnavailable) == 0 {
			// MaxUnavailable 不能为 0。
			allErrs = append(allErrs, field.Invalid(fldPathMaxUn, *rollingUpdate.MaxUnavailable, "cannot be 0"))
		}
		// 校验 MaxUnavailable 不超过 100%。
		allErrs = append(allErrs, IsNotMoreThan100Percent(*rollingUpdate.MaxUnavailable, fldPathMaxUn)...)
	}
	return allErrs
}

// ValidateDaemonSetUpdateStrategy 校验给定的 DaemonSetUpdateStrategy。
func ValidateDaemonSetUpdateStrategy(strategy *apps.DaemonSetUpdateStrategy, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	switch strategy.Type {
	case apps.OnDeleteDaemonSetStrategyType:
	case apps.RollingUpdateDaemonSetStrategyType:
		// 确保 RollingUpdate 字段不为 nil。
		if strategy.RollingUpdate == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("rollingUpdate"), ""))
			return allErrs
		}
		allErrs = append(allErrs, ValidateRollingUpdateDaemonSet(strategy.RollingUpdate, fldPath.Child("rollingUpdate"))...)
	default:
		validValues := []string{string(apps.RollingUpdateDaemonSetStrategyType), string(apps.OnDeleteDaemonSetStrategyType)}
		allErrs = append(allErrs, field.NotSupported(fldPath, strategy, validValues))
	}
	return allErrs
}

// ValidateDaemonSetName 可用于检查给定的 daemon set 名称是否合法。
// Prefix 表示该名称将作为生成名字的一部分使用，此时
// 允许结尾的连字符。
var ValidateDaemonSetName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateDeploymentName 校验给定的名称可以用作 deployment 名称。
var ValidateDeploymentName = apimachineryvalidation.NameIsDNSSubdomain

// ValidatePositiveIntOrPercent 测试给定值是否为合法的整数或
// 百分比。
func ValidatePositiveIntOrPercent(intOrPercent intstr.IntOrString, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	switch intOrPercent.Type {
	case intstr.String:
		for _, msg := range validation.IsValidPercent(intOrPercent.StrVal) {
			allErrs = append(allErrs, field.Invalid(fldPath, intOrPercent, msg))
		}
	case intstr.Int:
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(intOrPercent.IntValue()), fldPath)...)
	default:
		allErrs = append(allErrs, field.Invalid(fldPath, intOrPercent, "must be an integer or percentage (e.g '5%%')"))
	}
	return allErrs
}

func getPercentValue(intOrStringValue intstr.IntOrString) (int, bool) {
	if intOrStringValue.Type != intstr.String {
		return 0, false
	}
	if len(validation.IsValidPercent(intOrStringValue.StrVal)) != 0 {
		return 0, false
	}
	value, _ := strconv.Atoi(intOrStringValue.StrVal[:len(intOrStringValue.StrVal)-1])
	return value, true
}

func getIntOrPercentValue(intOrStringValue intstr.IntOrString) int {
	value, isPercent := getPercentValue(intOrStringValue)
	if isPercent {
		return value
	}
	return intOrStringValue.IntValue()
}

// IsNotMoreThan100Percent 测试某个值是否可以表示为百分比，
// 且该值不超过 100%。
func IsNotMoreThan100Percent(intOrStringValue intstr.IntOrString, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	value, isPercent := getPercentValue(intOrStringValue)
	if !isPercent || value <= 100 {
		return nil
	}
	allErrs = append(allErrs, field.Invalid(fldPath, intOrStringValue, "must not be greater than 100%"))
	return allErrs
}

// ValidateRollingUpdateDeployment 校验给定的 RollingUpdateDeployment。
func ValidateRollingUpdateDeployment(rollingUpdate *apps.RollingUpdateDeployment, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidatePositiveIntOrPercent(rollingUpdate.MaxUnavailable, fldPath.Child("maxUnavailable"))...)
	allErrs = append(allErrs, ValidatePositiveIntOrPercent(rollingUpdate.MaxSurge, fldPath.Child("maxSurge"))...)
	if getIntOrPercentValue(rollingUpdate.MaxUnavailable) == 0 && getIntOrPercentValue(rollingUpdate.MaxSurge) == 0 {
		// MaxSurge 和 MaxUnavailable 不能同时为零。
		allErrs = append(allErrs, field.Invalid(fldPath.Child("maxUnavailable"), rollingUpdate.MaxUnavailable, "may not be 0 when `maxSurge` is 0"))
	}
	// 校验 MaxUnavailable 不超过 100%。
	allErrs = append(allErrs, IsNotMoreThan100Percent(rollingUpdate.MaxUnavailable, fldPath.Child("maxUnavailable"))...)
	return allErrs
}

// ValidateDeploymentStrategy 校验给定的 DeploymentStrategy。
func ValidateDeploymentStrategy(strategy *apps.DeploymentStrategy, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	switch strategy.Type {
	case apps.RecreateDeploymentStrategyType:
		if strategy.RollingUpdate != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("rollingUpdate"), "may not be specified when strategy `type` is '"+string(apps.RecreateDeploymentStrategyType+"'")))
		}
	case apps.RollingUpdateDeploymentStrategyType:
		// 这种情况不应发生，因为它在 defaults.go 中被设置并检查
		if strategy.RollingUpdate == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("rollingUpdate"), "this should be defaulted and never be nil"))
		} else {
			allErrs = append(allErrs, ValidateRollingUpdateDeployment(strategy.RollingUpdate, fldPath.Child("rollingUpdate"))...)
		}
	default:
		validValues := []string{string(apps.RecreateDeploymentStrategyType), string(apps.RollingUpdateDeploymentStrategyType)}
		allErrs = append(allErrs, field.NotSupported(fldPath, strategy, validValues))
	}
	return allErrs
}

// ValidateRollback 校验给定的 RollbackConfig。
func ValidateRollback(rollback *apps.RollbackConfig, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	v := rollback.Revision
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(v), fldPath.Child("version"))...)
	return allErrs
}

// ValidateDeploymentSpec 校验给定的 deployment spec。
func ValidateDeploymentSpec(spec, oldSpec *apps.DeploymentSpec, fldPath *field.Path, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(spec.Replicas), fldPath.Child("replicas"))...)

	if spec.Selector == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("selector"), ""))
	} else {
		// 严格校验 selector，spec.selector 一直是必填项，才能通过下面的 LabelSelectorAsSelector
		allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(spec.Selector, unversionedvalidation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: false}, fldPath.Child("selector"))...)
		if len(spec.Selector.MatchLabels)+len(spec.Selector.MatchExpressions) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("selector"), spec.Selector, "empty selector is invalid for deployment"))
		}
	}

	selector, err := metav1.LabelSelectorAsSelector(spec.Selector)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("selector"), spec.Selector, "invalid label selector"))
	} else {
		allErrs = append(allErrs, ValidatePodTemplateSpecForReplicaSet(&spec.Template, selector, spec.Replicas, fldPath.Child("template"), opts)...)
	}

	allErrs = append(allErrs, ValidateDeploymentStrategy(&spec.Strategy, fldPath.Child("strategy"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(spec.MinReadySeconds), fldPath.Child("minReadySeconds"))...)
	if spec.RevisionHistoryLimit != nil {
		// 零是合法的 RevisionHistoryLimit
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.RevisionHistoryLimit), fldPath.Child("revisionHistoryLimit"))...)
	}
	if spec.RollbackTo != nil {
		allErrs = append(allErrs, ValidateRollback(spec.RollbackTo, fldPath.Child("rollback"))...)
	}
	if spec.ProgressDeadlineSeconds != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.ProgressDeadlineSeconds), fldPath.Child("progressDeadlineSeconds"))...)
		if *spec.ProgressDeadlineSeconds <= spec.MinReadySeconds {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("progressDeadlineSeconds"), spec.ProgressDeadlineSeconds, "must be greater than minReadySeconds"))
		}
	}
	return allErrs
}

// ValidateDeploymentStatus 校验给定的 deployment status。
func ValidateDeploymentStatus(status *apps.DeploymentStatus, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(status.ObservedGeneration, fldPath.Child("observedGeneration"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.Replicas), fldPath.Child("replicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.UpdatedReplicas), fldPath.Child("updatedReplicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.ReadyReplicas), fldPath.Child("readyReplicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.AvailableReplicas), fldPath.Child("availableReplicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.UnavailableReplicas), fldPath.Child("unavailableReplicas"))...)
	if status.TerminatingReplicas != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*status.TerminatingReplicas), fldPath.Child("terminatingReplicas"))...)
	}
	if status.CollisionCount != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*status.CollisionCount), fldPath.Child("collisionCount"))...)
	}
	msg := "cannot be greater than status.replicas"
	if status.UpdatedReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("updatedReplicas"), status.UpdatedReplicas, msg))
	}
	if status.ReadyReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("readyReplicas"), status.ReadyReplicas, msg))
	}
	if status.AvailableReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("availableReplicas"), status.AvailableReplicas, msg))
	}
	if status.AvailableReplicas > status.ReadyReplicas {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("availableReplicas"), status.AvailableReplicas, "cannot be greater than readyReplicas"))
	}
	return allErrs
}

// ValidateDeploymentUpdate 测试对 Deployment 的更新是否合法。
func ValidateDeploymentUpdate(update, old *apps.Deployment, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&update.ObjectMeta, &old.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateDeploymentSpec(&update.Spec, &old.Spec, field.NewPath("spec"), opts)...)
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(update.Spec.Selector, old.Spec.Selector, field.NewPath("spec").Child("selector"))...)
	return allErrs
}

// ValidateDeploymentStatusUpdate 测试对 Deployment status
// 的更新是否合法。
func ValidateDeploymentStatusUpdate(update, old *apps.Deployment) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&update.ObjectMeta, &old.ObjectMeta, field.NewPath("metadata"))
	fldPath := field.NewPath("status")
	allErrs = append(allErrs, ValidateDeploymentStatus(&update.Status, fldPath)...)
	if apivalidation.IsDecremented(update.Status.CollisionCount, old.Status.CollisionCount) {
		value := int32(0)
		if update.Status.CollisionCount != nil {
			value = *update.Status.CollisionCount
		}
		allErrs = append(allErrs, field.Invalid(fldPath.Child("collisionCount"), value, "cannot be decremented"))
	}
	return allErrs
}

// ValidateDeployment 校验给定的 Deployment。
func ValidateDeployment(obj *apps.Deployment, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&obj.ObjectMeta, true, ValidateDeploymentName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateDeploymentSpec(&obj.Spec, nil, field.NewPath("spec"), opts)...)
	return allErrs
}

// ValidateDeploymentRollback 校验给定的 DeploymentRollback。
func ValidateDeploymentRollback(obj *apps.DeploymentRollback) field.ErrorList {
	allErrs := apivalidation.ValidateAnnotations(obj.UpdatedAnnotations, field.NewPath("updatedAnnotations"))
	if len(obj.Name) == 0 {
		allErrs = append(allErrs, field.Required(field.NewPath("name"), ""))
	}
	allErrs = append(allErrs, ValidateRollback(&obj.RollbackTo, field.NewPath("rollback"))...)
	return allErrs
}

// ValidateReplicaSetName 可用于检查给定的 ReplicaSet 名称是否合法。
// Prefix 表示该名称将作为生成名字的一部分使用，此时
// 允许结尾的连字符。
var ValidateReplicaSetName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateReplicaSet 测试 ReplicaSet 中的必填字段是否已设置。
func ValidateReplicaSet(rs *apps.ReplicaSet, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&rs.ObjectMeta, true, ValidateReplicaSetName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateReplicaSetSpec(&rs.Spec, nil, field.NewPath("spec"), opts)...)
	return allErrs
}

// ValidateReplicaSetUpdate 测试 ReplicaSet 中的必填字段是否已设置。
func ValidateReplicaSetUpdate(rs, oldRs *apps.ReplicaSet, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, apivalidation.ValidateObjectMetaUpdate(&rs.ObjectMeta, &oldRs.ObjectMeta, field.NewPath("metadata"))...)
	allErrs = append(allErrs, ValidateReplicaSetSpec(&rs.Spec, &oldRs.Spec, field.NewPath("spec"), opts)...)
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(rs.Spec.Selector, oldRs.Spec.Selector, field.NewPath("spec").Child("selector"))...)
	return allErrs
}

// ValidateReplicaSetStatusUpdate 测试 ReplicaSet 中的必填字段是否已设置。
func ValidateReplicaSetStatusUpdate(rs, oldRs *apps.ReplicaSet) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, apivalidation.ValidateObjectMetaUpdate(&rs.ObjectMeta, &oldRs.ObjectMeta, field.NewPath("metadata"))...)
	allErrs = append(allErrs, ValidateReplicaSetStatus(rs.Status, field.NewPath("status"))...)
	return allErrs
}

// ValidateReplicaSetStatus 校验给定的 ReplicaSetStatus。
func ValidateReplicaSetStatus(status apps.ReplicaSetStatus, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.Replicas), fldPath.Child("replicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.FullyLabeledReplicas), fldPath.Child("fullyLabeledReplicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.ReadyReplicas), fldPath.Child("readyReplicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.AvailableReplicas), fldPath.Child("availableReplicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.ObservedGeneration), fldPath.Child("observedGeneration"))...)
	if status.TerminatingReplicas != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*status.TerminatingReplicas), fldPath.Child("terminatingReplicas"))...)
	}
	msg := "cannot be greater than status.replicas"
	if status.FullyLabeledReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("fullyLabeledReplicas"), status.FullyLabeledReplicas, msg))
	}
	if status.ReadyReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("readyReplicas"), status.ReadyReplicas, msg))
	}
	if status.AvailableReplicas > status.Replicas {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("availableReplicas"), status.AvailableReplicas, msg))
	}
	if status.AvailableReplicas > status.ReadyReplicas {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("availableReplicas"), status.AvailableReplicas, "cannot be greater than readyReplicas"))
	}
	return allErrs
}

// ValidateReplicaSetSpec 测试 ReplicaSet spec 中的必填字段是否已设置。
func ValidateReplicaSetSpec(spec, oldSpec *apps.ReplicaSetSpec, fldPath *field.Path, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(spec.Replicas), fldPath.Child("replicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(spec.MinReadySeconds), fldPath.Child("minReadySeconds"))...)

	if spec.Selector == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("selector"), ""))
	} else {
		// 严格校验 selector，spec.selector 一直是必填项，才能通过下面的 LabelSelectorAsSelector
		allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(spec.Selector, unversionedvalidation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: false}, fldPath.Child("selector"))...)
		if len(spec.Selector.MatchLabels)+len(spec.Selector.MatchExpressions) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("selector"), spec.Selector, "empty selector is invalid for deployment"))
		}
	}

	selector, err := metav1.LabelSelectorAsSelector(spec.Selector)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("selector"), spec.Selector, "invalid label selector"))
	} else {
		allErrs = append(allErrs, ValidatePodTemplateSpecForReplicaSet(&spec.Template, selector, spec.Replicas, fldPath.Child("template"), opts)...)
	}
	return allErrs
}

// ValidatePodTemplateSpecForReplicaSet 校验给定的模板，确保其与期望的 selector 和副本数一致。
func ValidatePodTemplateSpecForReplicaSet(template *api.PodTemplateSpec, selector labels.Selector, replicas int32, fldPath *field.Path, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if template == nil {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	} else {
		if !selector.Empty() {
			// 校验 ReplicaSet 的 selector 与模板中的 labels 相匹配。
			labels := labels.Set(template.Labels)
			if !selector.Matches(labels) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("metadata", "labels"), template.Labels, "`selector` does not match template `labels`"))
			}
		}
		allErrs = append(allErrs, apivalidation.ValidatePodTemplateSpec(template, fldPath, opts)...)
		// RestartPolicy 已按照 ValidatePodTemplateSpec() 完成了一阶校验。
		if template.Spec.RestartPolicy != api.RestartPolicyAlways {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("spec", "restartPolicy"), template.Spec.RestartPolicy, []string{string(api.RestartPolicyAlways)}))
		}
		if template.Spec.ActiveDeadlineSeconds != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("spec", "activeDeadlineSeconds"), "activeDeadlineSeconds in ReplicaSet is not Supported"))
		}
	}
	return allErrs
}
