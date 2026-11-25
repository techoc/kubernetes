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
	"regexp"
	"strconv"
	"strings"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimachineryapivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	unversionedvalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	apimachineryvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/component-helpers/scheduling/schedulingv1/workloadbuilder"
	jobutil "k8s.io/kubernetes/pkg/api/job"
	"k8s.io/kubernetes/pkg/apis/batch"
	api "k8s.io/kubernetes/pkg/apis/core"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/util/parsers"
	"k8s.io/utils/ptr"
)

// maxParallelismForIndexJob 是 Indexed Job 允许的最大 parallelism。
// 设置该上限是为了限制 .status.completedIndexes 的长度，避免 status 体积失控。
const maxParallelismForIndexedJob = 100000

// maxFailedIndexesForIndexedJob 是 Indexed Job 允许的最大失败索引数。
// 该阈值用于限制 .status.completedIndexes 和 .status.failedIndexes 的长度。
const maxFailedIndexesForIndexedJob = 100_000

const (
	completionsSoftLimit                    = 100_000
	parallelismLimitForHighCompletions      = 10_000
	maxFailedIndexesLimitForHighCompletions = 10_000

	// pod failure policy 中规则的最大数量
	maxPodFailurePolicyRules = 20

	// pod failure policy 中 OnExitCodes 需求项里值的最大数量
	maxPodFailurePolicyOnExitCodesValues = 255

	// pod failure policy 中 OnPodConditions 需求项里模式的最大数量
	maxPodFailurePolicyOnPodConditionsPatterns = 20

	// managedBy 字段值的最大长度
	maxManagedByLength = 63

	// JobSuccessPolicy 中 succeededIndexes 的最大长度。
	maxJobSuccessPolicySucceededIndexesLimit = 64 * 1024
	// successPolicy 中规则的最大数量。
	maxSuccessPolicyRule = 20
)

var (
	supportedPodFailurePolicyActions = sets.New(
		batch.PodFailurePolicyActionCount,
		batch.PodFailurePolicyActionFailIndex,
		batch.PodFailurePolicyActionFailJob,
		batch.PodFailurePolicyActionIgnore)

	supportedPodFailurePolicyOnExitCodesOperator = sets.New(
		batch.PodFailurePolicyOnExitCodesOpIn,
		batch.PodFailurePolicyOnExitCodesOpNotIn)

	supportedPodFailurePolicyOnPodConditionsStatus = sets.New(
		api.ConditionFalse,
		api.ConditionTrue,
		api.ConditionUnknown)

	supportedPodReplacementPolicy = sets.New(
		batch.Failed,
		batch.TerminatingOrFailed)
)

// validateGeneratedSelector 校验控制器对象上自动生成的选择器与控制器对象的
// 元数据相匹配，且 Pod 模板上的标签与生成的标签一致。
//
// TODO: 将其泛化以支持其他遵循相同模式的控制器对象（如 ReplicaSet 和 DaemonSet），
// 并迁移到新的位置。将 batch.Job 替换为一个接口。
func validateGeneratedSelector(obj *batch.Job, validateBatchLabels bool) field.ErrorList {
	allErrs := field.ErrorList{}
	if obj.Spec.ManualSelector != nil && *obj.Spec.ManualSelector {
		return allErrs
	}

	if obj.Spec.Selector == nil {
		return allErrs // 该情形调用方本应已经检查过，无需再叠加错误。
	}

	// 如果 uid 意外未设置，选择器会变成 "controller-uid="，
	// 这是不可接受的。
	if obj.ObjectMeta.UID == "" {
		allErrs = append(allErrs, field.Required(field.NewPath("metadata").Child("uid"), ""))
	}

	// 如果请求了选择器自动生成，则 Pod 模板上必须存在预期的标签，
	// 且必须与 Job 的 uid 和名称匹配。自动生成（非手动）的选择器/标签
	// 可确保与其他控制器互不重叠。手动模式则允许 Pod 成为孤儿、被收养、
	// 保持向后兼容，并允许试验新的标签/选择方案。
	// 自动选择器生成本应在 Pod 上放置某些标签，但如果用户添加了
	// 冲突的标签，这一步可能失败。此处校验预期生成的标签确实存在。
	allErrs = append(allErrs, apivalidation.ValidateHasLabel(obj.Spec.Template.ObjectMeta, field.NewPath("spec").Child("template").Child("metadata"), batch.LegacyControllerUidLabel, string(obj.UID))...)
	allErrs = append(allErrs, apivalidation.ValidateHasLabel(obj.Spec.Template.ObjectMeta, field.NewPath("spec").Child("template").Child("metadata"), batch.LegacyJobNameLabel, string(obj.Name))...)
	expectedLabels := make(map[string]string)
	if validateBatchLabels {
		allErrs = append(allErrs, apivalidation.ValidateHasLabel(obj.Spec.Template.ObjectMeta, field.NewPath("spec").Child("template").Child("metadata"), batch.ControllerUidLabel, string(obj.UID))...)
		allErrs = append(allErrs, apivalidation.ValidateHasLabel(obj.Spec.Template.ObjectMeta, field.NewPath("spec").Child("template").Child("metadata"), batch.JobNameLabel, string(obj.Name))...)
		expectedLabels[batch.ControllerUidLabel] = string(obj.UID)
		expectedLabels[batch.JobNameLabel] = string(obj.Name)
	}
	// Kubernetes 项目创建的标签应带有 Kubernetes 前缀。
	// 这些标签因历史遗留原因而被设置。

	expectedLabels[batch.LegacyControllerUidLabel] = string(obj.UID)
	expectedLabels[batch.LegacyJobNameLabel] = string(obj.Name)
	// 无论是手动指定还是自动生成，Job 的选择器都必须与其将产生的 Pod 匹配。
	if selector, err := metav1.LabelSelectorAsSelector(obj.Spec.Selector); err == nil {
		if !selector.Matches(labels.Set(expectedLabels)) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec").Child("selector"), obj.Spec.Selector, "`selector` not auto-generated"))
		}
	}

	return allErrs
}

// ValidateJobCreate 校验 Job，并返回包含所有错误的 ErrorList。
func ValidateJobCreate(job *batch.Job, opts JobValidationOptions) field.ErrorList {
	// Job 和 RC 使用相同的名称校验规则
	allErrs := apivalidation.ValidateObjectMeta(&job.ObjectMeta, true, apimachineryapivalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrs = append(allErrs, validateGeneratedSelector(job, opts.RequirePrefixedLabels)...)
	allErrs = append(allErrs, ValidateJobSpec(&job.Spec, nil, field.NewPath("spec"), opts.PodValidationOptions)...)
	allErrs = append(allErrs, validateNameAllowsCompletions(job, opts)...)
	return allErrs
}
func validateNameAllowsCompletions(job *batch.Job, opts JobValidationOptions) field.ErrorList {
	var allErrs field.ErrorList
	if job.Spec.CompletionMode != nil && *job.Spec.CompletionMode == batch.IndexedCompletion && job.Spec.Completions != nil && *job.Spec.Completions > 0 {
		// 对于 Indexed Job，Job 控制器在创建 Pod 时会在 Pod 主机名后
		// 追加一个后缀（`-$INDEX`）。
		// 该索引最大为 `.spec.completions-1`。
		// 如果此处不校验，Indexed Job 稍后将无法创建 Pod。
		maximumPodHostname := fmt.Sprintf("%s-%d", job.ObjectMeta.Name, *job.Spec.Completions-1)
		if errs := apimachineryvalidation.IsDNS1123Label(maximumPodHostname); len(errs) > 0 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("metadata").Child("name"), job.ObjectMeta.Name, fmt.Sprintf("will not able to create pod with invalid DNS label: %s", maximumPodHostname)))
		}
	}
	return allErrs
}

// ValidateJobSpec 校验 JobSpec，并返回包含所有错误的 ErrorList。
// oldSpec 在创建时为 nil，在更新时非 nil。
func ValidateJobSpec(spec, oldSpec *batch.JobSpec, fldPath *field.Path, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := validateJobSpec(spec, oldSpec, fldPath, opts)
	if spec.Selector == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("selector"), ""))
	} else {
		labelSelectorValidationOpts := unversionedvalidation.LabelSelectorValidationOptions{
			AllowInvalidLabelValueInSelector: opts.AllowInvalidLabelValueInSelector,
		}
		allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(spec.Selector, labelSelectorValidationOpts, fldPath.Child("selector"))...)
	}

	// 无论是手动指定还是自动生成，Job 的选择器都必须与其将产生的 Pod 匹配。
	if selector, err := metav1.LabelSelectorAsSelector(spec.Selector); err == nil {
		labels := labels.Set(spec.Template.Labels)
		if !selector.Matches(labels) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("template", "metadata", "labels"), spec.Template.Labels, "`selector` does not match template `labels`"))
		}
	}
	return allErrs
}

// oldSpec 在创建时为 nil，在更新时非 nil。
func validateJobSpec(spec, oldSpec *batch.JobSpec, fldPath *field.Path, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	if spec.Parallelism != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.Parallelism), fldPath.Child("parallelism"))...)
	}
	if spec.Completions != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.Completions), fldPath.Child("completions"))...)
	}
	if spec.ActiveDeadlineSeconds != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.ActiveDeadlineSeconds), fldPath.Child("activeDeadlineSeconds"))...)
	}
	if spec.BackoffLimit != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.BackoffLimit), fldPath.Child("backoffLimit"))...)
	}
	if spec.TTLSecondsAfterFinished != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.TTLSecondsAfterFinished), fldPath.Child("ttlSecondsAfterFinished"))...)
	}
	if spec.BackoffLimitPerIndex != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.BackoffLimitPerIndex), fldPath.Child("backoffLimitPerIndex"))...)
	}
	if spec.MaxFailedIndexes != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.MaxFailedIndexes), fldPath.Child("maxFailedIndexes"))...)
		if spec.BackoffLimitPerIndex == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("backoffLimitPerIndex"), "when maxFailedIndexes is specified").WithOrigin("dependentRequired").MarkCoveredByDeclarative())
		}
	}
	if spec.ManagedBy != nil {
		allErrs = append(allErrs, apimachineryvalidation.IsDomainPrefixedPath(fldPath.Child("managedBy"), *spec.ManagedBy)...)
		if len(*spec.ManagedBy) > maxManagedByLength {
			allErrs = append(allErrs, field.TooLong(fldPath.Child("managedBy"), "" /*unused*/, maxManagedByLength))
		}
	}
	if spec.CompletionMode != nil {
		if *spec.CompletionMode != batch.NonIndexedCompletion && *spec.CompletionMode != batch.IndexedCompletion {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("completionMode"), spec.CompletionMode, []batch.CompletionMode{batch.NonIndexedCompletion, batch.IndexedCompletion}))
		}
		if *spec.CompletionMode == batch.IndexedCompletion {
			if spec.Completions == nil {
				allErrs = append(allErrs, field.Required(fldPath.Child("completions"), fmt.Sprintf("when completion mode is %s", batch.IndexedCompletion)))
			}
			if spec.Parallelism != nil && *spec.Parallelism > maxParallelismForIndexedJob {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("parallelism"), *spec.Parallelism, fmt.Sprintf("must be less than or equal to %d when completion mode is %s", maxParallelismForIndexedJob, batch.IndexedCompletion)))
			}
			if spec.Completions != nil && spec.MaxFailedIndexes != nil && *spec.MaxFailedIndexes > *spec.Completions {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("maxFailedIndexes"), *spec.MaxFailedIndexes, "must be less than or equal to completions"))
			}
			if spec.MaxFailedIndexes != nil && *spec.MaxFailedIndexes > maxFailedIndexesForIndexedJob {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("maxFailedIndexes"), *spec.MaxFailedIndexes, fmt.Sprintf("must be less than or equal to %d", maxFailedIndexesForIndexedJob)))
			}
			if spec.Completions != nil && *spec.Completions > completionsSoftLimit && spec.BackoffLimitPerIndex != nil {
				if spec.MaxFailedIndexes == nil {
					allErrs = append(allErrs, field.Required(fldPath.Child("maxFailedIndexes"), fmt.Sprintf("must be specified when completions is above %d", completionsSoftLimit)))
				}
				if spec.Parallelism != nil && *spec.Parallelism > parallelismLimitForHighCompletions {
					allErrs = append(allErrs, field.Invalid(fldPath.Child("parallelism"), *spec.Parallelism, fmt.Sprintf("must be less than or equal to %d when completions are above %d and used with backoff limit per index", parallelismLimitForHighCompletions, completionsSoftLimit)))
				}
				if spec.MaxFailedIndexes != nil && *spec.MaxFailedIndexes > maxFailedIndexesLimitForHighCompletions {
					allErrs = append(allErrs, field.Invalid(fldPath.Child("maxFailedIndexes"), *spec.MaxFailedIndexes, fmt.Sprintf("must be less than or equal to %d when completions are above %d and used with backoff limit per index", maxFailedIndexesLimitForHighCompletions, completionsSoftLimit)))
				}
			}
		}
	}
	if spec.CompletionMode == nil || *spec.CompletionMode == batch.NonIndexedCompletion {
		if spec.BackoffLimitPerIndex != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("backoffLimitPerIndex"), *spec.BackoffLimitPerIndex, "requires indexed completion mode"))
		}
		if spec.MaxFailedIndexes != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("maxFailedIndexes"), *spec.MaxFailedIndexes, "requires indexed completion mode"))
		}
	}

	if spec.PodFailurePolicy != nil {
		allErrs = append(allErrs, validatePodFailurePolicy(spec, fldPath.Child("podFailurePolicy"))...)
	}
	if spec.SuccessPolicy != nil {
		if ptr.Deref(spec.CompletionMode, batch.NonIndexedCompletion) != batch.IndexedCompletion {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("successPolicy"), *spec.SuccessPolicy, "requires indexed completion mode"))
		} else {
			allErrs = append(allErrs, validateSuccessPolicy(spec, fldPath.Child("successPolicy"))...)
		}
	}

	allErrs = append(allErrs, validatePodReplacementPolicy(spec, fldPath.Child("podReplacementPolicy"))...)

	// 调度 gang 的 minCount 不得超过 parallelism。该校验在创建和更新时都会执行，
	// 也适用于嵌入 CronJob jobTemplate 中的 Job。
	// 当 spec.scheduling 未设置时，此校验为空操作。
	allErrs = append(allErrs, validateGangMinCount(spec, fldPath.Child("scheduling"))...)

	// 声明式校验无法表达的控制器策略检查：
	// 解析出的调度策略和中断模式必须在 Job 的允许列表内，
	// 且 Basic 策略不能与 All 中断模式组合使用。
	// 结构性的构建块约束由 DV 负责强制执行。
	allErrs = append(allErrs, validateJobScheduling(spec, oldSpec, fldPath.Child("scheduling"))...)

	allErrs = append(allErrs, apivalidation.ValidatePodTemplateSpec(&spec.Template, fldPath.Child("template"), opts)...)

	// 当用户未显式指定 spec.Template.Spec.RestartPolicy 时，
	// SetDefaults_PodSpec 函数可能将其默认设置为 RestartPolicyAlways，
	// 因此这里要同时检查空值和 RestartPolicyAlways 两种情形
	if spec.Template.Spec.RestartPolicy == api.RestartPolicyAlways || spec.Template.Spec.RestartPolicy == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("template", "spec", "restartPolicy"),
			fmt.Sprintf("valid values: %q, %q", api.RestartPolicyOnFailure, api.RestartPolicyNever)))
	} else if spec.Template.Spec.RestartPolicy != api.RestartPolicyOnFailure && spec.Template.Spec.RestartPolicy != api.RestartPolicyNever {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("template", "spec", "restartPolicy"),
			spec.Template.Spec.RestartPolicy, []api.RestartPolicy{api.RestartPolicyOnFailure, api.RestartPolicyNever}))
	} else if spec.PodFailurePolicy != nil && spec.Template.Spec.RestartPolicy != api.RestartPolicyNever {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("template", "spec", "restartPolicy"),
			spec.Template.Spec.RestartPolicy, fmt.Sprintf("only %q is supported when podFailurePolicy is specified", api.RestartPolicyNever)))
	}
	return allErrs
}

func validatePodFailurePolicy(spec *batch.JobSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	rulesPath := fldPath.Child("rules")
	if len(spec.PodFailurePolicy.Rules) > maxPodFailurePolicyRules {
		allErrs = append(allErrs, field.TooMany(rulesPath, len(spec.PodFailurePolicy.Rules), maxPodFailurePolicyRules))
	}
	containerNames := sets.NewString()
	for _, containerSpec := range spec.Template.Spec.Containers {
		containerNames.Insert(containerSpec.Name)
	}
	for _, containerSpec := range spec.Template.Spec.InitContainers {
		containerNames.Insert(containerSpec.Name)
	}
	for i, rule := range spec.PodFailurePolicy.Rules {
		allErrs = append(allErrs, validatePodFailurePolicyRule(spec, &rule, rulesPath.Index(i), containerNames)...)
	}
	return allErrs
}

func validatePodReplacementPolicy(spec *batch.JobSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if spec.PodReplacementPolicy != nil {
		// 如果指定了 PodFailurePolicy，则仅允许 Failed。
		if spec.PodFailurePolicy != nil {
			if *spec.PodReplacementPolicy != batch.Failed {
				allErrs = append(allErrs, field.NotSupported(fldPath, *spec.PodReplacementPolicy, []batch.PodReplacementPolicy{batch.Failed}))
			}
			// 如果未指定 PodFailurePolicy，则允许 supportedPodReplacementPolicy 中的取值。
		} else if !supportedPodReplacementPolicy.Has(*spec.PodReplacementPolicy) {
			allErrs = append(allErrs, field.NotSupported(fldPath, *spec.PodReplacementPolicy, sets.List(supportedPodReplacementPolicy)))
		}
	}
	return allErrs
}

func validatePodFailurePolicyRule(spec *batch.JobSpec, rule *batch.PodFailurePolicyRule, rulePath *field.Path, containerNames sets.String) field.ErrorList {
	var allErrs field.ErrorList
	actionPath := rulePath.Child("action")
	if rule.Action == "" {
		allErrs = append(allErrs, field.Required(actionPath, fmt.Sprintf("valid values: %q", sets.List(supportedPodFailurePolicyActions))))
	} else if rule.Action == batch.PodFailurePolicyActionFailIndex {
		if spec.BackoffLimitPerIndex == nil {
			allErrs = append(allErrs, field.Invalid(actionPath, rule.Action, "requires the backoffLimitPerIndex to be set"))
		}
	} else if !supportedPodFailurePolicyActions.Has(rule.Action) {
		allErrs = append(allErrs, field.NotSupported(actionPath, rule.Action, sets.List(supportedPodFailurePolicyActions)))
	}
	if rule.OnExitCodes != nil {
		allErrs = append(allErrs, validatePodFailurePolicyRuleOnExitCodes(rule.OnExitCodes, rulePath.Child("onExitCodes"), containerNames)...)
	}
	if len(rule.OnPodConditions) > 0 {
		allErrs = append(allErrs, validatePodFailurePolicyRuleOnPodConditions(rule.OnPodConditions, rulePath.Child("onPodConditions"))...)
	}
	if rule.OnExitCodes != nil && len(rule.OnPodConditions) > 0 {
		allErrs = append(allErrs, field.Invalid(rulePath, field.OmitValueType{}, "specifying both OnExitCodes and OnPodConditions is not supported"))
	}
	if rule.OnExitCodes == nil && len(rule.OnPodConditions) == 0 {
		allErrs = append(allErrs, field.Invalid(rulePath, field.OmitValueType{}, "specifying one of OnExitCodes and OnPodConditions is required"))
	}
	return allErrs
}

func validatePodFailurePolicyRuleOnPodConditions(onPodConditions []batch.PodFailurePolicyOnPodConditionsPattern, onPodConditionsPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(onPodConditions) > maxPodFailurePolicyOnPodConditionsPatterns {
		allErrs = append(allErrs, field.TooMany(onPodConditionsPath, len(onPodConditions), maxPodFailurePolicyOnPodConditionsPatterns))
	}
	for j, pattern := range onPodConditions {
		patternPath := onPodConditionsPath.Index(j)
		statusPath := patternPath.Child("status")
		allErrs = append(allErrs, apivalidation.ValidateQualifiedName(string(pattern.Type), patternPath.Child("type"))...)
		if pattern.Status == "" {
			allErrs = append(allErrs, field.Required(statusPath, fmt.Sprintf("valid values: %q", sets.List(supportedPodFailurePolicyOnPodConditionsStatus))))
		} else if !supportedPodFailurePolicyOnPodConditionsStatus.Has(pattern.Status) {
			allErrs = append(allErrs, field.NotSupported(statusPath, pattern.Status, sets.List(supportedPodFailurePolicyOnPodConditionsStatus)))
		}
	}
	return allErrs
}

func validatePodFailurePolicyRuleOnExitCodes(onExitCode *batch.PodFailurePolicyOnExitCodesRequirement, onExitCodesPath *field.Path, containerNames sets.String) field.ErrorList {
	var allErrs field.ErrorList
	operatorPath := onExitCodesPath.Child("operator")
	if onExitCode.Operator == "" {
		allErrs = append(allErrs, field.Required(operatorPath, fmt.Sprintf("valid values: %q", sets.List(supportedPodFailurePolicyOnExitCodesOperator))))
	} else if !supportedPodFailurePolicyOnExitCodesOperator.Has(onExitCode.Operator) {
		allErrs = append(allErrs, field.NotSupported(operatorPath, onExitCode.Operator, sets.List(supportedPodFailurePolicyOnExitCodesOperator)))
	}
	if onExitCode.ContainerName != nil && !containerNames.Has(*onExitCode.ContainerName) {
		allErrs = append(allErrs, field.Invalid(onExitCodesPath.Child("containerName"), *onExitCode.ContainerName, "must be one of the container or initContainer names in the pod template"))
	}
	valuesPath := onExitCodesPath.Child("values")
	if len(onExitCode.Values) == 0 {
		allErrs = append(allErrs, field.Invalid(valuesPath, onExitCode.Values, "at least one value is required"))
	} else if len(onExitCode.Values) > maxPodFailurePolicyOnExitCodesValues {
		allErrs = append(allErrs, field.TooMany(valuesPath, len(onExitCode.Values), maxPodFailurePolicyOnExitCodesValues))
	}
	isOrdered := true
	uniqueValues := sets.NewInt32()
	for j, exitCodeValue := range onExitCode.Values {
		valuePath := valuesPath.Index(j)
		if onExitCode.Operator == batch.PodFailurePolicyOnExitCodesOpIn && exitCodeValue == 0 {
			allErrs = append(allErrs, field.Invalid(valuePath, exitCodeValue, "must not be 0 for the In operator"))
		}
		if uniqueValues.Has(exitCodeValue) {
			allErrs = append(allErrs, field.Duplicate(valuePath, exitCodeValue))
		} else {
			uniqueValues.Insert(exitCodeValue)
		}
		if j > 0 && onExitCode.Values[j-1] > exitCodeValue {
			isOrdered = false
		}
	}
	if !isOrdered {
		allErrs = append(allErrs, field.Invalid(valuesPath, onExitCode.Values, "must be ordered"))
	}

	return allErrs
}

func validateSuccessPolicy(spec *batch.JobSpec, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	rulesPath := fldPath.Child("rules")
	if len(spec.SuccessPolicy.Rules) == 0 {
		allErrs = append(allErrs, field.Required(rulesPath, "at least one rules must be specified when the successPolicy is specified"))
	}
	if len(spec.SuccessPolicy.Rules) > maxSuccessPolicyRule {
		allErrs = append(allErrs, field.TooMany(rulesPath, len(spec.SuccessPolicy.Rules), maxSuccessPolicyRule))
	}
	for i, rule := range spec.SuccessPolicy.Rules {
		allErrs = append(allErrs, validateSuccessPolicyRule(spec, &rule, rulesPath.Index(i))...)
	}
	return allErrs
}

func validateSuccessPolicyRule(spec *batch.JobSpec, rule *batch.SuccessPolicyRule, rulePath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if rule.SucceededCount == nil && rule.SucceededIndexes == nil {
		allErrs = append(allErrs, field.Required(rulePath, "at least one of succeededCount or succeededIndexes must be specified"))
	}
	var totalIndexes int32
	if rule.SucceededIndexes != nil {
		succeededIndexes := rulePath.Child("succeededIndexes")
		if len(*rule.SucceededIndexes) > maxJobSuccessPolicySucceededIndexesLimit {
			allErrs = append(allErrs, field.TooLong(succeededIndexes, "" /*unused*/, maxJobSuccessPolicySucceededIndexesLimit))
		}
		var err error
		if totalIndexes, err = validateIndexesFormat(*rule.SucceededIndexes, *spec.Completions); err != nil {
			allErrs = append(allErrs, field.Invalid(succeededIndexes, *rule.SucceededIndexes, fmt.Sprintf("error parsing succeededIndexes: %s", err.Error())))
		}
	}
	if rule.SucceededCount != nil {
		succeededCountPath := rulePath.Child("succeededCount")
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*rule.SucceededCount), succeededCountPath)...)
		if *rule.SucceededCount > *spec.Completions {
			allErrs = append(allErrs, field.Invalid(succeededCountPath, *rule.SucceededCount, fmt.Sprintf("must be less than or equal to %d (the number of specified completions)", *spec.Completions)))
		}
		if rule.SucceededIndexes != nil && *rule.SucceededCount > totalIndexes {
			allErrs = append(allErrs, field.Invalid(succeededCountPath, *rule.SucceededCount, fmt.Sprintf("must be less than or equal to %d (the number of indexes in the specified succeededIndexes field)", totalIndexes)))
		}
	}
	return allErrs
}

// validateJobStatus 校验 JobStatus，并返回包含所有错误的 ErrorList。
func validateJobStatus(job *batch.Job, fldPath *field.Path, opts JobStatusValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	status := job.Status
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.Active), fldPath.Child("active"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.Succeeded), fldPath.Child("succeeded"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.Failed), fldPath.Child("failed"))...)
	if status.Ready != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*status.Ready), fldPath.Child("ready"))...)
	}
	if status.Terminating != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*status.Terminating), fldPath.Child("terminating"))...)
	}
	if status.UncountedTerminatedPods != nil {
		path := fldPath.Child("uncountedTerminatedPods")
		seen := sets.New[types.UID]()
		for i, k := range status.UncountedTerminatedPods.Succeeded {
			p := path.Child("succeeded").Index(i)
			if k == "" {
				allErrs = append(allErrs, field.Invalid(p, k, "must not be empty"))
			} else if seen.Has(k) {
				allErrs = append(allErrs, field.Duplicate(p, k))
			} else {
				seen.Insert(k)
			}
		}
		for i, k := range status.UncountedTerminatedPods.Failed {
			p := path.Child("failed").Index(i)
			if k == "" {
				allErrs = append(allErrs, field.Invalid(p, k, "must not be empty"))
			} else if seen.Has(k) {
				allErrs = append(allErrs, field.Duplicate(p, k))
			} else {
				seen.Insert(k)
			}
		}
	}
	if opts.RejectCompleteJobWithFailedCondition {
		if IsJobComplete(job) && IsJobFailed(job) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("conditions"), field.OmitValueType{}, "cannot set Complete=True and Failed=true conditions"))
		}
	}
	if opts.RejectCompleteJobWithFailureTargetCondition {
		if IsJobComplete(job) && IsConditionTrue(status.Conditions, batch.JobFailureTarget) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("conditions"), field.OmitValueType{}, "cannot set Complete=True and FailureTarget=true conditions"))
		}
	}
	if opts.RejectNotCompleteJobWithCompletionTime {
		if status.CompletionTime != nil && !IsJobComplete(job) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("completionTime"), status.CompletionTime, "cannot set completionTime when there is no Complete=True condition"))
		}
	}
	if opts.RejectCompleteJobWithoutCompletionTime {
		if status.CompletionTime == nil && IsJobComplete(job) {
			allErrs = append(allErrs, field.Required(fldPath.Child("completionTime"), "completionTime is required for Complete jobs"))
		}
	}
	if opts.RejectCompletionTimeBeforeStartTime {
		if status.StartTime != nil && status.CompletionTime != nil && status.CompletionTime.Before(status.StartTime) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("completionTime"), status.CompletionTime, "must be equal to or after `startTime`"))
		}
	}
	if opts.RejectFailedJobWithoutFailureTarget {
		if IsJobFailed(job) && !isJobFailureTarget(job) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("conditions"), field.OmitValueType{}, "cannot set Failed=True condition without the FailureTarget=true condition"))
		}
	}
	if opts.RejectCompleteJobWithoutSuccessCriteriaMet {
		if IsJobComplete(job) && !isJobSuccessCriteriaMet(job) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("conditions"), field.OmitValueType{}, "cannot set Complete=True condition without the SuccessCriteriaMet=true condition"))
		}
	}
	isJobFinished := IsJobFinished(job)
	if opts.RejectFinishedJobWithActivePods {
		if status.Active > 0 && isJobFinished {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("active"), status.Active, "active>0 is invalid for finished job"))
		}
	}
	if opts.RejectFinishedJobWithoutStartTime {
		if status.StartTime == nil && isJobFinished {
			allErrs = append(allErrs, field.Required(fldPath.Child("startTime"), "startTime is required for finished job"))
		}
	}
	if opts.RejectFinishedJobWithUncountedTerminatedPods {
		if isJobFinished && status.UncountedTerminatedPods != nil && len(status.UncountedTerminatedPods.Failed)+len(status.UncountedTerminatedPods.Succeeded) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("uncountedTerminatedPods"), status.UncountedTerminatedPods, "must be empty for finished job"))
		}
	}
	if opts.RejectInvalidCompletedIndexes {
		if job.Spec.Completions != nil {
			if _, err := validateIndexesFormat(status.CompletedIndexes, int32(*job.Spec.Completions)); err != nil {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("completedIndexes"), status.CompletedIndexes, fmt.Sprintf("error parsing completedIndexes: %s", err.Error())))
			}
		}
	}
	if opts.RejectInvalidFailedIndexes {
		if job.Spec.Completions != nil && job.Spec.BackoffLimitPerIndex != nil && status.FailedIndexes != nil {
			if _, err := validateIndexesFormat(*status.FailedIndexes, int32(*job.Spec.Completions)); err != nil {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("failedIndexes"), status.FailedIndexes, fmt.Sprintf("error parsing failedIndexes: %s", err.Error())))
			}
		}
	}
	isIndexed := ptr.Deref(job.Spec.CompletionMode, batch.NonIndexedCompletion) == batch.IndexedCompletion
	if opts.RejectCompletedIndexesForNonIndexedJob {
		if len(status.CompletedIndexes) != 0 && !isIndexed {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("completedIndexes"), status.CompletedIndexes, "cannot set non-empty completedIndexes when non-indexed completion mode"))
		}
	}
	if opts.RejectFailedIndexesForNoBackoffLimitPerIndex {
		// 注意，此检查同时确保 FailedIndexes 不会用于普通（非 indexed）Job，
		// 因为普通 Job 的 backoffLimitPerIndex = nil。
		if job.Spec.BackoffLimitPerIndex == nil && status.FailedIndexes != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("failedIndexes"), *status.FailedIndexes, "cannot set non-null failedIndexes when backoffLimitPerIndex is null"))
		}
	}
	if opts.RejectFailedIndexesOverlappingCompleted {
		if job.Spec.Completions != nil && status.FailedIndexes != nil {
			if err := validateFailedIndexesNotOverlapCompleted(status.CompletedIndexes, *status.FailedIndexes, int32(*job.Spec.Completions)); err != nil {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("failedIndexes"), *status.FailedIndexes, err.Error()))
			}
		}
	}
	if opts.RejectFinishedJobWithTerminatingPods {
		if status.Terminating != nil && *status.Terminating > 0 && isJobFinished {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("terminating"), status.Terminating, "terminating>0 is invalid for finished job"))
		}
	}
	if opts.RejectMoreReadyThanActivePods {
		if status.Ready != nil && *status.Ready > status.Active {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("ready"), *status.Ready, "cannot set more ready pods than active"))
		}
	}
	if isJobSuccessCriteriaMet(job) && IsJobFailed(job) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("conditions"), field.OmitValueType{}, "cannot set SuccessCriteriaMet=True and Failed=true conditions"))
	}
	if isJobSuccessCriteriaMet(job) && isJobFailureTarget(job) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("conditions"), field.OmitValueType{}, "cannot set SuccessCriteriaMet=True and FailureTarget=true conditions"))
	}
	if job.Spec.SuccessPolicy != nil && !isJobSuccessCriteriaMet(job) && IsJobComplete(job) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("conditions"), field.OmitValueType{}, "cannot set Complete=True for Job with SuccessPolicy unless SuccessCriteriaMet=True"))
	}
	return allErrs
}

// ValidateJobUpdate 校验对 Job 的更新，并返回包含所有错误的 ErrorList。
func ValidateJobUpdate(job, oldJob *batch.Job, opts JobValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&job.ObjectMeta, &oldJob.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateJobSpecUpdate(job.Spec, oldJob.Spec, field.NewPath("spec"), opts)...)
	allErrs = append(allErrs, validateNameAllowsCompletions(job, opts)...)
	return allErrs
}

// ValidateJobUpdateStatus 校验对 Job 状态的更新，并返回包含所有错误的 ErrorList。
func ValidateJobUpdateStatus(job, oldJob *batch.Job, opts JobStatusValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&job.ObjectMeta, &oldJob.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateJobStatusUpdate(job, oldJob, opts)...)
	return allErrs
}

// ValidateJobSpecUpdate 校验对 JobSpec 的更新，并返回包含所有错误的 ErrorList。
func ValidateJobSpecUpdate(spec, oldSpec batch.JobSpec, fldPath *field.Path, opts JobValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateJobSpec(&spec, &oldSpec, fldPath, opts.PodValidationOptions)...)
	allErrs = append(allErrs, validateCompletions(spec, oldSpec, fldPath.Child("completions"), opts)...)
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(spec.Selector, oldSpec.Selector, fldPath.Child("selector"))...)
	allErrs = append(allErrs, validatePodTemplateUpdate(spec, oldSpec, fldPath, opts)...)
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(spec.CompletionMode, oldSpec.CompletionMode, fldPath.Child("completionMode"))...)
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(spec.PodFailurePolicy, oldSpec.PodFailurePolicy, fldPath.Child("podFailurePolicy"))...)
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(spec.BackoffLimitPerIndex, oldSpec.BackoffLimitPerIndex, fldPath.Child("backoffLimitPerIndex"))...)
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(spec.ManagedBy, oldSpec.ManagedBy, fldPath.Child("managedBy"))...)
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(spec.SuccessPolicy, oldSpec.SuccessPolicy, fldPath.Child("successPolicy"))...)
	allErrs = append(allErrs, validateJobSchedulingUpdate(spec, oldSpec, fldPath.Child("scheduling"))...)
	return allErrs
}

// gangMinCountExceedsParallelism 判断 spec 是否设置了显式的 gang minCount
// 且其值大于 parallelism。
func gangMinCountExceedsParallelism(spec *batch.JobSpec) bool {
	if spec == nil || spec.Scheduling == nil ||
		spec.Scheduling.SchedulingPolicy == nil ||
		spec.Scheduling.SchedulingPolicy.Gang == nil {
		return false
	}
	minCount := spec.Scheduling.SchedulingPolicy.Gang.MinCount
	if minCount == nil {
		return false
	}
	parallelism := int32(1)
	if spec.Parallelism != nil {
		parallelism = *spec.Parallelism
	}
	return *minCount > parallelism
}

// validateGangMinCount 拒绝超过 Job parallelism 的 gang minCount。
func validateGangMinCount(spec *batch.JobSpec, fldPath *field.Path) field.ErrorList {
	if !gangMinCountExceedsParallelism(spec) {
		return nil
	}
	parallelism := int32(1)
	if spec.Parallelism != nil {
		parallelism = *spec.Parallelism
	}
	return field.ErrorList{field.Invalid(
		fldPath.Child("schedulingPolicy", "gang", "minCount"),
		*spec.Scheduling.SchedulingPolicy.Gang.MinCount,
		fmt.Sprintf("must not be greater than parallelism (%d)", parallelism))}
}

// validateJobSchedulingUpdate 强制执行声明式校验无法表达的 spec.scheduling
// 不可变约束：basic/gang 策略变体在创建后固定不变。只有
// schedulingPolicy.gang.minCount 可以修改。
// scheduling 或 schedulingPolicy 字段本身的新增与删除由声明式校验处理，
// 因此这里仅防止原地切换变体。
func validateJobSchedulingUpdate(spec, oldSpec batch.JobSpec, fldPath *field.Path) field.ErrorList {
	if spec.Scheduling == nil || oldSpec.Scheduling == nil {
		return nil
	}
	newPolicy, oldPolicy := spec.Scheduling.SchedulingPolicy, oldSpec.Scheduling.SchedulingPolicy
	if newPolicy == nil || oldPolicy == nil {
		return nil
	}
	if (oldPolicy.Basic != nil) != (newPolicy.Basic != nil) {
		return field.ErrorList{field.Invalid(
			fldPath.Child("schedulingPolicy"), nil,
			"the basic/gang policy variant is immutable after creation; only schedulingPolicy.gang.minCount may be updated")}
	}
	return nil
}

// validateJobScheduling 通过共享的 workloadbuilder 库执行声明式校验
// 无法表达的控制器策略检查。
// 结构性构建块约束来自 DV，因此不在此重复。
// 当 spec.scheduling 未设置时为空操作。
// oldSpec 在创建时为 nil，在更新时非 nil。
func validateJobScheduling(spec, oldSpec *batch.JobSpec, fldPath *field.Path) field.ErrorList {
	if spec.Scheduling == nil {
		return nil
	}
	// 构建与 Job 控制器编译出的完全相同的逻辑工作负载树，
	// 确保校验与控制器永不漂移。
	input := jobutil.WorkloadInputForJobInternal(spec.Scheduling)
	item := jobutil.WorkloadItemForJob("job", spec.Template.Spec.PriorityClassName, spec.Parallelism, input, fldPath)

	var oldItem *workloadbuilder.WorkloadItem
	if oldSpec != nil {
		oldInput := jobutil.WorkloadInputForJobInternal(oldSpec.Scheduling)
		// 旧条目的 Path 不会被使用：Validate 会在新条目的
		// Path 上报错，因此这里保持为 nil。
		oldItem = jobutil.WorkloadItemForJob("job", oldSpec.Template.Spec.PriorityClassName, oldSpec.Parallelism, oldInput, nil) // +k8s:verify-mutation:reason=clone
	}

	return workloadbuilder.NewBuilder(item, workloadbuilder.BuildOptions{
		AllowedPolicies:        []workloadbuilder.SchedulingPolicyOption{workloadbuilder.BasicPolicy, workloadbuilder.GangPolicy},
		AllowedDisruptionModes: []workloadbuilder.DisruptionModeOption{workloadbuilder.SingleMode, workloadbuilder.AllMode},
		// apiserver 已经对版本化的构建块运行了声明式校验，因此这里的
		// Validate 只针对解析后的配置执行控制器策略检查；
		// 它既不编译 Workload，也不需要 owner。
		DisableDeclarativeValidation: true,
	}).Validate(context.Background(), workloadbuilder.ValidationInput{OldRoot: oldItem})
}

func validatePodTemplateUpdate(spec, oldSpec batch.JobSpec, fldPath *field.Path, opts JobValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	template := &spec.Template
	oldTemplate := &oldSpec.Template
	if opts.AllowMutableSchedulingDirectives {
		oldTemplate = oldSpec.Template.DeepCopy() // +k8s:verify-mutation:reason=clone
		switch {
		case template.Spec.Affinity == nil && oldTemplate.Spec.Affinity != nil:
			// 如果旧模板除 NodeAffinity 外没有其他亲和性指令，则允许清空 Affinity 字段
			oldTemplate.Spec.Affinity.NodeAffinity = nil // +k8s:verify-mutation:reason=clone
			if (*oldTemplate.Spec.Affinity) == (api.Affinity{}) {
				oldTemplate.Spec.Affinity = nil // +k8s:verify-mutation:reason=clone
			}
		case template.Spec.Affinity != nil && oldTemplate.Spec.Affinity == nil:
			// 允许 NodeAffinity 字段跳过不可变性检查
			oldTemplate.Spec.Affinity = &api.Affinity{NodeAffinity: template.Spec.Affinity.NodeAffinity} // +k8s:verify-mutation:reason=clone
		case template.Spec.Affinity != nil && oldTemplate.Spec.Affinity != nil:
			// 允许 NodeAffinity 字段跳过不可变性检查
			oldTemplate.Spec.Affinity.NodeAffinity = template.Spec.Affinity.NodeAffinity // +k8s:verify-mutation:reason=clone
		}
		oldTemplate.Spec.NodeSelector = template.Spec.NodeSelector       // +k8s:verify-mutation:reason=clone
		oldTemplate.Spec.Tolerations = template.Spec.Tolerations         // +k8s:verify-mutation:reason=clone
		oldTemplate.Annotations = template.Annotations                   // +k8s:verify-mutation:reason=clone
		oldTemplate.Labels = template.Labels                             // +k8s:verify-mutation:reason=clone
		oldTemplate.Spec.SchedulingGates = template.Spec.SchedulingGates // +k8s:verify-mutation:reason=clone
	}
	suspended := oldSpec.Suspend != nil && *oldSpec.Suspend
	if !suspended {
		// 如果 Job 未处于挂起状态，则不允许对模板做任何修改
		allErrs = append(allErrs, apivalidation.ValidateImmutableField(template, oldTemplate, fldPath.Child("template"))...)
	} else {
		if !opts.AllowMutablePodResources {
			allErrs = append(allErrs, apivalidation.ValidateImmutableField(template, oldTemplate, fldPath.Child("template"))...)
		} else {
			// 仅当 AllowMutablePodResources 为 true 时允许更新容器资源
			allErrs = append(allErrs, validatePodResourceUpdatesOnly(template.Spec, oldTemplate.Spec, fldPath.Child("template.spec"))...)
		}
	}
	return allErrs
}

// validatePodResourceUpdatesOnly 校验是否仅更新了容器资源。
func validatePodResourceUpdatesOnly(newPod api.PodSpec, oldPod api.PodSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 创建旧 Pod spec 的深拷贝以便修改
	oldPodCopy := oldPod.DeepCopy()
	// 在拷贝中更新容器资源，使其与新 Pod spec 一致
	if len(newPod.Containers) == len(oldPodCopy.Containers) {
		for i := range newPod.Containers {
			if oldPodCopy.Containers[i].Name == newPod.Containers[i].Name {
				oldPodCopy.Containers[i].Resources = newPod.Containers[i].Resources // +k8s:verify-mutation:reason=clone
			}
		}
	}

	// 在拷贝中更新 init 容器资源，使其与新 Pod spec 一致
	if len(newPod.InitContainers) == len(oldPodCopy.InitContainers) {
		for i := range newPod.InitContainers {
			if oldPodCopy.InitContainers[i].Name == newPod.InitContainers[i].Name {
				oldPodCopy.InitContainers[i].Resources = newPod.InitContainers[i].Resources // +k8s:verify-mutation:reason=clone
			}
		}
	}

	// 校验修改后的拷贝与新 Pod spec 完全一致，
	// 以确保只有容器资源发生了变化
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(newPod, *oldPodCopy, fldPath)...)

	return allErrs
}

// ValidateJobStatusUpdate 校验对 JobStatus 的更新，并返回包含所有错误的 ErrorList。
func ValidateJobStatusUpdate(job, oldJob *batch.Job, opts JobStatusValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	statusFld := field.NewPath("status")
	allErrs = append(allErrs, validateJobStatus(job, statusFld, opts)...)

	if opts.RejectDisablingTerminalCondition {
		for _, cType := range []batch.JobConditionType{batch.JobFailed, batch.JobComplete, batch.JobFailureTarget} {
			if IsConditionTrue(oldJob.Status.Conditions, cType) && !IsConditionTrue(job.Status.Conditions, cType) {
				allErrs = append(allErrs, field.Invalid(statusFld.Child("conditions"), field.OmitValueType{}, fmt.Sprintf("cannot disable the terminal %s=True condition", string(cType))))
			}
		}
	}
	if opts.RejectDecreasingFailedCounter {
		if job.Status.Failed < oldJob.Status.Failed {
			allErrs = append(allErrs, field.Invalid(statusFld.Child("failed"), job.Status.Failed, "cannot decrease the failed counter"))
		}
	}
	if opts.RejectDecreasingSucceededCounter {
		if job.Status.Succeeded < oldJob.Status.Succeeded {
			allErrs = append(allErrs, field.Invalid(statusFld.Child("succeeded"), job.Status.Succeeded, "cannot decrease the succeeded counter"))
		}
	}
	if opts.RejectMutatingCompletionTime {
		// 注意，仅当 `job.Status.CompletionTime != nil` 时才检查该条件，
		// 这是因为我们不想在 Job 尚未结束的情况下阻止
		// 向 completionTime = nil 的转换。
		// 对已结束的 Job 设置 completionTime = nil 的情形由
		// RejectCompleteJobWithoutCompletionTime 负责阻止。
		if job.Status.CompletionTime != nil && oldJob.Status.CompletionTime != nil && !ptr.Equal(job.Status.CompletionTime, oldJob.Status.CompletionTime) {
			allErrs = append(allErrs, field.Invalid(statusFld.Child("completionTime"), job.Status.CompletionTime, "field is immutable"))
		}
	}
	if opts.RejectStartTimeUpdateForUnsuspendedJob {
		// 注意，这里检查 `oldJob.Status.StartTime != nil`，是为了允许未挂起的 Job
		// 从 startTime = nil 转换到 startTime != nil，这是期望的转换。
		if oldJob.Status.StartTime != nil && !ptr.Equal(oldJob.Status.StartTime, job.Status.StartTime) && !ptr.Deref(job.Spec.Suspend, false) {
			allErrs = append(allErrs, field.Invalid(statusFld.Child("startTime"), job.Status.StartTime, "field is immutable for unsuspended job once set"))
		}
	}
	if isJobSuccessCriteriaMet(oldJob) && !isJobSuccessCriteriaMet(job) {
		allErrs = append(allErrs, field.Invalid(statusFld.Child("conditions"), field.OmitValueType{}, "cannot disable the SuccessCriteriaMet=True condition"))
	}
	if IsJobComplete(oldJob) && !isJobSuccessCriteriaMet(oldJob) && isJobSuccessCriteriaMet(job) {
		allErrs = append(allErrs, field.Invalid(statusFld.Child("conditions"), field.OmitValueType{}, "cannot set SuccessCriteriaMet=True for Job already has Complete=true conditions"))
	}
	return allErrs
}

// ValidateCronJobCreate 校验新创建的 CronJob，并返回包含所有错误的 ErrorList。
func ValidateCronJobCreate(cronJob *batch.CronJob, opts apivalidation.PodValidationOptions) field.ErrorList {
	// CronJob 和 RC 使用相同的名称校验规则
	allErrs := apivalidation.ValidateObjectMeta(&cronJob.ObjectMeta, true, apimachineryapivalidation.NameIsDNSSubdomain, field.NewPath("metadata"))
	allErrs = append(allErrs, validateCronJobSpec(&cronJob.Spec, nil, field.NewPath("spec"), opts)...)
	if len(cronJob.ObjectMeta.Name) > apimachineryvalidation.DNS1035LabelMaxLength-11 {
		// CronJob 控制器在创建 Job 时会在 cronjob 名称后追加 11 个字符的后缀
		// （`-$TIMESTAMP`）。Job 名称长度上限为 63 个字符。
		// 因此 cronjob 名称长度必须 <= 63-11=52。如果此处不校验，
		// Job 的创建稍后会失败。
		allErrs = append(allErrs, field.Invalid(field.NewPath("metadata").Child("name"), cronJob.ObjectMeta.Name, "must be no more than 52 characters"))
	}
	return allErrs
}

// ValidateCronJobUpdate 校验对 CronJob 的更新，并返回包含所有错误的 ErrorList。
func ValidateCronJobUpdate(job, oldJob *batch.CronJob, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&job.ObjectMeta, &oldJob.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, validateCronJobSpec(&job.Spec, &oldJob.Spec, field.NewPath("spec"), opts)...)

	// 更新校验时跳过 52 字符的名称长度限制，
	// 以便名称超过 52 个字符的旧 cronjob 也能被更新/删除
	return allErrs
}

// validateCronJobSpec 校验 CronJobSpec，并返回包含所有错误的 ErrorList。
func validateCronJobSpec(spec, oldSpec *batch.CronJobSpec, fldPath *field.Path, opts apivalidation.PodValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(spec.Schedule) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("schedule"), "").MarkCoveredByDeclarative())
	} else {
		allowTZInSchedule := false
		if oldSpec != nil {
			allowTZInSchedule = strings.Contains(oldSpec.Schedule, "TZ")
		}
		allErrs = append(allErrs, validateScheduleFormat(spec.Schedule, allowTZInSchedule, spec.TimeZone, fldPath.Child("schedule"))...)
	}

	if spec.StartingDeadlineSeconds != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.StartingDeadlineSeconds), fldPath.Child("startingDeadlineSeconds"))...)
	}

	if oldSpec == nil || !ptr.Equal(oldSpec.TimeZone, spec.TimeZone) {
		allErrs = append(allErrs, validateTimeZone(spec.TimeZone, fldPath.Child("timeZone"))...)
	}

	var oldTemplateSpec *batch.JobTemplateSpec
	if oldSpec != nil {
		oldTemplateSpec = &oldSpec.JobTemplate // +k8s:verify-mutation:reason=clone
	}
	allErrs = append(allErrs, validateConcurrencyPolicy(&spec.ConcurrencyPolicy, fldPath.Child("concurrencyPolicy"))...)
	allErrs = append(allErrs, ValidateJobTemplateSpec(&spec.JobTemplate, oldTemplateSpec, fldPath.Child("jobTemplate"), opts)...)

	if spec.SuccessfulJobsHistoryLimit != nil {
		// 0 是合法的 SuccessfulJobsHistoryLimit
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.SuccessfulJobsHistoryLimit), fldPath.Child("successfulJobsHistoryLimit"))...)
	}
	if spec.FailedJobsHistoryLimit != nil {
		// 0 是合法的 SuccessfulJobsHistoryLimit
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*spec.FailedJobsHistoryLimit), fldPath.Child("failedJobsHistoryLimit"))...)
	}

	return allErrs
}

func validateConcurrencyPolicy(concurrencyPolicy *batch.ConcurrencyPolicy, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	switch *concurrencyPolicy {
	case batch.AllowConcurrent, batch.ForbidConcurrent, batch.ReplaceConcurrent:
		break
	case "":
		allErrs = append(allErrs, field.Required(fldPath, ""))
	default:
		validValues := []batch.ConcurrencyPolicy{batch.AllowConcurrent, batch.ForbidConcurrent, batch.ReplaceConcurrent}
		allErrs = append(allErrs, field.NotSupported(fldPath, *concurrencyPolicy, validValues))
	}

	return allErrs
}

func validateScheduleFormat(schedule string, allowTZInSchedule bool, timeZone *string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if _, err := parsers.ParseCronScheduleWithPanicRecovery(schedule); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, schedule, err.Error()))
	}
	switch {
	case allowTZInSchedule && strings.Contains(schedule, "TZ") && timeZone != nil:
		allErrs = append(allErrs, field.Invalid(fldPath, schedule, "cannot use both timeZone field and TZ or CRON_TZ in schedule"))
	case !allowTZInSchedule && strings.Contains(schedule, "TZ"):
		allErrs = append(allErrs, field.Invalid(fldPath, schedule, "cannot use TZ or CRON_TZ in schedule, use timeZone field instead"))
	}

	return allErrs
}

// https://data.iana.org/time-zones/theory.html#naming
// * 名称不能为空，不能包含 '//'，也不能以 '/' 开头或结尾。
// * 不要使用文件名组件 '.' 和 '..'。
// * 在文件名组件内，只能使用 ASCII 字母、'.'、'-' 和 '_'。
// * 不要使用数字，因为这可能与 POSIX TZ 字符串产生歧义。
// * 文件名组件长度不得超过 14 个字符，也不得以 '-' 开头
//
// 为兼容历史遗留名称，容忍使用 0-9 和 + 字符
var validTimeZoneCharacters = regexp.MustCompile(`^[A-Za-z\.\-_0-9+]{1,14}$`)

func validateTimeZone(timeZone *string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if timeZone == nil {
		return allErrs
	}

	if len(*timeZone) == 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, timeZone, "timeZone must be nil or non-empty string"))
		return allErrs
	}

	for _, part := range strings.Split(*timeZone, "/") {
		if part == "." || part == ".." || strings.HasPrefix(part, "-") || !validTimeZoneCharacters.MatchString(part) {
			allErrs = append(allErrs, field.Invalid(fldPath, timeZone, fmt.Sprintf("unknown time zone %s", *timeZone)))
			return allErrs
		}
	}

	if strings.EqualFold(*timeZone, "Local") {
		allErrs = append(allErrs, field.Invalid(fldPath, timeZone, "timeZone must be an explicit time zone as defined in https://www.iana.org/time-zones"))
	}

	if _, err := time.LoadLocation(*timeZone); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, timeZone, err.Error()))
	}

	return allErrs
}

// ValidateJobTemplateSpec 校验 JobTemplateSpec，并返回包含所有错误的 ErrorList。
// oldSpec 在创建时为 nil，在更新时非 nil。
func ValidateJobTemplateSpec(spec, oldSpec *batch.JobTemplateSpec, fldPath *field.Path, opts apivalidation.PodValidationOptions) field.ErrorList {
	var oldJobSpec *batch.JobSpec
	if oldSpec != nil {
		oldJobSpec = &oldSpec.Spec // +k8s:verify-mutation:reason=clone
	}
	allErrs := validateJobSpec(&spec.Spec, oldJobSpec, fldPath.Child("spec"), opts)

	// jobTemplate 的选择器将始终自动生成
	if spec.Spec.Selector != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("spec", "selector"), spec.Spec.Selector, "`selector` will be auto-generated"))
	}
	if spec.Spec.ManualSelector != nil && *spec.Spec.ManualSelector {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("spec", "manualSelector"), spec.Spec.ManualSelector, []string{"nil", "false"}))
	}
	return allErrs
}

func validateCompletions(spec, oldSpec batch.JobSpec, fldPath *field.Path, opts JobValidationOptions) field.ErrorList {
	// 对于非 Indexed 的 Job，Completions 不可变；对于 Indexed Job，则可变。
	isIndexedJob := spec.CompletionMode != nil && *spec.CompletionMode == batch.IndexedCompletion
	if !isIndexedJob {
		return apivalidation.ValidateImmutableField(spec.Completions, oldSpec.Completions, fldPath)
	}

	var allErrs field.ErrorList
	if apiequality.Semantic.DeepEqual(spec.Completions, oldSpec.Completions) {
		return allErrs
	}
	// Indexed Job 不能将 completions 设为 nil。
	// validateJobSpec 中已经做了 nil 检查，无需再叠加错误。
	if spec.Completions == nil {
		return allErrs
	}

	if *spec.Completions != *spec.Parallelism {
		allErrs = append(allErrs, field.Invalid(fldPath, spec.Completions, fmt.Sprintf("can only be modified in tandem with %s", fldPath.Root().Child("parallelism").String())))
	}
	return allErrs
}

func IsJobFinished(job *batch.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == batch.JobComplete || c.Type == batch.JobFailed) && c.Status == api.ConditionTrue {
			return true
		}
	}
	return false
}

func IsJobComplete(job *batch.Job) bool {
	return IsConditionTrue(job.Status.Conditions, batch.JobComplete)
}

func IsJobFailed(job *batch.Job) bool {
	return IsConditionTrue(job.Status.Conditions, batch.JobFailed)
}

func isJobSuccessCriteriaMet(job *batch.Job) bool {
	return IsConditionTrue(job.Status.Conditions, batch.JobSuccessCriteriaMet)
}

func isJobFailureTarget(job *batch.Job) bool {
	return IsConditionTrue(job.Status.Conditions, batch.JobFailureTarget)
}

func IsConditionTrue(list []batch.JobCondition, cType batch.JobConditionType) bool {
	for _, c := range list {
		if c.Type == cType && c.Status == api.ConditionTrue {
			return true
		}
	}
	return false
}

func IsConditionFalse(list []batch.JobCondition, cType batch.JobConditionType) bool {
	for _, c := range list {
		if c.Type == cType && c.Status == api.ConditionFalse {
			return true
		}
	}
	return false
}

func validateFailedIndexesNotOverlapCompleted(completedIndexesStr string, failedIndexesStr string, completions int32) error {
	if len(completedIndexesStr) == 0 || len(failedIndexesStr) == 0 {
		return nil
	}
	completedIndexesIntervals := strings.Split(completedIndexesStr, ",")
	failedIndexesIntervals := strings.Split(failedIndexesStr, ",")
	var completedPos, failedPos int
	cX, cY, cErr := parseIndexInterval(completedIndexesIntervals[completedPos], completions)
	fX, fY, fErr := parseIndexInterval(failedIndexesIntervals[failedPos], completions)
	for completedPos < len(completedIndexesIntervals) && failedPos < len(failedIndexesIntervals) {
		if cErr != nil {
			// 解析 "completed" 区间失败。我们继续处理下一个区间，
			// 该错误会在校验格式时报告给用户。
			completedPos++
			if completedPos < len(completedIndexesIntervals) {
				cX, cY, cErr = parseIndexInterval(completedIndexesIntervals[completedPos], completions)
			}
		} else if fErr != nil {
			// 解析 "failed" 区间失败。我们继续处理下一个区间，
			// 该错误会在校验格式时报告给用户。
			failedPos++
			if failedPos < len(failedIndexesIntervals) {
				fX, fY, fErr = parseIndexInterval(failedIndexesIntervals[failedPos], completions)
			}
		} else {
			// 已解析出一个 failed 区间和一个 completed 区间。
			if cX <= fY && fX <= cY {
				return fmt.Errorf("failedIndexes and completedIndexes overlap at index: %d", max(cX, fX))
			}
			// 无重叠，继续处理下一个区间。
			if cX <= fX {
				completedPos++
				if completedPos < len(completedIndexesIntervals) {
					cX, cY, cErr = parseIndexInterval(completedIndexesIntervals[completedPos], completions)
				}
			} else {
				failedPos++
				if failedPos < len(failedIndexesIntervals) {
					fX, fY, fErr = parseIndexInterval(failedIndexesIntervals[failedPos], completions)
				}
			}
		}
	}
	return nil
}

func validateIndexesFormat(indexesStr string, completions int32) (int32, error) {
	if len(indexesStr) == 0 {
		return 0, nil
	}
	var lastIndex *int32
	var total int32
	for _, intervalStr := range strings.Split(indexesStr, ",") {
		x, y, err := parseIndexInterval(intervalStr, completions)
		if err != nil {
			return 0, err
		}
		if lastIndex != nil && *lastIndex >= x {
			return 0, fmt.Errorf("non-increasing order, previous: %d, current: %d", *lastIndex, x)
		}
		total += y - x + 1
		lastIndex = &y
	}
	return total, nil
}

func parseIndexInterval(intervalStr string, completions int32) (int32, int32, error) {
	limitsStr := strings.Split(intervalStr, "-")
	if len(limitsStr) > 2 {
		return 0, 0, fmt.Errorf("the fragment %q violates the requirement that an index interval can have at most two parts separated by '-'", intervalStr)
	}
	x, err := strconv.Atoi(limitsStr[0])
	if err != nil {
		return 0, 0, fmt.Errorf("cannot convert string to integer for index: %q", limitsStr[0])
	}
	if x >= int(completions) {
		return 0, 0, fmt.Errorf("too large index: %q", limitsStr[0])
	}
	if len(limitsStr) > 1 {
		y, err := strconv.Atoi(limitsStr[1])
		if err != nil {
			return 0, 0, fmt.Errorf("cannot convert string to integer for index: %q", limitsStr[1])
		}
		if y >= int(completions) {
			return 0, 0, fmt.Errorf("too large index: %q", limitsStr[1])
		}
		if x >= y {
			return 0, 0, fmt.Errorf("non-increasing order, previous: %d, current: %d", x, y)
		}
		return int32(x), int32(y), nil
	}
	return int32(x), int32(x), nil
}

type JobValidationOptions struct {
	apivalidation.PodValidationOptions
	// 允许模板的节点亲和性、选择器和容忍度可变
	AllowMutableSchedulingDirectives bool
	// 要求 Job 带有 batch.kubernetes.io/job-name 和 batch.kubernetes.io/controller-uid 标签
	RequirePrefixedLabels bool
	// 允许 Pod 资源可变
	AllowMutablePodResources bool
}

type JobStatusValidationOptions struct {
	RejectDecreasingSucceededCounter             bool
	RejectDecreasingFailedCounter                bool
	RejectDisablingTerminalCondition             bool
	RejectInvalidCompletedIndexes                bool
	RejectInvalidFailedIndexes                   bool
	RejectFailedIndexesOverlappingCompleted      bool
	RejectCompletedIndexesForNonIndexedJob       bool
	RejectFailedIndexesForNoBackoffLimitPerIndex bool
	RejectFailedJobWithoutFailureTarget          bool
	RejectCompleteJobWithoutSuccessCriteriaMet   bool
	RejectFinishedJobWithActivePods              bool
	RejectFinishedJobWithoutStartTime            bool
	RejectFinishedJobWithUncountedTerminatedPods bool
	RejectStartTimeUpdateForUnsuspendedJob       bool
	RejectCompletionTimeBeforeStartTime          bool
	RejectMutatingCompletionTime                 bool
	RejectCompleteJobWithoutCompletionTime       bool
	RejectNotCompleteJobWithCompletionTime       bool
	RejectCompleteJobWithFailedCondition         bool
	RejectCompleteJobWithFailureTargetCondition  bool
	RejectMoreReadyThanActivePods                bool
	RejectFinishedJobWithTerminatingPods         bool
}
