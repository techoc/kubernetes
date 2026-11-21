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

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate/content"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
)

const (
	// MaxPeriodSeconds 是扩缩容策略允许的最大周期（单位：秒）。
	// 设置上限可防止配置过长的统计窗口，导致策略计算失真或异常波动被人为放大。
	MaxPeriodSeconds int32 = 1800
	// MaxStabilizationWindowSeconds 是允许的最大稳定窗口（单位：秒）。
	// 稳定窗口用于抑制短时间内的反复扩缩；上限保证 HPA 能在合理时间内收敛。
	MaxStabilizationWindowSeconds int32 = 3600
)

// ValidateScale 校验一个 Scale 对象，并返回包含所有校验错误的 ErrorList。
func ValidateScale(scale *autoscaling.Scale) field.ErrorList {
	allErrs := field.ErrorList{}
	// 只校验 managedFields 与 replicas；其他 metadata 字段会被 scale 子资源处理器
	// 忽略并丢弃，因此在这里校验它们没有意义。
	allErrs = append(allErrs, metav1validation.ValidateManagedFields(scale.GetManagedFields(), field.NewPath("metadata").Child("managedFields"))...)
	// 副本数小于零必然非法；该规则已由声明式校验覆盖，因此标记以避免重复报错。
	if scale.Spec.Replicas < 0 {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(scale.Spec.Replicas), field.NewPath("spec", "replicas")).MarkCoveredByDeclarative()...)
	}

	return allErrs
}

// ValidateScaleUpdate 是手写 Scale 校验与声明式校验的唯一组合入口，
// 供所有 in-tree 的 ScaleREST.UpdatedObject 调用
// （replicationcontroller、deployment、replicaset、statefulset），
// 同时也供 test/declarative_validation/{autoscaling,apps}/scale 下的测试调用。
// mapper 将请求中父资源的 group/version 映射到对应的 Scale GVK；
// 测试中传入 nil，因为它们直接把 RequestInfo 设置为 Scale 的 GV。
// 注意：这里的 mapper 只影响声明式校验查找 Scale GVK 的方式，不改变手写校验结果。
func ValidateScaleUpdate(ctx context.Context, newScale, oldScale *autoscaling.Scale, scheme *runtime.Scheme, mapper rest.GroupVersionKindProvider) field.ErrorList {
	errs := ValidateScale(newScale)
	dv := rest.DeclarativeValidation{Scheme: scheme}
	return dv.ValidateDeclaratively(ctx, newScale, oldScale, errs, operation.Update, rest.DeclarativeValidationConfig{SubresourceGVKMapper: mapper})
}

// ValidateHorizontalPodAutoscalerName 用于检查给定的自动扩缩容名称是否合法。
// Prefix 表示这个名称会被用于生成其他名字，此时允许末尾带连字符。
var ValidateHorizontalPodAutoscalerName = apimachineryvalidation.NameIsDNSSubdomain

// validateHorizontalPodAutoscalerSpec 校验 HPA spec：副本数边界、扩缩容目标引用、
// 指标来源以及扩缩容行为配置。
// opts 由调用方（含声明式校验路径）传入，用于统一上下界和兼容性开关。
func validateHorizontalPodAutoscalerSpec(autoscaler autoscaling.HorizontalPodAutoscalerSpec, fldPath *field.Path, opts HorizontalPodAutoscalerSpecValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	// minReplicas 可省略；一旦填写就不能低于 API 规定的下界。
	if autoscaler.MinReplicas != nil && *autoscaler.MinReplicas < opts.MinReplicasLowerBound {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("minReplicas"), *autoscaler.MinReplicas,
			fmt.Sprintf("must be greater than or equal to %d", opts.MinReplicasLowerBound)).WithOrigin("minimum").MarkCoveredByDeclarative())
	}
	// maxReplicas 必须显式设置且为正数：0 视为漏填，负数单独按数值非法报错。
	if autoscaler.MaxReplicas == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("maxReplicas"), "must be set and greater than 0").MarkCoveredByDeclarative())
	} else if autoscaler.MaxReplicas < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("maxReplicas"), autoscaler.MaxReplicas, "must be greater than or equal to 1").WithOrigin("minimum").MarkCoveredByDeclarative())
	}
	// maxReplicas 不能小于 minReplicas，否则 HPA 无法构造合法的副本数区间。
	if autoscaler.MinReplicas != nil && autoscaler.MaxReplicas < *autoscaler.MinReplicas {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("maxReplicas"), autoscaler.MaxReplicas, "must be greater than or equal to `minReplicas`"))
	}
	// scaleTargetRef 指向被扩缩对象，需要单独校验其 Kind/Name/apiVersion。
	if refErrs := ValidateCrossVersionObjectReference(autoscaler.ScaleTargetRef, fldPath.Child("scaleTargetRef"), opts.ScaleTargetRefValidationOptions); len(refErrs) > 0 {
		allErrs = append(allErrs, refErrs...)
	}
	// 指标校验依赖 minReplicas：缩容到零要求至少有一个 Object 或 External 指标。
	if refErrs := validateMetrics(autoscaler.Metrics, fldPath.Child("metrics"), autoscaler.MinReplicas, opts.ObjectMetricsValidationOptions); len(refErrs) > 0 {
		allErrs = append(allErrs, refErrs...)
	}
	// behavior 可为空；非空时分别校验扩容与缩容策略。
	if refErrs := validateBehavior(autoscaler.Behavior, fldPath.Child("behavior"), opts); len(refErrs) > 0 {
		allErrs = append(allErrs, refErrs...)
	}
	return allErrs
}

// ValidateCrossVersionObjectReference 校验一个 CrossVersionObjectReference，
// 并返回包含所有校验错误的 ErrorList。
func ValidateCrossVersionObjectReference(ref autoscaling.CrossVersionObjectReference, fldPath *field.Path, opts CrossVersionObjectReferenceValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	// Kind 不能为空；空值直接报“必填”，非空时再检查是否为合法的路径段名称。
	if len(ref.Kind) == 0 {
		err := field.Required(fldPath.Child("kind"), "")
		if opts.RequiredCoveredByDeclarative {
			err = err.MarkCoveredByDeclarative()
		}
		allErrs = append(allErrs, err)
	} else {
		// Kind/Name 会作为 URL 路径段使用，因此复用路径段名称规则校验。
		for _, msg := range content.IsPathSegmentName(ref.Kind) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("kind"), ref.Kind, msg))
		}
	}

	// Name 同样不允许为空。
	if len(ref.Name) == 0 {
		err := field.Required(fldPath.Child("name"), "")
		if opts.RequiredCoveredByDeclarative {
			err = err.MarkCoveredByDeclarative()
		}
		allErrs = append(allErrs, err)
	} else {
		// Name 同样作为路径段使用，校验其合法性。
		for _, msg := range content.IsPathSegmentName(ref.Name) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), ref.Name, msg))
		}
	}
	// apiVersion 的可解析性与 API group 要求由 ValidateAPIVersion 统一判断。
	if err := ValidateAPIVersion(ref, opts); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("apiVersion"), ref.APIVersion, err.Error()))
	}
	return allErrs
}

// ValidateAPIVersion 校验 CrossVersionObjectReference 的 apiVersion 字段。
// 返回 nil 表示合法；调用方据此构造 field.Invalid 错误。
func ValidateAPIVersion(ref autoscaling.CrossVersionObjectReference, opts CrossVersionObjectReferenceValidationOptions) error {
	// 显式放行时不解析 apiVersion，保持与旧版本对象的兼容性。
	if opts.AllowInvalidAPIVersion {
		return nil
	}
	// 先按 group/version 语法解析；解析失败的错误信息直接反馈给调用方。
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return err
	} else if !opts.AllowEmptyAPIGroup && gv.Group == "" {
		// 默认要求写明 API group，避免 core group 的空字符串造成引用歧义。
		return fmt.Errorf("apiVersion must specify API group")
	}
	return nil
}

// ValidateHorizontalPodAutoscaler 校验一个 HorizontalPodAutoscaler，
// 并返回包含所有校验错误的 ErrorList。
func ValidateHorizontalPodAutoscaler(autoscaler *autoscaling.HorizontalPodAutoscaler, opts HorizontalPodAutoscalerSpecValidationOptions) field.ErrorList {
	// 创建时校验元数据的命名空间与名称合法性；spec 的错误随后追加到同一错误列表。
	allErrs := apivalidation.ValidateObjectMeta(&autoscaler.ObjectMeta, true, ValidateHorizontalPodAutoscalerName, field.NewPath("metadata"))
	allErrs = append(allErrs, validateHorizontalPodAutoscalerSpec(autoscaler.Spec, field.NewPath("spec"), opts)...)
	return allErrs
}

// ValidateHorizontalPodAutoscalerUpdate 校验一次 HorizontalPodAutoscaler 更新，
// 并返回包含所有校验错误的 ErrorList。
func ValidateHorizontalPodAutoscalerUpdate(newAutoscaler, oldAutoscaler *autoscaling.HorizontalPodAutoscaler, opts HorizontalPodAutoscalerSpecValidationOptions) field.ErrorList {
	// 更新时先校验元数据（如名称、命名空间不可变），再按与创建相同的规则校验 spec。
	allErrs := apivalidation.ValidateObjectMetaUpdate(&newAutoscaler.ObjectMeta, &oldAutoscaler.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, validateHorizontalPodAutoscalerSpec(newAutoscaler.Spec, field.NewPath("spec"), opts)...)
	return allErrs
}

// ValidateHorizontalPodAutoscalerStatusUpdate 校验 HorizontalPodAutoscaler 的 status 更新，
// 并返回包含所有校验错误的 ErrorList。
// status 是独立子资源：只校验元数据更新与状态字段的取值，不重新校验 spec。
// 返回包含所有校验错误的 ErrorList。
func ValidateHorizontalPodAutoscalerStatusUpdate(newAutoscaler, oldAutoscaler *autoscaling.HorizontalPodAutoscaler) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&newAutoscaler.ObjectMeta, &oldAutoscaler.ObjectMeta, field.NewPath("metadata"))
	status := newAutoscaler.Status
	// condition 上记录的 observedGeneration 也是副本数语义，必须非负。
	for i, condition := range status.Conditions {
		if condition.ObservedGeneration != nil {
			allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(*condition.ObservedGeneration, field.NewPath("status").Child("conditions").Index(i).Child("observedGeneration"))...)
		}
	}
	// 当前副本数与期望副本数都不能为负数。
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.CurrentReplicas), field.NewPath("status", "currentReplicas"))...)
	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(status.DesiredReplicas), field.NewPath("status", "desiredReplicas"))...)
	return allErrs
}

// CrossVersionObjectReferenceValidationOptions 保存校验
// CrossVersionObjectReference 时可用的各项开关。
// 其中 RequiredCoveredByDeclarative 只针对必填错误，与格式错误无关。
type CrossVersionObjectReferenceValidationOptions struct {
	// 是否允许 API 版本（API group）为空。
	AllowEmptyAPIGroup bool
	// AllowInvalidAPIVersion 为 true 时跳过 apiVersion 的格式校验。
	AllowInvalidAPIVersion bool
	// RequiredCoveredByDeclarative 将 Kind 和 Name 的“必填”错误标记为已由声明式校验覆盖。
	// 只有声明式校验确实会校验该引用时（scaleTargetRef）才应设为 true；
	// describedObject 位于不透明的 metrics 子树下，只由手写校验负责，应保持为 false。
	RequiredCoveredByDeclarative bool
}

// HorizontalPodAutoscalerSpecValidationOptions 保存 HPA spec 校验所需的各项配置。
// ScaleTargetRef 与 Object 指标共用引用校验逻辑，但允许的开关不同。
type HorizontalPodAutoscalerSpecValidationOptions struct {
	// minReplicas 允许的最小值。
	MinReplicasLowerBound           int32
	ScaleTargetRefValidationOptions CrossVersionObjectReferenceValidationOptions
	ObjectMetricsValidationOptions  CrossVersionObjectReferenceValidationOptions
}

// validateMetrics 逐条校验指标，并检查“缩容到零”的额外前置条件。
// minReplicas 为 0 时，HPA 必须依赖 Object 或 External 指标才能把副本数降到零。
func validateMetrics(newMetrics []autoscaling.MetricSpec, fldPath *field.Path, minReplicas *int32, opts CrossVersionObjectReferenceValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	// 遍历时顺带记录是否出现 Object/External 类型，供后续缩容到零检查使用。
	hasObjectMetrics := false
	hasExternalMetrics := false
	for i, metricSpec := range newMetrics {
		idxPath := fldPath.Index(i)
		if targetErrs := validateMetricSpec(metricSpec, idxPath, opts); len(targetErrs) > 0 {
			allErrs = append(allErrs, targetErrs...)
		}
		if metricSpec.Type == autoscaling.ObjectMetricSourceType {
			hasObjectMetrics = true
		}
		if metricSpec.Type == autoscaling.ExternalMetricSourceType {
			hasExternalMetrics = true
		}
	}

	// Resource/Pods/ContainerResource 指标没有外部数值可用，无法支持缩容到零。
	if minReplicas != nil && *minReplicas == 0 {
		if !hasObjectMetrics && !hasExternalMetrics {
			allErrs = append(allErrs, field.Forbidden(fldPath, "must specify at least one Object or External metric to support scaling to zero replicas"))
		}
	}

	return allErrs
}

// validateBehavior 在 behavior 非空时分别校验扩容和缩容两组的稳定窗口、策略选择等配置。
func validateBehavior(behavior *autoscaling.HorizontalPodAutoscalerBehavior, fldPath *field.Path, opts HorizontalPodAutoscalerSpecValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if behavior != nil {
		if scaleUpErrs := validateScalingRules(behavior.ScaleUp, fldPath.Child("scaleUp"), opts); len(scaleUpErrs) > 0 {
			allErrs = append(allErrs, scaleUpErrs...)
		}
		if scaleDownErrs := validateScalingRules(behavior.ScaleDown, fldPath.Child("scaleDown"), opts); len(scaleDownErrs) > 0 {
			allErrs = append(allErrs, scaleDownErrs...)
		}
	}
	return allErrs
}

// validSelectPolicyTypes 列出 selectPolicy 允许的三种取值：最大、最小、禁用。
var validSelectPolicyTypes = sets.NewString(string(autoscaling.MaxPolicySelect), string(autoscaling.MinPolicySelect), string(autoscaling.DisabledPolicySelect))

// 预先生成有序列表，使 NotSupported 错误信息稳定可预期。
var validSelectPolicyTypesList = validSelectPolicyTypes.List()

// validateScalingRules 校验一组扩缩容规则：稳定窗口、策略选择、策略列表与容错度。
func validateScalingRules(rules *autoscaling.HPAScalingRules, fldPath *field.Path, opts HorizontalPodAutoscalerSpecValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if rules != nil {
		// 稳定窗口以秒计，负值没有语义。
		if rules.StabilizationWindowSeconds != nil && *rules.StabilizationWindowSeconds < 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("stabilizationWindowSeconds"), rules.StabilizationWindowSeconds, "must be greater than or equal to zero"))
		}
		// 稳定窗口也不能超过上限，否则扩缩容决策会长期滞后于指标变化。
		if rules.StabilizationWindowSeconds != nil && *rules.StabilizationWindowSeconds > MaxStabilizationWindowSeconds {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("stabilizationWindowSeconds"), rules.StabilizationWindowSeconds,
				fmt.Sprintf("must be less than or equal to %v", MaxStabilizationWindowSeconds)))
		}
		// selectPolicy 只能取预定义的枚举值。
		if rules.SelectPolicy != nil && !validSelectPolicyTypes.Has(string(*rules.SelectPolicy)) {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("selectPolicy"), rules.SelectPolicy, validSelectPolicyTypesList))
		}
		policiesPath := fldPath.Child("policies")
		// 至少配置一条策略，否则扩容/缩容行为没有计算依据。
		if len(rules.Policies) == 0 {
			allErrs = append(allErrs, field.Required(policiesPath, "must specify at least one Policy"))
		}
		for i, policy := range rules.Policies {
			idxPath := policiesPath.Index(i)
			if policyErrs := validateScalingPolicy(policy, idxPath); len(policyErrs) > 0 {
				allErrs = append(allErrs, policyErrs...)
			}
		}
		// tolerance 是以 Quantity 表示的非负阈值，用于忽略指标微小波动。
		if rules.Tolerance != nil {
			allErrs = append(allErrs, apivalidation.ValidateNonnegativeQuantity(*rules.Tolerance, fldPath.Child("tolerance"))...)
		}
	}
	return allErrs
}

// validPolicyTypes 列出单条扩缩容策略允许的类型：Pods 或 Percent。
var validPolicyTypes = sets.NewString(string(autoscaling.PodsScalingPolicy), string(autoscaling.PercentScalingPolicy))

// 预先生成列表，保证策略类型报错信息稳定。
var validPolicyTypesList = validPolicyTypes.List()

// validateScalingPolicy 校验单条扩缩容策略的类型、步长与统计周期。
func validateScalingPolicy(policy autoscaling.HPAScalingPolicy, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// 策略类型只有按 Pod 个数和按百分比两种。
	if policy.Type != autoscaling.PodsScalingPolicy && policy.Type != autoscaling.PercentScalingPolicy {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("type"), policy.Type, validPolicyTypesList))
	}
	// 步长必须为正数，零或负值不会产生任何扩缩容效果。
	if policy.Value <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("value"), policy.Value, "must be greater than zero"))
	}
	// 周期必须为正数，否则无法界定统计区间。
	if policy.PeriodSeconds <= 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("periodSeconds"), policy.PeriodSeconds, "must be greater than zero"))
	}
	// 周期同样受上限约束，避免策略过度平滑。
	if policy.PeriodSeconds > MaxPeriodSeconds {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("periodSeconds"), policy.PeriodSeconds,
			fmt.Sprintf("must be less than or equal to %v", MaxPeriodSeconds)))
	}
	return allErrs
}

// validMetricSourceTypes 列出 HPA 支持的全部指标来源类型。
var validMetricSourceTypes = sets.NewString(
	string(autoscaling.ObjectMetricSourceType), string(autoscaling.PodsMetricSourceType),
	string(autoscaling.ResourceMetricSourceType), string(autoscaling.ExternalMetricSourceType),
	string(autoscaling.ContainerResourceMetricSourceType))

// 预生成列表，保证指标类型报错信息稳定。
var validMetricSourceTypesList = validMetricSourceTypes.List()

// validateMetricSpec 校验单条 MetricSpec：类型合法、恰好填写一个对应来源字段、
// 且该来源字段自身的内容合法。
func validateMetricSpec(spec autoscaling.MetricSpec, fldPath *field.Path, opts CrossVersionObjectReferenceValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	// type 是必填字段，空字符串无法决定应校验哪个来源。
	if len(string(spec.Type)) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("type"), "must specify a metric source type"))
	}

	// type 必须是已知枚举值，未知值直接拒绝。
	if !validMetricSourceTypes.Has(string(spec.Type)) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("type"), spec.Type, validMetricSourceTypesList))
	}

	// 用集合统计实际填写的来源字段：只允许出现一个，否则后续报“多余字段”。
	typesPresent := sets.NewString()
	// 逐个来源字段检查：只有第一个被填写的来源会被深入校验，
	// 避免同一条指标因多个来源字段重复产生大量错误。
	if spec.Object != nil {
		typesPresent.Insert("object")
		if typesPresent.Len() == 1 {
			allErrs = append(allErrs, validateObjectSource(spec.Object, fldPath.Child("object"), opts)...)
		}
	}

	if spec.External != nil {
		typesPresent.Insert("external")
		if typesPresent.Len() == 1 {
			allErrs = append(allErrs, validateExternalSource(spec.External, fldPath.Child("external"))...)
		}
	}

	if spec.Pods != nil {
		typesPresent.Insert("pods")
		if typesPresent.Len() == 1 {
			allErrs = append(allErrs, validatePodsSource(spec.Pods, fldPath.Child("pods"))...)
		}
	}

	if spec.Resource != nil {
		typesPresent.Insert("resource")
		if typesPresent.Len() == 1 {
			allErrs = append(allErrs, validateResourceSource(spec.Resource, fldPath.Child("resource"))...)
		}
	}

	if spec.ContainerResource != nil {
		typesPresent.Insert("containerResource")
		if typesPresent.Len() == 1 {
			allErrs = append(allErrs, validateContainerResourceSource(spec.ContainerResource, fldPath.Child("containerResource"))...)
		}
	}

	// 根据 type 推导出期望填写的字段名，用于检查缺失与多余字段。
	var expectedField string
	switch spec.Type {

	case autoscaling.ObjectMetricSourceType:
		if spec.Object == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("object"), "must populate information for the given metric source"))
		}
		expectedField = "object"
	case autoscaling.PodsMetricSourceType:
		if spec.Pods == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("pods"), "must populate information for the given metric source"))
		}
		expectedField = "pods"
	case autoscaling.ResourceMetricSourceType:
		if spec.Resource == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("resource"), "must populate information for the given metric source"))
		}
		expectedField = "resource"
	case autoscaling.ExternalMetricSourceType:
		if spec.External == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("external"), "must populate information for the given metric source"))
		}
		expectedField = "external"
	case autoscaling.ContainerResourceMetricSourceType:
		if spec.ContainerResource == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("containerResource"), "must populate information for the given metric source"))
		}
		expectedField = "containerResource"
	default:
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("type"), spec.Type, validMetricSourceTypesList))
	}

	// 若填写的来源字段数量不等于一，剔除期望字段后，其余字段都视为多余。
	if typesPresent.Len() != 1 {
		typesPresent.Delete(expectedField)
		for typ := range typesPresent {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child(typ), "must populate the given metric source only"))
		}
	}

	return allErrs
}

// validateObjectSource 校验 Object 指标：被描述对象引用、指标标识与目标值。
// describedObject 位于不透明的 metrics 子树，必填错误不能标记为已由声明式校验覆盖。
func validateObjectSource(src *autoscaling.ObjectMetricSource, fldPath *field.Path, opts CrossVersionObjectReferenceValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateCrossVersionObjectReference(src.DescribedObject, fldPath.Child("describedObject"), opts)...)
	allErrs = append(allErrs, validateMetricIdentifier(src.Metric, fldPath.Child("metric"))...)
	allErrs = append(allErrs, validateMetricTarget(src.Target, fldPath.Child("target"))...)

	// 目标值二选一：原始值或每 Pod 平均值，至少要设置一个。
	if src.Target.Value == nil && src.Target.AverageValue == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("target").Child("averageValue"), "must set either a target value or averageValue"))
	}

	return allErrs
}

// validateExternalSource 校验 External 指标：指标标识与目标值。
func validateExternalSource(src *autoscaling.ExternalMetricSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, validateMetricIdentifier(src.Metric, fldPath.Child("metric"))...)
	allErrs = append(allErrs, validateMetricTarget(src.Target, fldPath.Child("target"))...)

	// External 指标必须给出原始目标值或每 Pod 目标值之一。
	if src.Target.Value == nil && src.Target.AverageValue == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("target").Child("averageValue"), "must set either a target value for metric or a per-pod target"))
	}

	// 两者同时设置会造成语义冲突，因此明确禁止。
	if src.Target.Value != nil && src.Target.AverageValue != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("target").Child("value"), "may not set both a target value for metric and a per-pod target"))
	}

	return allErrs
}

// validatePodsSource 校验 Pods 指标：指标标识与目标平均值。
func validatePodsSource(src *autoscaling.PodsMetricSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, validateMetricIdentifier(src.Metric, fldPath.Child("metric"))...)
	allErrs = append(allErrs, validateMetricTarget(src.Target, fldPath.Child("target"))...)

	// Pods 指标以每 Pod 平均值为目标，因此 averageValue 必填。
	if src.Target.AverageValue == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("target").Child("averageValue"), "must specify a positive target averageValue"))
	}

	return allErrs
}

// validateContainerResourceSource 校验 ContainerResource 指标：资源名、容器名与目标值。
func validateContainerResourceSource(src *autoscaling.ContainerResourceMetricSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 资源名不能为空，非空时按合法容器资源名规则校验。
	if len(src.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), "must specify a resource name"))
	} else {
		allErrs = append(allErrs, apivalidation.ValidateContainerResourceName(src.Name, fldPath.Child("name"))...)
	}

	// 容器名不能为空，且必须符合 DNS-1123 标签规范。
	if len(src.Container) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("container"), "must specify a container"))
	} else {
		allErrs = append(allErrs, apivalidation.ValidateDNS1123Label(src.Container, fldPath.Child("container"))...)
	}

	allErrs = append(allErrs, validateMetricTarget(src.Target, fldPath.Child("target"))...)

	// 资源指标须指定原始目标值或利用率目标之一。
	if src.Target.AverageUtilization == nil && src.Target.AverageValue == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("target").Child("averageUtilization"), "must set either a target raw value or a target utilization"))
	}

	// 原始值与利用率不能同时设置，避免目标含义不明确。
	if src.Target.AverageUtilization != nil && src.Target.AverageValue != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("target").Child("averageValue"), "may not set both a target raw value and a target utilization"))
	}

	return allErrs
}

// validateResourceSource 校验 Resource 指标：资源名与目标值。
func validateResourceSource(src *autoscaling.ResourceMetricSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// resource 来源必须给出资源名。
	if len(src.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), "must specify a resource name"))
	}

	allErrs = append(allErrs, validateMetricTarget(src.Target, fldPath.Child("target"))...)

	// 须在原始目标值与利用率目标之间二选一。
	if src.Target.AverageUtilization == nil && src.Target.AverageValue == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("target").Child("averageUtilization"), "must set either a target raw value or a target utilization"))
	}

	// 两者同时设置会造成目标语义冲突。
	if src.Target.AverageUtilization != nil && src.Target.AverageValue != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("target").Child("averageValue"), "may not set both a target raw value and a target utilization"))
	}

	return allErrs
}

// validateMetricTarget 校验所有指标来源共用的目标值结构：目标类型及各数值字段。
// Sign() != 1 表示 Quantity 非正（零或负数）。
func validateMetricTarget(mt autoscaling.MetricTarget, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 指标目标类型是必填字段。
	if len(mt.Type) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("type"), "must specify a metric target type"))
	}

	// 类型只允许 Utilization、Value、AverageValue 三种。
	if mt.Type != autoscaling.UtilizationMetricType &&
		mt.Type != autoscaling.ValueMetricType &&
		mt.Type != autoscaling.AverageValueMetricType {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("type"), mt.Type, "must be either Utilization, Value, or AverageValue"))
	}

	// 原始目标值必须严格大于零。
	if mt.Value != nil && mt.Value.Sign() != 1 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("value"), mt.Value, "must be positive"))
	}

	// 每 Pod 平均目标值同样必须严格大于零。
	if mt.AverageValue != nil && mt.AverageValue.Sign() != 1 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("averageValue"), mt.AverageValue, "must be positive"))
	}

	// 利用率按百分比表示，最小合法值为 1。
	if mt.AverageUtilization != nil && *mt.AverageUtilization < 1 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("averageUtilization"), mt.AverageUtilization, "must be greater than 0"))
	}

	return allErrs
}

// validateMetricIdentifier 校验指标标识：名称必填且必须是合法的路径段名称。
func validateMetricIdentifier(id autoscaling.MetricIdentifier, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 指标名会出现在 API 路径与查询参数中，因此复用路径段名称规则。
	if len(id.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), "must specify a metric name"))
	} else {
		for _, msg := range content.IsPathSegmentName(id.Name) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), id.Name, msg))
		}
	}
	return allErrs
}
