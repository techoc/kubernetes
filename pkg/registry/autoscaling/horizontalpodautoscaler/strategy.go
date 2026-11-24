/*
Copyright 2015 The Kubernetes Authors.

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

// 本文件实现 horizontalpodautoscaler 的 REST Strategy，控制创建/更新字段处理、业务校验、告警和 Status 子资源边界。
package horizontalpodautoscaler

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	"k8s.io/kubernetes/pkg/apis/autoscaling/validation"
	"k8s.io/kubernetes/pkg/features"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// autoscalerStrategy 定义 HorizontalPodAutoscaler（HPA）的 REST 写入策略。
type autoscalerStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建和更新 HorizontalPodAutoscaler 时使用的默认策略。
var Strategy = autoscalerStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 表示 HPA 是命名空间级资源。
func (autoscalerStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回 Status 子资源更新时由服务端重置、不能由请求修改的字段集合。
func (autoscalerStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"autoscaling/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"autoscaling/v2": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 清理创建请求中不允许用户设置的字段，并按 Feature Gate 初始化 Generation。
func (autoscalerStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	newHPA := obj.(*autoscaling.HorizontalPodAutoscaler)

	// 创建请求不能直接设置 Status。
	newHPA.Status = autoscaling.HorizontalPodAutoscalerStatus{}

	// HPAGeneration 通过 Feature Gate 控制，以便为曾自行设置 Generation 的客户端保留一个版本的兼容窗口。
	if utilfeature.DefaultFeatureGate.Enabled(features.HPAGeneration) {
		newHPA.Generation = 1
	}

	dropDisabledFields(newHPA, nil)
}

// Validate 校验新创建的 HPA。
func (autoscalerStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	autoscaler := obj.(*autoscaling.HorizontalPodAutoscaler)
	opts := validationOptionsForHorizontalPodAutoscaler(autoscaler, nil)
	return validation.ValidateHorizontalPodAutoscaler(autoscaler, opts)
}

// DeclarativeValidationConfig 实现 rest.DeclarativeValidationConfigurer，为声明式校验提供特性选项。
func (autoscalerStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	// 启用 HPAScaleToZero 特性时允许缩容至零；更新旧对象时，如果其 MinReplicas 已为 0，也继续允许该配置。
	enableScaleToZero := utilfeature.DefaultFeatureGate.Enabled(features.HPAScaleToZero)
	if !enableScaleToZero && oldObj != nil {
		if oldHPA, ok := oldObj.(*autoscaling.HorizontalPodAutoscaler); ok {
			if oldHPA.Spec.MinReplicas != nil && *oldHPA.Spec.MinReplicas == 0 {
				enableScaleToZero = true
			}
		}
	}
	return rest.DeclarativeValidationConfig{Options: map[string]bool{
		string(features.HPAScaleToZero): enableScaleToZero,
	}}
}

// WarningsOnCreate 返回创建 HPA 时的非阻断性警告；当前不产生额外警告。
func (autoscalerStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化对象；当前策略无需额外处理。
func (autoscalerStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false：HPA 不能通过更新请求隐式创建。
func (autoscalerStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 恢复服务端维护的 Status，清理禁用字段，并在 Spec 变化时维护 Generation。
func (autoscalerStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newHPA := obj.(*autoscaling.HorizontalPodAutoscaler)
	oldHPA := old.(*autoscaling.HorizontalPodAutoscaler)
	// 主资源更新请求不能修改 Status。
	newHPA.Status = oldHPA.Status

	dropDisabledFields(newHPA, oldHPA)

	if utilfeature.DefaultFeatureGate.Enabled(features.HPAGeneration) {
		if !apiequality.Semantic.DeepEqual(newHPA.Spec, oldHPA.Spec) {
			newHPA.Generation = oldHPA.Generation + 1
		}
	}
}

// ValidateUpdate 执行面向普通用户的 HPA 更新校验。
func (autoscalerStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newHPA := obj.(*autoscaling.HorizontalPodAutoscaler)
	oldHPA := old.(*autoscaling.HorizontalPodAutoscaler)
	opts := validationOptionsForHorizontalPodAutoscaler(newHPA, oldHPA)
	return validation.ValidateHorizontalPodAutoscalerUpdate(newHPA, oldHPA, opts)
}

// WarningsOnUpdate 返回更新 HPA 时的非阻断性警告；当前不产生额外警告。
func (autoscalerStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true，允许不携带 ResourceVersion 前置条件的更新。
func (autoscalerStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type autoscalerStatusStrategy struct {
	autoscalerStrategy
}

// StatusStrategy 是更新 HPA Status 子资源时使用的专用策略。
var StatusStrategy = autoscalerStatusStrategy{Strategy}

// GetResetFields 返回 Status 子资源更新时由服务端重置、不能由请求修改的 Spec 字段集合。
func (autoscalerStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"autoscaling/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
		"autoscaling/v2": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 在更新校验前恢复不可由该路径修改的字段，并处理特性门控字段的向前/向后兼容。
func (autoscalerStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newAutoscaler := obj.(*autoscaling.HorizontalPodAutoscaler)
	oldAutoscaler := old.(*autoscaling.HorizontalPodAutoscaler)
	// Status 子资源请求只能修改 Status，不能借此更新 Spec。
	newAutoscaler.Spec = oldAutoscaler.Spec
}

// ValidateUpdate 结合新旧对象执行更新校验，检查不可变字段、字段约束和兼容性。
func (autoscalerStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateHorizontalPodAutoscalerStatusUpdate(obj.(*autoscaling.HorizontalPodAutoscaler), old.(*autoscaling.HorizontalPodAutoscaler))
}

// WarningsOnUpdate 返回更新 HPA Status 时的非阻断性警告；当前不产生额外警告。
func (autoscalerStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// validationOptionsForHorizontalPodAutoscaler 根据新旧 HPA 对象构造 Spec 校验选项；更新时允许兼容旧对象中已存在但当前规则不再接受的字段。
func validationOptionsForHorizontalPodAutoscaler(newHPA, oldHPA *autoscaling.HorizontalPodAutoscaler) validation.HorizontalPodAutoscalerSpecValidationOptions {
	opts := validation.HorizontalPodAutoscalerSpecValidationOptions{
		MinReplicasLowerBound:           1,
		ScaleTargetRefValidationOptions: validation.CrossVersionObjectReferenceValidationOptions{AllowInvalidAPIVersion: false, AllowEmptyAPIGroup: false, RequiredCoveredByDeclarative: true},
		ObjectMetricsValidationOptions: validation.CrossVersionObjectReferenceValidationOptions{
			AllowInvalidAPIVersion: false, AllowEmptyAPIGroup: true,
		},
	}

	oldHasZeroMinReplicas := oldHPA != nil && (oldHPA.Spec.MinReplicas != nil && *oldHPA.Spec.MinReplicas == 0)
	if utilfeature.DefaultFeatureGate.Enabled(features.HPAScaleToZero) || oldHasZeroMinReplicas {
		opts.MinReplicasLowerBound = 0
	}

	switch {
	case oldHPA != nil && oldHPA.Spec.ScaleTargetRef.APIVersion == newHPA.Spec.ScaleTargetRef.APIVersion && oldHPA.Spec.ScaleTargetRef.Kind == newHPA.Spec.ScaleTargetRef.Kind:
		// 更新未改变目标 Kind 和 APIVersion 时，跳过 APIVersion 校验，以兼容历史对象。
		opts.ScaleTargetRefValidationOptions.AllowInvalidAPIVersion = true
	case newHPA.Spec.ScaleTargetRef.Kind == "ReplicationController":
		// ReplicationController 是 core/v1 中唯一可扩缩的资源，允许其目标引用省略 API Group。
		opts.ScaleTargetRefValidationOptions.AllowEmptyAPIGroup = true
	}

	if oldHPA != nil {
		for _, metric := range oldHPA.Spec.Metrics {
			if metric.Type == autoscaling.ObjectMetricSourceType && metric.Object != nil {
				if err := validation.ValidateAPIVersion(metric.Object.DescribedObject, opts.ObjectMetricsValidationOptions); err != nil {
					// 旧对象中的指标引用已经不合法；为避免无关更新被阻断，允许保留原有 APIVersion。
					opts.ObjectMetricsValidationOptions.AllowInvalidAPIVersion = true
					break
				}
			}
		}
	}
	return opts
}

// dropDisabledFields 清除 Feature Gate 已关闭且旧 HPA 从未使用过的字段；oldHPA 为 nil 时按新对象处理。
func dropDisabledFields(newHPA, oldHPA *autoscaling.HorizontalPodAutoscaler) {
	if utilfeature.DefaultFeatureGate.Enabled(features.HPAConfigurableTolerance) {
		return
	}
	if toleranceInUse(oldHPA) {
		return
	}
	newBehavior := newHPA.Spec.Behavior
	if newBehavior == nil {
		return
	}

	for _, sr := range []*autoscaling.HPAScalingRules{newBehavior.ScaleDown, newBehavior.ScaleUp} {
		if sr != nil {
			sr.Tolerance = nil
		}
	}
}

// toleranceInUse 判断 HPA 是否显式使用指标容差配置，帮助区分默认值与用户主动配置。
func toleranceInUse(hpa *autoscaling.HorizontalPodAutoscaler) bool {
	if hpa == nil || hpa.Spec.Behavior == nil {
		return false
	}
	for _, sr := range []*autoscaling.HPAScalingRules{hpa.Spec.Behavior.ScaleDown, hpa.Spec.Behavior.ScaleUp} {
		if sr != nil && sr.Tolerance != nil {
			return true
		}
	}
	return false
}
