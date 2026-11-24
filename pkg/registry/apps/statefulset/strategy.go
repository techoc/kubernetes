/*
Copyright 2014 The Kubernetes Authors.

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

package statefulset

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	pvcutil "k8s.io/kubernetes/pkg/api/persistentvolumeclaim"
	"k8s.io/kubernetes/pkg/api/pod"
	"k8s.io/kubernetes/pkg/apis/apps"
	"k8s.io/kubernetes/pkg/apis/apps/validation"
	"k8s.io/kubernetes/pkg/features"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// statefulSetStrategy 定义 StatefulSet 主资源策略，处理 Pod 模板、PVC 模板、更新策略和 Feature Gate 兼容。
type statefulSetStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建和更新 StatefulSet 时使用的默认策略。
var Strategy = statefulSetStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// 编译期检查：确保 Strategy 实现对应的 REST 策略接口。
var _ = rest.GarbageCollectionDeleteStrategy(Strategy)

// DefaultGarbageCollectionPolicy 返回 DeleteDependents：删除 StatefulSet 时级联删除其依赖对象。
func (statefulSetStrategy) DefaultGarbageCollectionPolicy(ctx context.Context) rest.GarbageCollectionPolicy {
	return rest.DeleteDependents
}

// NamespaceScoped 表示 StatefulSet 必须属于某个命名空间。
func (statefulSetStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回 Status 子资源更新时需要重置、不能由请求修改的字段集合。
func (statefulSetStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"apps/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 在创建 StatefulSet 前清空由控制器维护的 Status。
// 创建时重置 Status、初始化 Generation，并清理禁用的 StatefulSet 字段与 PodTemplate 字段。
func (statefulSetStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	statefulSet := obj.(*apps.StatefulSet)
	// 创建请求不能直接设置 Status。
	statefulSet.Status = apps.StatefulSetStatus{}

	statefulSet.Generation = 1

	dropStatefulSetDisabledFields(statefulSet, nil)
	pod.DropDisabledTemplateFields(&statefulSet.Spec.Template, nil)
}

// maxUnavailableInUse 判断 StatefulSet 是否已经使用 maxUnavailable 字段。
// maxUnavailableInUse 判断旧 StatefulSet 是否已配置 RollingUpdate.MaxUnavailable，用于关闭特性时保留已有字段。
func maxUnavailableInUse(statefulset *apps.StatefulSet) bool {
	if statefulset == nil {
		return false
	}
	if statefulset.Spec.UpdateStrategy.RollingUpdate == nil {
		return false
	}

	return statefulset.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable != nil
}

// PrepareForUpdate 清理更新时不允许终端用户设置的字段。
// 更新时恢复 Status，按旧对象使用情况清理禁用字段；任何 Spec 变化都会递增 Generation。
func (statefulSetStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newStatefulSet := obj.(*apps.StatefulSet)
	oldStatefulSet := old.(*apps.StatefulSet)
	// 主资源更新请求不允许修改 Status。
	newStatefulSet.Status = oldStatefulSet.Status

	dropStatefulSetDisabledFields(newStatefulSet, oldStatefulSet)
	pod.DropDisabledTemplateFields(&newStatefulSet.Spec.Template, &oldStatefulSet.Spec.Template)

	// Spec 的任何变化都会递增 Generation；Status 写入者应报告其观察到的 Spec 版本对应的 Generation。
	// Generation 的详细语义参见 metav1.ObjectMeta 文档。
	if !apiequality.Semantic.DeepEqual(oldStatefulSet.Spec, newStatefulSet.Spec) {
		newStatefulSet.Generation = oldStatefulSet.Generation + 1
	}
}

// dropStatefulSetDisabledFields 在对应 Feature Gate 未启用且旧对象未使用相关字段时，清理这些字段。
// 典型处理模式如下：
//
//	if !utilfeature.DefaultFeatureGate.Enabled(features.MyFeature) && !myFeatureInUse(oldSvc) {
//	    newSvc.Spec.MyFeature = nil
//	}
//
// Feature Gate 关闭时仅丢弃此前未使用的 MaxUnavailable，避免升级后破坏旧对象已有配置。
func dropStatefulSetDisabledFields(newSS *apps.StatefulSet, oldSS *apps.StatefulSet) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.MaxUnavailableStatefulSet) && !maxUnavailableInUse(oldSS) {
		if newSS.Spec.UpdateStrategy.RollingUpdate != nil {
			newSS.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable = nil
		}
	}
}

// Validate 校验新创建的 StatefulSet。
// 创建时必须使用合法 ServiceName 和 PVC 模板；Recreate 更新策略则按对应 Feature Gate 决定是否允许。
func (statefulSetStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	statefulSet := obj.(*apps.StatefulSet)
	opts := pod.GetValidationOptionsFromPodTemplate(&statefulSet.Spec.Template, nil)
	setOpts := validation.StatefulSetValidationOptions{
		AllowInvalidServiceName:          false, // require valid serviceNames in new StatefulSets
		AllowStatefulSetRecreateStrategy: utilfeature.DefaultFeatureGate.Enabled(features.StatefulSetRecreateStrategy),
	}
	return validation.ValidateStatefulSet(statefulSet, setOpts, opts)
}

// WarningsOnCreate 返回创建 StatefulSet 时发现的非阻断性警告。
func (statefulSetStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	newStatefulSet := obj.(*apps.StatefulSet)
	warnings := pod.GetWarningsForPodTemplate(ctx, field.NewPath("spec", "template"), &newStatefulSet.Spec.Template, nil)
	for i, pvc := range newStatefulSet.Spec.VolumeClaimTemplates {
		warnings = append(warnings, pvcutil.GetWarningsForPersistentVolumeClaimSpec(field.NewPath("spec", "volumeClaimTemplates").Index(i), pvc.Spec)...)
	}

	if newStatefulSet.Spec.RevisionHistoryLimit != nil && *newStatefulSet.Spec.RevisionHistoryLimit < 0 {
		warnings = append(warnings, "spec.revisionHistoryLimit: a negative value retains all historical revisions; a value >= 0 is recommended")
	}
	return warnings
}

// Canonicalize 在校验后规范化对象；当前策略无需额外处理。
func (statefulSetStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false：创建 StatefulSet 必须使用 POST，不能通过更新请求隐式创建。
func (statefulSetStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 执行面向普通用户的 StatefulSet 更新校验。
// 更新时对历史不可变字段采用兼容校验：旧 ServiceName/PVC 模板即使当前规则不合法，也不因无关更新而被拒绝。
func (statefulSetStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newStatefulSet := obj.(*apps.StatefulSet)
	oldStatefulSet := old.(*apps.StatefulSet)

	setOpts := validation.StatefulSetValidationOptions{
		AllowInvalidServiceName:          true, // serviceName is immutable, tolerate existing invalid names on update
		SkipValidateVolumeClaimTemplates: true, // volumeClaimTemplates are immutable, tolerate previously persisted invalid values on update
		AllowStatefulSetRecreateStrategy: utilfeature.DefaultFeatureGate.Enabled(features.StatefulSetRecreateStrategy) || oldStatefulSet.Spec.UpdateStrategy.Type == apps.RecreateStatefulSetStrategyType,
	}
	opts := pod.GetValidationOptionsFromPodTemplate(&newStatefulSet.Spec.Template, &oldStatefulSet.Spec.Template)
	return validation.ValidateStatefulSetUpdate(newStatefulSet, oldStatefulSet, setOpts, opts)
}

// WarningsOnUpdate 返回本次更新产生的非阻断性警告。
func (statefulSetStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	var warnings []string
	newStatefulSet := obj.(*apps.StatefulSet)
	oldStatefulSet := old.(*apps.StatefulSet)
	if newStatefulSet.Generation != oldStatefulSet.Generation {
		warnings = pod.GetWarningsForPodTemplate(ctx, field.NewPath("spec", "template"), &newStatefulSet.Spec.Template, &oldStatefulSet.Spec.Template)
	}
	for i, pvc := range newStatefulSet.Spec.VolumeClaimTemplates {
		warnings = append(warnings, pvcutil.GetWarningsForPersistentVolumeClaimSpec(field.NewPath("spec", "volumeClaimTemplates").Index(i).Child("Spec"), pvc.Spec)...)
	}
	if newStatefulSet.Spec.RevisionHistoryLimit != nil && *newStatefulSet.Spec.RevisionHistoryLimit < 0 {
		warnings = append(warnings, "spec.revisionHistoryLimit: a negative value retains all historical revisions; a value >= 0 is recommended")
	}

	return warnings
}

// AllowUnconditionalUpdate 表示 StatefulSet 允许不携带 resourceVersion 的无条件更新。
func (statefulSetStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type statefulSetStatusStrategy struct {
	statefulSetStrategy
}

// StatusStrategy 专门处理 statefulsets/status；它允许控制器更新副本状态，但不能更改 Spec。
var StatusStrategy = statefulSetStatusStrategy{Strategy}

// GetResetFields 返回该策略会重置的字段集合，这些字段不允许用户通过当前子资源请求修改。
func (statefulSetStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"apps/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}

// PrepareForUpdate 清理 Status 更新请求中不允许终端用户设置的字段。
// 状态子资源恢复旧 Spec，再单独校验 Status 字段。
func (statefulSetStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newStatefulSet := obj.(*apps.StatefulSet)
	oldStatefulSet := old.(*apps.StatefulSet)
	// Status 变化不能反向修改 Spec。
	newStatefulSet.Spec = oldStatefulSet.Spec
}

// ValidateUpdate 校验 StatefulSet Status 子资源的更新。
func (statefulSetStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	// TODO：补充 StatefulSet Status 更新校验。
	return validation.ValidateStatefulSetStatusUpdate(obj.(*apps.StatefulSet), old.(*apps.StatefulSet))
}

// WarningsOnUpdate 返回本次更新产生的非阻断性警告。
func (statefulSetStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
