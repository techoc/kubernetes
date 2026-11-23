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

// Strategy is the default logic that applies when creating and updating Replication StatefulSet objects.
var Strategy = statefulSetStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// Make sure we correctly implement the interface.
var _ = rest.GarbageCollectionDeleteStrategy(Strategy)

// DefaultGarbageCollectionPolicy returns DeleteDependents for all currently served versions.
func (statefulSetStrategy) DefaultGarbageCollectionPolicy(ctx context.Context) rest.GarbageCollectionPolicy {
	return rest.DeleteDependents
}

// NamespaceScoped returns true because all StatefulSet' need to be within a namespace.
func (statefulSetStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields returns the set of fields that get reset by the strategy
// and should not be modified by the user.
func (statefulSetStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"apps/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate clears the status of an StatefulSet before creation.
// 创建时重置 Status、初始化 Generation，并清理禁用的 StatefulSet 字段与 PodTemplate 字段。
func (statefulSetStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	statefulSet := obj.(*apps.StatefulSet)
	// create cannot set status
	statefulSet.Status = apps.StatefulSetStatus{}

	statefulSet.Generation = 1

	dropStatefulSetDisabledFields(statefulSet, nil)
	pod.DropDisabledTemplateFields(&statefulSet.Spec.Template, nil)
}

// maxUnavailableInUse returns true if StatefulSet's maxUnavailable set(used)
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

// PrepareForUpdate clears fields that are not allowed to be set by end users on update.
// 更新时恢复 Status，按旧对象使用情况清理禁用字段；任何 Spec 变化都会递增 Generation。
func (statefulSetStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newStatefulSet := obj.(*apps.StatefulSet)
	oldStatefulSet := old.(*apps.StatefulSet)
	// Update is not allowed to set status
	newStatefulSet.Status = oldStatefulSet.Status

	dropStatefulSetDisabledFields(newStatefulSet, oldStatefulSet)
	pod.DropDisabledTemplateFields(&newStatefulSet.Spec.Template, &oldStatefulSet.Spec.Template)

	// Any changes to the spec increment the generation number, any changes to the
	// status should reflect the generation number of the corresponding object.
	// See metav1.ObjectMeta description for more information on Generation.
	if !apiequality.Semantic.DeepEqual(oldStatefulSet.Spec, newStatefulSet.Spec) {
		newStatefulSet.Generation = oldStatefulSet.Generation + 1
	}
}

// dropStatefulSetDisabledFields drops fields that are not used if their associated feature gates
// are not enabled.
// The typical pattern is:
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

// Validate validates a new StatefulSet.
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

// WarningsOnCreate returns warnings for the creation of the given object.
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

// Canonicalize normalizes the object after validation.
func (statefulSetStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate is false for StatefulSet; this means POST is needed to create one.
func (statefulSetStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate is the default update validation for an end user.
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

// WarningsOnUpdate returns warnings for the given update.
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

// AllowUnconditionalUpdate is the default update policy for StatefulSet objects.
func (statefulSetStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type statefulSetStatusStrategy struct {
	statefulSetStrategy
}

// StatusStrategy 专门处理 statefulsets/status；它允许控制器更新副本状态，但不能更改 Spec。
var StatusStrategy = statefulSetStatusStrategy{Strategy}

// GetResetFields returns the set of fields that get reset by the strategy
// and should not be modified by the user.
func (statefulSetStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"apps/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update of status
// 状态子资源恢复旧 Spec，再单独校验 Status 字段。
func (statefulSetStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newStatefulSet := obj.(*apps.StatefulSet)
	oldStatefulSet := old.(*apps.StatefulSet)
	// status changes are not allowed to update spec
	newStatefulSet.Spec = oldStatefulSet.Spec
}

// ValidateUpdate is the default update validation for an end user updating status
func (statefulSetStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	// TODO: Validate status updates.
	return validation.ValidateStatefulSetStatusUpdate(obj.(*apps.StatefulSet), old.(*apps.StatefulSet))
}

// WarningsOnUpdate returns warnings for the given update.
func (statefulSetStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
