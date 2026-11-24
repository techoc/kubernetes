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

package persistentvolume

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	pvutil "k8s.io/kubernetes/pkg/api/persistentvolume"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	volumevalidation "k8s.io/kubernetes/pkg/volume/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// persistentvolumeStrategy 定义 PV 主资源的 REST 策略，管理集群级作用域、状态隔离、卷规格兼容和存储参数校验。
type persistentvolumeStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建和更新 PersistentVolume（PV）时使用的默认策略。
var Strategy = persistentvolumeStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

func (persistentvolumeStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 返回该策略会重置、用户不能直接修改的字段集合。
func (persistentvolumeStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// ResetBeforeCreate 清理创建请求中不允许用户设置的 Status 字段。
// 创建时重置客户端提交的 Status，清理关闭特性对应的 Spec 字段，并初始化 Pending Phase 与转换时间。
func (persistentvolumeStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	pv := obj.(*api.PersistentVolume)
	pv.Status = api.PersistentVolumeStatus{}
	pvutil.DropDisabledSpecFields(&pv.Spec, nil)

	pv.Status.Phase = api.VolumePending
	now := NowFunc()
	pv.Status.LastPhaseTransitionTime = &now
}

// Validate 同时执行 Kubernetes PV 字段规则和 volume 层的存储源校验，后者覆盖具体卷插件的参数约束。
func (persistentvolumeStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	persistentvolume := obj.(*api.PersistentVolume)
	opts := validation.ValidationOptionsForPersistentVolume(persistentvolume, nil)
	errorList := validation.ValidatePersistentVolume(persistentvolume, opts)
	return append(errorList, volumevalidation.ValidatePersistentVolume(persistentvolume)...)
}

// WarningsOnCreate 返回创建 PV 时的非阻断性警告。
func (persistentvolumeStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return pvutil.GetWarningsForPersistentVolume(obj.(*api.PersistentVolume))
}

// Canonicalize 在校验后规范化 PV 对象；当前策略无需额外处理。
func (persistentvolumeStrategy) Canonicalize(obj runtime.Object) {
}

func (persistentvolumeStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 恢复普通更新请求中的旧 Status，防止用户通过主资源接口修改卷状态。
// 普通 PV 更新保留旧 Status，只允许修改 Spec 中符合不可变性和 Feature Gate 规则的字段。
func (persistentvolumeStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPv := obj.(*api.PersistentVolume)
	oldPv := old.(*api.PersistentVolume)
	newPv.Status = oldPv.Status
	pvutil.DropDisabledSpecFields(&newPv.Spec, &oldPv.Spec)
}

// 更新校验依次执行 PV 通用字段校验、卷插件参数校验和新旧对象不可变字段校验。
func (persistentvolumeStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newPv := obj.(*api.PersistentVolume)
	oldPv := old.(*api.PersistentVolume)
	opts := validation.ValidationOptionsForPersistentVolume(newPv, oldPv)
	errorList := validation.ValidatePersistentVolume(newPv, opts)
	errorList = append(errorList, volumevalidation.ValidatePersistentVolume(newPv)...)
	return append(errorList, validation.ValidatePersistentVolumeUpdate(newPv, oldPv, opts)...)
}

// WarningsOnUpdate 返回更新 PV 时的非阻断性警告。
func (persistentvolumeStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return pvutil.GetWarningsForPersistentVolume(obj.(*api.PersistentVolume))
}

func (persistentvolumeStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type persistentvolumeStatusStrategy struct {
	persistentvolumeStrategy
}

var StatusStrategy = persistentvolumeStatusStrategy{Strategy}

// GetResetFields 返回该子资源写入时由策略重置、用户不能直接修改的字段集合。
func (persistentvolumeStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

var NowFunc = metav1.Now

// PrepareForUpdate 恢复旧 Spec，并根据 Phase 变化维护状态转换时间。
// /status 不允许更改持久卷 Spec；Phase 改变时自动设置 LastPhaseTransitionTime，同一 Phase 则保留旧转换时间。
func (persistentvolumeStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPv := obj.(*api.PersistentVolume)
	oldPv := old.(*api.PersistentVolume)
	newPv.Spec = oldPv.Spec

	switch {
	case oldPv.Status.Phase == newPv.Status.Phase && newPv.Status.LastPhaseTransitionTime == nil:
		// Phase 未变化时，保留已有的状态转换时间。
		newPv.Status.LastPhaseTransitionTime = oldPv.Status.LastPhaseTransitionTime

	case oldPv.Status.Phase != newPv.Status.Phase && (newPv.Status.LastPhaseTransitionTime == nil || newPv.Status.LastPhaseTransitionTime.Equal(oldPv.Status.LastPhaseTransitionTime)):
		// Phase 已变化且客户端未提供新的转换时间时，由服务端设置当前时间。
		now := NowFunc()
		newPv.Status.LastPhaseTransitionTime = &now
	}

}

func (persistentvolumeStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidatePersistentVolumeStatusUpdate(obj.(*api.PersistentVolume), old.(*api.PersistentVolume))
}

// WarningsOnUpdate 返回更新 PV 时的非阻断性警告。
func (persistentvolumeStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// GetAttrs 提取 PV 的 Labels 和可筛选字段，供 LIST/WATCH 过滤使用。
// GetAttrs 提取 PV 的标签和可选择字段，供集群级 LIST/WATCH 过滤。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	persistentvolumeObj, ok := obj.(*api.PersistentVolume)
	if !ok {
		return nil, nil, fmt.Errorf("not a persistentvolume")
	}
	return labels.Set(persistentvolumeObj.Labels), PersistentVolumeToSelectableFields(persistentvolumeObj), nil
}

// MatchPersistentVolume 根据 LabelSelector 和 FieldSelector 创建 PV 存储层选择谓词。
// MatchPersistentVolumes 将标签/字段选择器封装为存储层 SelectionPredicate。
func MatchPersistentVolumes(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// PersistentVolumeToSelectableFields 返回 PV 支持 FieldSelector 查询的字段集合。
func PersistentVolumeToSelectableFields(persistentvolume *api.PersistentVolume) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&persistentvolume.ObjectMeta, false)
	specificFieldsSet := fields.Set{
		// name 并非规范的可选字段，但为保持向后兼容，必须继续支持。
		"name": persistentvolume.Name,
	}
	return generic.MergeFieldsSets(objectMetaFieldsSet, specificFieldsSet)
}
