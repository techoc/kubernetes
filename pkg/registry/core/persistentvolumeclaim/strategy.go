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

package persistentvolumeclaim

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"k8s.io/kubernetes/pkg/api/legacyscheme"
	pvcutil "k8s.io/kubernetes/pkg/api/persistentvolumeclaim"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
)

// persistentvolumeclaimStrategy 定义 PVC 主资源写入策略，维护状态隔离、卷请求约束以及 dataSource 版本兼容。
type persistentvolumeclaimStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建和更新 PersistentVolumeClaim（PVC）时使用的默认策略。
var Strategy = persistentvolumeclaimStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

func (persistentvolumeclaimStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回该策略会重置、用户不能直接修改的字段集合。
func (persistentvolumeclaimStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 清空创建请求中不允许用户设置的 Status 字段。
// 创建时清空服务端维护的 Status，清理禁用字段，并兼容旧 dataSource 与新 dataSourceRef 两种来源表示。
func (persistentvolumeclaimStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	pvc := obj.(*api.PersistentVolumeClaim)
	pvc.Status = api.PersistentVolumeClaimStatus{}
	pvcutil.DropDisabledFields(&pvc.Spec, nil)

	// 为实现 KEP 1495 中的数据源兼容，需要完成以下两步处理。

	// 先处理历史客户端可能写入的非 PVC/VolumeSnapshot dataSource 值，避免它们绕过新字段校验。
	// 第一步：在满足兼容条件时，清除 spec.dataSource 中无效的值（既不是 PVC，也不是 VolumeSnapshot 的来源）。
	pvcutil.EnforceDataSourceBackwardsCompatibility(&pvc.Spec, nil)

	// 再将单边填写的数据源字段同步到另一字段，使新旧客户端读取到一致的来源信息。
	// 第二步：如果 dataSource 与 dataSourceRef 只有一方有值，则将该值同步到另一字段。
	pvcutil.NormalizeDataSources(&pvc.Spec)
}

// Validate 根据当前与旧版兼容特性计算校验选项，检查 PVC 容量、访问模式、StorageClass 和数据源引用。
func (persistentvolumeclaimStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	pvc := obj.(*api.PersistentVolumeClaim)
	opts := validation.ValidationOptionsForPersistentVolumeClaim(pvc, nil)
	return validation.ValidatePersistentVolumeClaim(pvc, opts)
}

// WarningsOnCreate 返回创建 PVC 时发现的非阻断性警告。
func (persistentvolumeclaimStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return pvcutil.GetWarningsForPersistentVolumeClaim(obj.(*api.PersistentVolumeClaim))
}

// Canonicalize 在校验后规范化 PVC 对象；当前策略无需额外处理。
func (persistentvolumeclaimStrategy) Canonicalize(obj runtime.Object) {
}

func (persistentvolumeclaimStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 恢复旧 Status，并对数据源字段执行兼容性清理和规范化。
// 普通更新保留旧 Status；规范化新旧 dataSource 后再校验，兼容旧客户端省略服务端补全字段的请求。
func (persistentvolumeclaimStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPvc := obj.(*api.PersistentVolumeClaim)
	oldPvc := old.(*api.PersistentVolumeClaim)
	newPvc.Status = oldPvc.Status

	pvcutil.DropDisabledFields(&newPvc.Spec, &oldPvc.Spec)

	// 此处复用创建阶段的数据源处理逻辑：一方面保持旧行为，创建和更新时忽略无效数据源；
	// 另一方面兼容可能省略 dataSourceRef 的旧客户端，因为该字段曾由服务端自动补齐，所以更新时需要再次补齐。
	pvcutil.EnforceDataSourceBackwardsCompatibility(&newPvc.Spec, &oldPvc.Spec)
	pvcutil.NormalizeDataSources(&newPvc.Spec)

	// 旧对象也要规范化后再比较，避免历史版本未回填 dataSourceRef 的持久化对象在无关更新时被误判非法。
	// 同时规范化旧 PVC 的数据源字段，使早期版本保存、尚未同步两个字段的对象仍能通过校验。
	pvcutil.NormalizeDataSources(&oldPvc.Spec)
}

func (persistentvolumeclaimStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newPvc := obj.(*api.PersistentVolumeClaim)
	oldPvc := old.(*api.PersistentVolumeClaim)
	opts := validation.ValidationOptionsForPersistentVolumeClaim(newPvc, oldPvc)
	return validation.ValidatePersistentVolumeClaimUpdate(newPvc, oldPvc, opts)
}

// WarningsOnUpdate 返回更新 PVC 时发现的非阻断性警告。
func (persistentvolumeclaimStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return pvcutil.GetWarningsForPersistentVolumeClaim(obj.(*api.PersistentVolumeClaim))
}

func (persistentvolumeclaimStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type persistentvolumeclaimStatusStrategy struct {
	persistentvolumeclaimStrategy
}

var StatusStrategy = persistentvolumeclaimStatusStrategy{Strategy}

// GetResetFields 返回该子资源写入时由策略重置、用户不能直接修改的字段集合。
func (persistentvolumeclaimStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 恢复旧 Spec，确保 /status 请求不能修改 PVC.Spec。
// /status 子资源不能修改 PVC.Spec；恢复旧 Spec 后再剔除当前 Feature Gate 不允许写入的 Status 字段。
func (persistentvolumeclaimStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPVC := obj.(*api.PersistentVolumeClaim)
	oldPVC := old.(*api.PersistentVolumeClaim)
	newPVC.Spec = oldPVC.Spec
	pvcutil.DropDisabledFieldsFromStatus(newPVC, oldPVC)
}

func (persistentvolumeclaimStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newPvc := obj.(*api.PersistentVolumeClaim)
	oldPvc := old.(*api.PersistentVolumeClaim)
	opts := validation.ValidationOptionsForPersistentVolumeClaim(newPvc, oldPvc)
	return validation.ValidatePersistentVolumeClaimStatusUpdate(newPvc, oldPvc, opts)
}

// WarningsOnUpdate 返回更新 PVC 时的非阻断性警告。
func (persistentvolumeclaimStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// GetAttrs 提取 PVC 的 Labels 和可筛选字段，供命名空间内 LIST/WATCH 过滤使用。
// GetAttrs 提取 PVC 标签和字段选择器属性，供 namespace 内 LIST/WATCH 过滤。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	persistentvolumeclaimObj, ok := obj.(*api.PersistentVolumeClaim)
	if !ok {
		return nil, nil, fmt.Errorf("not a persistentvolumeclaim")
	}
	return labels.Set(persistentvolumeclaimObj.Labels), PersistentVolumeClaimToSelectableFields(persistentvolumeclaimObj), nil
}

// MatchPersistentVolumeClaim 根据 LabelSelector 和 FieldSelector 创建 PVC 存储层选择谓词。
// MatchPersistentVolumeClaim 将 LabelSelector/FieldSelector 绑定到 PVC 属性提取函数。
func MatchPersistentVolumeClaim(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// PersistentVolumeClaimToSelectableFields 返回 PVC 支持 FieldSelector 查询的字段集合。
func PersistentVolumeClaimToSelectableFields(persistentvolumeclaim *api.PersistentVolumeClaim) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&persistentvolumeclaim.ObjectMeta, true)
	specificFieldsSet := fields.Set{
		// name 并非规范的可选字段，但为保持向后兼容，必须继续支持。
		"name": persistentvolumeclaim.Name,
	}
	return generic.MergeFieldsSets(objectMetaFieldsSet, specificFieldsSet)
}
