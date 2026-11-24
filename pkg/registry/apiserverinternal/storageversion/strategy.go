/*
Copyright 2020 The Kubernetes Authors.

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

package storageversion

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/apiserverinternal"
	"k8s.io/kubernetes/pkg/apis/apiserverinternal/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// storageVersionStrategy 定义 StorageVersion 的创建、更新和校验策略。
type storageVersionStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 StorageVersion 对象时使用的默认 REST 策略。
var Strategy = storageVersionStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，因为 StorageVersion 是集群级资源。
func (storageVersionStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 声明主资源写入时需要重置的 Status 字段，防止客户端绕过 StorageVersion 控制器直接修改状态。
func (storageVersionStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"internal.apiserver.k8s.io/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 在创建 StorageVersion 时清空客户端提交的 Status。
func (storageVersionStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	sv := obj.(*apiserverinternal.StorageVersion)
	sv.Status = apiserverinternal.StorageVersionStatus{}
}

// PrepareForUpdate 恢复旧 Status，防止普通更新接口修改控制器维护的存储版本状态。
func (storageVersionStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	sv := obj.(*apiserverinternal.StorageVersion)
	sv.Status = old.(*apiserverinternal.StorageVersion).Status
}

// Validate 校验 StorageVersion 的名称、存储版本报告及状态字段约束。
func (storageVersionStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	sv := obj.(*apiserverinternal.StorageVersion)
	return validation.ValidateStorageVersion(sv)
}

// WarningsOnCreate 返回创建 StorageVersion 时的非阻断性警告；当前不产生额外警告。
func (storageVersionStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 StorageVersion；当前不执行额外转换。
func (storageVersionStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，StorageVersion 必须通过 POST 创建。
func (storageVersionStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 根据新旧对象校验 StorageVersion 更新是否合法。
func (storageVersionStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newStorageVersion := obj.(*apiserverinternal.StorageVersion)
	oldStorageVersion := old.(*apiserverinternal.StorageVersion)
	validationErrorList := validation.ValidateStorageVersionUpdate(newStorageVersion, oldStorageVersion)
	return validationErrorList
}

// WarningsOnUpdate 返回 StorageVersion 主资源更新时的非阻断性警告；当前不产生额外警告。
func (storageVersionStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，更新必须携带 ResourceVersion 并与存储版本匹配。
func (storageVersionStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

type storageVersionStatusStrategy struct {
	storageVersionStrategy
}

// StatusStrategy 是 StorageVersion /status 子资源使用的策略，继承主资源策略并单独定义字段重置边界。
var StatusStrategy = storageVersionStatusStrategy{Strategy}

// GetResetFields 声明 Status 子资源写入时需要重置的 Spec 和 metadata，确保该入口只能修改 Status。
func (storageVersionStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"internal.apiserver.k8s.io/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 恢复旧 Spec，并重置受保护的元数据字段，只允许更新 Status。
func (storageVersionStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newSV := obj.(*apiserverinternal.StorageVersion)
	oldSV := old.(*apiserverinternal.StorageVersion)

	newSV.Spec = oldSV.Spec
	metav1.ResetObjectMetaForStatus(&newSV.ObjectMeta, &oldSV.ObjectMeta)
}

// ValidateUpdate 校验 StorageVersion Status 更新是否合法。
func (storageVersionStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateStorageVersionStatusUpdate(obj.(*apiserverinternal.StorageVersion), old.(*apiserverinternal.StorageVersion))
}

// WarningsOnUpdate 返回 StorageVersion Status 更新时的非阻断性警告；当前不产生额外警告。
func (storageVersionStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
