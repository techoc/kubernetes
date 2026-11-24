/*
Copyright 2024 The Kubernetes Authors.

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

// Package storagemigration 提供 StorageVersionMigration 的 REST 策略和存储接口实现。
// 该资源用于协调集群中 API 对象存储版本的迁移。
package storagemigration

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/storagemigration"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	svmvalidation "k8s.io/kubernetes/pkg/apis/storagemigration/validation"
)

// strategy 定义 StorageVersionMigration 的创建、更新和删除策略。
type strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 StorageVersionMigration 对象时使用的默认 REST 策略。
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

var _ rest.RESTCreateStrategy = Strategy
var _ rest.RESTUpdateStrategy = Strategy
var _ rest.RESTDeleteStrategy = Strategy

// NamespaceScoped 返回 false，因为 StorageVersionMigration 是集群级资源。
func (strategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 返回写入时需要重置的字段集合，防止客户端修改由控制器维护的字段。
func (strategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"storagemigration.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"storagemigration.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 在创建 StorageVersionMigration 时清空客户端提交的 Status。
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	svm := obj.(*storagemigration.StorageVersionMigration)
	svm.Status = storagemigration.StorageVersionMigrationStatus{}
}

// PrepareForUpdate 在普通更新时恢复旧 Status，避免客户端绕过迁移控制器修改状态。
func (s strategy) PrepareForUpdate(ctx context.Context, new, old runtime.Object) {
	svm := new.(*storagemigration.StorageVersionMigration)
	svm.Status = old.(*storagemigration.StorageVersionMigration).Status
}

// Validate 校验 StorageVersionMigration 的 Spec、目标资源和迁移状态等字段。
func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	bundle := obj.(*storagemigration.StorageVersionMigration)
	return svmvalidation.ValidateStorageVersionMigration(bundle)
}

// WarningsOnCreate 返回创建 StorageVersionMigration 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 StorageVersionMigration；当前无需额外转换。
func (strategy) Canonicalize(obj runtime.Object) {}

// AllowCreateOnUpdate 返回 false，StorageVersionMigration 必须通过 POST 创建。
func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 对照旧对象校验 StorageVersionMigration 更新是否合法。
func (s strategy) ValidateUpdate(ctx context.Context, new, old runtime.Object) field.ErrorList {
	newBundle := new.(*storagemigration.StorageVersionMigration)
	oldBundle := old.(*storagemigration.StorageVersionMigration)
	return svmvalidation.ValidateStorageVersionMigrationUpdate(newBundle, oldBundle)
}

// WarningsOnUpdate 返回更新 StorageVersionMigration 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，更新必须携带 ResourceVersion 并与存储版本匹配。
func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

type statusStrategy struct {
	strategy
}

// StatusStrategy 是 StorageVersionMigration /status 子资源使用的策略。
var StatusStrategy = statusStrategy{Strategy}

// GetResetFields 返回 /status 子资源需要重置的 metadata 和 Spec 字段，确保该入口只能更新 Status。
func (statusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"storagemigration.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
		"storagemigration.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 在 Status 更新时恢复旧 Spec 和受保护的元数据字段。
func (statusStrategy) PrepareForUpdate(ctx context.Context, new, old runtime.Object) {
	newBundle := new.(*storagemigration.StorageVersionMigration)
	oldBundle := old.(*storagemigration.StorageVersionMigration)

	newBundle.Spec = oldBundle.Spec
	metav1.ResetObjectMetaForStatus(&newBundle.ObjectMeta, &oldBundle.ObjectMeta)
}

// ValidateUpdate 校验 StorageVersionMigration Status 更新是否合法。
func (s statusStrategy) ValidateUpdate(ctx context.Context, new, old runtime.Object) field.ErrorList {
	newSVM := new.(*storagemigration.StorageVersionMigration)
	oldSVM := old.(*storagemigration.StorageVersionMigration)

	return svmvalidation.ValidateStorageVersionMigrationStatusUpdate(newSVM, oldSVM)
}

// WarningsOnUpdate 返回 StorageVersionMigration Status 更新时的非阻断性警告；当前不产生额外警告。
func (statusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
