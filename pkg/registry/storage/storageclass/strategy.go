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

// 本文件实现 storageclass 的 REST Strategy，定义作用域、写入前字段处理、校验、告警以及更新规则。
package storageclass

import (
	"context"

	"k8s.io/apiserver/pkg/registry/rest"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	storageutil "k8s.io/kubernetes/pkg/api/storage"
	"k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/apis/storage/validation"
)

// storageClassStrategy 定义 StorageClass 的创建、更新、校验和告警策略。
type storageClassStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建、更新和删除 StorageClass 时使用的默认策略。
var Strategy = storageClassStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，因为 StorageClass 是集群级资源。
func (storageClassStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 是创建前的字段预处理入口；StorageClass 没有可由客户端设置的 Status 字段。
func (storageClassStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

// Validate 执行创建请求的业务合法性校验，将字段路径和错误原因整理为 field.ErrorList；返回非空错误时 API 请求会被拒绝。
func (storageClassStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	storageClass := obj.(*storage.StorageClass)
	return validation.ValidateStorageClass(storageClass)
}

// WarningsOnCreate 返回 StorageClass 创建时的拓扑和参数相关警告。
func (storageClassStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return storageutil.GetWarningsForStorageClass(obj.(*storage.StorageClass))
}

// Canonicalize 在校验后规范化 StorageClass；当前不执行额外转换。
func (storageClassStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，StorageClass 必须通过 POST 创建。
func (storageClassStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 是更新前的字段预处理入口；StorageClass 没有 Status 子资源需要恢复。
func (storageClassStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// ValidateUpdate 对更新后的对象及其旧版本进行联合校验，检查字段合法性、不可变字段和跨版本兼容约束。
func (storageClassStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	allErrs := validation.ValidateStorageClass(obj.(*storage.StorageClass))
	allErrs = append(allErrs, validation.ValidateStorageClassUpdate(obj.(*storage.StorageClass), old.(*storage.StorageClass))...)
	return allErrs
}

// WarningsOnUpdate 返回更新 StorageClass 时的拓扑和参数相关警告。
func (storageClassStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return storageutil.GetWarningsForStorageClass(obj.(*storage.StorageClass))
}

// AllowUnconditionalUpdate 返回 true，允许通用更新流程为缺少 ResourceVersion 的请求填入最新版本。
func (storageClassStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}
