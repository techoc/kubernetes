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

// 本文件实现 csistoragecapacity 的 REST Strategy，定义作用域、写入前字段处理、校验、告警以及更新规则。
package csistoragecapacity

import (
	"context"

	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	storageutil "k8s.io/kubernetes/pkg/api/storage"
	"k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/apis/storage/validation"
)

// csiStorageCapacityStrategy implements behavior for CSIStorageCapacity objects
type csiStorageCapacityStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating
// CSIStorageCapacity objects via the REST API.
var Strategy = csiStorageCapacityStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 声明该资源是否属于命名空间级别；返回 false 表示对象通过集群级 URL 管理，返回 true 表示 URL 中必须包含 namespace。
func (csiStorageCapacityStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate is currently a NOP.
func (csiStorageCapacityStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

// Validate 执行创建请求的业务合法性校验，将字段路径和错误原因整理为 field.ErrorList；返回非空错误时 API 请求会被拒绝。
func (csiStorageCapacityStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	csiStorageCapacity := obj.(*storage.CSIStorageCapacity)
	opts := validation.CSIStorageCapacityValidateOptions{
		AllowInvalidLabelValueInSelector: false,
	}
	errs := validation.ValidateCSIStorageCapacity(csiStorageCapacity, opts)
	return errs
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (csiStorageCapacityStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return storageutil.GetWarningsForCSIStorageCapacity(obj.(*storage.CSIStorageCapacity))
}

// Canonicalize normalizes the object after validation.
func (csiStorageCapacityStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 决定 Update 请求在对象不存在时是否允许退化为创建；false 表示必须先显式 Create。
func (csiStorageCapacityStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate is currently a NOP.
func (csiStorageCapacityStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// ValidateUpdate 对更新后的对象及其旧版本进行联合校验，检查字段合法性、不可变字段和跨版本兼容约束。
func (csiStorageCapacityStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newCSIStorageCapacityObj := obj.(*storage.CSIStorageCapacity)
	oldCSIStorageCapacityObj := old.(*storage.CSIStorageCapacity)
	opts := validation.CSIStorageCapacityValidateOptions{
		AllowInvalidLabelValueInSelector: hasInvalidLabelValueInLabelSelector(oldCSIStorageCapacityObj),
	}
	errorList := validation.ValidateCSIStorageCapacity(newCSIStorageCapacityObj, opts)
	return append(errorList, validation.ValidateCSIStorageCapacityUpdate(newCSIStorageCapacityObj, oldCSIStorageCapacityObj)...)
}

// WarningsOnUpdate returns warnings for the given update.
func (csiStorageCapacityStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return storageutil.GetWarningsForCSIStorageCapacity(obj.(*storage.CSIStorageCapacity))
}

// AllowUnconditionalUpdate 声明是否允许不携带 ResourceVersion 前置条件的更新；true 表示允许无条件更新，false 要求并发版本检查。
func (csiStorageCapacityStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// hasInvalidLabelValueInLabelSelector 检查旧 CSIStorageCapacity 的 NodeTopology 是否包含当前规则下无效的 Label 值，用于判断更新时是否需要兼容历史数据。
func hasInvalidLabelValueInLabelSelector(capacity *storage.CSIStorageCapacity) bool {
	labelSelectorValidationOptions := metav1validation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: false}
	return len(metav1validation.ValidateLabelSelector(capacity.NodeTopology, labelSelectorValidationOptions, nil)) > 0
}
