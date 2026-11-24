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

// csiStorageCapacityStrategy 定义 CSIStorageCapacity 的创建、更新和校验策略。
type csiStorageCapacityStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建、更新和删除 CSIStorageCapacity 时使用的默认策略。
var Strategy = csiStorageCapacityStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 true，因为 CSIStorageCapacity 属于命名空间。
func (csiStorageCapacityStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate 当前无需修改 CSIStorageCapacity 的创建请求对象。
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

// WarningsOnCreate 返回 CSIStorageCapacity 创建时的拓扑和容量相关警告。
func (csiStorageCapacityStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return storageutil.GetWarningsForCSIStorageCapacity(obj.(*storage.CSIStorageCapacity))
}

// Canonicalize 在校验后规范化 CSIStorageCapacity；当前不执行额外转换。
func (csiStorageCapacityStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，CSIStorageCapacity 必须通过 POST 创建。
func (csiStorageCapacityStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 当前无需修改 CSIStorageCapacity 的更新请求对象。
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

// WarningsOnUpdate 返回更新 CSIStorageCapacity 时的拓扑和容量相关警告。
func (csiStorageCapacityStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return storageutil.GetWarningsForCSIStorageCapacity(obj.(*storage.CSIStorageCapacity))
}

// AllowUnconditionalUpdate 返回 false，更新请求必须携带 ResourceVersion。
func (csiStorageCapacityStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// hasInvalidLabelValueInLabelSelector 检查旧 CSIStorageCapacity 的 NodeTopology 是否包含当前规则下无效的 Label 值，用于判断更新时是否需要兼容历史数据。
func hasInvalidLabelValueInLabelSelector(capacity *storage.CSIStorageCapacity) bool {
	labelSelectorValidationOptions := metav1validation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: false}
	return len(metav1validation.ValidateLabelSelector(capacity.NodeTopology, labelSelectorValidationOptions, nil)) > 0
}
