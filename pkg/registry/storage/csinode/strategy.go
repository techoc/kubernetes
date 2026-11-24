/*
Copyright 2019 The Kubernetes Authors.

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

// 本文件实现 csinode 的 REST Strategy，定义作用域、写入前字段处理、校验、告警以及更新规则。
package csinode

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/apis/storage/validation"
	"k8s.io/kubernetes/pkg/features"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// csiNodeStrategy 定义 CSINode 主资源和 Status 子资源的创建、更新及校验策略。
type csiNodeStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建、更新和删除 CSINode 时使用的默认策略。
var Strategy = csiNodeStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，因为 CSINode 是集群级资源。
func (csiNodeStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 声明主资源写入时需要重置的 Status 字段，防止用户直接修改驱动维护的节点状态。
func (csiNodeStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"storage.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}
	return fields
}

// PrepareForCreate 清空客户端提交的 Status，并按 CSIVolumeHealth 特性门控清理不可用字段。
func (csiNodeStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	csiNode := obj.(*storage.CSINode)
	csiNode.Status = storage.CSINodeStatus{}
	dropDisabledCSINodeFields(csiNode, nil)
}

// Validate 执行创建请求的业务合法性校验，将字段路径和错误原因整理为 field.ErrorList；返回非空错误时 API 请求会被拒绝。
func (csiNodeStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	csiNode := obj.(*storage.CSINode)
	return validation.ValidateCSINode(csiNode)
}

// WarningsOnCreate 返回创建 CSINode 时的非阻断性警告；当前不产生额外警告。
func (csiNodeStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// Canonicalize 在校验后规范化 CSINode；当前不执行额外转换。
func (csiNodeStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，CSINode 必须通过 POST 创建。
func (csiNodeStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 恢复旧 Status，防止用户修改由 CSI 驱动维护的 Allocatable 等字段。
func (csiNodeStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newCSINode := obj.(*storage.CSINode)
	oldCSINode := old.(*storage.CSINode)
	newCSINode.Status = oldCSINode.Status
	dropDisabledCSINodeFields(newCSINode, oldCSINode)
}

// ValidateUpdate 对更新后的对象及其旧版本进行联合校验，检查字段合法性、不可变字段和跨版本兼容约束。
func (csiNodeStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newCSINodeObj := obj.(*storage.CSINode)
	oldCSINodeObj := old.(*storage.CSINode)
	return validation.ValidateCSINodeUpdate(newCSINodeObj, oldCSINodeObj)
}

// WarningsOnUpdate 返回更新 CSINode 时的非阻断性警告；当前不产生额外警告。
func (csiNodeStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，更新请求必须携带 ResourceVersion。
func (csiNodeStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// csiNodeStatusStrategy 定义 CSINode /status 子资源的更新策略。
type csiNodeStatusStrategy struct {
	csiNodeStrategy
}

// StatusStrategy 是通过 REST API 更新 CSINode Status 时使用的默认策略。
var StatusStrategy = csiNodeStatusStrategy{Strategy}

// GetResetFields 声明 Status 子资源写入时需要重置的 metadata 和 Spec，确保该入口只能修改 Status。
func (csiNodeStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"storage.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}
	return fields
}

// PrepareForUpdate 恢复旧 Spec 并重置受保护的 metadata，确保 Status 更新不会修改主资源配置。
func (csiNodeStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newCSINode := obj.(*storage.CSINode)
	oldCSINode := old.(*storage.CSINode)
	newCSINode.Spec = oldCSINode.Spec
	metav1.ResetObjectMetaForStatus(&newCSINode.ObjectMeta, &oldCSINode.ObjectMeta)
	dropDisabledCSINodeFields(newCSINode, oldCSINode)
}

// ValidateUpdate 对更新后的对象及其旧版本进行联合校验，检查字段合法性、不可变字段和跨版本兼容约束。
func (csiNodeStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newCSINode := obj.(*storage.CSINode)
	oldCSINode := old.(*storage.CSINode)
	return validation.ValidateCSINodeStatusUpdate(newCSINode, oldCSINode)
}

// dropDisabledCSINodeFields 根据 Feature Gate 和旧对象内容清理当前版本不允许新写入的 CSINode 字段；若旧对象已有字段值，则保留以支持降级兼容。
func dropDisabledCSINodeFields(newObj, oldObj *storage.CSINode) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.CSIVolumeHealth) {
		if oldObj == nil || len(oldObj.Status.StorageHealth) == 0 {
			newObj.Status.StorageHealth = nil
		}
	}
}
