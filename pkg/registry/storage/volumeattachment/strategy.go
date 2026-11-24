/*
Copyright 2017 The Kubernetes Authors.

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

// 本文件实现 volumeattachment 的 REST Strategy，定义作用域、写入前字段处理、校验、告警以及更新规则。
package volumeattachment

import (
	"context"

	"k8s.io/apiserver/pkg/registry/rest"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/apis/storage/validation"
	"k8s.io/kubernetes/pkg/features"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// volumeAttachmentStrategy 定义 VolumeAttachment 主资源和 Status 子资源的更新策略。
type volumeAttachmentStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建、更新和删除 VolumeAttachment 时使用的默认策略。
var Strategy = volumeAttachmentStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

func (volumeAttachmentStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 声明主资源写入时需要重置的 Status 字段，防止用户直接修改控制器维护的附加状态。
func (volumeAttachmentStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"storage.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 清空客户端提交的 Status，附加状态由 Attach/Detach 控制器维护。
func (volumeAttachmentStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	volumeAttachment := obj.(*storage.VolumeAttachment)
	volumeAttachment.Status = storage.VolumeAttachmentStatus{}
}

// Validate 执行创建请求的业务合法性校验，将字段路径和错误原因整理为 field.ErrorList；返回非空错误时 API 请求会被拒绝。
func (volumeAttachmentStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	volumeAttachment := obj.(*storage.VolumeAttachment)
	return validation.ValidateVolumeAttachment(volumeAttachment)
}

// WarningsOnCreate 返回创建 VolumeAttachment 时的非阻断性警告；当前不产生额外警告。
func (volumeAttachmentStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 VolumeAttachment；当前不执行额外转换。
func (volumeAttachmentStrategy) Canonicalize(obj runtime.Object) {
}

func (volumeAttachmentStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 恢复旧 Status；VolumeAttachment 的 Spec 不允许更新，因此无需递增 Generation。
func (volumeAttachmentStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newVolumeAttachment := obj.(*storage.VolumeAttachment)
	oldVolumeAttachment := old.(*storage.VolumeAttachment)

	newVolumeAttachment.Status = oldVolumeAttachment.Status

}

// ValidateUpdate 对更新后的对象及其旧版本进行联合校验，检查字段合法性、不可变字段和跨版本兼容约束。
func (volumeAttachmentStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newVolumeAttachmentObj := obj.(*storage.VolumeAttachment)
	oldVolumeAttachmentObj := old.(*storage.VolumeAttachment)
	return validation.ValidateVolumeAttachmentUpdate(newVolumeAttachmentObj, oldVolumeAttachmentObj)
}

// WarningsOnUpdate 返回更新 VolumeAttachment 时的非阻断性警告；当前不产生额外警告。
func (volumeAttachmentStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (volumeAttachmentStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// volumeAttachmentStatusStrategy 定义 VolumeAttachment /status 子资源的更新策略。
type volumeAttachmentStatusStrategy struct {
	volumeAttachmentStrategy
}

// StatusStrategy 是通过 REST API 更新 VolumeAttachment Status 时使用的默认策略。
var StatusStrategy = volumeAttachmentStatusStrategy{Strategy}

// GetResetFields 声明 Status 子资源写入时需要重置的 Spec 和 metadata，确保该入口只能修改 Status。
func (volumeAttachmentStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"storage.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 恢复旧 Spec 并重置受保护的 metadata，只允许更新 Status。
func (volumeAttachmentStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newVolumeAttachment := obj.(*storage.VolumeAttachment)
	oldVolumeAttachment := old.(*storage.VolumeAttachment)

	newVolumeAttachment.Spec = oldVolumeAttachment.Spec
	metav1.ResetObjectMetaForStatus(&newVolumeAttachment.ObjectMeta, &oldVolumeAttachment.ObjectMeta)

	if !feature.DefaultFeatureGate.Enabled(features.MutableCSINodeAllocatableCount) {
		// 只有旧对象未设置 ErrorCode 时才清理新对象中的该字段，以兼容已有状态数据。
		if newVolumeAttachment.Status.AttachError != nil {
			if oldVolumeAttachment.Status.AttachError == nil || oldVolumeAttachment.Status.AttachError.ErrorCode == nil {
				newVolumeAttachment.Status.AttachError.ErrorCode = nil
			}
		}
		if newVolumeAttachment.Status.DetachError != nil {
			if oldVolumeAttachment.Status.DetachError == nil || oldVolumeAttachment.Status.DetachError.ErrorCode == nil {
				newVolumeAttachment.Status.DetachError.ErrorCode = nil
			}
		}
	}
}
