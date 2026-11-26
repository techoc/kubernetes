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

package rest

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/utils/ptr"
)

// RESTDeleteStrategy 定义遵循 Kubernetes API 约定的对象删除行为。
type RESTDeleteStrategy interface {
	runtime.ObjectTyper
}

// GarbageCollectionPolicy 表示删除对象时对其依赖对象的默认处理方式。
// 只有实现了 GarbageCollectionDeleteStrategy 的资源才有意义。
type GarbageCollectionPolicy string

const (
	// DeleteDependents 级联删除依赖对象（默认）。
	DeleteDependents GarbageCollectionPolicy = "DeleteDependents"
	// OrphanDependents 保留依赖对象，仅移除其 ownerReferences。
	OrphanDependents GarbageCollectionPolicy = "OrphanDependents"
	// Unsupported 表示该资源无法被 GC，因此 finalizer
	// 绝不应写入存储。
	Unsupported GarbageCollectionPolicy = "Unsupported"
)

// 默认孤立依赖对象的 registry 必须实现 GarbageCollectionDeleteStrategy。
type GarbageCollectionDeleteStrategy interface {
	// DefaultGarbageCollectionPolicy 返回默认的垃圾回收行为。
	// 仅当请求未显式指定 orphanDependents / propagationPolicy 时使用。
	DefaultGarbageCollectionPolicy(ctx context.Context) GarbageCollectionPolicy
}

// 支持优雅删除的 registry 必须实现 RESTGracefulDeleteStrategy。
type RESTGracefulDeleteStrategy interface {
	// 对象可优雅删除时 CheckGracefulDelete 应返回 true，并为 DeleteOptions
	// 设置默认值。
	// 注意：返回 true 时 `options.GracePeriodSeconds` 必须非 nil（nil 会失败），
	// 它决定删除的“优雅”程度。
	CheckGracefulDelete(ctx context.Context, obj runtime.Object, options *metav1.DeleteOptions) bool
}

// BeforeDelete 测试对象能否被优雅删除。
// graceful 置位时应优雅删除；gracefulPending 置位表示
// 对象此前已被优雅删除（且所给宽限期长于删除所需时间）。
// 无法检查条件或 gracePeriodSeconds 非法时返回错误。graceful 为 true 时
// options 参数可被更新为默认值。设置 deletionTimestamp 的第二处
// 在 pkg/registry/generic/registry/store.go。
// 本函数负责在优雅删除期间设置 deletionTimestamp，
// 另一处负责级联删除。
//
// 两个布尔返回值的语义：
//   - graceful: 本次请求应执行优雅删除，调用方需持久化更新后的对象；
//   - gracefulPending: 对象已处于优雅删除中，本次请求只需等待，不做写入。
//
// 注意：这里在写入前提前校验 preconditions 只是尽早失败，真正的强制仍在
// 存储层完成，二者不可互相替代（存在检查与写入之间的竞态）。
func BeforeDelete(strategy RESTDeleteStrategy, ctx context.Context, obj runtime.Object, options *metav1.DeleteOptions) (graceful, gracefulPending bool, err error) {
	objectMeta, gvk, kerr := objectMetaAndKind(strategy, obj)
	if kerr != nil {
		return false, false, kerr
	}
	// 先校验 DeleteOptions 本身，避免把非法参数带入后续逻辑。
	if errs := validation.ValidateDeleteOptions(options); len(errs) > 0 {
		return false, false, errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "DeleteOptions"}, "", errs)
	}
	// 在此处检查前置条件以便尽早失败；真正执行删除时这些前置条件同样会被强制校验。
	// UID 不匹配通常意味着“对象已被删除并重建”，这与 ResourceVersion 冲突
	// 是两类不同的用户错误，错误信息中要能区分。
	if options.Preconditions != nil {
		if options.Preconditions.UID != nil && *options.Preconditions.UID != objectMeta.GetUID() {
			return false, false, errors.NewConflict(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, objectMeta.GetName(), fmt.Errorf("the UID in the precondition (%s) does not match the UID in record (%s). The object might have been deleted and then recreated", *options.Preconditions.UID, objectMeta.GetUID()))
		}
		if options.Preconditions.ResourceVersion != nil && *options.Preconditions.ResourceVersion != objectMeta.GetResourceVersion() {
			return false, false, errors.NewConflict(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, objectMeta.GetName(), fmt.Errorf("the ResourceVersion in the precondition (%s) does not match the ResourceVersion in record (%s). The object might have been modified", *options.Preconditions.ResourceVersion, objectMeta.GetResourceVersion()))
		}
	}

	// 负数在删除路径上会被当作 `1s` 处理。
	// 负数在旧客户端里表示“立即删除”，这里统一规范为 1s，避免下游出现
	// 负时长导致的计时异常。
	if gracePeriodSeconds := options.GracePeriodSeconds; gracePeriodSeconds != nil && *gracePeriodSeconds < 0 {
		options.GracePeriodSeconds = ptr.To[int64](1)
	}
	if deletionGracePeriodSeconds := objectMeta.GetDeletionGracePeriodSeconds(); deletionGracePeriodSeconds != nil && *deletionGracePeriodSeconds < 0 {
		objectMeta.SetDeletionGracePeriodSeconds(ptr.To[int64](1))
	}

	gracefulStrategy, ok := strategy.(RESTGracefulDeleteStrategy)
	if !ok {
		// 非优雅删除时无需更新 Generation，因为删除前
		// 不会更新对象本身。
		return false, false, nil
	}
	// 对象已在删除中时，无需更新 generation。
	if objectMeta.GetDeletionTimestamp() != nil {
		// 已在删除中时，只能缩短宽限期。
		// 这说明对象此前已被优雅删除，但未设置 deletionGracePeriodSeconds，
		// 因此强制立即删除。
		// 重要：
		// 删除分两个阶段进行。
		// 1. 更新以设置 DeletionGracePeriodSeconds 与 DeletionTimestamp
		// 2. 从存储中删除对象。
		// 若更新成功但删除失败（网络错误、内部存储错误等），
		// 资源此前会陷入不可恢复状态。这里检查已存储资源
		// 的宽限期是否为 0，若是则尝试立即删除以恢复。
		// 若更新成功但删除失败（网络错误、内部存储错误等），
		if objectMeta.GetDeletionGracePeriodSeconds() == nil || *objectMeta.GetDeletionGracePeriodSeconds() == 0 {
			return false, false, nil
		}
		// 用户只能提供更短的优雅删除期。
		// 只允许缩短宽限期：延长会破坏“删除已开始”的承诺，导致对象迟迟不消失。
		if options.GracePeriodSeconds != nil {
			period := int64(*options.GracePeriodSeconds)
			if period >= *objectMeta.GetDeletionGracePeriodSeconds() {
				return false, true, nil
			}
			// 先把现有 deletionTimestamp 回退现有的 DeletionGracePeriod，再前进 options 的宽限期。
			// 回退是因为此代码路径只能缩短宽限期。
			newDeletionTimestamp := metav1.NewTime(
				objectMeta.GetDeletionTimestamp().Add(-time.Second * time.Duration(*objectMeta.GetDeletionGracePeriodSeconds())).
					Add(time.Second * time.Duration(*options.GracePeriodSeconds)))
			// 防止缩短宽限期导致 deletionTimestamp 落到过去
			if now := metav1Now(); newDeletionTimestamp.Before(&now) {
				newDeletionTimestamp = now
				if period != 0 {
					// 请求了优雅删除（options.GracePeriodSeconds != 0），但整个宽限期已过，
					// 因此缩短到仍视为优雅删除的最小周期。
					// 这意味着 apiserver 更新对象，另一个角色观察到该更新，
					// 并仍负责以 options.GracePeriodSeconds == 0 完成最终删除。
					period = int64(1)
				}
			}
			objectMeta.SetDeletionTimestamp(&newDeletionTimestamp)
			objectMeta.SetDeletionGracePeriodSeconds(&period)
			return true, false, nil
		}
		// 优雅删除待处理时，不做任何操作
		options.GracePeriodSeconds = objectMeta.GetDeletionGracePeriodSeconds()
		return false, true, nil
	}

	// `CheckGracefulDelete` 由具体的删除策略实现。
	// 由具体资源决定是否需要优雅删除，并负责填充默认的 GracePeriodSeconds。
	if !gracefulStrategy.CheckGracefulDelete(ctx, obj, options) {
		return false, false, nil
	}

	if options.GracePeriodSeconds == nil {
		return false, false, errors.NewInternalError(fmt.Errorf("options.GracePeriodSeconds should not be nil"))
	}

	requestedDeletionTimestamp := metav1.NewTime(metav1Now().Add(time.Second * time.Duration(*options.GracePeriodSeconds)))
	objectMeta.SetDeletionTimestamp(&requestedDeletionTimestamp)
	objectMeta.SetDeletionGracePeriodSeconds(options.GracePeriodSeconds)
	// 首次优雅删除时，要把 DeletionTimestamp 设为非 nil。
	// 被删除对象的控制器不应执行任何重要操作，因此其行为会改变。
	// 所以需要增加对象的 Generation（若存在）。这里处理优雅删除时的 generation 递增。
	// 不支持优雅删除的对象的递增在 pkg/registry/generic/registry/store.go 中处理。
	if objectMeta.GetGeneration() > 0 {
		objectMeta.SetGeneration(objectMeta.GetGeneration() + 1)
	}

	return true, false, nil
}

// AdmissionToValidateObjectDeleteFunc 返回用于对象删除的准入校验函数
//
// 删除路径的准入使用一个“虚拟”的属性记录：新对象为 nil，旧对象为待删除对象
// 的深拷贝，避免准入插件意外修改存储中的对象。
func AdmissionToValidateObjectDeleteFunc(admit admission.Interface, staticAttributes admission.Attributes, objInterfaces admission.ObjectInterfaces) ValidateObjectFunc {
	mutatingAdmission, isMutatingAdmission := admit.(admission.MutationInterface)
	validatingAdmission, isValidatingAdmission := admit.(admission.ValidationInterface)

	mutating := isMutatingAdmission && mutatingAdmission.Handles(staticAttributes.GetOperation())
	validating := isValidatingAdmission && validatingAdmission.Handles(staticAttributes.GetOperation())

	return func(ctx context.Context, old runtime.Object) error {
		// 两种准入都未注册时完全跳过，删除路径不应为此付出额外开销。
		if !mutating && !validating {
			return nil
		}
		finalAttributes := admission.NewAttributesRecord(
			nil,
			// 深拷贝对象，避免意外修改原对象。
			old.DeepCopyObject(),
			staticAttributes.GetKind(),
			staticAttributes.GetNamespace(),
			staticAttributes.GetName(),
			staticAttributes.GetResource(),
			staticAttributes.GetSubresource(),
			staticAttributes.GetOperation(),
			staticAttributes.GetOperationOptions(),
			staticAttributes.IsDryRun(),
			staticAttributes.GetUserInfo(),
		)
		if mutating {
			if err := mutatingAdmission.Admit(ctx, finalAttributes, objInterfaces); err != nil {
				return err
			}
		}
		if validating {
			if err := validatingAdmission.Validate(ctx, finalAttributes, objInterfaces); err != nil {
				return err
			}
		}
		return nil
	}
}
