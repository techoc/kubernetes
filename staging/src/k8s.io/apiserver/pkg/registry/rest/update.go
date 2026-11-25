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

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/operation"
	genericvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/admission"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/warning"
)

// RESTUpdateStrategy defines the minimum validation, accepted input, and
// name generation behavior to update an object that follows Kubernetes
// API conventions. A resource may have many UpdateStrategies, depending on
// the call pattern in use.
//
// The context that is passed to these methods can be used to retrieve
// information about the request with request.RequestInfoFrom(ctx) and
// to return different responses depending on the APIVersion in the
// request.
type RESTUpdateStrategy interface {
	runtime.ObjectTyper
	// NamespaceScoped returns true if the object must be within a namespace.
	NamespaceScoped() bool
	// AllowCreateOnUpdate returns true if the object can be created by a PUT.
	AllowCreateOnUpdate(ctx context.Context) bool
	// PrepareForUpdate is invoked on update before validation to normalize
	// the object.  For example: remove fields that are not to be persisted,
	// sort order-insensitive list fields, etc.  This should not remove fields
	// whose presence would be considered a validation error.
	PrepareForUpdate(ctx context.Context, obj, old runtime.Object)
	// ValidateUpdate is invoked after default fields in the object have been
	// filled in before the object is persisted.  This method should not mutate
	// the object.
	ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList
	// WarningsOnUpdate returns warnings to the client performing the update.
	// WarningsOnUpdate is invoked after default fields in the object have been filled in
	// and after ValidateUpdate has passed, before Canonicalize is called, and before the object is persisted.
	// This method must not mutate either object.
	//
	// Be brief; limit warnings to 120 characters if possible.
	// Don't include a "Warning:" prefix in the message (that is added by clients on output).
	// Warnings returned about a specific field should be formatted as "path.to.field: message".
	// For example: `spec.imagePullSecrets[0].name: invalid empty name ""`
	//
	// Use warning messages to describe problems the client making the API request should correct or be aware of.
	// For example:
	// - use of deprecated fields/labels/annotations that will stop working in a future release
	// - use of obsolete fields/labels/annotations that are non-functional
	// - malformed or invalid specifications that prevent successful handling of the submitted object,
	//   but are not rejected by validation for compatibility reasons
	//
	// Warnings should not be returned for fields which cannot be resolved by the caller.
	// For example, do not warn about spec fields in a status update.
	WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string
	// Canonicalize allows an object to be mutated into a canonical form. This
	// ensures that code that operates on these objects can rely on the common
	// form for things like comparison.  Canonicalize is invoked after
	// validation has succeeded but before the object has been persisted.
	// This method may mutate the object.
	Canonicalize(obj runtime.Object)
	// AllowUnconditionalUpdate returns true if the object can be updated
	// unconditionally (irrespective of the latest resource version), when
	// there is no resource version specified in the object.
	AllowUnconditionalUpdate(ctx context.Context) bool
}

// TODO: add other common fields that require global validation.
// validateCommonFields 校验所有资源共有的元数据：名称/命名空间合法性，
// 以及更新时不允许改变的字段（UID、创建时间等由 ValidateObjectMetaAccessorUpdate 处理）。
func validateCommonFields(obj, old runtime.Object, strategy RESTUpdateStrategy) field.ErrorList {
	allErrs := field.ErrorList{}
	objectMeta, err := meta.Accessor(obj)
	if err != nil {
		allErrs = append(allErrs, field.InternalError(field.NewPath("metadata"), fmt.Errorf("failed to get new object metadata: %w", err)))
		return allErrs
	}
	oldObjectMeta, err := meta.Accessor(old)
	if err != nil {
		allErrs = append(allErrs, field.InternalError(field.NewPath("metadata"), fmt.Errorf("failed to get old object metadata: %w", err)))
		return allErrs
	}
	allErrs = append(allErrs, genericvalidation.ValidateObjectMetaAccessor(objectMeta, strategy.NamespaceScoped(), validatePathSegment, field.NewPath("metadata"))...)
	allErrs = append(allErrs, genericvalidation.ValidateObjectMetaAccessorUpdate(objectMeta, oldObjectMeta, field.NewPath("metadata"))...)

	return allErrs
}

// BeforeUpdate 负责在所有资源更新时执行公共操作，只返回可转换为 api.Status 的错误，
// 并使用给定的已存在对象与更新后对象调用更新校验。
// 只有当对象相应字段尚无零值时，它才写入零值。
// 这里显式回填 generation、UID、时间戳与优雅删除参数：这些字段由服务器管理，
// 客户端在 PUT 中携带的任何改动都必须被覆盖，否则会破坏对象生命周期语义。
func BeforeUpdate(strategy RESTUpdateStrategy, ctx context.Context, obj, old runtime.Object) error {
	objectMeta, kind, kerr := objectMetaAndKind(strategy, obj)
	if kerr != nil {
		return kerr
	}

	// ensure namespace on the object is correct, or error if a conflicting namespace was set in the object
	requestNamespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return errors.NewInternalError(fmt.Errorf("no namespace information found in request context"))
	}
	if err := EnsureObjectNamespaceMatchesRequestNamespace(ExpectedNamespaceForScope(requestNamespace, strategy.NamespaceScoped()), objectMeta); err != nil {
		return err
	}

	// Ensure requests cannot update generation
	oldMeta, err := meta.Accessor(old)
	if err != nil {
		return err
	}
	objectMeta.SetGeneration(oldMeta.GetGeneration())

	strategy.PrepareForUpdate(ctx, obj, old)

	// 未提供 UID 时沿用旧对象的 UID。
	// 允许客户端省略 UID（服务端补齐），但不允许改成另一个值——那会被后续校验拒绝。
	if len(objectMeta.GetUID()) == 0 {
		objectMeta.SetUID(oldMeta.GetUID())
	}
	// ignore changes to timestamp
	if oldCreationTime := oldMeta.GetCreationTimestamp(); !oldCreationTime.IsZero() {
		objectMeta.SetCreationTimestamp(oldMeta.GetCreationTimestamp())
	}
	// an update can never remove/change a deletion timestamp
	if !oldMeta.GetDeletionTimestamp().IsZero() {
		objectMeta.SetDeletionTimestamp(oldMeta.GetDeletionTimestamp())
	}
	// 更新操作永远不能移除或修改优雅删除期秒数。
	// 注意只在客户端未提供时回填：finalizer 等流程可能合法地调整优雅删除期。
	if oldMeta.GetDeletionGracePeriodSeconds() != nil && objectMeta.GetDeletionGracePeriodSeconds() == nil {
		objectMeta.SetDeletionGracePeriodSeconds(oldMeta.GetDeletionGracePeriodSeconds())
	}
	errs := ValidateUpdate(ctx, obj, old, strategy)
	if len(errs) > 0 {
		RecordDuplicateValidationErrors(ctx, kind.GroupKind(), errs)
		return errors.NewInvalid(kind.GroupKind(), objectMeta.GetName(), errs)
	}

	for _, w := range strategy.WarningsOnUpdate(ctx, obj, old) {
		warning.AddWarning(ctx, "", w)
	}

	strategy.Canonicalize(obj)

	return nil
}

// ValidateUpdate performs common and strategy-specific validation for an update operation.
func ValidateUpdate(ctx context.Context, obj runtime.Object, old runtime.Object, strategy RESTUpdateStrategy) field.ErrorList {

	// TODO: Replace this check with the ObjectMeta name validation (validatePathSegment) once we are sure that all other validations are covered by strategy.Validate and strategy.ValidateDeclaratively.
	// Ensure some common fields, like UID, are validated for all resources.
	errs := validateCommonFields(obj, old, strategy)

	errs = append(errs, strategy.ValidateUpdate(ctx, obj, old)...)
	// 声明式校验与命令式校验的错误合并返回，便于客户端一次看到全部问题。
	if dv, ok := strategy.(DeclarativeValidationStrategy); ok {
		errs = dv.ValidateDeclaratively(ctx, obj, old, errs, operation.Update, dv.DeclarativeValidationConfig(ctx, obj, old))
	}
	return errs
}

// TransformFunc 用于转换并返回 newObj。
// 变换函数可以在持久化前注入服务端生成的字段，或基于旧对象做局部更新。
type TransformFunc func(ctx context.Context, newObj runtime.Object, oldObj runtime.Object) (transformedNewObj runtime.Object, err error)

// defaultUpdatedObjectInfo implements UpdatedObjectInfo
type defaultUpdatedObjectInfo struct {
	// obj is the updated object
	obj runtime.Object

	// transformers is an optional list of transforming functions that modify or
	// replace obj using information from the context, old object, or other sources.
	transformers []TransformFunc
}

// DefaultUpdatedObjectInfo returns an UpdatedObjectInfo impl based on the specified object.
func DefaultUpdatedObjectInfo(obj runtime.Object, transformers ...TransformFunc) UpdatedObjectInfo {
	return &defaultUpdatedObjectInfo{obj, transformers}
}

// Preconditions 实现 UpdatedObjectInfo 接口。
// 若对象带有 UID，则生成 UID 前置条件，保证删除后重建的同名对象不会被误更新。
func (i *defaultUpdatedObjectInfo) Preconditions() *metav1.Preconditions {
	// Attempt to get the UID out of the object
	accessor, err := meta.Accessor(i.obj)
	if err != nil {
		// If no UID can be read, no preconditions are possible
		return nil
	}

	// If empty, no preconditions needed
	uid := accessor.GetUID()
	if len(uid) == 0 {
		return nil
	}

	return &metav1.Preconditions{UID: &uid}
}

// UpdatedObject satisfies the UpdatedObjectInfo interface.
// It returns a copy of the held obj, passed through any configured transformers.
func (i *defaultUpdatedObjectInfo) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	var err error
	// Start with the configured object
	newObj := i.obj

	// 若原始对象非 nil（当第一个 transformer 基于 oldObj 构造对象时可能为 nil），就做一次
	// 拷贝，以免把原始对象返回出去：BeforeUpdate 可能修改返回的对象，例如清空
	// ResourceVersion；如果被再次调用，我们必须能返回未被污染的版本。
	// 深拷贝是关键：UpdatedObject 可能在重试中被多次调用，必须每次返回未受污染的原始对象。
	if newObj != nil {
		newObj = newObj.DeepCopyObject()
	}

	// Allow any configured transformers to update the new object
	for _, transformer := range i.transformers {
		newObj, err = transformer(ctx, newObj, oldObj)
		if err != nil {
			return nil, err
		}
	}

	return newObj, nil
}

// wrappedUpdatedObjectInfo allows wrapping an existing objInfo and
// chaining additional transformations/checks on the result of UpdatedObject()
type wrappedUpdatedObjectInfo struct {
	// obj is the updated object
	objInfo UpdatedObjectInfo

	// transformers is an optional list of transforming functions that modify or
	// replace obj using information from the context, old object, or other sources.
	transformers []TransformFunc
}

// WrapUpdatedObjectInfo returns an UpdatedObjectInfo impl that delegates to
// the specified objInfo, then calls the passed transformers
func WrapUpdatedObjectInfo(objInfo UpdatedObjectInfo, transformers ...TransformFunc) UpdatedObjectInfo {
	return &wrappedUpdatedObjectInfo{objInfo, transformers}
}

// Preconditions 实现 UpdatedObjectInfo 接口。
// 前置条件由被包装对象决定，包装层不额外添加。
func (i *wrappedUpdatedObjectInfo) Preconditions() *metav1.Preconditions {
	return i.objInfo.Preconditions()
}

// UpdatedObject satisfies the UpdatedObjectInfo interface.
// It delegates to the wrapped objInfo and passes the result through any configured transformers.
func (i *wrappedUpdatedObjectInfo) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	newObj, err := i.objInfo.UpdatedObject(ctx, oldObj)
	if err != nil {
		return newObj, err
	}

	// Allow any configured transformers to update the new object or error
	for _, transformer := range i.transformers {
		newObj, err = transformer(ctx, newObj, oldObj)
		if err != nil {
			return nil, err
		}
	}

	return newObj, nil
}

// AdmissionToValidateObjectUpdateFunc 把校验型准入转换为 rest 的更新对象校验函数。
// 与创建版本对称：准入不处理该操作时直接放行，否则用新旧对象重新构造属性执行校验。
func AdmissionToValidateObjectUpdateFunc(admit admission.Interface, staticAttributes admission.Attributes, o admission.ObjectInterfaces) ValidateObjectUpdateFunc {
	validatingAdmission, ok := admit.(admission.ValidationInterface)
	if !ok {
		return func(ctx context.Context, obj, old runtime.Object) error { return nil }
	}
	return func(ctx context.Context, obj, old runtime.Object) error {
		finalAttributes := admission.NewAttributesRecord(
			obj,
			old,
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
		if !validatingAdmission.Handles(finalAttributes.GetOperation()) {
			return nil
		}
		return validatingAdmission.Validate(ctx, finalAttributes, o)
	}
}
