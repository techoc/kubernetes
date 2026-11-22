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
	"k8s.io/apimachinery/pkg/api/validate/content"
	genericvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/admission"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/apiserver/pkg/warning"
)

// RESTCreateStrategy defines the minimum validation, accepted input, and
// name generation behavior to create an object that follows Kubernetes
// API conventions.
// 各资源的实现只需关注自身字段语义；命名空间、UID 等通用元数据由本包统一处理。
type RESTCreateStrategy interface {
	runtime.ObjectTyper
	// The name generator is used when the standard GenerateName field is set.
	// The NameGenerator will be invoked prior to validation.
	names.NameGenerator

	// NamespaceScoped returns true if the object must be within a namespace.
	NamespaceScoped() bool
	// PrepareForCreate is invoked on create before validation to normalize
	// the object.  For example: remove fields that are not to be persisted,
	// sort order-insensitive list fields, etc.  This should not remove fields
	// whose presence would be considered a validation error.
	//
	// Often implemented as a type check and an initailization or clearing of
	// status. Clear the status because status changes are internal. External
	// callers of an api (users) should not be setting an initial status on
	// newly created objects.
	PrepareForCreate(ctx context.Context, obj runtime.Object)
	// Validate returns an ErrorList with validation errors or nil.  Validate
	// is invoked after default fields in the object have been filled in
	// before the object is persisted.  This method should not mutate the
	// object.
	Validate(ctx context.Context, obj runtime.Object) field.ErrorList
	// WarningsOnCreate returns warnings to the client performing a create.
	// WarningsOnCreate is invoked after default fields in the object have been filled in
	// and after Validate has passed, before Canonicalize is called, and the object is persisted.
	// This method must not mutate the object.
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
	// For example, do not warn about spec fields in a subresource creation request.
	WarningsOnCreate(ctx context.Context, obj runtime.Object) []string
	// Canonicalize allows an object to be mutated into a canonical form. This
	// ensures that code that operates on these objects can rely on the common
	// form for things like comparison.  Canonicalize is invoked after
	// validation has succeeded but before the object has been persisted.
	// This method may mutate the object. Often implemented as a type check or
	// empty method.
	Canonicalize(obj runtime.Object)
}

// validatePathSegment 按“是否作为前缀”选择路径段校验规则：
// generateName 允许不完整的名称，因此走 Prefix 校验。
func validatePathSegment(name string, prefix bool) []string {
	if prefix {
		return content.IsPathSegmentPrefix(name)
	}
	return content.IsPathSegmentName(name)
}

// BeforeCreate ensures that common operations for all resources are performed on creation. It only returns
// errors that can be converted to api.Status. It invokes PrepareForCreate, then Validate.
// It returns nil if the object should be created.
// 执行顺序不可随意调整：先确认系统字段与命名空间，再归一化、校验、告警、规范化，
// 任何一步失败都必须阻止对象落库。
func BeforeCreate(strategy RESTCreateStrategy, ctx context.Context, obj runtime.Object) error {
	objectMeta, kind, kerr := objectMetaAndKind(strategy, obj)
	if kerr != nil {
		return kerr
	}

	// ensure that system-critical metadata has been populated
	// UID、创建时间等系统字段由存储层/处理器预填；缺失说明调用链有误，属于内部错误。
	if !metav1.HasObjectMetaSystemFieldValues(objectMeta) {
		return errors.NewInternalError(fmt.Errorf("system metadata was not initialized"))
	}

	// ensure the name has been generated
	if len(objectMeta.GetGenerateName()) > 0 && len(objectMeta.GetName()) == 0 {
		return errors.NewInternalError(fmt.Errorf("metadata.name was not generated"))
	}

	// ensure namespace on the object is correct, or error if a conflicting namespace was set in the object
	// 以请求 URL 中的命名空间为准，拒绝对象自带冲突命名空间，防止跨命名空间写入。
	requestNamespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return errors.NewInternalError(fmt.Errorf("no namespace information found in request context"))
	}
	if err := EnsureObjectNamespaceMatchesRequestNamespace(ExpectedNamespaceForScope(requestNamespace, strategy.NamespaceScoped()), objectMeta); err != nil {
		return err
	}

	strategy.PrepareForCreate(ctx, obj)

	errs := ValidateCreate(ctx, obj, strategy)
	if len(errs) != 0 {
		return errors.NewInvalid(kind.GroupKind(), objectMeta.GetName(), errs)
	}

	// 告警不阻断创建，通过 Warning 头回传给客户端。
	for _, w := range strategy.WarningsOnCreate(ctx, obj) {
		warning.AddWarning(ctx, "", w)
	}

	// 规范化放在校验之后：校验看到的是用户输入，持久化的是规范形式。
	strategy.Canonicalize(obj)

	return nil
}

// ValidateCreate performs common and strategy-specific validation for a create operation.
func ValidateCreate(ctx context.Context, obj runtime.Object, strategy RESTCreateStrategy) field.ErrorList {
	errs := strategy.Validate(ctx, obj)

	// 通用元数据校验放在策略校验之后，保证能优先返回更具体的错误消息。
	if len(errs) == 0 {
		objectMeta, err := meta.Accessor(obj)
		if err != nil {
			return append(errs, field.InternalError(field.NewPath("metadata"), fmt.Errorf("failed to get object metadata: %w", err)))
		}

		// TODO: Replace this check with the ObjectMeta name validation (validatePathSegment) once we are sure that all other validations are covered by strategy.Validate and strategy.ValidateDeclaratively.

		// Custom validation (including name validation) passed
		// Now run common validation on object meta
		// Do this *after* custom validation so that specific error messages are shown whenever possible
		errs = append(errs, genericvalidation.ValidateObjectMetaAccessor(objectMeta, strategy.NamespaceScoped(), validatePathSegment, field.NewPath("metadata"))...)
	}

	// 声明式校验（基于 CEL 的标签）是可选的扩展；启用后与手写校验的错误合并返回。
	if dv, ok := strategy.(DeclarativeValidationStrategy); ok {
		errs = dv.ValidateDeclaratively(ctx, obj, nil, errs, operation.Create, dv.DeclarativeValidationConfig(ctx, obj, nil))
	}

	return errs
}

// CheckGeneratedNameError checks whether an error that occurred creating a resource is due
// to generation being unable to pick a valid name.
// 只有 AlreadyExists 且确实使用了 generateName 时才改写错误，
// 否则保留原错误（用户显式指定的名称冲突应原样返回）。
func CheckGeneratedNameError(ctx context.Context, strategy RESTCreateStrategy, err error, obj runtime.Object) error {
	if !errors.IsAlreadyExists(err) {
		return err
	}

	objectMeta, gvk, kerr := objectMetaAndKind(strategy, obj)
	if kerr != nil {
		return kerr
	}

	if len(objectMeta.GetGenerateName()) == 0 {
		// If we don't have a generated name, return the original error (AlreadyExists).
		// When we're here, the user picked a name that is causing a conflict.
		return err
	}

	// Get the group resource information from the context, if populated.
	gr := schema.GroupResource{}
	if requestInfo, found := genericapirequest.RequestInfoFrom(ctx); found {
		gr = schema.GroupResource{Group: gvk.Group, Resource: requestInfo.Resource}
	}

	// If we have a name and generated name, the server picked a name
	// that already exists.
	// 返回专门的 GenerateNameConflict，提示客户端重试即可，而非名称本身有问题。
	return errors.NewGenerateNameConflict(gr, objectMeta.GetName(), 1)
}

// objectMetaAndKind retrieves kind and ObjectMeta from a runtime object, or returns an error.
// 任何失败都视为内部错误：调用方传入的对象本应是合法的 runtime.Object。
func objectMetaAndKind(typer runtime.ObjectTyper, obj runtime.Object) (metav1.Object, schema.GroupVersionKind, error) {
	objectMeta, err := meta.Accessor(obj)
	if err != nil {
		return nil, schema.GroupVersionKind{}, errors.NewInternalError(err)
	}
	kinds, _, err := typer.ObjectKinds(obj)
	if err != nil {
		return nil, schema.GroupVersionKind{}, errors.NewInternalError(err)
	}
	// 同一 Go 类型可能对应多个 kind，取第一个作为规范 kind。
	return objectMeta, kinds[0], nil
}

// NamespaceScopedStrategy has a method to tell if the object must be in a namespace.
type NamespaceScopedStrategy interface {
	// NamespaceScoped returns if the object must be in a namespace.
	NamespaceScoped() bool
}

// AdmissionToValidateObjectFunc converts validating admission to a rest validate object func
// 用于在删除等“对象已从存储读出、尚未变更”的路径上重新执行准入校验。
// 若准入实现不支持 ValidationInterface，返回恒成功的函数，避免影响主流程。
func AdmissionToValidateObjectFunc(admit admission.Interface, staticAttributes admission.Attributes, o admission.ObjectInterfaces) ValidateObjectFunc {
	validatingAdmission, ok := admit.(admission.ValidationInterface)
	if !ok {
		return func(ctx context.Context, obj runtime.Object) error { return nil }
	}
	return func(ctx context.Context, obj runtime.Object) error {
		name := staticAttributes.GetName()
		// in case the generated name is populated
		// generateName 场景下请求中没有最终名称，需要从对象本身补齐，
		// 否则准入策略看到的 name 为空，可能误判。
		if len(name) == 0 {
			if metadata, err := meta.Accessor(obj); err == nil {
				name = metadata.GetName()
			}
		}

		finalAttributes := admission.NewAttributesRecord(
			obj,
			staticAttributes.GetOldObject(),
			staticAttributes.GetKind(),
			staticAttributes.GetNamespace(),
			name,
			staticAttributes.GetResource(),
			staticAttributes.GetSubresource(),
			staticAttributes.GetOperation(),
			staticAttributes.GetOperationOptions(),
			staticAttributes.IsDryRun(),
			staticAttributes.GetUserInfo(),
		)
		// 准入器可能只关心部分操作；不处理时直接放行。
		if !validatingAdmission.Handles(finalAttributes.GetOperation()) {
			return nil
		}
		return validatingAdmission.Validate(ctx, finalAttributes, o)
	}
}
