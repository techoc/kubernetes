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

// RESTUpdateStrategy 定义更新遵循 Kubernetes API 约定的对象时，
// 所需的最小校验、可接受的输入以及名称生成行为。一个资源可能有多个
// UpdateStrategy，取决于所使用的调用模式。
//
// 传给这些方法的 context 可以用来通过 request.RequestInfoFrom(ctx)
// 获取有关请求的信息，并据此根据请求中的 APIVersion 返回不同的响应。
type RESTUpdateStrategy interface {
	runtime.ObjectTyper
	// NamespaceScoped 在对象必须位于某个 namespace 内时返回 true。
	NamespaceScoped() bool
	// AllowCreateOnUpdate 在对象可以通过 PUT 创建时返回 true。
	AllowCreateOnUpdate(ctx context.Context) bool
	// PrepareForUpdate 在校验之前于更新时调用，用于规范化对象。
	// 例如：移除不应持久化的字段、对顺序不敏感的列表字段排序等。
	// 它不应移除那些一旦出现就会被视为校验错误的字段。
	PrepareForUpdate(ctx context.Context, obj, old runtime.Object)
	// ValidateUpdate 在对象中的默认字段填充完毕后、对象持久化之前调用。
	// 该方法不应修改对象。
	ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList
	// WarningsOnUpdate 返回给执行更新的客户端的警告。
	// WarningsOnUpdate 在对象中的默认字段填充完毕、且 ValidateUpdate 通过之后调用，
	// 在 Canonicalize 被调用之前，也在对象持久化之前。
	// 该方法必须不修改任何一个对象。
	//
	// 保持简短；警告尽量限制在 120 个字符以内。
	// 不要在消息中包含 "Warning:" 前缀（该前缀由客户端在输出时添加）。
	// 关于特定字段的警告应当格式化为 "path.to.field: message"。
	// 例如：`spec.imagePullSecrets[0].name: invalid empty name ""`
	//
	// 使用警告消息来描述发起 API 请求的客户端应当修正或需要知晓的问题。
	// 例如：
	// - 使用了将在未来版本停止工作的已弃用字段/标签/注解
	// - 使用了已废弃且无功能的字段/标签/注解
	// - 格式错误或无效的配置，会导致提交的对象无法被成功处理，
	//   但出于兼容性原因校验并未拒绝它们
	//
	// 对于调用方无法解决的问题字段，不应返回警告。
	// 例如，不要在 status 更新中针对 spec 字段发出警告。
	WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string
	// Canonicalize 允许将对象修改为规范形式。这确保操作这些对象的
	// 代码可以依赖统一的形式来做比较之类的事情。Canonicalize 在校验
	// 成功之后、对象持久化之前调用。
	// 该方法可以修改对象。
	Canonicalize(obj runtime.Object)
	// AllowUnconditionalUpdate 在对象中未指定 resource version 时，
	// 若对象可以被无条件更新（无论最新的 resource version 是什么），
	// 则返回 true。
	AllowUnconditionalUpdate(ctx context.Context) bool
}

// TODO: 添加其他需要全局校验的公共字段。
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

	// 确保对象上的 namespace 正确；若对象中设置了冲突的 namespace 则报错
	requestNamespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return errors.NewInternalError(fmt.Errorf("no namespace information found in request context"))
	}
	if err := EnsureObjectNamespaceMatchesRequestNamespace(ExpectedNamespaceForScope(requestNamespace, strategy.NamespaceScoped()), objectMeta); err != nil {
		return err
	}

	// 确保请求无法更新 generation
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
	// 忽略对 creationTimestamp 的修改
	if oldCreationTime := oldMeta.GetCreationTimestamp(); !oldCreationTime.IsZero() {
		objectMeta.SetCreationTimestamp(oldMeta.GetCreationTimestamp())
	}
	// 更新操作永远不能移除或修改 deletion timestamp
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

// ValidateUpdate 针对一次更新操作执行公共校验和策略特定的校验。
func ValidateUpdate(ctx context.Context, obj runtime.Object, old runtime.Object, strategy RESTUpdateStrategy) field.ErrorList {

	// TODO: 一旦确认所有其他校验都已由 strategy.Validate 和 strategy.ValidateDeclaratively 覆盖，就用 ObjectMeta 名称校验（validatePathSegment）替换此检查。
	// 确保所有资源的一些公共字段（例如 UID）经过校验。
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

// defaultUpdatedObjectInfo 实现 UpdatedObjectInfo
type defaultUpdatedObjectInfo struct {
	// obj 是被更新的对象
	obj runtime.Object

	// transformers 是可选的变换函数列表，用于借助来自上下文、旧对象
	// 或其他来源的信息来修改或替换 obj。
	transformers []TransformFunc
}

// DefaultUpdatedObjectInfo 返回一个基于指定对象的 UpdatedObjectInfo 实现。
func DefaultUpdatedObjectInfo(obj runtime.Object, transformers ...TransformFunc) UpdatedObjectInfo {
	return &defaultUpdatedObjectInfo{obj, transformers}
}

// Preconditions 实现 UpdatedObjectInfo 接口。
// 若对象带有 UID，则生成 UID 前置条件，保证删除后重建的同名对象不会被误更新。
func (i *defaultUpdatedObjectInfo) Preconditions() *metav1.Preconditions {
	// 尝试从对象中读取 UID
	accessor, err := meta.Accessor(i.obj)
	if err != nil {
		// 如果读不出 UID，就不可能有前置条件
		return nil
	}

	// 如果为空，则不需要前置条件
	uid := accessor.GetUID()
	if len(uid) == 0 {
		return nil
	}

	return &metav1.Preconditions{UID: &uid}
}

// UpdatedObject 满足 UpdatedObjectInfo 接口。
// 它返回所持 obj 的副本，并让其经过所有已配置的 transformers 处理。
func (i *defaultUpdatedObjectInfo) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	var err error
	// 从配置的对象开始
	newObj := i.obj

	// 若原始对象非 nil（当第一个 transformer 基于 oldObj 构造对象时可能为 nil），就做一次
	// 拷贝，以免把原始对象返回出去：BeforeUpdate 可能修改返回的对象，例如清空
	// ResourceVersion；如果被再次调用，我们必须能返回未被污染的版本。
	// 深拷贝是关键：UpdatedObject 可能在重试中被多次调用，必须每次返回未受污染的原始对象。
	if newObj != nil {
		newObj = newObj.DeepCopyObject()
	}

	// 允许所有已配置的 transformers 更新新对象
	for _, transformer := range i.transformers {
		newObj, err = transformer(ctx, newObj, oldObj)
		if err != nil {
			return nil, err
		}
	}

	return newObj, nil
}

// wrappedUpdatedObjectInfo 允许包装一个已有的 objInfo，
// 并在 UpdatedObject() 的结果上串联额外的转换/检查
type wrappedUpdatedObjectInfo struct {
	// obj 是被更新的对象
	objInfo UpdatedObjectInfo

	// transformers 是可选的变换函数列表，用于借助来自上下文、旧对象
	// 或其他来源的信息来修改或替换 obj。
	transformers []TransformFunc
}

// WrapUpdatedObjectInfo 返回一个 UpdatedObjectInfo 实现：先委托给
// 指定的 objInfo，然后调用传入的 transformers
func WrapUpdatedObjectInfo(objInfo UpdatedObjectInfo, transformers ...TransformFunc) UpdatedObjectInfo {
	return &wrappedUpdatedObjectInfo{objInfo, transformers}
}

// Preconditions 实现 UpdatedObjectInfo 接口。
// 前置条件由被包装对象决定，包装层不额外添加。
func (i *wrappedUpdatedObjectInfo) Preconditions() *metav1.Preconditions {
	return i.objInfo.Preconditions()
}

// UpdatedObject 满足 UpdatedObjectInfo 接口。
// 它委托给被包装的 objInfo，并把结果传递给所有已配置的 transformers。
func (i *wrappedUpdatedObjectInfo) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	newObj, err := i.objInfo.UpdatedObject(ctx, oldObj)
	if err != nil {
		return newObj, err
	}

	// 允许所有已配置的 transformers 更新新对象或返回错误
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
