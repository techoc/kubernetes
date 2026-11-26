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

// RESTCreateStrategy 定义创建一个遵循 Kubernetes API 约定对象所必需的最小校验、
// 可接受输入以及名称生成行为。
// 各资源的实现只需关注自身字段语义；命名空间、UID 等通用元数据由本包统一处理。
type RESTCreateStrategy interface {
	runtime.ObjectTyper
	// 当对象设置了 GenerateName 时使用；在校验之前调用。
	names.NameGenerator

	// 对象是否必须属于某个命名空间。
	NamespaceScoped() bool
	// 在创建校验前调用，用于归一化对象：如剔除无需持久化的字段、
	// 对顺序无关的列表排序等。但不得删除那些「出现即应判为校验错误」的字段。
	//
	// 通常做类型检查并初始化或清空 status。清空是因为状态变化属于内部行为，
	// API 的外部调用方不应在创建时设置初始状态。
	PrepareForCreate(ctx context.Context, obj runtime.Object)
	// 返回校验错误列表或 nil。在默认值填充后、对象落库前调用；
	// 实现不得修改对象。
	Validate(ctx context.Context, obj runtime.Object) field.ErrorList
	// 向发起创建的客户端返回警告。调用时机为默认值填充且 Validate 通过之后、
	// Canonicalize 与落库之前；实现不得修改对象。
	//
	// 警告应简短，尽量不超过 120 字符。不要加 "Warning:" 前缀（由客户端输出时添加）。
	// 针对具体字段的警告格式为 "path.to.field: message"。
	// 例如：`spec.imagePullSecrets[0].name: invalid empty name ""`
	//
	// 警告应描述调用方需要修正或知晓的问题，例如：
	// - 使用了未来版本将失效的弃用字段/标签/注解
	// - 使用了已无效果的过时字段/标签/注解
	// - 规格有误会影响处理，但出于兼容性考虑未被校验拒绝
	//
	// 调用方无法处理的字段不要告警，例如子资源创建请求中的 spec 字段。
	WarningsOnCreate(ctx context.Context, obj runtime.Object) []string
	// 把对象规范化为统一形式，便于下游代码依赖一致形态做比较等操作。
	// 在校验通过后、落库前调用；实现可以修改对象，通常只是类型检查或直接留空。
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

// BeforeCreate 负责在创建时执行所有资源的公共操作，只返回可转换为 api.Status 的错误。
// 它先调用 PrepareForCreate，再调用 Validate；若该对象应当被创建则返回 nil。
// 执行顺序不可随意调整：先确认系统字段与命名空间，再归一化、校验、告警、规范化，
// 任何一步失败都必须阻止对象落库。
func BeforeCreate(strategy RESTCreateStrategy, ctx context.Context, obj runtime.Object) error {
	objectMeta, kind, kerr := objectMetaAndKind(strategy, obj)
	if kerr != nil {
		return kerr
	}

	// 确认系统关键元数据已经填充完毕。
	// UID、创建时间等系统字段由存储层/处理器预填；缺失说明调用链有误，属于内部错误。
	if !metav1.HasObjectMetaSystemFieldValues(objectMeta) {
		return errors.NewInternalError(fmt.Errorf("system metadata was not initialized"))
	}

	// 确认名称已生成。
	if len(objectMeta.GetGenerateName()) > 0 && len(objectMeta.GetName()) == 0 {
		return errors.NewInternalError(fmt.Errorf("metadata.name was not generated"))
	}

	// 确认对象上的命名空间正确；若对象内设置了与之冲突的命名空间则报错。
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

// 执行创建操作时的通用校验与策略专属校验。
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
		// 接着执行对象元数据的通用校验。
		// 放在自定义校验之后执行，尽量优先展示更具体的错误信息。
		errs = append(errs, genericvalidation.ValidateObjectMetaAccessor(objectMeta, strategy.NamespaceScoped(), validatePathSegment, field.NewPath("metadata"))...)
	}

	// 声明式校验（基于 CEL 的标签）是可选的扩展；启用后与手写校验的错误合并返回。
	if dv, ok := strategy.(DeclarativeValidationStrategy); ok {
		errs = dv.ValidateDeclaratively(ctx, obj, nil, errs, operation.Create, dv.DeclarativeValidationConfig(ctx, obj, nil))
	}

	return errs
}

// CheckGeneratedNameError 检查创建资源时发生的错误是否由于无法生成有效名称所致。
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
		// 没有 generateName 时直接返回原始错误（AlreadyExists）：走到这里说明是用户自己选的名字冲突。
		return err
	}

	// 上下文中有组资源信息时取出它。
	gr := schema.GroupResource{}
	if requestInfo, found := genericapirequest.RequestInfoFrom(ctx); found {
		gr = schema.GroupResource{Group: gvk.Group, Resource: requestInfo.Resource}
	}

	// 如果对象同时具有 name 和 generateName，说明服务端挑出的名称已经存在。
	// 返回专门的 GenerateNameConflict，提示客户端重试即可，而非名称本身有问题。
	return errors.NewGenerateNameConflict(gr, objectMeta.GetName(), 1)
}

// objectMetaAndKind 从 runtime 对象中取出 kind 与 ObjectMeta，失败时返回错误。
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

// 能判断对象是否必须属于命名空间的策略接口。
type NamespaceScopedStrategy interface {
	// 对象是否必须属于某个命名空间。
	NamespaceScoped() bool
}

// AdmissionToValidateObjectFunc 把校验型准入转换为 rest 的对象校验函数。
// 用于在删除等“对象已从存储读出、尚未变更”的路径上重新执行准入校验。
// 若准入实现不支持 ValidationInterface，返回恒成功的函数，避免影响主流程。
func AdmissionToValidateObjectFunc(admit admission.Interface, staticAttributes admission.Attributes, o admission.ObjectInterfaces) ValidateObjectFunc {
	validatingAdmission, ok := admit.(admission.ValidationInterface)
	if !ok {
		return func(ctx context.Context, obj runtime.Object) error { return nil }
	}
	return func(ctx context.Context, obj runtime.Object) error {
		name := staticAttributes.GetName()
		// 应对名称由 generateName 生成的情况。
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
