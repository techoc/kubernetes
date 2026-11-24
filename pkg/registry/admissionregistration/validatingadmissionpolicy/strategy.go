/*
Copyright 2022 The Kubernetes Authors.

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

package validatingadmissionpolicy

import (
	"context"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/admissionregistration"
	"k8s.io/kubernetes/pkg/apis/admissionregistration/validation"
	"k8s.io/kubernetes/pkg/registry/admissionregistration/resolver"
)

// validatingAdmissionPolicyStrategy 实现 ValidatingAdmissionPolicy 的写入策略与授权校验。
// validatingAdmissionPolicyStrategy 定义集群级策略写入边界，负责 CEL/匹配规则校验以及 ParamKind 的读取授权。
type validatingAdmissionPolicyStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
	authorizer       authorizer.UnconditionalAuthorizer
	resourceResolver resolver.ResourceResolver
}

// NewStrategy 创建用于 ValidatingAdmissionPolicy 创建和更新操作的策略，并注入授权器与资源解析器。
func NewStrategy(authorizer authorizer.UnconditionalAuthorizer, resourceResolver resolver.ResourceResolver) *validatingAdmissionPolicyStrategy {
	return &validatingAdmissionPolicyStrategy{
		DeclarativeValidation: rest.DeclarativeValidation{Scheme: legacyscheme.Scheme},
		NameGenerator:         names.SimpleNameGenerator,
		authorizer:            authorizer,
		resourceResolver:      resourceResolver,
	}
}

// NamespaceScoped 返回 false，表示 ValidatingAdmissionPolicy 是集群级资源。
func (v *validatingAdmissionPolicyStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 在创建时清空控制器维护的 Status，并将 Generation 初始化为 1。
// 创建时清空控制器维护的 Status，并将 Generation 初始化为 1。
func (v *validatingAdmissionPolicyStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	ic := obj.(*admissionregistration.ValidatingAdmissionPolicy)
	ic.Status = admissionregistration.ValidatingAdmissionPolicyStatus{}
	ic.Generation = 1
}

// PrepareForUpdate 保留服务端维护的 Status，并在 Spec 发生变化时递增 Generation。
// 主资源更新不允许客户端改写 Status；只有 Spec 语义变化才递增 Generation。
func (v *validatingAdmissionPolicyStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newIC := obj.(*admissionregistration.ValidatingAdmissionPolicy)
	oldIC := old.(*admissionregistration.ValidatingAdmissionPolicy)

	// 主资源更新不得修改由控制器维护的 Status。
	newIC.Status = oldIC.Status

	// Spec 发生变化时递增 Generation；控制器通过 Status 中的 observedGeneration 跟踪处理进度。
	// 控制器通过 Status 中的 observedGeneration 跟踪已处理的对象版本。
	// Generation 字段的语义详见 metav1.ObjectMeta 文档。
	if !apiequality.Semantic.DeepEqual(oldIC.Spec, newIC.Spec) {
		newIC.Generation = oldIC.Generation + 1
	}
}

// Validate 校验策略的 CEL 表达式、匹配约束、变量和参数类型，并在需要时检查参数资源读取权限。
// Validate 检查 CEL 表达式、MatchConstraints、Variables 与 ParamKind；结构合法后才检查参数资源 get 权限。
func (v *validatingAdmissionPolicyStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	ic := obj.(*admissionregistration.ValidatingAdmissionPolicy)
	errs := validation.ValidateValidatingAdmissionPolicy(ic)
	if utilfeature.DefaultFeatureGate.Enabled(features.ManifestBasedAdmissionControlConfig) {
		errs = append(errs, validation.ValidateStaticSuffix(ic.Name, field.NewPath("metadata", "name"))...)
	}
	if len(errs) == 0 {
		// 对象结构校验通过后，再检查 ParamKind 对应参数资源的读取权限。
		if err := v.authorizeCreate(ctx, obj); err != nil {
			errs = append(errs, field.Forbidden(field.NewPath("spec", "paramKind"), err.Error()))
		}
	}
	return errs
}

// WarningsOnCreate 返回创建 ValidatingAdmissionPolicy 时需要提示的静态配置告警。
func (v *validatingAdmissionPolicyStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	ic := obj.(*admissionregistration.ValidatingAdmissionPolicy)
	if !utilfeature.DefaultFeatureGate.Enabled(features.ManifestBasedAdmissionControlConfig) {
		return validation.WarningsForStaticSuffix(ic.Name)
	}
	return nil
}

// Canonicalize 在校验后规范化对象；当前策略无需额外处理。
func (v *validatingAdmissionPolicyStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，禁止通过 PUT 更新请求隐式创建策略。
func (v *validatingAdmissionPolicyStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 校验 ValidatingAdmissionPolicy 更新，并在 ParamKind 变化时重新检查参数资源读取权限。
// 更新时重新校验策略表达式，仅在 ParamKind 变化时重新执行参数资源读取授权。
func (v *validatingAdmissionPolicyStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newIC := obj.(*admissionregistration.ValidatingAdmissionPolicy)
	oldIC := old.(*admissionregistration.ValidatingAdmissionPolicy)
	errs := validation.ValidateValidatingAdmissionPolicyUpdate(newIC, oldIC)
	if len(errs) == 0 {
		// 对象结构校验通过后，再检查 ParamKind 对应参数资源的读取权限。
		if err := v.authorizeUpdate(ctx, obj, old); err != nil {
			errs = append(errs, field.Forbidden(field.NewPath("spec", "paramKind"), err.Error()))
		}
	}
	return errs
}

// WarningsOnUpdate 返回本次 ValidatingAdmissionPolicy 更新涉及的配置告警。
func (v *validatingAdmissionPolicyStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	newIC := obj.(*admissionregistration.ValidatingAdmissionPolicy)
	return validation.WarningsForStaticSuffix(newIC.Name)
}

// AllowUnconditionalUpdate 返回 false，要求更新基于匹配的资源版本，避免覆盖并发修改。
// 更新必须基于匹配的资源版本执行。
func (v *validatingAdmissionPolicyStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// GetResetFields 返回该策略会重置、因此客户端不能通过此接口修改的字段集合。
// 这些字段由服务端策略维护，不接受用户在当前接口中直接设置。
func (v *validatingAdmissionPolicyStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"admissionregistration.k8s.io/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"admissionregistration.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"admissionregistration.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

type validatingAdmissionPolicyStatusStrategy struct {
	*validatingAdmissionPolicyStrategy
}

// ValidateUpdate 校验 ValidatingAdmissionPolicy 的 Status 子资源更新。
func (s *validatingAdmissionPolicyStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateValidatingAdmissionPolicyStatusUpdate(obj.(*admissionregistration.ValidatingAdmissionPolicy), old.(*admissionregistration.ValidatingAdmissionPolicy))
}

// PrepareForUpdate 与主资源策略相反：Status 子资源允许更新状态，但必须保留原有 Spec。
// Status 子资源允许更新状态，但不允许修改 Spec。
// /status 只允许策略控制器更新状态与条件；Spec 和相关元数据必须从旧对象恢复。
func (s *validatingAdmissionPolicyStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newIC := obj.(*admissionregistration.ValidatingAdmissionPolicy)
	oldIC := old.(*admissionregistration.ValidatingAdmissionPolicy)

	// Status 子资源更新不得修改策略 Spec。
	newIC.Spec = oldIC.Spec

	metav1.ResetObjectMetaForStatus(&newIC.ObjectMeta, &oldIC.ObjectMeta)
	// Status 更新不改变 Generation。
}

// GetResetFields 返回该策略会重置、因此客户端不能通过此接口修改的字段集合。
// 这些字段由服务端策略维护，不接受用户在当前接口中直接设置。
func (s *validatingAdmissionPolicyStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"admissionregistration.k8s.io/v1alpha1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata"),
		),
		"admissionregistration.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata"),
		),
		"admissionregistration.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata"),
		),
	}
}

// NewStatusStrategy 基于主资源策略创建专用于 /status 子资源的写入策略。
func NewStatusStrategy(policyStrategy *validatingAdmissionPolicyStrategy) *validatingAdmissionPolicyStatusStrategy {
	return &validatingAdmissionPolicyStatusStrategy{validatingAdmissionPolicyStrategy: policyStrategy}
}
