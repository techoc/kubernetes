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

package mutatingadmissionpolicy

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
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

// mutatingAdmissionPolicyStrategy 实现 MutatingAdmissionPolicy 的写入校验和参数资源授权。
// mutatingAdmissionPolicyStrategy 定义 MutatingAdmissionPolicy 写入策略，负责 CEL 变更表达式、参数类型和读取授权。
type mutatingAdmissionPolicyStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
	authorizer       authorizer.UnconditionalAuthorizer
	resourceResolver resolver.ResourceResolver
}

// NewStrategy 创建 MutatingAdmissionPolicy 的默认写入策略，并注入授权器与资源解析器。
func NewStrategy(authorizer authorizer.UnconditionalAuthorizer, resourceResolver resolver.ResourceResolver) *mutatingAdmissionPolicyStrategy {
	return &mutatingAdmissionPolicyStrategy{
		DeclarativeValidation: rest.DeclarativeValidation{Scheme: legacyscheme.Scheme},
		NameGenerator:         names.SimpleNameGenerator,
		authorizer:            authorizer,
		resourceResolver:      resourceResolver,
	}
}

// NamespaceScoped 返回 false，表示 MutatingAdmissionPolicy 是集群级资源。
func (v *mutatingAdmissionPolicyStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 在创建时将 Generation 初始化为 1。
func (v *mutatingAdmissionPolicyStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	ic := obj.(*admissionregistration.MutatingAdmissionPolicy)
	ic.Generation = 1
}

// PrepareForUpdate 仅在策略 Spec 发生语义变化时递增 Generation。
func (v *mutatingAdmissionPolicyStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newIC := obj.(*admissionregistration.MutatingAdmissionPolicy)
	oldIC := old.(*admissionregistration.MutatingAdmissionPolicy)

	// Spec 发生语义变化时递增 Generation。
	// Generation 字段的语义详见 metav1.ObjectMeta 文档。
	if !apiequality.Semantic.DeepEqual(oldIC.Spec, newIC.Spec) {
		newIC.Generation = oldIC.Generation + 1
	}
}

// Validate 校验变更策略的匹配约束、变量、Mutation 表达式及参数类型，并检查参数资源读取权限。
// Validate 校验匹配约束、变量、Mutation、ReinvocationPolicy 和 ParamKind，结构合法后再检查参数资源 get 权限。
func (v *mutatingAdmissionPolicyStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	ic := obj.(*admissionregistration.MutatingAdmissionPolicy)
	errs := validation.ValidateMutatingAdmissionPolicy(ic)
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

// WarningsOnCreate 返回创建 MutatingAdmissionPolicy 时需要提示的静态配置告警。
func (v *mutatingAdmissionPolicyStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	ic := obj.(*admissionregistration.MutatingAdmissionPolicy)
	if !utilfeature.DefaultFeatureGate.Enabled(features.ManifestBasedAdmissionControlConfig) {
		return validation.WarningsForStaticSuffix(ic.Name)
	}
	return nil
}

// Canonicalize 在校验后规范化对象；当前策略无需额外处理。
func (v *mutatingAdmissionPolicyStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，禁止通过 PUT 更新请求隐式创建 MutatingAdmissionPolicy。
func (v *mutatingAdmissionPolicyStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 校验 MutatingAdmissionPolicy 更新，并在 ParamKind 变化时重新检查参数资源读取权限。
// 更新时重新校验 CEL 变更逻辑，并在 ParamKind 发生变化时重做参数读取授权。
func (v *mutatingAdmissionPolicyStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newIC := obj.(*admissionregistration.MutatingAdmissionPolicy)
	oldIC := old.(*admissionregistration.MutatingAdmissionPolicy)
	errs := validation.ValidateMutatingAdmissionPolicyUpdate(newIC, oldIC)
	if len(errs) == 0 {
		// 对象结构校验通过后，再检查 ParamKind 对应参数资源的读取权限。
		if err := v.authorizeUpdate(ctx, obj, old); err != nil {
			errs = append(errs, field.Forbidden(field.NewPath("spec", "paramKind"), err.Error()))
		}
	}
	return errs
}

// WarningsOnUpdate 返回本次 MutatingAdmissionPolicy 更新涉及的配置告警。
func (v *mutatingAdmissionPolicyStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	newIC := obj.(*admissionregistration.MutatingAdmissionPolicy)
	return validation.WarningsForStaticSuffix(newIC.Name)
}

// AllowUnconditionalUpdate 返回 false，要求更新基于匹配的资源版本。
// 更新必须基于匹配的资源版本执行。
func (v *mutatingAdmissionPolicyStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}
