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

package validatingadmissionpolicybinding

import (
	"context"

	"k8s.io/apiserver/pkg/registry/rest"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/admissionregistration"
	"k8s.io/kubernetes/pkg/apis/admissionregistration/validation"
	"k8s.io/kubernetes/pkg/registry/admissionregistration/resolver"
)

// validatingAdmissionPolicyBindingStrategy 实现 ValidatingAdmissionPolicyBinding 的写入校验和参数访问授权。
// validatingAdmissionPolicyBindingStrategy 定义集群级 PolicyBinding 写入策略，负责 Policy 关联、匹配资源和参数引用授权。
type validatingAdmissionPolicyBindingStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
	authorizer       authorizer.UnconditionalAuthorizer
	policyGetter     PolicyGetter
	resourceResolver resolver.ResourceResolver
}

var Strategy = validatingAdmissionPolicyBindingStrategy{
	DeclarativeValidation: rest.DeclarativeValidation{Scheme: legacyscheme.Scheme},
}

type PolicyGetter interface {
	// GetValidatingAdmissionPolicy 根据名称读取集群级 ValidatingAdmissionPolicy。
	// 该资源是集群级资源，因此查询不需要 Namespace 参数。
	GetValidatingAdmissionPolicy(ctx context.Context, name string) (*admissionregistration.ValidatingAdmissionPolicy, error)
}

// NewStrategy 创建 Binding 的默认写入策略，并注入授权器、Policy 查询器和资源解析器。
func NewStrategy(authorizer authorizer.UnconditionalAuthorizer, policyGetter PolicyGetter, resourceResolver resolver.ResourceResolver) *validatingAdmissionPolicyBindingStrategy {
	return &validatingAdmissionPolicyBindingStrategy{
		DeclarativeValidation: rest.DeclarativeValidation{Scheme: legacyscheme.Scheme},
		NameGenerator:         names.SimpleNameGenerator,
		authorizer:            authorizer,
		policyGetter:          policyGetter,
		resourceResolver:      resourceResolver,
	}
}

// NamespaceScoped 返回 false，表示 ValidatingAdmissionPolicyBinding 是集群级资源。
func (v *validatingAdmissionPolicyBindingStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 在创建 Binding 时初始化 Generation。
// 创建时初始化 Generation；Binding 本身不维护由控制器回写的状态字段。
func (v *validatingAdmissionPolicyBindingStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	ic := obj.(*admissionregistration.ValidatingAdmissionPolicyBinding)
	ic.Generation = 1
}

// PrepareForUpdate 仅在 Binding 的 Spec 发生语义变化时递增 Generation。
func (v *validatingAdmissionPolicyBindingStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newIC := obj.(*admissionregistration.ValidatingAdmissionPolicyBinding)
	oldIC := old.(*admissionregistration.ValidatingAdmissionPolicyBinding)

	// Spec 发生语义变化时递增 Generation。
	// Generation 字段的语义详见 metav1.ObjectMeta 文档。
	if !apiequality.Semantic.DeepEqual(oldIC.Spec, newIC.Spec) {
		newIC.Generation = oldIC.Generation + 1
	}
}

// Validate 校验 Binding 的 Policy 引用、匹配规则和 ParamRef，并检查参数资源读取权限。
// Validate 校验 PolicyName、MatchResources 和 ParamRef，再确保策略存在且调用者能读取参数对象。
func (v *validatingAdmissionPolicyBindingStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	ic := obj.(*admissionregistration.ValidatingAdmissionPolicyBinding)
	errs := validation.ValidateValidatingAdmissionPolicyBinding(ic)
	if utilfeature.DefaultFeatureGate.Enabled(features.ManifestBasedAdmissionControlConfig) {
		errs = append(errs, validation.ValidateStaticSuffix(ic.Name, field.NewPath("metadata", "name"))...)
	}
	if len(errs) == 0 {
		// 结构校验通过后，再对 ParamRef 指向的参数资源执行授权检查。
		if err := v.authorizeCreate(ctx, obj); err != nil {
			errs = append(errs, field.Forbidden(field.NewPath("spec", "paramRef"), err.Error()))
		}
	}
	return errs
}

// WarningsOnCreate 返回创建 ValidatingAdmissionPolicyBinding 时需要提示的静态配置告警。
func (v *validatingAdmissionPolicyBindingStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	ic := obj.(*admissionregistration.ValidatingAdmissionPolicyBinding)
	if !utilfeature.DefaultFeatureGate.Enabled(features.ManifestBasedAdmissionControlConfig) {
		return validation.WarningsForStaticSuffix(ic.Name)
	}
	return nil
}

// Canonicalize 在校验后规范化对象；当前策略无需额外处理。
func (v *validatingAdmissionPolicyBindingStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，禁止通过 PUT 更新请求隐式创建 Binding。
func (v *validatingAdmissionPolicyBindingStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 校验 ValidatingAdmissionPolicyBinding 更新，并在 ParamRef 变化时重新执行授权检查。
// 更新时重新校验 ParamRef；若参数目标或关联 Policy 改变，必须重新执行授权检查。
func (v *validatingAdmissionPolicyBindingStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newIC := obj.(*admissionregistration.ValidatingAdmissionPolicyBinding)
	oldIC := old.(*admissionregistration.ValidatingAdmissionPolicyBinding)
	errs := validation.ValidateValidatingAdmissionPolicyBindingUpdate(newIC, oldIC)
	if len(errs) == 0 {
		// 结构校验通过后，再对 ParamRef 指向的参数资源执行授权检查。
		if err := v.authorizeUpdate(ctx, obj, old); err != nil {
			errs = append(errs, field.Forbidden(field.NewPath("spec", "paramRef"), err.Error()))
		}
	}
	return errs
}

// WarningsOnUpdate 返回本次 ValidatingAdmissionPolicyBinding 更新涉及的配置告警。
func (v *validatingAdmissionPolicyBindingStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	newIC := obj.(*admissionregistration.ValidatingAdmissionPolicyBinding)
	return validation.WarningsForStaticSuffix(newIC.Name)
}

// AllowUnconditionalUpdate 返回 false，要求更新携带匹配的资源版本。
// 更新必须基于匹配的资源版本执行。
func (v *validatingAdmissionPolicyBindingStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}
