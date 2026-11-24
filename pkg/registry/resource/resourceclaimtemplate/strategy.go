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

package resourceclaimtemplate

import (
	"context"
	"errors"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/api/resourceclaimspec"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/apis/resource/validation"
	resourceutils "k8s.io/kubernetes/pkg/registry/resource"
)

// resourceClaimTemplateStrategy 定义 ResourceClaimTemplate 的创建、更新和校验策略。
type resourceClaimTemplateStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
	nsClient v1.NamespaceInterface
}

// NewStrategy 创建 ResourceClaimTemplate 默认策略，并注入 Namespace 客户端以校验 AdminAccess 权限。
func NewStrategy(nsClient v1.NamespaceInterface) *resourceClaimTemplateStrategy {
	return &resourceClaimTemplateStrategy{
		rest.DeclarativeValidation{Scheme: legacyscheme.Scheme},
		names.SimpleNameGenerator,
		nsClient,
	}
}

// NamespaceScoped 返回 true，因为 ResourceClaimTemplate 属于命名空间。
func (*resourceClaimTemplateStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate 根据 Feature Gate 清理 ResourceClaimTemplate Spec 中不可用的新字段。
func (*resourceClaimTemplateStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	claimTemplate := obj.(*resource.ResourceClaimTemplate)
	dropDisabledFields(claimTemplate, nil)
}

// Validate 先检查请求是否具备 AdminAccess 权限，再执行 ResourceClaimTemplate 字段校验。
func (s *resourceClaimTemplateStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	resourceClaimTemplate := obj.(*resource.ResourceClaimTemplate)
	allErrs := resourceutils.AuthorizedForAdmin(ctx, resourceClaimTemplate.Spec.Spec.Devices.Requests, resourceClaimTemplate.Namespace, s.nsClient)
	return append(allErrs, validation.ValidateResourceClaimTemplate(resourceClaimTemplate)...)
}

// DeclarativeValidationConfig 使用与 ResourceClaim 相同的路径规范化规则，
// 将 v1beta1 展平的请求路径（例如 spec.spec.devices.requests[i].tolerations）
// 与手写校验中的 .exactly.* 路径正确配对，避免产生误报。
func (*resourceClaimTemplateStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{NormalizationRules: validation.ResourceNormalizationRules}
}

// WarningsOnCreate 返回创建 ResourceClaimTemplate 时的非阻断性警告；当前不产生额外警告。
func (*resourceClaimTemplateStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 ResourceClaimTemplate；当前不执行额外转换。
func (*resourceClaimTemplateStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，ResourceClaimTemplate 必须通过 POST 创建。
func (*resourceClaimTemplateStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 根据旧对象清理新对象中当前 Feature Gate 不允许新设置的字段。
func (*resourceClaimTemplateStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	claimTemplate, oldClaimTemplate := obj.(*resource.ResourceClaimTemplate), old.(*resource.ResourceClaimTemplate)
	dropDisabledFields(claimTemplate, oldClaimTemplate)
}

// ValidateUpdate 校验模板字段合法性，并执行 ResourceClaimTemplate 更新约束检查。
func (s *resourceClaimTemplateStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	// Spec 创建后不可变，因此更新时无需再次执行 AuthorizedForAdmin 检查。
	errorList := validation.ValidateResourceClaimTemplate(obj.(*resource.ResourceClaimTemplate))
	return append(errorList, validation.ValidateResourceClaimTemplateUpdate(obj.(*resource.ResourceClaimTemplate), old.(*resource.ResourceClaimTemplate))...)
}

// WarningsOnUpdate 返回更新 ResourceClaimTemplate 时的非阻断性警告；当前不产生额外警告。
func (*resourceClaimTemplateStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true，允许通用更新流程为缺少 ResourceVersion 的请求填入最新版本。
func (*resourceClaimTemplateStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// GetAttrs 提取 ResourceClaimTemplate 的标签和字段，供 List、Watch 选择器过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	template, ok := obj.(*resource.ResourceClaimTemplate)
	if !ok {
		return nil, nil, errors.New("not a resourceclaimtemplate")
	}
	return labels.Set(template.Labels), toSelectableFields(template), nil
}

// toSelectableFields 将 ResourceClaimTemplate 转换为可供字段选择器匹配的字段集合。
func toSelectableFields(template *resource.ResourceClaimTemplate) fields.Set {
	fields := generic.ObjectMetaFieldsSet(&template.ObjectMeta, true)
	return fields
}

func dropDisabledFields(newClaimTemplate, oldClaimTemplate *resource.ResourceClaimTemplate) {
	var oldClaimSpec *resource.ResourceClaimSpec
	if oldClaimTemplate != nil {
		oldClaimSpec = &oldClaimTemplate.Spec.Spec
	}
	resourceclaimspec.DropDisabledFields(&newClaimTemplate.Spec.Spec, oldClaimSpec)
}
