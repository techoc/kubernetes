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

// 本文件实现 poddisruptionbudget 的 REST Strategy，控制创建/更新字段处理、业务校验、告警和 Status 子资源边界。
package poddisruptionbudget

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/policy"
	"k8s.io/kubernetes/pkg/apis/policy/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// podDisruptionBudgetStrategy 定义 PodDisruptionBudget 的创建、更新和校验策略。
type podDisruptionBudgetStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 PodDisruptionBudget 对象时使用的默认策略。
var Strategy = podDisruptionBudgetStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 true，因为 PodDisruptionBudget 必须归属于某个命名空间。
func (podDisruptionBudgetStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 声明主资源写入时需要重置的 Status 字段，避免用户通过普通创建或更新接口修改控制器状态。
func (podDisruptionBudgetStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"policy/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"policy/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 在创建前清空客户端提交的 Status，将 Generation 初始化为 1，并处理旧版 Selector 兼容字段。
func (podDisruptionBudgetStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	podDisruptionBudget := obj.(*policy.PodDisruptionBudget)
	// 创建请求不能设置 Status；驱逐预算状态由 PDB 控制器维护。
	podDisruptionBudget.Status = policy.PodDisruptionBudgetStatus{}

	podDisruptionBudget.Generation = 1

	dropDisabledFields(podDisruptionBudget, nil)
}

// PrepareForUpdate 在普通更新时恢复旧 Status；Spec 变化时递增 Generation，并处理旧版 Selector 兼容字段。
func (podDisruptionBudgetStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPodDisruptionBudget := obj.(*policy.PodDisruptionBudget)
	oldPodDisruptionBudget := old.(*policy.PodDisruptionBudget)
	// 普通更新请求不能修改 Status，避免绕过 PDB 控制器的状态管理。
	newPodDisruptionBudget.Status = oldPodDisruptionBudget.Status

	// Spec 发生变化时递增 Generation；Status 变化不应改变 Generation。
	// 控制器可据此判断当前状态对应的 PDB 规则版本。
	if !apiequality.Semantic.DeepEqual(oldPodDisruptionBudget.Spec, newPodDisruptionBudget.Spec) {
		newPodDisruptionBudget.Generation = oldPodDisruptionBudget.Generation + 1
	}

	dropDisabledFields(newPodDisruptionBudget, oldPodDisruptionBudget)
}

// dropDisabledFields 规范化旧版 v1beta1 空选择器兼容标签：保留精确匹配全部/无对象语义的选择器，并从普通选择器中移除该保留标签。
func dropDisabledFields(pdb, oldPDB *policy.PodDisruptionBudget) {
	if oldPDB != nil && apiequality.Semantic.DeepEqual(oldPDB.Spec.Selector, pdb.Spec.Selector) {
		return
	}
	switch {
	case apiequality.Semantic.DeepEqual(pdb.Spec.Selector, policy.NonV1beta1MatchNoneSelector):
		// 对于用于表达匹配全部或匹配无对象语义的兼容选择器，保留其特殊标签，不做清理。
	case apiequality.Semantic.DeepEqual(pdb.Spec.Selector, policy.NonV1beta1MatchAllSelector):
		// 对于用于表达匹配全部或匹配无对象语义的兼容选择器，保留其特殊标签，不做清理。
	default:
		// 普通选择器需要移除仅用于旧版匹配全部或匹配无对象语义的特殊标签，避免它与用户自定义条件组合。
		policy.StripPDBV1beta1Label(pdb.Spec.Selector)
	}
}

// Validate 校验新 PDB 的选择器、MinAvailable、MaxUnavailable 及其他驱逐预算约束。
func (podDisruptionBudgetStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	podDisruptionBudget := obj.(*policy.PodDisruptionBudget)
	opts := validation.PodDisruptionBudgetValidationOptions{
		AllowInvalidLabelValueInSelector: false,
	}
	return validation.ValidatePodDisruptionBudget(podDisruptionBudget, opts)
}

// WarningsOnCreate 返回创建 PDB 时的非阻断性警告；当前不产生额外警告。
func (podDisruptionBudgetStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 PDB；当前无需执行额外转换。
func (podDisruptionBudgetStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，PDB 必须通过 POST 创建，不能由 PUT 更新请求隐式创建。
func (podDisruptionBudgetStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 根据旧对象是否包含历史无效标签值，选择兼容性校验选项后校验更新对象。
func (podDisruptionBudgetStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	opts := validation.PodDisruptionBudgetValidationOptions{
		AllowInvalidLabelValueInSelector: hasInvalidLabelValueInLabelSelector(old.(*policy.PodDisruptionBudget)),
	}
	return validation.ValidatePodDisruptionBudget(obj.(*policy.PodDisruptionBudget), opts)
}

// WarningsOnUpdate 返回更新 PDB 主资源时的非阻断性警告；当前不产生额外警告。
func (podDisruptionBudgetStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，更新请求必须携带 ResourceVersion 并与存储版本匹配，避免并发更新覆盖 PDB 状态。
func (podDisruptionBudgetStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

type podDisruptionBudgetStatusStrategy struct {
	podDisruptionBudgetStrategy
}

// StatusStrategy 定义通过 /status 子资源更新 PDB 状态时使用的默认策略。
var StatusStrategy = podDisruptionBudgetStatusStrategy{Strategy}

// GetResetFields 声明 /status 子资源写入时需要重置的 Spec 字段，避免状态更新请求修改驱逐预算规则。
func (podDisruptionBudgetStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"policy/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
		"policy/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 在 Status 更新时恢复旧 Spec，确保该子资源不能修改驱逐预算规则。
func (podDisruptionBudgetStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPodDisruptionBudget := obj.(*policy.PodDisruptionBudget)
	oldPodDisruptionBudget := old.(*policy.PodDisruptionBudget)
	// Status 更新请求不能修改 Spec，必须沿用旧对象中的驱逐预算配置。
	newPodDisruptionBudget.Spec = oldPodDisruptionBudget.Spec
}

// ValidateUpdate 校验 PDB Status 更新；校验规则会根据请求 API 版本决定是否执行。
func (podDisruptionBudgetStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	var apiVersion schema.GroupVersion
	if requestInfo, found := genericapirequest.RequestInfoFrom(ctx); found {
		apiVersion = schema.GroupVersion{
			Group:   requestInfo.APIGroup,
			Version: requestInfo.APIVersion,
		}
	}
	return validation.ValidatePodDisruptionBudgetStatusUpdate(obj.(*policy.PodDisruptionBudget).Status,
		old.(*policy.PodDisruptionBudget).Status, field.NewPath("status"), apiVersion)
}

// WarningsOnUpdate 返回更新 PDB Status 时的非阻断性警告；当前不产生额外警告。
func (podDisruptionBudgetStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// hasInvalidLabelValueInLabelSelector 检查旧 PDB 的 LabelSelector 是否包含历史无效 Label 值，以便更新时兼容已有对象。
func hasInvalidLabelValueInLabelSelector(pdb *policy.PodDisruptionBudget) bool {
	if pdb.Spec.Selector != nil {
		labelSelectorValidationOptions := metav1validation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: false}
		return len(metav1validation.ValidateLabelSelector(pdb.Spec.Selector, labelSelectorValidationOptions, nil)) > 0
	}
	return false
}
