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

package carp

import (
	"context"
	"errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/testapigroup"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// carpStrategy 定义测试用 Carp 资源的创建、更新和状态校验策略。
type carpStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
	nsClient v1.NamespaceInterface
}

// NewStrategy 创建 Carp 主资源使用的默认策略，并注入 Namespace 客户端。
func NewStrategy(nsClient v1.NamespaceInterface) *carpStrategy {
	return &carpStrategy{
		rest.DeclarativeValidation{Scheme: legacyscheme.Scheme},
		names.SimpleNameGenerator,
		nsClient,
	}
}

func (*carpStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回 Carp 主资源写入时需要重置的字段集合；创建和普通更新时均禁止客户端设置 Status。
func (*carpStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"testapigroup.apimachinery.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

func (*carpStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	claim := obj.(*testapigroup.Carp)
	// 创建请求不能设置 Status，状态字段由专用状态更新入口维护。
	claim.Status = testapigroup.CarpStatus{}
}

// Validate 校验新 Carp 对象；该测试 API 当前不附加业务校验规则。
func (s *carpStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return nil
}

// WarningsOnCreate 返回创建 Carp 时的非阻断性警告；当前不产生额外警告。
func (*carpStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 Carp；当前不执行额外转换。
func (*carpStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，Carp 必须通过 POST 创建。
func (*carpStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 恢复旧 Status，防止普通更新请求修改状态字段。
func (*carpStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newClaim := obj.(*testapigroup.Carp)
	oldClaim := old.(*testapigroup.Carp)
	newClaim.Status = oldClaim.Status
}

// ValidateUpdate 校验 Carp 更新；该测试 API 当前不附加业务校验规则。
func (s *carpStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return nil
}

// WarningsOnUpdate 返回更新 Carp 时的非阻断性警告；当前不产生额外警告。
func (*carpStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true，允许更新请求省略 ResourceVersion，由通用更新流程填入最新版本。
func (*carpStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type carpStatusStrategy struct {
	*carpStrategy
}

// NewStatusStrategy 创建 Carp /status 子资源使用的策略，并复用主资源策略。
func NewStatusStrategy(carpStrategy *carpStrategy) *carpStatusStrategy {
	return &carpStatusStrategy{carpStrategy}
}

// GetResetFields 返回 /status 子资源需要重置的 Spec 字段集合，确保状态更新不能修改资源配置。
func (*carpStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"testapigroup.apimachinery.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 在状态更新时恢复旧 Spec 和受保护的元数据字段。
func (*carpStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newClaim := obj.(*testapigroup.Carp)
	oldClaim := old.(*testapigroup.Carp)
	newClaim.Spec = oldClaim.Spec
	metav1.ResetObjectMetaForStatus(&newClaim.ObjectMeta, &oldClaim.ObjectMeta)
}

// ValidateUpdate 校验 Carp Status 更新；该测试 API 当前不附加业务校验规则。
func (r *carpStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return nil
}

// WarningsOnUpdate 返回 Carp Status 更新时的非阻断性警告；当前不产生额外警告。
func (*carpStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// Match 根据标签选择器和字段选择器构造通用存储过滤条件。
func Match(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// GetAttrs 提取 Carp 对象的标签和可筛选字段，供 List、Watch 过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	claim, ok := obj.(*testapigroup.Carp)
	if !ok {
		return nil, nil, errors.New("not a carp")
	}
	return labels.Set(claim.Labels), toSelectableFields(claim), nil
}

// toSelectableFields 将 Carp 的 ObjectMeta 转换为可供字段选择器匹配的字段集合。
func toSelectableFields(claim *testapigroup.Carp) fields.Set {
	fields := generic.ObjectMetaFieldsSet(&claim.ObjectMeta, true)
	return fields
}
