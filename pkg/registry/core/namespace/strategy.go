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

package namespace

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	apistorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// namespaceStrategy 定义集群级 Namespace 主资源策略，负责初始 Phase、系统 Finalizer、名称标签及用户可写字段边界。
type namespaceStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建和更新 Namespace 时使用的默认策略。
var Strategy = namespaceStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false：Namespace 是集群级资源，不隶属于任何命名空间。
func (namespaceStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 返回主资源写入时由策略重置、用户不能直接修改的字段集合。
func (namespaceStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}
}

// PrepareForCreate 清理创建请求中不允许用户设置的字段。
// 创建时强制 Phase=Active，并确保 kubernetes Finalizer 存在；它负责触发命名空间内容清理完成前的保护。
func (namespaceStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	// 新建 Namespace 的初始 Phase 为 Active。
	namespace := obj.(*api.Namespace)
	namespace.Status = api.NamespaceStatus{
		Phase: api.NamespaceActive,
	}
	// 创建时必须包含 kubernetes Finalizer。不能仅依赖默认值转换补充，因为对象生命周期中允许该 Finalizer 被移除。
	hasKubeFinalizer := false
	for i := range namespace.Spec.Finalizers {
		if namespace.Spec.Finalizers[i] == api.FinalizerKubernetes {
			hasKubeFinalizer = true
			break
		}
	}
	if !hasKubeFinalizer {
		if len(namespace.Spec.Finalizers) == 0 {
			namespace.Spec.Finalizers = []api.FinalizerName{api.FinalizerKubernetes}
		} else {
			namespace.Spec.Finalizers = append(namespace.Spec.Finalizers, api.FinalizerKubernetes)
		}
	}
}

// PrepareForUpdate 清理普通更新请求中不允许用户修改的字段。
// 普通 Namespace 更新不允许客户端直接修改 Spec.Finalizers 或 Status，这两类字段分别由 finalize/status 子资源管理。
func (namespaceStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newNamespace := obj.(*api.Namespace)
	oldNamespace := old.(*api.Namespace)
	newNamespace.Spec.Finalizers = oldNamespace.Spec.Finalizers
	newNamespace.Status = oldNamespace.Status
}

// Validate 校验新创建的 Namespace。
func (namespaceStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	namespace := obj.(*api.Namespace)
	return validation.ValidateNamespace(namespace)
}

// WarningsOnCreate 返回创建 Namespace 时的非阻断性警告；当前不产生额外警告。
func (namespaceStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 Namespace 对象。
// Canonicalize 保证 kubernetes.io/metadata.name 标签始终与最终 Namespace 名称一致，尤其用于 GenerateName 创建路径。
func (namespaceStrategy) Canonicalize(obj runtime.Object) {
	// 对于通过 GenerateName 创建的 Namespace，默认值处理时最终名称尚未生成，因此要在此处确保标签值与最终名称一致。
	// 本逻辑必须与 Namespace 默认值实现（pkg/apis/core/v1）保持同步。
	// 为什么默认值处理和 Canonicalize 两处都需要处理？
	// 创建：默认值处理和 PrepareForCreate 发生在 generateName 展开之前，此时名称尚未确定；校验阶段不应修改对象；
	// Canonicalize 执行得更晚，适合在最终名称生成后设置标签。
	// 更新：默认值处理和 Canonicalize 执行时都能获取完整名称。
	// 读取：只执行默认值处理。
	ns := obj.(*api.Namespace)
	if ns.Labels == nil {
		ns.Labels = map[string]string{}
	}
	ns.Labels[v1.LabelMetadataName] = ns.Name
}

// AllowCreateOnUpdate 返回 false：Namespace 不能通过更新请求隐式创建。
func (namespaceStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 执行面向普通用户的 Namespace 更新校验。
func (namespaceStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	errorList := validation.ValidateNamespace(obj.(*api.Namespace))
	return append(errorList, validation.ValidateNamespaceUpdate(obj.(*api.Namespace), old.(*api.Namespace))...)
}

// WarningsOnUpdate 返回更新 Namespace 时的非阻断性警告；当前不产生额外警告。
func (namespaceStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (namespaceStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type namespaceStatusStrategy struct {
	namespaceStrategy
}

var StatusStrategy = namespaceStatusStrategy{Strategy}

// GetResetFields 返回子资源写入时由策略重置、用户不能直接修改的字段集合。
func (namespaceStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}

// /status 只能更新 Namespace.Status；Spec 从旧对象恢复，防止通过状态子资源移除保护性 Finalizer。
func (namespaceStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newNamespace := obj.(*api.Namespace)
	oldNamespace := old.(*api.Namespace)
	newNamespace.Spec = oldNamespace.Spec
}

func (namespaceStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateNamespaceStatusUpdate(obj.(*api.Namespace), old.(*api.Namespace))
}

// WarningsOnUpdate 返回对应更新操作的非阻断性警告；当前不产生额外警告。
func (namespaceStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

type namespaceFinalizeStrategy struct {
	namespaceStrategy
}

var FinalizeStrategy = namespaceFinalizeStrategy{Strategy}

// /finalize 使用专门校验器检查 Finalizer 的增删规则，允许调用方在命名空间内容清理完毕后移除剩余 Finalizer。
func (namespaceFinalizeStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateNamespaceFinalizeUpdate(obj.(*api.Namespace), old.(*api.Namespace))
}

// WarningsOnUpdate 返回对应更新操作的非阻断性警告；当前不产生额外警告。
func (namespaceFinalizeStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// GetResetFields 返回子资源写入时由策略重置、用户不能直接修改的字段集合。
func (namespaceFinalizeStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}
}

// PrepareForUpdate 清理该子资源更新请求中不允许用户修改的字段。
// /finalize 只允许改变 Spec.Finalizers；Status 必须沿用旧值，避免终结流程绕过 Namespace 生命周期状态机。
func (namespaceFinalizeStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newNamespace := obj.(*api.Namespace)
	oldNamespace := old.(*api.Namespace)
	newNamespace.Status = oldNamespace.Status
}

// GetAttrs 提取 Namespace 的 Labels 和可筛选字段，供 LIST/WATCH 过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	namespaceObj, ok := obj.(*api.Namespace)
	if !ok {
		return nil, nil, fmt.Errorf("not a namespace")
	}
	return labels.Set(namespaceObj.Labels), NamespaceToSelectableFields(namespaceObj), nil
}

// MatchNamespace 根据 LabelSelector 和 FieldSelector 创建 Namespace 存储层选择谓词。
func MatchNamespace(label labels.Selector, field fields.Selector) apistorage.SelectionPredicate {
	return apistorage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// NamespaceToSelectableFields 返回 Namespace 支持 FieldSelector 查询的字段集合。
// NamespaceToSelectableFields 除标准 metadata 外，提供 status.phase 和历史兼容字段 name 供 FieldSelector 使用。
func NamespaceToSelectableFields(namespace *api.Namespace) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&namespace.ObjectMeta, false)
	specificFieldsSet := fields.Set{
		"status.phase": string(namespace.Status.Phase),
		// name 并非规范的可选字段，但为保持向后兼容，必须继续支持。
		"name": namespace.Name,
	}
	return generic.MergeFieldsSets(objectMetaFieldsSet, specificFieldsSet)
}
