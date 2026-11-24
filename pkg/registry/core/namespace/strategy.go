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

// Strategy is the default logic that applies when creating and updating Namespace
// objects via the REST API.
var Strategy = namespaceStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped is false for namespaces.
func (namespaceStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields returns the set of fields that get reset by the strategy
// and should not be modified by the user.
func (namespaceStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}
}

// PrepareForCreate clears fields that are not allowed to be set by end users on creation.
// 创建时强制 Phase=Active，并确保 kubernetes Finalizer 存在；它负责触发命名空间内容清理完成前的保护。
func (namespaceStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	// on create, status is active
	namespace := obj.(*api.Namespace)
	namespace.Status = api.NamespaceStatus{
		Phase: api.NamespaceActive,
	}
	// on create, we require the kubernetes value
	// we cannot use this in defaults conversion because we let it get removed over life of object
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

// PrepareForUpdate clears fields that are not allowed to be set by end users on update.
// 普通 Namespace 更新不允许客户端直接修改 Spec.Finalizers 或 Status，这两类字段分别由 finalize/status 子资源管理。
func (namespaceStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newNamespace := obj.(*api.Namespace)
	oldNamespace := old.(*api.Namespace)
	newNamespace.Spec.Finalizers = oldNamespace.Spec.Finalizers
	newNamespace.Status = oldNamespace.Status
}

// Validate validates a new namespace.
func (namespaceStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	namespace := obj.(*api.Namespace)
	return validation.ValidateNamespace(namespace)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (namespaceStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize normalizes the object after validation.
// Canonicalize 保证 kubernetes.io/metadata.name 标签始终与最终 Namespace 名称一致，尤其用于 GenerateName 创建路径。
func (namespaceStrategy) Canonicalize(obj runtime.Object) {
	// Ensure the label matches the name for namespaces just created using GenerateName,
	// where the final name wasn't available for defaulting to make this change.
	// This code needs to be kept in sync with the implementation that exists
	// in Namespace defaulting (pkg/apis/core/v1)
	// Why this hook *and* defaults.go?
	//
	// CREATE:
	// Defaulting and PrepareForCreate happen before generateName is completed
	// (i.e. the name is not yet known). Validation happens after generateName
	// but should not modify objects. Canonicalize happens later, which makes
	// it the best hook for setting the label.
	//
	// UPDATE:
	// Defaulting and Canonicalize will both trigger with the full name.
	//
	// GET:
	// Only defaulting will be applied.
	ns := obj.(*api.Namespace)
	if ns.Labels == nil {
		ns.Labels = map[string]string{}
	}
	ns.Labels[v1.LabelMetadataName] = ns.Name
}

// AllowCreateOnUpdate is false for namespaces.
func (namespaceStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate is the default update validation for an end user.
func (namespaceStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	errorList := validation.ValidateNamespace(obj.(*api.Namespace))
	return append(errorList, validation.ValidateNamespaceUpdate(obj.(*api.Namespace), old.(*api.Namespace))...)
}

// WarningsOnUpdate returns warnings for the given update.
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

// GetResetFields returns the set of fields that get reset by the strategy
// and should not be modified by the user.
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

// WarningsOnUpdate returns warnings for the given update.
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

// WarningsOnUpdate returns warnings for the given update.
func (namespaceFinalizeStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// GetResetFields returns the set of fields that get reset by the strategy
// and should not be modified by the user.
func (namespaceFinalizeStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}
}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update.
// /finalize 只允许改变 Spec.Finalizers；Status 必须沿用旧值，避免终结流程绕过 Namespace 生命周期状态机。
func (namespaceFinalizeStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newNamespace := obj.(*api.Namespace)
	oldNamespace := old.(*api.Namespace)
	newNamespace.Status = oldNamespace.Status
}

// GetAttrs returns labels and fields of a given object for filtering purposes.
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	namespaceObj, ok := obj.(*api.Namespace)
	if !ok {
		return nil, nil, fmt.Errorf("not a namespace")
	}
	return labels.Set(namespaceObj.Labels), NamespaceToSelectableFields(namespaceObj), nil
}

// MatchNamespace returns a generic matcher for a given label and field selector.
func MatchNamespace(label labels.Selector, field fields.Selector) apistorage.SelectionPredicate {
	return apistorage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// NamespaceToSelectableFields returns a field set that represents the object
// NamespaceToSelectableFields 除标准 metadata 外，提供 status.phase 和历史兼容字段 name 供 FieldSelector 使用。
func NamespaceToSelectableFields(namespace *api.Namespace) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&namespace.ObjectMeta, false)
	specificFieldsSet := fields.Set{
		"status.phase": string(namespace.Status.Phase),
		// This is a bug, but we need to support it for backward compatibility.
		"name": namespace.Name,
	}
	return generic.MergeFieldsSets(objectMetaFieldsSet, specificFieldsSet)
}
