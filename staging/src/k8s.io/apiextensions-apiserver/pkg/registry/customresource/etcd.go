/*
Copyright 2017 The Kubernetes Authors.

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

// Package customresource 实现自定义资源的 REST Storage：
// 主资源、status 与 scale 子资源，以及 CRUD 和字段过滤。
package customresource

import (
	"context"
	"fmt"
	"strings"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// CustomResourceStorage 汇总自定义资源及其 status、scale 子资源的存储实现。
type CustomResourceStorage struct {
	CustomResource *REST
	Status         *StatusREST
	Scale          *ScaleREST
}

// NewStorage 构造自定义资源的 REST Storage。只有当 CRD 声明了对应子资源时，
// Status/Scale 才会被初始化，未声明的保持 nil。
func NewStorage(resource schema.GroupResource, singularResource schema.GroupResource, kind, listKind schema.GroupVersionKind, strategy customResourceStrategy, optsGetter generic.RESTOptionsGetter, categories []string, tableConvertor rest.TableConvertor, replicasPathMapping managedfields.ResourcePathMappings) (CustomResourceStorage, error) {
	var storage CustomResourceStorage
	store := &genericregistry.Store{
		NewFunc: func() runtime.Object {
			// 在新对象上设置期望的 group/version/kind，作为版本化解码器的信号。
			ret := &unstructured.Unstructured{}
			ret.SetGroupVersionKind(kind)
			return ret
		},
		NewListFunc: func() runtime.Object {
			// 列表不会被存储，只是动态生成，因此直接覆盖正确的 kind。
			ret := &unstructured.UnstructuredList{}
			ret.SetGroupVersionKind(listKind)
			return ret
		},
		PredicateFunc:             strategy.MatchCustomResourceDefinitionStorage,
		DefaultQualifiedResource:  resource,
		SingularQualifiedResource: singularResource,

		CreateStrategy:      strategy,
		UpdateStrategy:      strategy,
		DeleteStrategy:      strategy,
		ResetFieldsStrategy: strategy,

		TableConvertor: tableConvertor,
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter, AttrFunc: strategy.GetAttrs}
	if err := store.CompleteWithOptions(options); err != nil {
		return storage, fmt.Errorf("failed to update store with options: %w", err)
	}
	storage.CustomResource = &REST{store, categories}

	if strategy.status != nil {
		statusStore := *store
		statusStrategy := NewStatusStrategy(strategy)
		statusStore.UpdateStrategy = statusStrategy
		statusStore.ResetFieldsStrategy = statusStrategy
		storage.Status = &StatusREST{store: &statusStore}
	}

	if scale := strategy.scale; scale != nil {
		var labelSelectorPath string
		if scale.LabelSelectorPath != nil {
			labelSelectorPath = *scale.LabelSelectorPath
		}

		storage.Scale = &ScaleREST{
			store:               store,
			specReplicasPath:    scale.SpecReplicasPath,
			statusReplicasPath:  scale.StatusReplicasPath,
			labelSelectorPath:   labelSelectorPath,
			parentGV:            kind.GroupVersion(),
			replicasPathMapping: replicasPathMapping,
		}
	}

	return storage, nil
}

// REST 基于 etcd 实现自定义资源的 RESTStorage。
type REST struct {
	*genericregistry.Store
	categories []string
}

// 确保 REST 实现 CategoriesProvider。
var _ rest.CategoriesProvider = &REST{}

// Categories 实现 CategoriesProvider，返回资源所属的分类列表。
func (r *REST) Categories() []string {
	return r.categories
}

// StatusREST 实现修改自定义资源 status 的 REST 端点。
type StatusREST struct {
	store *genericregistry.Store
}

var _ = rest.Patcher(&StatusREST{})

// New 返回 status 子资源使用的空对象。
func (r *StatusREST) New() runtime.Object {
	return r.store.New()
}

// Get 从存储读取对象；Patch 需要先读取当前值。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	o, err := r.store.Get(ctx, name, options)
	if err != nil {
		return nil, err
	}
	return o, nil
}

// Update 只更新对象中 status 部分。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 这里显式把 forceAllowCreate 设为 false：子资源永远不允许在 update 时创建。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 实现 rest.ResetFieldsStrategy。
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

// ScaleREST 实现自定义资源的 scale 子资源。
type ScaleREST struct {
	store               *genericregistry.Store
	specReplicasPath    string
	statusReplicasPath  string
	labelSelectorPath   string
	parentGV            schema.GroupVersion
	replicasPathMapping managedfields.ResourcePathMappings
}

// 确保 ScaleREST 实现 Patcher 与 GroupVersionKindProvider。
var _ = rest.Patcher(&ScaleREST{})
var _ = rest.GroupVersionKindProvider(&ScaleREST{})

// GroupVersionKind 固定返回 autoscaling/v1 Scale；scale 子资源与宿主 CR 的 group 无关。
func (r *ScaleREST) GroupVersionKind(containingGV schema.GroupVersion) schema.GroupVersionKind {
	return autoscalingv1.SchemeGroupVersion.WithKind("Scale")
}

// New 返回新的 Scale 对象。
func (r *ScaleREST) New() runtime.Object {
	return &autoscalingv1.Scale{}
}

// Get 读取 CR 并转换为 autoscaling/v1 Scale。
// spec.replicas 字段不存在时返回内部错误，因为 scale 子资源依赖该路径。
func (r *ScaleREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	obj, err := r.store.Get(ctx, name, options)
	if err != nil {
		return nil, err
	}
	cr := obj.(*unstructured.Unstructured)

	scaleObject, replicasFound, err := scaleFromCustomResource(cr, r.specReplicasPath, r.statusReplicasPath, r.labelSelectorPath)
	if err != nil {
		return nil, err
	}
	if !replicasFound {
		return nil, apierrors.NewInternalError(fmt.Errorf("the spec replicas field %q does not exist", r.specReplicasPath))
	}
	return scaleObject, err
}

// Update 把对 Scale 的更新映射回 CR 的 spec.replicas，并返回更新后的 Scale。
func (r *ScaleREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	scaleObjInfo := &scaleUpdatedObjectInfo{
		reqObjInfo:          objInfo,
		specReplicasPath:    r.specReplicasPath,
		labelSelectorPath:   r.labelSelectorPath,
		statusReplicasPath:  r.statusReplicasPath,
		parentGV:            r.parentGV,
		replicasPathMapping: r.replicasPathMapping,
	}

	// 校验函数也需要看到 Scale 而不是 CR，因此先做转换再包一层。
	obj, _, err := r.store.Update(
		ctx,
		name,
		scaleObjInfo,
		toScaleCreateValidation(createValidation, r.specReplicasPath, r.statusReplicasPath, r.labelSelectorPath),
		toScaleUpdateValidation(updateValidation, r.specReplicasPath, r.statusReplicasPath, r.labelSelectorPath),
		false,
		options,
	)
	if err != nil {
		return nil, false, err
	}
	cr := obj.(*unstructured.Unstructured)

	newScale, _, err := scaleFromCustomResource(cr, r.specReplicasPath, r.statusReplicasPath, r.labelSelectorPath)
	if err != nil {
		return nil, false, apierrors.NewBadRequest(err.Error())
	}

	return newScale, false, err
}

// toScaleCreateValidation 包装创建校验：先把 CR 转成 Scale，再交给原校验函数。
func toScaleCreateValidation(f rest.ValidateObjectFunc, specReplicasPath, statusReplicasPath, labelSelectorPath string) rest.ValidateObjectFunc {
	return func(ctx context.Context, obj runtime.Object) error {
		scale, _, err := scaleFromCustomResource(obj.(*unstructured.Unstructured), specReplicasPath, statusReplicasPath, labelSelectorPath)
		if err != nil {
			return err
		}
		return f(ctx, scale)
	}
}

// toScaleUpdateValidation 包装更新校验：把新旧 CR 分别转成 Scale 后交给原校验函数。
func toScaleUpdateValidation(f rest.ValidateObjectUpdateFunc, specReplicasPath, statusReplicasPath, labelSelectorPath string) rest.ValidateObjectUpdateFunc {
	return func(ctx context.Context, obj, old runtime.Object) error {
		newScale, _, err := scaleFromCustomResource(obj.(*unstructured.Unstructured), specReplicasPath, statusReplicasPath, labelSelectorPath)
		if err != nil {
			return err
		}
		oldScale, _, err := scaleFromCustomResource(old.(*unstructured.Unstructured), specReplicasPath, statusReplicasPath, labelSelectorPath)
		if err != nil {
			return err
		}
		return f(ctx, newScale, oldScale)
	}
}

// 忽略开头的点，再按句点拆分路径。
func splitReplicasPath(replicasPath string) []string {
	return strings.Split(strings.TrimPrefix(replicasPath, "."), ".")
}

// scaleFromCustomResource 把 CR 转换成 Scale，并返回 specReplicas 是否存在的标志。
func scaleFromCustomResource(cr *unstructured.Unstructured, specReplicasPath, statusReplicasPath, labelSelectorPath string) (*autoscalingv1.Scale, bool, error) {
	specReplicas, foundSpecReplicas, err := unstructured.NestedInt64(cr.UnstructuredContent(), splitReplicasPath(specReplicasPath)...)
	if err != nil {
		return nil, false, err
	} else if !foundSpecReplicas {
		// spec.replicas 缺失按 0 处理，但调用方需要知道它原本不存在。
		specReplicas = 0
	}

	statusReplicas, found, err := unstructured.NestedInt64(cr.UnstructuredContent(), splitReplicasPath(statusReplicasPath)...)
	if err != nil {
		return nil, false, err
	} else if !found {
		// status.replicas 缺失是正常情况（尚未上报状态）。
		statusReplicas = 0
	}

	var labelSelector string
	if len(labelSelectorPath) > 0 {
		// labelSelectorPath 是可选项，只有配置了才读取。
		labelSelector, _, err = unstructured.NestedString(cr.UnstructuredContent(), splitReplicasPath(labelSelectorPath)...)
		if err != nil {
			return nil, false, err
		}
	}

	scale := &autoscalingv1.Scale{
		// 填充 apiVersion 和 kind，让转换逻辑识别出已经在目标 GVK，不再尝试转换。
		TypeMeta: metav1.TypeMeta{
			APIVersion: "autoscaling/v1",
			Kind:       "Scale",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              cr.GetName(),
			Namespace:         cr.GetNamespace(),
			UID:               cr.GetUID(),
			ResourceVersion:   cr.GetResourceVersion(),
			CreationTimestamp: cr.GetCreationTimestamp(),
		},
		Spec: autoscalingv1.ScaleSpec{
			Replicas: int32(specReplicas),
		},
		Status: autoscalingv1.ScaleStatus{
			Replicas: int32(statusReplicas),
			Selector: labelSelector,
		},
	}

	return scale, foundSpecReplicas, nil
}

// scaleUpdatedObjectInfo 把对 Scale 的更新转换为对宿主 CR 的更新。
type scaleUpdatedObjectInfo struct {
	reqObjInfo          rest.UpdatedObjectInfo
	specReplicasPath    string
	statusReplicasPath  string
	labelSelectorPath   string
	parentGV            schema.GroupVersion
	replicasPathMapping managedfields.ResourcePathMappings
}

// Preconditions 直接透传原始请求的 Preconditions。
func (i *scaleUpdatedObjectInfo) Preconditions() *metav1.Preconditions {
	return i.reqObjInfo.Preconditions()
}

// UpdatedObject 生成更新后的 CR：先把旧 CR 转成 Scale 交给请求方修改，
// 再把 Scale 的 replicas 写回 CR 的 spec 路径。
func (i *scaleUpdatedObjectInfo) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	cr := oldObj.DeepCopyObject().(*unstructured.Unstructured)
	const invalidSpecReplicas = -2147483648 // 最小的 int32

	// managedFields 需要在 Scale 与 CR 之间搬运字段路径。
	managedFieldsHandler := managedfields.NewScaleHandler(
		cr.GetManagedFields(),
		i.parentGV,
		i.replicasPathMapping,
	)

	oldScale, replicasFound, err := scaleFromCustomResource(cr, i.specReplicasPath, i.statusReplicasPath, i.labelSelectorPath)
	if err != nil {
		return nil, err
	}
	if !replicasFound {
		oldScale.Spec.Replicas = invalidSpecReplicas // 标记该字段此前未设置
	}

	scaleManagedFields, err := managedFieldsHandler.ToSubresource()
	if err != nil {
		return nil, err
	}
	oldScale.ManagedFields = scaleManagedFields

	obj, err := i.reqObjInfo.UpdatedObject(ctx, oldScale)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("nil update passed to Scale"))
	}

	scale, ok := obj.(*autoscalingv1.Scale)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("wrong object passed to Scale update: %v", obj))
	}

	// 更新后仍为哨兵值，说明请求方没有提供 spec.replicas。
	if scale.Spec.Replicas == invalidSpecReplicas {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("the spec replicas field %q cannot be empty", i.specReplicasPath))
	}

	if err := unstructured.SetNestedField(cr.Object, int64(scale.Spec.Replicas), splitReplicasPath(i.specReplicasPath)...); err != nil {
		return nil, err
	}
	if len(scale.ResourceVersion) != 0 {
		// 客户端提供了 resourceVersion 前置条件；设置它，让冲突错误原样返回给客户端。
		cr.SetResourceVersion(scale.ResourceVersion)
	}

	updatedEntries, err := managedFieldsHandler.ToParent(scale.ManagedFields)
	if err != nil {
		return nil, err
	}
	cr.SetManagedFields(updatedEntries)

	return cr, nil
}
