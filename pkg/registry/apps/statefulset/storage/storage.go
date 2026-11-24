/*
Copyright 2015 The Kubernetes Authors.

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

package storage

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/apps"
	appsv1beta1 "k8s.io/kubernetes/pkg/apis/apps/v1beta1"
	appsv1beta2 "k8s.io/kubernetes/pkg/apis/apps/v1beta2"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	autoscalingv1 "k8s.io/kubernetes/pkg/apis/autoscaling/v1"
	autoscalingvalidation "k8s.io/kubernetes/pkg/apis/autoscaling/validation"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/apps/statefulset"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// StatefulSetStorage 聚合 StatefulSet 主资源、Status 和 Scale 子资源。
type StatefulSetStorage struct {
	StatefulSet *REST
	Status      *StatusREST
	Scale       *ScaleREST
}

// ReplicasPathMappings 返回各 API 版本中 replicas 字段对应的父资源路径映射。
// ReplicasPathMappings 声明 apps/v1 与历史版本中 replicas 字段的父资源路径，供 Scale ManagedFields 映射使用。
func ReplicasPathMappings() managedfields.ResourcePathMappings {
	return replicasPathInStatefulSet
}

// 将 API 组版本映射到 StatefulSet 对象中的 replicas 字段路径。
var replicasPathInStatefulSet = managedfields.ResourcePathMappings{
	schema.GroupVersion{Group: "apps", Version: "v1beta1"}.String(): fieldpath.MakePathOrDie("spec", "replicas"),
	schema.GroupVersion{Group: "apps", Version: "v1beta2"}.String(): fieldpath.MakePathOrDie("spec", "replicas"),
	schema.GroupVersion{Group: "apps", Version: "v1"}.String():      fieldpath.MakePathOrDie("spec", "replicas"),
}

// NewStorage 创建并返回 StatefulSetStorage 实例。
// NewStorage 将 StatefulSet 主资源、Status 和 Scale Handler 组合到同一个存储对象中。
func NewStorage(optsGetter generic.RESTOptionsGetter) (StatefulSetStorage, error) {
	statefulSetRest, statefulSetStatusRest, err := NewREST(optsGetter)
	if err != nil {
		return StatefulSetStorage{}, err
	}

	return StatefulSetStorage{
		StatefulSet: statefulSetRest,
		Status:      statefulSetStatusRest,
		Scale:       &ScaleREST{store: statefulSetRest.Store},
	}, nil
}

// REST 实现面向 etcd 中 StatefulSet 资源的 RESTStorage。
type REST struct {
	*genericregistry.Store
}

// NewREST returns a RESTStorage object that will work against statefulsets.
// NewREST 创建 StatefulSet 通用 Store，并为 Status 子资源复制 Store 后设置独立策略。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &apps.StatefulSet{} },
		NewListFunc:               func() runtime.Object { return &apps.StatefulSetList{} },
		DefaultQualifiedResource:  apps.Resource("statefulsets"),
		SingularQualifiedResource: apps.Resource("statefulset"),

		CreateStrategy:      statefulset.Strategy,
		UpdateStrategy:      statefulset.Strategy,
		DeleteStrategy:      statefulset.Strategy,
		ResetFieldsStrategy: statefulset.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}

	// Status 子资源共享底层 Storage，但只使用 StatusStrategy 接受状态字段写入。
	statusStore := *store
	statusStore.UpdateStrategy = statefulset.StatusStrategy
	statusStore.ResetFieldsStrategy = statefulset.StatusStrategy
	return &REST{store}, &StatusREST{store: &statusStore}, nil
}

// 实现 CategoriesProvider 接口。
var _ rest.CategoriesProvider = &REST{}

// Categories 实现 CategoriesProvider，返回该资源所属的类别列表。
func (r *REST) Categories() []string {
	return []string{"all"}
}

// StatusREST 实现用于修改 StatefulSet 状态的 REST 子资源端点。
type StatusREST struct {
	store *genericregistry.Store
}

// New 返回一个空的 StatefulSet 对象。
func (r *StatusREST) New() runtime.Object {
	return &apps.StatefulSet{}
}

// Destroy 在 API Server 关闭时清理该 REST 存储所拥有的资源。
func (r *StatusREST) Destroy() {
	// 底层 Store 与主资源 REST 共享，因此不能在此处重复销毁。
}

// Get 从底层存储读取对象；REST 子资源需要实现该方法以支持 Patch。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 更新对象的 Status 子集，不允许通过该入口修改其他字段。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 调用底层 Store 时显式将 forceAllowCreate 设为 false，因为子资源绝不能通过更新请求创建对象。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 实现 rest.ResetFieldsStrategy，返回该子资源需要重置的字段集合。
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

// 实现 ShortNamesProvider 接口。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 实现 ShortNamesProvider，返回该资源支持的简称列表。
func (r *REST) ShortNames() []string {
	return []string{"sts"}
}

// ScaleREST 为 Deployment 实现标准 Scale 子资源。
type ScaleREST struct {
	store *genericregistry.Store
}

// 确认 ScaleREST 实现 Patcher 接口。
var _ = rest.Patcher(&ScaleREST{})
var _ = rest.GroupVersionKindProvider(&ScaleREST{})

// GroupVersionKind 根据父资源 API 版本返回 StatefulSet Scale 对象的 GVK。
func (r *ScaleREST) GroupVersionKind(containingGV schema.GroupVersion) schema.GroupVersionKind {
	switch containingGV {
	case appsv1beta1.SchemeGroupVersion:
		return appsv1beta1.SchemeGroupVersion.WithKind("Scale")
	case appsv1beta2.SchemeGroupVersion:
		return appsv1beta2.SchemeGroupVersion.WithKind("Scale")
	default:
		return autoscalingv1.SchemeGroupVersion.WithKind("Scale")
	}
}

// New 创建一个空的 Scale 对象。
func (r *ScaleREST) New() runtime.Object {
	return &autoscaling.Scale{}
}

// Destroy cleans up resources on shutdown.
func (r *ScaleREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Get 从父资源存储读取对象，并转换为 Scale 子资源表示。
// Get 将 StatefulSet 当前副本期望值、运行值和 Selector 投影为标准 Scale 对象。
func (r *ScaleREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	obj, err := r.store.Get(ctx, name, options)
	if err != nil {
		return nil, err
	}
	ss := obj.(*apps.StatefulSet)
	scale, err := scaleFromStatefulSet(ss)
	if err != nil {
		return nil, errors.NewBadRequest(fmt.Sprintf("%v", err))
	}
	return scale, err
}

// Update 更新 StatefulSet 的 Scale 子集，并将变更写回父资源。
// Update 将客户端 Scale 更新转换为 StatefulSet 父资源更新，最终只改变 Spec.Replicas 及对应 ManagedFields。
func (r *ScaleREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	obj, _, err := r.store.Update(
		ctx,
		name,
		&scaleUpdatedObjectInfo{name, objInfo, r},
		toScaleCreateValidation(createValidation),
		toScaleUpdateValidation(updateValidation),
		false,
		options,
	)
	if err != nil {
		return nil, false, err
	}
	ss := obj.(*apps.StatefulSet)
	newScale, err := scaleFromStatefulSet(ss)
	if err != nil {
		return nil, false, errors.NewBadRequest(fmt.Sprintf("%v", err))
	}
	return newScale, false, err
}

func (r *ScaleREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

func toScaleCreateValidation(f rest.ValidateObjectFunc) rest.ValidateObjectFunc {
	return func(ctx context.Context, obj runtime.Object) error {
		scale, err := scaleFromStatefulSet(obj.(*apps.StatefulSet))
		if err != nil {
			return err
		}
		return f(ctx, scale)
	}
}

func toScaleUpdateValidation(f rest.ValidateObjectUpdateFunc) rest.ValidateObjectUpdateFunc {
	return func(ctx context.Context, obj, old runtime.Object) error {
		newScale, err := scaleFromStatefulSet(obj.(*apps.StatefulSet))
		if err != nil {
			return err
		}
		oldScale, err := scaleFromStatefulSet(old.(*apps.StatefulSet))
		if err != nil {
			return err
		}
		return f(ctx, newScale, oldScale)
	}
}

// scaleFromStatefulSet 将 StatefulSet 转换为对应的 Scale 子资源对象。
// scaleFromStatefulSet 将 StatefulSet 的副本数、Selector 和元数据转为 autoscaling.Scale。
func scaleFromStatefulSet(ss *apps.StatefulSet) (*autoscaling.Scale, error) {
	selector, err := metav1.LabelSelectorAsSelector(ss.Spec.Selector)
	if err != nil {
		return nil, err
	}
	return &autoscaling.Scale{
		// TODO: Create a variant of ObjectMeta type that only contains the fields below.
		ObjectMeta: metav1.ObjectMeta{
			Name:              ss.Name,
			Namespace:         ss.Namespace,
			UID:               ss.UID,
			ResourceVersion:   ss.ResourceVersion,
			CreationTimestamp: ss.CreationTimestamp,
		},
		Spec: autoscaling.ScaleSpec{
			Replicas: ss.Spec.Replicas,
		},
		Status: autoscaling.ScaleStatus{
			Replicas: ss.Status.Replicas,
			Selector: selector.String(),
		},
	}, nil
}

// scaleUpdatedObjectInfo transforms existing statefulset -> existing scale -> new scale -> new statefulset
type scaleUpdatedObjectInfo struct {
	name           string
	reqObjInfo     rest.UpdatedObjectInfo
	scaleGVKMapper rest.GroupVersionKindProvider
}

func (i *scaleUpdatedObjectInfo) Preconditions() *metav1.Preconditions {
	return i.reqObjInfo.Preconditions()
}

// UpdatedObject 在 StatefulSet/Scale 两种对象视图之间转换，校验请求、处理 ManagedFields，再把副本数写回父对象。
func (i *scaleUpdatedObjectInfo) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	statefulset, ok := oldObj.DeepCopyObject().(*apps.StatefulSet)
	if !ok {
		return nil, errors.NewBadRequest(fmt.Sprintf("expected existing object type to be StatefulSet, got %T", statefulset))
	}
	// if zero-value, the existing object does not exist
	if len(statefulset.ResourceVersion) == 0 {
		return nil, errors.NewNotFound(apps.Resource("statefulsets/scale"), i.name)
	}

	groupVersion := schema.GroupVersion{Group: "apps", Version: "v1"}
	if requestInfo, found := genericapirequest.RequestInfoFrom(ctx); found {
		requestGroupVersion := schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}
		if _, ok := replicasPathInStatefulSet[requestGroupVersion.String()]; ok {
			groupVersion = requestGroupVersion
		} else {
			klog.Fatalf("Unrecognized group/version in request info %q", requestGroupVersion.String())
		}
	}

	managedFieldsHandler := managedfields.NewScaleHandler(
		statefulset.ManagedFields,
		groupVersion,
		replicasPathInStatefulSet,
	)

	// statefulset -> old scale
	oldScale, err := scaleFromStatefulSet(statefulset)
	if err != nil {
		return nil, err
	}
	scaleManagedFields, err := managedFieldsHandler.ToSubresource()
	if err != nil {
		return nil, err
	}
	oldScale.ManagedFields = scaleManagedFields

	// old scale -> new scale
	newScaleObj, err := i.reqObjInfo.UpdatedObject(ctx, oldScale)
	if err != nil {
		return nil, err
	}
	if newScaleObj == nil {
		return nil, errors.NewBadRequest("nil update passed to Scale")
	}
	scale, ok := newScaleObj.(*autoscaling.Scale)
	if !ok {
		return nil, errors.NewBadRequest(fmt.Sprintf("expected input object type to be Scale, but %T", newScaleObj))
	}

	// validate
	if errs := autoscalingvalidation.ValidateScaleUpdate(ctx, scale, oldScale, legacyscheme.Scheme, i.scaleGVKMapper); len(errs) > 0 {
		return nil, errors.NewInvalid(autoscaling.Kind("Scale"), statefulset.Name, errs)
	}

	// validate precondition if specified (resourceVersion matching is handled by storage)
	if len(scale.UID) > 0 && scale.UID != statefulset.UID {
		return nil, errors.NewConflict(
			apps.Resource("statefulsets/scale"),
			statefulset.Name,
			fmt.Errorf("Precondition failed: UID in precondition: %v, UID in object meta: %v", scale.UID, statefulset.UID),
		)
	}

	// move replicas/resourceVersion fields to object and return
	statefulset.Spec.Replicas = scale.Spec.Replicas
	statefulset.ResourceVersion = scale.ResourceVersion

	updatedEntries, err := managedFieldsHandler.ToParent(scale.ManagedFields)
	if err != nil {
		return nil, err
	}
	statefulset.ManagedFields = updatedEntries

	return statefulset, nil
}
