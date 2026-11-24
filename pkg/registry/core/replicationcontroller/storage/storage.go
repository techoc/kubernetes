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

// ReplicationController 与 ReplicaSet 的 Storage/Scale 适配器有共通逻辑；修改时应同步检查 ReplicaSet 对应实现。

package storage

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	autoscalingv1 "k8s.io/kubernetes/pkg/apis/autoscaling/v1"
	"k8s.io/kubernetes/pkg/apis/autoscaling/validation"
	api "k8s.io/kubernetes/pkg/apis/core"
	extensionsv1beta1 "k8s.io/kubernetes/pkg/apis/extensions/v1beta1"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/core/replicationcontroller"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// ControllerStorage 聚合 ReplicationController 主资源、Status 和 Scale 三个 REST 入口。
type ControllerStorage struct {
	Controller *REST
	Status     *StatusREST
	Scale      *ScaleREST
}

// ReplicasPathMappings returns the mappings between each group version and a replicas path
// ReplicasPathMappings 告诉 ManagedFields：core/v1 Scale 子资源的 replicas 字段映射回父对象 spec.replicas。
func ReplicasPathMappings() managedfields.ResourcePathMappings {
	return replicasPathInReplicationController
}

// maps a group version to the replicas path in a deployment object
var replicasPathInReplicationController = managedfields.ResourcePathMappings{
	schema.GroupVersion{Group: "", Version: "v1"}.String(): fieldpath.MakePathOrDie("spec", "replicas"),
}

// NewStorage 创建主资源与 Status Store，并让 ScaleREST 复用同一底层 Store，保证三个入口访问同一对象。
func NewStorage(optsGetter generic.RESTOptionsGetter) (ControllerStorage, error) {
	controllerREST, statusREST, err := NewREST(optsGetter)
	if err != nil {
		return ControllerStorage{}, err
	}

	return ControllerStorage{
		Controller: controllerREST,
		Status:     statusREST,
		Scale:      &ScaleREST{store: controllerREST.Store},
	}, nil
}

type REST struct {
	*genericregistry.Store
}

// NewREST returns a RESTStorage object that will work against replication controllers.
// NewREST 配置 ReplicationController 的对象类型、字段筛选器和主资源策略，再复制 Store 并替换 StatusStrategy。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &api.ReplicationController{} },
		NewListFunc:               func() runtime.Object { return &api.ReplicationControllerList{} },
		PredicateFunc:             replicationcontroller.MatchController,
		DefaultQualifiedResource:  api.Resource("replicationcontrollers"),
		SingularQualifiedResource: api.Resource("replicationcontroller"),

		CreateStrategy:      replicationcontroller.Strategy,
		UpdateStrategy:      replicationcontroller.Strategy,
		DeleteStrategy:      replicationcontroller.Strategy,
		ResetFieldsStrategy: replicationcontroller.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter, AttrFunc: replicationcontroller.GetAttrs}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = replicationcontroller.StatusStrategy
	statusStore.ResetFieldsStrategy = replicationcontroller.StatusStrategy

	return &REST{store}, &StatusREST{store: &statusStore}, nil
}

// Implement ShortNamesProvider
var _ rest.ShortNamesProvider = &REST{}

// ShortNames implements the ShortNamesProvider interface. Returns a list of short names for a resource.
func (r *REST) ShortNames() []string {
	return []string{"rc"}
}

// Implement CategoriesProvider
var _ rest.CategoriesProvider = &REST{}

// Categories implements the CategoriesProvider interface. Returns a list of categories a resource is part of.
func (r *REST) Categories() []string {
	return []string{"all"}
}

// StatusREST implements the REST endpoint for changing the status of a replication controller
type StatusREST struct {
	store *genericregistry.Store
}

func (r *StatusREST) New() runtime.Object {
	return &api.ReplicationController{}
}

// Destroy cleans up resources on shutdown.
func (r *StatusREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Get retrieves the object from the storage. It is required to support Patch.
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update alters the status subset of an object.
// Status 子资源禁止 update-on-create；其更新由独立 StatusStrategy 限制只能回写控制器状态。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// We are explicitly setting forceAllowCreate to false in the call to the underlying storage because
	// subresources should never allow create on update.
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields implements rest.ResetFieldsStrategy
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

type ScaleREST struct {
	store *genericregistry.Store
}

// ScaleREST implements Patcher
var _ = rest.Patcher(&ScaleREST{})
var _ = rest.GroupVersionKindProvider(&ScaleREST{})

// Scale 的 GVK 随父资源请求版本变化：旧 extensions/v1beta1 请求沿用对应版本，其余走 autoscaling/v1。
func (r *ScaleREST) GroupVersionKind(containingGV schema.GroupVersion) schema.GroupVersionKind {
	switch containingGV {
	case extensionsv1beta1.SchemeGroupVersion:
		return extensionsv1beta1.SchemeGroupVersion.WithKind("Scale")
	default:
		return autoscalingv1.SchemeGroupVersion.WithKind("Scale")
	}
}

// New creates a new Scale object
func (r *ScaleREST) New() runtime.Object {
	return &autoscaling.Scale{}
}

// Destroy cleans up resources on shutdown.
func (r *ScaleREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Get 先读取父 ReplicationController，再把 Spec.Replicas、Status.Replicas 和 Selector 投影成标准 autoscaling.Scale。
func (r *ScaleREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	obj, err := r.store.Get(ctx, name, options)
	if err != nil {
		return nil, err
	}
	rc := obj.(*api.ReplicationController)
	return scaleFromRC(rc), nil
}

// Update 将 Scale 请求包装成父资源更新，先完成 Scale 专属校验，最终只把副本数和字段所有权写回 ReplicationController。
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
	rc := obj.(*api.ReplicationController)
	return scaleFromRC(rc), false, nil
}

func (r *ScaleREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

func toScaleCreateValidation(f rest.ValidateObjectFunc) rest.ValidateObjectFunc {
	return func(ctx context.Context, obj runtime.Object) error {
		return f(ctx, scaleFromRC(obj.(*api.ReplicationController)))
	}
}

func toScaleUpdateValidation(f rest.ValidateObjectUpdateFunc) rest.ValidateObjectUpdateFunc {
	return func(ctx context.Context, obj, old runtime.Object) error {
		return f(
			ctx,
			scaleFromRC(obj.(*api.ReplicationController)),
			scaleFromRC(old.(*api.ReplicationController)),
		)
	}
}

// scaleFromRC returns a scale subresource for a replication controller.
// scaleFromRC 将父对象元数据、副本期望值/当前值和 Selector 映射为 autoscaling.Scale。
func scaleFromRC(rc *api.ReplicationController) *autoscaling.Scale {
	return &autoscaling.Scale{
		ObjectMeta: metav1.ObjectMeta{
			Name:              rc.Name,
			Namespace:         rc.Namespace,
			UID:               rc.UID,
			ResourceVersion:   rc.ResourceVersion,
			CreationTimestamp: rc.CreationTimestamp,
		},
		Spec: autoscaling.ScaleSpec{
			Replicas: *rc.Spec.Replicas,
		},
		Status: autoscaling.ScaleStatus{
			Replicas: rc.Status.Replicas,
			Selector: labels.SelectorFromSet(rc.Spec.Selector).String(),
		},
	}
}

// scaleUpdatedObjectInfo 负责 ReplicationController → Scale → 更新后的 Scale → ReplicationController 双向适配，并同步 ManagedFields。
type scaleUpdatedObjectInfo struct {
	name           string
	reqObjInfo     rest.UpdatedObjectInfo
	scaleGVKMapper rest.GroupVersionKindProvider
}

func (i *scaleUpdatedObjectInfo) Preconditions() *metav1.Preconditions {
	return i.reqObjInfo.Preconditions()
}

// UpdatedObject 在副本数更新前先构造 Scale 视图、转换子资源 ManagedFields、执行 Scale 校验，再把变更合并回父对象。
func (i *scaleUpdatedObjectInfo) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	replicationcontroller, ok := oldObj.DeepCopyObject().(*api.ReplicationController)
	if !ok {
		return nil, errors.NewBadRequest(fmt.Sprintf("expected existing object type to be ReplicationController, got %T", replicationcontroller))
	}
	// if zero-value, the existing object does not exist
	if len(replicationcontroller.ResourceVersion) == 0 {
		return nil, errors.NewNotFound(api.Resource("replicationcontrollers/scale"), i.name)
	}

	groupVersion := schema.GroupVersion{Group: "", Version: "v1"}
	if requestInfo, found := genericapirequest.RequestInfoFrom(ctx); found {
		requestGroupVersion := schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}
		if _, ok := replicasPathInReplicationController[requestGroupVersion.String()]; ok {
			groupVersion = requestGroupVersion
		} else {
			klog.Fatalf("Unrecognized group/version in request info %q", requestGroupVersion.String())
		}
	}

	managedFieldsHandler := managedfields.NewScaleHandler(
		replicationcontroller.ManagedFields,
		groupVersion,
		replicasPathInReplicationController,
	)

	// replicationcontroller -> old scale
	oldScale := scaleFromRC(replicationcontroller)
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

	errs := validation.ValidateScaleUpdate(ctx, scale, oldScale, legacyscheme.Scheme, i.scaleGVKMapper)

	if len(errs) > 0 {
		return nil, errors.NewInvalid(autoscaling.Kind("Scale"), replicationcontroller.Name, errs)
	}

	// validate precondition if specified (resourceVersion matching is handled by storage)
	if len(scale.UID) > 0 && scale.UID != replicationcontroller.UID {
		return nil, errors.NewConflict(
			api.Resource("replicationcontrollers/scale"),
			replicationcontroller.Name,
			fmt.Errorf("Precondition failed: UID in precondition: %v, UID in object meta: %v", scale.UID, replicationcontroller.UID),
		)
	}

	// move replicas/resourceVersion fields to object and return
	replicationcontroller.Spec.Replicas = &scale.Spec.Replicas
	replicationcontroller.ResourceVersion = scale.ResourceVersion

	updatedEntries, err := managedFieldsHandler.ToParent(scale.ManagedFields)
	if err != nil {
		return nil, err
	}
	replicationcontroller.ManagedFields = updatedEntries

	return replicationcontroller, nil
}
