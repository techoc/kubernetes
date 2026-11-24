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
	"net/http"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	storeerr "k8s.io/apiserver/pkg/storage/errors"
	"k8s.io/apiserver/pkg/util/dryrun"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/apps"
	appsv1beta1 "k8s.io/kubernetes/pkg/apis/apps/v1beta1"
	appsv1beta2 "k8s.io/kubernetes/pkg/apis/apps/v1beta2"
	appsvalidation "k8s.io/kubernetes/pkg/apis/apps/validation"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	autoscalingv1 "k8s.io/kubernetes/pkg/apis/autoscaling/v1"
	autoscalingvalidation "k8s.io/kubernetes/pkg/apis/autoscaling/validation"
	extensionsv1beta1 "k8s.io/kubernetes/pkg/apis/extensions/v1beta1"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/apps/deployment"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// DeploymentStorage 聚合 Deployment 主资源、Status、Scale 和 Rollback 四个 REST 入口。
type DeploymentStorage struct {
	Deployment *REST
	Status     *StatusREST
	Scale      *ScaleREST
	Rollback   *RollbackREST
}

// ReplicasPathMappings 返回各 API 版本中 replicas 字段对应的父资源路径映射。
// ReplicasPathMappings 声明不同 apps API 版本中副本字段在父资源里的路径，供 Scale ManagedFields 在父子资源间转换。
func ReplicasPathMappings() managedfields.ResourcePathMappings {
	return replicasPathInDeployment
}

// 将 API 组版本映射到 Deployment 对象中的 replicas 字段路径。
var replicasPathInDeployment = managedfields.ResourcePathMappings{
	schema.GroupVersion{Group: "apps", Version: "v1beta1"}.String(): fieldpath.MakePathOrDie("spec", "replicas"),
	schema.GroupVersion{Group: "apps", Version: "v1beta2"}.String(): fieldpath.MakePathOrDie("spec", "replicas"),
	schema.GroupVersion{Group: "apps", Version: "v1"}.String():      fieldpath.MakePathOrDie("spec", "replicas"),
}

// NewStorage 创建并返回 DeploymentStorage 实例。
// NewStorage 组合主资源、Status、Scale 和 Rollback；Scale 与 Rollback 复用 Deployment 的底层 Store。
func NewStorage(optsGetter generic.RESTOptionsGetter) (DeploymentStorage, error) {
	deploymentRest, deploymentStatusRest, deploymentRollbackRest, err := NewREST(optsGetter)
	if err != nil {
		return DeploymentStorage{}, err
	}

	return DeploymentStorage{
		Deployment: deploymentRest,
		Status:     deploymentStatusRest,
		Scale:      &ScaleREST{store: deploymentRest.Store},
		Rollback:   deploymentRollbackRest,
	}, nil
}

// REST 为 Deployment 实现 RESTStorage 接口。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建并返回用于访问 Deployment 资源的 RESTStorage。
// NewREST 装配通用 Store，并为 Status 子资源复制 Store 后替换 UpdateStrategy，限制它只能写 Status。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, *RollbackREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &apps.Deployment{} },
		NewListFunc:               func() runtime.Object { return &apps.DeploymentList{} },
		DefaultQualifiedResource:  apps.Resource("deployments"),
		SingularQualifiedResource: apps.Resource("deployment"),

		CreateStrategy:      deployment.Strategy,
		UpdateStrategy:      deployment.Strategy,
		DeleteStrategy:      deployment.Strategy,
		ResetFieldsStrategy: deployment.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = deployment.StatusStrategy
	statusStore.ResetFieldsStrategy = deployment.StatusStrategy
	return &REST{store}, &StatusREST{store: &statusStore}, &RollbackREST{store: store}, nil
}

// 实现 ShortNamesProvider 接口。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 实现 ShortNamesProvider，返回该资源支持的简称列表。
func (r *REST) ShortNames() []string {
	return []string{"deploy"}
}

// 实现 CategoriesProvider 接口。
var _ rest.CategoriesProvider = &REST{}

// Categories 实现 CategoriesProvider，返回该资源所属的类别列表。
func (r *REST) Categories() []string {
	return []string{"all"}
}

// StatusREST 实现用于修改 Deployment 状态的 REST 子资源端点。
type StatusREST struct {
	store *genericregistry.Store
}

// New 返回一个空的 Deployment 对象。
func (r *StatusREST) New() runtime.Object {
	return &apps.Deployment{}
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

// RollbackREST 实现用于触发 Deployment 回滚的 REST 子资源端点。
type RollbackREST struct {
	store *genericregistry.Store
}

// ProducesMIMETypes 返回指定 HTTP 方法（GET、POST、DELETE、PATCH）可响应的 MIME 类型；nil 表示使用默认处理。
func (r *RollbackREST) ProducesMIMETypes(verb string) []string {
	return nil
}

// ProducesObject 返回指定 HTTP 方法的响应对象类型。若返回值非 nil，会覆盖存储对象；这里只使用对象类型，忽略其具体值。
func (r *RollbackREST) ProducesObject(verb string) interface{} {
	return metav1.Status{}
}

var _ = rest.StorageMetadata(&RollbackREST{})

// New 创建一个 DeploymentRollback 请求对象。
func (r *RollbackREST) New() runtime.Object {
	return &apps.DeploymentRollback{}
}

// Destroy 在 API Server 关闭时清理该 REST 存储所拥有的资源。
func (r *RollbackREST) Destroy() {
	// 底层 Store 与主资源 REST 共享，因此不能在此处重复销毁。
}

var _ = rest.NamedCreater(&RollbackREST{})

// Create 校验回滚请求，并执行 Deployment 回滚操作。
// Create 校验 DeploymentRollback 请求后，通过更新 Deployment.Spec.RollbackTo 触发控制器执行回滚。
func (r *RollbackREST) Create(ctx context.Context, name string, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	rollback, ok := obj.(*apps.DeploymentRollback)
	if !ok {
		return nil, errors.NewBadRequest(fmt.Sprintf("not a DeploymentRollback: %#v", obj))
	}

	if errs := appsvalidation.ValidateDeploymentRollback(rollback); len(errs) != 0 {
		return nil, errors.NewInvalid(apps.Kind("DeploymentRollback"), rollback.Name, errs)
	}
	if name != rollback.Name {
		return nil, errors.NewBadRequest("name in URL does not match name in DeploymentRollback object")
	}

	if createValidation != nil {
		if err := createValidation(ctx, obj.DeepCopyObject()); err != nil {
			return nil, err
		}
	}

	// 将 DeploymentRollback 中的信息写入 Deployment，以触发回滚流程。
	err := r.rollbackDeployment(ctx, rollback.Name, &rollback.RollbackTo, rollback.UpdatedAnnotations, dryrun.IsDryRun(options.DryRun))
	if err != nil {
		return nil, err
	}
	return &metav1.Status{
		Status:  metav1.StatusSuccess,
		Message: fmt.Sprintf("rollback request for deployment %q succeeded", rollback.Name),
		Code:    http.StatusOK,
	}, nil
}

func (r *RollbackREST) rollbackDeployment(ctx context.Context, deploymentID string, config *apps.RollbackConfig, annotations map[string]string, dryRun bool) error {
	if _, err := r.setDeploymentRollback(ctx, deploymentID, config, annotations, dryRun); err != nil {
		err = storeerr.InterpretGetError(err, apps.Resource("deployments"), deploymentID)
		err = storeerr.InterpretUpdateError(err, apps.Resource("deployments"), deploymentID)
		if _, ok := err.(*errors.StatusError); !ok {
			err = errors.NewInternalError(err)
		}
		return err
	}
	return nil
}

// setDeploymentRollback 用 GuaranteedUpdate 基于最新 Deployment 合并回滚 Annotation 并写入 RollbackTo，避免并发更新覆盖。
func (r *RollbackREST) setDeploymentRollback(ctx context.Context, deploymentID string, config *apps.RollbackConfig, annotations map[string]string, dryRun bool) (*apps.Deployment, error) {
	dKey, err := r.store.KeyFunc(ctx, deploymentID)
	if err != nil {
		return nil, err
	}
	var finalDeployment *apps.Deployment
	err = r.store.Storage.GuaranteedUpdate(ctx, dKey, &apps.Deployment{}, false, nil, storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
		d, ok := obj.(*apps.Deployment)
		if !ok {
			return nil, fmt.Errorf("unexpected object: %#v", obj)
		}
		if d.Annotations == nil {
			d.Annotations = make(map[string]string)
		}
		for k, v := range annotations {
			d.Annotations[k] = v
		}
		d.Spec.RollbackTo = config
		finalDeployment = d
		return d, nil
	}), dryRun, nil)
	return finalDeployment, err
}

// ScaleREST 为 Deployment 实现标准 Scale 子资源。
type ScaleREST struct {
	store *genericregistry.Store
}

// 确认 ScaleREST 实现 Patcher 接口。
var _ = rest.Patcher(&ScaleREST{})
var _ = rest.GroupVersionKindProvider(&ScaleREST{})

// GroupVersionKind 根据父资源 API 版本返回 Deployment Scale 对象的 GVK。
// 根据调用方使用的 Deployment API 版本返回对应 Scale GVK，保持旧版本客户端的内容协商兼容。
func (r *ScaleREST) GroupVersionKind(containingGV schema.GroupVersion) schema.GroupVersionKind {
	switch containingGV {
	case extensionsv1beta1.SchemeGroupVersion:
		return extensionsv1beta1.SchemeGroupVersion.WithKind("Scale")
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

// Destroy 在 API Server 关闭时清理该 REST 存储所拥有的资源。
func (r *ScaleREST) Destroy() {
	// 由于底层 Store 与主资源 REST 共享，
	// 因此不在此处显式销毁。
}

// Get 从父资源存储读取对象，并转换为 Scale 子资源表示。
// Get 从 Deployment 读取副本期望值与状态值，再投影为 autoscaling.Scale 对象。
func (r *ScaleREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	obj, err := r.store.Get(ctx, name, options)
	if err != nil {
		return nil, err
	}
	deployment := obj.(*apps.Deployment)
	scale, err := scaleFromDeployment(deployment)
	if err != nil {
		return nil, errors.NewBadRequest(fmt.Sprintf("%v", err))
	}
	return scale, nil
}

// Update 更新 Deployment 的 Scale 子集，并将变更写回父资源。
// Update 将 Scale 子资源请求适配成 Deployment 更新，并强制禁止通过 scale 子资源创建对象。
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
	deployment := obj.(*apps.Deployment)
	newScale, err := scaleFromDeployment(deployment)
	if err != nil {
		return nil, false, errors.NewBadRequest(fmt.Sprintf("%v", err))
	}
	return newScale, false, nil
}

func (r *ScaleREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

func toScaleCreateValidation(f rest.ValidateObjectFunc) rest.ValidateObjectFunc {
	return func(ctx context.Context, obj runtime.Object) error {
		scale, err := scaleFromDeployment(obj.(*apps.Deployment))
		if err != nil {
			return err
		}
		return f(ctx, scale)
	}
}

func toScaleUpdateValidation(f rest.ValidateObjectUpdateFunc) rest.ValidateObjectUpdateFunc {
	return func(ctx context.Context, obj, old runtime.Object) error {
		newScale, err := scaleFromDeployment(obj.(*apps.Deployment))
		if err != nil {
			return err
		}
		oldScale, err := scaleFromDeployment(old.(*apps.Deployment))
		if err != nil {
			return err
		}
		return f(ctx, newScale, oldScale)
	}
}

// scaleFromDeployment 将 Deployment 转换为对应的 Scale 子资源对象。
// scaleFromDeployment 把 Deployment 的副本数、Selector 和元数据映射成 autoscaling.Scale 返回给客户端。
func scaleFromDeployment(deployment *apps.Deployment) (*autoscaling.Scale, error) {
	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, err
	}

	return &autoscaling.Scale{
		// TODO：创建一个仅包含下列字段的 ObjectMeta 变体类型，避免复制不需要的元数据字段。
		ObjectMeta: metav1.ObjectMeta{
			Name:              deployment.Name,
			Namespace:         deployment.Namespace,
			UID:               deployment.UID,
			ResourceVersion:   deployment.ResourceVersion,
			CreationTimestamp: deployment.CreationTimestamp,
		},
		Spec: autoscaling.ScaleSpec{
			Replicas: deployment.Spec.Replicas,
		},
		Status: autoscaling.ScaleStatus{
			Replicas: deployment.Status.Replicas,
			Selector: selector.String(),
		},
	}, nil
}

// scaleUpdatedObjectInfo 适配器负责 Deployment → Scale → 修改后的 Scale → Deployment 的转换，同时同步子资源 ManagedFields。
type scaleUpdatedObjectInfo struct {
	name           string
	reqObjInfo     rest.UpdatedObjectInfo
	scaleGVKMapper rest.GroupVersionKindProvider
}

func (i *scaleUpdatedObjectInfo) Preconditions() *metav1.Preconditions {
	return i.reqObjInfo.Preconditions()
}

// UpdatedObject 先从父对象提取 Scale 和子资源 ManagedFields，执行客户端变更/校验，再把 replicas 和字段所有权写回父对象。
func (i *scaleUpdatedObjectInfo) UpdatedObject(ctx context.Context, oldObj runtime.Object) (runtime.Object, error) {
	deployment, ok := oldObj.DeepCopyObject().(*apps.Deployment)
	if !ok {
		return nil, errors.NewBadRequest(fmt.Sprintf("expected existing object type to be Deployment, got %T", deployment))
	}
	// objInfo 返回零值时，表示目标对象当前不存在。
	if len(deployment.ResourceVersion) == 0 {
		return nil, errors.NewNotFound(apps.Resource("deployments/scale"), i.name)
	}

	groupVersion := schema.GroupVersion{Group: "apps", Version: "v1"}
	if requestInfo, found := genericapirequest.RequestInfoFrom(ctx); found {
		requestGroupVersion := schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}
		if _, ok := replicasPathInDeployment[requestGroupVersion.String()]; ok {
			groupVersion = requestGroupVersion
		} else {
			klog.Fatalf("Unrecognized group/version in request info %q", requestGroupVersion.String())
		}
	}

	managedFieldsHandler := managedfields.NewScaleHandler(
		deployment.ManagedFields,
		groupVersion,
		replicasPathInDeployment,
	)

	// 将旧 Deployment 转换为旧 Scale 表示。
	oldScale, err := scaleFromDeployment(deployment)
	if err != nil {
		return nil, err
	}
	scaleManagedFields, err := managedFieldsHandler.ToSubresource()
	if err != nil {
		return nil, err
	}
	oldScale.ManagedFields = scaleManagedFields

	// 基于旧 Scale 应用客户端修改，得到新 Scale。
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

	// 校验新旧 Scale 的变更是否合法。
	if errs := autoscalingvalidation.ValidateScaleUpdate(ctx, scale, oldScale, legacyscheme.Scheme, i.scaleGVKMapper); len(errs) > 0 {
		return nil, errors.NewInvalid(autoscaling.Kind("Scale"), deployment.Name, errs)
	}

	// 若请求指定了前置条件，则在此校验；resourceVersion 匹配由底层存储层负责。
	if len(scale.UID) > 0 && scale.UID != deployment.UID {
		return nil, errors.NewConflict(
			apps.Resource("deployments/scale"),
			deployment.Name,
			fmt.Errorf("Precondition failed: UID in precondition: %v, UID in object meta: %v", scale.UID, deployment.UID),
		)
	}

	// 将 replicas 与 resourceVersion 字段写回父 Deployment 对象并返回。
	deployment.Spec.Replicas = scale.Spec.Replicas
	deployment.ResourceVersion = scale.ResourceVersion

	updatedEntries, err := managedFieldsHandler.ToParent(scale.ManagedFields)
	if err != nil {
		return nil, err
	}
	deployment.ManagedFields = updatedEntries

	return deployment, nil
}
