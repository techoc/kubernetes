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

// Package customresourcedefinition 实现 CRD 的 REST Storage。
// 负责 CRD 的 CRUD 操作、删除时的 finalizer 添加、
// 以及 Table 转换（kubectl get 的输出格式）。
package customresourcedefinition

import (
	"context"
	"fmt"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	crdtable "k8s.io/apiextensions-apiserver/pkg/registry/customresourcedefinition/tableconvertor"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	storageerr "k8s.io/apiserver/pkg/storage/errors"
	"k8s.io/apiserver/pkg/util/dryrun"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// REST 是 CRD 的 etcd 存储实现，封装了通用 registry store。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建 CRD 的 REST Storage，配置策略、存储和表转换器。
func NewREST(scheme *runtime.Scheme, optsGetter generic.RESTOptionsGetter) (*REST, error) {
	strategy := NewStrategy(scheme)

	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &apiextensions.CustomResourceDefinition{} },
		NewListFunc:               func() runtime.Object { return &apiextensions.CustomResourceDefinitionList{} },
		PredicateFunc:             MatchCustomResourceDefinition,
		DefaultQualifiedResource:  apiextensions.Resource("customresourcedefinitions"),
		SingularQualifiedResource: apiextensions.Resource("customresourcedefinition"),

		CreateStrategy:      strategy,
		UpdateStrategy:      strategy,
		DeleteStrategy:      strategy,
		ResetFieldsStrategy: strategy,

		TableConvertor: crdtable.New(),
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter, AttrFunc: GetAttrs}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}
	return &REST{store}, nil
}

// 声明 REST 实现 ShortNamesProvider 接口。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 实现 ShortNamesProvider 接口，返回 CRD 资源的 kubectl 简称。
func (r *REST) ShortNames() []string {
	return []string{"crd", "crds"}
}

// 声明 REST 实现 CategoriesProvider 接口。
var _ rest.CategoriesProvider = &REST{}

// Categories 实现 CategoriesProvider 接口，返回 CRD 所属的资源分类。
func (r *REST) Categories() []string {
	return []string{"api-extensions"}
}

// Delete 覆盖默认删除行为，在删除 CRD 时：
// 1. 确保 UID 前置条件（防止误删）
// 2. 添加 finalizer 触发级联删除 CR 实例
// 3. 调用底层存储删除
func (r *REST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	obj, err := r.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		return nil, false, err
	}

	crd := obj.(*apiextensions.CustomResourceDefinition)

	// 确保删除请求包含 UID 前置条件，避免误删名称相同但 UID 已变化的 CRD。
	if options == nil {
		options = metav1.NewDeleteOptions(0)
	}
	if options.Preconditions == nil {
		options.Preconditions = &metav1.Preconditions{}
	}
	if options.Preconditions.UID == nil {
		options.Preconditions.UID = &crd.UID
	} else if *options.Preconditions.UID != crd.UID {
		err = apierrors.NewConflict(
			apiextensions.Resource("customresourcedefinitions"),
			name,
			fmt.Errorf("precondition failed: UID in precondition: %v, UID in object meta: %v", *options.Preconditions.UID, crd.UID),
		)
		return nil, false, err
	}
	if options.Preconditions.ResourceVersion != nil && *options.Preconditions.ResourceVersion != crd.ResourceVersion {
		err = apierrors.NewConflict(
			apiextensions.Resource("customresourcedefinitions"),
			name,
			fmt.Errorf("precondition failed: ResourceVersion in precondition: %v, ResourceVersion in object meta: %v", *options.Preconditions.ResourceVersion, crd.ResourceVersion),
		)
		return nil, false, err
	}

	// 首次收到删除请求时，先添加清理 finalizer 并标记 Terminating，再由后续请求执行真正删除。
	if crd.DeletionTimestamp.IsZero() {
		key, err := r.Store.KeyFunc(ctx, name)
		if err != nil {
			return nil, false, err
		}

		preconditions := storage.Preconditions{UID: options.Preconditions.UID, ResourceVersion: options.Preconditions.ResourceVersion}

		out := r.Store.NewFunc()
		err = r.Store.Storage.GuaranteedUpdate(
			ctx, key, out, false, &preconditions,
			storage.SimpleUpdate(func(existing runtime.Object) (runtime.Object, error) {
				existingCRD, ok := existing.(*apiextensions.CustomResourceDefinition)
				if !ok {
					// 底层存储返回了非预期的对象类型。
					return nil, fmt.Errorf("expected *apiextensions.CustomResourceDefinition, got %v", existing)
				}
				if err := deleteValidation(ctx, existingCRD); err != nil {
					return nil, err
				}

				// 尚未进入删除流程时，记录当前删除时间。
				if existingCRD.DeletionTimestamp.IsZero() {
					now := metav1.Now()
					existingCRD.DeletionTimestamp = &now
				}

				if !apiextensions.CRDHasFinalizer(existingCRD, apiextensions.CustomResourceCleanupFinalizer) {
					existingCRD.Finalizers = append(existingCRD.Finalizers, apiextensions.CustomResourceCleanupFinalizer)
				}
				// 同步更新 Status 条件，告知客户端 CRD 已进入终止阶段。
				apiextensions.SetCRDCondition(existingCRD, apiextensions.CustomResourceDefinitionCondition{
					Type:    apiextensions.Terminating,
					Status:  apiextensions.ConditionTrue,
					Reason:  "InstanceDeletionPending",
					Message: "CustomResourceDefinition marked for deletion; CustomResource deletion will begin soon",
				})
				return existingCRD, nil
			}),
			dryrun.IsDryRun(options.DryRun),
			nil,
		)

		if err != nil {
			err = storageerr.InterpretGetError(err, apiextensions.Resource("customresourcedefinitions"), name)
			err = storageerr.InterpretUpdateError(err, apiextensions.Resource("customresourcedefinitions"), name)
			if _, ok := err.(*apierrors.StatusError); !ok {
				err = apierrors.NewInternalError(err)
			}
			return nil, false, err
		}

		return out, false, nil
	}

	return r.Store.Delete(ctx, name, deleteValidation, options)
}

// NewStatusREST 创建权限范围更窄的 CRD /status 子资源存储。
// 它复用主资源的底层 Store，但禁用创建和删除策略，并使用专用 StatusStrategy。
func NewStatusREST(scheme *runtime.Scheme, rest *REST) *StatusREST {
	statusStore := *rest.Store
	statusStore.CreateStrategy = nil
	statusStore.DeleteStrategy = nil
	statusStrategy := NewStatusStrategy(scheme)
	statusStore.UpdateStrategy = statusStrategy
	statusStore.ResetFieldsStrategy = statusStrategy
	return &StatusREST{store: &statusStore}
}

// StatusREST 实现 CRD Status 子资源的读取和更新端点。
type StatusREST struct {
	store *genericregistry.Store
}

var _ = rest.Patcher(&StatusREST{})

// New 返回一个空的 CustomResourceDefinition 对象，供 Status 子资源请求使用。
func (r *StatusREST) New() runtime.Object {
	return &apiextensions.CustomResourceDefinition{}
}

// Destroy 在 API Server 关闭时清理 Status 子资源拥有的资源。
func (r *StatusREST) Destroy() {
	// 底层 Store 与主资源 REST 共享，因此这里不能重复销毁。
}

// Get 从底层存储读取 CRD；REST 子资源需要实现该方法以支持 Patch。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 只更新 CRD 的 Status 子集，不允许通过该入口修改 Spec。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 调用底层 Store 时显式将 forceAllowCreate 设为 false，Status 子资源绝不能通过更新请求创建对象。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 实现 rest.ResetFieldsStrategy，返回 Status 子资源需要重置的字段集合。
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}
