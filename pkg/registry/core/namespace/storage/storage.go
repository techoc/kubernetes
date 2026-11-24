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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	storageerr "k8s.io/apiserver/pkg/storage/errors"
	"k8s.io/apiserver/pkg/util/dryrun"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/core/namespace"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// REST 将 Namespace 主资源及共享通用 Store 封装为 API Server 接口；status 和 finalize 使用独立写入策略。
type REST struct {
	store  *genericregistry.Store
	status *genericregistry.Store
}

// StatusREST 实现用于修改 Namespace.Status 的 REST 子资源端点。
type StatusREST struct {
	store *genericregistry.Store
}

// FinalizeREST 实现用于修改 Namespace.Spec.Finalizers 的 REST 子资源端点。
type FinalizeREST struct {
	store *genericregistry.Store
}

// NewREST 创建 Namespace 主资源、status 子资源和 finalize 子资源对应的 RESTStorage。
// NewREST 创建主资源 Store，并复制出 StatusStrategy/FinalizeStrategy 两份 Store，使三个端点对字段的写入权限互相隔离。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, *FinalizeREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &api.Namespace{} },
		NewListFunc:               func() runtime.Object { return &api.NamespaceList{} },
		PredicateFunc:             namespace.MatchNamespace,
		DefaultQualifiedResource:  api.Resource("namespaces"),
		SingularQualifiedResource: api.Resource("namespace"),

		CreateStrategy:      namespace.Strategy,
		UpdateStrategy:      namespace.Strategy,
		DeleteStrategy:      namespace.Strategy,
		ResetFieldsStrategy: namespace.Strategy,
		ReturnDeletedObject: true,

		ShouldDeleteDuringUpdate: ShouldDeleteNamespaceDuringUpdate,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter, AttrFunc: namespace.GetAttrs}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = namespace.StatusStrategy
	statusStore.ResetFieldsStrategy = namespace.StatusStrategy

	finalizeStore := *store
	finalizeStore.UpdateStrategy = namespace.FinalizeStrategy
	finalizeStore.ResetFieldsStrategy = namespace.FinalizeStrategy

	return &REST{store: store, status: &statusStore}, &StatusREST{store: &statusStore}, &FinalizeREST{store: &finalizeStore}, nil
}

func (r *REST) NamespaceScoped() bool {
	return r.store.NamespaceScoped()
}

var _ rest.SingularNameProvider = &REST{}

func (r *REST) GetSingularName() string {
	return r.store.GetSingularName()
}

func (r *REST) New() runtime.Object {
	return r.store.New()
}

// Destroy 在 API Server 关闭时清理该 REST 存储拥有的资源。
func (r *REST) Destroy() {
	r.store.Destroy()
}

func (r *REST) NewList() runtime.Object {
	return r.store.NewList()
}

func (r *REST) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	return r.store.List(ctx, options)
}

func (r *REST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	return r.store.Create(ctx, obj, createValidation, options)
}

func (r *REST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, forceAllowCreate, options)
}

func (r *REST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

func (r *REST) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	return r.store.Watch(ctx, options)
}

// Delete 执行 Namespace 终止流程的生命周期约束。
// Delete 是 Namespace 终止状态机入口：先校验 UID/ResourceVersion，再把 Phase 置为 Terminating 并设置 DeletionTimestamp/删除传播 Finalizer。
func (r *REST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	nsObj, err := r.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		return nil, false, err
	}

	namespace := nsObj.(*api.Namespace)

	// 确保删除请求包含 UID 前置条件，防止误删名称相同但已被重建的 Namespace。
	if options == nil {
		options = metav1.NewDeleteOptions(0)
	}
	if options.Preconditions == nil {
		options.Preconditions = &metav1.Preconditions{}
	}
	if options.Preconditions.UID == nil {
		options.Preconditions.UID = &namespace.UID
	} else if *options.Preconditions.UID != namespace.UID {
		err = apierrors.NewConflict(
			api.Resource("namespaces"),
			name,
			fmt.Errorf("Precondition failed: UID in precondition: %v, UID in object meta: %v", *options.Preconditions.UID, namespace.UID),
		)
		return nil, false, err
	}
	if options.Preconditions.ResourceVersion != nil && *options.Preconditions.ResourceVersion != namespace.ResourceVersion {
		err = apierrors.NewConflict(
			api.Resource("namespaces"),
			name,
			fmt.Errorf("Precondition failed: ResourceVersion in precondition: %v, ResourceVersion in object meta: %v", *options.Preconditions.ResourceVersion, namespace.ResourceVersion),
		)
		return nil, false, err
	}

	// 首次收到删除请求时，将 Namespace 切换到 Terminating，正式启动命名空间终止流程。
	// TODO：增强优雅删除流程对 DeleteStrategy 的支持，使其能够统一处理 Phase 变更和 Finalizer 模式。
	if namespace.DeletionTimestamp.IsZero() {
		key, err := r.store.KeyFunc(ctx, name)
		if err != nil {
			return nil, false, err
		}

		preconditions := storage.Preconditions{UID: options.Preconditions.UID, ResourceVersion: options.Preconditions.ResourceVersion}

		out := r.store.NewFunc()
		err = r.store.Storage.GuaranteedUpdate(
			ctx, key, out, false, &preconditions,
			storage.SimpleUpdate(func(existing runtime.Object) (runtime.Object, error) {
				existingNamespace, ok := existing.(*api.Namespace)
				if !ok {
					// 底层对象类型不符合预期。
					return nil, fmt.Errorf("expected *api.Namespace, got %v", existing)
				}
				if err := deleteValidation(ctx, existingNamespace); err != nil {
					return nil, err
				}
				// 尚未设置删除时间戳时，记录当前时间。
				if existingNamespace.DeletionTimestamp.IsZero() {
					now := metav1.Now()
					existingNamespace.DeletionTimestamp = &now
				}
				// 尚未进入终止阶段时，将 Namespace Phase 设置为 Terminating。
				if existingNamespace.Status.Phase != api.NamespaceTerminating {
					existingNamespace.Status.Phase = api.NamespaceTerminating
				}

				// 收集 Namespace 当前已有的 Finalizer。
				currentFinalizers := map[string]bool{}
				for _, f := range existingNamespace.Finalizers {
					currentFinalizers[f] = true
				}
				// 根据删除选项确定本次删除应保留的 Finalizer。
				shouldHaveFinalizers := map[string]bool{
					metav1.FinalizerOrphanDependents: shouldHaveOrphanFinalizer(options, currentFinalizers[metav1.FinalizerOrphanDependents]),
					metav1.FinalizerDeleteDependents: shouldHaveDeleteDependentsFinalizer(options, currentFinalizers[metav1.FinalizerDeleteDependents]),
				}
				// 判断 Finalizer 集合是否需要变更。
				changeNeeded := false
				for finalizer, shouldHave := range shouldHaveFinalizers {
					changeNeeded = currentFinalizers[finalizer] != shouldHave || changeNeeded
					if shouldHave {
						currentFinalizers[finalizer] = true
					} else {
						delete(currentFinalizers, finalizer)
					}
				}
				// 仅在确实发生变化时写回新的 Finalizer 集合。
				if changeNeeded {
					newFinalizers := []string{}
					for f := range currentFinalizers {
						newFinalizers = append(newFinalizers, f)
					}
					existingNamespace.Finalizers = newFinalizers
				}
				return existingNamespace, nil
			}),
			dryrun.IsDryRun(options.DryRun),
			nil,
		)

		if err != nil {
			err = storageerr.InterpretGetError(err, api.Resource("namespaces"), name)
			err = storageerr.InterpretUpdateError(err, api.Resource("namespaces"), name)
			if _, ok := err.(*apierrors.StatusError); !ok {
				err = apierrors.NewInternalError(err)
			}
			return nil, false, err
		}

		return out, false, nil
	}

	// 真正删除 Namespace 之前，必须确保所有 Finalizer 均已清空。
	if len(namespace.Spec.Finalizers) != 0 {
		return namespace, false, nil
	}
	return r.store.Delete(ctx, name, deleteValidation, options)
}

// ShouldDeleteNamespaceDuringUpdate 在通用 ShouldDeleteDuringUpdate 规则之上，增加 Namespace 专属的 Spec.Finalizers 检查。
// ShouldDeleteNamespaceDuringUpdate 让更新请求在 Namespace 已进入删除流程且 Finalizer 已清空时完成真正删除。
func ShouldDeleteNamespaceDuringUpdate(ctx context.Context, key string, obj, existing runtime.Object) bool {
	ns, ok := obj.(*api.Namespace)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("unexpected type %T", obj))
		return false
	}
	return len(ns.Spec.Finalizers) == 0 && genericregistry.ShouldDeleteDuringUpdate(ctx, key, obj, existing)
}

func shouldHaveOrphanFinalizer(options *metav1.DeleteOptions, haveOrphanFinalizer bool) bool {
	//nolint:staticcheck // SA1019 backwards compatibility
	if options.OrphanDependents != nil {
		return *options.OrphanDependents
	}
	if options.PropagationPolicy != nil {
		return *options.PropagationPolicy == metav1.DeletePropagationOrphan
	}
	return haveOrphanFinalizer
}

func shouldHaveDeleteDependentsFinalizer(options *metav1.DeleteOptions, haveDeleteDependentsFinalizer bool) bool {
	//nolint:staticcheck // SA1019 backwards compatibility
	if options.OrphanDependents != nil {
		return *options.OrphanDependents == false
	}
	if options.PropagationPolicy != nil {
		return *options.PropagationPolicy == metav1.DeletePropagationForeground
	}
	return haveDeleteDependentsFinalizer
}

func (e *REST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return e.store.ConvertToTable(ctx, object, tableOptions)
}

// 实现 ShortNamesProvider 接口。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 实现 ShortNamesProvider，返回 Namespace 资源支持的简称列表。
func (r *REST) ShortNames() []string {
	return []string{"ns"}
}

var _ rest.StorageVersionProvider = &REST{}

func (r *REST) StorageVersion() runtime.GroupVersioner {
	return r.store.StorageVersion()
}

// GetResetFields 实现 rest.ResetFieldsStrategy，返回该子资源会重置的字段集合。
func (r *REST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}
func (r *StatusREST) New() runtime.Object {
	return r.store.New()
}

// Destroy 在 API Server 关闭时清理该 REST 子资源拥有的资源。
func (r *StatusREST) Destroy() {
	// 底层 Store 与主资源 REST 共享，因此不能在此处重复销毁。
}

// Get 从底层存储读取 Namespace；子资源需要实现该方法以支持 Patch。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 只更新 Namespace.Status，不允许通过该入口修改 Spec。
// Update 通过 StatusStrategy 更新 Namespace.Status，并明确禁止 update-on-create。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 显式将 forceAllowCreate 设为 false，因为子资源绝不能通过更新请求创建对象。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 实现 rest.ResetFieldsStrategy，返回子资源需要重置的字段集合。
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

func (r *FinalizeREST) New() runtime.Object {
	return r.store.New()
}

// Destroy 在 API Server 关闭时清理该 REST 子资源拥有的资源。
func (r *FinalizeREST) Destroy() {
	// 底层 Store 与 Namespace 主资源 REST 共享，因此不能在此处重复销毁。
}

// Update 只更新 Namespace.Spec.Finalizers，不允许通过 finalize 子资源修改 Status。
// Update 通过 FinalizeStrategy 修改 Namespace.Spec.Finalizers；Namespace Controller 依赖此接口在清理完成后解除删除阻塞。
func (r *FinalizeREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 显式将 forceAllowCreate 设为 false，因为子资源绝不能通过更新请求创建对象。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 实现 rest.ResetFieldsStrategy，返回子资源需要重置的字段集合。
func (r *FinalizeREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}
