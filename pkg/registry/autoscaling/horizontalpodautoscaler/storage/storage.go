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

// 本文件构建 horizontalpodautoscaler 的 REST Store，将 API 对象、Strategy 与通用存储后端连接起来。
package storage

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/autoscaling/horizontalpodautoscaler"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// REST 为 HPA 实现基于通用 Store 的 RESTStorage，底层对象持久化到 etcd。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建 HPA 主资源 Store，并派生使用 StatusStrategy 的状态子资源 Store。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &autoscaling.HorizontalPodAutoscaler{} },
		NewListFunc:               func() runtime.Object { return &autoscaling.HorizontalPodAutoscalerList{} },
		DefaultQualifiedResource:  autoscaling.Resource("horizontalpodautoscalers"),
		SingularQualifiedResource: autoscaling.Resource("horizontalpodautoscaler"),

		CreateStrategy:      horizontalpodautoscaler.Strategy,
		UpdateStrategy:      horizontalpodautoscaler.Strategy,
		DeleteStrategy:      horizontalpodautoscaler.Strategy,
		ResetFieldsStrategy: horizontalpodautoscaler.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = horizontalpodautoscaler.StatusStrategy
	statusStore.ResetFieldsStrategy = horizontalpodautoscaler.StatusStrategy
	return &REST{store}, &StatusREST{store: &statusStore}, nil
}

// 实现 ShortNamesProvider 接口。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 实现 ShortNamesProvider，返回 HPA 资源支持的简称列表。
func (r *REST) ShortNames() []string {
	return []string{"hpa"}
}

// 实现 CategoriesProvider 接口。
var _ rest.CategoriesProvider = &REST{}

// Categories 实现 CategoriesProvider，返回 HPA 资源所属的类别列表。
func (r *REST) Categories() []string {
	return []string{"all"}
}

// StatusREST 实现用于修改 HPA Status 的 REST 子资源端点。
type StatusREST struct {
	store *genericregistry.Store
}

// New 返回一个空的 HorizontalPodAutoscaler 对象。
func (r *StatusREST) New() runtime.Object {
	return &autoscaling.HorizontalPodAutoscaler{}
}

// Destroy 在 API Server 关闭时清理该 REST 存储所拥有的资源。
func (r *StatusREST) Destroy() {
	// 底层 Store 与主资源 REST 共享，因此不能在此处重复销毁。
}

// Get 从底层存储读取 HPA；REST 子资源需要实现该方法以支持 Patch。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 只更新 HPA 的 Status 子集，不允许通过该入口修改 Spec。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 调用底层 Store 时显式将 forceAllowCreate 设为 false，因为子资源绝不能通过更新请求创建对象。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 实现 rest.ResetFieldsStrategy，返回 Status 子资源需要重置的字段集合。
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

// ConvertToTable 将对象转换为 metav1.Table，支持 kubectl 表格输出。
func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}
