/*
Copyright 2019 The Kubernetes Authors.

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

// 本文件构造 csinode 的 REST Store，将资源 Strategy、对象类型和通用存储后端连接起来。
package storage

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	storageapi "k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/storage/csinode"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// CSINodeStorage 汇总 CSINode 主资源及其子资源的 REST 存储。
type CSINodeStorage struct {
	CSINode *REST
	Status  *StatusREST
}

// REST 将 CSINode 的 REST 请求映射到底层通用 Store。
type REST struct {
	*genericregistry.Store
}

// NewStorage 创建 CSINode 的 REST Store，并配置资源策略和表格输出转换器。
func NewStorage(optsGetter generic.RESTOptionsGetter) (*CSINodeStorage, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &storageapi.CSINode{} },
		NewListFunc:               func() runtime.Object { return &storageapi.CSINodeList{} },
		DefaultQualifiedResource:  storageapi.Resource("csinodes"),
		SingularQualifiedResource: storageapi.Resource("csinode"),

		CreateStrategy:      csinode.Strategy,
		UpdateStrategy:      csinode.Strategy,
		DeleteStrategy:      csinode.Strategy,
		ResetFieldsStrategy: csinode.Strategy,
		ReturnDeletedObject: true,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = csinode.StatusStrategy
	statusStore.ResetFieldsStrategy = csinode.StatusStrategy

	return &CSINodeStorage{
		CSINode: &REST{store},
		Status:  &StatusREST{store: &statusStore},
	}, nil
}

// StatusREST 实现用于更新 CSINode Status 的 REST 子资源端点。
type StatusREST struct {
	store *genericregistry.Store
}

var _ = rest.Patcher(&StatusREST{})

// New 返回一个空的 CSINode 对象，供 Status 子资源请求使用。
func (r *StatusREST) New() runtime.Object {
	return &storageapi.CSINode{}
}

// Destroy 在 API Server 关闭时清理 Status 子资源拥有的资源。
func (r *StatusREST) Destroy() {
	// 底层 Store 与主资源 REST 共享，因此这里不能重复销毁。
}

// Get 从底层存储读取 CSINode；REST 子资源需要实现该方法以支持 Patch。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 只更新 CSINode 的 Status 子集，并禁止通过该子资源创建对象。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 调用底层 Store 时显式将 forceAllowCreate 设为 false，Status 子资源绝不能通过更新请求创建对象。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 实现 rest.ResetFieldsStrategy，返回 Status 子资源需要重置的字段集合。
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

// ConvertToTable 将资源对象转换为 metav1.Table，供 kubectl 等客户端以表格形式展示。
func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}
