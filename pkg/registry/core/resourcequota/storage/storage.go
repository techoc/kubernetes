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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/core/resourcequota"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// ResourceQuotaStorage 聚合主资源 REST 与 StatusREST：两者共享后端，但字段写权限不同。
type REST struct {
	*genericregistry.Store
}

// NewREST 构造主资源 Store，并复制配置创建 StatusStrategy Store，两个端点共享后端但具有不同字段写权限。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &api.ResourceQuota{} },
		NewListFunc:               func() runtime.Object { return &api.ResourceQuotaList{} },
		DefaultQualifiedResource:  api.Resource("resourcequotas"),
		SingularQualifiedResource: api.Resource("resourcequota"),

		CreateStrategy:      resourcequota.Strategy,
		UpdateStrategy:      resourcequota.Strategy,
		DeleteStrategy:      resourcequota.Strategy,
		ResetFieldsStrategy: resourcequota.Strategy,
		ReturnDeletedObject: true,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = resourcequota.StatusStrategy
	statusStore.ResetFieldsStrategy = resourcequota.StatusStrategy

	return &REST{store}, &StatusREST{store: &statusStore}, nil
}

// 实现 ShortNamesProvider
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 实现 ShortNamesProvider 接口，返回该资源的一组简称。
func (r *REST) ShortNames() []string {
	return []string{"quota"}
}

// StatusREST 实现了修改 resourcequota 状态的 REST 端点。
type StatusREST struct {
	store *genericregistry.Store
}

// New 创建一个新的 ResourceQuota 对象。
func (r *StatusREST) New() runtime.Object {
	return &api.ResourceQuota{}
}

// Destroy 在关闭时清理资源。
func (r *StatusREST) Destroy() {
	// 底层 store 与 REST 共享，因此这里不显式销毁它。
}

// Get 从存储中读取对象，这是支持 Patch 所必需的。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 委托给 StatusStrategy 更新 Used/Conditions 等状态，并禁止通过 status 子资源隐式创建新配额。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 在调用底层存储时我们显式把 forceAllowCreate 设为 false，因为
	// 子资源永远不应允许在更新时创建。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 实现 rest.ResetFieldsStrategy
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}
