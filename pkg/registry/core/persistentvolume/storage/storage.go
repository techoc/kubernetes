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
	"k8s.io/kubernetes/pkg/registry/core/persistentvolume"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// REST 将 PersistentVolume 通用 CRUD Store 暴露为 API Server RESTStorage；StatusREST 使用专门策略隔离状态写入。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建集群级 PV 主资源 Store，并派生使用 StatusStrategy 的状态子资源 Store。
// NewREST 创建集群级 PV Store，配置字段筛选器和主资源策略，再复制 Store 为 status 子资源安装 StatusStrategy。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &api.PersistentVolume{} },
		NewListFunc:               func() runtime.Object { return &api.PersistentVolumeList{} },
		PredicateFunc:             persistentvolume.MatchPersistentVolumes,
		DefaultQualifiedResource:  api.Resource("persistentvolumes"),
		SingularQualifiedResource: api.Resource("persistentvolume"),

		CreateStrategy:      persistentvolume.Strategy,
		UpdateStrategy:      persistentvolume.Strategy,
		DeleteStrategy:      persistentvolume.Strategy,
		ReturnDeletedObject: true,
		ResetFieldsStrategy: persistentvolume.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter, AttrFunc: persistentvolume.GetAttrs}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = persistentvolume.StatusStrategy
	statusStore.ResetFieldsStrategy = persistentvolume.StatusStrategy

	return &REST{store}, &StatusREST{store: &statusStore}, nil
}

// 实现 ShortNamesProvider 接口。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 实现 ShortNamesProvider，返回 PV 资源支持的简称列表。
func (r *REST) ShortNames() []string {
	return []string{"pv"}
}

// StatusREST 实现用于修改 PersistentVolume.Status 的 REST 子资源端点。
type StatusREST struct {
	store *genericregistry.Store
}

// New 返回一个空的 PersistentVolume 对象。
func (r *StatusREST) New() runtime.Object {
	return &api.PersistentVolume{}
}

// Destroy 在 API Server 关闭时清理该 REST 存储拥有的资源。
func (r *StatusREST) Destroy() {
	// 底层 Store 与主资源 REST 共享，因此不能在此处重复销毁。
}

// Get 从底层存储读取 PV；子资源需要实现该方法以支持 Patch。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 只更新 PV.Status，不允许通过该入口修改 Spec。
// Update 通过 StatusStrategy 修改 PV 状态，并禁止 status 子资源在对象不存在时创建新 PV。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 显式将 forceAllowCreate 设为 false，因为子资源绝不能通过更新请求创建对象。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 实现 rest.ResetFieldsStrategy，返回 Status 子资源需要重置的字段集合。
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}
