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
	"k8s.io/kubernetes/pkg/apis/apps"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/apps/daemonset"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// REST 实现 DaemonSet 主资源的 RESTStorage；Status 子资源使用单独的 Strategy 副本。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建并返回用于访问 DaemonSet 资源的 RESTStorage。
// NewREST 创建 DaemonSet Store，并使用 StatusStrategy 派生状态更新存储，确保主资源和状态写入边界分离。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &apps.DaemonSet{} },
		NewListFunc:               func() runtime.Object { return &apps.DaemonSetList{} },
		DefaultQualifiedResource:  apps.Resource("daemonsets"),
		SingularQualifiedResource: apps.Resource("daemonset"),

		CreateStrategy:      daemonset.Strategy,
		UpdateStrategy:      daemonset.Strategy,
		DeleteStrategy:      daemonset.Strategy,
		ResetFieldsStrategy: daemonset.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}

	// Status Handler 复用 etcd 存储配置，但替换写入策略，避免 /status 更新 Spec。
	statusStore := *store
	statusStore.UpdateStrategy = daemonset.StatusStrategy
	statusStore.ResetFieldsStrategy = daemonset.StatusStrategy

	return &REST{store}, &StatusREST{store: &statusStore}, nil
}

// 实现 ShortNamesProvider 接口。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 实现 ShortNamesProvider，返回该资源支持的简称列表。
func (r *REST) ShortNames() []string {
	return []string{"ds"}
}

var _ rest.CategoriesProvider = &REST{}

// Categories 实现 CategoriesProvider，返回该资源所属的类别列表。
func (r *REST) Categories() []string {
	return []string{"all"}
}

// StatusREST 实现用于修改 DaemonSet 状态的 REST 子资源端点。
type StatusREST struct {
	store *genericregistry.Store
}

// New 返回一个空的 DaemonSet 对象。
func (r *StatusREST) New() runtime.Object {
	return &apps.DaemonSet{}
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
// Update 固定 forceAllowCreate=false，状态子资源只能更新已有 DaemonSet，不能在更新时隐式创建。
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
