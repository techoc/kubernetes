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

	pvcutil "k8s.io/kubernetes/pkg/api/persistentvolumeclaim"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/core/persistentvolumeclaim"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// REST 将 PVC 通用 CRUD Store 封装为 API Server RESTStorage，并在读取路径为旧对象补齐数据源关联字段。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建 PVC 主资源 Store 和 Status 子资源 Store，并配置读取时的数据源兼容补全。
// NewREST 创建 PVC 主资源 Store 与独立 Status Store，并注册 Decorator 让 GET/LIST 返回时应用只读兼容默认值。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &api.PersistentVolumeClaim{} },
		NewListFunc:               func() runtime.Object { return &api.PersistentVolumeClaimList{} },
		PredicateFunc:             persistentvolumeclaim.MatchPersistentVolumeClaim,
		DefaultQualifiedResource:  api.Resource("persistentvolumeclaims"),
		SingularQualifiedResource: api.Resource("persistentvolumeclaim"),

		CreateStrategy:      persistentvolumeclaim.Strategy,
		UpdateStrategy:      persistentvolumeclaim.Strategy,
		DeleteStrategy:      persistentvolumeclaim.Strategy,
		ReturnDeletedObject: true,
		ResetFieldsStrategy: persistentvolumeclaim.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter, AttrFunc: persistentvolumeclaim.GetAttrs}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = persistentvolumeclaim.StatusStrategy
	statusStore.ResetFieldsStrategy = persistentvolumeclaim.StatusStrategy

	rest := &REST{store}
	store.Decorator = rest.defaultOnRead

	return rest, &StatusREST{store: &statusStore}, nil
}

// 实现 ShortNamesProvider 接口。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 实现 ShortNamesProvider，返回 PVC 资源支持的简称列表。
func (r *REST) ShortNames() []string {
	return []string{"pvc"}
}

// defaultOnRead 在读取时补齐相互关联、历史对象中可能缺失的字段。
// 不能放在常规默认值处理阶段，因为常规默认值逻辑同时作用于 Get、Create 和 Update，而这里必须只处理读取路径。
// 此方法既可能接收 PersistentVolumeClaim，也可能接收 PersistentVolumeClaimList。
// defaultOnRead 只在读取时做兼容性补全，不将默认值写回 etcd，避免 GET/LIST 改变持久化对象。
func (r *REST) defaultOnRead(obj runtime.Object) {
	switch s := obj.(type) {
	case *api.PersistentVolumeClaim:
		r.defaultOnReadPvc(s)
	case *api.PersistentVolumeClaimList:
		r.defaultOnReadPvcList(s)
	default:
		// 当前对象类型不支持此处的默认值补全；这不是错误，因为缓存层也可能将其他对象传递到这里。
	}
}

// defaultOnReadPvcList 为 PersistentVolumeClaimList 中的每个 PVC 补齐兼容字段。
// 对 LIST 返回的每个 PVC 应用相同的数据源字段补全逻辑。
func (r *REST) defaultOnReadPvcList(pvcList *api.PersistentVolumeClaimList) {
	if pvcList == nil {
		return
	}

	for i := range pvcList.Items {
		r.defaultOnReadPvc(&pvcList.Items[i])
	}
}

// defaultOnReadPvc 为单个 PersistentVolumeClaim 补齐兼容字段。
// 老 PVC 可能只保存 dataSource；读取时补齐 dataSourceRef，让新客户端看到统一的数据源表示。
func (r *REST) defaultOnReadPvc(pvc *api.PersistentVolumeClaim) {
	if pvc == nil {
		return
	}

	// 新创建的 PVC 会在创建时将 dataSourceRef 同步为 dataSource 的值；但历史 PVC 可能只保存 dataSource，
	// 因此读取时需要补齐 dataSourceRef，保证新旧对象对客户端呈现一致的数据源信息。
	pvcutil.NormalizeDataSources(&pvc.Spec)
}

// StatusREST 实现用于修改 PersistentVolumeClaim.Status 的 REST 子资源端点。
type StatusREST struct {
	store *genericregistry.Store
}

// New 返回一个空的 PersistentVolumeClaim 对象。
func (r *StatusREST) New() runtime.Object {
	return &api.PersistentVolumeClaim{}
}

// Destroy 在 API Server 关闭时清理该 REST 存储拥有的资源。
func (r *StatusREST) Destroy() {
	// 底层 Store 与主资源 REST 共享，因此不能在此处重复销毁。
}

// Get 从底层存储读取 PVC；子资源需要实现该方法以支持 Patch。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 只更新 PVC.Status，不允许通过该入口修改 Spec。
// Update 将状态变更交给 StatusStrategy，明确禁止通过 status 子资源创建 PVC。
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
