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

	"k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/kubernetes/pkg/apis/batch"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/batch/job"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// JobStorage 聚合 Job 主资源与 Status 子资源的 REST Handler。
type JobStorage struct {
	Job    *REST
	Status *StatusREST
}

// NewStorage 创建 Job 主资源与状态存储，并将两者组装成统一 JobStorage。
func NewStorage(optsGetter generic.RESTOptionsGetter) (JobStorage, error) {
	jobRest, jobStatusRest, err := NewREST(optsGetter)
	if err != nil {
		return JobStorage{}, err
	}

	return JobStorage{
		Job:    jobRest,
		Status: jobStatusRest,
	}, nil
}

var deleteOptionWarnings = "child pods are preserved by default when jobs are deleted; " +
	"set propagationPolicy=Background to remove them or set propagationPolicy=Orphan to suppress this warning"

// REST implements a RESTStorage for jobs against etcd
type REST struct {
	*genericregistry.Store
}

// NewREST 配置 Job 主资源 Store 与选择器/表格转换，并复制 Store 为 Status 子资源安装 StatusStrategy。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &batch.Job{} },
		NewListFunc:               func() runtime.Object { return &batch.JobList{} },
		PredicateFunc:             job.MatchJob,
		DefaultQualifiedResource:  batch.Resource("jobs"),
		SingularQualifiedResource: batch.Resource("job"),

		CreateStrategy:      job.Strategy,
		UpdateStrategy:      job.Strategy,
		DeleteStrategy:      job.Strategy,
		ResetFieldsStrategy: job.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter, AttrFunc: job.GetAttrs}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = job.StatusStrategy
	statusStore.ResetFieldsStrategy = job.StatusStrategy

	return &REST{store}, &StatusREST{store: &statusStore}, nil
}

// Implement CategoriesProvider
var _ rest.CategoriesProvider = &REST{}

// Categories implements the CategoriesProvider interface. Returns a list of categories a resource is part of.
func (r *REST) Categories() []string {
	return []string{"all"}
}

// Delete 在 batch/v1 默认孤儿化传播且用户未显式指定策略时发出 Warning，再委托通用 Store 执行删除。
func (r *REST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	//nolint:staticcheck // SA1019 backwards compatibility
	//nolint: staticcheck
	if options != nil && options.PropagationPolicy == nil && options.OrphanDependents == nil &&
		job.Strategy.DefaultGarbageCollectionPolicy(ctx) == rest.OrphanDependents {
		// Throw a warning if delete options are not explicitly set as Job deletion strategy by default is orphaning
		// pods in v1.
		warning.AddWarning(ctx, "", deleteOptionWarnings)
	}
	return r.Store.Delete(ctx, name, deleteValidation, options)
}

// DeleteCollection 对批量删除同样提示默认孤儿化行为，之后由通用 Store 执行集合删除。
func (r *REST) DeleteCollection(ctx context.Context, deleteValidation rest.ValidateObjectFunc, deleteOptions *metav1.DeleteOptions, listOptions *internalversion.ListOptions) (runtime.Object, error) {
	//nolint:staticcheck // SA1019 backwards compatibility
	if deleteOptions.PropagationPolicy == nil && deleteOptions.OrphanDependents == nil &&
		job.Strategy.DefaultGarbageCollectionPolicy(ctx) == rest.OrphanDependents {
		// Throw a warning if delete options are not explicitly set as Job deletion strategy by default is orphaning
		// pods in v1.
		warning.AddWarning(ctx, "", deleteOptionWarnings)
	}
	return r.Store.DeleteCollection(ctx, deleteValidation, deleteOptions, listOptions)
}

// StatusREST implements the REST endpoint for changing the status of a resourcequota.
type StatusREST struct {
	store *genericregistry.Store
}

// New creates a new Job object.
func (r *StatusREST) New() runtime.Object {
	return &batch.Job{}
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
// Update 使用 StatusStrategy 且固定禁止 update-on-create，保证 /status 只能修改已存在 Job 的状态字段。
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
