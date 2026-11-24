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

// REST 将 Job 通用 CRUD Store 暴露为 API Server RESTStorage。
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

// 编译期接口断言：确保 REST 实现资源分类提供接口。
var _ rest.CategoriesProvider = &REST{}

// Categories 返回 Job 所属的资源分类；加入 all 后可通过通用分类查询发现 Job。
func (r *REST) Categories() []string {
	return []string{"all"}
}

// Delete 在 batch/v1 默认孤儿化传播且用户未显式指定策略时发出 Warning，再委托通用 Store 执行删除。
func (r *REST) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	//nolint:staticcheck // SA1019 backwards compatibility
	//nolint: staticcheck
	if options != nil && options.PropagationPolicy == nil && options.OrphanDependents == nil &&
		job.Strategy.DefaultGarbageCollectionPolicy(ctx) == rest.OrphanDependents {
		// 用户未显式指定删除传播策略时，提醒其 batch/v1 默认采用孤儿化删除，Job 创建的 Pod 不会随 Job 一并删除。
		warning.AddWarning(ctx, "", deleteOptionWarnings)
	}
	return r.Store.Delete(ctx, name, deleteValidation, options)
}

// DeleteCollection 对批量删除同样提示默认孤儿化行为，之后由通用 Store 执行集合删除。
func (r *REST) DeleteCollection(ctx context.Context, deleteValidation rest.ValidateObjectFunc, deleteOptions *metav1.DeleteOptions, listOptions *internalversion.ListOptions) (runtime.Object, error) {
	//nolint:staticcheck // SA1019 backwards compatibility
	if deleteOptions.PropagationPolicy == nil && deleteOptions.OrphanDependents == nil &&
		job.Strategy.DefaultGarbageCollectionPolicy(ctx) == rest.OrphanDependents {
		// 用户未显式指定删除传播策略时，提醒其 batch/v1 默认采用孤儿化删除，Job 创建的 Pod 不会随 Job 一并删除。
		warning.AddWarning(ctx, "", deleteOptionWarnings)
	}
	return r.Store.DeleteCollection(ctx, deleteValidation, deleteOptions, listOptions)
}

// StatusREST 实现 Job /status 子资源的 REST 接口，只负责读取对象及更新状态字段。
type StatusREST struct {
	store *genericregistry.Store
}

// New 创建一个空 Job 对象，供 REST 层解码请求体使用。
func (r *StatusREST) New() runtime.Object {
	return &batch.Job{}
}

// Destroy 在 REST 层关闭时释放资源；底层 Store 与主资源共享，因此这里不重复销毁。
func (r *StatusREST) Destroy() {
	// 底层 Store 与主 REST 资源共享，由主 REST 资源负责销毁，这里不重复执行清理。
}

// Get 从底层存储读取 Job；REST 的 Patch 流程需要先读取当前对象作为合并基础。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 通过 /status 子资源更新 Job 状态字段。
// Update 使用 StatusStrategy 且固定禁止 update-on-create，保证 /status 只能修改已存在 Job 的状态字段。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 显式向底层 Store 传入 false：子资源更新绝不能在对象不存在时退化为创建操作。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 返回 /status 更新时需要重置、禁止由该请求修改的字段集合。
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}
