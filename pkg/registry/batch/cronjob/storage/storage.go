/*
Copyright 2016 The Kubernetes Authors.

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
	"k8s.io/kubernetes/pkg/apis/batch"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/batch/cronjob"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// REST 将 CronJob 通用 CRUD Store 暴露为 API Server RESTStorage。
type REST struct {
	*genericregistry.Store
}

// NewREST 构建 CronJob 主资源 RESTStorage，并返回使用独立状态更新策略的 /status 子资源。
// NewREST 创建 CronJob 主资源 Store，并复制 Store 配置给 Status 子资源以替换 StatusStrategy。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &batch.CronJob{} },
		NewListFunc:               func() runtime.Object { return &batch.CronJobList{} },
		DefaultQualifiedResource:  batch.Resource("cronjobs"),
		SingularQualifiedResource: batch.Resource("cronjob"),

		CreateStrategy:      cronjob.Strategy,
		UpdateStrategy:      cronjob.Strategy,
		DeleteStrategy:      cronjob.Strategy,
		ResetFieldsStrategy: cronjob.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}

	statusStore := *store
	statusStore.UpdateStrategy = cronjob.StatusStrategy
	statusStore.ResetFieldsStrategy = cronjob.StatusStrategy

	return &REST{store}, &StatusREST{store: &statusStore}, nil
}

var _ rest.CategoriesProvider = &REST{}
var _ rest.ShortNamesProvider = &REST{}

// Categories 返回 CronJob 所属的资源分类；加入 all 后可通过通用分类查询发现 CronJob。
func (r *REST) Categories() []string {
	return []string{"all"}
}

// ShortNames 返回 CronJob 在 kubectl 中使用的资源简称 cj。
func (r *REST) ShortNames() []string {
	return []string{"cj"}
}

// StatusREST 实现 CronJob /status 子资源的 REST 接口，只负责读取对象及更新状态字段。
type StatusREST struct {
	store *genericregistry.Store
}

// New 创建一个空 CronJob 对象，供 REST 层解码请求体使用。
func (r *StatusREST) New() runtime.Object {
	return &batch.CronJob{}
}

// Destroy 在 REST 层关闭时释放资源；底层 Store 与主资源共享，因此这里不重复销毁。
func (r *StatusREST) Destroy() {
	// 底层 Store 与主 REST 资源共享，由主 REST 资源负责销毁，这里不重复执行清理。
}

// Get 从底层存储读取 CronJob；REST 的 Patch 流程需要先读取当前对象作为合并基础。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 通过 /status 子资源更新 CronJob 状态字段。
// Update 只允许更新 CronJob.Status，并禁止 status 请求在目标对象不存在时隐式创建 CronJob。
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
