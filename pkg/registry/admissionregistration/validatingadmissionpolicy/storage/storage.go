/*
Copyright 2022 The Kubernetes Authors.

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

// 本包创建 ValidatingAdmissionPolicy 的 REST Store；CEL、ParamKind 与读取授权由上层 Strategy 负责校验。
package storage

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/kubernetes/pkg/apis/admissionregistration"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/admissionregistration/resolver"
	"k8s.io/kubernetes/pkg/registry/admissionregistration/validatingadmissionpolicy"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// REST 提供 ValidatingAdmissionPolicy 主资源的 REST 存储接口。
type REST struct {
	*genericregistry.Store
}

// StatusREST 仅暴露 ValidatingAdmissionPolicy 的 Status 子资源操作。
type StatusREST struct {
	// 不要直接嵌入 Store；这里只显式暴露 Status 子资源允许使用的方法。
	store *genericregistry.Store
}

var groupResource = admissionregistration.Resource("validatingadmissionpolicies")

// NewREST 创建主资源 Store 和独立的 /status Store；二者共享底层存储配置。
func NewREST(optsGetter generic.RESTOptionsGetter, authorizer authorizer.UnconditionalAuthorizer, resourceResolver resolver.ResourceResolver) (*REST, *StatusREST, error) {
	r := &REST{}
	strategy := validatingadmissionpolicy.NewStrategy(authorizer, resourceResolver)
	store := &genericregistry.Store{
		NewFunc:     func() runtime.Object { return &admissionregistration.ValidatingAdmissionPolicy{} },
		NewListFunc: func() runtime.Object { return &admissionregistration.ValidatingAdmissionPolicyList{} },
		ObjectNameFunc: func(obj runtime.Object) (string, error) {
			return obj.(*admissionregistration.ValidatingAdmissionPolicy).Name, nil
		},
		DefaultQualifiedResource:  groupResource,
		SingularQualifiedResource: admissionregistration.Resource("validatingadmissionpolicy"),

		CreateStrategy:      strategy,
		UpdateStrategy:      strategy,
		DeleteStrategy:      strategy,
		ResetFieldsStrategy: strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}
	r.Store = store
	statusStrategy := validatingadmissionpolicy.NewStatusStrategy(strategy)
	statusStore := *store
	statusStore.UpdateStrategy = statusStrategy
	statusStore.ResetFieldsStrategy = statusStrategy
	sr := &StatusREST{store: &statusStore}
	return r, sr, nil
}

// 确保 REST 实现资源类别提供者接口。
var _ rest.CategoriesProvider = &REST{}

// Categories 返回该资源所属的 API 资源类别。
func (r *REST) Categories() []string {
	return []string{"api-extensions"}
}

// New 创建用于 Status 子资源请求解码的 ValidatingAdmissionPolicy 对象。
func (r *StatusREST) New() runtime.Object {
	return &admissionregistration.ValidatingAdmissionPolicy{}
}

// Destroy 释放 REST 资源；StatusREST 与主 REST 共享 Store，因此此处不单独销毁。
func (r *StatusREST) Destroy() {
	// 底层 Store 与主资源 REST 共享，
	// 因此这里不显式销毁它。
}

// Get 从底层存储读取对象，供 Status 子资源的 Patch 等操作使用。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// GetResetFields 返回 /status 接口会重置的字段集合。
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

// ConvertToTable 委托底层 Store 将对象转换为表格响应。
func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

// Update 委托底层 Store 更新对象的 Status 子集。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, forceAllowCreate, options)
}
