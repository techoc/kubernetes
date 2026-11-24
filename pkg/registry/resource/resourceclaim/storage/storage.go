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

package storage

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/resource/resourceclaim"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// REST 将 ResourceClaim 的 REST 请求映射到底层通用 Store。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建命名空间级 ResourceClaim 主资源 Store，并派生使用 StatusStrategy 的 /status 子资源 Store。
func NewREST(optsGetter generic.RESTOptionsGetter, nsClient v1.NamespaceInterface, authorizer authorizer.UnconditionalAuthorizer) (*REST, *StatusREST, error) {
	if nsClient == nil {
		return nil, nil, fmt.Errorf("namespace client is required")
	}
	if authorizer == nil {
		return nil, nil, fmt.Errorf("authorizer is required")
	}

	strategy := resourceclaim.NewStrategy(nsClient, authorizer)
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &resource.ResourceClaim{} },
		NewListFunc:               func() runtime.Object { return &resource.ResourceClaimList{} },
		PredicateFunc:             resourceclaim.Match,
		DefaultQualifiedResource:  resource.Resource("resourceclaims"),
		SingularQualifiedResource: resource.Resource("resourceclaim"),

		CreateStrategy:      strategy,
		UpdateStrategy:      strategy,
		DeleteStrategy:      strategy,
		ReturnDeletedObject: true,
		ResetFieldsStrategy: strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter, AttrFunc: resourceclaim.GetAttrs}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, err
	}

	statusStore := *store
	statusStrategy := resourceclaim.NewStatusStrategy(strategy)
	statusStore.UpdateStrategy = statusStrategy
	statusStore.ResetFieldsStrategy = statusStrategy

	rest := &REST{store}

	return rest, &StatusREST{store: &statusStore}, nil
}

// StatusREST 实现用于更新 ResourceClaim Status 的 REST 子资源端点。
type StatusREST struct {
	store *genericregistry.Store
}

// New 返回一个空的 ResourceClaim 对象，供 Status 子资源请求使用。
func (r *StatusREST) New() runtime.Object {
	return &resource.ResourceClaim{}
}

func (r *StatusREST) Destroy() {
	// 底层 Store 与主资源 REST 共享，因此这里不能重复销毁。
}

// Get 从底层存储读取 ResourceClaim；REST 子资源需要实现该方法以支持 Patch。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 只更新 ResourceClaim 的 Status 子集，并禁止通过该子资源创建对象。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 调用底层 Store 时显式将 forceAllowCreate 设为 false，Status 子资源绝不能通过更新请求创建对象。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 实现 rest.ResetFieldsStrategy，返回 Status 子资源需要重置的字段集合。
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}
