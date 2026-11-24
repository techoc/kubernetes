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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/core/secret"
)

// REST 将 Secret 的通用 CRUD 存储封装为 API Server RESTStorage。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建并返回用于访问 Secret 资源的 RESTStorage。
// NewREST 注册 Secret 对象构造器、字段筛选器、读写策略和表格转换器，再初始化底层持久化 Store。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &api.Secret{} },
		NewListFunc:               func() runtime.Object { return &api.SecretList{} },
		PredicateFunc:             secret.Matcher,
		DefaultQualifiedResource:  api.Resource("secrets"),
		SingularQualifiedResource: api.Resource("secret"),

		CreateStrategy: secret.Strategy,
		UpdateStrategy: secret.Strategy,
		DeleteStrategy: secret.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{
		RESTOptions: optsGetter,
		AttrFunc:    secret.GetAttrs,
	}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}
	return &REST{store}, nil
}
