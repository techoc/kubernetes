/*
Copyright 2014 The Kubernetes Authors.

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
	"k8s.io/kubernetes/pkg/registry/core/podtemplate"
)

// REST 将 PodTemplate 通用 CRUD Store 封装为 API Server RESTStorage。
type REST struct {
	*genericregistry.Store
}

// NewREST returns a RESTStorage object that will work against pod templates.
// NewREST 注册 PodTemplate 对象构造器、资源名、Strategy 与表格输出器，并初始化底层持久化 Store。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &api.PodTemplate{} },
		NewListFunc:               func() runtime.Object { return &api.PodTemplateList{} },
		DefaultQualifiedResource:  api.Resource("podtemplates"),
		SingularQualifiedResource: api.Resource("podtemplate"),

		CreateStrategy: podtemplate.Strategy,
		UpdateStrategy: podtemplate.Strategy,
		DeleteStrategy: podtemplate.Strategy,

		ReturnDeletedObject: true,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}
	return &REST{store}, nil
}
