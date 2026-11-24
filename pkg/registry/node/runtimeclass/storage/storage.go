/*
Copyright 2019 The Kubernetes Authors.

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
	"k8s.io/kubernetes/pkg/apis/node"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/node/runtimeclass"
)

// REST 将 RuntimeClass 的 REST 请求映射到底层通用 Store，并通过 etcd 持久化。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建 RuntimeClass 的 REST 存储，配置对象名称提取函数、资源策略和表格输出转换器。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
	store := &genericregistry.Store{
		NewFunc:     func() runtime.Object { return &node.RuntimeClass{} },
		NewListFunc: func() runtime.Object { return &node.RuntimeClassList{} },
		ObjectNameFunc: func(obj runtime.Object) (string, error) {
			return obj.(*node.RuntimeClass).Name, nil
		},
		DefaultQualifiedResource:  node.Resource("runtimeclasses"),
		SingularQualifiedResource: node.Resource("runtimeclass"),

		CreateStrategy: runtimeclass.Strategy,
		UpdateStrategy: runtimeclass.Strategy,
		DeleteStrategy: runtimeclass.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}
	return &REST{store}, nil
}
