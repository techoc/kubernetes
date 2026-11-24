/*
Copyright 2020 The Kubernetes Authors.

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

// 本文件构造 csistoragecapacity 的 REST Store，将资源 Strategy、对象类型和通用存储后端连接起来。
package storage

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	storageapi "k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/registry/storage/csistoragecapacity"
)

// CSIStorageCapacityStorage 汇总 CSIStorageCapacity 主资源及其子资源的 REST 存储。
type CSIStorageCapacityStorage struct {
	CSIStorageCapacity *REST
}

// REST 将 CSIStorageCapacity 的 REST 请求映射到底层通用 Store。
type REST struct {
	*genericregistry.Store
}

// NewStorage 创建命名空间级 CSIStorageCapacity Store，并配置资源策略和默认表格转换器。
func NewStorage(optsGetter generic.RESTOptionsGetter) (*CSIStorageCapacityStorage, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &storageapi.CSIStorageCapacity{} },
		NewListFunc:               func() runtime.Object { return &storageapi.CSIStorageCapacityList{} },
		DefaultQualifiedResource:  storageapi.Resource("csistoragecapacities"),
		SingularQualifiedResource: storageapi.Resource("csistoragecapacity"),

		TableConvertor: rest.NewDefaultTableConvertor(storageapi.Resource("csistoragecapacities")),

		CreateStrategy: csistoragecapacity.Strategy,
		UpdateStrategy: csistoragecapacity.Strategy,
		DeleteStrategy: csistoragecapacity.Strategy,
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}

	return &CSIStorageCapacityStorage{
		CSIStorageCapacity: &REST{store},
	}, nil
}
