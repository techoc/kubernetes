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

// 本文件构造 csidriver 的 REST Store，将资源 Strategy、对象类型和通用存储后端连接起来。
package storage

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	storageapi "k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/storage/csidriver"
)

// CSIDriverStorage 汇总 CSIDriver 主资源及其子资源的 REST 存储。
type CSIDriverStorage struct {
	CSIDriver *REST
}

// REST 将 CSIDriver 的 REST 请求映射到底层通用 Store。
type REST struct {
	*genericregistry.Store
}

// NewStorage 创建 CSIDriver 的 REST Store，并配置资源策略和表格输出转换器。
func NewStorage(optsGetter generic.RESTOptionsGetter) (*CSIDriverStorage, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &storageapi.CSIDriver{} },
		NewListFunc:               func() runtime.Object { return &storageapi.CSIDriverList{} },
		DefaultQualifiedResource:  storageapi.Resource("csidrivers"),
		SingularQualifiedResource: storageapi.Resource("csidriver"),

		CreateStrategy:      csidriver.Strategy,
		UpdateStrategy:      csidriver.Strategy,
		DeleteStrategy:      csidriver.Strategy,
		ReturnDeletedObject: true,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}

	return &CSIDriverStorage{
		CSIDriver: &REST{store},
	}, nil
}
