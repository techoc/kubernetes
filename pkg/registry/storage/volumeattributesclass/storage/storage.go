/*
Copyright 2023 The Kubernetes Authors.

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

// 本文件构造 volumeattributesclass 的 REST Store，将资源 Strategy、对象类型和通用存储后端连接起来。
package storage

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	storageapi "k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/storage/volumeattributesclass"
)

// REST 将 VolumeAttributesClass 的 REST 请求映射到底层通用 Store。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建集群级 VolumeAttributesClass Store，并配置资源策略和表格输出转换器。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &storageapi.VolumeAttributesClass{} },
		NewListFunc:               func() runtime.Object { return &storageapi.VolumeAttributesClassList{} },
		DefaultQualifiedResource:  storageapi.Resource("volumeattributesclasses"),
		SingularQualifiedResource: storageapi.Resource("volumeattributesclass"),

		CreateStrategy:      volumeattributesclass.Strategy,
		UpdateStrategy:      volumeattributesclass.Strategy,
		DeleteStrategy:      volumeattributesclass.Strategy,
		ReturnDeletedObject: true,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}

	return &REST{store}, nil
}

// 声明 REST 实现 ShortNamesProvider 接口。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 返回 VolumeAttributesClass 资源的 kubectl 简称 vac。
func (r *REST) ShortNames() []string {
	return []string{"vac"}
}
