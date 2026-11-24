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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/kubernetes/pkg/apis/networking"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/networking/ipaddress"
)

// REST 将 IPAddress 的 REST 请求映射到底层通用 Store，并通过 etcd 持久化。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建集群级 IPAddress Store，为 Service IP 分配器持久化地址所有权记录。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &networking.IPAddress{} },
		NewListFunc:               func() runtime.Object { return &networking.IPAddressList{} },
		DefaultQualifiedResource:  networking.Resource("ipaddresses"),
		SingularQualifiedResource: networking.Resource("ipaddress"),

		CreateStrategy: ipaddress.Strategy,
		UpdateStrategy: ipaddress.Strategy,
		DeleteStrategy: ipaddress.Strategy,

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

// ShortNames 返回 IPAddress 资源的 kubectl 简称列表。
func (r *REST) ShortNames() []string {
	return []string{"ip"}
}
