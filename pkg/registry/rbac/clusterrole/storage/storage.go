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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/kubernetes/pkg/apis/rbac"
	"k8s.io/kubernetes/pkg/registry/rbac/clusterrole"
)

// REST 将 ClusterRole 的 REST 请求映射到底层通用 Store。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建集群级 ClusterRole Store，注册对应的资源策略和表格输出转换器。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &rbac.ClusterRole{} },
		NewListFunc:               func() runtime.Object { return &rbac.ClusterRoleList{} },
		DefaultQualifiedResource:  rbac.Resource("clusterroles"),
		SingularQualifiedResource: rbac.Resource("clusterrole"),

		CreateStrategy: clusterrole.Strategy,
		UpdateStrategy: clusterrole.Strategy,
		DeleteStrategy: clusterrole.Strategy,

		// TODO：考虑实现包含名称和创建时间以外更多字段的表格转换器。
		TableConvertor: rest.NewDefaultTableConvertor(rbac.Resource("clusterroles")),
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}

	return &REST{store}, nil
}
