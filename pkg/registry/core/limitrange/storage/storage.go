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
	"k8s.io/apiserver/pkg/registry/rest"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/registry/core/limitrange"
)

// REST 将 LimitRange 的通用 CRUD Store 暴露为 API Server RESTStorage。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建并返回用于访问 LimitRange 资源的 RESTStorage。
// NewREST 注册 LimitRange 对象构造器、资源名、Strategy 和默认表格转换器，并完成后端存储初始化。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &api.LimitRange{} },
		NewListFunc:               func() runtime.Object { return &api.LimitRangeList{} },
		DefaultQualifiedResource:  api.Resource("limitranges"),
		SingularQualifiedResource: api.Resource("limitrange"),

		CreateStrategy: limitrange.Strategy,
		UpdateStrategy: limitrange.Strategy,
		DeleteStrategy: limitrange.Strategy,

		// TODO：定义更完整的表格转换器，除名称和创建时间外，还应展示更多 LimitRange 信息。
		TableConvertor: rest.NewDefaultTableConvertor(api.Resource("limitranges")),
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}
	return &REST{store}, nil
}

// 实现 ShortNamesProvider 接口。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 实现 ShortNamesProvider，返回 LimitRange 资源支持的简称列表。
func (r *REST) ShortNames() []string {
	return []string{"limits"}
}
