/*
Copyright 2017 The Kubernetes Authors.

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

package flunder

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/sample-apiserver/pkg/apis/wardle"
	"k8s.io/sample-apiserver/pkg/registry"
)

// NewREST 创建基于 etcd 的 Flunder RESTStorage。
// 存储层只认内部类型，外部版本在 HTTP 层完成转换。
func NewREST(scheme *runtime.Scheme, optsGetter generic.RESTOptionsGetter) (*registry.REST, error) {
	// 同一份策略同时负责创建、更新和删除，校验规则不会因操作而异。
	strategy := NewStrategy(scheme)

	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &wardle.Flunder{} },
		NewListFunc:               func() runtime.Object { return &wardle.FlunderList{} },
		PredicateFunc:             MatchFlunder,
		DefaultQualifiedResource:  wardle.Resource("flunders"),
		SingularQualifiedResource: wardle.Resource("flunder"),

		CreateStrategy: strategy,
		UpdateStrategy: strategy,
		DeleteStrategy: strategy,

		// 默认转换器只给 name 和创建时间，示例够用，要加列就替换掉它。
		// TODO：定义表格转换器，暴露名称和创建时间之外的字段。
		TableConvertor: rest.NewDefaultTableConvertor(wardle.Resource("flunders")),
	}
	// AttrFunc 提供命名空间/名称属性，供策略判断和索引使用。
	options := &generic.StoreOptions{RESTOptions: optsGetter, AttrFunc: GetAttrs}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}
	// 用公共 REST 包装层承载 Store，新增资源时只需换掉策略与类型。
	return &registry.REST{Store: store}, nil
}
