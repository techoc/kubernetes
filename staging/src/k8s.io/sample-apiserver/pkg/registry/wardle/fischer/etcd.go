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

package fischer

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/sample-apiserver/pkg/apis/wardle"
	"k8s.io/sample-apiserver/pkg/registry"
)

// NewREST returns a RESTStorage object that will work against API services.
// 用通用 Store 接上策略与后端选项，启动时的选项校验错误在这里就暴露出来。
func NewREST(scheme *runtime.Scheme, optsGetter generic.RESTOptionsGetter) (*registry.REST, error) {
	strategy := NewStrategy(scheme)

	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &wardle.Fischer{} },
		NewListFunc:               func() runtime.Object { return &wardle.FischerList{} },
		PredicateFunc:             MatchFischer,
		DefaultQualifiedResource:  wardle.Resource("fischers"),
		SingularQualifiedResource: wardle.Resource("fischer"),

		CreateStrategy: strategy,
		UpdateStrategy: strategy,
		DeleteStrategy: strategy,

		// TODO: define table converter that exposes more than name/creation timestamp
		// 默认 converter 只够 kubectl get 显示基本信息，够示例演示用。
		TableConvertor: rest.NewDefaultTableConvertor(wardle.Resource("fischers")),
	}
	// AttrFunc 让后端按标签和字段过滤，GET 列表不必先反序列化全部对象。
	options := &generic.StoreOptions{RESTOptions: optsGetter, AttrFunc: GetAttrs}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}
	return &registry.REST{Store: store}, nil
}
