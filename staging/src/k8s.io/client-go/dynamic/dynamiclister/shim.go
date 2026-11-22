/*
Copyright 2018 The Kubernetes Authors.

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

package dynamiclister

import (
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

// 编译期断言：两个 shim 必须分别满足 cache 包中的通用 Lister 接口。
// 这样 dynamic 客户端也能复用那些只依赖 GenericLister 的组件（控制器、informer 等）。
var _ cache.GenericLister = &dynamicListerShim{}
var _ cache.GenericNamespaceLister = &dynamicNamespaceListerShim{}

// dynamicListerShim implements the cache.GenericLister interface.
//
// 它把返回 dynamic.Object 的 Lister 适配成返回 runtime.Object 的通用接口；
// 适配只需转换切片元素类型，不复制对象本身，因此开销很小。
type dynamicListerShim struct {
	// lister 是底层基于 dynamic 对象的 Lister，所有读取操作都委托给它。
	lister Lister
}

// NewRuntimeObjectShim returns a new shim for Lister.
// It wraps Lister so that it implements cache.GenericLister interface
//
// 返回的接口背后仍是传入的 lister，调用方不应假设存在额外的缓存层。
func NewRuntimeObjectShim(lister Lister) cache.GenericLister {
	return &dynamicListerShim{lister: lister}
}

// List will return all objects across namespaces
//
// 返回的是 runtime.Object 切片；元素与底层 dynamic 对象共享同一份数据。
func (s *dynamicListerShim) List(selector labels.Selector) (ret []runtime.Object, err error) {
	objs, err := s.lister.List(selector)
	if err != nil {
		return nil, err
	}

	// 逐元素做接口转换，无法整片类型转换，因此需要显式分配结果切片。
	ret = make([]runtime.Object, len(objs))
	for index, obj := range objs {
		ret[index] = obj
	}
	return ret, err
}

// Get will attempt to retrieve assuming that name==key
//
// 这里沿用 dynamic Lister 的约定：name 参数同时充当缓存键。
func (s *dynamicListerShim) Get(name string) (runtime.Object, error) {
	return s.lister.Get(name)
}

// ByNamespace 返回限定到单个 namespace 的视图。
//
// 底层的 NamespaceLister 在每次调用时通过 s.lister.Namespace 获取，
// 这样 shim 不需要额外保存 namespace 状态。
func (s *dynamicListerShim) ByNamespace(namespace string) cache.GenericNamespaceLister {
	return &dynamicNamespaceListerShim{
		namespaceLister: s.lister.Namespace(namespace),
	}
}

// dynamicNamespaceListerShim implements the NamespaceLister interface.
// It wraps NamespaceLister so that it implements cache.GenericNamespaceLister interface
//
// 与 dynamicListerShim 类似，只是把操作范围收窄到一个 namespace。
type dynamicNamespaceListerShim struct {
	// namespaceLister 是底层已限定 namespace 的 Lister。
	namespaceLister NamespaceLister
}

// List will return all objects in this namespace
func (ns *dynamicNamespaceListerShim) List(selector labels.Selector) (ret []runtime.Object, err error) {
	objs, err := ns.namespaceLister.List(selector)
	if err != nil {
		return nil, err
	}

	// 同 dynamicListerShim.List：仅做元素类型转换，不复制对象。
	ret = make([]runtime.Object, len(objs))
	for index, obj := range objs {
		ret[index] = obj
	}
	return ret, err
}

// Get will attempt to retrieve by namespace and name
//
// namespace 由构造时的 NamespaceLister 决定，这里只需给出 name。
func (ns *dynamicNamespaceListerShim) Get(name string) (runtime.Object, error) {
	return ns.namespaceLister.Get(name)
}
