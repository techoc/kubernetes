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

// shim.go 把 metadatalister 的类型化 lister 适配为 informer 基础设施所需的
// cache.GenericLister；包级说明见 lister.go。

package metadatalister

import (
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

// 编译期断言：两个 shim 分别实现对应的 generic 接口。
var _ cache.GenericLister = &metadataListerShim{}
var _ cache.GenericNamespaceLister = &metadataNamespaceListerShim{}

// metadataListerShim 实现 cache.GenericLister 接口。
type metadataListerShim struct {
	lister Lister
}

// NewRuntimeObjectShim 为 Lister 创建适配器，使其实现 cache.GenericLister 接口。
func NewRuntimeObjectShim(lister Lister) cache.GenericLister {
	return &metadataListerShim{lister: lister}
}

// List 返回所有命名空间中的对象。
func (s *metadataListerShim) List(selector labels.Selector) (ret []runtime.Object, err error) {
	objs, err := s.lister.List(selector)
	if err != nil {
		return nil, err
	}

	// 逐元素拷贝到 runtime.Object 切片；这里不是原地转换，
	// 因为切片元素类型不同，必须新建切片。
	ret = make([]runtime.Object, len(objs))
	for index, obj := range objs {
		ret[index] = obj
	}
	return ret, err
}

// Get 假定 name 等于 key，据此尝试读取对象。
func (s *metadataListerShim) Get(name string) (runtime.Object, error) {
	return s.lister.Get(name)
}

// ByNamespace 返回绑定到指定命名空间的 generic lister 适配器。
func (s *metadataListerShim) ByNamespace(namespace string) cache.GenericNamespaceLister {
	return &metadataNamespaceListerShim{
		namespaceLister: s.lister.Namespace(namespace),
	}
}

// metadataNamespaceListerShim 包装 NamespaceLister，使其实现 cache.GenericNamespaceLister 接口。
type metadataNamespaceListerShim struct {
	namespaceLister NamespaceLister
}

// List 返回该命名空间中的所有对象。
func (ns *metadataNamespaceListerShim) List(selector labels.Selector) (ret []runtime.Object, err error) {
	objs, err := ns.namespaceLister.List(selector)
	if err != nil {
		return nil, err
	}

	// 同 List：因元素类型不同而必须新建切片。
	ret = make([]runtime.Object, len(objs))
	for index, obj := range objs {
		ret[index] = obj
	}
	return ret, err
}

// Get 按命名空间与名称读取对象。
func (ns *metadataNamespaceListerShim) Get(name string) (runtime.Object, error) {
	return ns.namespaceLister.Get(name)
}
