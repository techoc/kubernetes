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

package listers

import (
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

// ResourceIndexer 封装 indexer、resource 以及可选的 namespace，仅供 lister-gen 生成的 Lister 使用。
//
// 泛型封装让 lister-gen 生成的代码可以复用同一套缓存访问逻辑，
// 同时保留 GroupResource 以便构造与 apiserver 一致的 NotFound 错误。
type ResourceIndexer[T runtime.Object] struct {
	indexer   cache.Indexer
	resource  schema.GroupResource
	namespace string // empty for non-namespaced types
}

// New 为指定类型创建包装给定 indexer 与 resource 的 ResourceIndexer，仅供生成的 Lister 使用。
func New[T runtime.Object](indexer cache.Indexer, resource schema.GroupResource) ResourceIndexer[T] {
	return ResourceIndexer[T]{indexer: indexer, resource: resource}
}

// NewNamespaced 基于父 ResourceIndexer 与指定 namespace 创建命名空间级实例，仅供生成的 Lister 使用。
//
// 复用父对象的 indexer/resource，只替换 namespace，避免重复持有缓存。
func NewNamespaced[T runtime.Object](parent ResourceIndexer[T], namespace string) ResourceIndexer[T] {
	return ResourceIndexer[T]{indexer: parent.indexer, resource: parent.resource, namespace: namespace}
}

// List 返回索引器中匹配给定 selector 的所有资源。
func (l ResourceIndexer[T]) List(selector labels.Selector) (ret []T, err error) {
	// namespace 为空时 ListAllByNamespace 退化为 ListAll。
	err = cache.ListAllByNamespace(l.indexer, l.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(T))
	})
	return ret, err
}

// Get 按名称从索引器中读取资源。
func (l ResourceIndexer[T]) Get(name string) (T, error) {
	// 索引 key 的格式必须与 cache.MetaNamespaceKeyFunc 一致：namespaced 为 ns/name，集群级为 name。
	var key string
	if l.namespace == "" {
		key = name
	} else {
		key = l.namespace + "/" + name
	}
	obj, exists, err := l.indexer.GetByKey(key)
	if err != nil {
		return *new(T), err
	}
	if !exists {
		// 返回带 GroupResource 的 NotFound，保持与直接调用 API server 时的错误语义一致。
		return *new(T), errors.NewNotFound(l.resource, name)
	}
	return obj.(T), nil
}
