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

// Package metadatalister 为仅需对象元数据（PartialObjectMetadata）的场景
// 提供与 typed lister 相同的 List/Get 语义。其数据来自 metadata informer，
// 不反序列化完整对象，因而内存占用与解码开销都更小。
package metadatalister

import (
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

// 编译期断言，确保实现与接口同步演进。
var _ Lister = &metadataLister{}
var _ NamespaceLister = &metadataNamespaceLister{}

// metadataLister 实现 Lister 接口。
//
// gvr 只用于构造 NotFound 错误，使错误信息中的资源名与请求侧一致。
type metadataLister struct {
	indexer cache.Indexer
	gvr     schema.GroupVersionResource
}

// New 创建新的 Lister。
//
// indexer 应由 metadata informer 提供；Lister 本身不加锁，依赖 Indexer 的并发安全。
func New(indexer cache.Indexer, gvr schema.GroupVersionResource) Lister {
	return &metadataLister{indexer: indexer, gvr: gvr}
}

// List 列出 indexer 中的全部资源。
//
// 返回的切片仅包含匹配对象，顺序由 Indexer 遍历顺序决定，不保证稳定。
func (l *metadataLister) List(selector labels.Selector) (ret []*metav1.PartialObjectMetadata, err error) {
	err = cache.ListAll(l.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*metav1.PartialObjectMetadata))
	})
	return ret, err
}

// Get 按给定名称从 indexer 中取资源。
//
// 缓存未命中时返回 API 风格的 NotFound 错误，而非通用 error。
func (l *metadataLister) Get(name string) (*metav1.PartialObjectMetadata, error) {
	obj, exists, err := l.indexer.GetByKey(name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(l.gvr.GroupResource(), name)
	}
	return obj.(*metav1.PartialObjectMetadata), nil
}

// Namespace 返回可列出和读取指定 namespace 资源的对象。
//
// namespace lister 只是携带 namespace 的轻量视图，不复制底层缓存。
func (l *metadataLister) Namespace(namespace string) NamespaceLister {
	return &metadataNamespaceLister{indexer: l.indexer, namespace: namespace, gvr: l.gvr}
}

// metadataNamespaceLister 实现 NamespaceLister 接口。
type metadataNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
	gvr       schema.GroupVersionResource
}

// List 列出 indexer 中指定 namespace 的全部资源。
func (l *metadataNamespaceLister) List(selector labels.Selector) (ret []*metav1.PartialObjectMetadata, err error) {
	err = cache.ListAllByNamespace(l.indexer, l.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*metav1.PartialObjectMetadata))
	})
	return ret, err
}

// Get 按 namespace 与 name 从 indexer 中取资源。
func (l *metadataNamespaceLister) Get(name string) (*metav1.PartialObjectMetadata, error) {
	// Indexer 的 key 约定是 "namespace/name"；集群级对象则仅用 name。
	obj, exists, err := l.indexer.GetByKey(l.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(l.gvr.GroupResource(), name)
	}
	return obj.(*metav1.PartialObjectMetadata), nil
}
