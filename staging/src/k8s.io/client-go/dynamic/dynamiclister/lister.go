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

// Package dynamiclister 为 unstructured 资源提供只读的 Lister 视图。
//
// Lister 包装一个共享的 cache.Indexer（通常由 dynamicinformer 维护），
// 读取的是 informer 本地缓存而非 API Server，因此不产生网络往返；
// 返回的对象由调用方只读使用，修改它们会污染共享缓存。
package dynamiclister

import (
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

// 编译期断言：两个实现都满足对应接口。
var _ Lister = &dynamicLister{}
var _ NamespaceLister = &dynamicNamespaceLister{}

// dynamicLister 是 Lister 接口的实现，按 GVR 维度封装索引器。
type dynamicLister struct {
	indexer cache.Indexer
	// gvr 用于在对象缺失时构造带资源信息的 NotFound 错误。
	gvr schema.GroupVersionResource
}

// New 基于给定的索引器与 GVR 构造 Lister。
//
// indexer 必须与 gvr 对应的是同一类对象，否则类型断言会 panic。
func New(indexer cache.Indexer, gvr schema.GroupVersionResource) Lister {
	return &dynamicLister{indexer: indexer, gvr: gvr}
}

// List 返回索引器中匹配 selector 的全部资源。
func (l *dynamicLister) List(selector labels.Selector) (ret []*unstructured.Unstructured, err error) {
	// ListAll 会自行处理 nil selector 与索引器错误；这里只做类型断言收集。
	err = cache.ListAll(l.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*unstructured.Unstructured))
	})
	return ret, err
}

// Get 按名称从索引器取回单个资源。
func (l *dynamicLister) Get(name string) (*unstructured.Unstructured, error) {
	obj, exists, err := l.indexer.GetByKey(name)
	if err != nil {
		return nil, err
	}
	if !exists {
		// 与 typed lister 保持一致：缓存未命中返回标准 NotFound 错误，
		// 以便调用方用 apierrors.IsNotFound 统一判断。
		return nil, errors.NewNotFound(l.gvr.GroupResource(), name)
	}
	return obj.(*unstructured.Unstructured), nil
}

// Namespace 返回限定到指定命名空间的 lister 视图。
func (l *dynamicLister) Namespace(namespace string) NamespaceLister {
	return &dynamicNamespaceLister{indexer: l.indexer, namespace: namespace, gvr: l.gvr}
}

// dynamicNamespaceLister 是 NamespaceLister 接口的实现，复用底层索引器。
type dynamicNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
	gvr       schema.GroupVersionResource
}

// List 返回索引器中指定命名空间内匹配 selector 的全部资源。
func (l *dynamicNamespaceLister) List(selector labels.Selector) (ret []*unstructured.Unstructured, err error) {
	err = cache.ListAllByNamespace(l.indexer, l.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*unstructured.Unstructured))
	})
	return ret, err
}

// Get 按名称从索引器取回指定命名空间内的单个资源。
func (l *dynamicNamespaceLister) Get(name string) (*unstructured.Unstructured, error) {
	// 索引器的 key 约定为 "namespace/name"，命名空间 lister 依赖这一布局。
	obj, exists, err := l.indexer.GetByKey(l.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(l.gvr.GroupResource(), name)
	}
	return obj.(*unstructured.Unstructured), nil
}
