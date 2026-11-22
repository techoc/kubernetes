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

package generic

import (
	"fmt"
	"net/http"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

var _ Lister[runtime.Object] = lister[runtime.Object]{}

// namespacedLister 是限定到单个 namespace 的只读视图，底层仍复用同一个 indexer，
// 因此不会额外占用缓存，也无需与全量 lister 保持同步。
type namespacedLister[T runtime.Object] struct {
	indexer   cache.Indexer
	namespace string
}

// List 返回该 namespace 下匹配 selector 的全部对象。
func (w namespacedLister[T]) List(selector labels.Selector) (ret []T, err error) {
	err = cache.ListAllByNamespace(w.indexer, w.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(T))
	})
	return ret, err
}

// Get 按 "namespace/name" 键查找对象；未找到时返回 NotFound 形态的 StatusError。
func (w namespacedLister[T]) Get(name string) (T, error) {
	var result T

	obj, exists, err := w.indexer.GetByKey(w.namespace + "/" + name)
	if err != nil {
		return result, err
	}
	if !exists {
		return result, &kerrors.StatusError{ErrStatus: metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    http.StatusNotFound,
			Reason:  metav1.StatusReasonNotFound,
			Message: fmt.Sprintf("%s not found", name),
		}}
	}
	result = obj.(T)
	return result, nil
}

// lister 是全量（跨 namespace）的只读视图，直接包装 informer 的 indexer。
type lister[T runtime.Object] struct {
	indexer cache.Indexer
}

// List 返回 indexer 中匹配 selector 的全部对象。
func (w lister[T]) List(selector labels.Selector) (ret []T, err error) {
	err = cache.ListAll(w.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(T))
	})
	return ret, err
}

// Get 按对象键查找；非命名空间资源使用 name，命名空间资源需调用方传入 "ns/name"。
func (w lister[T]) Get(name string) (T, error) {
	var result T

	obj, exists, err := w.indexer.GetByKey(name)
	if err != nil {
		return result, err
	}
	if !exists {
		// kerrors.StatusNotFound requires a GroupResource we cannot provide
		return result, &kerrors.StatusError{ErrStatus: metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    http.StatusNotFound,
			Reason:  metav1.StatusReasonNotFound,
			Message: fmt.Sprintf("%s not found", name),
		}}
	}
	result = obj.(T)
	return result, nil
}

// Namespaced 返回限定到给定 namespace 的视图，共享底层 indexer。
func (w lister[T]) Namespaced(namespace string) NamespacedLister[T] {
	return namespacedLister[T]{namespace: namespace, indexer: w.indexer}
}

// NewLister 基于已有的 informer indexer 构造类型安全的 lister。
// 调用方必须保证 indexer 中缓存的对象都能断言为 T，否则 List/Get 会 panic。
func NewLister[T runtime.Object](indexer cache.Indexer) lister[T] {
	return lister[T]{indexer: indexer}
}
