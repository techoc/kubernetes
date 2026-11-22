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

package metadatalister

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// Lister helps list resources.
//
// Lister 面向只关心对象元数据的场景，返回 PartialObjectMetadata，避免把完整
// 对象解码进内存。所有方法都只读本地缓存，不会访问 API Server。
type Lister interface {
	// List lists all resources in the indexer.
	//
	// List 返回当前缓存中匹配 selector 的全部对象快照；结果可能略滞后于
	// 服务端，消费方需接受最终一致性。
	List(selector labels.Selector) (ret []*metav1.PartialObjectMetadata, err error)
	// Get retrieves a resource from the indexer with the given name
	//
	// 未命中时返回 apierrors.NewNotFound 风格的错误，而不是 nil, nil。
	Get(name string) (*metav1.PartialObjectMetadata, error)
	// Namespace returns an object that can list and get resources in a given namespace.
	Namespace(namespace string) NamespaceLister
}

// NamespaceLister helps list and get resources.
//
// 把命名空间作为固定的查询维度，避免调用方每次都传 namespace 参数。
type NamespaceLister interface {
	// List lists all resources in the indexer for a given namespace.
	List(selector labels.Selector) (ret []*metav1.PartialObjectMetadata, err error)
	// Get retrieves a resource from the indexer for a given namespace and name.
	//
	// 只在该命名空间内查找；跨命名空间同名对象不会被返回。
	Get(name string) (*metav1.PartialObjectMetadata, error)
}
