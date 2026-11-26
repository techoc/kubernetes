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

// Lister 用于列举资源。
//
// Lister 面向只关心对象元数据的场景，返回 PartialObjectMetadata，避免把完整
// 对象解码进内存。所有方法都只读本地缓存，不会访问 API Server。
type Lister interface {
	// List 返回索引器中的所有资源。
	//
	// List 返回当前缓存中匹配 selector 的全部对象快照；结果可能略滞后于
	// 服务端，消费方需接受最终一致性。
	List(selector labels.Selector) (ret []*metav1.PartialObjectMetadata, err error)
	// Get 按名称从索引器中读取资源。
	//
	// 未命中时返回 apierrors.NewNotFound 风格的错误，而不是 nil, nil。
	Get(name string) (*metav1.PartialObjectMetadata, error)
	// Namespace 返回可列举、获取指定命名空间资源的对象。
	Namespace(namespace string) NamespaceLister
}

// NamespaceLister 用于列举和获取单个命名空间内的资源。
//
// 把命名空间作为固定的查询维度，避免调用方每次都传 namespace 参数。
type NamespaceLister interface {
	// List 返回索引器中指定命名空间的所有资源。
	List(selector labels.Selector) (ret []*metav1.PartialObjectMetadata, err error)
	// Get 按命名空间与名称从索引器读取资源。
	//
	// 只在该命名空间内查找；跨命名空间同名对象不会被返回。
	Get(name string) (*metav1.PartialObjectMetadata, error)
}
