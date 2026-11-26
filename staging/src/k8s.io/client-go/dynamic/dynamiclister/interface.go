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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
)

// Lister 是动态客户端缓存的只读视图：从共享索引器读取 unstructured 对象，
// 不发起任何服务端请求。对象以指针形式返回，调用方应视其为只读。
type Lister interface {
	// List 返回索引器中所有匹配 selector 的资源；selector 为 nil 表示不筛选。
	List(selector labels.Selector) (ret []*unstructured.Unstructured, err error)
	// Get 按名称从索引器读取资源；若不存在返回错误。
	Get(name string) (*unstructured.Unstructured, error)
	// Namespace 返回绑定到指定命名空间的 lister 视图。
	Namespace(namespace string) NamespaceLister
}

// NamespaceLister 是对单个命名空间进行列举与获取的只读接口。
type NamespaceLister interface {
	// List 返回索引器中指定命名空间下的所有资源。
	List(selector labels.Selector) (ret []*unstructured.Unstructured, err error)
	// Get 按命名空间与名称从索引器读取资源。
	Get(name string) (*unstructured.Unstructured, error)
}
