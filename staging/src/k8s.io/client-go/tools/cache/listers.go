/*
Copyright 2014 The Kubernetes Authors.

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

package cache

import (
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// AppendFunc 回调接收每个匹配项；调用方可用它把结果追加到任意目标容器。
type AppendFunc func(interface{})

// ListAll 遍历 store，对每个匹配 selector 的对象调用 appendFn。
// 当 selector 匹配一切时跳过标签计算，这是列举全部对象时最常见的路径，
// 可避免对每个对象做反射式取值。
func ListAll(store Store, selector labels.Selector, appendFn AppendFunc) error {
	// 匹配不到任何对象的 selector 直接短路，省去遍历。
	if labels.MatchesNothing(selector) {
		return nil
	}

	selectAll := selector.Empty()
	for _, m := range store.List() {
		if selectAll {
			// 避免计算对象的标签，以加速“列举全部对象”这一常见路径。
			appendFn(m)
			continue
		}
		metadata, err := meta.Accessor(m)
		if err != nil {
			return err
		}
		if selector.Matches(labels.Set(metadata.GetLabels())) {
			appendFn(m)
		}
	}
	return nil
}

// ListAllByNamespace 遍历 store，对给定命名空间内每个匹配 selector 的对象调用 appendFn。
// 若指定了空白命名空间（NamespaceAll），则直接委托给 ListAll()。
//
// 优先通过 NamespaceIndex 索引缩小范围；若索引不可用则退化为全量遍历过滤。
func ListAllByNamespace(indexer Indexer, namespace string, selector labels.Selector, appendFn AppendFunc) error {
	if labels.MatchesNothing(selector) {
		return nil
	}

	if namespace == metav1.NamespaceAll {
		return ListAll(indexer, selector, appendFn)
	}

	items, err := indexer.Index(NamespaceIndex, &metav1.ObjectMeta{Namespace: namespace})
	if err != nil {
		// 忽略错误；改用无索引的慢速搜索。
		//
		// ListAllByNamespace 由生成代码（k8s.io/client-go/listers）调用，
		// 为此改成上下文日志需要改动整个生成 API 体系，得不偿失。
		//
		// 索引查询失败不影响正确性，退回全量遍历；此处保持无损日志以兼容生成代码。
		klog.TODO().Info("Warning: can not retrieve list of objects using index", "err", err)
		for _, m := range indexer.List() {
			metadata, err := meta.Accessor(m)
			if err != nil {
				return err
			}
			if metadata.GetNamespace() == namespace && selector.Matches(labels.Set(metadata.GetLabels())) {
				appendFn(m)
			}

		}
		return nil
	}

	selectAll := selector.Empty()
	for _, m := range items {
		if selectAll {
			// 避免计算对象的标签，以加速“列举全部对象”这一常见路径。
			appendFn(m)
			continue
		}
		metadata, err := meta.Accessor(m)
		if err != nil {
			return err
		}
		if selector.Matches(labels.Set(metadata.GetLabels())) {
			appendFn(m)
		}
	}

	return nil
}

// GenericLister 是建立在通用 Indexer 之上的 lister，只做类型擦除，
// 供无法生成类型化 lister 的场景（如动态客户端）使用。
type GenericLister interface {
	// List 返回跨所有命名空间中匹配 selector 的对象
	List(selector labels.Selector) (ret []runtime.Object, err error)
	// Get 假定 name 即存储 key，尝试直接获取对象
	Get(name string) (runtime.Object, error)
	// ByNamespace 返回指定命名空间的 GenericNamespaceLister
	ByNamespace(namespace string) GenericNamespaceLister
}

// GenericNamespaceLister 是建立在通用 Indexer 之上的单命名空间 lister。
type GenericNamespaceLister interface {
	// List 返回本命名空间内匹配 selector 的对象
	List(selector labels.Selector) (ret []runtime.Object, err error)
	// Get 按命名空间和名称尝试获取对象
	Get(name string) (runtime.Object, error)
}

// NewGenericLister 创建一个 genericLister 实例。
//
// resource 仅用于生成 NotFound 错误中的资源描述，不影响读取逻辑。
func NewGenericLister(indexer Indexer, resource schema.GroupResource) GenericLister {
	return &genericLister{indexer: indexer, resource: resource}
}

type genericLister struct {
	indexer  Indexer
	resource schema.GroupResource
}

// List 列举所有命名空间中匹配 selector 的对象。
func (s *genericLister) List(selector labels.Selector) (ret []runtime.Object, err error) {
	err = ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(runtime.Object))
	})
	return ret, err
}

// ByNamespace 返回指定命名空间视图。
func (s *genericLister) ByNamespace(namespace string) GenericNamespaceLister {
	return &genericNamespaceLister{indexer: s.indexer, namespace: namespace, resource: s.resource}
}

// Get 以 name 作为 key 直接查索引；这是集群级（非命名空间）对象的约定。
func (s *genericLister) Get(name string) (runtime.Object, error) {
	obj, exists, err := s.indexer.GetByKey(name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(s.resource, name)
	}
	return obj.(runtime.Object), nil
}

type genericNamespaceLister struct {
	indexer   Indexer
	namespace string
	resource  schema.GroupResource
}

// List 只列举本命名空间内匹配 selector 的对象。
func (s *genericNamespaceLister) List(selector labels.Selector) (ret []runtime.Object, err error) {
	err = ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(runtime.Object))
	})
	return ret, err
}

// Get 使用 "namespace/name" 作为索引 key，这是 MetaNamespaceKeyFunc 的默认格式。
func (s *genericNamespaceLister) Get(name string) (runtime.Object, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(s.resource, name)
	}
	return obj.(runtime.Object), nil
}
