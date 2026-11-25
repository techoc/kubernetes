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
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/sets"
)

// Indexer 在 Store 之上支持多套索引，并限定每个索引桶只保存当前对象
// （Delete 之后对应条目为空）。
//
// 这里涉及三类字符串：
//  1. 存储键，其定义见 Store 接口；
//  2. 索引名；
//  3. “索引值”，由 IndexFunc 产生，可以是字段值或从对象计算出的任意字符串。
//
// 索引与主存储保持同步：对象写入/删除时，对应的索引项也一并更新，
// 因此 List/ByIndex 的结果总是反映同一时刻的缓存快照。
type Indexer interface {
	Store
	// Index 在指定索引下，返回索引值集合与给定对象索引值集合有交集的已存储对象。
	Index(indexName string, obj interface{}) ([]interface{}, error)
	// IndexKeys 在指定索引下，返回包含给定索引值的已存储对象的存储键。
	IndexKeys(indexName, indexedValue string) ([]string, error)
	// ListIndexFuncValues 返回给定索引的全部索引值。
	ListIndexFuncValues(indexName string) []string
	// ByIndex 在指定索引下，返回包含给定索引值的已存储对象。
	ByIndex(indexName, indexedValue string) ([]interface{}, error)
	// GetIndexers 返回当前已注册的索引函数表。
	GetIndexers() Indexers

	// AddIndexers 向该存储追加索引器。支持在存储已有对象之后再添加索引。
	AddIndexers(newIndexers Indexers) error
}

// IndexFunc 负责计算一个对象对应的索引值集合。
//
// 一个对象可以对应多个索引值（例如多个标签），返回空切片表示不被索引。
type IndexFunc func(obj interface{}) ([]string, error)

// IndexFuncToKeyFuncAdapter 把 indexFunc 适配成 keyFunc。仅当索引函数对每个对象
// 返回唯一值时才有意义；若找出多个键，这个转换会报错。更推荐分别编写
// 合适的键函数与索引函数。
func IndexFuncToKeyFuncAdapter(indexFunc IndexFunc) KeyFunc {
	return func(obj interface{}) (string, error) {
		indexKeys, err := indexFunc(obj)
		if err != nil {
			return "", err
		}
		// 键函数必须唯一，因此索引值多于一个直接报错，而不是随意取一个。
		if len(indexKeys) > 1 {
			return "", fmt.Errorf("too many keys: %v", indexKeys)
		}
		if len(indexKeys) == 0 {
			return "", fmt.Errorf("unexpected empty indexKeys")
		}
		return indexKeys[0], nil
	}
}

const (
	// NamespaceIndex 是最常用索引函数的查找名，即按 namespace 字段建索引。
	NamespaceIndex string = "namespace"
)

// MetaNamespaceIndexFunc 是默认索引函数，按对象的 namespace 建立索引。
//
// 无法获取 ObjectMeta 时返回空 namespace 并附带错误信息，
// 让调用方能够定位是哪个对象不满足索引前提。
func MetaNamespaceIndexFunc(obj interface{}) ([]string, error) {
	meta, err := meta.Accessor(obj)
	if err != nil {
		return []string{""}, fmt.Errorf("object has no meta: %v", err)
	}
	return []string{meta.GetNamespace()}, nil
}

// index 把索引值映射到存储中匹配该值的键集合。
type index map[string]sets.Set[string]

// Indexers 把索引名映射到 IndexFunc。
type Indexers map[string]IndexFunc

// Indices 把索引名映射到具体的索引内容。
type Indices map[string]index

// TypedIndexers 是类型安全的索引函数表，键为索引名。
type TypedIndexers[T any] map[string]TypedIndexFunc[T]

// TypedIndexFunc 是类型安全的 IndexFunc，参数已确定为 T，内部无需再做断言。
type TypedIndexFunc[T any] func(obj T) ([]string, error)

// TypedIndexer 在普通 Indexer 之上提供返回 []T 的便捷方法。
// 底层仍是同一份索引，只是省去调用方反复做类型断言。
type TypedIndexer[T any] interface {
	Indexer

	TypedIndex(indexName string, obj T) ([]T, error)
	ByTypedIndex(indexName, indexedValue string) ([]T, error)
	AddTypedIndexers(newIndexers TypedIndexers[T]) error
}

// TypedIndexersToIndexers 把一组只接受特定类型对象的索引函数包装成可用于
// [Indexer] 的普通 Indexers。主要用于配置 [SharedIndexInformerOptions.Indexers]。
// 若使用类型安全的 API 注册索引函数，这层转换会自动完成。
func TypedIndexersToIndexers[T any](indexers TypedIndexers[T]) Indexers {
	untyped := make(Indexers, len(indexers))
	for i, indexer := range indexers {
		untyped[i] = TypedIndexerFuncToIndexerFunc(indexer)
	}
	return untyped
}

// TypedIndexerFuncToIndexerFunc 把一个只接受特定类型对象的索引函数包装成可用于
// [Indexer] 的普通 IndexFunc。主要用于配置 [SharedIndexInformerOptions.Indexers]。
// 若使用类型安全的 API 注册索引函数，这层转换会自动完成。
//
// 这里的类型断言不做检查：调用方通过类型安全的 API 注册，
// 传入的对象类型由 informer 保证与 T 一致；类型不符会直接 panic，属于编程错误。
func TypedIndexerFuncToIndexerFunc[T any](indexer TypedIndexFunc[T]) IndexFunc {
	return func(obj any) ([]string, error) {
		return indexer(obj.(T))
	}
}
