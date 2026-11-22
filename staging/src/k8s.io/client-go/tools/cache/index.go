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

// Indexer extends Store with multiple indices and restricts each
// accumulator to simply hold the current object (and be empty after
// Delete).
//
// There are three kinds of strings here:
//  1. a storage key, as defined in the Store interface,
//  2. a name of an index, and
//  3. an "indexed value", which is produced by an IndexFunc and
//     can be a field value or any other string computed from the object.
//
// 索引与主存储保持同步：对象写入/删除时，对应的索引项也一并更新，
// 因此 List/ByIndex 的结果总是反映同一时刻的缓存快照。
type Indexer interface {
	Store
	// Index returns the stored objects whose set of indexed values
	// intersects the set of indexed values of the given object, for
	// the named index
	Index(indexName string, obj interface{}) ([]interface{}, error)
	// IndexKeys returns the storage keys of the stored objects whose
	// set of indexed values for the named index includes the given
	// indexed value
	IndexKeys(indexName, indexedValue string) ([]string, error)
	// ListIndexFuncValues returns all the indexed values of the given index
	ListIndexFuncValues(indexName string) []string
	// ByIndex returns the stored objects whose set of indexed values
	// for the named index includes the given indexed value
	ByIndex(indexName, indexedValue string) ([]interface{}, error)
	// GetIndexers return the indexers
	GetIndexers() Indexers

	// AddIndexers adds more indexers to this store. This supports adding indexes after the store already has items.
	AddIndexers(newIndexers Indexers) error
}

// IndexFunc knows how to compute the set of indexed values for an object.
//
// 一个对象可以对应多个索引值（例如多个标签），返回空切片表示不被索引。
type IndexFunc func(obj interface{}) ([]string, error)

// IndexFuncToKeyFuncAdapter adapts an indexFunc to a keyFunc.  This is only useful if your index function returns
// unique values for every object.  This conversion can create errors when more than one key is found.  You
// should prefer to make proper key and index functions.
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
	// NamespaceIndex is the lookup name for the most common index function, which is to index by the namespace field.
	NamespaceIndex string = "namespace"
)

// MetaNamespaceIndexFunc is a default index function that indexes based on an object's namespace
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

// Index maps the indexed value to a set of keys in the store that match on that value
type index map[string]sets.Set[string]

// Indexers maps a name to an IndexFunc
type Indexers map[string]IndexFunc

// Indices maps a name to an Index
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

// TypedIndexersToIndexers wraps several indexer functions which expect objects
// of a certain type so that they can be used in an [Indexer].
// This is needed primarily for setting up [SharedIndexInformerOptions.Indexers].
// When using one of the typed APIs for installing an indexer function
// the conversion is done automatically.
func TypedIndexersToIndexers[T any](indexers TypedIndexers[T]) Indexers {
	untyped := make(Indexers, len(indexers))
	for i, indexer := range indexers {
		untyped[i] = TypedIndexerFuncToIndexerFunc(indexer)
	}
	return untyped
}

// TypedIndexersFuncToIndexerFunc wraps one indexer function which expect objects
// of a certain type so that they can be used in an [Indexer].
// This is needed primarily for setting up [SharedIndexInformerOptions.Indexers].
// When using one of the typed APIs for installing an indexer function
// the conversion is done automatically.
//
// 这里的类型断言不做检查：调用方通过类型安全的 API 注册，
// 传入的对象类型由 informer 保证与 T 一致；类型不符会直接 panic，属于编程错误。
func TypedIndexerFuncToIndexerFunc[T any](indexer TypedIndexFunc[T]) IndexFunc {
	return func(obj any) ([]string, error) {
		return indexer(obj.(T))
	}
}
