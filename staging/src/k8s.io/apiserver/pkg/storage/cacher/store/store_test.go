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

package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

// TestStoreSingleKey 对 cache.Indexer 与 btree 两种实现运行同一组基本操作，
// 确保二者在 Add/Update/Replace/Delete 语义上完全一致（btree 是性能优化替代品）。
func TestStoreSingleKey(t *testing.T) {
	t.Run("cache.Indexer", func(t *testing.T) {
		store := NewIndexer(testStoreIndexers())
		testStoreSingleKey(t, store)
	})
	t.Run("btree", func(t *testing.T) {
		store := newThreadedBtreeStoreIndexer(ElementIndexers(testStoreIndexers()), btreeDegree)
		testStoreSingleKey(t, store)
	})
}

// testStoreSingleKey 覆盖单 key 的生命周期：新增、更新（含相同值不同 RV）、
// Replace 全量替换，以及重复删除（幂等，不返回 NotFound）。
func testStoreSingleKey(t *testing.T, store Indexer) {
	assertStoreEmpty(t, store, "foo")

	require.NoError(t, store.Add(testStorageElement("foo", "bar", 1)))
	assertStoreSingleKey(t, store, "foo", "bar", 1)

	require.NoError(t, store.Update(testStorageElement("foo", "baz", 2)))
	assertStoreSingleKey(t, store, "foo", "baz", 2)

	require.NoError(t, store.Update(testStorageElement("foo", "baz", 3)))
	assertStoreSingleKey(t, store, "foo", "baz", 3)

	require.NoError(t, store.Replace([]interface{}{testStorageElement("foo", "bar", 4)}, ""))
	assertStoreSingleKey(t, store, "foo", "bar", 4)

	require.NoError(t, store.Delete(testStorageElement("foo", "", 0)))
	assertStoreEmpty(t, store, "foo")

	require.NoError(t, store.Delete(testStorageElement("foo", "", 0)))
}

// TestStoreIndexerSingleKey 验证二级索引在对象更新时会被正确维护：
// 值变化后旧索引项必须消失、新索引项必须出现，Replace/Delete 后索引同样被清理。
func TestStoreIndexerSingleKey(t *testing.T) {
	t.Run("cache.Indexer", func(t *testing.T) {
		store := NewIndexer(testStoreIndexers())
		testStoreIndexerSingleKey(t, store)
	})
	t.Run("btree", func(t *testing.T) {
		store := newThreadedBtreeStoreIndexer(ElementIndexers(testStoreIndexers()), btreeDegree)
		testStoreIndexerSingleKey(t, store)
	})
}

// testStoreIndexerSingleKey 通过 by_val 索引观察对象在 store 中的可见性变化。
func testStoreIndexerSingleKey(t *testing.T, store Indexer) {
	items, err := store.ByIndex("by_val", "bar")
	require.NoError(t, err)
	assert.Empty(t, items)

	require.NoError(t, store.Add(testStorageElement("foo", "bar", 1)))
	items, err = store.ByIndex("by_val", "bar")
	require.NoError(t, err)
	assert.Equal(t, []interface{}{
		testStorageElement("foo", "bar", 1),
	}, items)

	require.NoError(t, store.Update(testStorageElement("foo", "baz", 2)))
	items, err = store.ByIndex("by_val", "bar")
	require.NoError(t, err)
	assert.Empty(t, items)
	items, err = store.ByIndex("by_val", "baz")
	require.NoError(t, err)
	assert.Equal(t, []interface{}{
		testStorageElement("foo", "baz", 2),
	}, items)

	require.NoError(t, store.Update(testStorageElement("foo", "baz", 3)))
	items, err = store.ByIndex("by_val", "bar")
	require.NoError(t, err)
	assert.Empty(t, items)
	items, err = store.ByIndex("by_val", "baz")
	require.NoError(t, err)
	assert.Equal(t, []interface{}{
		testStorageElement("foo", "baz", 3),
	}, items)

	require.NoError(t, store.Replace([]interface{}{
		testStorageElement("foo", "bar", 4),
	}, ""))
	items, err = store.ByIndex("by_val", "bar")
	require.NoError(t, err)
	assert.Equal(t, []interface{}{
		testStorageElement("foo", "bar", 4),
	}, items)
	items, err = store.ByIndex("by_val", "baz")
	require.NoError(t, err)
	assert.Empty(t, items)

	require.NoError(t, store.Delete(testStorageElement("foo", "", 0)))
	items, err = store.ByIndex("by_val", "baz")
	require.NoError(t, err)
	assert.Empty(t, items)

	require.NoError(t, store.Delete(testStorageElement("foo", "", 0)))
}

// assertStoreEmpty 从 Get/GetByKey/List 三个入口确认 store 为空，
// 避免单一入口的缓存掩盖不一致。
func assertStoreEmpty(t *testing.T, store Indexer, nonExistingKey string) {
	item, ok, err := store.Get(testStorageElement(nonExistingKey, "", 0))
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, item)

	item, ok, err = store.GetByKey(nonExistingKey)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, item)

	items := store.List()
	assert.Empty(t, items)
}

// assertStoreSingleKey 断言 store 中恰好只有指定 key，且值/资源版本符合预期。
func assertStoreSingleKey(t *testing.T, store Indexer, expectKey, expectValue string, expectRV int) {
	item, ok, err := store.Get(testStorageElement(expectKey, "", expectRV))
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, expectValue, item.(*Element).Object.(fakeObj).value)

	item, ok, err = store.GetByKey(expectKey)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, expectValue, item.(*Element).Object.(fakeObj).value)

	items := store.List()
	assert.Equal(t, []interface{}{testStorageElement(expectKey, expectValue, expectRV)}, items)
}

// testStorageElement 构造带 key 的测试元素；rv 用于验证更新后存储的是新版本。
func testStorageElement(key, value string, rv int) *Element {
	return &Element{Key: key, Object: fakeObj{value: value, rv: rv}}
}

// fakeObj 是最小的 runtime.Object 替身，只需携带索引值与 RV。
type fakeObj struct {
	value string
	rv    int
}

// GetObjectKind 返回 nil：测试不依赖 GVK。
func (f fakeObj) GetObjectKind() schema.ObjectKind { return nil }

// DeepCopyObject 返回 nil：store 测试不做深拷贝。
func (f fakeObj) DeepCopyObject() runtime.Object { return nil }

var _ runtime.Object = (*fakeObj)(nil)

// testStoreIndexFunc 以对象的值作为唯一索引键。
func testStoreIndexFunc(obj interface{}) ([]string, error) {
	return []string{obj.(fakeObj).value}, nil
}

// testStoreIndexers 构造名为 by_val 的索引集合。
func testStoreIndexers() *cache.Indexers {
	indexers := cache.Indexers{}
	indexers["by_val"] = testStoreIndexFunc
	return &indexers
}
