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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
)

// 测试公开接口。
//
// doTestStore 是所有 Store 实现共享的一致性测试：增删改查、List 与 Replace 语义。
func doTestStore(t *testing.T, store Store) {
	mkObj := func(id string, val string) testStoreObject {
		return testStoreObject{id: id, val: val}
	}

	store.Add(mkObj("foo", "bar"))
	if item, ok, _ := store.Get(mkObj("foo", "")); !ok {
		t.Errorf("didn't find inserted item")
	} else {
		if e, a := "bar", item.(testStoreObject).val; e != a {
			t.Errorf("expected %v, got %v", e, a)
		}
	}
	store.Update(mkObj("foo", "baz"))
	if item, ok, _ := store.Get(mkObj("foo", "")); !ok {
		t.Errorf("didn't find inserted item")
	} else {
		if e, a := "baz", item.(testStoreObject).val; e != a {
			t.Errorf("expected %v, got %v", e, a)
		}
	}
	store.Delete(mkObj("foo", ""))
	if _, ok, _ := store.Get(mkObj("foo", "")); ok {
		t.Errorf("found deleted item??")
	}

	// Test List.
	store.Add(mkObj("a", "b"))
	store.Add(mkObj("c", "d"))
	store.Add(mkObj("e", "e"))
	{
		found := sets.Set[string]{}
		for _, item := range store.List() {
			found.Insert(item.(testStoreObject).val)
		}
		if !found.HasAll("b", "d", "e") {
			t.Errorf("missing items, found: %v", found)
		}
		if len(found) != 3 {
			t.Errorf("extra items")
		}
	}

	// Test Replace.
	// Replace 会整体替换内容，之前的 a/c/e 必须全部消失。
	store.Replace([]interface{}{
		mkObj("foo", "foo"),
		mkObj("bar", "bar"),
	}, "0")

	{
		found := sets.Set[string]{}
		for _, item := range store.List() {
			found.Insert(item.(testStoreObject).val)
		}
		if !found.HasAll("foo", "bar") {
			t.Errorf("missing items")
		}
		if len(found) != 2 {
			t.Errorf("extra items")
		}
	}
}

// 测试公开接口。
func doTestIndex(t *testing.T, indexer Indexer) {
	// 期望的索引：val 作为索引键，映射到拥有该 val 的对象 id 集合。
	mkObj := func(id string, val string) testStoreObject {
		return testStoreObject{id: id, val: val}
	}

	// Test Index
	expected := map[string]sets.Set[string]{}
	expected["b"] = sets.New("a", "c")
	expected["f"] = sets.New("e")
	expected["h"] = sets.New("g")
	indexer.Add(mkObj("a", "b"))
	indexer.Add(mkObj("c", "b"))
	indexer.Add(mkObj("e", "f"))
	indexer.Add(mkObj("g", "h"))
	{
		for k, v := range expected {
			found := sets.Set[string]{}
			indexResults, err := indexer.Index("by_val", mkObj("", k))
			if err != nil {
				t.Errorf("Unexpected error %v", err)
			}
			for _, item := range indexResults {
				found.Insert(item.(testStoreObject).id)
			}
			items := sets.List(v)
			if !found.HasAll(items...) {
				t.Errorf("missing items, index %s, expected %v but found %v", k, items, sets.List(found))
			}
		}
	}
}

func testStoreKeyFunc(obj interface{}) (string, error) {
	return obj.(testStoreObject).id, nil
}

func testStoreIndexFunc(obj interface{}) ([]string, error) {
	return []string{obj.(testStoreObject).val}, nil
}

func testStoreIndexers() Indexers {
	indexers := Indexers{}
	indexers["by_val"] = testStoreIndexFunc
	return indexers
}

type testStoreObject struct {
	id  string
	val string
}

// TestCache 验证普通 Store 的基本语义。
func TestCache(t *testing.T) {
	doTestStore(t, NewStore(testStoreKeyFunc))
}

// TestCacheWithTransformer 验证 WithTransformer 注入的转换函数确实在写入时被调用。
func TestCacheWithTransformer(t *testing.T) {
	transformerCalled := &atomic.Bool{}
	doTestStore(t, NewStore(testStoreKeyFunc, WithTransformer(func(i interface{}) (interface{}, error) {
		transformerCalled.Store(true)
		obj, ok := i.(testStoreObject)
		if !ok {
			return nil, errors.New("wrong object type")
		}
		return obj, nil
	})))
	if !transformerCalled.Load() {
		t.Error("informer was not called")
	}
}

// TestFIFOCache 验证 FIFO 同样满足 Store 契约（额外的队列行为由其他测试覆盖）。
func TestFIFOCache(t *testing.T) {
	doTestStore(t, NewFIFO(testStoreKeyFunc))
}

// TestUndeltaStore 验证 UndeltaStore 在保持 Store 语义的同时不会因未注册回调而出错。
func TestUndeltaStore(t *testing.T) {
	nop := func([]interface{}) {}
	doTestStore(t, NewUndeltaStore(nop, testStoreKeyFunc))
}

// TestIndex 验证 Indexer 按索引函数返回正确的结果集合。
func TestIndex(t *testing.T) {
	doTestIndex(t, NewIndexer(testStoreKeyFunc, testStoreIndexers()))
}

// TestKeyError 验证 KeyError 支持 errors.Unwrap/Is，嵌套包装也能正确匹配。
func TestKeyError(t *testing.T) {
	obj := 100
	err := errors.New("error")
	keyErr := KeyError{obj, err}

	if errors.Unwrap(keyErr) != err {
		t.Errorf("expected unwrap error: %v", err)
	}

	nestedKeyErr := KeyError{obj, keyErr}
	if !errors.Is(keyErr, err) || !errors.Is(nestedKeyErr, err) {
		t.Errorf("not match target error: %v", err)
	}
}

// TestCacheTransactionShouldIndexErrors 验证事务式写入部分失败时，
// SuccessfulIndices 准确记录成功的下标，便于调用方对失败项重试。
func TestCacheTransactionShouldIndexErrors(t *testing.T) {
	successObj1 := 1
	successObj2 := 2
	failObj1 := 3
	failObj2 := 4
	testTxnType := TransactionTypeAdd
	testCases := []struct {
		name        string
		objs        []interface{}
		assertError func(*TransactionError) bool
	}{
		{
			name: "txn all success objects should work",
			objs: []interface{}{successObj1, successObj2},
		},
		{
			name: "txn all fail objects should work",
			objs: []interface{}{failObj1, failObj2},
			assertError: func(err *TransactionError) bool {
				return assert.Equal(t, []int{}, err.SuccessfulIndices)
			},
		},
		{
			name: "txn mix success and fail objects should work",
			objs: []interface{}{successObj1, failObj1, successObj2, failObj2},
			assertError: func(err *TransactionError) bool {
				return assert.Equal(t, []int{0, 2}, err.SuccessfulIndices)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testKeyFunc := func(obj interface{}) (string, error) {
				if obj == successObj1 || obj == successObj2 {
					return "", nil
				}
				return "", errors.New("test error")
			}
			testStore := NewStore(testKeyFunc)
			txnStore := testStore.(TransactionStore)
			txns := make([]Transaction, len(tc.objs))
			for i := range tc.objs {
				txns[i] = Transaction{
					Object: tc.objs[i],
					Type:   testTxnType,
				}
			}
			txnErr := txnStore.Transaction(txns...)
			if tc.assertError != nil {
				tc.assertError(txnErr)
				return
			}
			if txnErr != nil {
				t.Errorf("unexpected error: %v", txnErr)
			}
		})
	}
}

// TestWithStoreMetricsDefaulting 验证传入 nil 的 metrics provider 时会回退到默认实现，
// 而不是留下 nil 导致后续写入 panic。
func TestWithStoreMetricsDefaulting(t *testing.T) {
	name, err := NewInformerName("test-informer")
	if err != nil {
		t.Fatalf("failed to create informer name: %v", err)
	}
	defer name.Release()
	gvr := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	id := name.WithResource(gvr)

	s := NewStore(testStoreKeyFunc, WithStoreMetrics(id, nil)).(*cache)

	if s.metrics == nil {
		t.Errorf("expected c.metrics to be populated, got nil")
	}
}
