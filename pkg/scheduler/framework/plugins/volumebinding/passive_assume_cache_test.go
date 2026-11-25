/*
Copyright 2017 The Kubernetes Authors.

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

package volumebinding

import (
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-helpers/storage/volume"
	"k8s.io/klog/v2/ktesting"
)

// testInformer 是一个最小化的 informer 替身，足够支撑一个 assume cache 的测试。
type testInformer struct {
	handler cache.ResourceEventHandler
	indexer cache.Indexer
	t       *testing.T
}

// AddEventHandler 记录测试要触发的事件处理器。
func (i *testInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	i.handler = handler
	return nil, nil
}

// GetIndexer 返回底层索引器，供测试直接预置对象。
func (i *testInformer) GetIndexer() cache.Indexer {
	return i.indexer
}

// add 模拟 informer 的新增事件，先更新索引再通知处理器。
func (i *testInformer) add(obj interface{}) {
	if err := i.indexer.Add(obj); err != nil {
		i.t.Fatalf("failed to add object into indexer: %v", err)
	}
	i.handler.OnAdd(obj, false)
}

// update 模拟 informer 的更新事件。
func (i *testInformer) update(oldObj, obj interface{}) {
	if err := i.indexer.Update(obj); err != nil {
		i.t.Fatalf("failed to update object to indexer: %v", err)
	}
	i.handler.OnUpdate(oldObj, obj)
}

// delete 模拟 informer 的删除事件。
func (i *testInformer) delete(obj interface{}) {
	if err := i.indexer.Delete(obj); err != nil {
		i.t.Fatalf("failed to delete object from indexer: %v", err)
	}
	i.handler.OnDelete(obj)
}

// verifyList 校验按指定索引值查询到的对象集合与预期一致。
func verifyList(t *testing.T, cache testCache, expected map[string]*testObj, indexedValue string) {
	t.Helper()
	got, err := cache.ByIndex(testIndex, indexedValue)
	if err != nil {
		t.Fatalf("failed to get indexed objects: %v", err)
	}
	if len(got) != len(expected) {
		t.Errorf("ByIndex() returned %v objects, expected %v", len(got), len(expected))
	}
	for _, obj := range got {
		expected, ok := expected[obj.Name]
		if !ok {
			t.Errorf("ByIndex() returned unexpected object %q", obj.Name)
		}
		if expected != obj {
			t.Errorf("ByIndex() returned object %p, expected %p", obj, expected)
		}
	}
}

// TestBasicCache 验证 assume cache 的基本增删改查。
func TestBasicCache(t *testing.T) {
	informer, cache := newTestCache(t)

	// 查询不存在的对象应返回错误且对象为 nil。
	obj, err := cache.Get("nothere")
	if err == nil {
		t.Errorf("Get() returned unexpected success")
	}
	if obj != nil {
		t.Errorf("Get() returned unexpected PV %q", obj.Name)
	}

	// 批量新增对象。
	objects := map[string]*testObj{}
	for i := range 10 {
		obj := makeObj(fmt.Sprintf("test-%v", i), "1", "")
		objects[obj.Name] = obj
		informer.add(obj)
	}

	// 校验列表。
	verifyList(t, cache, objects, "")

	// 更新一个对象。
	updated := makeObj("test-3", "1", "")
	informer.update(objects["test-3"], updated)
	objects[updated.Name] = updated

	// 再次校验列表。
	verifyList(t, cache, objects, "")

	// 删除一个对象。
	deleted := objects["test-7"]
	delete(objects, deleted.Name)
	informer.delete(deleted)

	// 再次校验列表。
	verifyList(t, cache, objects, "")
}

// TestPVCacheWithIndex 验证带索引查询在增改删之后仍返回正确的对象分组。
func TestPVCacheWithIndex(t *testing.T) {
	informer, cache := newTestCache(t)

	// 批量新增对象。
	objects1 := map[string]*testObj{}
	for i := range 10 {
		obj := makeObj(fmt.Sprintf("test-%v", i), "1", "")
		obj.Annotations["test"] = "test1"
		objects1[obj.Name] = obj
		informer.add(obj)
	}

	// 再新增一组不同索引值的对象。
	objects2 := map[string]*testObj{}
	for i := range 10 {
		obj := makeObj(fmt.Sprintf("test2-%v", i), "1", "")
		obj.Annotations["test"] = "test2"
		objects2[obj.Name] = obj
		informer.add(obj)
	}

	// 再次校验列表。
	verifyList(t, cache, objects1, "test1")
	verifyList(t, cache, objects2, "test2")

	// 更新一个对象。
	updated := makeObj("test-3", "2", "")
	updated.Annotations["test"] = "test1"
	informer.update(objects1[updated.Name], updated)
	objects1[updated.Name] = updated

	// 再次校验列表。
	verifyList(t, cache, objects1, "test1")
	verifyList(t, cache, objects2, "test2")

	// 删除一个对象。
	deletedPV := objects1["test-7"]
	delete(objects1, deletedPV.Name)
	informer.delete(deletedPV)

	// 再次校验列表。
	verifyList(t, cache, objects1, "test1")
	verifyList(t, cache, objects2, "test2")
}

// testObj 是 assume cache 测试中使用的轻量对象类型。
type testObj = metav1.ObjectMeta

// makeObj 构造一个带名字、资源版本和命名空间的测试对象。
func makeObj(name, version, namespace string) *testObj {
	return &testObj{
		Name:            name,
		Namespace:       namespace,
		ResourceVersion: version,
		Annotations:     map[string]string{},
	}
}

// testCache 是测试中使用的 assume cache 别名。
type testCache = *passiveAssumeCache[*testObj]

// verifyObj 校验按 key 查询到的对象指针与预期完全相同。
func verifyObj(cache testCache, key string, expected *testObj) error {
	obj, err := cache.Get(key)
	if err != nil {
		return err
	}
	if obj != expected {
		return fmt.Errorf("Get() returned %p, expected %p", obj, expected)
	}
	return nil
}

// testIndex 是测试中使用的索引名。
const testIndex = "testIndex"

// newTestCache 创建一个带 name->对象 索引的测试用 assume cache。
func newTestCache(t *testing.T) (*testInformer, testCache) {
	logger, _ := ktesting.NewTestContext(t)
	informer := &testInformer{
		indexer: cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
			testIndex: func(obj interface{}) ([]string, error) {
				return []string{obj.(*testObj).Annotations["test"]}, nil
			},
		}),
		t: t,
	}
	cache, err := newAssumeCache[*testObj](logger, informer, schema.GroupResource{Resource: "tests"})
	if err != nil {
		t.Fatalf("newAssumeCache() failed: %v", err)
	}
	return informer, cache
}

// TestAssume 覆盖 Assume 在版本相同或更新时的成功/失败场景。
func TestAssume(t *testing.T) {
	scenarios := map[string]struct {
		old           *testObj
		new           *testObj
		shouldSucceed bool
	}{
		"success-same-version": {
			old:           makeObj("pvc1", "5", "ns1"),
			new:           makeObj("pvc1", "5", "ns1"),
			shouldSucceed: true,
		},
		"fail-new-higher-version": {
			old:           makeObj("pvc1", "5", "ns1"),
			new:           makeObj("pvc1", "6", "ns1"),
			shouldSucceed: false,
		},
		"fail-old-not-found": {
			old:           makeObj("pvc2", "5", "ns1"),
			new:           makeObj("pvc1", "5", "ns1"),
			shouldSucceed: false,
		},
		"fail-new-lower-version": {
			old:           makeObj("pvc1", "5", "ns1"),
			new:           makeObj("pvc1", "4", "ns1"),
			shouldSucceed: false,
		},
		"fail-new-bad-version": {
			old:           makeObj("pvc1", "5", "ns1"),
			new:           makeObj("pvc1", "a", "ns1"),
			shouldSucceed: false,
		},
		"fail-old-bad-version": {
			old:           makeObj("pvc1", "a", "ns1"),
			new:           makeObj("pvc1", "5", "ns1"),
			shouldSucceed: false,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			informer, cache := newTestCache(t)

			// 先把旧版本加入缓存。
			informer.add(scenario.old)
			if err := verifyObj(cache, keyOf(scenario.old), scenario.old); err != nil {
				t.Fatalf("Failed to Get() after initial update: %v", err)
			}

			// 尝试假定新版本。
			err := cache.Assume(scenario.new)
			if scenario.shouldSucceed && err != nil {
				t.Errorf("Test %q failed: Assume() returned error %v", name, err)
			}
			if !scenario.shouldSucceed && err == nil {
				t.Errorf("Test %q failed: Assume() returned success but expected error", name)
			}

			// 校验缓存中保留的是正确版本。
			expectedPV := scenario.new
			if !scenario.shouldSucceed {
				expectedPV = scenario.old
			}
			if err := verifyObj(cache, keyOf(scenario.old), expectedPV); err != nil {
				t.Errorf("Failed to Get() after initial update: %v", err)
			}
		})
	}
}

// TestRestore 验证 Restore 能把假定对象还原为 informer 中的版本。
func TestRestore(t *testing.T) {
	informer, cache := newTestCache(t)

	old := makeObj("pvc1", "5", "ns1")
	new := makeObj("pvc1", "5", "ns1")

	// 还原一个不存在的对象不应产生副作用。
	cache.Restore(&testObj{})

	// 把旧版本加入缓存。
	informer.add(old)
	if err := verifyObj(cache, keyOf(old), old); err != nil {
		t.Fatalf("Failed to Get() after initial update: %v", err)
	}

	// 还原尚未被假定的对象。
	cache.Restore(old)
	if err := verifyObj(cache, keyOf(old), old); err != nil {
		t.Fatalf("Failed to Get() after initial restore: %v", err)
	}

	// 假定新版本。
	if err := cache.Assume(new); err != nil {
		t.Fatalf("Assume() returned error %v", err)
	}
	if err := verifyObj(cache, keyOf(old), new); err != nil {
		t.Fatalf("Failed to Get() after Assume: %v", err)
	}

	// 还原被假定的对象，应回到 informer 中的旧版本。
	cache.Restore(new)
	if err := verifyObj(cache, keyOf(old), old); err != nil {
		t.Fatalf("Failed to Get() after restore: %v", err)
	}
}

// TestConcurrentAssume 验证旧假定对象被还原后不会覆盖新的假定版本。
func TestConcurrentAssume(t *testing.T) {
	informer, cache := newTestCache(t)

	obj1 := makeObj("pvc1", "5", "ns1")
	obj1Update := makeObj("pvc1", "5", "ns1")
	// 把对象加入缓存。
	informer.add(obj1)

	// 假定对象 1。
	if err := cache.Assume(obj1Update); err != nil {
		t.Fatalf("Assume() returned error %v", err)
	}
	if err := verifyObj(cache, keyOf(obj1Update), obj1Update); err != nil {
		t.Fatalf("Failed to Get() after Assume: %v", err)
	}

	obj2 := makeObj("pvc1", "7", "ns1")
	obj2Update := makeObj("pvc1", "7", "ns1")
	// 模拟对象被外部更新。
	informer.add(obj2)

	// 假定对象 2。
	if err := cache.Assume(obj2Update); err != nil {
		t.Fatalf("Assume() returned error %v", err)
	}
	// 对象 1 因冲突被还原。
	cache.Restore(obj1Update)
	// 缓存中仍应保留对象 2 的新版本。
	if err := verifyObj(cache, keyOf(obj2Update), obj2Update); err != nil {
		t.Fatalf("Failed to Get() after restore: %v", err)
	}
}

// TestAssumeUpdateCache 验证 Assume 会更新缓存，且随后到达的旧版本事件不会覆盖它。
func TestAssumeUpdateCache(t *testing.T) {
	informer, cache := newTestCache(t)

	// 加入一个对象。
	obj := makeObj("test-pvc0", "1", "test-ns")
	informer.add(obj)
	if err := verifyObj(cache, keyOf(obj), obj); err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	// 假定带已选节点注解的新版本。
	newObj := obj.DeepCopy()
	newObj.Annotations[volume.AnnSelectedNode] = "test-node"
	if err := cache.Assume(newObj); err != nil {
		t.Fatalf("failed to assume: %v", err)
	}
	if err := verifyObj(cache, keyOf(obj), newObj); err != nil {
		t.Fatalf("failed to get after assume: %v", err)
	}

	// 旧版本事件迟到，不应覆盖假定版本。
	informer.add(obj)
	if err := verifyObj(cache, keyOf(obj), newObj); err != nil {
		t.Fatalf("failed to get after old added: %v", err)
	}
}

// TestDelayedInformerEvent 验证 informer 延迟事件不会覆盖缓存中的假定版本。
func TestDelayedInformerEvent(t *testing.T) {
	informer, cache := newTestCache(t)

	obj1 := makeObj("test-pvc0", "1", "test-ns")
	obj2 := makeObj("test-pvc0", "2", "test-ns")
	// 只更新索引器，模拟 informer 事件尚未到达。
	if err := informer.indexer.Add(obj2); err != nil {
		t.Fatalf("failed to add: %v", err)
	}

	newObj := obj2.DeepCopy()
	newObj.Annotations[volume.AnnSelectedNode] = "test-node"
	if err := cache.Assume(newObj); err != nil {
		t.Fatalf("failed to assume: %v", err)
	}

	// 发送延迟到达的增删事件。
	informer.handler.OnAdd(obj1, false)
	informer.handler.OnDelete(obj1)
	informer.handler.OnAdd(obj2, false)
	// 假定版本不应被延迟事件覆盖。
	if err := verifyObj(cache, keyOf(newObj), newObj); err != nil {
		t.Fatalf("failed to get after assume: %v", err)
	}
}
