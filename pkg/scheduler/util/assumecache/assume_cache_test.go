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

package assumecache

import (
	"fmt"
	"slices"
	"sort"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// testInformer 实现 [Informer] 接口，用于单元测试中向 assume cache 灌入变更。
// 只支持单个事件处理器——一个 assume cache 也只需要一个。
type testInformer struct {
	handler cache.ResourceEventHandler
}

// AddEventHandler 注册一个事件处理器；测试用实现只保留单个 handler。
func (i *testInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	i.handler = handler
	return nil, nil
}

// add 模拟 informer 的 Add 事件，把对象喂给 assume cache。
func (i *testInformer) add(obj interface{}) {
	if i.handler == nil {
		return
	}
	i.handler.OnAdd(obj, false)
}

// update 模拟 informer 的 Update 事件，把对象喂给 assume cache。
func (i *testInformer) update(obj interface{}) {
	if i.handler == nil {
		return
	}
	i.handler.OnUpdate(nil, obj)
}

// delete 模拟 informer 的 Delete 事件，把对象喂给 assume cache。
func (i *testInformer) delete(obj interface{}) {
	if i.handler == nil {
		return
	}
	i.handler.OnDelete(obj)
}

// makeObj 构造带指定名称、资源版本和命名空间的测试对象。
func makeObj(name, version, namespace string) metav1.Object {
	return &metav1.ObjectMeta{
		Name:            name,
		Namespace:       namespace,
		ResourceVersion: version,
	}
}

// newTest 创建一个不带索引器的测试用 AssumeCache 及 informer。
func newTest(t *testing.T) (ktesting.TContext, *AssumeCache, *testInformer) {
	return newTestWithIndexer(t, "", nil)
}

// newTestWithIndexer 创建带指定索引器的测试用 AssumeCache 及 informer。
func newTestWithIndexer(t *testing.T, indexName string, indexFunc cache.IndexFunc) (ktesting.TContext, *AssumeCache, *testInformer) {
	tCtx := ktesting.Init(t)
	informer := new(testInformer)
	cache := NewAssumeCache(tCtx.Logger(), informer, "TestObject", indexName, indexFunc)
	return tCtx, cache, informer
}

// verify 校验 Get 和 GetAPIObj 返回的都是期望对象。
func verify(tCtx ktesting.TContext, cache *AssumeCache, key string, expectedObject, expectedAPIObject interface{}) {
	tCtx.Helper()
	actualObject, err := cache.Get(key)
	if err != nil {
		tCtx.Fatalf("unexpected error retrieving object for key %s: %v", key, err)
	}
	if actualObject != expectedObject {
		tCtx.Fatalf("Get() returned %v, expected %v", actualObject, expectedObject)
	}
	actualAPIObject, err := cache.GetAPIObj(key)
	if err != nil {
		tCtx.Fatalf("unexpected error retrieving API object for key %s: %v", key, err)
	}
	if actualAPIObject != expectedAPIObject {
		tCtx.Fatalf("GetAPIObject() returned %v, expected %v", actualAPIObject, expectedAPIObject)
	}
}

// verifyList 校验 List 返回的对象集合与期望值一致（比较时忽略顺序）。
func verifyList(tCtx ktesting.TContext, assumeCache *AssumeCache, expectedObjs []interface{}, indexObj interface{}) {
	actualObjs := assumeCache.List(indexObj)
	diff := cmp.Diff(expectedObjs, actualObjs, cmpopts.SortSlices(func(x, y interface{}) bool {
		xKey, err := cache.MetaNamespaceKeyFunc(x)
		if err != nil {
			tCtx.Fatalf("unexpected error determining key for %v: %v", x, err)
		}
		yKey, err := cache.MetaNamespaceKeyFunc(y)
		if err != nil {
			tCtx.Fatalf("unexpected error determining key for %v: %v", y, err)
		}
		return xKey < yKey
	}))
	if diff != "" {
		tCtx.Fatalf("List() result differs (- expected, + actual):\n%s", diff)
	}
}

// mockEventHandler 记录 assume cache 派发的事件，支持阻塞事件派发以测试并发行为。
type mockEventHandler struct {
	mutex  sync.Mutex
	events []event
	cache  *AssumeCache
	block  <-chan struct{}
}

// event 表示一次事件派发的记录，用于与期望事件序列比较。
type event struct {
	What        string
	OldObj, Obj interface{}
	InitialList bool
}

// OnAdd 记录一次 Add 事件。
func (m *mockEventHandler) OnAdd(obj interface{}, initialList bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.events = append(m.events, event{
		What:        "add",
		Obj:         obj,
		InitialList: initialList,
	})

	if m.cache != nil {
		// 在事件回调里访问缓存绝不能死锁。
		m.cache.List(nil)
	}
	if m.block != nil {
		<-m.block
	}
}

// OnUpdate 记录一次 Update 事件。
func (m *mockEventHandler) OnUpdate(oldObj, obj interface{}) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.events = append(m.events, event{
		What:   "update",
		OldObj: oldObj,
		Obj:    obj,
	})
}

// OnDelete 记录一次 Delete 事件。
func (m *mockEventHandler) OnDelete(obj interface{}) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.events = append(m.events, event{
		What: "delete",
		Obj:  obj,
	})
}

// verifyAndFlush 比较已记录事件与期望值，通过后清空事件列表。
func (m *mockEventHandler) verifyAndFlush(tCtx ktesting.TContext, expectedEvents []event) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	tCtx.Helper()
	if diff := cmp.Diff(expectedEvents, m.events); diff != "" {
		tCtx.Fatalf("unexpected events (- expected, + actual):\n%s", diff)
	}
	m.events = nil
}

// sortEvents 按给定比较函数对已记录事件排序，消除并发派发带来的顺序不确定性。
func (m *mockEventHandler) sortEvents(cmp func(objI, objJ interface{}) bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	sort.Slice(m.events, func(i, j int) bool {
		return cmp(m.events[i].Obj, m.events[j].Obj)
	})
}

// TestAssume 验证 Assume 在各种版本与对象合法性场景下的行为，以及事件是否正确派发。
func TestAssume(t *testing.T) {
	scenarios := map[string]struct {
		oldObj    metav1.Object
		newObj    interface{}
		expectErr error
	}{
		"success-same-version": {
			oldObj: makeObj("pvc1", "5", ""),
			newObj: makeObj("pvc1", "5", ""),
		},
		"success-new-higher-version": {
			oldObj: makeObj("pvc1", "5", ""),
			newObj: makeObj("pvc1", "6", ""),
		},
		"fail-old-not-found": {
			oldObj:    makeObj("pvc2", "5", ""),
			newObj:    makeObj("pvc1", "5", ""),
			expectErr: ErrNotFound,
		},
		"fail-new-lower-version": {
			oldObj:    makeObj("pvc1", "5", ""),
			newObj:    makeObj("pvc1", "4", ""),
			expectErr: cmpopts.AnyError,
		},
		"fail-new-bad-version": {
			oldObj:    makeObj("pvc1", "5", ""),
			newObj:    makeObj("pvc1", "a", ""),
			expectErr: cmpopts.AnyError,
		},
		"fail-old-bad-version": {
			oldObj:    makeObj("pvc1", "a", ""),
			newObj:    makeObj("pvc1", "5", ""),
			expectErr: cmpopts.AnyError,
		},
		"fail-new-bad-object": {
			oldObj:    makeObj("pvc1", "5", ""),
			newObj:    1,
			expectErr: ErrObjectName,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			tCtx, cache, informer := newTest(t)
			var events mockEventHandler
			cache.AddEventHandler(&events)

			// 先把旧对象加入缓存。
			informer.add(scenario.oldObj)
			verify(tCtx, cache, scenario.oldObj.GetName(), scenario.oldObj, scenario.oldObj)

			// 再 Assume 新对象。
			err := cache.Assume(scenario.newObj)
			if diff := cmp.Diff(scenario.expectErr, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("Assume() returned error: %v\ndiff (- expected, + actual):\n%s", err, diff)
			}

			// 校验 Get 返回了正确的对象，且事件已按预期派发。
			expectEvents := []event{{What: "add", Obj: scenario.oldObj}}
			expectedObj := scenario.newObj
			if scenario.expectErr != nil {
				expectedObj = scenario.oldObj
			} else {
				expectEvents = append(expectEvents, event{What: "update", OldObj: scenario.oldObj, Obj: scenario.newObj})
			}
			verify(tCtx, cache, scenario.oldObj.GetName(), expectedObj, scenario.oldObj)
			events.verifyAndFlush(tCtx, expectEvents)
		})
	}
}

// TestAssumeRace 模拟如下事件序列：
//   - informer 更新到达，事件处理器被调用但处理很慢。
//   - 同时对同一对象调用 Assume，它必须阻塞直到 informer 触发的事件派发完成。
func TestAssumeRace(t *testing.T) { ktesting.Init(t).SyncTest("", testAssumeRace) }

// testAssumeRace 复现 informer 事件处理与 Assume 并发时的竞态：Assume 必须等待事件派发完成。
func testAssumeRace(tCtx ktesting.TContext) {
	var informer testInformer
	testObj := makeObj("pvc1", "1", "")
	ac := NewAssumeCache(tCtx.Logger(), &informer, "TestObject", "", nil)
	blockEvent := tCtx.WithCancel()
	defer blockEvent.Cancel("test done")
	eventBlocked := false
	eventDone := false
	ac.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			eventBlocked = true
			<-blockEvent.Done()
			eventDone = true
		},
	})

	// 模拟真实客户端发起 Create：Assume 之前 assume cache 可能看到、也可能没看到
	// informer 送来的新对象——这里假定它先到。
	go informer.add(testObj)

	// 等待事件处理开始。
	tCtx.Wait()
	if !eventBlocked {
		tCtx.Fatal("Event handler should have been called and wasn't.")
	}

	// Assume 应当阻塞，直到我们放开事件派发。
	assumeDone := false
	go func() {
		err := ac.Assume(testObj)
		tCtx.AssertNoError(err, "Assume failed")
		assumeDone = true
	}()

	// 等待 Assume 在其内部阻塞。
	tCtx.Wait()
	if assumeDone {
		tCtx.Fatal("Assume should have blocked and didn't.")
	}

	// 同时放开两个 goroutine。
	blockEvent.Cancel("proceed")
	tCtx.Wait()
	if !eventDone {
		tCtx.Fatal("Event should have been delivered and wasn't.")
	}
	if !assumeDone {
		tCtx.Fatal("Assume should have returned and didn't.")
	}
}

// TestRestore 验证 Restore 能把被 Assume 修改的对象恢复为 API 侧的版本。
func TestRestore(t *testing.T) {
	tCtx, cache, informer := newTest(t)
	var events mockEventHandler
	cache.AddEventHandler(&events)

	// 这里 Assume 的对象与 API 对象版本相同。assume cache 支持这种做法，
	// 但真实代码中会有竞态：若此时 apiserver 来了个无关更新，本地修改会被丢弃。
	oldObj := makeObj("pvc1", "5", "")
	newObj := makeObj("pvc1", "5", "")

	// Restore 一个不存在的对象。
	tCtx.Step("empty cache", func(tCtx ktesting.TContext) {
		cache.Restore("nothing")
		events.verifyAndFlush(tCtx, nil)
	})

	// 把旧对象加入缓存。
	tCtx.Step("initial update", func(tCtx ktesting.TContext) {
		informer.add(oldObj)
		verify(tCtx, cache, oldObj.GetName(), oldObj, oldObj)
		events.verifyAndFlush(tCtx, []event{{What: "add", Obj: oldObj}})
	})

	// Restore 同一个对象。
	tCtx.Step("initial Restore", func(tCtx ktesting.TContext) {
		cache.Restore(oldObj.GetName())
		verify(tCtx, cache, oldObj.GetName(), oldObj, oldObj)
		events.verifyAndFlush(tCtx, nil)
	})

	// Assume 新对象。
	tCtx.Step("Assume", func(tCtx ktesting.TContext) {
		if err := cache.Assume(newObj); err != nil {
			tCtx.Fatalf("Assume() returned error %v", err)
		}
		verify(tCtx, cache, oldObj.GetName(), newObj, oldObj)
		events.verifyAndFlush(tCtx, []event{{What: "update", OldObj: oldObj, Obj: newObj}})
	})

	// Restore 同一个对象。
	tCtx.Step("second Restore", func(tCtx ktesting.TContext) {
		cache.Restore(oldObj.GetName())
		verify(tCtx, cache, oldObj.GetName(), oldObj, oldObj)
		events.verifyAndFlush(tCtx, []event{{What: "update", OldObj: newObj, Obj: oldObj}})
	})
}

// TestEvents 验证 informer 的 Add/Update/Delete 事件能正确更新缓存并派发给处理器。
func TestEvents(t *testing.T) {
	tCtx, cache, informer := newTest(t)

	oldObj := makeObj("pvc1", "5", "")
	newObj := makeObj("pvc1", "6", "")
	key := oldObj.GetName()

	// 把旧对象加入缓存。
	informer.add(oldObj)
	verify(tCtx.WithStep("after initial update"), cache, key, oldObj, oldObj)

	// 注册处理器后会收到一次初始列表事件。
	var events mockEventHandler
	cache.AddEventHandler(&events)
	events.verifyAndFlush(tCtx.WithStep("initial list"), []event{{What: "add", Obj: oldObj, InitialList: true}})

	// 更新对象。
	tCtx.Step("initial update", func(tCtx ktesting.TContext) {
		informer.update(newObj)
		verify(tCtx, cache, key, newObj, newObj)
		events.verifyAndFlush(tCtx, []event{{What: "update", OldObj: oldObj, Obj: newObj}})
	})

	// 一些异常输入（实际不会发生）。
	tCtx.Step("nop add", func(tCtx ktesting.TContext) {
		informer.add(1)
		verify(tCtx, cache, key, newObj, newObj)
		events.verifyAndFlush(tCtx, nil)
	})
	tCtx.Step("nil add", func(tCtx ktesting.TContext) {
		informer.add(nil)
		verify(tCtx, cache, key, newObj, newObj)
		events.verifyAndFlush(tCtx, nil)
	})
	tCtx.Step("nop update", func(tCtx ktesting.TContext) {
		informer.update(oldObj)
		events.verifyAndFlush(tCtx, nil)
		verify(tCtx, cache, key, newObj, newObj)
	})
	tCtx.Step("nil update", func(tCtx ktesting.TContext) {
		informer.update(nil)
		verify(tCtx, cache, key, newObj, newObj)
		events.verifyAndFlush(tCtx, nil)
	})
	tCtx.Step("nop delete", func(tCtx ktesting.TContext) {
		informer.delete(nil)
		verify(tCtx, cache, key, newObj, newObj)
		events.verifyAndFlush(tCtx, nil)
	})

	// 删除对象。
	tCtx.Step("delete", func(tCtx ktesting.TContext) {
		informer.delete(oldObj)
		events.verifyAndFlush(tCtx, []event{{What: "delete", Obj: newObj}})
		_, err := cache.Get(key)
		if diff := cmp.Diff(ErrNotFound, err, cmpopts.EquateErrors()); diff != "" {
			tCtx.Errorf("Get did not return expected error: %v\ndiff (- expected, + actual):\n%s", err, diff)
		}
	})
}

// TestEventHandlers 验证多个事件处理器都能收到完整事件序列，且在 OnAdd 中访问缓存不会死锁。
func TestEventHandlers(t *testing.T) {
	tCtx, cache, informer := newTest(t)
	handlers := make([]mockEventHandler, 5)

	var objs []metav1.Object
	for i := 0; i < 5; i++ {
		objs = append(objs, makeObj(fmt.Sprintf("test-pvc%v", i), "1", ""))
		informer.add(objs[i])
	}

	// 在 OnAdd 回调中访问缓存绝不能死锁。
	handlers[0].cache = cache

	// 事件派发顺序是随机的，需自行按名称排序后再比较。
	var expectedEvents []event
	for _, obj := range objs {
		expectedEvents = append(expectedEvents,
			event{
				What:        "add",
				Obj:         obj,
				InitialList: true,
			},
		)
	}
	for i := range handlers {
		cache.AddEventHandler(&handlers[i])
		handlers[i].sortEvents(func(objI, objJ interface{}) bool {
			return objI.(*metav1.ObjectMeta).Name <
				objJ.(*metav1.ObjectMeta).Name
		})
		handlers[i].verifyAndFlush(tCtx, expectedEvents)
	}

	for i := 5; i < 7; i++ {
		objs = append(objs, makeObj(fmt.Sprintf("test-pvc%v", i), "1", ""))
		informer.add(objs[i])
		for e := range handlers {
			handlers[e].verifyAndFlush(tCtx, []event{{What: "add", Obj: objs[i]}})
		}
	}

	for i, oldObj := range objs {
		newObj := makeObj(fmt.Sprintf("test-pvc%v", i), "2", "")
		objs[i] = newObj
		informer.update(newObj)
		for e := range handlers {
			handlers[e].verifyAndFlush(tCtx, []event{{What: "update", OldObj: oldObj, Obj: newObj}})
		}
	}

	for _, obj := range objs {
		informer.delete(obj)
		for e := range handlers {
			handlers[e].verifyAndFlush(tCtx, []event{{What: "delete", Obj: obj}})
		}
	}
}

// TestEventHandlerConcurrency 验证并发 informer 更新时多个事件处理器仍能收到一致的事件序列。
func TestEventHandlerConcurrency(t *testing.T) {
	tCtx, cache, informer := newTest(t)
	handlers := make([]mockEventHandler, 5)

	var objs []metav1.Object
	for i := 0; i < 5; i++ {
		objs = append(objs, makeObj(fmt.Sprintf("test-pvc%v", i), "1", ""))
	}

	// 在 OnAdd 回调中访问缓存绝不能死锁。
	handlers[0].cache = cache

	// 每次 Add 都会阻塞，直到该上下文被取消。
	tCancelCtx := tCtx.WithCancel()
	var wg sync.WaitGroup

	for i := range handlers {
		handlers[i].block = tCancelCtx.Done()
		cache.AddEventHandler(&handlers[i])
	}

	// Add 调用的执行顺序随机，因此比较前要重新排序。
	var expectedEvents []event
	for _, obj := range objs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			informer.add(obj)
		}()
		expectedEvents = append(expectedEvents,
			event{
				What: "add",
				Obj:  obj,
			},
		)
	}

	tCancelCtx.Cancel("proceed")
	wg.Wait()

	for i := range handlers {
		handlers[i].sortEvents(func(objI, objJ interface{}) bool {
			return objI.(*metav1.ObjectMeta).Name <
				objJ.(*metav1.ObjectMeta).Name
		})
		handlers[i].verifyAndFlush(tCtx, expectedEvents)
	}
}

// TestListNoIndexer 验证无索引器时 List 能返回缓存中的全部对象。
func TestListNoIndexer(t *testing.T) {
	tCtx, cache, informer := newTest(t)

	// 加入一批对象。
	var objs []interface{}
	for i := 0; i < 10; i++ {
		obj := makeObj(fmt.Sprintf("test-pvc%v", i), "1", "")
		objs = append(objs, obj)
		informer.add(obj)
	}

	// 列出对象并校验。
	verifyList(tCtx.WithStep("after add"), cache, objs, "")

	// 更新一个对象。
	updatedObj := makeObj("test-pvc3", "2", "")
	objs[3] = updatedObj
	informer.update(updatedObj)

	// 列出对象并校验。
	verifyList(tCtx.WithStep("after update"), cache, objs, "")

	// 删除一个对象。
	deletedObj := objs[7]
	objs = slices.Delete(objs, 7, 8)
	informer.delete(deletedObj)

	// 列出对象并校验。
	verifyList(tCtx.WithStep("after delete"), cache, objs, "")
}

// TestListWithIndexer 验证按索引器条件 List 只返回匹配索引值的对象。
func TestListWithIndexer(t *testing.T) {
	namespaceIndexer := func(obj interface{}) ([]string, error) {
		objAccessor, err := meta.Accessor(obj)
		if err != nil {
			return nil, err
		}
		return []string{objAccessor.GetNamespace()}, nil
	}
	tCtx, cache, informer := newTestWithIndexer(t, "myNamespace", namespaceIndexer)

	// 加入一批对象。
	ns := "ns1"
	var objs []interface{}
	for i := 0; i < 10; i++ {
		obj := makeObj(fmt.Sprintf("test-pvc%v", i), "1", ns)
		objs = append(objs, obj)
		informer.add(obj)
	}

	// 加入另一命名空间的一批对象。
	for i := 0; i < 10; i++ {
		obj := makeObj(fmt.Sprintf("test-pvc%v", i), "1", "ns2")
		informer.add(obj)
	}

	// 列出对象并校验。
	verifyList(tCtx.WithStep("after add"), cache, objs, objs[0])

	// 更新一个对象。
	updatedObj := makeObj("test-pvc3", "2", ns)
	objs[3] = updatedObj
	informer.update(updatedObj)

	// 列出对象并校验。
	verifyList(tCtx.WithStep("after update"), cache, objs, objs[0])

	// 删除一个对象。
	deletedObj := objs[7]
	objs = slices.Delete(objs, 7, 8)
	informer.delete(deletedObj)

	// 列出对象并校验。
	verifyList(tCtx.WithStep("after delete"), cache, objs, objs[0])
}
