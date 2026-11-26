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

package cache

import (
	"sync"
	"testing"
	"time"
)

// testHeapObjectKeyFunc 从 testHeapObject 提取 name 作为堆的键。
func testHeapObjectKeyFunc(obj interface{}) (string, error) {
	return obj.(testHeapObject).name, nil
}

// testHeapObject 是测试用的堆元素：name 作为键，val 作为比较值。
type testHeapObject struct {
	name string
	val  interface{}
}

func mkHeapObj(name string, val interface{}) testHeapObject {
	return testHeapObject{name: name, val: val}
}

// compareInts 定义最小堆比较：val 小的元素优先出堆。
func compareInts(val1 interface{}, val2 interface{}) bool {
	first := val1.(testHeapObject).val.(int)
	second := val2.(testHeapObject).val.(int)
	return first < second
}

// TestHeapBasic 测试 Heap 的不变式与同步行为。
//
// 并发写入 + 单线程读取：验证 Heap 内部加锁正确，且最终弹出顺序为升序。
func TestHeapBasic(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	var wg sync.WaitGroup
	wg.Add(2)
	const amount = 500
	var i, u int
	// 两个 goroutine 以相反顺序向堆中插入元素，制造并发乱序写入。
	go func() {
		for i = amount; i > 0; i-- {
			h.Add(mkHeapObj(string([]rune{'a', rune(i)}), i))
		}
		wg.Done()
	}()
	go func() {
		for u = 0; u < amount; u++ {
			h.Add(mkHeapObj(string([]rune{'b', rune(u)}), u+1))
		}
		wg.Done()
	}()
	// 等待两个 goroutine 结束。
	wg.Wait()
	// 确认数字按升序弹出。
	prevNum := 0
	for i := 0; i < amount*2; i++ {
		obj, err := h.Pop()
		num := obj.(testHeapObject).val.(int)
		// 所有条目都必须有序。
		if err != nil || prevNum > num {
			t.Errorf("got %v out of order, last was %v", obj, prevNum)
		}
		prevNum = num
	}
}

// 测试 Heap.Add，确保添加条目后不变式仍成立。
func TestHeap_Add(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))
	h.Add(mkHeapObj("baz", 11))
	h.Add(mkHeapObj("zab", 30))
	h.Add(mkHeapObj("foo", 13)) // This updates "foo".
	// 同名元素再次 Add 视为更新：foo 的值变为 13，不会出现两个 foo。

	item, err := h.Pop()
	if e, a := 1, item.(testHeapObject).val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	item, err = h.Pop()
	if e, a := 11, item.(testHeapObject).val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	h.Delete(mkHeapObj("baz", 11)) // Nothing is deleted.
	// 刚弹出的 baz 已不在堆中，Delete 不产生效果（按 key 定位）。
	h.Add(mkHeapObj("foo", 14)) // foo is updated.
	item, err = h.Pop()
	if e, a := 14, item.(testHeapObject).val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	item, err = h.Pop()
	if e, a := 30, item.(testHeapObject).val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
}

// TestHeap_BulkAdd 测试 Heap.BulkAdd，并确保所有条目入队后不变式仍成立。
// 传给 BulkAdd 的条目在 Pop 读取前就已加入队列。
func TestHeap_BulkAdd(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	const amount = 500
	// 在一个 goroutine 中以相反顺序向堆中插入元素。
	// BulkAdd 与并发 Pop 交错执行，验证批量写入具备原子性（Pop 不会读到半批数据）。
	go func() {
		l := []interface{}{}
		for i := amount; i > 0; i-- {
			l = append(l, mkHeapObj(string([]rune{'a', rune(i)}), i))
		}
		h.BulkAdd(l)
	}()
	prevNum := -1
	for i := 0; i < amount; i++ {
		obj, err := h.Pop()
		num := obj.(testHeapObject).val.(int)
		// 所有条目都必须有序。
		if err != nil || prevNum >= num {
			t.Errorf("got %v out of order, last was %v", obj, prevNum)
		}
		prevNum = num
	}
}

// TestHeapEmptyPop 测试 heap 关闭后 pop 能正常返回。
//
// 验证空堆上 Pop 会阻塞，直到 Close 后返回 closed 错误，而不是永久挂起。
func TestHeapEmptyPop(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	go func() {
		time.Sleep(1 * time.Second)
		h.Close()
	}()
	_, err := h.Pop()
	if err == nil || err.Error() != closedMsg {
		t.Errorf("pop should have returned heap closed error: %v", err)
	}
}

// TestHeap_AddIfNotPresent 测试 Heap.AddIfNotPresent，并确保添加条目后不变式仍成立。
//
// 已存在的键不会被覆盖；元素被弹出后可以再次加入。
func TestHeap_AddIfNotPresent(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.AddIfNotPresent(mkHeapObj("foo", 10))
	h.AddIfNotPresent(mkHeapObj("bar", 1))
	h.AddIfNotPresent(mkHeapObj("baz", 11))
	h.AddIfNotPresent(mkHeapObj("zab", 30))
	h.AddIfNotPresent(mkHeapObj("foo", 13)) // This is not added.
	// foo 已存在，因此仍为 10，元素总数保持 4。

	if len := len(h.data.items); len != 4 {
		t.Errorf("unexpected number of items: %d", len)
	}
	if val := h.data.items["foo"].obj.(testHeapObject).val; val != 10 {
		t.Errorf("unexpected value: %d", val)
	}
	item, err := h.Pop()
	if e, a := 1, item.(testHeapObject).val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	item, err = h.Pop()
	if e, a := 10, item.(testHeapObject).val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	// bar 已被弹出，再加一个。
	h.AddIfNotPresent(mkHeapObj("bar", 14))
	item, err = h.Pop()
	if e, a := 11, item.(testHeapObject).val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	item, err = h.Pop()
	if e, a := 14, item.(testHeapObject).val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
}

// TestHeap_Delete 测试 Heap.Delete，并确保删除条目后不变式仍成立。
// 删除条目后不变式仍然保持。
//
// 覆盖删除头、尾、不存在的元素以及重复值中的一个。
func TestHeap_Delete(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))
	h.Add(mkHeapObj("bal", 31))
	h.Add(mkHeapObj("baz", 11))

	// 删除 head；Delete 只关心 key，不关心 value。
	if err := h.Delete(mkHeapObj("bar", 200)); err != nil {
		t.Fatalf("Failed to delete head.")
	}
	item, err := h.Pop()
	if e, a := 10, item.(testHeapObject).val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	h.Add(mkHeapObj("zab", 30))
	h.Add(mkHeapObj("faz", 30))
	len := h.data.Len()
	// 删除不存在的条目。
	if err = h.Delete(mkHeapObj("non-existent", 10)); err == nil || len != h.data.Len() {
		t.Fatalf("Didn't expect any item removal")
	}
	// 删除 tail。
	if err = h.Delete(mkHeapObj("bal", 31)); err != nil {
		t.Fatalf("Failed to delete tail.")
	}
	// 删除一个 value 为 30 的条目。
	if err = h.Delete(mkHeapObj("zab", 30)); err != nil {
		t.Fatalf("Failed to delete item.")
	}
	item, err = h.Pop()
	if e, a := 11, item.(testHeapObject).val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	item, err = h.Pop()
	if e, a := 30, item.(testHeapObject).val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	if h.data.Len() != 0 {
		t.Fatalf("expected an empty heap.")
	}
}

// TestHeap_Update 测试 Heap.Update，并确保更新条目后不变式仍成立。
// 添加条目后不变式仍然保持。
//
// 更新元素值后需上浮或下沉，验证索引维护与堆序正确。
func TestHeap_Update(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))
	h.Add(mkHeapObj("bal", 31))
	h.Add(mkHeapObj("baz", 11))

	// 把条目更新为应被推到队首的值。
	h.Update(mkHeapObj("baz", 0))
	if h.data.queue[0] != "baz" || h.data.items["baz"].index != 0 {
		t.Fatalf("expected baz to be at the head")
	}
	item, err := h.Pop()
	if e, a := 0, item.(testHeapObject).val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	// 更新 bar，把它推到队列更靠后的位置。
	h.Update(mkHeapObj("bar", 100))
	if h.data.queue[0] != "foo" || h.data.items["foo"].index != 0 {
		t.Fatalf("expected foo to be at the head")
	}
}

// TestHeap_Get 测试 Heap.Get。
//
// Get 仅按 key 查找，传入对象的 val 可任意。
func TestHeap_Get(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))
	h.Add(mkHeapObj("bal", 31))
	h.Add(mkHeapObj("baz", 11))

	// Get 使用 key 工作。
	obj, exists, err := h.Get(mkHeapObj("baz", 0))
	if err != nil || exists == false || obj.(testHeapObject).val != 11 {
		t.Fatalf("unexpected error in getting element")
	}
	// Get 不存在的对象。
	_, exists, err = h.Get(mkHeapObj("non-existing", 0))
	if err != nil || exists {
		t.Fatalf("didn't expect to get any object")
	}
}

// TestHeap_GetByKey 测试 Heap.GetByKey，与 TestHeap_Get 非常相似。
func TestHeap_GetByKey(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))
	h.Add(mkHeapObj("bal", 31))
	h.Add(mkHeapObj("baz", 11))

	obj, exists, err := h.GetByKey("baz")
	if err != nil || exists == false || obj.(testHeapObject).val != 11 {
		t.Fatalf("unexpected error in getting element")
	}
	// Get 不存在的对象。
	_, exists, err = h.GetByKey("non-existing")
	if err != nil || exists {
		t.Fatalf("didn't expect to get any object")
	}
}

// TestHeap_Close 测试 Heap.Close 与 Heap.IsClosed。
func TestHeap_Close(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))

	if h.IsClosed() {
		t.Fatalf("didn't expect heap to be closed")
	}
	h.Close()
	if !h.IsClosed() {
		t.Fatalf("expect heap to be closed")
	}
}

// TestHeap_List 测试 Heap.List。
//
// List 返回无序快照，因此只校验元素集合一致。
func TestHeap_List(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	list := h.List()
	if len(list) != 0 {
		t.Errorf("expected an empty list")
	}

	items := map[string]int{
		"foo": 10,
		"bar": 1,
		"bal": 30,
		"baz": 11,
		"faz": 30,
	}
	for k, v := range items {
		h.Add(mkHeapObj(k, v))
	}
	list = h.List()
	if len(list) != len(items) {
		t.Errorf("expected %d items, got %d", len(items), len(list))
	}
	for _, obj := range list {
		heapObj := obj.(testHeapObject)
		v, ok := items[heapObj.name]
		if !ok || v != heapObj.val {
			t.Errorf("unexpected item in the list: %v", heapObj)
		}
	}
}

// TestHeap_ListKeys 测试 Heap.ListKeys，场景与上面相同。
// TestHeap_list.
func TestHeap_ListKeys(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	list := h.ListKeys()
	if len(list) != 0 {
		t.Errorf("expected an empty list")
	}

	items := map[string]int{
		"foo": 10,
		"bar": 1,
		"bal": 30,
		"baz": 11,
		"faz": 30,
	}
	for k, v := range items {
		h.Add(mkHeapObj(k, v))
	}
	list = h.ListKeys()
	if len(list) != len(items) {
		t.Errorf("expected %d items, got %d", len(items), len(list))
	}
	for _, key := range list {
		_, ok := items[key]
		if !ok {
			t.Errorf("unexpected item in the list: %v", key)
		}
	}
}

// TestHeapAddAfterClose 测试 heap 关闭后添加条目会返回错误。
// 在它关闭之后。
//
// 覆盖 Add、AddIfNotPresent、BulkAdd 三种写入路径都必须返回 closed 错误。
func TestHeapAddAfterClose(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Close()
	if err := h.Add(mkHeapObj("test", 1)); err == nil || err.Error() != closedMsg {
		t.Errorf("expected heap closed error")
	}
	if err := h.AddIfNotPresent(mkHeapObj("test", 1)); err == nil || err.Error() != closedMsg {
		t.Errorf("expected heap closed error")
	}
	if err := h.BulkAdd([]interface{}{mkHeapObj("test", 1)}); err == nil || err.Error() != closedMsg {
		t.Errorf("expected heap closed error")
	}
}
