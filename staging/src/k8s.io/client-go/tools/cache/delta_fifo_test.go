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
	"reflect"
	"runtime"
	"testing"
	"time"
)

// List 返回所有条目，其中对象取自最新的 Delta。Delta 内对象应视为不可变。
// 该方法移到这里是因为它不符合通常的 List 语义，但单测中仍会用到。
func (f *DeltaFIFO) list() []interface{} {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.listLocked()
}

// 该方法移到这里是因为它不符合通常的 List 语义，但单测中仍会用到。
func (f *DeltaFIFO) listLocked() []interface{} {
	list := make([]interface{}, 0, len(f.items))
	for _, item := range f.items {
		list = append(list, item.Newest().Object)
	}
	return list
}

// ListKeys 返回当前所有对象的 key。
// 该方法移到这里是因为它不符合通常的 List 语义，但单测中仍会用到。
func (f *DeltaFIFO) listKeys() []string {
	f.lock.RLock()
	defer f.lock.RUnlock()
	list := make([]string, 0, len(f.queue))
	for _, key := range f.queue {
		list = append(list, key)
	}
	return list
}

// Get 返回指定条目的完整 Delta 列表，其中对象应视为不可变。
// 该方法移到这里是因为它不符合通常的 List 语义，但单测中仍会用到。
func (f *DeltaFIFO) get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := f.KeyOf(obj)
	if err != nil {
		return nil, false, KeyError{obj, err}
	}
	return f.getByKey(key)
}

// GetByKey 返回指定条目的完整 Delta 列表，列表为空时 exists=false。
// Delta 内对象应视为不可变。
// 该方法移到这里是因为它不符合通常的 List 语义，但单测中仍会用到。
func (f *DeltaFIFO) getByKey(key string) (item interface{}, exists bool, err error) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	d, exists := f.items[key]
	if exists {
		// 复制条目的 slice，避免后续操作影响原数据。
		// 不会影响返回的对象。
		d = copyDeltas(d)
	}
	return d, exists, nil
}

// 辅助函数，用于减少命名上的重复（stuttering）。
// testPop 取出下一个条目并返回其最新 delta 的对象；FIFO 为空时会阻塞，
// 因此只用于确定有数据的场景。
func testPop(f *DeltaFIFO) testFifoObject {
	return Pop(f).(Deltas).Newest().Object.(testFifoObject)
}

// testPopIfAvailable 在 Pop 返回 nil 对象时返回 `{}, false`。
// 用于后台消费协程：Close 后 Pop 返回 nil，它便自然退出。
func testPopIfAvailable(f *DeltaFIFO) (testFifoObject, bool) {
	obj := Pop(f)
	if obj == nil {
		return testFifoObject{}, false
	}
	return obj.(Deltas).Newest().Object.(testFifoObject), true
}

// literalListerGetter 是基于字面量列表的 KeyListerGetter。
// 该函数返回要 list/get 的对象 slice，且每次必须返回相同的对象。
type literalListerGetter func() []testFifoObject

var _ KeyListerGetter = literalListerGetter(nil)

// ListKeys 直接调用 kl。
func (kl literalListerGetter) ListKeys() []string {
	result := []string{}
	for _, fifoObj := range kl() {
		result = append(result, fifoObj.name)
	}
	return result
}

// GetByKey 在 kl 返回的列表中查找 key。
func (kl literalListerGetter) GetByKey(key string) (interface{}, bool, error) {
	for _, v := range kl() {
		if v.name == key {
			return v, true, nil
		}
	}
	return nil, false, nil
}

// TestDeltaFIFO_basic 用两个并发生产者验证：同一 key 的多条 delta 会合并，
// 且全局出队顺序稳定；每个 key 的值必须单调递增。
func TestDeltaFIFO_basic(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{KeyFunction: testFifoObjectKeyFunc})
	const amount = 500
	go func() {
		for i := 0; i < amount; i++ {
			f.Add(mkFifoObj(string([]rune{'a', rune(i)}), i+1))
		}
	}()
	go func() {
		for u := uint64(0); u < amount; u++ {
			f.Add(mkFifoObj(string([]rune{'b', rune(u)}), u+1))
		}
	}()

	lastInt := int(0)
	lastUint := uint64(0)
	for i := 0; i < amount*2; i++ {
		switch obj := testPop(f).val.(type) {
		case int:
			if obj <= lastInt {
				t.Errorf("got %v (int) out of order, last was %v", obj, lastInt)
			}
			lastInt = obj
		case uint64:
			if obj <= lastUint {
				t.Errorf("got %v (uint) out of order, last was %v", obj, lastUint)
			} else {
				lastUint = obj
			}
		default:
			t.Fatalf("unexpected type %#v", obj)
		}
	}
}

// TestDeltaFIFO_replaceWithDeleteDeltaIn 验证：调用 .Replace 且 `O` 位于待删除对象之中时，
// 会为 ID `X` 的对象 `O` 加入 `Sync` delta；若不加 `Sync` delta，则只会有 ID `X` 的
// `Delete` delta。
// 若不补 Sync delta，SharedIndexInformer 会漏掉 `O` 的创建通知，见
// https://github.com/kubernetes/kubernetes/issues/83810
// 详见相关说明。
func TestDeltaFIFO_replaceWithDeleteDeltaIn(t *testing.T) {
	oldObj := mkFifoObj("foo", 1)
	newObj := mkFifoObj("foo", 2)

	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{oldObj}
		}),
	})

	f.Delete(oldObj)
	f.Replace([]interface{}{newObj}, "")

	actualDeltas := Pop(f)
	expectedDeltas := Deltas{
		Delta{Type: Deleted, Object: oldObj},
		Delta{Type: Sync, Object: newObj},
	}
	if !reflect.DeepEqual(expectedDeltas, actualDeltas) {
		t.Errorf("expected %#v, got %#v", expectedDeltas, actualDeltas)
	}
}

// TestDeltaFIFOW_ReplaceMakesDeletionsForObjectsOnlyInQueue 验证 Replace 的删除
// 语义：队列中存在但新快照中缺失的对象应产生一条 Delete（或 tombstone），
// 且无论是否配置 KnownObjects 结果一致、不残留额外 delta。
func TestDeltaFIFOW_ReplaceMakesDeletionsForObjectsOnlyInQueue(t *testing.T) {
	obj := mkFifoObj("foo", 2)
	objV2 := mkFifoObj("foo", 3)
	table := []struct {
		name           string
		operations     func(f *DeltaFIFO)
		expectedDeltas Deltas
	}{
		{
			name: "Added object should be deleted on Replace",
			operations: func(f *DeltaFIFO) {
				f.Add(obj)
				f.Replace([]interface{}{}, "0")
			},
			expectedDeltas: Deltas{
				{Added, obj},
				{Deleted, DeletedFinalStateUnknown{Key: "foo", Obj: obj}},
			},
		},
		{
			name: "Replaced object should have only a single Delete",
			operations: func(f *DeltaFIFO) {
				f.emitDeltaTypeReplaced = true
				f.Add(obj)
				f.Replace([]interface{}{obj}, "0")
				f.Replace([]interface{}{}, "0")
			},
			expectedDeltas: Deltas{
				{Added, obj},
				{Replaced, obj},
				{Deleted, DeletedFinalStateUnknown{Key: "foo", Obj: obj}},
			},
		},
		{
			name: "Deleted object should have only a single Delete",
			operations: func(f *DeltaFIFO) {
				f.Add(obj)
				f.Delete(obj)
				f.Replace([]interface{}{}, "0")
			},
			expectedDeltas: Deltas{
				{Added, obj},
				{Deleted, obj},
			},
		},
		{
			name: "Synced objects should have a single delete",
			operations: func(f *DeltaFIFO) {
				f.Add(obj)
				f.Replace([]interface{}{obj}, "0")
				f.Replace([]interface{}{obj}, "0")
				f.Replace([]interface{}{}, "0")
			},
			expectedDeltas: Deltas{
				{Added, obj},
				{Sync, obj},
				{Sync, obj},
				{Deleted, DeletedFinalStateUnknown{Key: "foo", Obj: obj}},
			},
		},
		{
			name: "Added objects should have a single delete on multiple Replaces",
			operations: func(f *DeltaFIFO) {
				f.Add(obj)
				f.Replace([]interface{}{}, "0")
				f.Replace([]interface{}{}, "1")
			},
			expectedDeltas: Deltas{
				{Added, obj},
				{Deleted, DeletedFinalStateUnknown{Key: "foo", Obj: obj}},
			},
		},
		{
			name: "Added and deleted and added object should be deleted",
			operations: func(f *DeltaFIFO) {
				f.Add(obj)
				f.Delete(obj)
				f.Add(objV2)
				f.Replace([]interface{}{}, "0")
			},
			expectedDeltas: Deltas{
				{Added, obj},
				{Deleted, obj},
				{Added, objV2},
				{Deleted, DeletedFinalStateUnknown{Key: "foo", Obj: objV2}},
			},
		},
	}
	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			// 测试带 KnownObjects 后端的 DeltaFIFO。
			fWithKnownObjects := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
				KeyFunction: testFifoObjectKeyFunc,
				KnownObjects: literalListerGetter(func() []testFifoObject {
					return []testFifoObject{}
				}),
			})
			tt.operations(fWithKnownObjects)
			actualDeltasWithKnownObjects := Pop(fWithKnownObjects)
			if !reflect.DeepEqual(tt.expectedDeltas, actualDeltasWithKnownObjects) {
				t.Errorf("expected %#v, got %#v", tt.expectedDeltas, actualDeltasWithKnownObjects)
			}
			if len(fWithKnownObjects.items) != 0 {
				t.Errorf("expected no extra deltas (empty map), got %#v", fWithKnownObjects.items)
			}

			// 测试不带 KnownObjects 后端的 DeltaFIFO。
			fWithoutKnownObjects := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
				KeyFunction: testFifoObjectKeyFunc,
			})
			tt.operations(fWithoutKnownObjects)
			actualDeltasWithoutKnownObjects := Pop(fWithoutKnownObjects)
			if !reflect.DeepEqual(tt.expectedDeltas, actualDeltasWithoutKnownObjects) {
				t.Errorf("expected %#v, got %#v", tt.expectedDeltas, actualDeltasWithoutKnownObjects)
			}
			if len(fWithoutKnownObjects.items) != 0 {
				t.Errorf("expected no extra deltas (empty map), got %#v", fWithoutKnownObjects.items)
			}
		})
	}
}

// TestDeltaFIFO_addUpdate 验证同一 key 的 Add/Update/Delete 会合并为一个条目，
// Pop 只返回最新对象，消费后条目被移除。
func TestDeltaFIFO_addUpdate(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{KeyFunction: testFifoObjectKeyFunc})
	defer f.Close()
	f.Add(mkFifoObj("foo", 10))
	f.Update(mkFifoObj("foo", 12))
	f.Delete(mkFifoObj("foo", 15))

	if e, a := []interface{}{mkFifoObj("foo", 15)}, f.list(); !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %+v, got %+v", e, a)
	}
	if e, a := []string{"foo"}, f.listKeys(); !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %+v, got %+v", e, a)
	}

	got := make(chan testFifoObject, 2)
	go func() {
		for {
			obj, ok := testPopIfAvailable(f)
			if !ok {
				return
			}
			t.Logf("got a thing %#v", obj)
			t.Logf("D len: %v", len(f.queue))
			got <- obj
		}
	}()

	first := <-got
	if e, a := 15, first.val; e != a {
		t.Errorf("Didn't get updated value (%v), got %v", e, a)
	}
	select {
	case unexpected := <-got:
		t.Errorf("Got second value %v", unexpected.val)
	case <-time.After(50 * time.Millisecond):
	}
	_, exists, _ := f.get(mkFifoObj("foo", ""))
	if exists {
		t.Errorf("item did not get removed")
	}
}

// rvAndXfrm 在测试对象上记录资源版本与变换次数。
type rvAndXfrm struct {
	rv   int
	xfrm int
}

// TestDeltaFIFO_transformer 验证 Transformer 对每次入队恰好调用一次：
// 普通对象与 tombstone 包装的对象都不应被重复变换。
func TestDeltaFIFO_transformer(t *testing.T) {
	mk := func(name string, rv int) testFifoObject {
		return mkFifoObj(name, &rvAndXfrm{rv, 0})
	}
	xfrm := TransformFunc(func(obj interface{}) (interface{}, error) {
		switch v := obj.(type) {
		case testFifoObject:
			v.val.(*rvAndXfrm).xfrm++
		case DeletedFinalStateUnknown:
			if x := v.Obj.(testFifoObject).val.(*rvAndXfrm).xfrm; x != 1 {
				return nil, fmt.Errorf("object has been transformed wrong number of times: %#v", obj)
			}
		default:
			return nil, fmt.Errorf("unexpected object: %#v", obj)
		}
		return obj, nil
	})

	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}

	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		Transformer: xfrm,
	})
	must(f.Add(mk("foo", 10)))
	must(f.Add(mk("bar", 11)))
	must(f.Update(mk("foo", 12)))
	must(f.Delete(mk("foo", 15)))
	must(f.Replace([]interface{}{}, ""))
	must(f.Add(mk("bar", 16)))
	must(f.Replace([]interface{}{}, ""))

	// 这里预期为空。
	if e, a := []string{"foo", "bar"}, f.listKeys(); !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %+v, got %+v", e, a)
	}

	for i := 0; i < 2; i++ {
		obj, err := f.Pop(func(o interface{}, isInInitialList bool) error { return nil })
		if err != nil {
			t.Fatalf("got nothing on try %v?", i)
		}
		obj = obj.(Deltas).Newest().Object
		switch v := obj.(type) {
		case testFifoObject:
			if v.name != "foo" {
				t.Errorf("expected regular deletion of foo, got %q", v.name)
			}
			rx := v.val.(*rvAndXfrm)
			if rx.rv != 15 {
				t.Errorf("expected last message, got %#v", obj)
			}
			if rx.xfrm != 1 {
				t.Errorf("obj %v transformed wrong number of times.", obj)
			}
		case DeletedFinalStateUnknown:
			tf := v.Obj.(testFifoObject)
			rx := tf.val.(*rvAndXfrm)
			if tf.name != "bar" {
				t.Errorf("expected tombstone deletion of bar, got %q", tf.name)
			}
			if rx.rv != 16 {
				t.Errorf("expected last message, got %#v", obj)
			}
			if rx.xfrm != 1 {
				t.Errorf("tombstoned obj %v transformed wrong number of times.", obj)
			}
		default:
			t.Errorf("unknown item %#v", obj)
		}
	}
}

// TestDeltaFIFO_enqueueingNoLister 无 KnownObjects 时，删除一个从未出现过的 key
// 不应产生任何 delta。
func TestDeltaFIFO_enqueueingNoLister(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{KeyFunction: testFifoObjectKeyFunc})
	f.Add(mkFifoObj("foo", 10))
	f.Update(mkFifoObj("bar", 15))
	f.Add(mkFifoObj("qux", 17))
	f.Delete(mkFifoObj("qux", 18))

	// 这次删除不会入队，因为 baz 不存在。
	f.Delete(mkFifoObj("baz", 20))

	expectList := []int{10, 15, 18}
	for _, expect := range expectList {
		if e, a := expect, testPop(f).val; e != a {
			t.Errorf("Didn't get updated value (%v), got %v", e, a)
		}
	}
	if e, a := 0, len(f.items); e != a {
		t.Errorf("queue unexpectedly not empty: %v != %v\n%#v", e, a, f.items)
	}
}

// TestDeltaFIFO_enqueueingWithLister 有 KnownObjects 时，删除一个仅存在于
// KnownObjects 的 key 也应入队，确保 informer 能看到删除事件。
func TestDeltaFIFO_enqueueingWithLister(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
	})
	f.Add(mkFifoObj("foo", 10))
	f.Update(mkFifoObj("bar", 15))

	// 这次删除会入队，因为 "baz" 在 key lister 中。
	f.Delete(mkFifoObj("baz", 20))

	expectList := []int{10, 15, 20}
	for _, expect := range expectList {
		if e, a := expect, testPop(f).val; e != a {
			t.Errorf("Didn't get updated value (%v), got %v", e, a)
		}
	}
	if e, a := 0, len(f.items); e != a {
		t.Errorf("queue unexpectedly not empty: %v != %v", e, a)
	}
}

// TestDeltaFIFO_addReplace 验证 Add 后紧跟 Replace 时，只保留最新版本的
// 一次消费，不会产生重复出队。
func TestDeltaFIFO_addReplace(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{KeyFunction: testFifoObjectKeyFunc})
	defer f.Close()
	f.Add(mkFifoObj("foo", 10))
	f.Replace([]interface{}{mkFifoObj("foo", 15)}, "0")
	got := make(chan testFifoObject, 2)
	go func() {
		for {
			obj, ok := testPopIfAvailable(f)
			if !ok {
				return
			}
			got <- obj
		}
	}()

	first := <-got
	if e, a := 15, first.val; e != a {
		t.Errorf("Didn't get updated value (%v), got %v", e, a)
	}
	select {
	case unexpected := <-got:
		t.Errorf("Got second value %v", unexpected.val)
	case <-time.After(50 * time.Millisecond):
	}
	_, exists, _ := f.get(mkFifoObj("foo", ""))
	if exists {
		t.Errorf("item did not get removed")
	}
}

// TestDeltaFIFO_ResyncNonExisting 对已删除的 key 做 Resync 不应重新产生 Sync，
// 已有的 Delete 保持原样。
func TestDeltaFIFO_ResyncNonExisting(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5)}
		}),
	})
	f.Delete(mkFifoObj("foo", 10))
	f.Resync()

	deltas := f.items["foo"]
	if len(deltas) != 1 {
		t.Fatalf("unexpected deltas length: %v", deltas)
	}
	if deltas[0].Type != Deleted {
		t.Errorf("unexpected delta: %v", deltas[0])
	}
}

// TestDeltaFIFO_Resync 对 KnownObjects 中的每个对象为队列补一条 Sync delta。
func TestDeltaFIFO_Resync(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5)}
		}),
	})
	f.Resync()

	deltas := f.items["foo"]
	if len(deltas) != 1 {
		t.Fatalf("unexpected deltas length: %v", deltas)
	}
	if deltas[0].Type != Sync {
		t.Errorf("unexpected delta: %v", deltas[0])
	}
}

// TestDeltaFIFO_DeleteExistingNonPropagated 验证 Delete 会追加在已有 delta 之后，
// 而不是覆盖它们（历史 delta 仍需被消费者看到）。
func TestDeltaFIFO_DeleteExistingNonPropagated(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{}
		}),
	})
	f.Add(mkFifoObj("foo", 5))
	f.Delete(mkFifoObj("foo", 6))

	deltas := f.items["foo"]
	if len(deltas) != 2 {
		t.Fatalf("unexpected deltas length: %v", deltas)
	}
	if deltas[len(deltas)-1].Type != Deleted {
		t.Errorf("unexpected delta: %v", deltas[len(deltas)-1])
	}
}

// TestDeltaFIFO_ReplaceMakesDeletions 覆盖 Replace 生成删除事件的多种历史组合：
// 先有 Delete、先有 Add、队列中已删除又重建、以及替换前已 Sync 等场景，
// 重点确认 tombstone 携带的是“最新”的对象版本。
func TestDeltaFIFO_ReplaceMakesDeletions(t *testing.T) {
	// 只用一个已有对象测试，因为没有
	// 对删除操作的顺序作出的承诺。

	// 尝试以已有的 Delete 开始。
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
	})
	f.Delete(mkFifoObj("baz", 10))
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	expectedList := []Deltas{
		{{Deleted, mkFifoObj("baz", 10)}},
		{{Sync, mkFifoObj("foo", 5)}},
		// 由于 "bar" 没有 delete 事件且不在 Replace 列表中，它应得到带正确 Obj 的 tombstone key。
		{{Deleted, DeletedFinalStateUnknown{Key: "bar", Obj: mkFifoObj("bar", 6)}}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}

	// 现在改为以 Add 而非 Delete 开始。
	f = NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
	})
	f.Add(mkFifoObj("baz", 10))
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	expectedList = []Deltas{
		{{Added, mkFifoObj("baz", 10)},
			{Deleted, DeletedFinalStateUnknown{Key: "baz", Obj: mkFifoObj("baz", 10)}}},
		{{Sync, mkFifoObj("foo", 5)}},
		// 由于 "bar" 没有 delete 事件且不在 Replace 列表中，它应得到带正确 Obj 的 tombstone key。
		{{Deleted, DeletedFinalStateUnknown{Key: "bar", Obj: mkFifoObj("bar", 6)}}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}

	// 现在尝试在队列中删除并重建对象，再通过 Replace 删除它。
	f = NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
	})
	f.Delete(mkFifoObj("bar", 6))
	f.Add(mkFifoObj("bar", 100))
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	expectedList = []Deltas{
		{
			{Deleted, mkFifoObj("bar", 6)},
			{Added, mkFifoObj("bar", 100)},
			// 由于队列中 "bar" 的对象比状态中的更新，它应得到带队列中最新对象的 tombstone key。
			{Deleted, DeletedFinalStateUnknown{Key: "bar", Obj: mkFifoObj("bar", 100)}},
		},
		{{Sync, mkFifoObj("foo", 5)}},
		{{Deleted, DeletedFinalStateUnknown{Key: "baz", Obj: mkFifoObj("baz", 7)}}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}

	// 现在尝试先同步它，确保删除使用最新版本。
	f = NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
	})
	f.Replace([]interface{}{mkFifoObj("bar", 100), mkFifoObj("foo", 5)}, "0")
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	expectedList = []Deltas{
		{
			{Sync, mkFifoObj("bar", 100)},
			// 由于 "bar" 没有 delete 事件且不在 Replace 列表中，它应得到带正确 Obj 的 tombstone key。
			{Deleted, DeletedFinalStateUnknown{Key: "bar", Obj: mkFifoObj("bar", 100)}},
		},
		{
			{Sync, mkFifoObj("foo", 5)},
			{Sync, mkFifoObj("foo", 5)},
		},
		{{Deleted, DeletedFinalStateUnknown{Key: "baz", Obj: mkFifoObj("baz", 7)}}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}

	// 现在尝试不显式提供 KeyListerGetter。
	f = NewDeltaFIFOWithOptions(DeltaFIFOOptions{KeyFunction: testFifoObjectKeyFunc})
	f.Add(mkFifoObj("baz", 10))
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	expectedList = []Deltas{
		{{Added, mkFifoObj("baz", 10)},
			{Deleted, DeletedFinalStateUnknown{Key: "baz", Obj: mkFifoObj("baz", 10)}}},
		{{Sync, mkFifoObj("foo", 5)}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}
}

// TestDeltaFIFO_ReplaceMakesDeletionsReplaced 与上面的测试相同，但
// 确保会发出 Replaced 类型的 Delta。
func TestDeltaFIFO_ReplaceMakesDeletionsReplaced(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
		EmitDeltaTypeReplaced: true,
	})

	f.Delete(mkFifoObj("baz", 10))
	f.Replace([]interface{}{mkFifoObj("foo", 6)}, "0")

	expectedList := []Deltas{
		{{Deleted, mkFifoObj("baz", 10)}},
		{{Replaced, mkFifoObj("foo", 6)}},
		// 由于 "bar" 没有 delete 事件且不在 Replace 列表中，它应得到带正确 Obj 的 tombstone key。
		{{Deleted, DeletedFinalStateUnknown{Key: "bar", Obj: mkFifoObj("bar", 6)}}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}
}

// TestDeltaFIFO_ReplaceDeltaType 检查传入 EmitDeltaTypeReplaced 时
// 即证明 Replaced 被正确发出。
func TestDeltaFIFO_ReplaceDeltaType(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5)}
		}),
		EmitDeltaTypeReplaced: true,
	})
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	expectedList := []Deltas{
		{{Replaced, mkFifoObj("foo", 5)}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}
}

// TestDeltaFIFO_UpdateResyncRace 复现 Update 与 Resync 的竞态：
// Resync 不应覆盖或重复已存在的 Update delta。
func TestDeltaFIFO_UpdateResyncRace(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5)}
		}),
	})
	f.Update(mkFifoObj("foo", 6))
	f.Resync()

	expectedList := []Deltas{
		{{Updated, mkFifoObj("foo", 6)}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}
}

// pop2 与 Pop() 不同，同时捕获两个参数：
// 拿到对象和 isInInitialList 标志，用于验证初次同步状态。
func pop2[T any](queue Queue) (T, bool) {
	var result interface{}
	var isList bool
	queue.Pop(func(obj interface{}, isInInitialList bool) error {
		result = obj
		isList = isInInitialList
		return nil
	})
	return result.(T), isList
}

// TestDeltaFIFO_HasSyncedCorrectOnDeletion 验证 Replace 之后、所有初始条目
// （包括生成的删除事件）被消费之前，HasSynced 保持 false；全部出队后才为 true。
func TestDeltaFIFO_HasSyncedCorrectOnDeletion(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
	})
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	expectedList := []Deltas{
		{{Sync, mkFifoObj("foo", 5)}},
		// 由于 "bar" 没有 delete 事件且不在 Replace 列表中，它应得到带正确 Obj 的 tombstone key。
		{{Deleted, DeletedFinalStateUnknown{Key: "bar", Obj: mkFifoObj("bar", 6)}}},
		{{Deleted, DeletedFinalStateUnknown{Key: "baz", Obj: mkFifoObj("baz", 7)}}},
	}

	for _, expected := range expectedList {
		if f.HasSynced() {
			t.Errorf("Expected HasSynced to be false")
		}
		cur, initial := pop2[Deltas](f)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
		if initial != true {
			t.Error("Expected initial list item")
		}
	}
	if !f.HasSynced() {
		t.Errorf("Expected HasSynced to be true")
	}
}

// TestDeltaFIFO_detectLineJumpers 同一个 key 再次入队不应跳到队首：
// 它保持原有队列位置，但 Pop 时返回最新值。
func TestDeltaFIFO_detectLineJumpers(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{KeyFunction: testFifoObjectKeyFunc})

	f.Add(mkFifoObj("foo", 10))
	f.Add(mkFifoObj("bar", 1))
	f.Add(mkFifoObj("foo", 11))
	f.Add(mkFifoObj("foo", 13))
	f.Add(mkFifoObj("zab", 30))

	if e, a := 13, testPop(f).val; a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}

	f.Add(mkFifoObj("foo", 14)) // ensure foo doesn't jump back in line

	if e, a := 1, testPop(f).val; a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}

	if e, a := 30, testPop(f).val; a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}

	if e, a := 14, testPop(f).val; a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
}

// TestDeltaFIFO_KeyOf 验证 KeyOf 能从裸对象、tombstone 以及 Deltas 中
// 一致地提取 key。
func TestDeltaFIFO_KeyOf(t *testing.T) {
	f := DeltaFIFO{keyFunc: testFifoObjectKeyFunc}

	table := []struct {
		obj interface{}
		key string
	}{
		{obj: testFifoObject{name: "A"}, key: "A"},
		{obj: DeletedFinalStateUnknown{Key: "B", Obj: nil}, key: "B"},
		{obj: Deltas{{Object: testFifoObject{name: "C"}}}, key: "C"},
		{obj: Deltas{{Object: DeletedFinalStateUnknown{Key: "D", Obj: nil}}}, key: "D"},
	}

	for _, item := range table {
		got, err := f.KeyOf(item.obj)
		if err != nil {
			t.Errorf("Unexpected error for %q: %v", item.obj, err)
			continue
		}
		if e, a := item.key, got; e != a {
			t.Errorf("Expected %v, got %v", e, a)
		}
	}
}

// TestDeltaFIFO_HasSynced 以表驱动方式验证 HasSynced 的判定：
// 空 FIFO 为 false；Add 或空 Replace 后为 true；非空 Replace 必须等初始条目
// 全部消费后才为 true。
func TestDeltaFIFO_HasSynced(t *testing.T) {
	tests := []struct {
		actions        []func(f *DeltaFIFO)
		expectedSynced bool
	}{
		{
			actions:        []func(f *DeltaFIFO){},
			expectedSynced: false,
		},
		{
			actions: []func(f *DeltaFIFO){
				func(f *DeltaFIFO) { f.Add(mkFifoObj("a", 1)) },
			},
			expectedSynced: true,
		},
		{
			actions: []func(f *DeltaFIFO){
				func(f *DeltaFIFO) { f.Replace([]interface{}{}, "0") },
			},
			expectedSynced: true,
		},
		{
			actions: []func(f *DeltaFIFO){
				func(f *DeltaFIFO) { f.Replace([]interface{}{mkFifoObj("a", 1), mkFifoObj("b", 2)}, "0") },
			},
			expectedSynced: false,
		},
		{
			actions: []func(f *DeltaFIFO){
				func(f *DeltaFIFO) { f.Replace([]interface{}{mkFifoObj("a", 1), mkFifoObj("b", 2)}, "0") },
				func(f *DeltaFIFO) { Pop(f) },
			},
			expectedSynced: false,
		},
		{
			actions: []func(f *DeltaFIFO){
				func(f *DeltaFIFO) { f.Replace([]interface{}{mkFifoObj("a", 1), mkFifoObj("b", 2)}, "0") },
				func(f *DeltaFIFO) { Pop(f) },
				func(f *DeltaFIFO) { Pop(f) },
			},
			expectedSynced: true,
		},
		{
			// 该用例实际不会发生：目前 DeltaFIFO 唯一的生产者 Reflector 总会传入时间上一致的
			// 完整快照；列表里也不可能出现重复 key，除非 apiserver 有问题。
			actions: []func(f *DeltaFIFO){
				func(f *DeltaFIFO) { f.Replace([]interface{}{mkFifoObj("a", 1), mkFifoObj("a", 2)}, "0") },
				func(f *DeltaFIFO) { Pop(f) },
			},
			expectedSynced: true,
		},
	}

	for i, test := range tests {
		f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{KeyFunction: testFifoObjectKeyFunc})

		for _, action := range test.actions {
			action(f)
		}
		if e, a := test.expectedSynced, f.HasSynced(); a != e {
			t.Errorf("test case %v failed, expected: %v , got %v", i, e, a)
		}
	}
}

// TestDeltaFIFO_PopShouldUnblockWhenClosed 检查空队列上的阻塞 Pop 在关闭后解除阻塞。
// 调用 Close 后应解除阻塞并返回。
func TestDeltaFIFO_PopShouldUnblockWhenClosed(t *testing.T) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{
		KeyFunction: testFifoObjectKeyFunc,
		KnownObjects: literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5)}
		}),
	})

	c := make(chan struct{})
	const jobs = 10
	for i := 0; i < jobs; i++ {
		go func() {
			f.Pop(func(obj interface{}, isInInitialList bool) error {
				return nil
			})
			c <- struct{}{}
		}()
	}

	runtime.Gosched()
	f.Close()

	for i := 0; i < jobs; i++ {
		select {
		case <-c:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for Pop to return after Close")
		}
	}
}

// BenchmarkDeltaFIFOListKeys 评估大量条目下 listKeys 的并发读性能；
// 重点在于 RLock 路径不应随条目数量产生额外拷贝开销。
func BenchmarkDeltaFIFOListKeys(b *testing.B) {
	f := NewDeltaFIFOWithOptions(DeltaFIFOOptions{KeyFunction: testFifoObjectKeyFunc})
	const amount = 10000

	for i := 0; i < amount; i++ {
		f.Add(mkFifoObj(string([]rune{'a', rune(i)}), i+1))
	}
	for u := uint64(0); u < amount; u++ {
		f.Add(mkFifoObj(string([]rune{'b', rune(u)}), u+1))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = f.listKeys()
		}
	})
	b.StopTimer()
}
