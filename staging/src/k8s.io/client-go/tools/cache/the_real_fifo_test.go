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
	"fmt"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
)

// getItems 在锁内复制一份待处理 delta 列表，避免测试与后台 Pop 竞争。
func (f *RealFIFO) getItems() []Delta {
	f.lock.Lock()
	defer f.lock.Unlock()

	ret := make([]Delta, len(f.items))
	copy(ret, f.items)
	return ret
}

// 这些哨兵名让 testRealFIFOPop 能把“队列关闭/原子替换/bookmark”也当成普通值断言。
const closedFIFOName = "FIFO WAS CLOSED"
const isAtomic = "ATOMIC REPLACED OBJ"
const isBookmark = "BOOKMARK OBJ"

// popN 连续 Pop n 次并收集对象，用于把队列内容展开成 Deltas。
func popN(queue Queue, count int) []interface{} {
	result := []interface{}{}
	for i := 0; i < count; i++ {
		queue.Pop(func(obj interface{}, isInInitialList bool) error {
			result = append(result, obj)
			return nil
		})
	}
	return result
}

// 辅助函数，减少重复代码。
// 把 Pop 的结果归一化为 testFifoObject：关闭、ReplacedAll、Bookmark 都用特殊 name 表示。
func testRealFIFOPop(f *RealFIFO) testFifoObject {
	val := Pop(f)
	if val == nil {
		return testFifoObject{name: closedFIFOName}
	}
	if val.(Deltas).Newest().Type == ReplacedAll {
		var objs []testFifoObject
		for _, obj := range val.(Deltas).Newest().Object.(ReplacedAllInfo).Objects {
			objs = append(objs, obj.(testFifoObject))
		}
		return testFifoObject{name: isAtomic, val: objs}
	}
	if val.(Deltas).Newest().Type == Bookmark {
		return testFifoObject{name: isBookmark}
	}
	return val.(Deltas).Newest().Object.(testFifoObject)
}

// emptyKnownObjects 返回空的 KeyListerGetter，表示 FIFO 不持有任何已知对象快照。
func emptyKnownObjects() KeyListerGetter {
	return literalListerGetter(
		func() []testFifoObject {
			return []testFifoObject{}
		},
	)
}

// TestRealFIFO_basic 校验并发写入时，同一来源（int/uint）内部的顺序不乱。
func TestRealFIFO_basic(t *testing.T) {
	f := NewRealFIFO(testFifoObjectKeyFunc, emptyKnownObjects(), nil)
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
		switch obj := testRealFIFOPop(f).val.(type) {
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

// TestRealFIFO_replaceWithDeleteDeltaIn 验证：调用 .Replace 且 `O` 位于待删除对象之中时，
// 会为 ID `X` 的对象 `O` 加入 `Sync` delta；若不加 `Sync` delta，只会有 ID `X` 的
// `Delete` delta。注意：与 delta_fifo_test 不同，RealFIFO 没有 emitDeltaTypeReplaced 选项。
// 若不补 Sync delta，SharedIndexInformer 会漏掉 `O` 的创建通知，见
// https://github.com/kubernetes/kubernetes/issues/83810
// 详见相关说明。
func TestRealFIFO_replaceWithDeleteDeltaIn(t *testing.T) {
	oldObj := mkFifoObj("foo", 1)
	newObj := mkFifoObj("foo", 2)

	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		literalListerGetter(func() []testFifoObject {
			return []testFifoObject{oldObj}
		}),
		nil,
	)

	f.Delete(oldObj)
	f.Replace([]interface{}{newObj}, "")

	actualDeltas := f.getItems()
	expectedDeltas := []Delta{
		{Type: Deleted, Object: oldObj},
		{Type: Replaced, Object: newObj},
	}
	if !reflect.DeepEqual(expectedDeltas, actualDeltas) {
		t.Errorf("expected %#v, got %#v", expectedDeltas, actualDeltas)
	}
}

// TestRealFIFOW_ReplaceMakesDeletionsForObjectsOnlyInQueue 校验 Replace 对“仅在队列中”的对象
// 产生删除，且删除只产生一次；涵盖先加后删、重复 Replace、bookmark 等组合。
func TestRealFIFOW_ReplaceMakesDeletionsForObjectsOnlyInQueue(t *testing.T) {
	obj := mkFifoObj("foo", 2)
	objV2 := mkFifoObj("foo", 3)
	table := []struct {
		name           string
		operations     func(f *RealFIFO)
		expectedDeltas Deltas
	}{
		{
			name: "Added object should be deleted on Replace",
			operations: func(f *RealFIFO) {
				f.Add(obj)
				f.Replace([]interface{}{}, "0")
			},
			expectedDeltas: Deltas{
				{Added, obj},
				{Deleted, DeletedFinalStateUnknown{Key: "foo", Obj: obj}},
			},
		},
		//{
		//	// 注意：与 delta_fifo_test 不同，没有 emitDeltaTypeReplaced 选项。
		//	name: "替换后的对象应只有一个 Delete",
		//	operations: func(f *RealFIFO) {
		//		f.emitDeltaTypeReplaced = true
		//		f.Add(obj)
		//		f.Replace([]interface{}{obj}, "0")
		//		f.Replace([]interface{}{}, "0")
		//	},
		//	expectedDeltas: Deltas{
		//		{Added, obj},
		//		{Replaced, obj},
		//		{Deleted, DeletedFinalStateUnknown{Key: "foo", Obj: obj}},
		//	},
		//},
		{
			name: "Deleted object should have only a single Delete",
			operations: func(f *RealFIFO) {
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
			operations: func(f *RealFIFO) {
				f.Add(obj)
				f.Replace([]interface{}{obj}, "0")
				f.Replace([]interface{}{obj}, "0")
				f.Replace([]interface{}{}, "0")
			},
			expectedDeltas: Deltas{
				{Added, obj},
				{Replaced, obj},
				{Replaced, obj},
				{Deleted, DeletedFinalStateUnknown{Key: "foo", Obj: obj}},
			},
		},
		{
			name: "Added objects should have a single delete on multiple Replaces",
			operations: func(f *RealFIFO) {
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
			operations: func(f *RealFIFO) {
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
		{
			name: "Bookmark object should not be added without atomic",
			operations: func(f *RealFIFO) {
				f.bookmarkTest(t, "123")
				f.replaceTest(t, []interface{}{}, "0")
				f.replaceTest(t, []interface{}{}, "1")
			},
			expectedDeltas: Deltas{},
		},
	}
	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			// 测试带 KnownObjects 后端的 RealFIFO。
			fWithKnownObjects := NewRealFIFO(
				testFifoObjectKeyFunc,
				literalListerGetter(func() []testFifoObject {
					return []testFifoObject{}
				}),
				nil,
			)
			tt.operations(fWithKnownObjects)
			actualDeltasWithKnownObjects := popN(fWithKnownObjects, len(fWithKnownObjects.getItems()))
			actualAsDeltas := collapseDeltas(actualDeltasWithKnownObjects)
			if !reflect.DeepEqual(tt.expectedDeltas, actualAsDeltas) {
				t.Errorf("expected %#v, got %#v", tt.expectedDeltas, actualAsDeltas)
			}
			if len(fWithKnownObjects.items) != 0 {
				t.Errorf("expected no extra deltas (empty map), got %#v", fWithKnownObjects.items)
			}

			// 注意：与 delta_fifo_test 不同，没有不带 knownObjects 的选项。
		})
	}
}

// collapseDeltas 把多次 Pop 得到的 Deltas 展平成一个按时间排序的 Deltas。
func collapseDeltas(ins []interface{}) Deltas {
	ret := Deltas{}
	for _, curr := range ins {
		for _, delta := range curr.(Deltas) {
			ret = append(ret, delta)
		}
	}
	return ret
}

// 注意：与 delta_fifo_test 不同，已没有 requeue 选项。
// func TestDeltaFIFO_requeueOnPop(t *testing.T) {

// TestRealFIFO_addUpdate 校验 Add/Update/Delete 全部按发生顺序入队，且 Pop 后队列清空。
func TestRealFIFO_addUpdate(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		emptyKnownObjects(),
		nil,
	)
	f.Add(mkFifoObj("foo", 10))
	f.Update(mkFifoObj("foo", 12))
	f.Delete(mkFifoObj("foo", 15))

	// 注意：与 delta_fifo_test 不同，列表中包含所有条目；DeltaFIFO.List 只显示最新条目，但 Pop 会处理全部。
	expected1 := []Delta{
		{
			Type:   Added,
			Object: mkFifoObj("foo", 10),
		},
		{
			Type:   Updated,
			Object: mkFifoObj("foo", 12),
		},
		{
			Type:   Deleted,
			Object: mkFifoObj("foo", 15),
		},
	}
	if e, a := expected1, f.getItems(); !reflect.DeepEqual(e, a) {
		t.Errorf("Expected %+v, got %+v", e, a)
	}

	got := make(chan testFifoObject, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			obj := testRealFIFOPop(f)
			if obj.name == closedFIFOName {
				break
			}
			t.Logf("got a thing %#v", obj)
			t.Logf("D len: %v", len(f.items))
			got <- obj
		}
	}()

	first := <-got
	if e, a := 10, first.val; e != a {
		t.Errorf("Didn't get updated value (%v), got %v", e, a)
	}
	second := <-got
	if e, a := 12, second.val; e != a {
		t.Errorf("Didn't get updated value (%v), got %v", e, a)
	}
	third := <-got
	if e, a := 15, third.val; e != a {
		t.Errorf("Didn't get updated value (%v), got %v", e, a)
	}
	select {
	case unexpected := <-got:
		t.Errorf("Got second value %v", unexpected.val)
	case <-time.After(50 * time.Millisecond):
	}

	if e, a := 0, len(f.getItems()); e != a {
		t.Errorf("Didn't get updated value (%v), got %v", e, a)
	}
	f.Close()
	<-done
}

// TestRealFIFO_transformer 校验 TransformFunc 只作用一次，且对 DeletedFinalStateUnknown
// 包裹的对象也不会重复变换。
func TestRealFIFO_transformer(t *testing.T) {
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
	mustTransform := func(obj interface{}) interface{} {
		ret, err := xfrm(obj)
		must(err)
		return ret
	}

	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		emptyKnownObjects(),
		xfrm,
	)
	must(f.Add(mk("foo", 10)))
	must(f.Add(mk("bar", 11)))
	must(f.Update(mk("foo", 12)))
	must(f.Delete(mk("foo", 15)))
	must(f.Replace([]interface{}{}, ""))
	must(f.Add(mk("bar", 16)))
	must(f.Replace([]interface{}{}, ""))

	// 注意：与 delta_fifo_test 不同，不做压缩，保留所有条目，包括多次被删除的 bar。
	//            DeltaFIFO 先检查 key，而 RealFIFO 先检查类型和 key。
	expected1 := []Delta{
		{Type: Added, Object: mustTransform(mk("foo", 10))},
		{Type: Added, Object: mustTransform(mk("bar", 11))},
		{Type: Updated, Object: mustTransform(mk("foo", 12))},
		{Type: Deleted, Object: mustTransform(mk("foo", 15))},
		{Type: Deleted, Object: DeletedFinalStateUnknown{Key: "bar", Obj: mustTransform(mk("bar", 11))}},
		{Type: Added, Object: mustTransform(mk("bar", 16))},
		{Type: Deleted, Object: DeletedFinalStateUnknown{Key: "bar", Obj: mustTransform(mk("bar", 16))}},
	}
	actual1 := f.getItems()
	if len(expected1) != len(actual1) {
		t.Fatalf("Expected %+v, got %+v", expected1, actual1)
	}
	for i := 0; i < len(actual1); i++ {
		e := expected1[i]
		a := actual1[i]
		if e.Type != a.Type {
			t.Errorf("%d Expected %+v, got %+v", i, e, a)
		}
		eKey, err := f.keyOf(e)
		if err != nil {
			t.Fatal(err)
		}
		aKey, err := f.keyOf(a)
		if err != nil {
			t.Fatal(err)
		}
		if eKey != aKey {
			t.Errorf("%d Expected %+v, got %+v", i, eKey, aKey)
		}
	}

	for i := 0; i < len(expected1); i++ {
		obj, err := f.Pop(func(o interface{}, isInInitialList bool) error { return nil })
		if err != nil {
			t.Fatalf("got nothing on try %v?", i)
		}
		a := obj.(Deltas)[0]
		e := expected1[i]
		if !reflect.DeepEqual(e, a) {
			t.Errorf("%d Expected %+v, got %+v", i, e, a)
		}
	}
}

// TestRealFIFO_enqueueingNoLister 校验没有 KnownObjects 时所有操作都被入队，包括删除。
func TestRealFIFO_enqueueingNoLister(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		emptyKnownObjects(),
		nil,
	)
	f.Add(mkFifoObj("foo", 10))
	f.Update(mkFifoObj("bar", 15))
	f.Add(mkFifoObj("qux", 17))
	f.Delete(mkFifoObj("qux", 18))

	// RealFIFO 对所有内容都入队。
	f.Delete(mkFifoObj("baz", 20))

	// 注意：与 delta_fifo_test 不同，不做压缩，每个条目都入队。
	expectList := []int{10, 15, 17, 18, 20}
	for _, expect := range expectList {
		if e, a := expect, testRealFIFOPop(f).val; e != a {
			t.Errorf("Didn't get updated value (%v), got %v", e, a)
		}
	}
	if e, a := 0, len(f.getItems()); e != a {
		t.Errorf("queue unexpectedly not empty: %v != %v\n%#v", e, a, f.getItems())
	}
}

// TestRealFIFO_enqueueingWithLister 校验删除已知对象会被入队，从而让消费者感知删除。
func TestRealFIFO_enqueueingWithLister(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
		nil,
	)
	f.Add(mkFifoObj("foo", 10))
	f.Update(mkFifoObj("bar", 15))

	// 这次删除会入队，因为 "baz" 在 key lister 中。
	f.Delete(mkFifoObj("baz", 20))

	expectList := []int{10, 15, 20}
	for _, expect := range expectList {
		if e, a := expect, testRealFIFOPop(f).val; e != a {
			t.Errorf("Didn't get updated value (%v), got %v", e, a)
		}
	}
	if e, a := 0, len(f.items); e != a {
		t.Errorf("queue unexpectedly not empty: %v != %v", e, a)
	}
}

// TestRealFIFO_addReplace 校验 Add 之后 Replace 会同时保留旧事件与快照事件，按顺序交付。
func TestRealFIFO_addReplace(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		emptyKnownObjects(),
		nil,
	)
	f.Add(mkFifoObj("foo", 10))
	f.Replace([]interface{}{mkFifoObj("foo", 15)}, "0")
	got := make(chan testFifoObject, 3)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			obj := testRealFIFOPop(f)
			if obj.name == closedFIFOName {
				break
			}
			got <- obj
		}
	}()

	// 注意：与 delta_fifo_test 不同，会得到每个事件而非 .Newest，后者会让测试跳过一些事件（运行时不会）。
	curr := <-got
	if e, a := 10, curr.val; e != a {
		t.Errorf("Didn't get updated value (%v), got %v", e, a)
	}
	curr = <-got
	if e, a := 15, curr.val; e != a {
		t.Errorf("Didn't get updated value (%v), got %v", e, a)
	}

	select {
	case unexpected := <-got:
		t.Errorf("Got second value %v", unexpected.val)
	case <-time.After(50 * time.Millisecond):
	}

	if items := f.getItems(); len(items) > 0 {
		t.Errorf("item did not get removed")
	}
	f.Close()
	<-done
}

// TestRealFIFO_ResyncNonExisting 校验对已删除且不在 KnownObjects 中的对象执行 Resync 不再产生 Sync。
func TestRealFIFO_ResyncNonExisting(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5)}
		}),
		nil,
	)
	f.Delete(mkFifoObj("foo", 10))
	f.Resync()

	deltas := f.getItems()
	if len(deltas) != 1 {
		t.Fatalf("unexpected deltas length: %v", deltas)
	}
	if deltas[0].Type != Deleted {
		t.Errorf("unexpected delta: %v", deltas[0])
	}
}

// TestRealFIFO_Resync 校验 Resync 为 KnownObjects 中的对象生成 Sync delta。
func TestRealFIFO_Resync(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5)}
		}),
		nil,
	)
	f.Resync()

	deltas := f.getItems()
	if len(deltas) != 1 {
		t.Fatalf("unexpected deltas length: %v", deltas)
	}
	if deltas[0].Type != Sync {
		t.Errorf("unexpected delta: %v", deltas[0])
	}
}

// TestRealFIFO_DeleteExistingNonPropagated 校验先 Add 再 Delete 时两条 delta 都被保留，
// 消费者因此能先看到创建再看到删除。
func TestRealFIFO_DeleteExistingNonPropagated(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		emptyKnownObjects(),
		nil,
	)
	f.Add(mkFifoObj("foo", 5))
	f.Delete(mkFifoObj("foo", 6))

	deltas := f.getItems()
	if len(deltas) != 2 {
		t.Fatalf("unexpected deltas length: %v", deltas)
	}
	if deltas[len(deltas)-1].Type != Deleted {
		t.Errorf("unexpected delta: %v", deltas[len(deltas)-1])
	}
}

// TestRealFIFO_ReplaceMakesDeletions 覆盖 Replace 为“消失的已知对象”生成 tombstone 删除的各种情形，
// 包括对象在队列中被删除后又重建、以及多次 Replace 的顺序。
func TestRealFIFO_ReplaceMakesDeletions(t *testing.T) {
	// 只用一个已有对象测试，因为没有
	// 对删除操作的顺序作出的承诺。

	// 尝试以已有的 Delete 开始。
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
		nil,
	)
	f.Delete(mkFifoObj("baz", 10))
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	expectedList := []Deltas{
		{{Deleted, mkFifoObj("baz", 10)}},
		// 注意：与 delta_fifo_test 不同，已知对象的删除逻辑上应发生在新对象加入之前，因此这次删除现在会提前发生。
		// 由于 "bar" 没有 delete 事件且不在 Replace 列表中，
		// 它应得到带正确 Obj 的 tombstone key。
		{{Deleted, DeletedFinalStateUnknown{Key: "bar", Obj: mkFifoObj("bar", 6)}}},
		{{Replaced, mkFifoObj("foo", 5)}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}

	// 现在改为以 Add 而非 Delete 开始。
	f = NewRealFIFO(
		testFifoObjectKeyFunc,
		literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
		nil,
	)
	f.Add(mkFifoObj("baz", 10))
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	// 注意：与 delta_fifo_test 不同，每个事件各自构成一个只含一个条目的 Deltas。
	expectedList = []Deltas{
		{{Added, mkFifoObj("baz", 10)}},
		{{Deleted, DeletedFinalStateUnknown{Key: "baz", Obj: mkFifoObj("baz", 10)}}},
		// 注意：与 delta_fifo_test 不同，已知对象的删除逻辑上应发生在新对象加入之前，因此这次删除现在会提前发生。
		// 由于 "bar" 没有 delete 事件且不在 Replace 列表中，
		// 它应得到带正确 Obj 的 tombstone key。
		{{Deleted, DeletedFinalStateUnknown{Key: "bar", Obj: mkFifoObj("bar", 6)}}},
		{{Replaced, mkFifoObj("foo", 5)}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}

	// 现在尝试在队列中删除并重建对象，再通过 Replace 删除它。
	f = NewRealFIFO(
		testFifoObjectKeyFunc,
		literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
		nil,
	)
	f.Delete(mkFifoObj("bar", 6))
	f.Add(mkFifoObj("bar", 100))
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	// 注意：与 delta_fifo_test 不同，每个事件各自构成一个只含一个条目的 Deltas。
	expectedList = []Deltas{
		{{Deleted, mkFifoObj("bar", 6)}},
		{{Added, mkFifoObj("bar", 100)}},
		// 由于队列中 "bar" 的对象比状态中的更新，
		// 它应得到带队列中最新对象的 tombstone key。
		{{Deleted, DeletedFinalStateUnknown{Key: "bar", Obj: mkFifoObj("bar", 100)}}},
		// 注意：与 delta_fifo_test 不同，已知对象的删除逻辑上应发生在新对象加入之前，因此这次删除现在会提前发生。
		{{Deleted, DeletedFinalStateUnknown{Key: "baz", Obj: mkFifoObj("baz", 7)}}},
		{{Replaced, mkFifoObj("foo", 5)}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}

	// 现在尝试先同步它，确保删除使用最新版本。
	f = NewRealFIFO(
		testFifoObjectKeyFunc,
		literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
		nil,
	)
	f.Replace([]interface{}{mkFifoObj("bar", 100), mkFifoObj("foo", 5)}, "0")
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	// 注意：与 delta_fifo_test 不同，每个事件各自构成一个只含一个条目的 Deltas。
	// 注意：与 delta_fifo_test 不同，DeltaFIFO 按 key 关联，而 RealFIFO 跨所有 key 排序，因此顺序变了。
	expectedList = []Deltas{
		// 注意：与 delta_fifo_test 不同，已知对象的删除逻辑上应发生在新对象加入之前，因此这次删除现在会提前发生。
		// 由于 "baz" 没有 delete 事件且不在 Replace 列表中，
		{{Deleted, DeletedFinalStateUnknown{Key: "baz", Obj: mkFifoObj("baz", 7)}}},
		{{Replaced, mkFifoObj("bar", 100)}},
		{{Replaced, mkFifoObj("foo", 5)}},
		// 由于 "bar" 没有 delete 事件且不在 Replace 列表中，
		// 它应得到带正确 Obj 的 tombstone key。
		{{Deleted, DeletedFinalStateUnknown{Key: "bar", Obj: mkFifoObj("bar", 100)}}},
		{{Replaced, mkFifoObj("foo", 5)}},
	}

	for i, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("%d Expected %#v, got %#v", i, e, a)
		}
	}

	// 现在尝试不显式提供 KeyListerGetter。
	f = NewRealFIFO(
		testFifoObjectKeyFunc,
		emptyKnownObjects(),
		nil,
	)
	f.Add(mkFifoObj("baz", 10))
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	expectedList = []Deltas{
		{{Added, mkFifoObj("baz", 10)}},
		{{Deleted, DeletedFinalStateUnknown{Key: "baz", Obj: mkFifoObj("baz", 10)}}},
		{{Replaced, mkFifoObj("foo", 5)}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}
}

// TestRealFIFO_ReplaceMakesDeletionsReplaced 与上面的测试相同，但
// 确保会发出 Replaced 类型的 Delta。
func TestRealFIFO_ReplaceMakesDeletionsReplaced(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
		nil,
	)

	f.Delete(mkFifoObj("baz", 10))
	f.Replace([]interface{}{mkFifoObj("foo", 6)}, "0")

	expectedList := []Deltas{
		{{Deleted, mkFifoObj("baz", 10)}},
		// 注意：与 delta_fifo_test 不同，已知对象的删除逻辑上应发生在新对象加入之前，因此这次删除现在会提前发生。
		// 由于 "bar" 没有 delete 事件且不在 Replace 列表中，
		// 它应得到带正确 Obj 的 tombstone key。
		{{Deleted, DeletedFinalStateUnknown{Key: "bar", Obj: mkFifoObj("bar", 6)}}},
		{{Replaced, mkFifoObj("foo", 6)}},
	}

	for _, expected := range expectedList {
		cur := Pop(f).(Deltas)
		if e, a := expected, cur; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected %#v, got %#v", e, a)
		}
	}
}

// 注意：与 delta_fifo_test 不同，之前的值硬编码为 "Replace"，因此这里去掉了设置其他值的选项。
////func TestRealFIFO_ReplaceDeltaType(t *testing.T) {

// TestRealFIFO_UpdateResyncRace 校验 Update 与 Resync 竞争时不会把旧值重新入队。
func TestRealFIFO_UpdateResyncRace(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5)}
		}),
		nil,
	)
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

// TestRealFIFO_HasSyncedCorrectOnDeletion 校验首次 Replace 产生的 tombstone 删除也计入初始批次，
// 必须全部 Pop 完 HasSynced 才变为 true。
func TestRealFIFO_HasSyncedCorrectOnDeletion(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5), mkFifoObj("bar", 6), mkFifoObj("baz", 7)}
		}),
		nil,
	)
	f.Replace([]interface{}{mkFifoObj("foo", 5)}, "0")

	expectedList := []Deltas{
		// 注意：与 delta_fifo_test 不同，已知对象的删除逻辑上应发生在新对象加入之前，因此这次删除现在会提前发生。
		// 由于 "bar" 没有 delete 事件且不在 Replace 列表中，
		// 它应得到带正确 Obj 的 tombstone key。
		{{Deleted, DeletedFinalStateUnknown{Key: "bar", Obj: mkFifoObj("bar", 6)}}},
		{{Deleted, DeletedFinalStateUnknown{Key: "baz", Obj: mkFifoObj("baz", 7)}}},
		{{Replaced, mkFifoObj("foo", 5)}},
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

// TestRealFIFO_detectLineJumpers 校验同一对象被多次更新时不会“插队”，
// 后到的新版本必须排在已入队的所有 key 之后。
func TestRealFIFO_detectLineJumpers(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		emptyKnownObjects(),
		nil,
	)

	f.Add(mkFifoObj("foo", 10))
	f.Add(mkFifoObj("bar", 1))
	f.Add(mkFifoObj("foo", 11))
	f.Add(mkFifoObj("foo", 13))
	f.Add(mkFifoObj("zab", 30))

	// 注意：与 delta_fifo_test 不同，每个事件都按顺序送达。

	if e, a := 10, testRealFIFOPop(f).val; a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}

	f.Add(mkFifoObj("foo", 14)) // ensure foo doesn't jump back in line

	if e, a := 1, testRealFIFOPop(f).val; a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	if e, a := 11, testRealFIFOPop(f).val; a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	if e, a := 13, testRealFIFOPop(f).val; a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	if e, a := 30, testRealFIFOPop(f).val; a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	if e, a := 14, testRealFIFOPop(f).val; a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
}

// TestRealFIFO_KeyOf 校验 keyOf 对普通对象、tombstone、Deltas 的 key 提取规则。
func TestRealFIFO_KeyOf(t *testing.T) {
	f := RealFIFO{keyFunc: testFifoObjectKeyFunc}

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
		got, err := f.keyOf(item.obj)
		if err != nil {
			t.Errorf("Unexpected error for %q: %v", item.obj, err)
			continue
		}
		if e, a := item.key, got; e != a {
			t.Errorf("Expected %v, got %v", e, a)
		}
	}
}

// TestRealFIFO_HasSynced 校验 HasSynced 的边界：无操作时为 false，
// 首次 Replace 的项全部 Pop 后才为 true。
func TestRealFIFO_HasSynced(t *testing.T) {
	tests := []struct {
		actions        []func(f *RealFIFO)
		expectedSynced bool
	}{
		{
			actions:        []func(f *RealFIFO){},
			expectedSynced: false,
		},
		{
			actions: []func(f *RealFIFO){
				func(f *RealFIFO) { f.Add(mkFifoObj("a", 1)) },
			},
			expectedSynced: true,
		},
		{
			actions: []func(f *RealFIFO){
				func(f *RealFIFO) { f.Replace([]interface{}{}, "0") },
			},
			expectedSynced: true,
		},
		{
			actions: []func(f *RealFIFO){
				func(f *RealFIFO) { f.Replace([]interface{}{mkFifoObj("a", 1), mkFifoObj("b", 2)}, "0") },
			},
			expectedSynced: false,
		},
		{
			actions: []func(f *RealFIFO){
				func(f *RealFIFO) { f.Replace([]interface{}{mkFifoObj("a", 1), mkFifoObj("b", 2)}, "0") },
				func(f *RealFIFO) { Pop(f) },
			},
			expectedSynced: false,
		},
		{
			actions: []func(f *RealFIFO){
				func(f *RealFIFO) { f.Replace([]interface{}{mkFifoObj("a", 1), mkFifoObj("b", 2)}, "0") },
				func(f *RealFIFO) { Pop(f) },
				func(f *RealFIFO) { Pop(f) },
			},
			expectedSynced: true,
		},
		{
			// 该用例实际不会发生：目前 DeltaFIFO 唯一的生产者 Reflector 总会传入时间上一致的
			// 完整快照；列表里也不可能出现重复 key，除非 apiserver 有问题。
			actions: []func(f *RealFIFO){
				func(f *RealFIFO) { f.Replace([]interface{}{mkFifoObj("a", 1), mkFifoObj("a", 2)}, "0") },
				func(f *RealFIFO) { Pop(f) },
				// 注意：与 delta_fifo_test 不同，每个事件都会送达，因此 a 被列出两次，需要 Pop 两次才能移除。
				func(f *RealFIFO) { Pop(f) },
			},
			expectedSynced: true,
		},
	}

	for i, test := range tests {
		f := NewRealFIFO(
			testFifoObjectKeyFunc,
			emptyKnownObjects(),
			nil,
		)

		for _, action := range test.actions {
			action(f)
		}
		if e, a := test.expectedSynced, f.HasSynced(); a != e {
			t.Errorf("test case %v failed, expected: %v , got %v", i, e, a)
		}
	}
}

// TestRealFIFO_PopShouldUnblockWhenClosed 检查空队列上的阻塞 Pop 在关闭后解除阻塞。
// 调用 Close 后应解除阻塞并返回。
func TestRealFIFO_PopShouldUnblockWhenClosed(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		literalListerGetter(func() []testFifoObject {
			return []testFifoObject{mkFifoObj("foo", 5)}
		}),
		nil,
	)

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

// TestRealFIFO_PopMultipleDeltaInBatch 校验批量 Pop 的切批规则：
// 初始列表与非初始事件分批、同一 key 的重复事件单独成批、batchSize 上限生效。
func TestRealFIFO_PopMultipleDeltaInBatch(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.InOrderInformersBatchProcess, true)
	const unlimitedBatchSize = 999999
	obj1 := mkFifoObj("foo1", 5)
	obj2 := mkFifoObj("foo2", 5)
	obj3 := mkFifoObj("foo3", 5)
	obj4 := mkFifoObj("foo4", 5)
	testCases := []struct {
		name            string
		initialItems    []testFifoObject
		actions         []func(f *RealFIFO)
		batchSize       int
		expectedBatches [][]Delta
	}{
		{
			name: "non-split: pop unique items should work",
			initialItems: []testFifoObject{
				obj1, obj2, obj3,
			},
			actions:   []func(f *RealFIFO){},
			batchSize: unlimitedBatchSize,
			expectedBatches: [][]Delta{
				{{Replaced, obj1}, {Replaced, obj2}, {Replaced, obj3}},
			},
		},
		{
			name: "split due to initial list: initial 2 items with 2 updates should have 2 batches",
			initialItems: []testFifoObject{
				obj1, obj2,
			},
			actions: []func(f *RealFIFO){
				func(f *RealFIFO) { _ = f.Update(obj3) },
				func(f *RealFIFO) { _ = f.Update(obj4) },
			},
			batchSize: 2,
			expectedBatches: [][]Delta{
				{{Replaced, obj1}, {Replaced, obj2}},
				{{Updated, obj3}, {Updated, obj4}},
			},
		},
		{
			name: "split due to non-unique#1: update single item for multiple items should have separate batch",
			initialItems: []testFifoObject{
				obj1,
			},
			actions: []func(f *RealFIFO){
				func(f *RealFIFO) { _ = f.Update(obj1) },
				func(f *RealFIFO) { _ = f.Update(obj1) },
			},
			batchSize: unlimitedBatchSize,
			expectedBatches: [][]Delta{
				{{Replaced, obj1}},
				{{Updated, obj1}},
				{{Updated, obj1}},
			},
		},
		{
			name: "split due to non-unique#2: update 3 item for with non-unique item at the end should have separate batch",
			initialItems: []testFifoObject{
				obj1, obj2,
			},
			actions: []func(f *RealFIFO){
				func(f *RealFIFO) { _ = f.Update(obj2) },
				func(f *RealFIFO) { _ = f.Update(obj3) },
			},
			batchSize: unlimitedBatchSize,
			expectedBatches: [][]Delta{
				{{Replaced, obj1}, {Replaced, obj2}},
				{{Updated, obj2}, {Updated, obj3}},
			},
		},
		{
			name: "split due to non-unique#3: update 3 item for with non-unique item in the mid should have separate batch",
			initialItems: []testFifoObject{
				obj1, obj2, obj3,
			},
			actions: []func(f *RealFIFO){
				func(f *RealFIFO) { _ = f.Update(obj2) },
			},
			batchSize: unlimitedBatchSize,
			expectedBatches: [][]Delta{
				{{Replaced, obj1}, {Replaced, obj2}, {Replaced, obj3}},
				{{Updated, obj2}},
			},
		},
		{
			name: "split due to batch size#1: batching initial list should work",
			initialItems: []testFifoObject{
				obj1, obj2, obj3,
			},
			actions:   []func(f *RealFIFO){},
			batchSize: 2,
			expectedBatches: [][]Delta{
				{{Replaced, obj1}, {Replaced, obj2}},
				{{Replaced, obj3}},
			},
		},
		{
			name:         "split due to batch size#2: batching incoming non-initial deltas should work",
			initialItems: []testFifoObject{},
			actions: []func(f *RealFIFO){
				func(f *RealFIFO) { _ = f.Update(obj1) },
				func(f *RealFIFO) { _ = f.Update(obj2) },
				func(f *RealFIFO) { _ = f.Update(obj3) },
			},
			batchSize: 2,
			expectedBatches: [][]Delta{
				{{Updated, obj1}, {Updated, obj2}},
				{{Updated, obj3}},
			},
		},
		{
			name: "split due to batch size#3: pop 4 mixed initial & non-initial items with 2 batch size should have 3 batch",
			initialItems: []testFifoObject{
				obj1, obj2, obj3,
			},
			actions: []func(f *RealFIFO){
				func(f *RealFIFO) { _ = f.Update(obj4) },
			},
			batchSize: 2,
			expectedBatches: [][]Delta{
				{{Replaced, obj1}, {Replaced, obj2}},
				{{Replaced, obj3}},
				{{Updated, obj4}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := NewRealFIFO(
				testFifoObjectKeyFunc,
				literalListerGetter(func() []testFifoObject {
					return tc.initialItems
				}),
				nil)
			f.batchSize = tc.batchSize

			initialItems := make([]interface{}, len(tc.initialItems))
			for i, item := range tc.initialItems {
				initialItems[i] = item
			}
			_ = f.Replace(initialItems, "123")
			for _, action := range tc.actions {
				action(f)
			}

			const maxAttempts = 10
			receivedItems := make([][]Delta, 0)
			receivedInitialDeltas := make([][]Delta, 0)

			for i := 0; i < maxAttempts; i++ {
				received := make(chan []Delta, 100)
				receivedInitial := make(chan []Delta, 100)
				go func() {
					_ = f.PopBatch(func(obj []Delta, isInInitialList bool) error {
						received <- obj
						if isInInitialList {
							receivedInitial <- obj
						}
						return nil
					}, PopProcessFunc(func(obj interface{}, isInInitialList bool) error {
						received <- []Delta{*obj.(Deltas).Newest()}
						if isInInitialList {
							receivedInitial <- []Delta{*obj.(Deltas).Newest()}
						}
						return nil
					}))
				}()
				timer := time.NewTimer(time.Millisecond * 50)
				select {
				case <-timer.C:
					close(received)
				case item := <-received:
					receivedItems = append(receivedItems, item)
					close(received)
				}
				timerInitial := time.NewTimer(time.Millisecond * 50)
				select {
				case <-timerInitial.C:
					close(receivedInitial)
				case item := <-receivedInitial:
					receivedInitialDeltas = append(receivedInitialDeltas, item)
					close(receivedInitial)
				}
			}

			runtime.Gosched()
			f.Close()

			idx := 0
			for _, batch := range tc.expectedBatches {
				assert.Equal(t, receivedItems[idx], batch)
				idx++
			}
			receivedInitialItems := make([]testFifoObject, 0)
			for _, deltas := range receivedInitialDeltas {
				for _, delta := range deltas {
					receivedInitialItems = append(receivedInitialItems, delta.Object.(testFifoObject))
				}
			}

			assert.Equal(t, tc.initialItems, receivedInitialItems)
		})
	}
}

// TestRealFIFO_PopBrokenItemsInBatch 校验 keyFunc 失败的对象会在批处理边界被正确隔离。
func TestRealFIFO_PopBrokenItemsInBatch(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.InOrderInformersBatchProcess, true)
	const unlimitedBatchSize = 999999
	sucessObj1 := mkFifoObj("foo1", 5)
	sucessObj2 := mkFifoObj("foo2", 5)
	failObj3 := mkFifoObj("foo3", 5)
	failObj4 := mkFifoObj("foo4", 5)
	testDeltaType := Added
	testCases := []struct {
		name            string
		batchSize       int
		incomingItems   []testFifoObject
		expectedBatches [][]Delta
	}{
		{
			name: "1st item is broken",
			incomingItems: []testFifoObject{
				failObj3,
			},
			expectedBatches: [][]Delta{
				{{testDeltaType, failObj3}},
			},
		},
		{
			name: "nth item is broken",
			incomingItems: []testFifoObject{
				sucessObj1, sucessObj2, failObj3,
			},
			expectedBatches: [][]Delta{
				{{testDeltaType, sucessObj1}, {testDeltaType, sucessObj2}, {testDeltaType, failObj3}},
			},
		},
		{
			name: "multiple nth items are broken",
			incomingItems: []testFifoObject{
				sucessObj1, sucessObj2, failObj3, failObj4,
			},
			expectedBatches: [][]Delta{
				{{testDeltaType, sucessObj1}, {testDeltaType, sucessObj2}, {testDeltaType, failObj3}},
				{{testDeltaType, failObj4}},
			},
		},
		{
			name: "mixed 1st and nth items are broken",
			incomingItems: []testFifoObject{
				failObj3, sucessObj1, failObj4,
			},
			expectedBatches: [][]Delta{
				{{testDeltaType, failObj3}},
				{{testDeltaType, sucessObj1}, {testDeltaType, failObj4}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testBrokenItemKeyFunc := func(obj interface{}) (string, error) {
				if obj == failObj3 || obj == failObj4 {
					return "", errors.New("test key func error")
				}
				// 否则成功。
				return testFifoObjectKeyFunc(obj)
			}
			f := NewRealFIFO(
				testBrokenItemKeyFunc,
				literalListerGetter(func() []testFifoObject {
					return nil
				}),
				nil)
			f.batchSize = unlimitedBatchSize

			for _, item := range tc.incomingItems {
				f.items = append(f.items, Delta{testDeltaType, item})
			}

			const maxAttempts = 10
			receivedItems := make([][]Delta, 0)

			for i := 0; i < maxAttempts; i++ {
				received := make(chan []Delta, 100)
				go func() {
					_ = f.PopBatch(func(obj []Delta, isInInitialList bool) error {
						received <- obj
						return nil
					}, PopProcessFunc(func(obj interface{}, isInInitialList bool) error {
						received <- []Delta{*obj.(Deltas).Newest()}
						return nil
					}))
				}()
				timer := time.NewTimer(time.Millisecond * 50)
				select {
				case <-timer.C:
					close(received)
				case item := <-received:
					receivedItems = append(receivedItems, item)
					close(received)
				}
			}

			runtime.Gosched()
			f.Close()

			idx := 0
			assert.Len(t, tc.expectedBatches, len(receivedItems))
			for _, batch := range tc.expectedBatches {
				assert.Equal(t, receivedItems[idx], batch)
				idx++
			}
		})
	}
}

// TestRealFIFO_ReplaceAtomic 校验开启原子事件后，Replace 生成单个 ReplacedAll delta，
// 而不是逐对象的 Replaced。
func TestRealFIFO_ReplaceAtomic(t *testing.T) {
	obj := mkFifoObj("foo", 2)
	table := []struct {
		name           string
		operations     func(f *RealFIFO)
		expectedDeltas Deltas
	}{
		{
			name: "Base replace",
			operations: func(f *RealFIFO) {
				f.replaceTest(t, []interface{}{}, "123")
			},
			expectedDeltas: Deltas{
				{Type: ReplacedAll, Object: ReplacedAllInfo{
					ResourceVersion: "123",
					Objects:         []interface{}{},
				}},
			},
		},
		{
			name: "Added object exists in deltas on Atomic Replace",
			operations: func(f *RealFIFO) {
				f.addTest(t, obj)
				f.replaceTest(t, []interface{}{}, "123")
			},
			expectedDeltas: Deltas{
				{Type: Added, Object: obj},
				{Type: ReplacedAll, Object: ReplacedAllInfo{
					ResourceVersion: "123",
					Objects:         []interface{}{},
				}},
			},
		},
		{
			name: "Multiple replaces run in order",
			operations: func(f *RealFIFO) {
				f.addTest(t, obj)
				f.replaceTest(t, []interface{}{obj}, "10")
				f.replaceTest(t, []interface{}{obj}, "20")
				f.replaceTest(t, []interface{}{}, "56")
			},
			expectedDeltas: Deltas{
				{Type: Added, Object: obj},
				{Type: ReplacedAll, Object: ReplacedAllInfo{
					ResourceVersion: "10",
					Objects:         []interface{}{obj},
				}},
				{Type: ReplacedAll, Object: ReplacedAllInfo{
					ResourceVersion: "20",
					Objects:         []interface{}{obj},
				}},
				{Type: ReplacedAll, Object: ReplacedAllInfo{
					ResourceVersion: "56",
					Objects:         []interface{}{},
				}},
			},
		},
		{
			name: "Bookmark object should not be included in Replace",
			operations: func(f *RealFIFO) {
				f.bookmarkTest(t, "123")
				f.replaceTest(t, []interface{}{}, "1234")
			},
			expectedDeltas: Deltas{
				{Type: Bookmark, Object: BookmarkInfo{ResourceVersion: "123"}},
				{Type: ReplacedAll, Object: ReplacedAllInfo{
					ResourceVersion: "1234",
					Objects:         []interface{}{},
				}},
			},
		},
	}
	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			// 测试带 KnownObjects 后端的 RealFIFO。
			f := NewRealFIFO(
				testFifoObjectKeyFunc,
				literalListerGetter(func() []testFifoObject {
					return []testFifoObject{}
				}),
				nil,
			)
			f.emitAtomicEvents = true
			f.emitDeltaTypeBookmark = true
			tt.operations(f)
			actualDeltasWithKnownObjects := popN(f, len(f.getItems()))
			actualAsDeltas := collapseDeltas(actualDeltasWithKnownObjects)
			if !reflect.DeepEqual(tt.expectedDeltas, actualAsDeltas) {
				t.Errorf("expected %#v, got %#v", tt.expectedDeltas, actualAsDeltas)
			}
			if len(f.items) != 0 {
				t.Errorf("expected no extra deltas (empty map), got %#v", f.items)
			}

		})
	}
}

// TestRealFIFO_ReplaceAtomicPop 校验原子 Replace 的 delta 在 Pop 时按整体交付。
func TestRealFIFO_ReplaceAtomicPop(t *testing.T) {
	f := NewRealFIFO(
		testFifoObjectKeyFunc,
		emptyKnownObjects(),
		nil,
	)
	f.emitAtomicEvents = true
	f.addTest(t, mkFifoObj("foo", 10))
	f.replaceTest(t, []interface{}{mkFifoObj("foo", 15)}, "20")
	got := make(chan testFifoObject, 3)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			obj := testRealFIFOPop(f)
			if obj.name == closedFIFOName {
				break
			}
			got <- obj
		}
	}()

	added := <-got
	if e, a, v := "foo", added.name, added.val.(int); e != a || v != 10 {
		t.Errorf("Didn't get expected name (%v vs %v) or value (%v vs %v)", e, a, v, 10)
	}

	curr := <-got
	if e, a, v := isAtomic, curr.name, curr.val.([]testFifoObject)[0]; e != a && v.val == 15 {
		t.Errorf("Didn't get updated value (%v), got %v", e, a)
	}

	select {
	case unexpected := <-got:
		t.Errorf("Got second value %v", unexpected.val)
	case <-time.After(50 * time.Millisecond):
	}

	if items := f.getItems(); len(items) > 0 {
		t.Errorf("item did not get removed")
	}
	f.Close()
	<-done
}

// TestRealFIFO_ResyncAtomic 校验原子 Resync 生成单个 SyncAll delta。
func TestRealFIFO_ResyncAtomic(t *testing.T) {
	obj := mkFifoObj("foo", 2)
	table := []struct {
		name           string
		operations     func(f *RealFIFO)
		expectedDeltas Deltas
	}{
		{
			name: "Base resync",
			operations: func(f *RealFIFO) {
				f.resyncTest(t)
			},
			expectedDeltas: Deltas{
				{Type: SyncAll, Object: SyncAllInfo{}},
			},
		},
		{
			name: "Added object exists in deltas on Atomic Resync",
			operations: func(f *RealFIFO) {
				f.addTest(t, obj)
				f.resyncTest(t)
			},
			expectedDeltas: Deltas{
				{Type: Added, Object: obj},
				{Type: SyncAll, Object: SyncAllInfo{}},
			},
		},
	}
	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			// 测试带 KnownObjects 后端的 RealFIFO。
			f := NewRealFIFO(
				testFifoObjectKeyFunc,
				literalListerGetter(func() []testFifoObject {
					return []testFifoObject{}
				}),
				nil,
			)
			f.emitAtomicEvents = true
			tt.operations(f)
			actualDeltasWithKnownObjects := popN(f, len(f.getItems()))
			actualAsDeltas := collapseDeltas(actualDeltasWithKnownObjects)

			// Resync 会向列表追加条目，因此这里开启顺序保持检查。
			if !reflect.DeepEqual(tt.expectedDeltas, actualAsDeltas) {
				t.Errorf("expected %#v, got %#v", tt.expectedDeltas, actualAsDeltas)
			}
			if len(f.items) != 0 {
				t.Errorf("expected no extra deltas (empty map), got %#v", f.items)
			}
		})
	}
}

func (f *RealFIFO) addTest(t *testing.T, obj interface{}) {
	err := f.Add(obj)
	if err != nil {
		t.Fatalf("Test error on RealFIFO add: %s", err)
	}
}

func (f *RealFIFO) replaceTest(t *testing.T, objs []interface{}, rv string) {
	err := f.Replace(objs, rv)
	if err != nil {
		t.Fatalf("Test error on RealFIFO replace: %s", err)
	}
}

func (f *RealFIFO) resyncTest(t *testing.T) {
	err := f.Resync()
	if err != nil {
		t.Fatalf("Test error on RealFIFO resync: %s", err)
	}
}

func (f *RealFIFO) bookmarkTest(t *testing.T, bookmark string) {
	err := f.Bookmark(bookmark)
	if err != nil {
		t.Fatalf("Test error on RealFIFO bookmark: %s", err)
	}
}
