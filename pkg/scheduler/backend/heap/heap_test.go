/*
Copyright 2018 The Kubernetes Authors.

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

// 本文件复制自 client-go/tools/cache/heap.go，并针对本包非线程安全的堆做了修改。

package heap

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

// testHeapObjectKeyFunc 从测试对象中提取 name 作为堆的键，
// 使堆通过 name 而非值来定位元素。
func testHeapObjectKeyFunc(obj testHeapObject) string {
	return obj.name
}

// testHeapObject 是堆的测试元素：name 作为键，val 用于比较优先级，size 模拟资源大小。
type testHeapObject struct {
	name string
	val  interface{}
	size int
}

// Size 实现 metrics.Entity 接口，返回元素占有的资源量。
func (obj testHeapObject) Size() int {
	return obj.size
}

// Type 实现 metrics.Entity 接口，返回固定的测试类型标识。
func (obj testHeapObject) Type() fwk.EntityKeyType {
	return fwk.EntityKeyType("test")
}

// testMetricRecorder 以整数累计堆操作引起的资源量变化，
// 用底层数值直接断言 Add/Remove/Update/Clear 的差值是否正确。
type testMetricRecorder int

// Add 累加新增实体的大小；nil 接收者时不记录，堆可据此关闭指标。
func (tmr *testMetricRecorder) Add(entity metrics.Entity) {
	if tmr != nil {
		*tmr += testMetricRecorder(entity.Size())
	}
}

// Remove 减去移除实体的大小；nil 接收者时不记录。
func (tmr *testMetricRecorder) Remove(entity metrics.Entity) {
	if tmr != nil {
		*tmr -= testMetricRecorder(entity.Size())
	}
}

// Update 只记大小差值，避免同键覆盖时先减后加相互抵消。
func (tmr *testMetricRecorder) Update(oldEntity, newEntity metrics.Entity) {
	if tmr != nil {
		*tmr += testMetricRecorder(newEntity.Size() - oldEntity.Size())
	}
}

// Clear 重置累计值，用于验证堆清空后的状态。
func (tmr *testMetricRecorder) Clear() {
	if tmr != nil {
		*tmr = 0
	}
}

// mkHeapObj 构造测试元素，减少各测试中重复的字面量。
func mkHeapObj(name string, val interface{}, size int) testHeapObject {
	return testHeapObject{name: name, val: val, size: size}
}

// compareInts 按 val 的整数值升序比较，决定堆的弹出顺序。
func compareInts(val1 testHeapObject, val2 testHeapObject) bool {
	first := val1.val.(int)
	second := val2.val.(int)
	return first < second
}

// expectPopOrder 依次弹出堆中全部元素并断言顺序；
// 顺序不符通常意味着堆不变式在更新或删除后被破坏。
func expectPopOrder(t *testing.T, h *Heap[testHeapObject], want []int) {
	t.Helper()
	var got []int
	for h.Len() > 0 {
		item, err := h.Pop()
		if err != nil {
			t.Fatalf("unexpected pop error: %v", err)
		}
		got = append(got, item.val.(int))
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("unexpected pop order (-want, +got):\n%s", diff)
	}
}

// TestHeap_Peek 验证每次插入后 Peek 都返回当前最小元素，且空堆返回零值与 false。
func TestHeap_Peek(t *testing.T) {
	h := New(testHeapObjectKeyFunc, compareInts)
	const amount = 500
	var zero testHeapObject

	if item, ok := h.Peek(); ok || item != zero {
		t.Errorf("expected nil object but got %v", item)
	}

	for i := amount; i > 0; i-- {
		h.AddOrUpdate(mkHeapObj(string([]rune{'a', rune(i)}), i, 1))
		head, ok := h.Peek()
		if e, a := i, head.val; !ok || a != e {
			t.Errorf("expected %d, got %d", e, a)
		}
	}
}

// TestHeap_Delete 验证 Delete 只依赖键定位元素，且删除头、尾、中间及不存在的元素后
// 堆不变式依然保持。
func TestHeap_Delete(t *testing.T) {
	h := New(testHeapObjectKeyFunc, compareInts)
	h.AddOrUpdate(mkHeapObj("foo", 10, 1))
	h.AddOrUpdate(mkHeapObj("bar", 1, 2))
	h.AddOrUpdate(mkHeapObj("bal", 31, 3))
	h.AddOrUpdate(mkHeapObj("baz", 11, 4))

	// 删除只按键定位，传入的 value 与堆中实际值可以不同。
	if obj := h.Delete(mkHeapObj("bar", 200, 2)); obj.name == "" {
		t.Fatalf("Failed to delete head.")
	}
	item, err := h.Pop()
	if e, a := 10, item.val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	h.AddOrUpdate(mkHeapObj("zab", 30, 5))
	h.AddOrUpdate(mkHeapObj("faz", 30, 6))
	len := h.data.Len()
	// 删除不存在的元素不应改变堆的长度。
	if obj := h.Delete(mkHeapObj("non-existent", 10, 7)); obj.name != "" || len != h.data.Len() {
		t.Fatalf("Didn't expect any item removal")
	}
	// 删除尾部元素。
	if obj := h.Delete(mkHeapObj("bal", 31, 3)); obj.name == "" {
		t.Fatalf("Failed to delete tail.")
	}
	// 删除两个值为 30 的元素之一。
	if obj := h.Delete(mkHeapObj("zab", 30, 5)); obj.name == "" {
		t.Fatalf("Failed to delete item.")
	}
	item, err = h.Pop()
	if e, a := 11, item.val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	item, err = h.Pop()
	if e, a := 30, item.val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	if h.data.Len() != 0 {
		t.Fatalf("expected an empty heap.")
	}
}

// TestHeap_AddOrUpdate_Update 验证 AddOrUpdate 对已有键执行更新，
// 且无论元素被移到堆头还是堆尾，堆不变式都保持。
func TestHeap_AddOrUpdate_Update(t *testing.T) {
	h := New(testHeapObjectKeyFunc, compareInts)
	h.AddOrUpdate(mkHeapObj("foo", 10, 1))
	h.AddOrUpdate(mkHeapObj("bar", 1, 2))
	h.AddOrUpdate(mkHeapObj("bal", 31, 3))
	h.AddOrUpdate(mkHeapObj("baz", 11, 4))

	// 把元素更新为最小值，应将其上浮到堆头。
	h.AddOrUpdate(mkHeapObj("baz", 0, 5))
	if h.data.queue[0].key != "baz" || h.data.keyIndex["baz"] != 0 {
		t.Fatalf("expected baz to be at the head")
	}
	item, err := h.Pop()
	if e, a := 0, item.val; err != nil || a != e {
		t.Fatalf("expected %d, got %d", e, a)
	}
	// 把元素更新为最大值，应将其下沉到队尾。
	h.AddOrUpdate(mkHeapObj("bar", 100, 6))
	if h.data.queue[0].key != "foo" || h.data.keyIndex["foo"] != 0 {
		t.Fatalf("expected foo to be at the head")
	}
}

// TestHeap_Get 验证 Get 按对象中的键查找元素，而不是按值比较。
func TestHeap_Get(t *testing.T) {
	h := New(testHeapObjectKeyFunc, compareInts)
	h.AddOrUpdate(mkHeapObj("foo", 10, 1))
	h.AddOrUpdate(mkHeapObj("bar", 1, 2))
	h.AddOrUpdate(mkHeapObj("bal", 31, 3))
	h.AddOrUpdate(mkHeapObj("baz", 11, 4))

	// 即使传入的 value 与堆中不同，仍能按键命中。
	item, exists := h.Get(mkHeapObj("baz", 0, 4))
	if !exists || item.val != 11 {
		t.Fatalf("unexpected error in getting element")
	}
	// 查找不存在的键应返回 false。
	_, exists = h.Get(mkHeapObj("non-existing", 0, 5))
	if exists {
		t.Fatalf("didn't expect to get any object")
	}
}

// TestHeap_GetByKey 验证 GetByKey 直接按键查找，行为与 TestHeap_Get 基本一致。
func TestHeap_GetByKey(t *testing.T) {
	h := New(testHeapObjectKeyFunc, compareInts)
	h.AddOrUpdate(mkHeapObj("foo", 10, 1))
	h.AddOrUpdate(mkHeapObj("bar", 1, 2))
	h.AddOrUpdate(mkHeapObj("bal", 31, 3))
	h.AddOrUpdate(mkHeapObj("baz", 11, 4))

	item, exists := h.GetByKey("baz")
	if !exists || item.val != 11 {
		t.Fatalf("unexpected error in getting element")
	}
	// 查找不存在的键应返回 false。
	_, exists = h.GetByKey("non-existing")
	if exists {
		t.Fatalf("didn't expect to get any object")
	}
}

// TestHeap_List 验证 List 返回堆中全部元素，且空堆返回空列表。
func TestHeap_List(t *testing.T) {
	h := New(testHeapObjectKeyFunc, compareInts)
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
		h.AddOrUpdate(mkHeapObj(k, v, 1))
	}
	list = h.List()
	if len(list) != len(items) {
		t.Errorf("expected %d items, got %d", len(items), len(list))
	}
	for _, heapObj := range list {
		v, ok := items[heapObj.name]
		if !ok || v != heapObj.val {
			t.Errorf("unexpected item in the list: %v", heapObj)
		}
	}
}

// TestHeapHas 验证 Has 对空堆、有元素但键不存在、单元素与多元素命中以及删除后的判断。
func TestHeapHas(t *testing.T) {
	t.Run("empty heap", func(t *testing.T) {
		h := New(testHeapObjectKeyFunc, compareInts)
		if got := h.Has(mkHeapObj("foo", 0, 1)); got != false {
			t.Errorf("Has(%q) = %v, want %v", "foo", got, false)
		}
	})

	t.Run("non-existing item in populated heap", func(t *testing.T) {
		h := New(testHeapObjectKeyFunc, compareInts)
		h.AddOrUpdate(mkHeapObj("bar", 1, 1))
		if got := h.Has(mkHeapObj("foo", 0, 1)); got != false {
			t.Errorf("Has(%q) = %v, want %v", "foo", got, false)
		}
	})

	t.Run("existing item (single element)", func(t *testing.T) {
		h := New(testHeapObjectKeyFunc, compareInts)
		h.AddOrUpdate(mkHeapObj("foo", 10, 1))
		if got := h.Has(mkHeapObj("foo", 0, 1)); got != true {
			t.Errorf("Has(%q) = %v, want %v", "foo", got, true)
		}
	})

	t.Run("existing item (multiple elements)", func(t *testing.T) {
		h := New(testHeapObjectKeyFunc, compareInts)
		h.AddOrUpdate(mkHeapObj("bar", 1, 1))
		h.AddOrUpdate(mkHeapObj("foo", 10, 1))
		h.AddOrUpdate(mkHeapObj("baz", 11, 1))
		if got := h.Has(mkHeapObj("foo", 0, 1)); got != true {
			t.Errorf("Has(%q) = %v, want %v", "foo", got, true)
		}
	})

	t.Run("item missing after deletion", func(t *testing.T) {
		h := New(testHeapObjectKeyFunc, compareInts)
		h.AddOrUpdate(mkHeapObj("foo", 10, 1))
		h.Delete(mkHeapObj("foo", 0, 1))
		if got := h.Has(mkHeapObj("foo", 0, 1)); got != false {
			t.Errorf("Has(%q) = %v, want %v", "foo", got, false)
		}
	})
}

// TestHeapZeroAndOneElement 覆盖空堆与单元素堆的边界情况：
// 弹空堆应报错，单元素的增删查改都应正确。
func TestHeapZeroAndOneElement(t *testing.T) {
	t.Run("pop single element", func(t *testing.T) {
		h := New(testHeapObjectKeyFunc, compareInts)
		h.AddOrUpdate(mkHeapObj("foo", 42, 1))
		item, err := h.Pop()
		if e, a := 42, item.val; err != nil || a != e {
			t.Fatalf("expected %d, got %d", e, a)
		}
		if e, a := 0, h.Len(); a != e {
			t.Fatalf("expected %d, got %d", e, a)
		}
	})

	t.Run("pop empty heap errors", func(t *testing.T) {
		h := New(testHeapObjectKeyFunc, compareInts)
		_, err := h.Pop()
		if err == nil {
			t.Fatalf("expected error popping empty heap")
		}
	})

	t.Run("delete single element", func(t *testing.T) {
		h := New(testHeapObjectKeyFunc, compareInts)
		h.AddOrUpdate(mkHeapObj("bar", 7, 1))
		h.Delete(mkHeapObj("bar", 0, 1))
		if e, a := 0, h.Len(); a != e {
			t.Fatalf("expected %d, got %d", e, a)
		}
	})

	t.Run("peek single element", func(t *testing.T) {
		h := New(testHeapObjectKeyFunc, compareInts)
		h.AddOrUpdate(mkHeapObj("baz", 99, 1))
		item, ok := h.Peek()
		if e, a := 99, item.val; !ok || a != e {
			t.Fatalf("expected %d, got %d", e, a)
		}
	})
}

// TestHeapPopOrder 验证不同插入顺序（升序、降序、乱序及重复值）下弹出顺序始终升序。
func TestHeapPopOrder(t *testing.T) {
	tests := []struct {
		name     string
		inserts  []testHeapObject
		expected []int
	}{
		{
			name: "ascending insertion",
			inserts: []testHeapObject{
				mkHeapObj("foo", 1, 1),
				mkHeapObj("bar", 2, 1),
				mkHeapObj("baz", 3, 1),
			},
			expected: []int{1, 2, 3},
		},
		{
			name: "descending insertion",
			inserts: []testHeapObject{
				mkHeapObj("foo", 30, 1),
				mkHeapObj("bar", 20, 1),
				mkHeapObj("baz", 10, 1),
			},
			expected: []int{10, 20, 30},
		},
		{
			name: "random insertion",
			inserts: []testHeapObject{
				mkHeapObj("foo", 15, 1),
				mkHeapObj("bar", 3, 1),
				mkHeapObj("baz", 42, 1),
				mkHeapObj("zab", 1, 1),
				mkHeapObj("faz", 27, 1),
			},
			expected: []int{1, 3, 15, 27, 42},
		},
		{
			name: "duplicate values",
			inserts: []testHeapObject{
				mkHeapObj("foo", 5, 1),
				mkHeapObj("bar", 5, 1),
				mkHeapObj("baz", 5, 1),
			},
			expected: []int{5, 5, 5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := New(testHeapObjectKeyFunc, compareInts)
			for _, obj := range tt.inserts {
				h.AddOrUpdate(obj)
			}

			expectPopOrder(t, h, tt.expected)
		})
	}
}

// TestHeapDeleteMiddle 验证删除中间、头部及部分元素后，剩余元素仍能按序弹出。
func TestHeapDeleteMiddle(t *testing.T) {
	tests := []struct {
		name         string
		inserts      []testHeapObject
		deletes      []string
		expectedPops []int
	}{
		{
			name: "delete middle items",
			inserts: []testHeapObject{
				mkHeapObj("foo", 10, 1),
				mkHeapObj("bar", 1, 1),
				mkHeapObj("bal", 20, 1),
				mkHeapObj("baz", 15, 1),
				mkHeapObj("zab", 25, 1),
				mkHeapObj("faz", 5, 1),
			},
			deletes:      []string{"foo", "baz"},
			expectedPops: []int{1, 5, 20, 25},
		},
		{
			name: "delete head",
			inserts: []testHeapObject{
				mkHeapObj("foo", 1, 1),
				mkHeapObj("bar", 10, 1),
				mkHeapObj("baz", 20, 1),
			},
			deletes:      []string{"foo"},
			expectedPops: []int{10, 20},
		},
		{
			name: "delete all but one",
			inserts: []testHeapObject{
				mkHeapObj("foo", 10, 1),
				mkHeapObj("bar", 1, 1),
				mkHeapObj("baz", 20, 1),
			},
			deletes:      []string{"foo", "baz"},
			expectedPops: []int{1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := New(testHeapObjectKeyFunc, compareInts)
			for _, obj := range tt.inserts {
				h.AddOrUpdate(obj)
			}
			for _, key := range tt.deletes {
				h.Delete(mkHeapObj(key, 0, 1))
			}

			expectPopOrder(t, h, tt.expectedPops)
		})
	}
}

// TestHeapUpdatePriority 验证对已有元素更新优先级后，堆会重新调整位置，
// 包括更新为同值、更小值、更大值及整体顺序反转。
func TestHeapUpdatePriority(t *testing.T) {
	tests := []struct {
		name         string
		inserts      []testHeapObject
		updates      []testHeapObject
		expectedPops []int
	}{
		{
			name: "update to same value",
			inserts: []testHeapObject{
				mkHeapObj("foo", 10, 1),
				mkHeapObj("bar", 1, 1),
				mkHeapObj("baz", 11, 1),
			},
			updates:      []testHeapObject{mkHeapObj("foo", 10, 1)},
			expectedPops: []int{1, 10, 11},
		},
		{
			name: "update to lower value (move to head)",
			inserts: []testHeapObject{
				mkHeapObj("foo", 10, 1),
				mkHeapObj("bar", 1, 1),
				mkHeapObj("baz", 11, 1),
			},
			updates:      []testHeapObject{mkHeapObj("baz", 0, 1)},
			expectedPops: []int{0, 1, 10},
		},
		{
			name: "update to higher value (move to tail)",
			inserts: []testHeapObject{
				mkHeapObj("foo", 10, 1),
				mkHeapObj("bar", 1, 1),
				mkHeapObj("baz", 11, 1),
			},
			updates:      []testHeapObject{mkHeapObj("bar", 100, 1)},
			expectedPops: []int{10, 11, 100},
		},
		{
			name: "full order inversion",
			inserts: []testHeapObject{
				mkHeapObj("foo", 1, 1),
				mkHeapObj("bar", 2, 1),
				mkHeapObj("baz", 3, 1),
				mkHeapObj("zab", 4, 1),
				mkHeapObj("faz", 5, 1),
			},
			updates: []testHeapObject{
				mkHeapObj("faz", 0, 1),
				mkHeapObj("foo", 100, 1),
			},
			expectedPops: []int{0, 2, 3, 4, 100},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := New(testHeapObjectKeyFunc, compareInts)
			for _, obj := range tt.inserts {
				h.AddOrUpdate(obj)
			}
			for _, obj := range tt.updates {
				h.AddOrUpdate(obj)
			}

			expectPopOrder(t, h, tt.expectedPops)
		})
	}
}

// TestHeapLen 验证 Len 在增删、更新（不改变长度）及弹出操作后的计数正确。
func TestHeapLen(t *testing.T) {
	tests := []struct {
		name        string
		action      func(h *Heap[testHeapObject])
		expectedLen int
	}{
		{
			name:        "empty heap",
			action:      func(h *Heap[testHeapObject]) {},
			expectedLen: 0,
		},
		{
			name: "after one add",
			action: func(h *Heap[testHeapObject]) {
				h.AddOrUpdate(mkHeapObj("foo", 10, 1))
			},
			expectedLen: 1,
		},
		{
			name: "after three adds",
			action: func(h *Heap[testHeapObject]) {
				h.AddOrUpdate(mkHeapObj("foo", 10, 1))
				h.AddOrUpdate(mkHeapObj("bar", 1, 1))
				h.AddOrUpdate(mkHeapObj("baz", 11, 1))
			},
			expectedLen: 3,
		},
		{
			name: "update does not change len",
			action: func(h *Heap[testHeapObject]) {
				h.AddOrUpdate(mkHeapObj("foo", 10, 1))
				h.AddOrUpdate(mkHeapObj("bar", 1, 1))
				h.AddOrUpdate(mkHeapObj("foo", 20, 1))
			},
			expectedLen: 2,
		},
		{
			name: "after add and delete",
			action: func(h *Heap[testHeapObject]) {
				h.AddOrUpdate(mkHeapObj("foo", 10, 1))
				h.AddOrUpdate(mkHeapObj("bar", 1, 1))
				h.Delete(mkHeapObj("bar", 0, 1))
			},
			expectedLen: 1,
		},
		{
			name: "after add and pop",
			action: func(h *Heap[testHeapObject]) {
				h.AddOrUpdate(mkHeapObj("foo", 10, 1))
				h.AddOrUpdate(mkHeapObj("bar", 1, 1))
				_, _ = h.Pop()
			},
			expectedLen: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := New(testHeapObjectKeyFunc, compareInts)
			tt.action(h)
			if e, a := tt.expectedLen, h.Len(); a != e {
				t.Fatalf("expected %d, got %d", e, a)
			}
		})
	}
}

// TestHeapPopAllAndReuse 验证堆被弹空后仍可复用：用相同或不同的键重新插入，
// 都能得到正确的弹出顺序，不会残留旧状态。
func TestHeapPopAllAndReuse(t *testing.T) {
	tests := []struct {
		name         string
		firstRound   []testHeapObject
		secondRound  []testHeapObject
		expectedPops []int
	}{
		{
			name: "reuse with different keys",
			firstRound: []testHeapObject{
				mkHeapObj("foo", 3, 1),
				mkHeapObj("bar", 1, 1),
				mkHeapObj("baz", 2, 1),
			},
			secondRound: []testHeapObject{
				mkHeapObj("x", 20, 1),
				mkHeapObj("y", 10, 1),
				mkHeapObj("z", 15, 1),
			},
			expectedPops: []int{10, 15, 20},
		},
		{
			name: "reuse with same keys",
			firstRound: []testHeapObject{
				mkHeapObj("foo", 100, 1),
				mkHeapObj("bar", 200, 1),
			},
			secondRound: []testHeapObject{
				mkHeapObj("foo", 5, 1),
				mkHeapObj("bar", 3, 1),
			},
			expectedPops: []int{3, 5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := New(testHeapObjectKeyFunc, compareInts)
			for _, obj := range tt.firstRound {
				h.AddOrUpdate(obj)
			}
			for h.Len() > 0 {
				_, _ = h.Pop()
			}
			for _, obj := range tt.secondRound {
				h.AddOrUpdate(obj)
			}
			expectPopOrder(t, h, tt.expectedPops)
		})
	}
}

// TestHeapDeleteAll 验证以任意顺序删除全部元素后堆变空，且之后仍能继续插入和弹出。
func TestHeapDeleteAll(t *testing.T) {
	tests := []struct {
		name        string
		inserts     []testHeapObject
		deleteOrder []string
	}{
		{
			name: "delete in random order",
			inserts: []testHeapObject{
				mkHeapObj("foo", 10, 1),
				mkHeapObj("bar", 1, 1),
				mkHeapObj("bal", 31, 1),
				mkHeapObj("baz", 11, 1),
				mkHeapObj("zab", 30, 1),
			},
			deleteOrder: []string{"baz", "foo", "zab", "bar", "bal"},
		},
		{
			name: "delete head first",
			inserts: []testHeapObject{
				mkHeapObj("foo", 1, 1),
				mkHeapObj("bar", 10, 1),
				mkHeapObj("baz", 20, 1),
			},
			deleteOrder: []string{"foo", "bar", "baz"},
		},
		{
			name: "delete tail first",
			inserts: []testHeapObject{
				mkHeapObj("foo", 1, 1),
				mkHeapObj("bar", 10, 1),
				mkHeapObj("baz", 20, 1),
			},
			deleteOrder: []string{"baz", "bar", "foo"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := New(testHeapObjectKeyFunc, compareInts)
			for _, obj := range tt.inserts {
				h.AddOrUpdate(obj)
			}
			for _, key := range tt.deleteOrder {
				h.Delete(mkHeapObj(key, 0, 1))
			}
			if e, a := 0, h.Len(); a != e {
				t.Fatalf("expected %d, got %d", e, a)
			}
			// 全部删除后堆应处于可复用的干净状态。
			h.AddOrUpdate(mkHeapObj("new", 5, 1))
			item, err := h.Pop()
			if e, a := 5, item.val; err != nil || a != e {
				t.Fatalf("expected %d, got %d", e, a)
			}
		})
	}
}

// TestHeapLargeScale 以 1000 个元素做混合插入、更新、删除和弹出，
// 验证规模较大时堆仍能保持整体有序。
func TestHeapLargeScale(t *testing.T) {
	h := New(testHeapObjectKeyFunc, compareInts)
	const amount = 1000

	// 降序插入，构造最坏的输入顺序。
	for i := amount; i > 0; i-- {
		h.AddOrUpdate(mkHeapObj(string([]rune{'a', rune(i)}), i, 1))
	}

	// 更新部分元素的优先级，触发堆重排。
	for i := 1; i <= amount; i += 10 {
		h.AddOrUpdate(mkHeapObj(string([]rune{'a', rune(i)}), amount+i, 1))
	}

	// 删除部分元素。
	for i := 5; i <= amount; i += 10 {
		h.Delete(mkHeapObj(string([]rune{'a', rune(i)}), 0, 1))
	}

	// 依次弹出剩余元素，验证全局升序。
	prevNum := 0
	for h.Len() > 0 {
		item, err := h.Pop()
		if err != nil {
			t.Fatalf("unexpected pop error: %v", err)
		}
		num := item.val.(int)
		if prevNum > num {
			t.Fatalf("got %d out of order, last was %d", num, prevNum)
		}
		prevNum = num
	}
}

// TestHeapWithRecorder 验证 NewWithRecorder 接入的指标记录器随增删更新正确变化，
// 特别覆盖“查找对象与堆中对象 size 不一致”时仍按实际存储值扣减。
func TestHeapWithRecorder(t *testing.T) {
	metricRecorder := new(testMetricRecorder)
	h := NewWithRecorder(testHeapObjectKeyFunc, compareInts, metricRecorder)
	h.AddOrUpdate(mkHeapObj("foo", 10, 1))
	h.AddOrUpdate(mkHeapObj("bar", 1, 2))
	h.AddOrUpdate(mkHeapObj("baz", 100, 3))
	h.AddOrUpdate(mkHeapObj("qux", 11, 4))

	if *metricRecorder != 10 {
		t.Errorf("expected count to be 10 (1+2+3+4) but got %d", *metricRecorder)
	}
	if obj := h.Delete(mkHeapObj("bar", 1, 2)); obj.name == "" {
		t.Fatalf("Failed to delete item")
	}
	if *metricRecorder != 8 {
		t.Errorf("expected count to be 8 (1+3+4) but got %d", *metricRecorder)
	}
	if _, err := h.Pop(); err != nil {
		t.Fatal(err)
	}
	if *metricRecorder != 7 {
		t.Errorf("expected count to be 7 (3+4) but got %d", *metricRecorder)
	}
	// 更新已有元素的 size，指标应只记差值。
	h.AddOrUpdate(mkHeapObj("baz", 100, 5))
	if *metricRecorder != 9 {
		t.Errorf("expected count to be 9 (5+4) but got %d", *metricRecorder)
	}
	// 删除时按实际存储的元素大小扣减，而非查找对象的大小。
	if obj := h.Delete(mkHeapObj("baz", 100, 0)); obj.name == "" {
		t.Fatalf("Failed to delete item")
	}
	if *metricRecorder != 4 {
		t.Errorf("expected count to be 4 but got %d", *metricRecorder)
	}

	h.metricRecorder.Clear()
	if *metricRecorder != 0 {
		t.Errorf("expected count to be 0 but got %d", *metricRecorder)
	}
}
