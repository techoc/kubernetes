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

package sets_test

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
)

// TestSet 验证泛型 Set 的基本操作：Insert/Has/Delete/HasAll/IsSuperset 的语义。
func TestSet(t *testing.T) {
	s := sets.Set[string]{}
	s2 := sets.Set[string]{}
	if len(s) != 0 {
		t.Errorf("Expected len=0: %d", len(s))
	}
	s.Insert("a", "b")
	if len(s) != 2 {
		t.Errorf("Expected len=2: %d", len(s))
	}
	s.Insert("c")
	if s.Has("d") {
		t.Errorf("Unexpected contents: %#v", s)
	}
	if !s.Has("a") {
		t.Errorf("Missing contents: %#v", s)
	}
	s.Delete("a")
	if s.Has("a") {
		t.Errorf("Unexpected contents: %#v", s)
	}
	s.Insert("a")
	if s.HasAll("a", "b", "d") {
		t.Errorf("Unexpected contents: %#v", s)
	}
	if !s.HasAll("a", "b") {
		t.Errorf("Missing contents: %#v", s)
	}
	s2.Insert("a", "b", "d")
	if s.IsSuperset(s2) {
		t.Errorf("Unexpected contents: %#v", s)
	}
	s2.Delete("d")
	if !s.IsSuperset(s2) {
		t.Errorf("Missing contents: %#v", s)
	}
}

// TestSetDeleteMultiples 验证一次 Delete 多个元素时只移除给定元素。
func TestSetDeleteMultiples(t *testing.T) {
	s := sets.Set[string]{}
	s.Insert("a", "b", "c")
	if len(s) != 3 {
		t.Errorf("Expected len=3: %d", len(s))
	}

	s.Delete("a", "c")
	if len(s) != 1 {
		t.Errorf("Expected len=1: %d", len(s))
	}
	if s.Has("a") {
		t.Errorf("Unexpected contents: %#v", s)
	}
	if s.Has("c") {
		t.Errorf("Unexpected contents: %#v", s)
	}
	if !s.Has("b") {
		t.Errorf("Missing contents: %#v", s)
	}

}

// TestSetClear 验证 Clear 清空集合。
func TestSetClear(t *testing.T) {
	s := sets.Set[string]{}
	s.Insert("a", "b", "c")
	if s.Len() != 3 {
		t.Errorf("Expected len=3: %d", s.Len())
	}

	s.Clear()
	if s.Len() != 0 {
		t.Errorf("Expected len=0: %d", s.Len())
	}
}

// TestSetClearWithSharedReference 验证 Clear 作用于共享的底层 map：
// 对一个引用的清空会同时影响另一个引用（Set 是 map 的别名）。
func TestSetClearWithSharedReference(t *testing.T) {
	s := sets.Set[string]{}
	s.Insert("a", "b", "c")
	if s.Len() != 3 {
		t.Errorf("Expected len=3: %d", s.Len())
	}

	m := s
	s.Clear()
	if s.Len() != 0 {
		t.Errorf("Expected len=0 on the cleared set: %d", s.Len())
	}
	if m.Len() != 0 {
		t.Errorf("Expected len=0 on the shared reference: %d", m.Len())
	}
}

// TestSetClearInSeparateFunction 验证在另一个函数中 Clear 后继续 Insert 的行为，
// 确保清空与后续写入都作用于同一个共享 map。
func TestSetClearInSeparateFunction(t *testing.T) {
	s := sets.Set[string]{}
	s.Insert("a", "b", "c")
	if s.Len() != 3 {
		t.Errorf("Expected len=3: %d", s.Len())
	}

	clearSetAndAdd(s, "d")
	if s.Len() != 1 {
		t.Errorf("Expected len=1: %d", s.Len())
	}
	if !s.Has("d") {
		t.Errorf("Unexpected contents: %#v", s)
	}
}

// clearSetAndAdd 清空集合并插入一个新元素，用于验证跨函数调用时共享引用的语义。
func clearSetAndAdd[T comparable](s sets.Set[T], a T) {
	s.Clear()
	s.Insert(a)
}

// TestNewSet 验证 sets.New 用给定元素构造集合。
func TestNewSet(t *testing.T) {
	s := sets.New("a", "b", "c")
	if len(s) != 3 {
		t.Errorf("Expected len=3: %d", len(s))
	}
	if !s.Has("a") || !s.Has("b") || !s.Has("c") {
		t.Errorf("Unexpected contents: %#v", s)
	}
}

// TestKeySet 验证 KeySet 取 map 的全部键构造集合。
func TestKeySet(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2, "c": 3}
	ss := sets.KeySet[string](m)
	if !ss.Equal(sets.New("a", "b", "c")) {
		t.Errorf("Unexpected contents: %#v", sets.List(ss))
	}
}

// TestNewEmptySet 验证 New 不带参数时创建空集合，且之后可正常插入。
func TestNewEmptySet(t *testing.T) {
	s := sets.New[string]()
	if len(s) != 0 {
		t.Errorf("Expected len=0: %d", len(s))
	}
	s.Insert("a", "b", "c")
	if len(s) != 3 {
		t.Errorf("Expected len=3: %d", len(s))
	}
	if !s.Has("a") || !s.Has("b") || !s.Has("c") {
		t.Errorf("Unexpected contents: %#v", s)
	}
}

// TestSortedList 验证 List 返回按字典序排序的结果。
func TestSortedList(t *testing.T) {
	s := sets.New("z", "y", "x", "a")
	if !reflect.DeepEqual(sets.List(s), []string{"a", "x", "y", "z"}) {
		t.Errorf("List gave unexpected result: %#v", sets.List(s))
	}
}

// TestSetDifference 验证双向差集：a-b 与 b-a 各自只保留对方没有的元素。
func TestSetDifference(t *testing.T) {
	a := sets.New("1", "2", "3")
	b := sets.New("1", "2", "4", "5")
	c := a.Difference(b)
	d := b.Difference(a)
	if len(c) != 1 {
		t.Errorf("Expected len=1: %d", len(c))
	}
	if !c.Has("3") {
		t.Errorf("Unexpected contents: %#v", sets.List(c))
	}
	if len(d) != 2 {
		t.Errorf("Expected len=2: %d", len(d))
	}
	if !d.Has("4") || !d.Has("5") {
		t.Errorf("Unexpected contents: %#v", sets.List(d))
	}
}

// TestSetSymmetricDifference 验证对称差与运算顺序无关。
func TestSetSymmetricDifference(t *testing.T) {
	a := sets.New("1", "2", "3")
	b := sets.New("1", "2", "4", "5")
	c := a.SymmetricDifference(b)
	d := b.SymmetricDifference(a)
	if !c.Equal(sets.New("3", "4", "5")) {
		t.Errorf("Unexpected contents: %#v", sets.List(c))
	}
	if !d.Equal(sets.New("3", "4", "5")) {
		t.Errorf("Unexpected contents: %#v", sets.List(d))
	}
}

// TestSetHasAny 验证 HasAny 在至少一个元素存在时返回 true。
func TestSetHasAny(t *testing.T) {
	a := sets.New("1", "2", "3")

	if !a.HasAny("1", "4") {
		t.Errorf("expected true, got false")
	}

	if a.HasAny("0", "4") {
		t.Errorf("expected false, got true")
	}
}

// TestSetEquals 验证集合相等与顺序无关、重复元素被忽略，并覆盖空集合与空字符串等边界，
// 以及插入/删除后相等性的变化。
func TestSetEquals(t *testing.T) {
	//简单场景（顺序无关）
	a := sets.New("1", "2")
	b := sets.New("2", "1")
	if !a.Equal(b) {
		t.Errorf("Expected to be equal: %v vs %v", a, b)
	}

	//这是集合，重复元素会被忽略。
	b = sets.New("2", "2", "1")
	if !a.Equal(b) {
		t.Errorf("Expected to be equal: %v vs %v", a, b)
	}

	//空集合和空字符串的边界情况
	a = sets.New[string]()
	b = sets.New[string]()
	if !a.Equal(b) {
		t.Errorf("Expected to be equal: %v vs %v", a, b)
	}

	b = sets.New("1", "2", "3")
	if a.Equal(b) {
		t.Errorf("Expected to be not-equal: %v vs %v", a, b)
	}

	b = sets.New("1", "2", "")
	if a.Equal(b) {
		t.Errorf("Expected to be not-equal: %v vs %v", a, b)
	}

	//修改后检查是否相等
	a = sets.New[string]()
	a.Insert("1")
	if a.Equal(b) {
		t.Errorf("Expected to be not-equal: %v vs %v", a, b)
	}

	a.Insert("2")
	if a.Equal(b) {
		t.Errorf("Expected to be not-equal: %v vs %v", a, b)
	}

	a.Insert("")
	if !a.Equal(b) {
		t.Errorf("Expected to be equal: %v vs %v", a, b)
	}

	a.Delete("")
	if a.Equal(b) {
		t.Errorf("Expected to be not-equal: %v vs %v", a, b)
	}
}

// TestUnion 验证并集，包括与空集合运算及两个空集合运算。
func TestUnion(t *testing.T) {
	tests := []struct {
		s1       sets.Set[string]
		s2       sets.Set[string]
		expected sets.Set[string]
	}{
		{
			sets.New("1", "2", "3", "4"),
			sets.New("3", "4", "5", "6"),
			sets.New("1", "2", "3", "4", "5", "6"),
		},
		{
			sets.New("1", "2", "3", "4"),
			sets.New[string](),
			sets.New("1", "2", "3", "4"),
		},
		{
			sets.New[string](),
			sets.New("1", "2", "3", "4"),
			sets.New("1", "2", "3", "4"),
		},
		{
			sets.New[string](),
			sets.New[string](),
			sets.New[string](),
		},
	}

	for _, test := range tests {
		union := test.s1.Union(test.s2)
		if union.Len() != test.expected.Len() {
			t.Errorf("Expected union.Len()=%d but got %d", test.expected.Len(), union.Len())
		}

		if !union.Equal(test.expected) {
			t.Errorf("Expected union.Equal(expected) but not true.  union:%v expected:%v", sets.List(union), sets.List(test.expected))
		}
	}
}

// TestIntersection 验证交集，包括相同集合、与空集合运算及两个空集合运算。
func TestIntersection(t *testing.T) {
	tests := []struct {
		s1       sets.Set[string]
		s2       sets.Set[string]
		expected sets.Set[string]
	}{
		{
			sets.New("1", "2", "3", "4"),
			sets.New("3", "4", "5", "6"),
			sets.New("3", "4"),
		},
		{
			sets.New("1", "2", "3", "4"),
			sets.New("1", "2", "3", "4"),
			sets.New("1", "2", "3", "4"),
		},
		{
			sets.New("1", "2", "3", "4"),
			sets.New[string](),
			sets.New[string](),
		},
		{
			sets.New[string](),
			sets.New("1", "2", "3", "4"),
			sets.New[string](),
		},
		{
			sets.New[string](),
			sets.New[string](),
			sets.New[string](),
		},
	}

	for _, test := range tests {
		intersection := test.s1.Intersection(test.s2)
		if intersection.Len() != test.expected.Len() {
			t.Errorf("Expected intersection.Len()=%d but got %d", test.expected.Len(), intersection.Len())
		}

		if !intersection.Equal(test.expected) {
			t.Errorf("Expected intersection.Equal(expected) but not true.  intersection:%v expected:%v", sets.List(intersection), sets.List(intersection))
		}
	}
}

// BenchmarkListSmall 衡量对 10 个元素的小集合调用 List 的开销。
func BenchmarkListSmall(b *testing.B) {
	s := sets.New("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
	for b.Loop() {
		sets.List(s)
	}
}

// BenchmarkListLarge 衡量对约 1.2 万个元素的大集合调用 List（排序）的开销。
func BenchmarkListLarge(b *testing.B) {
	s := make(sets.Set[int], 12345)
	for i := range 12345 {
		s.Insert(i * 7)
	}
	for b.Loop() {
		sets.List(s)
	}
}
