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

package sets

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

// TestStringSet 验证集合的插入、删除、Has/HasAll 与 IsSuperset 的基本语义。
func TestStringSet(t *testing.T) {
	s := String{}
	s2 := String{}
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

// TestStringSetDeleteMultiples 验证 Delete 可一次删除多个元素，且仅删除指定的元素。
func TestStringSetDeleteMultiples(t *testing.T) {
	s := String{}
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

// TestNewStringSet 验证 NewString 用给定元素构造集合并保持去重语义。
func TestNewStringSet(t *testing.T) {
	s := NewString("a", "b", "c")
	if len(s) != 3 {
		t.Errorf("Expected len=3: %d", len(s))
	}
	if !s.Has("a") || !s.Has("b") || !s.Has("c") {
		t.Errorf("Unexpected contents: %#v", s)
	}
}

// TestStringSetList 验证 List 返回按字典序排序的切片，保证输出稳定。
func TestStringSetList(t *testing.T) {
	s := NewString("z", "y", "x", "a")
	if !reflect.DeepEqual(s.List(), []string{"a", "x", "y", "z"}) {
		t.Errorf("List gave unexpected result: %#v", s.List())
	}
}

// TestStringSetDifference 验证差集 a-b 只包含 a 中不属于 b 的元素，且运算不对称。
func TestStringSetDifference(t *testing.T) {
	a := NewString("1", "2", "3")
	b := NewString("1", "2", "4", "5")
	c := a.Difference(b)
	d := b.Difference(a)
	if len(c) != 1 {
		t.Errorf("Expected len=1: %d", len(c))
	}
	if !c.Has("3") {
		t.Errorf("Unexpected contents: %#v", c.List())
	}
	if len(d) != 2 {
		t.Errorf("Expected len=2: %d", len(d))
	}
	if !d.Has("4") || !d.Has("5") {
		t.Errorf("Unexpected contents: %#v", d.List())
	}
}

// TestStringSetSymmetricDifference 验证对称差（只属于其中一方的元素），且运算可交换。
func TestStringSetSymmetricDifference(t *testing.T) {
	a := NewString("1", "2", "3")
	b := NewString("1", "2", "4", "5")
	c := a.SymmetricDifference(b)
	d := b.SymmetricDifference(a)
	if !c.Equal(NewString("3", "4", "5")) {
		t.Errorf("Unexpected contents: %#v", c.List())
	}
	if !d.Equal(NewString("3", "4", "5")) {
		t.Errorf("Unexpected contents: %#v", d.List())
	}
}

// TestStringSetHasAny 验证 HasAny 在任一元素命中时为 true，全部不命中时为 false。
func TestStringSetHasAny(t *testing.T) {
	a := NewString("1", "2", "3")

	if !a.HasAny("1", "4") {
		t.Errorf("expected true, got false")
	}

	if a.HasAny("0", "4") {
		t.Errorf("expected false, got true")
	}
}

// TestStringSetEquals 验证集合相等性：与顺序无关、重复元素被忽略，
// 并覆盖空集合、空字符串元素以及增删操作后的相等判断。
func TestStringSetEquals(t *testing.T) {
	// 简单情况（顺序无关）
	a := NewString("1", "2")
	b := NewString("2", "1")
	if !a.Equal(b) {
		t.Errorf("Expected to be equal: %v vs %v", a, b)
	}

	// 这是集合；重复元素会被忽略
	b = NewString("2", "2", "1")
	if !a.Equal(b) {
		t.Errorf("Expected to be equal: %v vs %v", a, b)
	}

	// 空集合/空字符串元素的边界情况
	a = NewString()
	b = NewString()
	if !a.Equal(b) {
		t.Errorf("Expected to be equal: %v vs %v", a, b)
	}

	b = NewString("1", "2", "3")
	if a.Equal(b) {
		t.Errorf("Expected to be not-equal: %v vs %v", a, b)
	}

	b = NewString("1", "2", "")
	if a.Equal(b) {
		t.Errorf("Expected to be not-equal: %v vs %v", a, b)
	}

	// 验证变更后的相等性
	a = NewString()
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

// TestStringUnion 验证并集包含双方全部元素，并覆盖一方为空、双方皆空的情形。
func TestStringUnion(t *testing.T) {
	tests := []struct {
		s1       String
		s2       String
		expected String
	}{
		{
			NewString("1", "2", "3", "4"),
			NewString("3", "4", "5", "6"),
			NewString("1", "2", "3", "4", "5", "6"),
		},
		{
			NewString("1", "2", "3", "4"),
			NewString(),
			NewString("1", "2", "3", "4"),
		},
		{
			NewString(),
			NewString("1", "2", "3", "4"),
			NewString("1", "2", "3", "4"),
		},
		{
			NewString(),
			NewString(),
			NewString(),
		},
	}

	for _, test := range tests {
		union := test.s1.Union(test.s2)
		if union.Len() != test.expected.Len() {
			t.Errorf("Expected union.Len()=%d but got %d", test.expected.Len(), union.Len())
		}

		if !union.Equal(test.expected) {
			t.Errorf("Expected union.Equal(expected) but not true.  union:%v expected:%v", union.List(), test.expected.List())
		}
	}
}

// TestStringIntersection 验证交集只包含双方共有的元素，并覆盖空集合的情形。
func TestStringIntersection(t *testing.T) {
	tests := []struct {
		s1       String
		s2       String
		expected String
	}{
		{
			NewString("1", "2", "3", "4"),
			NewString("3", "4", "5", "6"),
			NewString("3", "4"),
		},
		{
			NewString("1", "2", "3", "4"),
			NewString("1", "2", "3", "4"),
			NewString("1", "2", "3", "4"),
		},
		{
			NewString("1", "2", "3", "4"),
			NewString(),
			NewString(),
		},
		{
			NewString(),
			NewString("1", "2", "3", "4"),
			NewString(),
		},
		{
			NewString(),
			NewString(),
			NewString(),
		},
	}

	for _, test := range tests {
		intersection := test.s1.Intersection(test.s2)
		if intersection.Len() != test.expected.Len() {
			t.Errorf("Expected intersection.Len()=%d but got %d", test.expected.Len(), intersection.Len())
		}

		if !intersection.Equal(test.expected) {
			t.Errorf("Expected intersection.Equal(expected) but not true.  intersection:%v expected:%v", intersection.List(), test.expected.List())
		}
	}
}

// randomStringAlphabet 是随机字符串的字符表。
type randomStringAlphabet string

// makeString 生成一个长度在 [minLen, maxLen) 随机区间内的随机字符串。
func (a randomStringAlphabet) makeString(minLen, maxLen int) string {
	n := minLen
	if minLen < maxLen {
		n += rand.Intn(maxLen - minLen)
	}
	var s string
	for i := 0; i < n; i++ {
		s += string(a[rand.Intn(len(a))])
	}
	return s
}

// randomStringMaker 是基准测试使用的字符表实例（小写字母加数字）。
var randomStringMaker = randomStringAlphabet("abcdefghijklmnopqrstuvwxyz0123456789")

// BenchmarkStringSet 在多种集合规模下测量插入、取键集合、查找、交并/对称差与列表的开销。
func BenchmarkStringSet(b *testing.B) {
	cases := []struct {
		size         int
		minStringLen int
		maxStringLen int
	}{
		{20, 10, 20},
		{50, 10, 30},
		{100, 20, 40},
		{500, 20, 50},
		{1000, 20, 60},
	}

	for i := range cases {
		here := cases[i]
		makeSet := func() String {
			s := NewString()
			for j := 0; j < here.size; j++ {
				s.Insert(randomStringMaker.makeString(here.minStringLen, here.maxStringLen))
			}
			return s
		}
		operands := make([]String, 500)
		for i := range operands {
			operands[i] = makeSet()
		}
		randOperand := func() String { return operands[rand.Intn(len(operands))] }

		b.Run(fmt.Sprintf("insert-%v", here.size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				makeSet()
			}
		})

		b.Run(fmt.Sprintf("key-set-%v", here.size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				StringKeySet(randOperand())
			}
		})

		b.Run(fmt.Sprintf("has-%v", here.size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				randOperand().Has(randomStringMaker.makeString(here.minStringLen, here.maxStringLen))
			}
		})

		b.Run(fmt.Sprintf("intersection-%v", here.size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				randOperand().Intersection(randOperand())
			}
		})

		b.Run(fmt.Sprintf("symmetric-difference-%v", here.size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				randOperand().SymmetricDifference(randOperand())
			}
		})

		b.Run(fmt.Sprintf("list-%v", here.size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				randOperand().List()
			}
		})
	}
}
