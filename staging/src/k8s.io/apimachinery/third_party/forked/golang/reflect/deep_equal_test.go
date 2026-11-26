// Copyright 2009 The Go Authors. All rights reserved.
// 本源码的使用受 BSD 风格许可证约束，详见 LICENSE 文件。

package reflect

import (
	"testing"
)

// TestEqualities 验证 Equalities.DeepEqual 与 DeepEqualWithNilDifferentFromEmpty 的判定：
// 前者把 nil 与空集合视为相等（与 reflect.DeepEqual 一致），
// 后者则区分二者；两者都应遵循通过 AddFuncs 注册的自定义比较函数。
func TestEqualities(t *testing.T) {
	e := Equalities{}
	type Bar struct {
		X int
	}
	type Baz struct {
		Y Bar
	}
	type Zap struct {
		A []int
		B map[string][]int
	}
	// 注册两个自定义比较函数：int 用 a+1==b，Bar 用 a.X*10==b.X。
	// 这些函数会覆盖默认的逐字段比较，用于验证自定义相等性在各层嵌套中的传播。
	err := e.AddFuncs(
		func(a, b int) bool {
			return a+1 == b
		},
		func(a, b Bar) bool {
			return a.X*10 == b.X
		},
	)
	if err != nil {
		t.Fatalf("Unexpected: %v", err)
	}

	type Foo struct {
		X int
	}

	table := []struct {
		a, b                               interface{}
		deepEqual                          bool // Expected result for DeepEqual
		deepEqualWithNilDifferentFromEmpty bool // Expected result for DeepEqualWithNilDifferentFromEmpty
	}{
		// 自定义相等函数：Foo 的 int 字段应使用注册的函数（1 与 2 相等）。
		{1, 2, true, true},
		{2, 1, false, false},
		{"foo", "fo", false, false},
		{"foo", "foo", true, true},
		{"foo", "foobar", false, false},
		{Foo{1}, Foo{2}, true, true},
		{Foo{2}, Foo{1}, false, false},
		{Bar{1}, Bar{10}, true, true},
		{&Bar{1}, &Bar{10}, true, true},
		{Baz{Bar{1}}, Baz{Bar{10}}, true, true},
		// 数组：长度与元素都必须匹配。
		{[...]string{}, [...]string{"1", "2", "3"}, false, false},
		{[...]string{"1"}, [...]string{"1", "2", "3"}, false, false},
		{[...]string{"1", "2", "3"}, [...]string{}, false, false},
		{[...]string{"1", "2", "3"}, [...]string{"1", "2", "3"}, true, true},
		// 带自定义相等性的 map：键集合与值都要比较，值使用自定义函数。
		{map[string]int{"foo": 1}, map[string]int{}, false, false},
		{map[string]int{"foo": 1}, map[string]int{"foo": 2}, true, true},
		{map[string]int{"foo": 2}, map[string]int{"foo": 1}, false, false},
		{map[string]int{"foo": 1}, map[string]int{"foo": 2, "bar": 6}, false, false},
		{map[string]int{"foo": 1, "bar": 6}, map[string]int{"foo": 2}, false, false},
		// nil 与空集合：DeepEqual 视为相等，DeepEqualWithNilDifferentFromEmpty 视为不同。
		{map[string]int{}, map[string]int(nil), true, false},
		{[]string(nil), []string(nil), true, true},
		{[]string{}, []string(nil), true, false},
		{[]string(nil), []string{}, true, false},
		{[]int{}, []int(nil), true, false},
		// nil 与有内容：两个函数都应返回 false。
		{[]string{"1"}, []string(nil), false, false},
		// 空集合与有内容：两个函数都应返回 false。
		{[]string{}, []string{"1"}, false, false},
		{[]string{}, []string{"1", "2", "3"}, false, false},
		{[]int{}, []int{1, 2, 3}, false, false},
		{map[string]int{}, map[string]int{"foo": 1}, false, false},
		// 有内容与空集合：两个函数都应返回 false。
		{[]string{"1"}, []string{}, false, false},
		{[]string{"1"}, []string{"1", "2", "3"}, false, false},
		{[]string{"1", "2", "3"}, []string{}, false, false},
		{[]int{1, 2, 3}, []int{}, false, false},
		{map[string]int{"foo": 1}, map[string]int{}, false, false},
		// 嵌套的 nil/空：同样只在 DeepEqualWithNilDifferentFromEmpty 中被视为不同。
		{map[string][]int{}, map[string][]int(nil), true, false},
		{map[string][]int{"foo": nil}, map[string][]int{"foo": {}}, true, false},
		{Zap{A: nil, B: map[string][]int{"foo": nil}}, Zap{A: []int{}, B: map[string][]int{"foo": {}}}, true, false},
	}

	for _, item := range table {
		if e, a := item.deepEqual, e.DeepEqual(item.a, item.b); e != a {
			t.Errorf("DeepEqual: Expected (%+v == %+v) == %v, but got %v", item.a, item.b, e, a)
		}
		if e, a := item.deepEqualWithNilDifferentFromEmpty, e.DeepEqualWithNilDifferentFromEmpty(item.a, item.b); e != a {
			t.Errorf("DeepEqualWithNilDifferentFromEmpty: Expected (%+v == %+v) == %v, but got %v", item.a, item.b, e, a)
		}
	}
}

// TestDerivatives 验证 DeepDerivative 的"派生"语义：
// 只比较 a 中出现的字段，b 中多出的字段被忽略；nil 与空集合视为相等，
// 且自定义相等函数同样生效。
func TestDerivatives(t *testing.T) {
	e := Equalities{}
	type Bar struct {
		X int
	}
	type Baz struct {
		Y Bar
	}
	// 注册两个自定义比较函数：int 用 a+1==b，Bar 用 a.X*10==b.X。
	// 这些函数会覆盖默认的逐字段比较，用于验证自定义相等性在各层嵌套中的传播。
	err := e.AddFuncs(
		func(a, b int) bool {
			return a+1 == b
		},
		func(a, b Bar) bool {
			return a.X*10 == b.X
		},
	)
	if err != nil {
		t.Fatalf("Unexpected: %v", err)
	}

	type Foo struct {
		X int
	}

	// 与 TestEqualities 相同的比较函数，但期望值按"a 是 b 的子集"语义给出。
	table := []struct {
		a, b  interface{}
		equal bool
	}{
		{1, 2, true},
		{2, 1, false},
		{"foo", "fo", false},
		{"foo", "foo", true},
		{"foo", "foobar", false},
		{Foo{1}, Foo{2}, true},
		{Foo{2}, Foo{1}, false},
		{Bar{1}, Bar{10}, true},
		{&Bar{1}, &Bar{10}, true},
		{Baz{Bar{1}}, Baz{Bar{10}}, true},
		{[...]string{}, [...]string{"1", "2", "3"}, false},
		{[...]string{"1"}, [...]string{"1", "2", "3"}, false},
		{[...]string{"1", "2", "3"}, [...]string{}, false},
		{[...]string{"1", "2", "3"}, [...]string{"1", "2", "3"}, true},
		{map[string]int{"foo": 1}, map[string]int{}, false},
		{map[string]int{"foo": 1}, map[string]int{"foo": 2}, true},
		{map[string]int{"foo": 2}, map[string]int{"foo": 1}, false},
		{map[string]int{"foo": 1}, map[string]int{"foo": 2, "bar": 6}, true},
		{map[string]int{"foo": 1, "bar": 6}, map[string]int{"foo": 2}, false},
		{map[string]int{}, map[string]int(nil), true},
		{[]string(nil), []string(nil), true},
		{[]string{}, []string(nil), true},
		{[]string(nil), []string{}, true},
		{[]string{"1"}, []string(nil), false},
		{[]string{}, []string{"1", "2", "3"}, true},
		{[]string{"1"}, []string{"1", "2", "3"}, true},
		{[]string{"1", "2", "3"}, []string{}, false},
	}

	for _, item := range table {
		if e, a := item.equal, e.DeepDerivative(item.a, item.b); e != a {
			t.Errorf("Expected (%+v ~ %+v) == %v, but got %v", item.a, item.b, e, a)
		}
	}
}
