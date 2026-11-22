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

package conversion

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
)

// TestConverter_byteSlice 确认未注册任何转换函数时，相同类型仍能完成默认的
// 逐字段/逐元素复制（[]byte 是常见需要此回退路径的类型）。
func TestConverter_byteSlice(t *testing.T) {
	c := NewConverter(nil)
	src := []byte{1, 2, 3}
	dest := []byte{}
	err := c.Convert(&src, &dest, nil)
	if err != nil {
		t.Fatalf("expected no error")
	}
	if e, a := src, dest; !reflect.DeepEqual(e, a) {
		t.Errorf("expected %#v, got %#v", e, a)
	}
}

// TestConverter_MismatchedTypes 验证不同类型的转换完全依赖已注册函数，
// 不会尝试结构体字段名匹配。
func TestConverter_MismatchedTypes(t *testing.T) {
	c := NewConverter(nil)

	convertFn := func(in *[]string, out *int, s Scope) error {
		if str, err := strconv.Atoi((*in)[0]); err != nil {
			return err
		} else {
			*out = str
			return nil
		}
	}
	if err := c.RegisterUntypedConversionFunc(
		(*[]string)(nil), (*int)(nil),
		func(a, b interface{}, s Scope) error {
			return convertFn(a.(*[]string), b.(*int), s)
		},
	); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	src := []string{"5"}
	var dest int
	if err := c.Convert(&src, &dest, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e, a := 5, dest; e != a {
		t.Errorf("expected %#v, got %#v", e, a)
	}
}

// TestConverter_CallsRegisteredFunctions 验证双向注册各自生效，
// 且已注册函数返回的错误会原样传播。
func TestConverter_CallsRegisteredFunctions(t *testing.T) {
	type A struct {
		Foo string
		Baz int
	}
	type B struct {
		Bar string
		Baz int
	}
	type C struct{}
	c := NewConverter(nil)
	convertFn1 := func(in *A, out *B, s Scope) error {
		out.Bar = in.Foo
		out.Baz = in.Baz
		return nil
	}
	if err := c.RegisterUntypedConversionFunc(
		(*A)(nil), (*B)(nil),
		func(a, b interface{}, s Scope) error {
			return convertFn1(a.(*A), b.(*B), s)
		},
	); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	convertFn2 := func(in *B, out *A, s Scope) error {
		out.Foo = in.Bar
		out.Baz = in.Baz
		return nil
	}
	if err := c.RegisterUntypedConversionFunc(
		(*B)(nil), (*A)(nil),
		func(a, b interface{}, s Scope) error {
			return convertFn2(a.(*B), b.(*A), s)
		},
	); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	x := A{"hello, intrepid test reader!", 3}
	y := B{}

	if err := c.Convert(&x, &y, nil); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if e, a := x.Foo, y.Bar; e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := x.Baz, y.Baz; e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	z := B{"all your test are belong to us", 42}
	w := A{}

	if err := c.Convert(&z, &w, nil); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if e, a := z.Bar, w.Foo; e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := z.Baz, w.Baz; e != a {
		t.Errorf("expected %v, got %v", e, a)
	}

	convertFn3 := func(in *A, out *C, s Scope) error {
		return fmt.Errorf("C can't store an A, silly")
	}
	if err := c.RegisterUntypedConversionFunc(
		(*A)(nil), (*C)(nil),
		func(a, b interface{}, s Scope) error {
			return convertFn3(a.(*A), b.(*C), s)
		},
	); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	// 转换函数返回错误时必须让 Convert 失败，而不是静默吞掉。
	if err := c.Convert(&A{}, &C{}, nil); err == nil {
		t.Errorf("unexpected non-error")
	}
}

// TestConverter_IgnoredConversion 验证 RegisterIgnoredConversion 会让匹配的
// 类型对直接跳过转换，即使已注册了真正的转换函数。
func TestConverter_IgnoredConversion(t *testing.T) {
	type A struct{}
	type B struct{}

	count := 0
	c := NewConverter(nil)
	convertFn := func(in *A, out *B, s Scope) error {
		count++
		return nil
	}
	if err := c.RegisterUntypedConversionFunc(
		(*A)(nil), (*B)(nil),
		func(a, b interface{}, s Scope) error {
			return convertFn(a.(*A), b.(*B), s)
		},
	); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if err := c.RegisterIgnoredConversion(&A{}, &B{}); err != nil {
		t.Fatal(err)
	}
	a := A{}
	b := B{}
	if err := c.Convert(&a, &b, nil); err != nil {
		t.Errorf("%v", err)
	}
	if count != 0 {
		t.Errorf("unexpected number of conversion invocations")
	}
}

// TestConverter_GeneratedConversionOverridden 验证后注册的 generated 函数
// 会覆盖先前注册的普通转换函数（生成的转换通常更精确）。
func TestConverter_GeneratedConversionOverridden(t *testing.T) {
	type A struct{}
	type B struct{}
	c := NewConverter(nil)
	convertFn1 := func(in *A, out *B, s Scope) error {
		return nil
	}
	if err := c.RegisterUntypedConversionFunc(
		(*A)(nil), (*B)(nil),
		func(a, b interface{}, s Scope) error {
			return convertFn1(a.(*A), b.(*B), s)
		},
	); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	convertFn2 := func(in *A, out *B, s Scope) error {
		return fmt.Errorf("generated function should be overridden")
	}
	if err := c.RegisterGeneratedUntypedConversionFunc(
		(*A)(nil), (*B)(nil),
		func(a, b interface{}, s Scope) error {
			return convertFn2(a.(*A), b.(*B), s)
		},
	); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	a := A{}
	b := B{}
	if err := c.Convert(&a, &b, nil); err != nil {
		t.Errorf("%v", err)
	}
}

// TestConverter_WithConversionOverridden 验证 WithConversions 返回的副本
// 使用外部注册的函数，而原 Converter 保持不变（不可变配置）。
func TestConverter_WithConversionOverridden(t *testing.T) {
	type A struct{}
	type B struct{}
	c := NewConverter(nil)
	convertFn1 := func(in *A, out *B, s Scope) error {
		return fmt.Errorf("conversion function should be overridden")
	}
	if err := c.RegisterUntypedConversionFunc(
		(*A)(nil), (*B)(nil),
		func(a, b interface{}, s Scope) error {
			return convertFn1(a.(*A), b.(*B), s)
		},
	); err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	convertFn2 := func(in *A, out *B, s Scope) error {
		return fmt.Errorf("generated function should be overridden")
	}
	if err := c.RegisterGeneratedUntypedConversionFunc(
		(*A)(nil), (*B)(nil),
		func(a, b interface{}, s Scope) error {
			return convertFn2(a.(*A), b.(*B), s)
		},
	); err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	ext := NewConversionFuncs()
	ext.AddUntyped(
		(*A)(nil), (*B)(nil),
		func(a, b interface{}, s Scope) error {
			return nil
		},
	)
	newc := c.WithConversions(ext)

	a := A{}
	b := B{}
	// 原 converter 仍走 generated 函数，故返回错误。
	if err := c.Convert(&a, &b, nil); err == nil || err.Error() != "conversion function should be overridden" {
		t.Errorf("unexpected error: %v", err)
	}
	// 新 converter 使用 WithConversions 注入的实现，应当成功。
	if err := newc.Convert(&a, &b, nil); err != nil {
		t.Errorf("%v", err)
	}
}

// TestConverter_meta 验证传给 Convert 的 Meta 会被原样传给注册的转换函数，
// 这是转换过程中共享上下文（如目标版本信息）的机制。
func TestConverter_meta(t *testing.T) {
	type Foo struct{ A string }
	type Bar struct{ A string }
	c := NewConverter(nil)
	checks := 0
	convertFn1 := func(in *Foo, out *Bar, s Scope) error {
		if s.Meta() == nil {
			t.Errorf("Meta did not get passed!")
		}
		checks++
		out.A = in.A
		return nil
	}
	if err := c.RegisterUntypedConversionFunc(
		(*Foo)(nil), (*Bar)(nil),
		func(a, b interface{}, s Scope) error {
			return convertFn1(a.(*Foo), b.(*Bar), s)
		},
	); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := c.Convert(&Foo{}, &Bar{}, &Meta{}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if checks != 1 {
		t.Errorf("Registered functions did not get called.")
	}
}
