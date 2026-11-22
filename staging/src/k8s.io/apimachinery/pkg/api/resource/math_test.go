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

package resource

import (
	"testing"
)

// TestDetectOverflowAdd 验证 int64Add 在正常与溢出边界上的结果与溢出标志。
// 重点覆盖 int64 极值：任何跨越最大值/最小值的加法都必须报告溢出，
// 且交换操作数后结果与溢出标志保持一致（加法可交换）。
func TestDetectOverflowAdd(t *testing.T) {
	for _, test := range []struct {
		a, b int64
		c    int64
		ok   bool
	}{
		{0, 0, 0, true},
		{-1, 1, 0, true},
		{0, 1, 1, true},
		{2, 2, 4, true},
		{2, -2, 0, true},
		{-2, -2, -4, true},

		{mostNegative, -1, 0, false},
		{mostNegative, 1, mostNegative + 1, true},
		{mostPositive, -1, mostPositive - 1, true},
		{mostPositive, 1, 0, false},

		{mostNegative, mostPositive, -1, true},
		{mostPositive, mostNegative, -1, true},
		{mostPositive, mostPositive, 0, false},
		{mostNegative, mostNegative, 0, false},

		{-mostPositive, mostNegative, 0, false},
		{mostNegative, -mostPositive, 0, false},
		{-mostPositive, -mostPositive, 0, false},
	} {
		c, ok := int64Add(test.a, test.b)
		if c != test.c {
			t.Errorf("%v: unexpected result: %d", test, c)
		}
		if ok != test.ok {
			t.Errorf("%v: unexpected overflow: %t", test, ok)
		}
		// addition is commutative
		// 交换操作数后结果与溢出标志必须完全一致。
		d, ok2 := int64Add(test.b, test.a)
		if c != d || ok != ok2 {
			t.Errorf("%v: not commutative: %d %t", test, d, ok2)
		}
	}
}

// TestDetectOverflowMultiply 验证 int64Multiply 的乘法结果与溢出标志。
// 覆盖 0、±1、正负组合以及 int64 极值相乘，并检查乘法可交换。
func TestDetectOverflowMultiply(t *testing.T) {
	for _, test := range []struct {
		a, b int64
		c    int64
		ok   bool
	}{
		{0, 0, 0, true},
		{-1, 1, -1, true},
		{-1, -1, 1, true},
		{1, 1, 1, true},
		{0, 1, 0, true},
		{1, 0, 0, true},
		{2, 2, 4, true},
		{2, -2, -4, true},
		{-2, -2, 4, true},

		{mostNegative, -1, 0, false},
		{mostNegative, 1, mostNegative, true},
		{mostPositive, -1, -mostPositive, true},
		{mostPositive, 1, mostPositive, true},

		{mostNegative, mostPositive, 0, false},
		{mostPositive, mostNegative, 0, false},
		{mostPositive, mostPositive, 1, false},
		{mostNegative, mostNegative, 0, false},

		{-mostPositive, mostNegative, 0, false},
		{mostNegative, -mostPositive, 0, false},
		{-mostPositive, -mostPositive, 1, false},
	} {
		c, ok := int64Multiply(test.a, test.b)
		if c != test.c {
			t.Errorf("%v: unexpected result: %d", test, c)
		}
		if ok != test.ok {
			t.Errorf("%v: unexpected overflow: %t", test, ok)
		}
		// multiplication is commutative
		// 交换操作数后结果与溢出标志必须完全一致。
		d, ok2 := int64Multiply(test.b, test.a)
		if c != d || ok != ok2 {
			t.Errorf("%v: not commutative: %d %t", test, d, ok2)
		}
	}
}

// TestDetectOverflowScale 校验乘以 10 的幂的专用实现与通用乘法一致。
// 这些专用函数是 Quantity 十进制缩放的快路径，正确性必须与 int64Multiply 等价。
func TestDetectOverflowScale(t *testing.T) {
	for _, a := range []int64{0, -1, 1, 10, -10, mostPositive, mostNegative, -mostPositive} {
		for _, b := range []int64{1, 2, 10, 100, 1000, mostPositive} {
			// 以通用乘法为基准，验证 int64MultiplyScale 的结果与溢出标志。
			expect, expectOk := int64Multiply(a, b)

			c, ok := int64MultiplyScale(a, b)
			if c != expect {
				t.Errorf("%d*%d: unexpected result: %d", a, b, c)
			}
			if ok != expectOk {
				t.Errorf("%d*%d: unexpected overflow: %t", a, b, ok)
			}
		}
		for _, test := range []struct {
			base int64
			fn   func(a int64) (int64, bool)
		}{
			{10, int64MultiplyScale10},
			{100, int64MultiplyScale100},
			{1000, int64MultiplyScale1000},
		} {
			expect, expectOk := int64Multiply(a, test.base)
			c, ok := test.fn(a)
			if c != expect {
				t.Errorf("%d*%d: unexpected result: %d", a, test.base, c)
			}
			if ok != expectOk {
				t.Errorf("%d*%d: unexpected overflow: %t", a, test.base, ok)
			}
		}
	}
}

// TestRemoveInt64Factors 验证从数值中反复除以 max 因子，返回剩余值与缩放次数。
func TestRemoveInt64Factors(t *testing.T) {
	for _, test := range []struct {
		value  int64
		max    int64
		result int64
		scale  int32
	}{
		{100, 10, 1, 2},
		{100, 10, 1, 2},
		{100, 100, 1, 1},
		{1, 10, 1, 0},
	} {
		r, s := removeInt64Factors(test.value, test.max)
		if r != test.result {
			t.Errorf("%v: unexpected result: %d", test, r)
		}
		if s != test.scale {
			t.Errorf("%v: unexpected scale: %d", test, s)
		}
	}
}

// TestNegativeScaleInt64 验证按 10 的负幂缩放时的舍入结果与“是否精确”标志。
// 正负数值都应向远离零的方向做对称舍入；负缩放属于未定义行为，仅验证其不崩溃。
func TestNegativeScaleInt64(t *testing.T) {
	for _, test := range []struct {
		base   int64
		scale  Scale
		result int64
		exact  bool
	}{
		{1234567, 0, 1234567, true},
		{1234567, 1, 123457, false},
		{1234567, 2, 12346, false},
		{1234567, 3, 1235, false},
		{1234567, 4, 124, false},

		{-1234567, 0, -1234567, true},
		{-1234567, 1, -123457, false},
		{-1234567, 2, -12346, false},
		{-1234567, 3, -1235, false},
		{-1234567, 4, -124, false},

		{1000, 0, 1000, true},
		{1000, 1, 100, true},
		{1000, 2, 10, true},
		{1000, 3, 1, true},
		{1000, 4, 1, false},

		{-1000, 0, -1000, true},
		{-1000, 1, -100, true},
		{-1000, 2, -10, true},
		{-1000, 3, -1, true},
		{-1000, 4, -1, false},

		{0, 0, 0, true},
		{0, 1, 0, true},
		{0, 2, 0, true},

		// negative scale is undefined behavior
		// 负缩放未定义，这里仅确认实现不会因此 panic 或产生异常结果。
		{1000, -1, 1000, true},
	} {
		result, exact := negativeScaleInt64(test.base, test.scale)
		if result != test.result {
			t.Errorf("%v: unexpected result: %d", test, result)
		}
		if exact != test.exact {
			t.Errorf("%v: unexpected exact: %t", test, exact)
		}
	}
}
