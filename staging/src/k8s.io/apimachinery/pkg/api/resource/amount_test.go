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

// TestInt64AmountAsInt64 验证 int64Amount 转 int64 仅在能精确表示时成功：
// 负 scale 会丢小数位，超大正 scale 会溢出，两者都必须返回 ok=false 而不是静默截断。
func TestInt64AmountAsInt64(t *testing.T) {
	for _, test := range []struct {
		value  int64
		scale  Scale
		result int64
		ok     bool
	}{
		{100, 0, 100, true},
		{100, 1, 1000, true},
		{100, -5, 0, false},
		{100, 100, 0, false},
	} {
		r, ok := int64Amount{value: test.value, scale: test.scale}.AsInt64()
		if r != test.result {
			t.Errorf("%v: unexpected result: %d", test, r)
		}
		if ok != test.ok {
			t.Errorf("%v: unexpected ok: %t", test, ok)
		}
	}
}

// TestInt64AmountAdd 验证 int64Amount 的加法：不同 scale 要先对齐，溢出时返回 false
// 且不得改写接收者；同时正反两个方向相加（交换律）结果一致。
func TestInt64AmountAdd(t *testing.T) {
	for _, test := range []struct {
		a, b, c int64Amount
		ok      bool
	}{
		{int64Amount{value: 100, scale: 1}, int64Amount{value: 10, scale: 2}, int64Amount{value: 200, scale: 1}, true},
		{int64Amount{value: 100, scale: 1}, int64Amount{value: 1, scale: 2}, int64Amount{value: 110, scale: 1}, true},
		{int64Amount{value: 100, scale: 1}, int64Amount{value: 1, scale: 100}, int64Amount{value: 1, scale: 100}, false},
		{int64Amount{value: -5, scale: 2}, int64Amount{value: 50, scale: 1}, int64Amount{value: 0, scale: 1}, true},
		{int64Amount{value: -5, scale: 2}, int64Amount{value: 5, scale: 2}, int64Amount{value: 0, scale: 2}, true},

		{int64Amount{value: mostPositive, scale: -1}, int64Amount{value: 1, scale: -1}, int64Amount{value: 0, scale: -1}, false},
		{int64Amount{value: mostPositive, scale: -1}, int64Amount{value: 0, scale: -1}, int64Amount{value: mostPositive, scale: -1}, true},
		{int64Amount{value: mostPositive / 10, scale: 1}, int64Amount{value: 10, scale: 0}, int64Amount{value: mostPositive, scale: -1}, false},
	} {
		c := test.a
		ok := c.Add(test.b)
		if ok != test.ok {
			t.Errorf("%v: unexpected ok: %t", test, ok)
		}
		if ok {
			if c != test.c {
				t.Errorf("%v: unexpected result: %d", test, c)
			}
		} else {
			if c != test.a {
				t.Errorf("%v: overflow addition mutated source: %d", test, c)
			}
		}

		// 加法具有交换律
		c = test.b
		if ok := c.Add(test.a); ok != test.ok {
			t.Errorf("%v: unexpected ok: %t", test, ok)
		}
		if ok {
			if c != test.c {
				t.Errorf("%v: unexpected result: %d", test, c)
			}
		} else {
			if c != test.b {
				t.Errorf("%v: overflow addition mutated source: %d", test, c)
			}
		}
	}
}

// TestInt64AmountMul 验证 int64Amount 的乘法：溢出或无法保持精度时返回 false，
// 并且失败时不得改写接收者；覆盖正负边界值。
func TestInt64AmountMul(t *testing.T) {
	for _, test := range []struct {
		a  int64Amount
		b  int64
		c  int64Amount
		ok bool
	}{
		{int64Amount{value: 100, scale: 1}, 1000, int64Amount{value: 100000, scale: 1}, true},
		{int64Amount{value: 100, scale: -1}, 1000, int64Amount{value: 100000, scale: -1}, true},
		{int64Amount{value: 1, scale: 100}, 10, int64Amount{value: 1, scale: 100}, false},
		{int64Amount{value: 1, scale: -100}, 10, int64Amount{value: 1, scale: -100}, false},
		{int64Amount{value: -5, scale: 2}, 500, int64Amount{value: -2500, scale: 2}, true},
		{int64Amount{value: -5, scale: -2}, 500, int64Amount{value: -2500, scale: -2}, true},
		{int64Amount{value: 0, scale: 1}, 0, int64Amount{value: 0, scale: 1}, true},

		{int64Amount{value: mostPositive, scale: -1}, 10, int64Amount{value: mostPositive, scale: -1}, false},
		{int64Amount{value: mostPositive, scale: -1}, 0, int64Amount{value: 0, scale: 0}, true},
		{int64Amount{value: mostPositive, scale: 0}, 1, int64Amount{value: mostPositive, scale: 0}, true},
		{int64Amount{value: mostPositive / 10, scale: 1}, 10, int64Amount{value: mostPositive / 10, scale: 1}, false},
		{int64Amount{value: mostPositive, scale: 0}, -1, int64Amount{value: -mostPositive, scale: 0}, true},
		{int64Amount{value: mostNegative, scale: 0}, 1, int64Amount{value: mostNegative, scale: 0}, true},
		{int64Amount{value: mostNegative, scale: 1}, 0, int64Amount{value: 0, scale: 0}, true},
		{int64Amount{value: mostNegative, scale: 1}, 1, int64Amount{value: mostNegative, scale: 1}, false},
	} {
		c := test.a
		ok := c.Mul(test.b)
		if ok && !test.ok {
			t.Errorf("unextected success: %v", c)
		} else if !ok && test.ok {
			t.Errorf("unexpeted failure: %v", c)
		} else if ok {
			if c != test.c {
				t.Errorf("%v: unexpected result: %d", test, c)
			}
		} else {
			if c != test.a {
				t.Errorf("%v: overflow multiplication mutated source: %d", test, c)
			}
		}
	}
}

// TestInt64AsCanonicalString 验证 int64Amount 渲染为规范形式：去掉尾随零，
// 返回的指数与字符串值相乘后应等于原值。
func TestInt64AsCanonicalString(t *testing.T) {
	for _, test := range []struct {
		value    int64
		scale    Scale
		result   string
		exponent int32
	}{
		{100, 0, "100", 0},
		{100, 1, "1", 3},
		{100, -1, "10", 0},
		{10800, -10, "1080", -9},
	} {
		r, exp := int64Amount{value: test.value, scale: test.scale}.AsCanonicalBytes(nil)
		if string(r) != test.result {
			t.Errorf("%v: unexpected result: %s", test, r)
		}
		if exp != test.exponent {
			t.Errorf("%v: unexpected exponent: %d", test, exp)
		}
	}
}

// TestAmountSign 验证符号判断只取决于 value 的正负/零，与 scale 无关。
func TestAmountSign(t *testing.T) {
	table := []struct {
		i      int64Amount
		expect int
	}{
		{int64Amount{value: -50, scale: 1}, -1},
		{int64Amount{value: 0, scale: 1}, 0},
		{int64Amount{value: 300, scale: 1}, 1},
		{int64Amount{value: -50, scale: -8}, -1},
		{int64Amount{value: 50, scale: -8}, 1},
		{int64Amount{value: 0, scale: -8}, 0},
		{int64Amount{value: -50, scale: 0}, -1},
		{int64Amount{value: 50, scale: 0}, 1},
		{int64Amount{value: 0, scale: 0}, 0},
	}
	for _, testCase := range table {
		if result := testCase.i.Sign(); result != testCase.expect {
			t.Errorf("i: %v, Expected: %v, Actual: %v", testCase.i, testCase.expect, result)
		}
	}
}

// TestInt64AmountAsScaledInt64 验证换算到指定 scale 时的小数截断与溢出语义：
// 截断向零取整，无法精确表示或溢出的情况由 ok 标记。
func TestInt64AmountAsScaledInt64(t *testing.T) {
	for _, test := range []struct {
		name   string
		i      int64Amount
		scaled Scale
		result int64
		ok     bool
	}{
		{"test when i.scale < scaled ", int64Amount{value: 100, scale: 0}, 5, 1, true},
		{"test when i.scale = scaled", int64Amount{value: 100, scale: 1}, 1, 100, true},
		{"test when i.scale > scaled and result doesn't overflow", int64Amount{value: 100, scale: 5}, 2, 100000, true},
		{"test when i.scale > scaled and result overflows", int64Amount{value: 876, scale: 30}, 4, 0, false},
		{"test when i.scale < 0 and fraction exists", int64Amount{value: 93, scale: -1}, 0, 10, true},
		{"test when i.scale < 0 and fraction doesn't exist", int64Amount{value: 100, scale: -1}, 0, 10, true},
		{"test when i.value < 0 and fraction exists", int64Amount{value: -1932, scale: 2}, 4, -20, true},
		{"test when i.value < 0 and fraction doesn't exists", int64Amount{value: -1900, scale: 2}, 4, -19, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, ok := test.i.AsScaledInt64(test.scaled)
			if r != test.result {
				t.Errorf("%v: expected result: %d, got result: %d", test.name, test.result, r)
			}
			if ok != test.ok {
				t.Errorf("%v: expected ok: %t, got ok: %t", test.name, test.ok, ok)
			}
		})
	}
}
