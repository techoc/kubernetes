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
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"strings"
	"testing"
	"unicode"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/pflag"
	inf "gopkg.in/inf.v0"
	"sigs.k8s.io/randfill"

	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"k8s.io/utils/ptr"
)

var (
	// 预先构造 int64 边界的大整数，供 BigDec 等测试直接复用。
	bigMostPositive = big.NewInt(mostPositive)
	bigMostNegative = big.NewInt(mostNegative)
)

// dec 构造一个指数表示法的十进制量：Scale 是 exponent 的相反数。
func dec(i int64, exponent int) infDecAmount {
	// 参见下方测试：scale 与指数互为相反数。
	return infDecAmount{inf.NewDec(i, inf.Scale(-exponent))}
}

// bigDec 与 dec 相同，但支持超出 int64 的大整数。
func bigDec(i *big.Int, exponent int) infDecAmount {
	// 参见下方测试：scale 与指数互为相反数。
	return infDecAmount{inf.NewDecBig(i, inf.Scale(-exponent))}
}

// decQuantity 构造以 inf.Dec 存储的 Quantity，用于测试期望值。
func decQuantity(i int64, exponent int, format Format) Quantity {
	return Quantity{d: dec(i, exponent), Format: format}
}

// bigDecQuantity 构造以大整数 inf.Dec 存储的 Quantity。
func bigDecQuantity(i *big.Int, exponent int, format Format) Quantity {
	return Quantity{d: bigDec(i, exponent), Format: format}
}

// intQuantity 构造以 int64Amount 存储的 Quantity。
func intQuantity(i int64, exponent Scale, format Format) Quantity {
	return Quantity{i: int64Amount{value: i, scale: exponent}, Format: format}
}

// TestDec 验证 inf.Dec 在给定 scale 下的字符串表现符合指数语义。
func TestDec(t *testing.T) {
	table := []struct {
		got    infDecAmount
		expect string
	}{
		{dec(1, 0), "1"},
		{dec(1, 1), "10"},
		{dec(5, 2), "500"},
		{dec(8, 3), "8000"},
		{dec(2, 0), "2"},
		{dec(1, -1), "0.1"},
		{dec(3, -2), "0.03"},
		{dec(4, -3), "0.004"},
	}

	for _, item := range table {
		if e, a := item.expect, item.got.Dec.String(); e != a {
			t.Errorf("expected %v, got %v", e, a)
		}
	}
}

// TestBigDec 验证大整数（超出 int64 范围）在正负指数下的字符串表现。
func TestBigDec(t *testing.T) {
	table := []struct {
		got    infDecAmount
		expect string
	}{
		{bigDec(big.NewInt(1), 0), "1"},
		{bigDec(big.NewInt(1), 1), "10"},
		{bigDec(big.NewInt(5), 2), "500"},
		{bigDec(big.NewInt(8), 3), "8000"},
		{bigDec(big.NewInt(2), 0), "2"},
		{bigDec(big.NewInt(1), -1), "0.1"},
		{bigDec(big.NewInt(3), -2), "0.03"},
		{bigDec(big.NewInt(4), -3), "0.004"},
		{bigDec(big.NewInt(0).Add(bigMostPositive, big.NewInt(1)), 0), "9223372036854775808"},
		{bigDec(big.NewInt(0).Add(bigMostPositive, big.NewInt(1)), 1), "92233720368547758080"},
		{bigDec(big.NewInt(0).Add(bigMostPositive, big.NewInt(1)), 2), "922337203685477580800"},
		{bigDec(big.NewInt(0).Add(bigMostPositive, big.NewInt(1)), -1), "922337203685477580.8"},
		{bigDec(big.NewInt(0).Add(bigMostPositive, big.NewInt(1)), -2), "92233720368547758.08"},
		{bigDec(big.NewInt(0).Sub(bigMostNegative, big.NewInt(1)), 0), "-9223372036854775809"},
		{bigDec(big.NewInt(0).Sub(bigMostNegative, big.NewInt(1)), 1), "-92233720368547758090"},
		{bigDec(big.NewInt(0).Sub(bigMostNegative, big.NewInt(1)), 2), "-922337203685477580900"},
		{bigDec(big.NewInt(0).Sub(bigMostNegative, big.NewInt(1)), -1), "-922337203685477580.9"},
		{bigDec(big.NewInt(0).Sub(bigMostNegative, big.NewInt(1)), -2), "-92233720368547758.09"},
	}

	for _, item := range table {
		if e, a := item.expect, item.got.Dec.String(); e != a {
			t.Errorf("expected %v, got %v", e, a)
		}
	}
}

// 零值无论以何种后缀解析，序列化都应得到 "0"，而不带任何单位。
func TestQuantityParseZero(t *testing.T) {
	zero := MustParse("0")
	if expected, actual := "0", zero.String(); expected != actual {
		t.Errorf("Expected %v, actual %v", expected, actual)
	}
}

// MustParse 在解析失败时会 panic，这里确认该约定没有被悄悄改掉。
func TestQuantityParseNonNumericPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("MustParse did not panic")
		}
	}()
	_ = MustParse("Non-Numeric")
}

// 加零不应改变值的规范字符串形式，两个操作数顺序都要覆盖。
func TestQuantityAddZeroPreservesSuffix(t *testing.T) {
	testValues := []string{"100m", "1Gi"}
	zero := MustParse("0")
	for _, testValue := range testValues {
		value := MustParse(testValue)
		v1 := value.DeepCopy()
		// 确保非零 + 零 = 非零（保留后缀）
		v1.Add(zero)
		// 确保零 + 非零 = 非零（保留后缀）
		v2 := zero.DeepCopy()
		v2.Add(value)

		if v1.String() != testValue {
			t.Errorf("Expected %v, actual %v", testValue, v1.String())
			continue
		}
		if v2.String() != testValue {
			t.Errorf("Expected %v, actual %v", testValue, v2.String())
		}
	}
}

// 减零保持值不变，零减非零则得到相反数，两种情况都要保留后缀表示。
func TestQuantitySubZeroPreservesSuffix(t *testing.T) {
	testValues := []string{"100m", "1Gi"}
	zero := MustParse("0")
	for _, testValue := range testValues {
		value := MustParse(testValue)
		v1 := value.DeepCopy()
		// 确保非零 - 零 = 非零（保留后缀）
		v1.Sub(zero)
		// 确保保留输入值。
		if v1.String() != testValue {
			t.Errorf("Expected %v, actual %v", testValue, v1.String())
		}

		// 确保零 - 非零 = -非零（保留后缀）
		v2 := zero.DeepCopy()
		v2.Sub(value)
		negVal := value.DeepCopy()
		negVal.Neg()
		if v2.String() != negVal.String() {
			t.Errorf("Expected %v, actual %v", negVal.String(), v2.String())
		}
	}
}

// 即 1000m 减去 1 之后内部为 0，规范字符串必须收敛为 "0" 而非 "0m"。
func TestQuantityCanocicalizeZero(t *testing.T) {
	val := MustParse("1000m")
	val.i.Sub(int64Amount{value: 1})
	zero := Quantity{i: val.i, Format: DecimalSI}
	if expected, actual := "0", zero.String(); expected != actual {
		t.Errorf("Expected %v, actual %v", expected, actual)
	}
}

// TestQuantityCmp 覆盖 int64Amount 与 inf.Dec 两种内部表示下的比较语义，
// 包括 nil 值（视为 0）与零值之间的边界。
func TestQuantityCmp(t *testing.T) {
	// 测试 d 为 nil 的情况。
	table := []struct {
		x      string
		y      string
		expect int
	}{
		{"0", "0", 0},
		{"100m", "50m", 1},
		{"50m", "100m", -1},
		{"10000T", "100Gi", 1},
	}
	for _, testCase := range table {
		q1 := MustParse(testCase.x)
		q2 := MustParse(testCase.y)
		if result := q1.Cmp(q2); result != testCase.expect {
			t.Errorf("X: %v, Y: %v, Expected: %v, Actual: %v", testCase.x, testCase.y, testCase.expect, result)
		}
	}
	// 测试 i 为 {0,0} 的情况。
	table2 := []struct {
		x      *inf.Dec
		y      *inf.Dec
		expect int
	}{
		{dec(0, 0).Dec, dec(0, 0).Dec, 0},
		{nil, dec(0, 0).Dec, 0},
		{dec(0, 0).Dec, nil, 0},
		{nil, nil, 0},
		{nil, dec(10, 0).Dec, -1},
		{nil, dec(-10, 0).Dec, 1},
		{dec(10, 0).Dec, nil, 1},
		{dec(-10, 0).Dec, nil, -1},
	}
	for _, testCase := range table2 {
		q1 := Quantity{d: infDecAmount{testCase.x}, Format: DecimalSI}
		q2 := Quantity{d: infDecAmount{testCase.y}, Format: DecimalSI}
		if result := q1.Cmp(q2); result != testCase.expect {
			t.Errorf("X: %v, Y: %v, Expected: %v, Actual: %v", testCase.x, testCase.y, testCase.expect, result)
		}
	}
}

// TestParseQuantityString 验证底层词法拆分：符号、整数部分、小数部分与后缀各自独立返回，
// 非法输入（如单独的小数点）也在这一层接受并交由上层处理。
func TestParseQuantityString(t *testing.T) {
	table := []struct {
		input              string
		positive           bool
		value              string
		num, denom, suffix string
	}{
		{"0.025Ti", true, "0.025", "0", "025", "Ti"},
		{"1.025Ti", true, "1.025", "1", "025", "Ti"},
		{"-1.025Ti", false, "-1.025", "1", "025", "Ti"},
		{".", true, ".", "0", "", ""},
		{"-.", false, "-.", "0", "", ""},
		{"1E-3", true, "1", "1", "", "E-3"},
	}
	for _, test := range table {
		positive, value, num, denom, suffix, err := parseQuantityString(test.input)
		if err != nil {
			t.Errorf("%s: error: %v", test.input, err)
			continue
		}
		if positive != test.positive || value != test.value || num != test.num || denom != test.denom || suffix != test.suffix {
			t.Errorf("%s: unmatched: %t %q %q %q %q", test.input, positive, value, num, denom, suffix)
		}
	}
}

// TestQuantityParse 是解析与序列化的主表驱动测试：覆盖二进制/十进制后缀、
// 指数记法、小数、饱和与舍入，以及非法输入。同一组用例还会分别以 AsDec 表示、
// 取负、加显式 "+" 的方式重跑，确保这些变换不改变数值与格式。
func TestQuantityParse(t *testing.T) {
	if _, err := ParseQuantity(""); err == nil {
		t.Errorf("expected empty string to return error")
	}

	table := []struct {
		input  string
		expect Quantity
	}{
		{"0", decQuantity(0, 0, DecimalSI)},
		{"0n", decQuantity(0, 0, DecimalSI)},
		{"0u", decQuantity(0, 0, DecimalSI)},
		{"0m", decQuantity(0, 0, DecimalSI)},
		{"0Ki", decQuantity(0, 0, BinarySI)},
		{"0k", decQuantity(0, 0, DecimalSI)},
		{"0Mi", decQuantity(0, 0, BinarySI)},
		{"0M", decQuantity(0, 0, DecimalSI)},
		{"0Gi", decQuantity(0, 0, BinarySI)},
		{"0G", decQuantity(0, 0, DecimalSI)},
		{"0Ti", decQuantity(0, 0, BinarySI)},
		{"0T", decQuantity(0, 0, DecimalSI)},

		// 允许小于 1 的数量。
		{"1", decQuantity(1, 0, DecimalSI)},

		// 二进制后缀。
		{"1Ki", decQuantity(1024, 0, BinarySI)},
		{"8Ki", decQuantity(8*1024, 0, BinarySI)},
		{"7Mi", decQuantity(7*1024*1024, 0, BinarySI)},
		{"6Gi", decQuantity(6*1024*1024*1024, 0, BinarySI)},
		{"5Ti", decQuantity(5*1024*1024*1024*1024, 0, BinarySI)},
		{"4Pi", decQuantity(4*1024*1024*1024*1024*1024, 0, BinarySI)},
		{"3Ei", decQuantity(3*1024*1024*1024*1024*1024*1024, 0, BinarySI)},

		{"10Ti", decQuantity(10*1024*1024*1024*1024, 0, BinarySI)},
		{"100Ti", decQuantity(100*1024*1024*1024*1024, 0, BinarySI)},

		// 十进制后缀。
		{"5n", decQuantity(5, -9, DecimalSI)},
		{"4u", decQuantity(4, -6, DecimalSI)},
		{"3m", decQuantity(3, -3, DecimalSI)},
		{"9", decQuantity(9, 0, DecimalSI)},
		{"8k", decQuantity(8, 3, DecimalSI)},
		{"50k", decQuantity(5, 4, DecimalSI)},
		{"7M", decQuantity(7, 6, DecimalSI)},
		{"6G", decQuantity(6, 9, DecimalSI)},
		{"5T", decQuantity(5, 12, DecimalSI)},
		{"40T", decQuantity(4, 13, DecimalSI)},
		{"300T", decQuantity(3, 14, DecimalSI)},
		{"2P", decQuantity(2, 15, DecimalSI)},
		{"1E", decQuantity(1, 18, DecimalSI)},

		// 十进制指数。
		{"1E-3", decQuantity(1, -3, DecimalExponent)},
		{"1e3", decQuantity(1, 3, DecimalExponent)},
		{"1E6", decQuantity(1, 6, DecimalExponent)},
		{"1e9", decQuantity(1, 9, DecimalExponent)},
		{"1E12", decQuantity(1, 12, DecimalExponent)},
		{"1e15", decQuantity(1, 15, DecimalExponent)},
		{"1E18", decQuantity(1, 18, DecimalExponent)},

		// 非标准但仍可解析。
		{"1e14", decQuantity(1, 14, DecimalExponent)},
		{"1e13", decQuantity(1, 13, DecimalExponent)},
		{"1e3", decQuantity(1, 3, DecimalExponent)},
		{"100.035k", decQuantity(100035, 0, DecimalSI)},

		// 看似浮点数的输入。
		{"0.001", decQuantity(1, -3, DecimalSI)},
		{"0.0005k", decQuantity(5, -1, DecimalSI)},
		{"0.005", decQuantity(5, -3, DecimalSI)},
		{"0.05", decQuantity(5, -2, DecimalSI)},
		{"0.5", decQuantity(5, -1, DecimalSI)},
		{"0.00050k", decQuantity(5, -1, DecimalSI)},
		{"0.00500", decQuantity(5, -3, DecimalSI)},
		{"0.05000", decQuantity(5, -2, DecimalSI)},
		{"0.50000", decQuantity(5, -1, DecimalSI)},
		{"0.5e0", decQuantity(5, -1, DecimalExponent)},
		{"0.5e-1", decQuantity(5, -2, DecimalExponent)},
		{"0.5e-2", decQuantity(5, -3, DecimalExponent)},
		{"0.5e0", decQuantity(5, -1, DecimalExponent)},
		{"10.035M", decQuantity(10035, 3, DecimalSI)},

		{"1.2e3", decQuantity(12, 2, DecimalExponent)},
		{"1.3E+6", decQuantity(13, 5, DecimalExponent)},
		{"1.40e9", decQuantity(14, 8, DecimalExponent)},
		{"1.53E12", decQuantity(153, 10, DecimalExponent)},
		{"1.6e15", decQuantity(16, 14, DecimalExponent)},
		{"1.7E18", decQuantity(17, 17, DecimalExponent)},

		{"9.01", decQuantity(901, -2, DecimalSI)},
		{"8.1k", decQuantity(81, 2, DecimalSI)},
		{"7.123456M", decQuantity(7123456, 0, DecimalSI)},
		{"6.987654321G", decQuantity(6987654321, 0, DecimalSI)},
		{"5.444T", decQuantity(5444, 9, DecimalSI)},
		{"40.1T", decQuantity(401, 11, DecimalSI)},
		{"300.2T", decQuantity(3002, 11, DecimalSI)},
		{"2.5P", decQuantity(25, 14, DecimalSI)},
		{"1.01E", decQuantity(101, 16, DecimalSI)},

		// 会饱和或舍入的输入。
		{"3.001n", decQuantity(4, -9, DecimalSI)},
		{"1.1E-9", decQuantity(2, -9, DecimalExponent)},
		{"0.0000000001", decQuantity(1, -9, DecimalSI)},
		{"0.0000000005", decQuantity(1, -9, DecimalSI)},
		{"0.00000000050", decQuantity(1, -9, DecimalSI)},
		{"0.5e-9", decQuantity(1, -9, DecimalExponent)},
		{"0.9n", decQuantity(1, -9, DecimalSI)},
		{"0.00000012345", decQuantity(124, -9, DecimalSI)},
		{"0.00000012354", decQuantity(124, -9, DecimalSI)},
		{"9Ei", Quantity{d: maxAllowed, Format: BinarySI}},
		{"9223372036854775807Ki", Quantity{d: maxAllowed, Format: BinarySI}},
		{"12E", decQuantity(12, 18, DecimalSI)},

		// 也接受分数的二进制单位。
		{"100.035Ki", decQuantity(10243584, -2, BinarySI)},
		{"0.5Mi", decQuantity(.5*1024*1024, 0, BinarySI)},
		{"0.05Gi", decQuantity(536870912, -1, BinarySI)},
		{"0.025Ti", decQuantity(274877906944, -1, BinarySI)},

		// 恶意构造的输入。
		{"0.000000000001Ki", decQuantity(2, -9, DecimalSI)}, // 向上取整并改变格式。
		{".001", decQuantity(1, -3, DecimalSI)},
		{".0001k", decQuantity(100, -3, DecimalSI)},
		{"1.", decQuantity(1, 0, DecimalSI)},
		{"1.G", decQuantity(1, 9, DecimalSI)},
	}

	// 分别以默认表示和强制转换为 inf.Dec 表示各跑一遍，验证两条内部路径结果一致。
	for _, asDec := range []bool{false, true} {
		for _, item := range table {
			got, err := ParseQuantity(item.input)
			if err != nil {
				t.Errorf("%v: unexpected error: %v", item.input, err)
				continue
			}
			if asDec {
				got.AsDec()
			}

			if e, a := item.expect, got; e.Cmp(a) != 0 {
				t.Errorf("%v: expected %v, got %v", item.input, e.String(), a.String())
			}
			if e, a := item.expect.Format, got.Format; e != a {
				t.Errorf("%v: expected %#v, got %#v", item.input, e, a)
			}

			if asDec {
				// 已用 inf.Dec 表示的量不可能再以 int64 精确返回。
				if i, ok := got.AsInt64(); i != 0 || ok {
					t.Errorf("%v: expected inf.Dec to return false for AsInt64: %d", item.input, i)
				}
				continue
			}
			i, ok := item.expect.AsInt64()
			if !ok {
				continue
			}
			j, ok := got.AsInt64()
			if !ok {
				if got.d.Dec == nil && got.i.scale >= 0 {
					t.Errorf("%v: is an int64Amount, but can't return AsInt64: %v", item.input, got)
				}
				continue
			}
			if i != j {
				t.Errorf("%v: expected equivalent representation as int64: %d %d", item.input, i, j)
			}
		}

		// 验证从 inf.Dec 再构造成 Quantity 后与原值可比较（三种 Format 都试）。
		for _, item := range table {
			got, err := ParseQuantity(item.input)
			if err != nil {
				t.Errorf("%v: unexpected error: %v", item.input, err)
				continue
			}

			if asDec {
				got.AsDec()
			}

			for _, format := range []Format{DecimalSI, BinarySI, DecimalExponent} {
				// 创建新的 inf.Dec，确保并非只比较指针。
				var copied inf.Dec
				copied.Add(inf.NewDec(0, inf.Scale(0)), got.AsDec())
				q := NewDecimalQuantity(copied, format)
				if c := q.Cmp(got); c != 0 {
					t.Errorf("%v: round trip from decimal back to quantity is not comparable: %d: %#v vs %#v", item.input, c, got, q)
				}
			}

			// 把输入拆成符号、整数、小数、后缀后再拼回，应得到同一数值。
			positive, _, num, denom, suffix, err := parseQuantityString(item.input)
			if err != nil {
				t.Errorf("%v: unexpected error: %v", item.input, err)
				continue
			}
			if got.Sign() >= 0 && !positive || got.Sign() < 0 && positive {
				t.Errorf("%v: positive was incorrect: %t", item.input, positive)
				continue
			}
			var value string
			if !positive {
				value = "-"
			}
			value += num
			if len(denom) > 0 {
				value += "." + denom
			}
			value += suffix
			if len(value) == 0 {
				t.Errorf("%v: did not parse correctly, %q %q %q", item.input, num, denom, suffix)
			}
			expected, err := ParseQuantity(value)
			if err != nil {
				t.Errorf("%v: unexpected error for %s: %v", item.input, value, err)
				continue
			}
			if expected.Cmp(got) != 0 {
				t.Errorf("%v: not the same as %s", item.input, value)
				continue
			}
		}

		// 取负后数值应为相反数、格式保持不变。
		desired := &inf.Dec{}
		expect := Quantity{d: infDecAmount{Dec: desired}}
		for _, item := range table {
			got, err := ParseQuantity("-" + strings.TrimLeftFunc(item.input, unicode.IsSpace))
			if err != nil {
				t.Errorf("-%v: unexpected error: %v", item.input, err)
				continue
			}
			if asDec {
				got.AsDec()
			}

			expected := item.expect
			desired.Neg(expected.AsDec())

			if e, a := expect, got; e.Cmp(a) != 0 {
				t.Errorf("%v: expected %s, got %s", item.input, e.String(), a.String())
			}
			if e, a := expected.Format, got.Format; e != a {
				t.Errorf("%v: expected %#v, got %#v", item.input, e, a)
			}
		}

		// 显式正号不应影响解析结果。
		for _, item := range table {
			got, err := ParseQuantity("+" + strings.TrimLeftFunc(item.input, unicode.IsSpace))
			if err != nil {
				t.Errorf("-%v: unexpected error: %v", item.input, err)
				continue
			}
			if asDec {
				got.AsDec()
			}

			if e, a := item.expect, got; e.Cmp(a) != 0 {
				t.Errorf("%v(%t): expected %s, got %s", item.input, asDec, e.String(), a.String())
			}
			if e, a := item.expect.Format, got.Format; e != a {
				t.Errorf("%v: expected %#v, got %#v", item.input, e, a)
			}
		}
	}

	// 非法输入必须被拒绝；注意首尾空白也不允许。
	invalid := []string{
		"1.1.M",
		"1+1.0M",
		"0.1mi",
		"0.1am",
		"aoeu",
		".5i",
		"1i",
		"-3.01i",
		"-3.01e-",

		// 禁止尾随空白。
		" 1",
		"1 ",
	}
	for _, item := range invalid {
		_, err := ParseQuantity(item)
		if err == nil {
			t.Errorf("%v parsed unexpectedly", item)
		}
	}
}

// TestQuantityRoundUp 验证按给定 scale 向上舍入：ok 表示舍入是否精确（未发生进位）；
// 同时覆盖正负值以及 int64Amount/inf.Dec 两种内部表示。
func TestQuantityRoundUp(t *testing.T) {
	table := []struct {
		in     string
		scale  Scale
		expect Quantity
		ok     bool
	}{
		{"9.01", -3, decQuantity(901, -2, DecimalSI), true},
		{"9.01", -2, decQuantity(901, -2, DecimalSI), true},
		{"9.01", -1, decQuantity(91, -1, DecimalSI), false},
		{"9.01", 0, decQuantity(10, 0, DecimalSI), false},
		{"9.01", 1, decQuantity(10, 0, DecimalSI), false},
		{"9.01", 2, decQuantity(100, 0, DecimalSI), false},

		{"-9.01", -3, decQuantity(-901, -2, DecimalSI), true},
		{"-9.01", -2, decQuantity(-901, -2, DecimalSI), true},
		{"-9.01", -1, decQuantity(-91, -1, DecimalSI), false},
		{"-9.01", 0, decQuantity(-10, 0, DecimalSI), false},
		{"-9.01", 1, decQuantity(-10, 0, DecimalSI), false},
		{"-9.01", 2, decQuantity(-100, 0, DecimalSI), false},
	}

	for _, asDec := range []bool{false, true} {
		for _, item := range table {
			got, err := ParseQuantity(item.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expect := item.expect.DeepCopy()
			if asDec {
				got.AsDec()
			}
			if ok := got.RoundUp(item.scale); ok != item.ok {
				t.Errorf("%s(%d,%t): unexpected ok: %t", item.in, item.scale, asDec, ok)
			}
			if got.Cmp(expect) != 0 {
				t.Errorf("%s(%d,%t): unexpected round: %s vs %s", item.in, item.scale, asDec, got.String(), expect.String())
			}
		}
	}
}

// TestQuantityCmpInt64AndDec 验证 int64Amount 与 inf.Dec 混用、以及大数溢出边界下的比较；
// 每轮都要求 a.Cmp(b) == -b.Cmp(a)，比较必须反对称。
func TestQuantityCmpInt64AndDec(t *testing.T) {
	table := []struct {
		a, b Quantity
		cmp  int
	}{
		{intQuantity(901, -2, DecimalSI), intQuantity(901, -2, DecimalSI), 0},
		{intQuantity(90, -1, DecimalSI), intQuantity(901, -2, DecimalSI), -1},
		{intQuantity(901, -2, DecimalSI), intQuantity(900, -2, DecimalSI), 1},
		{intQuantity(0, 0, DecimalSI), intQuantity(0, 0, DecimalSI), 0},
		{intQuantity(0, 1, DecimalSI), intQuantity(0, -1, DecimalSI), 0},
		{intQuantity(0, -1, DecimalSI), intQuantity(0, 1, DecimalSI), 0},
		{intQuantity(800, -3, DecimalSI), intQuantity(1, 0, DecimalSI), -1},
		{intQuantity(800, -3, DecimalSI), intQuantity(79, -2, DecimalSI), 1},

		{intQuantity(mostPositive, 0, DecimalSI), intQuantity(1, -1, DecimalSI), 1},
		{intQuantity(mostPositive, 1, DecimalSI), intQuantity(1, 0, DecimalSI), 1},
		{intQuantity(mostPositive, 1, DecimalSI), intQuantity(1, 1, DecimalSI), 1},
		{intQuantity(mostPositive, 1, DecimalSI), intQuantity(0, 1, DecimalSI), 1},
		{intQuantity(mostPositive, -16, DecimalSI), intQuantity(1, 3, DecimalSI), -1},

		{intQuantity(mostNegative, 0, DecimalSI), intQuantity(0, 0, DecimalSI), -1},
		{intQuantity(mostNegative, -18, DecimalSI), intQuantity(-1, 0, DecimalSI), -1},
		{intQuantity(mostNegative, -19, DecimalSI), intQuantity(-1, 0, DecimalSI), 1},

		{intQuantity(1*1000000*1000000*1000000, -17, DecimalSI), intQuantity(1, 1, DecimalSI), 0},
		{intQuantity(1*1000000*1000000*1000000, -17, DecimalSI), intQuantity(-10, 0, DecimalSI), 1},
		{intQuantity(-1*1000000*1000000*1000000, -17, DecimalSI), intQuantity(-10, 0, DecimalSI), 0},
		{intQuantity(1*1000000*1000000*1000000, -17, DecimalSI), intQuantity(1, 0, DecimalSI), 1},

		{intQuantity(1*1000000*1000000*1000000+1, -17, DecimalSI), intQuantity(1, 1, DecimalSI), 1},
		{intQuantity(1*1000000*1000000*1000000-1, -17, DecimalSI), intQuantity(1, 1, DecimalSI), -1},
	}

	for _, item := range table {
		if cmp := item.a.Cmp(item.b); cmp != item.cmp {
			t.Errorf("%#v: unexpected Cmp: %d", item, cmp)
		}
		if cmp := item.b.Cmp(item.a); cmp != -item.cmp {
			t.Errorf("%#v: unexpected inverted Cmp: %d", item, cmp)
		}
	}

	for _, item := range table {
		a, b := item.a.DeepCopy(), item.b.DeepCopy()
		a.AsDec()
		if cmp := a.Cmp(b); cmp != item.cmp {
			t.Errorf("%#v: unexpected Cmp: %d", item, cmp)
		}
		if cmp := b.Cmp(a); cmp != -item.cmp {
			t.Errorf("%#v: unexpected inverted Cmp: %d", item, cmp)
		}
	}

	for _, item := range table {
		a, b := item.a.DeepCopy(), item.b.DeepCopy()
		b.AsDec()
		if cmp := a.Cmp(b); cmp != item.cmp {
			t.Errorf("%#v: unexpected Cmp: %d", item, cmp)
		}
		if cmp := b.Cmp(a); cmp != -item.cmp {
			t.Errorf("%#v: unexpected inverted Cmp: %d", item, cmp)
		}
	}

	for _, item := range table {
		a, b := item.a.DeepCopy(), item.b.DeepCopy()
		a.AsDec()
		b.AsDec()
		if cmp := a.Cmp(b); cmp != item.cmp {
			t.Errorf("%#v: unexpected Cmp: %d", item, cmp)
		}
		if cmp := b.Cmp(a); cmp != -item.cmp {
			t.Errorf("%#v: unexpected inverted Cmp: %d", item, cmp)
		}
	}
}

// TestQuantityNeg 验证取负不修改源对象，且两种内部表示得到的规范字符串一致。
func TestQuantityNeg(t *testing.T) {
	table := []struct {
		a   Quantity
		out string
	}{
		{intQuantity(901, -2, DecimalSI), "-9010m"},
		{decQuantity(901, -2, DecimalSI), "-9010m"},
	}

	for i, item := range table {
		out := item.a.DeepCopy()
		out.Neg()
		if out.Cmp(item.a) == 0 {
			t.Errorf("%d: negating an item should not mutate the source: %s", i, out.String())
		}
		if out.String() != item.out {
			t.Errorf("%d: negating did not equal exact value: %s", i, out.String())
		}
	}
}

// TestQuantityString 验证各 Format 下的规范字符串输出，并检查解析会缓存规范串、
// 等价的替代写法会被归一化，且负数输出不会出现 "-0"。
func TestQuantityString(t *testing.T) {
	table := []struct {
		in        Quantity
		expect    string
		alternate string
	}{
		{decQuantity(1024*1024*1024, 0, BinarySI), "1Gi", "1024Mi"},
		{decQuantity(300*1024*1024, 0, BinarySI), "300Mi", "307200Ki"},
		{decQuantity(6*1024, 0, BinarySI), "6Ki", ""},
		{decQuantity(1001*1024*1024*1024, 0, BinarySI), "1001Gi", "1025024Mi"},
		{decQuantity(1024*1024*1024*1024, 0, BinarySI), "1Ti", "1024Gi"},
		{decQuantity(5, 0, BinarySI), "5", "5000m"},
		{decQuantity(500, -3, BinarySI), "500m", "0.5"},
		{decQuantity(1, 9, DecimalSI), "1G", "1000M"},
		{decQuantity(1000, 6, DecimalSI), "1G", "0.001T"},
		{decQuantity(1000000, 3, DecimalSI), "1G", ""},
		{decQuantity(1000000000, 0, DecimalSI), "1G", ""},
		{decQuantity(1, -3, DecimalSI), "1m", "1000u"},
		{decQuantity(80, -3, DecimalSI), "80m", ""},
		{decQuantity(1080, -3, DecimalSI), "1080m", "1.08"},
		{decQuantity(108, -2, DecimalSI), "1080m", "1080000000n"},
		{decQuantity(10800, -4, DecimalSI), "1080m", ""},
		{decQuantity(300, 6, DecimalSI), "300M", ""},
		{decQuantity(1, 12, DecimalSI), "1T", ""},
		{decQuantity(1234567, 6, DecimalSI), "1234567M", ""},
		{decQuantity(1234567, -3, BinarySI), "1234567m", ""},
		{decQuantity(3, 3, DecimalSI), "3k", ""},
		{decQuantity(1025, 0, BinarySI), "1025", ""},
		{decQuantity(0, 0, DecimalSI), "0", ""},
		{decQuantity(0, 0, BinarySI), "0", ""},
		{decQuantity(1, 9, DecimalExponent), "1e9", ".001e12"},
		{decQuantity(1, -3, DecimalExponent), "1e-3", "0.001e0"},
		{decQuantity(1, -9, DecimalExponent), "1e-9", "1000e-12"},
		{decQuantity(80, -3, DecimalExponent), "80e-3", ""},
		{decQuantity(300, 6, DecimalExponent), "300e6", ""},
		{decQuantity(1, 12, DecimalExponent), "1e12", ""},
		{decQuantity(1, 3, DecimalExponent), "1e3", ""},
		{decQuantity(3, 3, DecimalExponent), "3e3", ""},
		{decQuantity(3, 3, DecimalSI), "3k", ""},
		{decQuantity(0, 0, DecimalExponent), "0", "00"},
		{decQuantity(1, -9, DecimalSI), "1n", ""},
		{decQuantity(80, -9, DecimalSI), "80n", ""},
		{decQuantity(1080, -9, DecimalSI), "1080n", ""},
		{decQuantity(108, -8, DecimalSI), "1080n", ""},
		{decQuantity(10800, -10, DecimalSI), "1080n", ""},
		{decQuantity(1, -6, DecimalSI), "1u", ""},
		{decQuantity(80, -6, DecimalSI), "80u", ""},
		{decQuantity(1080, -6, DecimalSI), "1080u", ""},
	}
	for _, item := range table {
		got := item.in.String()
		if e, a := item.expect, got; e != a {
			t.Errorf("%#v: expected %v, got %v", item.in, e, a)
		}
		q, err := ParseQuantity(item.expect)
		if err != nil {
			t.Errorf("%#v: unexpected error: %v", item.expect, err)
		}
		if len(q.s) == 0 || q.s != item.expect {
			t.Errorf("%#v: did not copy canonical string on parse: %s", item.expect, q.s)
		}
		if len(item.alternate) == 0 {
			continue
		}
		q, err = ParseQuantity(item.alternate)
		if err != nil {
			t.Errorf("%#v: unexpected error: %v", item.expect, err)
			continue
		}
		if len(q.s) != 0 {
			t.Errorf("%#v: unexpected nested string: %v", item.expect, q.s)
		}
		if q.String() != item.expect {
			t.Errorf("%#v: unexpected alternate canonical: %v", item.expect, q.String())
		}
		if len(q.s) == 0 || q.s != item.expect {
			t.Errorf("%#v: did not set canonical string on ToString: %s", item.expect, q.s)
		}
	}
	desired := &inf.Dec{} // 避免修改表中的值。
	for _, item := range table {
		if item.in.Cmp(Quantity{}) == 0 {
			// 不应输出 "-0"。
			continue
		}
		q := item.in
		q.d = infDecAmount{desired.Neg(q.AsDec())}
		if e, a := "-"+item.expect, q.String(); e != a {
			t.Errorf("%#v: expected %v, got %v", item.in, e, a)
		}
	}
}

// TestQuantityParseEmit 验证“解析-输出”往返：某些非规范输入（如 1024Mi）会被归一到更简短的规范形式。
func TestQuantityParseEmit(t *testing.T) {
	table := []struct {
		in     string
		expect string
	}{
		{"1Ki", "1Ki"},
		{"1Mi", "1Mi"},
		{"1Gi", "1Gi"},
		{"1024Mi", "1Gi"},
		{"1000M", "1G"},
		{".001Ki", "1024m"},
		{".000001Ki", "1024u"},
		{".000000001Ki", "1024n"},
		{".000000000001Ki", "2n"},
	}

	for _, item := range table {
		q, err := ParseQuantity(item.in)
		if err != nil {
			t.Errorf("Couldn't parse %v", item.in)
			continue
		}
		if e, a := item.expect, q.String(); e != a {
			t.Errorf("%#v: expected %v, got %v", item.in, e, a)
		}
	}
	for _, item := range table {
		q, err := ParseQuantity("-" + item.in)
		if err != nil {
			t.Errorf("Couldn't parse %v", item.in)
			continue
		}
		if q.Cmp(Quantity{}) == 0 {
			continue
		}
		if e, a := "-"+item.expect, q.String(); e != a {
			t.Errorf("%#v: expected %v, got %v (%#v)", item.in, e, a, q.i)
		}
	}
}

// fuzzer 生成结构合法的随机 Quantity，覆盖三种 Format 及 int64/inf.Dec 两种存储形态，
// 特别构造 1Mi、1M 这类带单位的边界值。JSON/CBOR 往返测试共用它。
var fuzzer = randfill.New().Funcs(
	func(q *Quantity, c randfill.Continue) {
		q.i = Zero
		if c.Bool() {
			q.Format = BinarySI
			if c.Bool() {
				dec := &inf.Dec{}
				q.d = infDecAmount{Dec: dec}
				dec.SetScale(0)
				dec.SetUnscaled(c.Int63())
				return
			}
			// 确保覆盖 1Mi 等用例。
			dec := &inf.Dec{}
			q.d = infDecAmount{Dec: dec}
			dec.SetScale(0)
			dec.SetUnscaled(c.Int63n(1024) << uint(10*c.Intn(5)))
			return
		}
		if c.Bool() {
			q.Format = DecimalSI
		} else {
			q.Format = DecimalExponent
		}
		if c.Bool() {
			dec := &inf.Dec{}
			q.d = infDecAmount{Dec: dec}
			dec.SetScale(inf.Scale(c.Intn(4)))
			dec.SetUnscaled(c.Int63())
			return
		}
		// 确保覆盖 1M 等用例。
		dec := &inf.Dec{}
		q.d = infDecAmount{Dec: dec}
		dec.SetScale(inf.Scale(3 - c.Intn(15)))
		dec.SetUnscaled(c.Int63n(1000))
	},
)

// TestQuantityDeepCopy 验证深拷贝后修改副本不影响原值，覆盖 nil 与 inf.Dec 两种存储。
func TestQuantityDeepCopy(t *testing.T) {
	// 测试 d 为 nil 的情况。
	slice := []string{"0", "100m", "50m", "10000T"}
	for _, testCase := range slice {
		q := MustParse(testCase)
		if result := q.DeepCopy(); result != q {
			t.Errorf("Expected: %v, Actual: %v", q, result)
		}
	}
	table := []*inf.Dec{
		dec(0, 0).Dec,
		dec(10, 0).Dec,
		dec(-10, 0).Dec,
	}
	// 测试 i 为 {0,0} 的情况。
	for _, testCase := range table {
		q := Quantity{d: infDecAmount{testCase}, Format: DecimalSI}
		result := q.DeepCopy()
		if q.d.Cmp(result.AsDec()) != 0 {
			t.Errorf("Expected: %v, Actual: %v", q.String(), result.String())
		}
		result = Quantity{d: infDecAmount{dec(2, 0).Dec}, Format: DecimalSI}
		if q.d.Cmp(result.AsDec()) == 0 {
			t.Errorf("Modifying result has affected q")
		}
	}
}

// TestJSON 用随机生成的 Quantity 做 500 次 JSON 编解码往返，数值必须保持不变。
func TestJSON(t *testing.T) {
	for i := 0; i < 500; i++ {
		q := &Quantity{}
		fuzzer.Fill(q)
		b, err := json.Marshal(q)
		if err != nil {
			t.Errorf("error encoding %v: %v", q, err)
			continue
		}
		q2 := &Quantity{}
		err = json.Unmarshal(b, q2)
		if err != nil {
			t.Logf("%d: %s", i, string(b))
			t.Errorf("%v: error decoding %v: %v", q, string(b), err)
		}
		if q2.Cmp(*q) != 0 {
			t.Errorf("Expected equal: %v, %v (json was '%v')", q, q2, string(b))
		}
	}
}

// TestJSONWhitespace 验证 JSON 解码允许数字前后带空白（包括字符串形式），
// 这与 ParseQuantity 本身拒绝空白的规则不同，属于 JSON 层的容忍。
func TestJSONWhitespace(t *testing.T) {
	q := Quantity{}
	testCases := []struct {
		in     string
		expect string
	}{
		{`" 1"`, "1"},
		{`"1 "`, "1"},
		{`1`, "1"},
		{` 1`, "1"},
		{`1 `, "1"},
		{`10`, "10"},
		{`-1`, "-1"},
		{` -1`, "-1"},
	}
	for _, test := range testCases {
		if err := json.Unmarshal([]byte(test.in), &q); err != nil {
			t.Errorf("%q: %v", test.in, err)
		}
		if q.String() != test.expect {
			t.Errorf("unexpected string: %q", q.String())
		}
	}
}

// TestMilliNewSet 验证 NewMilliQuantity 与 SetMilli 的语义一致；BinarySI 下不精确的值
// 允许格式改变，因此只在 exact 为 true 时校验往返。
func TestMilliNewSet(t *testing.T) {
	table := []struct {
		value  int64
		format Format
		expect string
		exact  bool
	}{
		{1, DecimalSI, "1m", true},
		{1000, DecimalSI, "1", true},
		{1234000, DecimalSI, "1234", true},
		{1024, BinarySI, "1024m", false}, // 格式变化。
		{1000000, "invalidFormatDefaultsToExponent", "1e3", true},
		{1024 * 1024, BinarySI, "1048576m", false}, // 格式变化。
	}

	for _, item := range table {
		q := NewMilliQuantity(item.value, item.format)
		if e, a := item.expect, q.String(); e != a {
			t.Errorf("Expected %v, got %v; %#v", e, a, q)
		}
		if !item.exact {
			continue
		}
		q2, err := ParseQuantity(q.String())
		if err != nil {
			t.Errorf("Round trip failed on %v", q)
		}
		if e, a := item.value, q2.MilliValue(); e != a {
			t.Errorf("Expected %v, got %v", e, a)
		}
	}

	for _, item := range table {
		q := NewQuantity(0, item.format)
		q.SetMilli(item.value)
		if e, a := item.expect, q.String(); e != a {
			t.Errorf("Set: Expected %v, got %v; %#v", e, a, q)
		}
	}
}

// TestNewSet 验证 NewQuantity 与 Set 产生相同的规范字符串，并对 int64 和 inf.Dec 两条路径分别校验。
func TestNewSet(t *testing.T) {
	table := []struct {
		value  int64
		format Format
		expect string
	}{
		{1, DecimalSI, "1"},
		{1000, DecimalSI, "1k"},
		{1234000, DecimalSI, "1234k"},
		{1024, BinarySI, "1Ki"},
		{1000000, "invalidFormatDefaultsToExponent", "1e6"},
		{1024 * 1024, BinarySI, "1Mi"},
	}

	for _, asDec := range []bool{false, true} {
		for _, item := range table {
			q := NewQuantity(item.value, item.format)
			if asDec {
				q.ToDec()
			}
			if e, a := item.expect, q.String(); e != a {
				t.Errorf("Expected %v, got %v; %#v", e, a, q)
			}
			q2, err := ParseQuantity(q.String())
			if err != nil {
				t.Errorf("Round trip failed on %v", q)
			}
			if e, a := item.value, q2.Value(); e != a {
				t.Errorf("Expected %v, got %v", e, a)
			}
		}

		for _, item := range table {
			q := NewQuantity(0, item.format)
			q.Set(item.value)
			if asDec {
				q.ToDec()
			}
			if e, a := item.expect, q.String(); e != a {
				t.Errorf("Set: Expected %v, got %v; %#v", e, a, q)
			}
		}
	}
}

// TestNewScaledSet 验证 NewScaledQuantity 与 SetScaled 在相同 scale 下等价，
// 并覆盖 Nano/Micro/Milli/0 四档。
func TestNewScaledSet(t *testing.T) {
	table := []struct {
		value  int64
		scale  Scale
		expect string
	}{
		{1, Nano, "1n"},
		{1000, Nano, "1u"},
		{1, Micro, "1u"},
		{1000, Micro, "1m"},
		{1, Milli, "1m"},
		{1000, Milli, "1"},
		{1, 0, "1"},
		{0, Nano, "0"},
		{0, Micro, "0"},
		{0, Milli, "0"},
		{0, 0, "0"},
	}

	for _, item := range table {
		q := NewScaledQuantity(item.value, item.scale)
		if e, a := item.expect, q.String(); e != a {
			t.Errorf("Expected %v, got %v; %#v", e, a, q)
		}
		q2, err := ParseQuantity(q.String())
		if err != nil {
			t.Errorf("Round trip failed on %v", q)
		}
		if e, a := item.value, q2.ScaledValue(item.scale); e != a {
			t.Errorf("Expected %v, got %v", e, a)
		}
		q3 := NewQuantity(0, DecimalSI)
		q3.SetScaled(item.value, item.scale)
		if q.Cmp(*q3) != 0 {
			t.Errorf("Expected %v and %v to be equal", q, q3)
		}
	}
}

// TestScaledValue 验证 ScaledValue 的换算：把 1 从 fromScale 换算到 toScale 应得的整数倍。
func TestScaledValue(t *testing.T) {
	table := []struct {
		fromScale Scale
		toScale   Scale
		expected  int64
	}{
		{Nano, Nano, 1},
		{Nano, Micro, 1},
		{Nano, Milli, 1},
		{Nano, 0, 1},
		{Micro, Nano, 1000},
		{Micro, Micro, 1},
		{Micro, Milli, 1},
		{Micro, 0, 1},
		{Milli, Nano, 1000 * 1000},
		{Milli, Micro, 1000},
		{Milli, Milli, 1},
		{Milli, 0, 1},
		{0, Nano, 1000 * 1000 * 1000},
		{0, Micro, 1000 * 1000},
		{0, Milli, 1000},
		{0, 0, 1},
		{2, -2, 100 * 100},
	}

	for _, item := range table {
		q := NewScaledQuantity(1, item.fromScale)
		if e, a := item.expected, q.ScaledValue(item.toScale); e != a {
			t.Errorf("%v to %v: Expected %v, got %v", item.fromScale, item.toScale, e, a)
		}
	}
}

// TestUninitializedNoCrash 确保零值 Quantity 调用各访问器不会 panic——调用方经常依赖零值可用。
func TestUninitializedNoCrash(t *testing.T) {
	var q Quantity

	q.Value()
	q.MilliValue()
	q.DeepCopy()
	_ = q.String()
	q.MarshalJSON()
}

// TestDeepCopy 验证 DeepCopy 返回独立副本，修改副本不影响原值。
func TestDeepCopy(t *testing.T) {
	q := NewQuantity(5, DecimalSI)
	c := q.DeepCopy()
	c.Set(6)
	if q.Value() == 6 {
		t.Errorf("Copy didn't")
	}
}

// TestSub 验证减法：结果格式跟随被减数（接收者），零值接收者视为 0。
func TestSub(t *testing.T) {
	tests := []struct {
		a        Quantity
		b        Quantity
		expected Quantity
	}{
		{decQuantity(10, 0, DecimalSI), decQuantity(1, 1, DecimalSI), decQuantity(0, 0, DecimalSI)},
		{decQuantity(10, 0, DecimalSI), decQuantity(1, 0, BinarySI), decQuantity(9, 0, DecimalSI)},
		{decQuantity(10, 0, BinarySI), decQuantity(1, 0, DecimalSI), decQuantity(9, 0, BinarySI)},
		{Quantity{Format: DecimalSI}, decQuantity(50, 0, DecimalSI), decQuantity(-50, 0, DecimalSI)},
		{decQuantity(50, 0, DecimalSI), Quantity{Format: DecimalSI}, decQuantity(50, 0, DecimalSI)},
		{Quantity{Format: DecimalSI}, Quantity{Format: DecimalSI}, decQuantity(0, 0, DecimalSI)},
	}

	for i, test := range tests {
		test.a.Sub(test.b)
		if test.a.Cmp(test.expected) != 0 {
			t.Errorf("[%d] Expected %q, got %q", i, test.expected.String(), test.a.String())
		}
	}
}

// TestNeg 验证取负后数值相等但符号相反；inf.Dec 存储的零值取负后会退化为 int64Amount 零值。
func TestNeg(t *testing.T) {
	tests := []struct {
		a        Quantity
		b        Quantity
		expected Quantity
	}{
		{a: intQuantity(0, 0, DecimalSI), expected: intQuantity(0, 0, DecimalSI)},
		{a: Quantity{}, expected: Quantity{}},
		{a: intQuantity(10, 0, BinarySI), expected: intQuantity(-10, 0, BinarySI)},
		{a: intQuantity(-10, 0, BinarySI), expected: intQuantity(10, 0, BinarySI)},
		{a: decQuantity(0, 0, DecimalSI), expected: intQuantity(0, 0, DecimalSI)},
		{a: decQuantity(10, 0, BinarySI), expected: intQuantity(-10, 0, BinarySI)},
		{a: decQuantity(-10, 0, BinarySI), expected: intQuantity(10, 0, BinarySI)},
	}

	for i, test := range tests {
		a := test.a.DeepCopy()
		a.Neg()
		// 确保值相同。
		if a.Cmp(test.expected) != 0 {
			t.Errorf("[%d] Expected %q, got %q", i, test.expected.String(), a.String())
		}
	}
}

// TestAdd 验证加法：结果格式跟随被加数（接收者），零值接收者视为 0。
func TestAdd(t *testing.T) {
	tests := []struct {
		a        Quantity
		b        Quantity
		expected Quantity
	}{
		{decQuantity(10, 0, DecimalSI), decQuantity(1, 1, DecimalSI), decQuantity(20, 0, DecimalSI)},
		{decQuantity(10, 0, DecimalSI), decQuantity(1, 0, BinarySI), decQuantity(11, 0, DecimalSI)},
		{decQuantity(10, 0, BinarySI), decQuantity(1, 0, DecimalSI), decQuantity(11, 0, BinarySI)},
		{Quantity{Format: DecimalSI}, decQuantity(50, 0, DecimalSI), decQuantity(50, 0, DecimalSI)},
		{decQuantity(50, 0, DecimalSI), Quantity{Format: DecimalSI}, decQuantity(50, 0, DecimalSI)},
		{Quantity{Format: DecimalSI}, Quantity{Format: DecimalSI}, decQuantity(0, 0, DecimalSI)},
	}

	for i, test := range tests {
		test.a.Add(test.b)
		if test.a.Cmp(test.expected) != 0 {
			t.Errorf("[%d] Expected %q, got %q", i, test.expected.String(), test.a.String())
		}
	}
}

// TestMul 验证乘法返回值是否精确（ok），并覆盖 int64 溢出后降级为 inf.Dec 的情形。
func TestMul(t *testing.T) {
	tests := []struct {
		a        Quantity
		b        int64
		expected Quantity
		ok       bool
	}{
		{decQuantity(10, 0, DecimalSI), 10, decQuantity(100, 0, DecimalSI), true},
		{decQuantity(10, 0, DecimalSI), 1, decQuantity(10, 0, DecimalSI), true},
		{decQuantity(10, 0, BinarySI), 1, decQuantity(10, 0, BinarySI), true},
		{Quantity{Format: DecimalSI}, 50, decQuantity(0, 0, DecimalSI), true},
		{decQuantity(50, 0, DecimalSI), 0, decQuantity(0, 0, DecimalSI), true},
		{Quantity{Format: DecimalSI}, 0, decQuantity(0, 0, DecimalSI), true},

		{decQuantity(10, 0, DecimalSI), -10, decQuantity(-100, 0, DecimalSI), true},
		{decQuantity(-10, 0, DecimalSI), 1, decQuantity(-10, 0, DecimalSI), true},
		{decQuantity(10, 0, BinarySI), -1, decQuantity(-10, 0, BinarySI), true},
		{decQuantity(-50, 0, DecimalSI), 0, decQuantity(0, 0, DecimalSI), true},
		{decQuantity(-50, 0, DecimalSI), -50, decQuantity(2500, 0, DecimalSI), true},
		{Quantity{Format: DecimalSI}, -50, decQuantity(0, 0, DecimalSI), true},
		{decQuantity(mostPositive, 0, DecimalSI), 0, decQuantity(0, 1, DecimalSI), true},
		{decQuantity(mostPositive, 0, DecimalSI), 1, decQuantity(mostPositive, 0, DecimalSI), true},
		{decQuantity(mostPositive, 0, DecimalSI), -1, decQuantity(-mostPositive, 0, DecimalSI), true},
		{decQuantity(mostPositive/2, 0, DecimalSI), 2, decQuantity((mostPositive/2)*2, 0, DecimalSI), true},
		{decQuantity(mostPositive/-2, 0, DecimalSI), -2, decQuantity((mostPositive/2)*2, 0, DecimalSI), true},
		{decQuantity(mostPositive, 0, DecimalSI), 2,
			bigDecQuantity(big.NewInt(0).Mul(bigMostPositive, big.NewInt(2)), 0, DecimalSI), false},
		{decQuantity(mostPositive, 0, DecimalSI), 10, decQuantity(mostPositive, 1, DecimalSI), false},
		{decQuantity(mostPositive, 0, DecimalSI), -10, decQuantity(-mostPositive, 1, DecimalSI), false},
		{decQuantity(mostNegative, 0, DecimalSI), 0, decQuantity(0, 1, DecimalSI), true},
		{decQuantity(mostNegative, 0, DecimalSI), 1, decQuantity(mostNegative, 0, DecimalSI), true},
		{decQuantity(mostNegative, 0, DecimalSI), -1,
			bigDecQuantity(big.NewInt(0).Add(bigMostPositive, big.NewInt(1)), 0, DecimalSI), false},
		{decQuantity(mostNegative/2, 0, DecimalSI), 2, decQuantity(mostNegative, 0, DecimalSI), true},
		{decQuantity(mostNegative/-2, 0, DecimalSI), -2, decQuantity(mostNegative, 0, DecimalSI), true},
		{decQuantity(mostNegative, 0, DecimalSI), 2,
			bigDecQuantity(big.NewInt(0).Mul(bigMostNegative, big.NewInt(2)), 0, DecimalSI), false},
		{decQuantity(mostNegative, 0, DecimalSI), 10, decQuantity(mostNegative, 1, DecimalSI), false},
		{decQuantity(mostNegative, 0, DecimalSI), -10,
			bigDecQuantity(big.NewInt(0).Add(bigMostPositive, big.NewInt(1)), 1, DecimalSI), false},
	}

	for i, test := range tests {
		if ok := test.a.Mul(test.b); test.ok != ok {
			t.Errorf("[%d] Expected ok: %t, got ok: %t", i, test.ok, ok)
		}
		if test.a.Cmp(test.expected) != 0 {
			t.Errorf("[%d] Expected %q, got %q", i, test.expected.AsDec().String(), test.a.AsDec().String())
		}
	}
}

// TestAddSubRoundTrip 验证同一 scale 下连续加、再按原顺序减，结果精确归零（无精度漂移）。
func TestAddSubRoundTrip(t *testing.T) {
	for k := -10; k <= 10; k++ {
		q := Quantity{Format: DecimalSI}
		var order []int64
		for i := 0; i < 100; i++ {
			j := rand.Int63()
			order = append(order, j)
			q.Add(*NewScaledQuantity(j, Scale(k)))
		}
		for _, j := range order {
			q.Sub(*NewScaledQuantity(j, Scale(k)))
		}
		if !q.IsZero() {
			t.Errorf("addition and subtraction did not cancel: %s", &q)
		}
	}
}

// TestAddSubRoundTripAcrossScales 与 TestAddSubRoundTrip 类似，但每次操作使用不同的随机 scale，
// 验证跨 scale 加减仍能精确抵消。
func TestAddSubRoundTripAcrossScales(t *testing.T) {
	q := Quantity{Format: DecimalSI}
	var order []int64
	for i := 0; i < 100; i++ {
		j := rand.Int63()
		order = append(order, j)
		q.Add(*NewScaledQuantity(j, Scale(j%20-10)))
	}
	for _, j := range order {
		q.Sub(*NewScaledQuantity(j, Scale(j%20-10)))
	}
	if !q.IsZero() {
		t.Errorf("addition and subtraction did not cancel: %s", &q)
	}
}

// TestNegateRoundTrip 验证两次取负后与原值相等，int64 与 inf.Dec 两种表示都覆盖。
func TestNegateRoundTrip(t *testing.T) {
	for _, asDec := range []bool{false, true} {
		for k := -10; k <= 10; k++ {
			for i := 0; i < 100; i++ {
				j := rand.Int63()
				q := *NewScaledQuantity(j, Scale(k))
				if asDec {
					q.AsDec()
				}

				b := q.DeepCopy()
				b.Neg()
				b.Neg()
				if b.Cmp(q) != 0 {
					t.Errorf("double negation did not cancel: %s", &q)
				}
			}
		}
	}
}

// TestQuantityAsApproximateFloat64 验证快速近似转换的正确性（含 float64 溢出为 Inf 的情形）。
func TestQuantityAsApproximateFloat64(t *testing.T) {
	table := []struct {
		in  Quantity
		out float64
	}{
		{decQuantity(0, 0, DecimalSI), 0.0},
		{decQuantity(0, 0, DecimalExponent), 0.0},
		{decQuantity(0, 0, BinarySI), 0.0},

		{decQuantity(1, 0, DecimalSI), 1},
		{decQuantity(1, 0, DecimalExponent), 1},
		{decQuantity(1, 0, BinarySI), 1},

		// 二进制后缀。
		{decQuantity(1024, 0, BinarySI), 1024},
		{decQuantity(8*1024, 0, BinarySI), 8 * 1024},
		{decQuantity(7*1024*1024, 0, BinarySI), 7 * 1024 * 1024},
		{decQuantity(7*1024*1024, 1, BinarySI), (7 * 1024 * 1024) * 10},
		{decQuantity(7*1024*1024, 4, BinarySI), (7 * 1024 * 1024) * 10000},
		{decQuantity(7*1024*1024, 8, BinarySI), (7 * 1024 * 1024) * 100000000},
		{decQuantity(7*1024*1024, -1, BinarySI), (7 * 1024 * 1024) * math.Pow10(-1)}, // '* Pow10' 与 '/ float(10)' 的舍入方式不同。
		{decQuantity(7*1024*1024, -8, BinarySI), (7 * 1024 * 1024) / float64(100000000)},

		{decQuantity(1024, 0, DecimalSI), 1024},
		{decQuantity(8*1024, 0, DecimalSI), 8 * 1024},
		{decQuantity(7*1024*1024, 0, DecimalSI), 7 * 1024 * 1024},
		{decQuantity(7*1024*1024, 1, DecimalSI), (7 * 1024 * 1024) * 10},
		{decQuantity(7*1024*1024, 4, DecimalSI), (7 * 1024 * 1024) * 10000},
		{decQuantity(7*1024*1024, 8, DecimalSI), (7 * 1024 * 1024) * 100000000},
		{decQuantity(7*1024*1024, -1, DecimalSI), (7 * 1024 * 1024) * math.Pow10(-1)}, // '* Pow10' 与 '/ float(10)' 的舍入方式不同。
		{decQuantity(7*1024*1024, -8, DecimalSI), (7 * 1024 * 1024) / float64(100000000)},

		{decQuantity(1024, 0, DecimalExponent), 1024},
		{decQuantity(8*1024, 0, DecimalExponent), 8 * 1024},
		{decQuantity(7*1024*1024, 0, DecimalExponent), 7 * 1024 * 1024},
		{decQuantity(7*1024*1024, 1, DecimalExponent), (7 * 1024 * 1024) * 10},
		{decQuantity(7*1024*1024, 4, DecimalExponent), (7 * 1024 * 1024) * 10000},
		{decQuantity(7*1024*1024, 8, DecimalExponent), (7 * 1024 * 1024) * 100000000},
		{decQuantity(7*1024*1024, -1, DecimalExponent), (7 * 1024 * 1024) * math.Pow10(-1)}, // '* Pow10' 与 '/ float(10)' 的舍入方式不同。
		{decQuantity(7*1024*1024, -8, DecimalExponent), (7 * 1024 * 1024) / float64(100000000)},

		// 超大数字。
		{Quantity{d: maxAllowed, Format: DecimalSI}, math.MaxInt64},
		{Quantity{d: maxAllowed, Format: BinarySI}, math.MaxInt64},
		{decQuantity(12, 18, DecimalSI), 1.2e19},

		// float64 溢出导致的无穷值。
		{decQuantity(12, 500, DecimalSI), math.Inf(0)},
		{decQuantity(-12, 500, DecimalSI), math.Inf(-1)},
	}

	for i, item := range table {
		t.Run(fmt.Sprintf("%s %s", item.in.Format, item.in.String()), func(t *testing.T) {
			out := item.in.AsApproximateFloat64()
			if out != item.out {
				t.Fatalf("test %d expected %v, got %v", i+1, item.out, out)
			}
			if item.in.d.Dec != nil {
				if i, ok := item.in.AsInt64(); ok {
					q := intQuantity(i, 0, item.in.Format)
					out := q.AsApproximateFloat64()
					if out != item.out {
						t.Fatalf("as int quantity: expected %v, got %v", item.out, out)
					}
				}
			}
		})
	}
}

// TestQuantityAsFloat64Slow 验证慢速精确转换路径，与 TestQuantityAsApproximateFloat64 用同一张表对照。
func TestQuantityAsFloat64Slow(t *testing.T) {
	table := []struct {
		in  Quantity
		out float64
	}{
		{decQuantity(0, 0, DecimalSI), 0.0},
		{decQuantity(0, 0, DecimalExponent), 0.0},
		{decQuantity(0, 0, BinarySI), 0.0},

		{decQuantity(1, 0, DecimalSI), 1},
		{decQuantity(1, 0, DecimalExponent), 1},
		{decQuantity(1, 0, BinarySI), 1},

		// 二进制后缀。
		{decQuantity(1024, 0, BinarySI), 1024},
		{decQuantity(8*1024, 0, BinarySI), 8 * 1024},
		{decQuantity(7*1024*1024, 0, BinarySI), 7 * 1024 * 1024},
		{decQuantity(7*1024*1024, 1, BinarySI), (7 * 1024 * 1024) * 10},
		{decQuantity(7*1024*1024, 4, BinarySI), (7 * 1024 * 1024) * 10000},
		{decQuantity(7*1024*1024, 8, BinarySI), (7 * 1024 * 1024) * 100000000},
		{decQuantity(7*1024*1024, -1, BinarySI), (7 * 1024 * 1024) / float64(10)},
		{decQuantity(7*1024*1024, -8, BinarySI), (7 * 1024 * 1024) / float64(100000000)},

		{decQuantity(1024, 0, DecimalSI), 1024},
		{decQuantity(8*1024, 0, DecimalSI), 8 * 1024},
		{decQuantity(7*1024*1024, 0, DecimalSI), 7 * 1024 * 1024},
		{decQuantity(7*1024*1024, 1, DecimalSI), (7 * 1024 * 1024) * 10},
		{decQuantity(7*1024*1024, 4, DecimalSI), (7 * 1024 * 1024) * 10000},
		{decQuantity(7*1024*1024, 8, DecimalSI), (7 * 1024 * 1024) * 100000000},
		{decQuantity(7*1024*1024, -1, DecimalSI), (7 * 1024 * 1024) / float64(10)},
		{decQuantity(7*1024*1024, -8, DecimalSI), (7 * 1024 * 1024) / float64(100000000)},

		{decQuantity(1024, 0, DecimalExponent), 1024},
		{decQuantity(8*1024, 0, DecimalExponent), 8 * 1024},
		{decQuantity(7*1024*1024, 0, DecimalExponent), 7 * 1024 * 1024},
		{decQuantity(7*1024*1024, 1, DecimalExponent), (7 * 1024 * 1024) * 10},
		{decQuantity(7*1024*1024, 4, DecimalExponent), (7 * 1024 * 1024) * 10000},
		{decQuantity(7*1024*1024, 8, DecimalExponent), (7 * 1024 * 1024) * 100000000},
		{decQuantity(7*1024*1024, -1, DecimalExponent), (7 * 1024 * 1024) / float64(10)},
		{decQuantity(7*1024*1024, -8, DecimalExponent), (7 * 1024 * 1024) / float64(100000000)},

		// 超大数字。
		{Quantity{d: maxAllowed, Format: DecimalSI}, math.MaxInt64},
		{Quantity{d: maxAllowed, Format: BinarySI}, math.MaxInt64},
		{decQuantity(12, 18, DecimalSI), 1.2e19},

		// float64 溢出导致的无穷值。
		{decQuantity(12, 500, DecimalSI), math.Inf(0)},
		{decQuantity(-12, 500, DecimalSI), math.Inf(-1)},
	}

	for i, item := range table {
		t.Run(fmt.Sprintf("%s %s", item.in.Format, item.in.String()), func(t *testing.T) {
			out := item.in.AsFloat64Slow()
			if out != item.out {
				t.Fatalf("test %d expected %v, got %v", i+1, item.out, out)
			}
			if item.in.d.Dec != nil {
				if i, ok := item.in.AsInt64(); ok {
					q := intQuantity(i, 0, item.in.Format)
					out := q.AsFloat64Slow()
					if out != item.out {
						t.Fatalf("as int quantity: expected %v, got %v", item.out, out)
					}
				}
			}
		})
	}
}

// TestStringQuantityAsApproximateFloat64 从字符串输入出发，验证带小数的二进制单位换算为 float64 的精度。
func TestStringQuantityAsApproximateFloat64(t *testing.T) {
	table := []struct {
		in  string
		out float64
	}{
		{"2Ki", 2048},
		{"1.1Ki", 1126.4e+0},
		{"1Mi", 1.048576e+06},
		{"2Gi", 2.147483648e+09},
	}

	for _, item := range table {
		t.Run(item.in, func(t *testing.T) {
			in, err := ParseQuantity(item.in)
			if err != nil {
				t.Fatal(err)
			}
			out := in.AsApproximateFloat64()
			if out != item.out {
				t.Fatalf("expected %v, got %v", item.out, out)
			}
			if in.d.Dec != nil {
				if i, ok := in.AsInt64(); ok {
					q := intQuantity(i, 0, in.Format)
					out := q.AsApproximateFloat64()
					if out != item.out {
						t.Fatalf("as int quantity: expected %v, got %v", item.out, out)
					}
				}
			}
		})
	}
}

// TestStringQuantityAsFloat64Slow 从字符串输入出发，验证慢速精确转换路径。
func TestStringQuantityAsFloat64Slow(t *testing.T) {
	table := []struct {
		in  string
		out float64
	}{
		{"2Ki", 2048},
		{"1.1Ki", 1126.4e+0},
		{"1Mi", 1.048576e+06},
		{"2Gi", 2.147483648e+09},
	}

	for _, item := range table {
		t.Run(item.in, func(t *testing.T) {
			in, err := ParseQuantity(item.in)
			if err != nil {
				t.Fatal(err)
			}
			out := in.AsFloat64Slow()
			if out != item.out {
				t.Fatalf("expected %v, got %v", item.out, out)
			}
			if in.d.Dec != nil {
				if i, ok := in.AsInt64(); ok {
					q := intQuantity(i, 0, in.Format)
					out := q.AsFloat64Slow()
					if out != item.out {
						t.Fatalf("as int quantity: expected %v, got %v", item.out, out)
					}
				}
			}
		})
	}
}

// benchmarkQuantities 返回一组覆盖三种 Format 与不同 scale 的代表性 Quantity，供各基准复用。
func benchmarkQuantities() []Quantity {
	return []Quantity{
		intQuantity(1024*1024*1024, 0, BinarySI),
		intQuantity(1024*1024*1024*1024, 0, BinarySI),
		intQuantity(1000000, 3, DecimalSI),
		intQuantity(1000000000, 0, DecimalSI),
		intQuantity(1, -3, DecimalSI),
		intQuantity(80, -3, DecimalSI),
		intQuantity(1080, -3, DecimalSI),
		intQuantity(0, 0, BinarySI),
		intQuantity(1, 9, DecimalExponent),
		intQuantity(1, -9, DecimalSI),
		intQuantity(1000000, 10, DecimalSI),
	}
}

// BenchmarkQuantityString 测量清空缓存后的规范字符串生成开销。
func BenchmarkQuantityString(b *testing.B) {
	values := benchmarkQuantities()
	b.ResetTimer()
	var s string
	for i := 0; i < b.N; i++ {
		q := values[i%len(values)]
		q.s = ""
		s = q.String()
	}
	b.StopTimer()
	if len(s) == 0 {
		b.Fatal(s)
	}
}

// BenchmarkQuantityStringPrecalc 测量已缓存规范字符串时的读取开销，用于对比缓存收益。
func BenchmarkQuantityStringPrecalc(b *testing.B) {
	values := benchmarkQuantities()
	for i := range values {
		_ = values[i].String()
	}
	b.ResetTimer()
	var s string
	for i := 0; i < b.N; i++ {
		q := values[i%len(values)]
		s = q.String()
	}
	b.StopTimer()
	if len(s) == 0 {
		b.Fatal(s)
	}
}

// BenchmarkQuantityStringBinarySI 测量 BinarySI 格式下的字符串生成，该格式需按 1024 的幂换算。
func BenchmarkQuantityStringBinarySI(b *testing.B) {
	values := benchmarkQuantities()
	for i := range values {
		values[i].Format = BinarySI
	}
	b.ResetTimer()
	var s string
	for i := 0; i < b.N; i++ {
		q := values[i%len(values)]
		q.s = ""
		s = q.String()
	}
	b.StopTimer()
	if len(s) == 0 {
		b.Fatal(s)
	}
}

// BenchmarkQuantityMarshalJSON 测量 JSON 序列化（依赖规范字符串生成）的开销。
func BenchmarkQuantityMarshalJSON(b *testing.B) {
	values := benchmarkQuantities()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := values[i%len(values)]
		q.s = ""
		if _, err := q.MarshalJSON(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkQuantityUnmarshalJSON 测量 JSON 反序列化（即解析字符串）的开销。
func BenchmarkQuantityUnmarshalJSON(b *testing.B) {
	values := benchmarkQuantities()
	var json [][]byte
	for _, v := range values {
		data, _ := v.MarshalJSON()
		json = append(json, data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var q Quantity
		if err := q.UnmarshalJSON(json[i%len(values)]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkParseQuantity 测量字符串解析的开销。
func BenchmarkParseQuantity(b *testing.B) {
	values := benchmarkQuantities()
	var strings []string
	for _, v := range values {
		strings = append(strings, v.String())
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ParseQuantity(strings[i%len(values)]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkCanonicalize 测量复用缓冲区生成规范字节的开销。
func BenchmarkCanonicalize(b *testing.B) {
	values := benchmarkQuantities()
	b.ResetTimer()
	buffer := make([]byte, 0, 100)
	for i := 0; i < b.N; i++ {
		s, _ := values[i%len(values)].CanonicalizeBytes(buffer)
		if len(s) == 0 {
			b.Fatal(s)
		}
	}
	b.StopTimer()
}

// BenchmarkQuantityRoundUp 测量按 scale -3（毫）向上舍入的开销。
func BenchmarkQuantityRoundUp(b *testing.B) {
	values := benchmarkQuantities()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := values[i%len(values)]
		copied := q
		copied.RoundUp(-3)
	}
	b.StopTimer()
}

// BenchmarkQuantityCopy 测量 DeepCopy 的开销。
func BenchmarkQuantityCopy(b *testing.B) {
	values := benchmarkQuantities()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values[i%len(values)].DeepCopy()
	}
	b.StopTimer()
}

// BenchmarkQuantityAdd 测量把各基准量累加到同一个基数上的开销。
func BenchmarkQuantityAdd(b *testing.B) {
	values := benchmarkQuantities()
	base := &Quantity{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := values[i%len(values)]
		base.d.Dec = nil
		base.i = int64Amount{value: 100}
		base.Add(q)
	}
	b.StopTimer()
}

// BenchmarkQuantityCmp 测量自比较（相等）路径的开销。
func BenchmarkQuantityCmp(b *testing.B) {
	values := benchmarkQuantities()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := values[i%len(values)]
		if q.Cmp(q) != 0 {
			b.Fatal(q)
		}
	}
	b.StopTimer()
}

// BenchmarkQuantityAsApproximateFloat64 测量快速近似转换的开销。
func BenchmarkQuantityAsApproximateFloat64(b *testing.B) {
	values := benchmarkQuantities()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := values[i%len(values)]
		if q.AsApproximateFloat64() == -1 {
			b.Fatal(q)
		}
	}
	b.StopTimer()
}

// BenchmarkQuantityAsFloat64Slow 测量慢速精确转换的开销。
func BenchmarkQuantityAsFloat64Slow(b *testing.B) {
	values := benchmarkQuantities()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := values[i%len(values)]
		if q.AsFloat64Slow() == -1 {
			b.Fatal(q)
		}
	}
	b.StopTimer()
}

// 编译期断言：QuantityValue 必须实现 pflag.Value，供命令行参数使用。
var _ pflag.Value = &QuantityValue{}

// TestQuantityValueSet 验证 QuantityValue.Set 拒绝非法输入、接受合法值，并检查其 JSON 编码。
func TestQuantityValueSet(t *testing.T) {
	q := QuantityValue{}

	if err := q.Set("invalid"); err == nil {

		t.Error("'invalid' did not trigger a parse error")
	}

	if err := q.Set("1Mi"); err != nil {
		t.Errorf("parsing 1Mi should have worked, got: %v", err)
	}
	if q.Value() != 1024*1024 {
		t.Errorf("quantity should have been set to 1Mi, got: %v", q)
	}

	data, err := json.Marshal(q)
	if err != nil {
		t.Errorf("unexpected encoding error: %v", err)
	}
	expected := `"1Mi"`
	if string(data) != expected {
		t.Errorf("expected 1Mi value to be encoded as %q, got: %q", expected, string(data))
	}
}

// ExampleQuantityValue 演示把 QuantityValue 注册为 pflag 参数后，帮助信息中会显示默认值。
func ExampleQuantityValue() {
	q := QuantityValue{
		Quantity: MustParse("1Mi"),
	}
	fs := pflag.FlagSet{}
	fs.SetOutput(os.Stdout)
	fs.Var(&q, "mem", "sets amount of memory")
	fs.PrintDefaults()
	// Output:
	// --mem quantity   sets amount of memory (default 1Mi)
}

// TestQuantityUnmarshalCBOR 验证 CBOR 解码接受 null、文本串与字节串输入，
// 且空白被容忍、非法输入返回精确的错误信息。
func TestQuantityUnmarshalCBOR(t *testing.T) {
	for _, tc := range []struct {
		name       string
		in         []byte
		want       Quantity
		errMessage string
	}{
		{
			name: "null",
			in:   []byte{0xf6}, // null
			want: Quantity{},
		},
		{
			name: "text string input",
			in:   []byte("\x621M"), // "1M"
			want: Quantity{i: int64Amount{value: 1, scale: 6}},
		},
		{
			name: "byte string input",
			in:   []byte("\x421M"), // '1M'
			want: Quantity{i: int64Amount{value: 1, scale: 6}},
		},
		{
			name: "whitespace",
			in:   []byte("\x4a \t\n\r1M \t\n\r"), // h'20090a0d314d20090a0d'
			want: Quantity{i: int64Amount{value: 1, scale: 6}},
		},
		{
			name:       "empty byte string",
			in:         []byte{0x40},
			errMessage: ErrFormatWrong.Error(),
		},
		{
			name:       "empty text string",
			in:         []byte{0x60},
			errMessage: ErrFormatWrong.Error(),
		},
		{
			name:       "unsupported input type",
			in:         []byte{0x07}, // 7
			errMessage: "cbor: cannot unmarshal positive integer into Go value of type string",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got Quantity
			if err := got.UnmarshalCBOR(tc.in); err != nil {
				if tc.errMessage == "" {
					t.Fatalf("want nil error, got: %v", err)
				} else if gotMessage := err.Error(); tc.errMessage != gotMessage {
					t.Fatalf("want error: %q, got: %q", tc.errMessage, gotMessage)
				}
			} else if tc.errMessage != "" {
				t.Fatalf("got nil error, want: %s", tc.errMessage)
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("unexpected diff:\n%s", diff)
			}
		})
	}
}

// TestQuantityRoundtripCBOR 用随机生成的 Quantity 做 500 次 CBOR 编解码往返，数值必须保持不变。
func TestQuantityRoundtripCBOR(t *testing.T) {
	for i := 0; i < 500; i++ {
		var initial, final Quantity
		fuzzer.Fill(&initial)
		b, err := cbor.Marshal(initial)
		if err != nil {
			t.Errorf("error encoding %v: %v", initial, err)
			continue
		}
		err = cbor.Unmarshal(b, &final)
		if err != nil {
			t.Errorf("%v: error decoding %v: %v", initial, string(b), err)
		}
		if final.Cmp(initial) != 0 {
			diag, err := cbor.Diagnose(b)
			if err != nil {
				t.Logf("failed to produce diagnostic encoding of 0x%x: %v", b, err)
			}
			t.Errorf("Expected equal: %v, %v (cbor was '%s')", initial, final, diag)
		}
	}
}

// TestParseQuantity 针对 int64 边界附近的数值，逐项校验 AsInt64/AsDec 的返回以及
// 重新序列化得到的规范形式（canonical 为空表示应原样回显）。
func TestParseQuantity(t *testing.T) {
	ptrDec := func(s string) *infDecAmount {
		d, ok := new(inf.Dec).SetString(s)
		if !ok {
			t.Fatalf("invalid dec: %s", s)
		}
		return &infDecAmount{d}
	}

	tests := []struct {
		input       string
		wantAsInt64 *int64
		wantAsDec   *infDecAmount
		canonical   string
	}{
		// 最小/最大 18 位整数。
		{input: "-999999999999999999", wantAsInt64: ptr.To[int64](-999999999999999999), wantAsDec: ptrDec("-999999999999999999")},
		{input: "999999999999999999", wantAsInt64: ptr.To[int64](999999999999999999), wantAsDec: ptrDec("999999999999999999")},
		// .0
		{input: "-999999999999999999.0", wantAsInt64: nil, wantAsDec: ptrDec("-999999999999999999"), canonical: "-999999999999999999"},
		{input: "999999999999999999.0", wantAsInt64: nil, wantAsDec: ptrDec("999999999999999999"), canonical: "999999999999999999"},
		// .1
		{input: "-999999999999999999.1", wantAsInt64: nil, wantAsDec: ptrDec("-999999999999999999.1"), canonical: "-999999999999999999100m"},
		{input: "999999999999999999.1", wantAsInt64: nil, wantAsDec: ptrDec("999999999999999999.1"), canonical: "999999999999999999100m"},

		// 最小/最大 19 位整数。
		{input: "-9999999999999999999", wantAsInt64: nil, wantAsDec: ptrDec("-9999999999999999999")},
		{input: "9999999999999999999", wantAsInt64: nil, wantAsDec: ptrDec("9999999999999999999")},
		{input: "-1E", wantAsInt64: ptr.To[int64](-1000000000000000000), wantAsDec: ptrDec("-1000000000000000000")},
		{input: "1E", wantAsInt64: ptr.To[int64](1000000000000000000), wantAsDec: ptrDec("1000000000000000000")},
		{input: "-1000000000000000000", wantAsInt64: nil, wantAsDec: ptrDec("-1000000000000000000"), canonical: "-1E"}, // 应等于 wantAsInt64：<value>。
		{input: "1000000000000000000", wantAsInt64: nil, wantAsDec: ptrDec("1000000000000000000"), canonical: "1E"},    // 应等于 wantAsInt64：<value>。
		// .0
		{input: "-9999999999999999999.0", wantAsInt64: nil, wantAsDec: ptrDec("-9999999999999999999"), canonical: "-9999999999999999999"},
		{input: "9999999999999999999.0", wantAsInt64: nil, wantAsDec: ptrDec("9999999999999999999"), canonical: "9999999999999999999"},
		{input: "-1.0E", wantAsInt64: ptr.To[int64](-1000000000000000000), wantAsDec: ptrDec("-1000000000000000000"), canonical: "-1E"},
		{input: "1.0E", wantAsInt64: ptr.To[int64](1000000000000000000), wantAsDec: ptrDec("1000000000000000000"), canonical: "1E"},
		{input: "-1000000000000000000.0", wantAsInt64: nil, wantAsDec: ptrDec("-1000000000000000000"), canonical: "-1E"}, // 应等于 wantAsInt64：<value>。
		{input: "1000000000000000000.0", wantAsInt64: nil, wantAsDec: ptrDec("1000000000000000000"), canonical: "1E"},    // 应等于 wantAsInt64：<value>。
		// 000m（零值）。
		{input: "-9999999999999999999000m", wantAsInt64: nil, wantAsDec: ptrDec("-9999999999999999999"), canonical: "-9999999999999999999"},
		{input: "9999999999999999999000m", wantAsInt64: nil, wantAsDec: ptrDec("9999999999999999999"), canonical: "9999999999999999999"},
		// .1
		{input: "-9999999999999999999.1", wantAsInt64: nil, wantAsDec: ptrDec("-9999999999999999999.1"), canonical: "-9999999999999999999100m"},
		{input: "9999999999999999999.1", wantAsInt64: nil, wantAsDec: ptrDec("9999999999999999999.1"), canonical: "9999999999999999999100m"},
		{input: "-1.0000000000000000001E", wantAsInt64: nil, wantAsDec: ptrDec("-1000000000000000000.1"), canonical: "-1000000000000000000100m"},
		{input: "1.0000000000000000001E", wantAsInt64: nil, wantAsDec: ptrDec("1000000000000000000.1"), canonical: "1000000000000000000100m"},
		{input: "-1000000000000000000.1", wantAsInt64: nil, wantAsDec: ptrDec("-1000000000000000000.1"), canonical: "-1000000000000000000100m"},
		{input: "1000000000000000000.1", wantAsInt64: nil, wantAsDec: ptrDec("1000000000000000000.1"), canonical: "1000000000000000000100m"},
		// +1
		{input: "-1.000000000000000001E", wantAsInt64: nil, wantAsDec: ptrDec("-1000000000000000001"), canonical: "-1000000000000000001"}, // 应等于 wantAsInt64：<value>。
		{input: "1.000000000000000001E", wantAsInt64: nil, wantAsDec: ptrDec("1000000000000000001"), canonical: "1000000000000000001"},    // 应等于 wantAsInt64：<value>。
		{input: "-1000000000000000001", wantAsInt64: nil, wantAsDec: ptrDec("-1000000000000000001")},                                      // 应等于 wantAsInt64：<value>。
		{input: "1000000000000000001", wantAsInt64: nil, wantAsDec: ptrDec("1000000000000000001")},                                        // 应等于 wantAsInt64：<value>。

		// 最小/最大 20 位整数。
		{input: "-10E", wantAsInt64: nil, wantAsDec: ptrDec("-10000000000000000000")},
		{input: "10E", wantAsInt64: nil, wantAsDec: ptrDec("10000000000000000000")},
		{input: "-10000000000000000000", wantAsInt64: nil, wantAsDec: ptrDec("-10000000000000000000"), canonical: "-10E"},
		{input: "10000000000000000000", wantAsInt64: nil, wantAsDec: ptrDec("10000000000000000000"), canonical: "10E"},
		// .0
		{input: "-10.0E", wantAsInt64: nil, wantAsDec: ptrDec("-10000000000000000000"), canonical: "-10E"},
		{input: "10.0E", wantAsInt64: nil, wantAsDec: ptrDec("10000000000000000000"), canonical: "10E"},
		{input: "-10000000000000000000.0", wantAsInt64: nil, wantAsDec: ptrDec("-10000000000000000000"), canonical: "-10E"},
		{input: "10000000000000000000.0", wantAsInt64: nil, wantAsDec: ptrDec("10000000000000000000"), canonical: "10E"},
		// 000m（零值）。
		{input: "-10000000000000000000000m", wantAsInt64: nil, wantAsDec: ptrDec("-10000000000000000000"), canonical: "-10E"},
		{input: "10000000000000000000000m", wantAsInt64: nil, wantAsDec: ptrDec("10000000000000000000"), canonical: "10E"},
		// .1
		{input: "-10.0000000000000000001E", wantAsInt64: nil, wantAsDec: ptrDec("-10000000000000000000.1"), canonical: "-10000000000000000000100m"},
		{input: "10.0000000000000000001E", wantAsInt64: nil, wantAsDec: ptrDec("10000000000000000000.1"), canonical: "10000000000000000000100m"},
		{input: "-10000000000000000000.1", wantAsInt64: nil, wantAsDec: ptrDec("-10000000000000000000.1"), canonical: "-10000000000000000000100m"},
		{input: "10000000000000000000.1", wantAsInt64: nil, wantAsDec: ptrDec("10000000000000000000.1"), canonical: "10000000000000000000100m"},
		// +1
		{input: "-10.000000000000000001E", wantAsInt64: nil, wantAsDec: ptrDec("-10000000000000000001"), canonical: "-10000000000000000001"},
		{input: "10.000000000000000001E", wantAsInt64: nil, wantAsDec: ptrDec("10000000000000000001"), canonical: "10000000000000000001"},
		{input: "-10000000000000000001", wantAsInt64: nil, wantAsDec: ptrDec("-10000000000000000001")},
		{input: "10000000000000000001", wantAsInt64: nil, wantAsDec: ptrDec("10000000000000000001")},

		// 最小/最大 int64 减 1。
		{input: "-9223372036854775809", wantAsInt64: nil, wantAsDec: ptrDec("-9223372036854775809")},
		{input: "9223372036854775806", wantAsInt64: nil, wantAsDec: ptrDec("9223372036854775806")}, // 应等于 wantAsInt64：<value>。
		// .0
		{input: "-9223372036854775809.0", wantAsInt64: nil, wantAsDec: ptrDec("-9223372036854775809"), canonical: "-9223372036854775809"},
		{input: "9223372036854775806.0", wantAsInt64: nil, wantAsDec: ptrDec("9223372036854775806"), canonical: "9223372036854775806"}, // 应等于 wantAsInt64：<value>。
		// 000m（零值）。
		{input: "-9223372036854775809000m", wantAsInt64: nil, wantAsDec: ptrDec("-9223372036854775809"), canonical: "-9223372036854775809"},
		{input: "9223372036854775806000m", wantAsInt64: nil, wantAsDec: ptrDec("9223372036854775806"), canonical: "9223372036854775806"}, // 应等于 wantAsInt64：<value>。
		// .1
		{input: "-9223372036854775809.1", wantAsInt64: nil, wantAsDec: ptrDec("-9223372036854775809.1"), canonical: "-9223372036854775809100m"},
		{input: "9223372036854775806.1", wantAsInt64: nil, wantAsDec: ptrDec("9223372036854775806.1"), canonical: "9223372036854775806100m"},

		// 最小/最大 int64。
		{input: "-9223372036854775808", wantAsInt64: nil, wantAsDec: ptrDec("-9223372036854775808")}, // 应等于 wantAsInt64：<value>。
		{input: "9223372036854775807", wantAsInt64: nil, wantAsDec: ptrDec("9223372036854775807")},   // 应等于 wantAsInt64：<value>。
		// .0
		{input: "-9223372036854775808.0", wantAsInt64: nil, wantAsDec: ptrDec("-9223372036854775808"), canonical: "-9223372036854775808"}, // 应等于 wantAsInt64：<value>。
		{input: "9223372036854775807.0", wantAsInt64: nil, wantAsDec: ptrDec("9223372036854775807"), canonical: "9223372036854775807"},    // 应等于 wantAsInt64：<value>。
		// 000m（零值）。
		{input: "-9223372036854775808000m", wantAsInt64: nil, wantAsDec: ptrDec("-9223372036854775808"), canonical: "-9223372036854775808"}, // 应等于 wantAsInt64：<value>。
		{input: "9223372036854775807000m", wantAsInt64: nil, wantAsDec: ptrDec("9223372036854775807"), canonical: "9223372036854775807"},    // 应等于 wantAsInt64：<value>。
		// .1
		{input: "-9223372036854775808.1", wantAsInt64: nil, wantAsDec: ptrDec("-9223372036854775808.1"), canonical: "-9223372036854775808100m"},
		{input: "9223372036854775807.1", wantAsInt64: nil, wantAsDec: ptrDec("9223372036854775807.1"), canonical: "9223372036854775807100m"},

		// 最小/最大 int64 加 1。
		{input: "-9223372036854775807", wantAsInt64: nil, wantAsDec: ptrDec("-9223372036854775807")}, // 应等于 wantAsInt64：<value>。
		{input: "9223372036854775808", wantAsInt64: nil, wantAsDec: ptrDec("9223372036854775808")},
		// .0
		{input: "-9223372036854775807.0", wantAsInt64: nil, wantAsDec: ptrDec("-9223372036854775807"), canonical: "-9223372036854775807"}, // 应等于 wantAsInt64：<value>。
		{input: "9223372036854775808.0", wantAsInt64: nil, wantAsDec: ptrDec("9223372036854775808"), canonical: "9223372036854775808"},
		// 000m（零值）。
		{input: "-9223372036854775807000m", wantAsInt64: nil, wantAsDec: ptrDec("-9223372036854775807"), canonical: "-9223372036854775807"}, // 应等于 wantAsInt64：<value>。
		{input: "9223372036854775808000m", wantAsInt64: nil, wantAsDec: ptrDec("9223372036854775808"), canonical: "9223372036854775808"},
		// .1
		{input: "-9223372036854775807.1", wantAsInt64: nil, wantAsDec: ptrDec("-9223372036854775807.1"), canonical: "-9223372036854775807100m"},
		{input: "9223372036854775808.1", wantAsInt64: nil, wantAsDec: ptrDec("9223372036854775808.1"), canonical: "9223372036854775808100m"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			q, err := ParseQuantity(tt.input)
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tt.input, err)
			}

			val, ok := q.AsInt64()
			if tt.wantAsInt64 != nil {
				if !ok {
					t.Errorf("AsInt64() returned ok=false for input %q, want ok=true and value %d", tt.input, *tt.wantAsInt64)
				} else if val != *tt.wantAsInt64 {
					t.Errorf("AsInt64() returned value %d for input %q, want value %d", val, tt.input, *tt.wantAsInt64)
				}
			} else {
				if ok {
					t.Errorf("AsInt64() returned ok=true and value %d for input %q, want ok=false", val, tt.input)
				}
			}

			if tt.wantAsDec != nil {
				if q.AsDec().Cmp(tt.wantAsDec.Dec) != 0 {
					t.Errorf("AsDec() returned %s for input %q, want %s", q.AsDec().String(), tt.input, tt.wantAsDec.Dec.String())
				}
			}

			serialized := q.String()
			expectedString := tt.input
			if tt.canonical != "" {
				if tt.canonical == tt.input {
					t.Errorf("unnecessary identical explicit canonical value in testcase")
				}
				expectedString = tt.canonical
			}
			if serialized != expectedString {
				t.Errorf("expected input %q to reserialize to %q but got %q", tt.input, expectedString, serialized)
			}
		})
	}
}

// TestQuantityPtrEqual 验证 QuantityPtrEqual 对 nil、相等值、不等值的判定。
func TestQuantityPtrEqual(t *testing.T) {
	q1 := MustParse("100m")
	q2 := MustParse("100m")
	q3 := MustParse("200m")

	tests := []struct {
		name   string
		a      *Quantity
		b      *Quantity
		expect bool
	}{
		{
			name:   "both nil",
			a:      nil,
			b:      nil,
			expect: true,
		},
		{
			name:   "first nil",
			a:      nil,
			b:      &q1,
			expect: false,
		},
		{
			name:   "second nil",
			a:      &q1,
			b:      nil,
			expect: false,
		},
		{
			name:   "equal quantities",
			a:      &q1,
			b:      &q2,
			expect: true,
		},
		{
			name:   "unequal quantities",
			a:      &q1,
			b:      &q3,
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := QuantityPtrEqual(tt.a, tt.b); got != tt.expect {
				t.Errorf("QuantityPtrEqual() = %v, want %v", got, tt.expect)
			}
		})
	}
}
