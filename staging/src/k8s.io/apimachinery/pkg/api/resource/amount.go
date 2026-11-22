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
	"math/big"
	"strconv"

	inf "gopkg.in/inf.v0"
)

// Scale is used for getting and setting the base-10 scaled value.
// Base-2 scales are omitted for mathematical simplicity.
// See Quantity.ScaledValue for more details.
//
// Scale 是十进制指数：值 = 尾数 * 10^Scale。故意不支持二进制缩放以简化实现；
// 只有规范序列化时才把 1024 的幂用于 BinarySI 后缀。
type Scale int32

// infScale adapts a Scale value to an inf.Scale value.
//
// inf.Dec 的 Scale 语义与本包相反（其 scale 表示小数点后的位数），故取负号。
func (s Scale) infScale() inf.Scale {
	return inf.Scale(-s) // inf.Scale is upside-down
}

// 常用的十进制 Scale 常量，对应十进制的 SI 后缀。
const (
	Nano  Scale = -9
	Micro Scale = -6
	Milli Scale = -3
	Kilo  Scale = 3
	Mega  Scale = 6
	Giga  Scale = 9
	Tera  Scale = 12
	Peta  Scale = 15
	Exa   Scale = 18
)

var (
	// Zero 是可复用的零值，避免为常见零值分配。
	Zero = int64Amount{}

	// Used by quantity strings - treat as read only
	zeroBytes = []byte("0")
)

// int64Amount represents a fixed precision numerator and arbitrary scale exponent. It is faster
// than operations on inf.Dec for values that can be represented as int64.
// +k8s:openapi-gen=true
//
// int64Amount 用 int64 尾数加十进制指数表示数值，是 Quantity 的快速路径。
// 所有运算在溢出/损失精度时返回 false，调用方据此回退到 infDecAmount。
type int64Amount struct {
	value int64
	scale Scale
}

// Sign returns 0 if the value is zero, -1 if it is less than 0, or 1 if it is greater than 0.
func (a int64Amount) Sign() int {
	switch {
	case a.value == 0:
		return 0
	case a.value > 0:
		return 1
	default:
		return -1
	}
}

// AsInt64 returns the current amount as an int64 at scale 0, or false if the value cannot be
// represented in an int64 OR would result in a loss of precision. This method is intended as
// an optimization to avoid calling AsDec.
func (a int64Amount) AsInt64() (int64, bool) {
	if a.scale == 0 {
		return a.value, true
	}
	if a.scale < 0 {
		// TODO: attempt to reduce factors, although it is assumed that factors are reduced prior
		// to the int64Amount being created.
		// 负指数意味着值被放大了，无法无损转为整数，直接放弃。
		return 0, false
	}
	return positiveScaleInt64(a.value, a.scale)
}

// AsScaledInt64 returns an int64 representing the value of this amount at the specified scale,
// rounding up, or false if that would result in overflow. (1e20).AsScaledInt64(1) would result
// in overflow because 1e19 is not representable as an int64. Note that setting a scale larger
// than the current value may result in loss of precision - i.e. (1e-6).AsScaledInt64(0) would
// return 1, because 0.000001 is rounded up to 1.
func (a int64Amount) AsScaledInt64(scale Scale) (result int64, ok bool) {
	if a.scale < scale {
		// 目标 scale 更小，等价于除以 10 的幂；这里按向上取整处理。
		result, _ = negativeScaleInt64(a.value, scale-a.scale)
		return result, true
	}
	return positiveScaleInt64(a.value, a.scale-scale)
}

// AsDec returns an inf.Dec representation of this value.
func (a int64Amount) AsDec() *inf.Dec {
	var base inf.Dec
	base.SetUnscaled(a.value)
	base.SetScale(inf.Scale(-a.scale))
	return &base
}

// Cmp returns 0 if a and b are equal, 1 if a is greater than b, or -1 if a is less than b.
func (a int64Amount) Cmp(b int64Amount) int {
	switch {
	case a.scale == b.scale:
		// compare only the unscaled portion
		// scale 相同，直接比较尾数即可。
	case a.scale > b.scale:
		// 把 b 对齐到 a 的 scale；无法精确对齐时回退到任意精度比较。
		result, remainder, exact := divideByScaleInt64(b.value, a.scale-b.scale)
		if !exact {
			return a.AsDec().Cmp(b.AsDec())
		}
		if result == a.value {
			// 尾数相等时由余数决定大小：b 被截断的部分非零，说明 b 实际更大。
			switch {
			case remainder == 0:
				return 0
			case remainder > 0:
				return -1
			default:
				return 1
			}
		}
		b.value = result
	default:
		// 对称情形：把 a 对齐到 b 的 scale。
		result, remainder, exact := divideByScaleInt64(a.value, b.scale-a.scale)
		if !exact {
			return a.AsDec().Cmp(b.AsDec())
		}
		if result == b.value {
			switch {
			case remainder == 0:
				return 0
			case remainder > 0:
				return 1
			default:
				return -1
			}
		}
		a.value = result
	}

	switch {
	case a.value == b.value:
		return 0
	case a.value < b.value:
		return -1
	default:
		return 1
	}
}

// Add adds two int64Amounts together, matching scales. It will return false and not mutate
// a if overflow or underflow would result.
func (a *int64Amount) Add(b int64Amount) bool {
	switch {
	case b.value == 0:
		return true
	case a.value == 0:
		// 用 b 直接替换 a，避免无意义的对齐运算。
		a.value = b.value
		a.scale = b.scale
		return true
	case a.scale == b.scale:
		c, ok := int64Add(a.value, b.value)
		if !ok {
			return false
		}
		a.value = c
	case a.scale > b.scale:
		// 把 a 放大到 b 的 scale，再相加；任一步溢出都整体失败。
		c, ok := positiveScaleInt64(a.value, a.scale-b.scale)
		if !ok {
			return false
		}
		c, ok = int64Add(c, b.value)
		if !ok {
			return false
		}
		a.scale = b.scale
		a.value = c
	default:
		// 对称情形：放大 b 后与 a 相加。
		c, ok := positiveScaleInt64(b.value, b.scale-a.scale)
		if !ok {
			return false
		}
		c, ok = int64Add(a.value, c)
		if !ok {
			return false
		}
		a.value = c
	}
	return true
}

// Sub removes the value of b from the current amount, or returns false if underflow would result.
func (a *int64Amount) Sub(b int64Amount) bool {
	// 复用 Add，通过对 b 取负实现减法。
	return a.Add(int64Amount{value: -b.value, scale: b.scale})
}

// Mul multiplies the provided b to the current amount, or
// returns false if overflow or underflow would result.
func (a *int64Amount) Mul(b int64) bool {
	switch {
	case a.value == 0:
		return true
	case b == 0:
		// 乘 0 后统一归一化为 scale=0，便于后续比较与序列化。
		a.value = 0
		a.scale = 0
		return true
	case a.scale == 0:
		c, ok := int64Multiply(a.value, b)
		if !ok {
			return false
		}
		a.value = c
	case a.scale > 0:
		c, ok := int64Multiply(a.value, b)
		if !ok {
			return false
		}
		// 乘法后先检查指数放大是否溢出，避免写回半成品结果。
		if _, ok = positiveScaleInt64(c, a.scale); !ok {
			return false
		}
		a.value = c
	default:
		c, ok := int64Multiply(a.value, b)
		if !ok {
			return false
		}
		if _, ok = negativeScaleInt64(c, -a.scale); !ok {
			return false
		}
		a.value = c
	}
	return true
}

// AsScale adjusts this amount to set a minimum scale, rounding up, and returns true iff no precision
// was lost. (1.1e5).AsScale(5) would return 1.1e5, but (1.1e5).AsScale(6) would return 1e6.
func (a int64Amount) AsScale(scale Scale) (int64Amount, bool) {
	if a.scale >= scale {
		// 已经是至少需要的精度，无需调整。
		return a, true
	}
	result, exact := negativeScaleInt64(a.value, scale-a.scale)
	return int64Amount{value: result, scale: scale}, exact
}

// AsCanonicalBytes accepts a buffer to write the base-10 string value of this field to, and returns
// either that buffer or a larger buffer and the current exponent of the value. The value is adjusted
// until the exponent is a multiple of 3 - i.e. 1.1e5 would return "110", 3.
func (a int64Amount) AsCanonicalBytes(out []byte) (result []byte, exponent int32) {
	mantissa := a.value
	exponent = int32(a.scale)

	// 先把尾数中所有 10 的因子移到指数，得到最紧凑的尾数。
	amount, times := removeInt64Factors(mantissa, 10)
	exponent += int32(times)

	// make sure exponent is a multiple of 3
	// 规范形式要求指数为 3 的倍数；通过把 10 或 100 乘回尾数来对齐。
	var ok bool
	switch exponent % 3 {
	case 1, -2:
		amount, ok = int64MultiplyScale10(amount)
		if !ok {
			// 放大溢出时退回任意精度实现。
			return infDecAmount{a.AsDec()}.AsCanonicalBytes(out)
		}
		exponent = exponent - 1
	case 2, -1:
		amount, ok = int64MultiplyScale100(amount)
		if !ok {
			return infDecAmount{a.AsDec()}.AsCanonicalBytes(out)
		}
		exponent = exponent - 2
	}
	return strconv.AppendInt(out, amount, 10), exponent
}

// AsCanonicalBase1024Bytes accepts a buffer to write the base-1024 string value of this field to, and returns
// either that buffer or a larger buffer and the current exponent of the value. 2048 is 2 * 1024 ^ 1 and would
// return []byte("2048"), 1.
func (a int64Amount) AsCanonicalBase1024Bytes(out []byte) (result []byte, exponent int32) {
	// BinarySI 语法要求先转成 scale=0 的整数；无法无损转换时退回任意精度。
	value, ok := a.AsScaledInt64(0)
	if !ok {
		return infDecAmount{a.AsDec()}.AsCanonicalBase1024Bytes(out)
	}
	amount, exponent := removeInt64Factors(value, 1024)
	return strconv.AppendInt(out, amount, 10), exponent
}

// infDecAmount implements common operations over an inf.Dec that are specific to the quantity
// representation.
//
// infDecAmount 是任意精度的回退实现，用于 int64Amount 溢出的场景。
type infDecAmount struct {
	*inf.Dec
}

// AsScale adjusts this amount to set a minimum scale, rounding up, and returns true iff no precision
// was lost. (1.1e5).AsScale(5) would return 1.1e5, but (1.1e5).AsScale(6) would return 1e6.
func (a infDecAmount) AsScale(scale Scale) (infDecAmount, bool) {
	tmp := &inf.Dec{}
	// 向上取整后再与原值比较，相等即说明未损失精度。
	tmp.Round(a.Dec, scale.infScale(), inf.RoundUp)
	return infDecAmount{tmp}, tmp.Cmp(a.Dec) == 0
}

// AsCanonicalBytes accepts a buffer to write the base-10 string value of this field to, and returns
// either that buffer or a larger buffer and the current exponent of the value. The value is adjusted
// until the exponent is a multiple of 3 - i.e. 1.1e5 would return "110", 3.
func (a infDecAmount) AsCanonicalBytes(out []byte) (result []byte, exponent int32) {
	mantissa := a.Dec.UnscaledBig()
	exponent = int32(-a.Dec.Scale())
	amount := big.NewInt(0).Set(mantissa)
	// move all factors of 10 into the exponent for easy reasoning
	amount, times := removeBigIntFactors(amount, bigTen)
	exponent += times

	// make sure exponent is a multiple of 3
	// 用乘法（而不是除法）对齐指数，保证不会截断尾数。
	for exponent%3 != 0 {
		amount.Mul(amount, bigTen)
		exponent--
	}

	return append(out, amount.String()...), exponent
}

// AsCanonicalBase1024Bytes accepts a buffer to write the base-1024 string value of this field to, and returns
// either that buffer or a larger buffer and the current exponent of the value. 2048 is 2 * 1024 ^ 1 and would
// return []byte("2048"), 1.
func (a infDecAmount) AsCanonicalBase1024Bytes(out []byte) (result []byte, exponent int32) {
	// 先按 RoundUp 取整到整数，再剥离 1024 的因子。
	tmp := &inf.Dec{}
	tmp.Round(a.Dec, 0, inf.RoundUp)
	amount, exponent := removeBigIntFactors(tmp.UnscaledBig(), big1024)
	return append(out, amount.String()...), exponent
}
