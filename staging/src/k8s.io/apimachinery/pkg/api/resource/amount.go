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

// Scale 用于读取和设置十进制缩放值。
// 为简化数学运算，这里不使用以 2 为底的 Scale。
// 更多细节请参阅 Quantity.ScaledValue。
//
// Scale 是十进制指数：值 = 尾数 * 10^Scale。故意不支持二进制缩放以简化实现；
// 只有规范序列化时才把 1024 的幂用于 BinarySI 后缀。
type Scale int32

// infScale 将本包的 Scale 转换为 inf.Scale。
//
// inf.Dec 的 Scale 语义与本包相反（其 scale 表示小数点后的位数），故取负号。
func (s Scale) infScale() inf.Scale {
	return inf.Scale(-s) // inf.Scale 的缩放方向相反
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

	// 供 Quantity 字符串处理使用，只读，不应修改。
	zeroBytes = []byte("0")
)

// int64Amount 使用定点 int64 尾数和可变十进制指数表示数值。
// 对于能够由 int64 表示的数值，该实现比 inf.Dec 运算更快。
// +k8s:openapi-gen=true
//
// int64Amount 用 int64 尾数加十进制指数表示数值，是 Quantity 的快速路径。
// 所有运算在溢出/损失精度时返回 false，调用方据此回退到 infDecAmount。
type int64Amount struct {
	value int64
	scale Scale
}

// Sign 返回数值的符号：零返回 0，负数返回 -1，正数返回 1。
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

// AsInt64 将当前数值转换为 Scale 为 0 的 int64；如果无法表示或转换会损失精度，则返回 false。
// 该方法用于快速判断是否可以直接使用 int64，避免创建 AsDec 所需的任意精度对象。

func (a int64Amount) AsInt64() (int64, bool) {
	if a.scale == 0 {
		return a.value, true
	}
	if a.scale < 0 {
		// 负指数意味着值被放大了，无法无损转为整数，直接放弃。
		return 0, false
	}
	return positiveScaleInt64(a.value, a.scale)
}

// AsScaledInt64 将当前数值转换为指定 Scale 下的 int64，并在需要时向上取整。
// 如果转换导致溢出则返回 false。例如 1e20 转换到 Scale=1 需要得到 1e19，超出 int64 范围。
// 目标 Scale 比当前值更大时可能损失精度；例如 1e-6 转为 Scale=0 会向上取整为 1。

func (a int64Amount) AsScaledInt64(scale Scale) (result int64, ok bool) {
	if a.scale < scale {
		// 目标 scale 更小，等价于除以 10 的幂；这里按向上取整处理。
		result, _ = negativeScaleInt64(a.value, scale-a.scale)
		return result, true
	}
	return positiveScaleInt64(a.value, a.scale-scale)
}

// AsDec 将当前数值转换为 inf.Dec 任意精度十进制表示。
func (a int64Amount) AsDec() *inf.Dec {
	var base inf.Dec
	base.SetUnscaled(a.value)
	base.SetScale(inf.Scale(-a.scale))
	return &base
}

// Cmp 比较两个数值：相等返回 0，a 大于 b 返回 1，a 小于 b 返回 -1。
func (a int64Amount) Cmp(b int64Amount) int {
	switch {
	case a.scale == b.scale:
		// Scale 相同，只需比较未缩放的尾数。
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

// Add 将两个 int64Amount 对齐到相同 Scale 后相加；若发生溢出或下溢，返回 false 且不修改接收者。

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

// Sub 从当前数值中减去 b；若运算无法由 int64 精确表示，则返回 false。
func (a *int64Amount) Sub(b int64Amount) bool {
	// 复用 Add，通过对 b 取负实现减法。
	return a.Add(int64Amount{value: -b.value, scale: b.scale})
}

// Mul 将当前数值乘以 b；若发生溢出或下溢则返回 false。

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

// AsScale 将当前数值调整到至少指定的 Scale，并向上取整；返回值表示调整是否完全没有损失精度。
// 例如 1.1e5 转换到 Scale=5 保持为 1.1e5，转换到 Scale=6 则向上取整为 1e6。
func (a int64Amount) AsScale(scale Scale) (int64Amount, bool) {
	if a.scale >= scale {
		// 已经是至少需要的精度，无需调整。
		return a, true
	}
	result, exact := negativeScaleInt64(a.value, scale-a.scale)
	return int64Amount{value: result, scale: scale}, exact
}

// AsCanonicalBytes 将数值编码为十进制规范形式，并返回结果字节及对应指数。
// 函数会复用传入缓冲区；空间不足时分配更大的缓冲区。编码时会调整尾数与指数，
func (a int64Amount) AsCanonicalBytes(out []byte) (result []byte, exponent int32) {
	mantissa := a.value
	exponent = int32(a.scale)

	// 先把尾数中所有 10 的因子移到指数，得到最紧凑的尾数。
	amount, times := removeInt64Factors(mantissa, 10)
	exponent += int32(times)

	// 确保指数是 3 的倍数。
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

// AsCanonicalBase1024Bytes 将数值编码为以 1024 为基数的规范形式，并返回结果字节及指数。
// 例如 2048 等于 2 乘以 1024 的 1 次方，返回尾数 2048、指数 1。

func (a int64Amount) AsCanonicalBase1024Bytes(out []byte) (result []byte, exponent int32) {
	// BinarySI 语法要求先转成 scale=0 的整数；无法无损转换时退回任意精度。
	value, ok := a.AsScaledInt64(0)
	if !ok {
		return infDecAmount{a.AsDec()}.AsCanonicalBase1024Bytes(out)
	}
	amount, exponent := removeInt64Factors(value, 1024)
	return strconv.AppendInt(out, amount, 10), exponent
}

// infDecAmount 基于 inf.Dec 实现 Quantity 所需的任意精度数值运算。

// infDecAmount 是任意精度的回退实现，用于 int64Amount 溢出的场景。
type infDecAmount struct {
	*inf.Dec
}

// AsScale 将数值调整到指定的最小 scale 并向上取整；未损失精度时返回 true。
// 例如 (1.1e5).AsScale(5) 返回 1.1e5，而 AsScale(6) 返回 1e6。
func (a infDecAmount) AsScale(scale Scale) (infDecAmount, bool) {
	tmp := &inf.Dec{}
	// 向上取整后再与原值比较，相等即说明未损失精度。
	tmp.Round(a.Dec, scale.infScale(), inf.RoundUp)
	return infDecAmount{tmp}, tmp.Cmp(a.Dec) == 0
}

// AsCanonicalBytes 将十进制尾数写入缓冲区，并返回该缓冲区（或扩容后的缓冲区）与指数。
// 值会调整到指数为 3 的倍数；如 1.1e5 返回 "110" 和 3。
func (a infDecAmount) AsCanonicalBytes(out []byte) (result []byte, exponent int32) {
	mantissa := a.Dec.UnscaledBig()
	exponent = int32(-a.Dec.Scale())
	amount := big.NewInt(0).Set(mantissa)
	// 将尾数中所有 10 的因子移入指数，便于后续规范化计算。
	amount, times := removeBigIntFactors(amount, bigTen)
	exponent += times

	// 用乘法（而不是除法）对齐指数，保证不会截断尾数。
	for exponent%3 != 0 {
		amount.Mul(amount, bigTen)
		exponent--
	}

	return append(out, amount.String()...), exponent
}

// AsCanonicalBase1024Bytes 将以 1024 为底的尾数写入缓冲区，并返回缓冲区与指数。
// 如 2048 = 2 * 1024^1，返回 "2" 和 1。
// 返回 []byte("2048") 和 1。
func (a infDecAmount) AsCanonicalBase1024Bytes(out []byte) (result []byte, exponent int32) {
	// 先按 RoundUp 取整到整数，再剥离 1024 的因子。
	tmp := &inf.Dec{}
	tmp.Round(a.Dec, 0, inf.RoundUp)
	amount, exponent := removeBigIntFactors(tmp.UnscaledBig(), big1024)
	return append(out, amount.String()...), exponent
}
