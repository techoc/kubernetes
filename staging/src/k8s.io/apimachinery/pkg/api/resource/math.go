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

	inf "gopkg.in/inf.v0"
)

// 本文件实现 Quantity 所需的整数/十进制安全运算：所有乘除都显式检测溢出，
// 以便在解析与归一化资源数量时能可靠地判断"是否超出 int64 可表示范围"，
// 超出后调用方会退回使用 inf.Dec 大数表示。

const (
	// maxInt64Factors 是从 int64 中反复除去 10 的因子时检查的最大次数，
	// 同时也是 int64 能表示的最大十进制位数。
	maxInt64Factors = 18
)

var (
	// 常用的 big.Int 值，只读使用，切勿修改。
	// 共享同一实例可避免在热路径上反复分配。
	bigTen      = big.NewInt(10)
	bigZero     = big.NewInt(0)
	bigOne      = big.NewInt(1)
	bigThousand = big.NewInt(1000)
	big1024     = big.NewInt(1024)

	// 常用的 inf.Dec 值，只读使用。
	decZero = inf.NewDec(0, 0)
	decOne  = inf.NewDec(1, 0)

	// 允许的最大（按绝对值）数值，等价于 max int64。
	maxAllowed = infDecAmount{inf.NewDec((1<<63)-1, 0)} // == max int64

	// MaxMilliValue 是能够安全以毫单位表示的数值上限。
	// 可将其与 Quantity.Value() 的返回值比较，以判断调用 Quantity.MilliValue() 是否安全
	// （否则乘以 1000 可能溢出）。
	MaxMilliValue = int64(((1 << 63) - 1) / 1000)
)

// int64 的边界值，单独命名以免在溢出判断中散落魔法数字。
const mostNegative = -(mostPositive + 1)
const mostPositive = 1<<63 - 1

// int64Add 返回 a+b；若结果会溢出 int64 则返回 false。
// 利用 Go 中有符号整数加法回绕的特性：同号相加后符号翻转即说明溢出。
func int64Add(a, b int64) (int64, bool) {
	c := a + b
	switch {
	case a > 0 && b > 0:
		if c < 0 {
			return 0, false
		}
	case a < 0 && b < 0:
		if c > 0 {
			return 0, false
		}
		// mostNegative + mostNegative 在二进制补码下回绕后仍为负数，符号检查无法发现，
		// 需要单独拦截。
		if a == mostNegative && b == mostNegative {
			return 0, false
		}
	}
	return c, true
}

// int64Multiply 返回 a*b；若结果会溢出或下溢 int64 则返回 false。
// 通过回除校验：乘积除以 b 后能还原为 a 才认为没有溢出。
func int64Multiply(a, b int64) (int64, bool) {
	// 零因子与乘 1 不可能溢出，走快速路径。
	if a == 0 || b == 0 || a == 1 || b == 1 {
		return a * b, true
	}
	// mostNegative 乘以任何非 0/1 的值都会越界（或回绕后无法回除校验），直接拒绝。
	if a == mostNegative || b == mostNegative {
		return 0, false
	}
	c := a * b
	return c, c/b == a
}

// int64MultiplyScale 返回 a*b（假设 b 大于 1）；若结果会溢出或下溢 int64 则返回 false。
// 当已知 b > 1 时使用，可省略 b == 0/1 的检查。
// Use when b is known to be greater than one.
func int64MultiplyScale(a int64, b int64) (int64, bool) {
	if a == 0 || a == 1 {
		return a * b, true
	}
	if a == mostNegative && b != 1 {
		return 0, false
	}
	c := a * b
	return c, c/b == a
}

// int64MultiplyScale10 将 a 乘以 10；若会溢出则返回 false。
// 它比 int64Multiply(a, 10) 更快，因为常量因子乘法可被编译器优化。
func int64MultiplyScale10(a int64) (int64, bool) {
	if a == 0 || a == 1 {
		return a * 10, true
	}
	if a == mostNegative {
		return 0, false
	}
	c := a * 10
	return c, c/10 == a
}

// int64MultiplyScale100 将 a 乘以 100；若会溢出则返回 false。
// 常量因子使编译器可做更优的乘法实现，故比通用的 int64Multiply 更快。
func int64MultiplyScale100(a int64) (int64, bool) {
	if a == 0 || a == 1 {
		return a * 100, true
	}
	if a == mostNegative {
		return 0, false
	}
	c := a * 100
	return c, c/100 == a
}

// int64MultiplyScale1000 将 a 乘以 1000；若会溢出则返回 false。
// 同样依赖常量因子优化，速度快于通用的 int64Multiply。
func int64MultiplyScale1000(a int64) (int64, bool) {
	if a == 0 || a == 1 {
		return a * 1000, true
	}
	if a == mostNegative {
		return 0, false
	}
	c := a * 1000
	return c, c/1000 == a
}

// positiveScaleInt64 将 base 乘以 10^scale，溢出时返回 false。
// 传入负 scale 的行为未定义；常见 scale（0/1/2/3/6/9）走常量快速路径，其余逐次乘 10。
// 注意：该函数无法正确表达 10^scale 本身已超出 int64 的情况，调用方需自行保证 scale 合理。
func positiveScaleInt64(base int64, scale Scale) (int64, bool) {
	switch scale {
	case 0:
		return base, true
	case 1:
		return int64MultiplyScale10(base)
	case 2:
		return int64MultiplyScale100(base)
	case 3:
		return int64MultiplyScale1000(base)
	case 6:
		return int64MultiplyScale(base, 1000000)
	case 9:
		return int64MultiplyScale(base, 1000000000)
	default:
		value := base
		var ok bool
		for i := Scale(0); i < scale; i++ {
			if value, ok = int64MultiplyScale(value, 10); !ok {
				return 0, false
			}
		}
		return value, true
	}
}

// negativeScaleInt64 将 base 除以 10^scale，并在无法整除时按远离零的方向取整
// （正数向上、负数向下），直到值归零或达到指定 scale。传入负 scale 的行为未定义。
// 返回值中的 exact 表示除法是否恰好整除。
//
// 实现先逐位除以 10 而不是一次性除以 pow10，是为了在中间值归零时能立刻返回，
// 避免构造可能溢出的 10^scale。
func negativeScaleInt64(base int64, scale Scale) (result int64, exact bool) {
	if scale == 0 {
		return base, true
	}

	value := base
	var fraction bool
	for i := Scale(0); i < scale; i++ {
		// 一旦发现某一位非零，就说明结果不是精确值；后续只需继续缩小数值。
		if !fraction && value%10 != 0 {
			fraction = true
		}
		value = value / 10
		// 数值已缩小到 0：若有被丢弃的余数，按远离零方向返回 ±1，否则精确返回 0。
		if value == 0 {
			if fraction {
				if base > 0 {
					return 1, false
				}
				return -1, false
			}
			return 0, true
		}
	}
	// 循环结束后仍有非零余数，按远离零方向修正最后一位。
	if fraction {
		if base > 0 {
			value++
		} else {
			value--
		}
	}
	return value, !fraction
}

// pow10Int64 返回 10^b 的 int64 常量值；b 超出 0..18 时返回 0。
// 用 switch 展开而非 math.Pow，既避免浮点误差，也便于编译器内联。
func pow10Int64(b int64) int64 {
	switch b {
	case 0:
		return 1
	case 1:
		return 10
	case 2:
		return 100
	case 3:
		return 1000
	case 4:
		return 10000
	case 5:
		return 100000
	case 6:
		return 1000000
	case 7:
		return 10000000
	case 8:
		return 100000000
	case 9:
		return 1000000000
	case 10:
		return 10000000000
	case 11:
		return 100000000000
	case 12:
		return 1000000000000
	case 13:
		return 10000000000000
	case 14:
		return 100000000000000
	case 15:
		return 1000000000000000
	case 16:
		return 10000000000000000
	case 17:
		return 100000000000000000
	case 18:
		return 1000000000000000000
	default:
		return 0
	}
}

// divideByScaleInt64 返回 base 除以 10^scale 的商与余数；无法进行该除法时返回 false。
// 注释中的 "scale * 10" 实为笔误，含义是除以 10^scale。传入负 scale 的行为未定义。
func divideByScaleInt64(base int64, scale Scale) (result, remainder int64, exact bool) {
	if scale == 0 {
		return base, 0, true
	}
	// int64 最多能精确表示 18 位十进制数；scale 达到 18 时除数会超出 int64，无法计算。
	if scale >= 18 {
		return 0, base, false
	}
	divisor := pow10Int64(int64(scale))
	return base / divisor, base % divisor, true
}

// removeInt64Factors 反复除去 value 中的 base 因子；返回满足 value == result * base ^ times 的结果。
// 常用于把数量归一化到最小整数形式（例如 1000m -> 1）。符号在除因子前后单独处理，
// 这样对负数也能正确工作。
func removeInt64Factors(value int64, base int64) (result int64, times int32) {
	times = 0
	result = value
	// 先取绝对值，避免负数取模的符号问题；结束时再恢复符号。
	negative := result < 0
	if negative {
		result = -result
	}
	switch base {
	// 10 与 1024 是最常见的底数（十进制与二进制单位），单独展开便于编译器优化。
	case 10:
		for result >= 10 && result%10 == 0 {
			times++
			result = result / 10
		}
	// 1024 是二进制单位（Ki、Mi 等）的底数。
	case 1024:
		for result >= 1024 && result%1024 == 0 {
			times++
			result = result / 1024
		}
	default:
		for result >= base && result%base == 0 {
			times++
			result = result / base
		}
	}
	if negative {
		result = -result
	}
	return result, times
}

// removeBigIntFactors 反复除去大整数 d 中的 factor 因子；返回满足 d == result * factor ^ times 的结果。
// d 可能被就地修改。若 d == 0，返回 (0, 0)。
//
// 通过复用 q 与 d 的缓冲（d, q = q, d）避免每轮循环都分配新的 big.Int。
func removeBigIntFactors(d, factor *big.Int) (result *big.Int, times int32) {
	q := big.NewInt(0)
	m := big.NewInt(0)
	for d.Cmp(bigZero) != 0 {
		q.DivMod(d, factor, m)
		// 余数非零说明 d 不再含有 factor 因子，停止。
		if m.Cmp(bigZero) != 0 {
			break
		}
		times++
		// 交换缓冲：下一轮把商写回原来的 d 缓冲，减少分配。
		d, q = q, d
	}
	return d, times
}
