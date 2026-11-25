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
	"bytes"
	"errors"
	"fmt"
	math "math"
	"math/big"
	"strconv"
	"strings"

	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"

	inf "gopkg.in/inf.v0"
)

// Quantity 是数值的定点表示，支持 JSON/YAML 编解码，并提供 String() 与
// AsInt64() 等访问方法。序列化格式如下：
//
// ```
//
//	（<suffix> 可为空，对应 <decimalSI> 中的 ""。）
//
//	（国际单位制；参见 http://physics.nist.gov/cuu/Units/binary.html）
//
//	（注意 1024 = 1Ki，而 1000 = 1k；大小写区分是既定格式。）
//
// ```
//
// 无论使用哪种指数形式，Quantity 的绝对值都不得超过 2^63-1，且最多保留
// 3 位小数；超出范围的值会被截断或向上取整（如 0.1m 变为 1m）。
// 未来若需要更宽范围或更高精度，可能扩展此限制。
//
// 从字符串解析时会记住后缀类型，序列化时沿用同一类型。
// 序列化前会先转为规范形式：调整 Exponent/suffix 并相应缩放 Mantissa，
// 保证不损失精度、不输出小数位，并尽可能使用最大的指数或后缀。
// 非负数省略正号。例如 1.5 序列化为 "1500m"，1.5Gi 序列化为 "1536Mi"。
//
// Quantity 内部绝不使用浮点数，以促使调用方同样采用定点实现。
// 格式正确的非规范值也可解析，但会按规范形式重新输出；比较差异前应先规范化。
//
// +protobuf=true
// +protobuf.embed=string
// +protobuf.options.marshal=false
// +protobuf.options.(gogoproto.goproto_stringer)=false
// +k8s:deepcopy-gen=true
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=io.k8s.apimachinery.pkg.api.resource
type Quantity struct {
	// i 在 d.Dec == nil 时保存 int64 缩放形式的数量。
	i int64Amount
	// d 在 d.Dec != nil 时保存 inf.Dec 形式的数量。
	d infDecAmount
	// s 缓存生成的字符串，避免重复计算。
	s string

	// Format 可任意设置；规范化的详细规则见 CanonicalizeBytes。
	Format
}

// CanonicalValue 允许将数量转换为规范的尾数和指数。
type CanonicalValue interface {
	// AsCanonicalBytes 返回十进制尾数字节与 int32 指数；可传入字节切片以避免分配。
	AsCanonicalBytes(out []byte) ([]byte, int32)
	// AsCanonicalBase1024Bytes 返回以 1024 为底的尾数字节与 int32 指数；可传入字节切片以避免分配。
	AsCanonicalBase1024Bytes(out []byte) ([]byte, int32)
}

// Format 表示 Quantity 的三种格式，记录解析时使用的后缀风格；规范序列化会尽量保留该风格。
type Format string

const (
	DecimalExponent = Format("DecimalExponent") // 例如 12e6
	BinarySI        = Format("BinarySI")        // 例如 12Mi（12 * 2^20）
	DecimalSI       = Format("DecimalSI")       // 例如 12M（12 * 10^6）
)

// MustParse 将字符串解析为 Quantity；解析失败时 panic，仅适用于已知字符串合法的测试等场景。
func MustParse(str string) Quantity {
	q, err := ParseQuantity(str)
	if err != nil {
		panic(fmt.Errorf("cannot parse '%v': %v", str, err))
	}
	return q
}

const (
	// splitREString 用于拆分数字与后缀，刻意放宽匹配；后续会另行校验。
	splitREString = "^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$"
)

var (
	// 解析字符串时可能发生的错误。
	ErrFormatWrong = errors.New("quantities must match the regular expression '" + splitREString + "'")
	ErrNumeric     = errors.New("unable to parse numeric part of quantity")
	ErrSuffix      = errors.New("unable to parse quantity's suffix")
)

// parseQuantityString 是数量字符串的快速扫描器，拆出分子、分母和后缀，
// 避免在热路径使用正则表达式。
func parseQuantityString(str string) (positive bool, value, num, denom, suffix string, err error) {
	positive = true
	pos := 0
	end := len(str)

	// 处理前导符号。
	if pos < end {
		switch str[0] {
		case '-':
			positive = false
			pos++
		case '+':
			pos++
		}
	}

	// 去除前导零。
	// 全零值归一为 "0"，也使下方的规范形式检查更简单。
Zeroes:
	for i := pos; ; i++ {
		if i >= end {
			num = "0"
			value = num
			return
		}
		switch str[i] {
		case '0':
			pos++
		default:
			break Zeroes
		}
	}

	// 提取分子。
	// 到达字符串末尾时，剩余部分整体作为分子。
Num:
	for i := pos; ; i++ {
		if i >= end {
			num = str[pos:end]
			value = str[0:end]
			return
		}
		switch str[i] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		default:
			num = str[pos:i]
			pos = i
			break Num
		}
	}

	// 若分子部分为空，返回 0。
	if len(num) == 0 {
		num = "0"
	}

	// 处理分母。
	// '.' 分隔符可选，其后可没有数字（如 "1."）。
	if pos < end && str[pos] == '.' {
		pos++
	Denom:
		for i := pos; ; i++ {
			if i >= end {
				denom = str[pos:end]
				value = str[0:end]
				return
			}
			switch str[i] {
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			default:
				denom = str[pos:i]
				pos = i
				break Denom
			}
		}
		// TODO: 当前允许 1.G，未来可能禁止。
		// if len(denom) == 0 {
		// 	err = ErrFormatWrong
		// 	return
		// }
	}
	value = str[0:pos]

	// 提取后缀元素。
	// 先接受连续后缀字母，再接受十进制指数可选的正负号和数字（如 "e3"、"E-2"）。
	suffixStart := pos
	for i := pos; ; i++ {
		if i >= end {
			suffix = str[suffixStart:end]
			return
		}
		if !strings.ContainsAny(str[i:i+1], "eEinumkKMGTP") {
			pos = i
			break
		}
	}
	if pos < end {
		// 只有十进制指数可带正负号。
		switch str[pos] {
		case '-', '+':
			pos++
		}
	}
Suffix:
	for i := pos; ; i++ {
		if i >= end {
			suffix = str[suffixStart:end]
			return
		}
		switch str[i] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		default:
			break Suffix
		}
	}
	// Suffix 循环遇到非数字字符，且前一字符不是合法指数。
	err = ErrFormatWrong
	return
}

// ParseQuantity 将字符串解析为 Quantity。它先拆分数值与单位后缀，再选择 int64 快速路径或 inf.Dec 任意精度路径；格式不合法时返回错误。
func ParseQuantity(str string) (Quantity, error) {
	if len(str) == 0 {
		return Quantity{}, ErrFormatWrong
	}
	if str == "0" {
		// 快速处理常见的零值。
		return Quantity{Format: DecimalSI, s: str}, nil
	}

	positive, value, num, denom, suf, err := parseQuantityString(str)
	if err != nil {
		return Quantity{}, err
	}

	base, exponent, format, ok := quantitySuffixer.interpret(suffix(suf))
	if !ok {
		return Quantity{}, ErrSuffix
	}

	// 判断 int64 快速路径能否表示该值；precision 为负时需使用大数路径。
	precision := int32(0)
	scale := int32(0)
	mantissa := int64(1)
	switch format {
	case DecimalExponent, DecimalSI:
		// 小数部分决定 scale；精度受 int64 可容纳的位数限制。
		scale = exponent
		precision = maxInt64Factors - int32(len(num)+len(denom))
	case BinarySI:
		scale = 0
		switch {
		case exponent >= 0 && len(denom) == 0:
			// 快速路径仅处理正的二进制数。
			mantissa = int64(int64(mantissa) << uint64(exponent))
			// 1Mi（2^20）约有 6 位十进制精度，因此用 exponent*3/10-1 估算精度。
			precision = 15 - int32(len(num)) - int32(float32(exponent)*3/10) - 1
		default:
			// 负指数或二进制小数值回退到 inf.Dec。
			precision = -1
		}
	}

	if precision >= 0 {
		// 有分母时，将整个值左移分母位数。
		scale -= int32(len(denom))
		// Nano 是 Kubernetes 资源使用的最小 scale；更精细的值无法表示，交给慢路径取整。
		if scale >= int32(Nano) {
			shifted := num + denom

			var value int64
			value, err := strconv.ParseInt(shifted, 10, 64)
			if err != nil {
				return Quantity{}, ErrNumeric
			}
			if result, ok := int64Multiply(value, int64(mantissa)); ok {
				if !positive {
					result = -result
				}
				// 若数字已是规范形式，直接复用原字符串，避免 String() 重新序列化并保留用户写法。
				switch format {
				case BinarySI:
					if exponent%10 == 0 && (value&0x07 != 0) {
						return Quantity{i: int64Amount{value: result, scale: Scale(scale)}, Format: format, s: str}, nil
					}
				default:
					if scale%3 == 0 && !strings.HasSuffix(shifted, "000") && shifted[0] != '0' {
						return Quantity{i: int64Amount{value: result, scale: Scale(scale)}, Format: format, s: str}, nil
					}
				}
				return Quantity{i: int64Amount{value: result, scale: Scale(scale)}, Format: format}, nil
			}
		}
	}

	// 慢路径使用任意精度十进制，确保取整到 Nano 前不损失精度。
	amount := new(inf.Dec)
	if _, ok := amount.SetString(value); !ok {
		return Quantity{}, ErrNumeric
	}

	// 移除后缀，使后续运算无需关心单位。
	if base == 10 {
		amount.SetScale(amount.Scale() + Scale(exponent).infScale())
	} else if base == 2 {
		// numericSuffix = 2 ** exponent（数值后缀等于 2 的 exponent 次幂）（数值后缀等于 2 的 exponent 次幂）（数值后缀等于 2 的 exponent 次幂）（数值后缀等于 2 的 exponent 次幂）
		numericSuffix := big.NewInt(1).Lsh(bigOne, uint(exponent))
		ub := amount.UnscaledBig()
		amount.SetUnscaledBig(ub.Mul(ub, numericSuffix))
	}

	// 限制到最小/最大边界。
	sign := amount.Sign()
	// 用绝对值处理以保证对称取整，随后恢复符号。
	if sign == -1 {
		amount.Neg(amount)
	}

	// 非零值向上取整到最小可表示值：即便请求量过小，也应获得一些资源。
	// 虽然可视为 inf.RoundHalfUp（常规取整），但会把小于 .5n 的值舍入为零。
	if v, ok := amount.Unscaled(); v != int64(0) || !ok {
		amount.Round(amount, Nano.infScale(), inf.RoundUp)
	}

	// 最大值直接截断。
	// TODO: 这阻止了集群配额等场景累计超过 int64 的数量。
	// BinarySI 截断到最大 int64；十进制格式由调用方检测溢出。
	if format == BinarySI && amount.Cmp(maxAllowed.Dec) > 0 {
		amount.Set(maxAllowed.Dec)
	}

	// 小于单位的 BinarySI 值会向上取整为 1i，易误导用户，因此改用 DecimalSI。
	// 这样可避免取整和困惑。
	if format == BinarySI && amount.Cmp(decOne) < 0 && amount.Cmp(decZero) > 0 {
		// 这样可避免取整和困惑。
		format = DecimalSI
	}
	// 恢复原始符号。
	if sign == -1 {
		amount.Neg(amount)
	}

	return Quantity{d: infDecAmount{amount}, Format: format}, nil
}

// DeepCopy 返回 Quantity 的深拷贝；值接收者可在副本上原地修改后返回。
func (q Quantity) DeepCopy() Quantity {
	// inf.Dec 是指针，必须复制而非共享；int64Amount 是值类型，可直接复制。
	if q.d.Dec != nil {
		tmp := &inf.Dec{}
		q.d.Dec = tmp.Set(q.d.Dec)
	}
	return q
}

// OpenAPISchemaType 供 kube-openapi 生成器构造该类型的 OpenAPI schema。
//
// 参见 https://github.com/kubernetes/kube-openapi/tree/master/pkg/generators。
func (_ Quantity) OpenAPISchemaType() []string { return []string{"string"} }

// OpenAPISchemaFormat 供 kube-openapi 生成器构造该类型的 OpenAPI schema。
func (_ Quantity) OpenAPISchemaFormat() string { return "" }

// OpenAPIV3OneOfTypes 供 kube-openapi 生成器构造该类型的 OpenAPI v3 schema。
func (Quantity) OpenAPIV3OneOfTypes() []string { return []string{"string", "number"} }

// CanonicalizeBytes 将 Quantity 转换为规范化的尾数字节和单位后缀，尽可能复用调用方传入的缓冲区。
//
//	-1 和 +1 将按 q.Format 为 DecimalSI 输出；非整数会向上取整（1.1i 变为 2i）。
func (q *Quantity) CanonicalizeBytes(out []byte) (result, suffix []byte) {
	if q.IsZero() {
		return zeroBytes, nil
	}

	var rounded CanonicalValue
	format := q.Format
	switch format {
	case DecimalExponent, DecimalSI:
	case BinarySI:
		if q.CmpInt64(-1024) > 0 && q.CmpInt64(1024) < 0 {
			// 这样可避免取整和困惑。
			format = DecimalSI
		} else {
			var exact bool
			if rounded, exact = q.AsScale(0); !exact {
				// 避免损失精度，改用 DecimalSI 显示。
				format = DecimalSI
			}
		}
	default:
		// 未知格式回退到始终正确的十进制指数形式。
		format = DecimalExponent
	}

	// TODO: 请求 BinarySI 格式会导致取整时，改用其他格式。
	switch format {
	case DecimalExponent, DecimalSI:
		number, exponent := q.AsCanonicalBytes(out)
		suffix, _ := quantitySuffixer.constructBytes(10, exponent, format)
		return number, suffix
	default:
		// 此处格式必为 BinarySI。二进制指数以 1024 的幂表示，每级对应 10 bit；
		// 后缀表使用等价的十进制指数作键。
		number, exponent := rounded.AsCanonicalBase1024Bytes(out)
		suffix, _ := quantitySuffixer.constructBytes(2, exponent*10, format)
		return number, suffix
	}
}

// AsApproximateFloat64 将 Quantity 转换为 float64，优先保证速度，因此可能损失精度。
func (q *Quantity) AsApproximateFloat64() float64 {
	var base float64
	var exponent int
	// inf.Dec 保存未缩放整数与十进制 scale，指数为 scale 的相反数；int64 形式直接保存 scale。
	if q.d.Dec != nil {
		base, _ = big.NewFloat(0).SetInt(q.d.Dec.UnscaledBig()).Float64()
		exponent = int(-q.d.Dec.Scale())
	} else {
		base = float64(q.i.value)
		exponent = int(q.i.scale)
	}
	if exponent == 0 {
		return base
	}

	// 单次 Pow10 乘法保持快速路径，但可能溢出为 +/-Inf。
	return base * math.Pow10(exponent)
}

// AsFloat64Slow 使用任意精度中间计算后再转换为 float64，比 AsApproximateFloat64 更准确，但速度更慢。
func (q *Quantity) AsFloat64Slow() float64 {
	infDec := q.AsDec()

	// 用 big.Int 精确计算 10^|scale| 后乘除，避免大指数下 math.Pow10 的舍入误差。
	var absScale int64
	if infDec.Scale() < 0 {
		absScale = int64(-infDec.Scale())
	} else {
		absScale = int64(infDec.Scale())
	}
	pow10AbsScale := big.NewInt(10)
	pow10AbsScale = pow10AbsScale.Exp(pow10AbsScale, big.NewInt(absScale), nil)

	var resultBigFloat *big.Float
	if infDec.Scale() < 0 {
		resultBigInt := new(big.Int).Mul(infDec.UnscaledBig(), pow10AbsScale)
		resultBigFloat = new(big.Float).SetInt(resultBigInt)
	} else {
		pow10AbsScaleFloat := new(big.Float).SetInt(pow10AbsScale)
		resultBigFloat = new(big.Float).SetInt(infDec.UnscaledBig())
		resultBigFloat = resultBigFloat.Quo(resultBigFloat, pow10AbsScaleFloat)
	}

	result, _ := resultBigFloat.Float64()
	return result
}

// AsInt64 尝试将当前 Quantity 无损转换为 int64；无法快速、精确表示时返回 false，调用方应改用 AsDec。
func (q *Quantity) AsInt64() (int64, bool) {
	if q.d.Dec != nil {
		return 0, false
	}
	return q.i.AsInt64()
}

// ToDec 将 Quantity 原地提升为 inf.Dec 任意精度表示并返回自身。提升是单向的，避免 int64 与十进制两份数据不一致。
func (q *Quantity) ToDec() *Quantity {
	// 提升是单向的：填充 inf.Dec 后清空 int64 形式，避免两种表示不一致。
	if q.d.Dec == nil {
		q.d.Dec = q.i.AsDec()
		q.i = int64Amount{}
	}
	return q
}

// AsDec 返回当前 Quantity 的 inf.Dec 表示；如果当前仍是 int64 快速表示，会先原地提升为任意精度表示。
func (q *Quantity) AsDec() *inf.Dec {
	if q.d.Dec != nil {
		return q.d.Dec
	}
	q.d.Dec = q.i.AsDec()
	q.i = int64Amount{}
	return q.d.Dec
}

// AsCanonicalBytes 返回 Quantity 的规范尾数字节和十进制指数；传入 out 可避免额外分配。
func (q *Quantity) AsCanonicalBytes(out []byte) (result []byte, exponent int32) {
	if q.d.Dec != nil {
		return q.d.AsCanonicalBytes(out)
	}
	return q.i.AsCanonicalBytes(out)
}

// IsZero 判断 Quantity 是否为零。
func (q *Quantity) IsZero() bool {
	if q.d.Dec != nil {
		return q.d.Dec.Sign() == 0
	}
	return q.i.value == 0
}

// Sign 在数量为零、小于零、大于零时分别返回 0、-1、1。
func (q *Quantity) Sign() int {
	if q.d.Dec != nil {
		return q.d.Dec.Sign()
	}
	return q.i.Sign()
}

// AsScale 返回调整到指定 scale 并向上取整后的值；损失精度时返回 false。
func (q *Quantity) AsScale(scale Scale) (CanonicalValue, bool) {
	if q.d.Dec != nil {
		return q.d.AsScale(scale)
	}
	return q.i.AsScale(scale)
}

// RoundUp 将 Quantity 调整到指定 Scale，按远离零的方向取整，并更新内部数值与字符串缓存。
func (q *Quantity) RoundUp(scale Scale) bool {
	if q.d.Dec != nil {
		// 修改后字符串缓存已失效。
		q.s = ""
		d, exact := q.d.AsScale(scale)
		q.d = d
		return exact
	}
	// 已计算字符串且无需调整时避免清空缓存。
	if q.i.scale >= scale {
		return true
	}
	q.s = ""
	i, exact := q.i.AsScale(scale)
	q.i = i
	return exact
}

// Add 将 y 加到当前 Quantity 上。优先尝试 int64 快速路径；若任一操作数需要任意精度或发生溢出，则回退到 inf.Dec。
func (q *Quantity) Add(y Quantity) {
	// 任何算术运算都会使字符串缓存失效。
	q.s = ""
	if q.d.Dec == nil && y.d.Dec == nil {
		// 快速路径：双方均可按各自 scale 放入 int64。
		if q.i.value == 0 {
			q.Format = y.Format
		}
		if q.i.Add(y.i) {
			return
		}
	} else if q.IsZero() {
		q.Format = y.Format
	}
	// 任一操作数需要 inf.Dec，或 int64 加法溢出。
	q.ToDec().d.Dec.Add(q.d.Dec, y.AsDec())
}

// Sub 在原地从当前 Quantity 中减去 y；优先使用 int64 精确运算，无法完成时回退到 inf.Dec。
func (q *Quantity) Sub(y Quantity) {
	q.s = ""
	if q.IsZero() {
		q.Format = y.Format
	}
	// 仅当双方均为 int64 且减法精确时走快速路径。
	if q.d.Dec == nil && y.d.Dec == nil && q.i.Sub(y.i) {
		return
	}
	q.ToDec().d.Dec.Sub(q.d.Dec, y.AsDec())
}

// Mul 将当前 Quantity 乘以 y；返回值表示结果是否精确。优先使用 int64 快速路径，必要时提升为 inf.Dec。
func (q *Quantity) Mul(y int64) bool {
	q.s = ""
	// 乘积可放入 int64 时，快速路径避免提升表示。
	if q.d.Dec == nil && q.i.Mul(y) {
		return true
	}
	// 通过检查乘法后的未缩放乘积是否仍可放入 int64 来判断结果是否精确。
	return q.ToDec().d.Dec.Mul(q.d.Dec, inf.NewDec(y, inf.Scale(0))).UnscaledBig().IsInt64()
}

// Cmp 比较两个 Quantity：相等返回 0，小于返回 -1，大于返回 1。双方均为 int64 表示时走快速路径，否则使用任意精度比较。
func (q *Quantity) Cmp(y Quantity) int {
	// 仅当双方均未提升表示时在 int64 空间比较。
	if q.d.Dec == nil && y.d.Dec == nil {
		return q.i.Cmp(y.i)
	}
	return q.AsDec().Cmp(y.AsDec())
}

// CmpInt64 在数量等于、小于、大于 y 时分别返回 0、-1、1。
func (q *Quantity) CmpInt64(y int64) int {
	if q.d.Dec != nil {
		return q.d.Dec.Cmp(inf.NewDec(y, inf.Scale(0)))
	}
	return q.i.Cmp(int64Amount{value: y})
}

// Neg 将 Quantity 原地取负。
func (q *Quantity) Neg() {
	q.s = ""
	if q.d.Dec == nil {
		q.i.value = -q.i.value
		return
	}
	q.d.Dec.Neg(q.d.Dec)
}

// Equal 判断两个 Quantity 是否相等，便于配合 cmp.Equal 测试。
func (q Quantity) Equal(v Quantity) bool {
	return q.Cmp(v) == 0
}

// int64QuantityExpectedBytes 是大多数 Quantity 规范字符串的预期字节宽度。
const int64QuantityExpectedBytes = 18

// String 返回 Quantity 的规范化字符串，并缓存计算结果；任何修改数值的操作都必须清除此缓存。
func (q *Quantity) String() string {
	if q == nil {
		return "<nil>"
	}
	// 计算后以缓存为准；所有修改操作都必须清空缓存。
	if len(q.s) == 0 {
		result := make([]byte, 0, int64QuantityExpectedBytes)
		number, suffix := q.CanonicalizeBytes(result)
		number = append(number, suffix...)
		q.s = string(number)
	}
	return q.s
}

// MarshalJSON 实现 json.Marshaller 接口。
func (q Quantity) MarshalJSON() ([]byte, error) {
	// 快速路径：只需给缓存的规范字符串加引号。
	if len(q.s) > 0 {
		out := make([]byte, len(q.s)+2)
		out[0], out[len(out)-1] = '"', '"'
		copy(out[1:], q.s)
		return out, nil
	}
	// 在预留引号空间的定长缓冲区中渲染；18 字节可覆盖大多数数量，无需二次分配。
	result := make([]byte, int64QuantityExpectedBytes)
	result[0] = '"'
	number, suffix := q.CanonicalizeBytes(result[1:1])
	// 若返回的就是传入切片，将 number 复制回原切片并返回，避免再次分配。
	if len(number) > 0 && &number[0] == &result[1] && (len(number)+len(suffix)+2) <= int64QuantityExpectedBytes {
		number = append(number, suffix...)
		number = append(number, '"')
		return result[:1+len(number)], nil
	}
	// 若 CanonicalizeBytes 所需空间超过切片容量，需要重新分配，因此使用 append。
	result = result[:1]
	result = append(result, number...)
	result = append(result, suffix...)
	result = append(result, '"')
	return result, nil
}

func (q Quantity) MarshalCBOR() ([]byte, error) {
	// 值接收者地址不会为 nil，String() 不会返回 "<nil>"。
	// CBOR 与 JSON 一样，将数量编码为规范字符串。
	return cbor.Marshal(q.String())
}

// ToUnstructured 实现 value.UnstructuredConverter 接口。
func (q Quantity) ToUnstructured() interface{} {
	return q.String()
}

// UnmarshalJSON 从 JSON 字符串或兼容的未加引号数值中解析 Quantity；null 会将接收者重置为零值。
func (q *Quantity) UnmarshalJSON(value []byte) error {
	l := len(value)
	// "null" 将接收者重置为零值，而非返回错误。
	if l == 4 && bytes.Equal(value, []byte("null")) {
		q.d.Dec = nil
		q.i = int64Amount{}
		return nil
	}
	// Quantity 通常带引号，但也兼容未加引号的数字。
	if l >= 2 && value[0] == '"' && value[l-1] == '"' {
		value = value[1 : l-1]
	}

	// TrimSpace 刻意保持宽松，以兼容历史数据。
	parsed, err := ParseQuantity(strings.TrimSpace(string(value)))
	if err != nil {
		return err
	}

	// 此处复制安全，因为之后不再引用 parsed。
	*q = parsed
	return nil
}

func (q *Quantity) UnmarshalCBOR(value []byte) error {
	// CBOR 数量编码为字符串；nil 字符串对应 CBOR null。
	var s *string
	if err := cbor.Unmarshal(value, &s); err != nil {
		return err
	}

	if s == nil {
		q.d.Dec = nil
		q.i = int64Amount{}
		return nil
	}

	parsed, err := ParseQuantity(strings.TrimSpace(*s))
	if err != nil {
		return err
	}

	*q = parsed
	return nil
}

// NewDecimalQuantity 按指定格式创建表示给定 inf.Dec 值的 Quantity。
func NewDecimalQuantity(b inf.Dec, format Format) *Quantity {
	return &Quantity{
		d:      infDecAmount{&b},
		Format: format,
	}
}

// NewQuantity 按指定格式创建表示给定 int64 值的 Quantity。
func NewQuantity(value int64, format Format) *Quantity {
	return &Quantity{
		i:      int64Amount{value: value},
		Format: format,
	}
}

// NewMilliQuantity 按指定格式创建 value * 1/1000 的 Quantity。
// BinarySI 会取整小数；当 (-1 < x < 1) && (x != 0) 时改用 DecimalSI。
func NewMilliQuantity(value int64, format Format) *Quantity {
	return &Quantity{
		i:      int64Amount{value: value, scale: -3},
		Format: format,
	}
}

// NewScaledQuantity 以 DecimalSI 格式创建 value * 10^scale 的 Quantity。
func NewScaledQuantity(value int64, scale Scale) *Quantity {
	return &Quantity{
		i:      int64Amount{value: value, scale: scale},
		Format: DecimalSI,
	}
}

// Value 返回 q 按远离零方向取整到整数后的未缩放值。
func (q *Quantity) Value() int64 {
	return q.ScaledValue(0)
}

// MilliValue 返回 ceil(q * 1000)，可能溢出 int64；必要时先调用 Value() 确认数量足够小。
func (q *Quantity) MilliValue() int64 {
	return q.ScaledValue(Milli)
}

// ScaledValue 返回向上取整后的 q / 10^scale。该结果可能超出 int64 范围，调用方需要结合业务范围判断。
func (q *Quantity) ScaledValue(scale Scale) int64 {
	// Quantity 常用 scale 下，int64 路径可精确计算。
	if q.d.Dec == nil {
		i, _ := q.i.AsScaledInt64(scale)
		return i
	}
	dec := q.d.Dec
	return scaledValue(dec.UnscaledBig(), int(dec.Scale()), int(scale.infScale()))
}

// Set 将 q 设置为 value，并清空因值变化而失效的字符串缓存。
func (q *Quantity) Set(value int64) {
	q.SetScaled(value, 0)
}

// SetMilli 将 q 设置为 value * 1/1000。
func (q *Quantity) SetMilli(value int64) {
	q.SetScaled(value, Milli)
}

// SetScaled 将 q 设置为 value * 10^scale，丢弃 inf.Dec 表示并恢复 int64 快速形式。
func (q *Quantity) SetScaled(value int64, scale Scale) {
	q.s = ""
	q.d.Dec = nil
	q.i = int64Amount{value: value, scale: scale}
}

// QuantityValue 使 Quantity 可用作命令行参数值。
//
// +protobuf=true
// +protobuf.embed=string
// +protobuf.options.marshal=false
// +protobuf.options.(gogoproto.goproto_stringer)=false
// +k8s:deepcopy-gen=true
// +k8s:openapi-model-package=io.k8s.apimachinery.pkg.api.resource
type QuantityValue struct {
	Quantity
}

// Set 实现 pflag.Value.Set 与 Go flag.Value.Set。
func (q *QuantityValue) Set(s string) error {
	quantity, err := ParseQuantity(s)
	if err != nil {
		return err
	}
	q.Quantity = quantity
	return nil
}

// Type 实现 pflag.Value.Type。
func (q QuantityValue) Type() string {
	return "quantity"
}

// QuantityPtrEqual 比较两个 Quantity 指针；二者均为 nil 或指向相等数量时返回 true。
// 可选数量字段用它区分 nil 与显式零值。
func QuantityPtrEqual(a, b *Quantity) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equal(*b)
}
