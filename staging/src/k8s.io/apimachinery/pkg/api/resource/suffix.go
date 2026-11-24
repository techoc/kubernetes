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
	"strconv"
)

// suffix 表示量值字符串末尾的单位后缀，例如 "Ki"、"m"、"e6"。
type suffix string

// suffixer 负责解释和构造后缀。
// interpret 把后缀还原为基数、指数和格式；construct 做反向映射，用于序列化。
type suffixer interface {
	interpret(suffix) (base, exponent int32, fmt Format, ok bool)
	construct(base, exponent int32, fmt Format) (s suffix, ok bool)
	constructBytes(base, exponent int32, fmt Format) (s []byte, ok bool)
}

// quantitySuffixer 覆盖 Quantity 支持的全部三种格式的后缀表，全局只构造一次，
// 因为其中的 map 在初始化后即为只读，可安全被并发共享。
var quantitySuffixer = newSuffixer()

// bePair 是（基数, 指数）二元组，作为后缀表的键。
type bePair struct {
	base, exponent int32
}

// listSuffixer 维护某个格式下后缀与（基数, 指数）的双向映射，
// 字节切片映射用于避免序列化时重复分配。
type listSuffixer struct {
	suffixToBE      map[suffix]bePair
	beToSuffix      map[bePair]suffix
	beToSuffixBytes map[bePair][]byte
}

// addSuffix 登记一个后缀映射；各 map 按需惰性初始化，避免为空格式分配。
func (ls *listSuffixer) addSuffix(s suffix, pair bePair) {
	if ls.suffixToBE == nil {
		ls.suffixToBE = map[suffix]bePair{}
	}
	if ls.beToSuffix == nil {
		ls.beToSuffix = map[bePair]suffix{}
	}
	if ls.beToSuffixBytes == nil {
		ls.beToSuffixBytes = map[bePair][]byte{}
	}
	ls.suffixToBE[s] = pair
	ls.beToSuffix[pair] = s
	ls.beToSuffixBytes[pair] = []byte(s)
}

// lookup 按后缀查（基数, 指数）。
func (ls *listSuffixer) lookup(s suffix) (base, exponent int32, ok bool) {
	pair, ok := ls.suffixToBE[s]
	if !ok {
		return 0, 0, false
	}
	return pair.base, pair.exponent, true
}

// construct 按（基数, 指数）查后缀。
func (ls *listSuffixer) construct(base, exponent int32) (s suffix, ok bool) {
	s, ok = ls.beToSuffix[bePair{base, exponent}]
	return
}

// constructBytes 按（基数, 指数）查后缀的字节形式。
func (ls *listSuffixer) constructBytes(base, exponent int32) (s []byte, ok bool) {
	s, ok = ls.beToSuffixBytes[bePair{base, exponent}]
	return
}

// suffixHandler 分别持有十进制 SI 与二进制 SI 的后缀表。
type suffixHandler struct {
	decSuffixes listSuffixer
	binSuffixes listSuffixer
}

// fastLookup 在完整后缀表之上，为最常见的十进制后缀提供免 map 查询的快路径。
type fastLookup struct {
	*suffixHandler
}

// interpret 对高频十进制后缀做 switch 快查，其余情况回落到通用实现。
func (l fastLookup) interpret(s suffix) (base, exponent int32, format Format, ok bool) {
	switch s {
	case "":
		return 10, 0, DecimalSI, true
	case "n":
		return 10, -9, DecimalSI, true
	case "u":
		return 10, -6, DecimalSI, true
	case "m":
		return 10, -3, DecimalSI, true
	case "k":
		return 10, 3, DecimalSI, true
	case "M":
		return 10, 6, DecimalSI, true
	case "G":
		return 10, 9, DecimalSI, true
	}
	return l.suffixHandler.interpret(s)
}

// newSuffixer 构建后缀表，并返回带快路径的 fastLookup 包装。
func newSuffixer() suffixer {
	sh := &suffixHandler{}

	// 重要：修改本节后必须同步修改 fastLookup 中的高频后缀快查逻辑。
	// 二进制单位固定以 2 为基数，指数即 1024 的幂次。
	sh.binSuffixes.addSuffix("Ki", bePair{2, 10})
	sh.binSuffixes.addSuffix("Mi", bePair{2, 20})
	sh.binSuffixes.addSuffix("Gi", bePair{2, 30})
	sh.binSuffixes.addSuffix("Ti", bePair{2, 40})
	sh.binSuffixes.addSuffix("Pi", bePair{2, 50})
	sh.binSuffixes.addSuffix("Ei", bePair{2, 60})
	// 生成 2 的 0 次幂对应的后缀时不要报错。

	// 空后缀对应 2^0，使值为 1 的二进制量也能正常序列化。
	sh.decSuffixes.addSuffix("", bePair{2, 0})

	sh.decSuffixes.addSuffix("n", bePair{10, -9})
	sh.decSuffixes.addSuffix("u", bePair{10, -6})
	sh.decSuffixes.addSuffix("m", bePair{10, -3})
	sh.decSuffixes.addSuffix("", bePair{10, 0})
	sh.decSuffixes.addSuffix("k", bePair{10, 3})
	sh.decSuffixes.addSuffix("M", bePair{10, 6})
	sh.decSuffixes.addSuffix("G", bePair{10, 9})
	sh.decSuffixes.addSuffix("T", bePair{10, 12})
	sh.decSuffixes.addSuffix("P", bePair{10, 15})
	sh.decSuffixes.addSuffix("E", bePair{10, 18})

	return fastLookup{sh}
}

// construct 按格式与（基数, 指数）生成后缀。
// DecimalExponent 不查表，而是现场拼出 "e<exp>"；指数为 0 时返回空串。
func (sh *suffixHandler) construct(base, exponent int32, fmt Format) (s suffix, ok bool) {
	switch fmt {
	case DecimalSI:
		return sh.decSuffixes.construct(base, exponent)
	case BinarySI:
		return sh.binSuffixes.construct(base, exponent)
	case DecimalExponent:
		if base != 10 {
			return "", false
		}
		if exponent == 0 {
			return "", true
		}
		return suffix("e" + strconv.FormatInt(int64(exponent), 10)), true
	}
	return "", false
}

// constructBytes 与 construct 等价，但直接返回字节，省去 []byte(suffix) 转换。
// 这里预分配 8 字节并复用同一底层数组，避免每次序列化都分配。
func (sh *suffixHandler) constructBytes(base, exponent int32, format Format) (s []byte, ok bool) {
	switch format {
	case DecimalSI:
		return sh.decSuffixes.constructBytes(base, exponent)
	case BinarySI:
		return sh.binSuffixes.constructBytes(base, exponent)
	case DecimalExponent:
		if base != 10 {
			return nil, false
		}
		if exponent == 0 {
			return nil, true
		}
		result := make([]byte, 8)
		result[0] = 'e'
		number := strconv.AppendInt(result[1:1], int64(exponent), 10)
		if &result[1] == &number[0] {
			// AppendInt 直接在预分配缓冲上追加，可原地裁剪。
			return result[:1+len(number)], true
		}
		// 缓冲不够时 AppendInt 会重新分配，需要把 'e' 拼到新切片上。
		result = append(result[:1], number...)
		return result, true
	}
	return nil, false
}

// interpret 把后缀还原为（基数, 指数, 格式）。
// 先查十进制、二进制表，再识别 "e"/"E" 前缀的指数记法。
func (sh *suffixHandler) interpret(suffix suffix) (base, exponent int32, fmt Format, ok bool) {
	// 优先查询十进制和二进制后缀映射表。
	if b, e, ok := sh.decSuffixes.lookup(suffix); ok {
		return b, e, DecimalSI, true
	}
	if b, e, ok := sh.binSuffixes.lookup(suffix); ok {
		return b, e, BinarySI, true
	}

	// 形如 "e12" / "E-3" 的指数后缀，其后部分按十进制整数解析。
	if len(suffix) > 1 && (suffix[0] == 'E' || suffix[0] == 'e') {
		parsed, err := strconv.ParseInt(string(suffix[1:]), 10, 64)
		if err != nil {
			return 0, 0, DecimalExponent, false
		}
		return 10, int32(parsed), DecimalExponent, true
	}

	return 0, 0, DecimalExponent, false
}
