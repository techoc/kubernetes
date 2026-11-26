// Copyright 2013 The Go Authors. All rights reserved.
// 本源码的使用受 BSD 风格许可证约束，详见 LICENSE 文件。

// Package json 从 Go 标准库 fork 而来，用于找出给定 JSON key 映射到结构体的哪个字段。
package json

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

const (
	patchStrategyTagKey = "patchStrategy"
	patchMergeKeyTagKey = "patchMergeKey"
)

// 在给定结构体类型上，按字段的 JSON 名查找其 patchStrategy 与 patchMergeKey 结构体标签，
// 返回字段类型、patch 策略列表、merge key 和错误。
// TODO: 让返回的 error 可内省（introspectable）。
//
// 查找顺序与 encoding/json 一致：优先精确匹配 JSON 名，其次大小写不敏感匹配；
// 字段可能位于匿名字段链中，因此需要沿 index 逐层下钻。
func LookupPatchMetadataForStruct(t reflect.Type, jsonField string) (
	elemType reflect.Type, patchStrategies []string, patchMergeKey string, e error) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		e = fmt.Errorf("merging an object in json but data type is not struct, instead is: %s",
			t.Kind().String())
		return
	}
	jf := []byte(jsonField)
	// 找出 JSON 库实际会使用的字段。
	var f *field
	fields := cachedTypeFields(t)
	for i := range fields {
		ff := &fields[i]
		if bytes.Equal(ff.nameBytes, jf) {
			f = ff
			break
		}
		// 精确匹配失败时记录第一个大小写不敏感匹配，但不立即跳出，以便后续精确匹配优先。
		if f == nil && ff.equalFold(ff.nameBytes, jf) {
			f = ff
		}
	}
	if f != nil {
		// 找出优先级最高的结构体字段对应的 reflect.Value。
		tjf := t.Field(f.index[0])
		// 必须沿着匿名嵌入链逐层向下查找。
		for i := 1; i < len(f.index); i++ {
			tjf = tjf.Type.Field(f.index[i])
		}
		patchStrategy := tjf.Tag.Get(patchStrategyTagKey)
		patchMergeKey = tjf.Tag.Get(patchMergeKeyTagKey)
		patchStrategies = strings.Split(patchStrategy, ",")
		elemType = tjf.Type
		return
	}
	e = fmt.Errorf("unable to find api field in struct %s for the json field %q", t.Name(), jsonField)
	return
}

// field 表示结构体中的单个字段，与 encoding/json 内部的 field 对应，
// 但额外记录了 patch tag 所需的信息。
type field struct {
	name      string
	nameBytes []byte                 // []byte(name)
	equalFold func(s, t []byte) bool // bytes.EqualFold or equivalent

	tag bool
	// index 是从外层类型字段到该字段的索引序列。
	// 之所以用切片，是因为匿名结构体可能需要多步导航才能定位到目标字段。
	index     []int
	typ       reflect.Type
	omitEmpty bool
	quoted    bool
}

// String 便于调试时打印字段信息。
func (f field) String() string {
	return fmt.Sprintf("{name: %s, type: %v, tag: %v, index: %v, omitEmpty: %v, quoted: %v}", f.name, f.typ, f.tag, f.index, f.omitEmpty, f.quoted)
}

// fillField 预计算字段名的字节形式与大小写折叠函数，避免比较时重复转换。
func fillField(f field) field {
	f.nameBytes = []byte(f.name)
	f.equalFold = foldFunc(f.nameBytes)
	return f
}

// byName 按字段名排序，平局时依次以深度、"name came from json tag"、index 顺序打破，
// 排序规则与 encoding/json 一致，是后续"字段遮蔽"消除算法的前提。
type byName []field

func (x byName) Len() int { return len(x) }

func (x byName) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

func (x byName) Less(i, j int) bool {
	if x[i].name != x[j].name {
		return x[i].name < x[j].name
	}
	if len(x[i].index) != len(x[j].index) {
		return len(x[i].index) < len(x[j].index)
	}
	if x[i].tag != x[j].tag {
		return x[i].tag
	}
	return byIndex(x).Less(i, j)
}

// byIndex 按字段的 index 序列（即在结构体中的声明顺序）排序，使最终输出稳定。
type byIndex []field

func (x byIndex) Len() int { return len(x) }

func (x byIndex) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

// Less 逐层比较 index 序列，短序列优先。
func (x byIndex) Less(i, j int) bool {
	for k, xik := range x[i].index {
		if k >= len(x[j].index) {
			return false
		}
		if xik != x[j].index[k] {
			return xik < x[j].index[k]
		}
	}
	return len(x[i].index) < len(x[j].index)
}

// typeFields 返回 JSON 对给定类型应当识别的字段列表。
// 算法是对需纳入的结构体集合做广度优先搜索——先是顶层结构体，
// 再是所有可达的匿名结构体。
// 该实现直接取自 encoding/json，确保字段选择规则与标准库完全一致。
func typeFields(t reflect.Type) []field {
	// 当前层与下一层待遍历的匿名字段。
	current := []field{}
	next := []field{{typ: t}}

	// 当前层与下一层排队等待处理的名称数量。
	count := map[reflect.Type]int{}
	nextCount := map[reflect.Type]int{}

	// 在更早的层级已访问过的类型。
	visited := map[reflect.Type]bool{}

	// 已找到的字段。
	var fields []field

	// 逐层（BFS）展开匿名字段，浅层字段优先于深层字段。
	for len(next) > 0 {
		current, next = next, current[:0]
		count, nextCount = nextCount, map[reflect.Type]int{}

		for _, f := range current {
			if visited[f.typ] {
				continue
			}
			visited[f.typ] = true

			// 扫描 f.typ，找出需要纳入的字段。
			for i := 0; i < f.typ.NumField(); i++ {
				sf := f.typ.Field(i)
				if sf.PkgPath != "" { // unexported
					continue
				}
				tag := sf.Tag.Get("json")
				if tag == "-" {
					continue
				}
				name, opts := parseTag(tag)
				if !isValidTag(name) {
					name = ""
				}
				index := make([]int, len(f.index)+1)
				copy(index, f.index)
				index[len(f.index)] = i

				ft := sf.Type
				if ft.Name() == "" && ft.Kind() == reflect.Pointer {
					// 解引用指针。
					ft = ft.Elem()
				}

				// 记录找到的字段及其索引序列。
				if name != "" || !sf.Anonymous || ft.Kind() != reflect.Struct {
					tagged := name != ""
					if name == "" {
						name = sf.Name
					}
					fields = append(fields, fillField(field{
						name:      name,
						tag:       tagged,
						index:     index,
						typ:       ft,
						omitEmpty: opts.Contains("omitempty"),
						quoted:    opts.Contains("string"),
					}))
					if count[f.typ] > 1 {
						// 出现多个实例时再添加一个，让去重逻辑能识别出重复。
						// 该逻辑只区分 1 个和 2 个，因此无需生成更多副本。
						fields = append(fields, fields[len(fields)-1])
					}
					continue
				}

				// 记录下一轮要遍历的新匿名结构体。
				nextCount[ft]++
				if nextCount[ft] == 1 {
					next = append(next, fillField(field{name: ft.Name(), index: index, typ: ft}))
				}
			}
		}
	}

	sort.Sort(byName(fields))

	// 删除按 Go 嵌入字段规则被隐藏的字段，但带 JSON tag 的字段例外，仍需提升。

	// 字段按名字为主序、index 长度为次序排序。逐个名字循环，选出存活的 dominant 字段
	// 以删除被遮蔽字段；同名冲突时只保留 dominant 字段，排序后同一名字的字段必然相邻。
	out := fields[:0]
	for advance, i := 0, 0; i < len(fields); i += advance {
		// 每个名称处理一轮。
		// 找出与该字段同名的字段序列。
		fi := fields[i]
		name := fi.name
		for advance = 1; i+advance < len(fields); advance++ {
			fj := fields[i+advance]
			if fj.name != name {
				break
			}
		}
		if advance == 1 { // Only one field with this name
			out = append(out, fi)
			continue
		}
		dominant, ok := dominantField(fields[i : i+advance])
		if ok {
			out = append(out, dominant)
		}
	}

	fields = out
	sort.Sort(byIndex(fields))

	return fields
}

// dominantField 在一组同名字段中，按 Go 嵌入规则（并考虑 JSON tag 的影响）
// 找出唯一占主导地位的字段。若存在多个顶层同名字段，布尔返回值为 false：
// 这种情况在 Go 中是错误，这里选择跳过所有字段。
func dominantField(fields []field) (field, bool) {
	// 字段按索引长度递增排序，因此胜出者必然索引最短。
	// 丢掉更长的项即可，直接截断切片。
	length := len(fields[0].index)
	tagged := -1 // Index of first tagged field.
	for i, f := range fields {
		if len(f.index) > length {
			fields = fields[:i]
			break
		}
		if f.tag {
			if tagged >= 0 {
				// 同一层出现多个带 tag 的字段：冲突，不返回任何字段。
				return field{}, false
			}
			tagged = i
		}
	}
	if tagged >= 0 {
		return fields[tagged], true
	}
	// 剩余字段索引长度相同。若不止一个，说明存在冲突
	// （同一层有两个名为 "X" 的字段），此时不返回任何字段。
	if len(fields) > 1 {
		return field{}, false
	}
	return fields[0], true
}

// fieldCache 缓存每个类型计算出的字段列表。
// 字段解析只依赖类型本身，因此可以全局缓存；计算过程不持锁，允许并发重复计算。
var fieldCache struct {
	sync.RWMutex
	m map[reflect.Type][]field
}

// cachedTypeFields 与 typeFields 类似，但使用缓存避免重复计算。
func cachedTypeFields(t reflect.Type) []field {
	fieldCache.RLock()
	f := fieldCache.m[t]
	fieldCache.RUnlock()
	if f != nil {
		return f
	}

	// 不加锁计算字段：可能重复劳动，但不会阻塞其它计算。
	f = typeFields(t)
	if f == nil {
		// 用空切片而非 nil 表示"已缓存"，否则下次仍会重复计算。
		f = []field{}
	}

	fieldCache.Lock()
	if fieldCache.m == nil {
		fieldCache.m = map[reflect.Type][]field{}
	}
	fieldCache.m[t] = f
	fieldCache.Unlock()
	return f
}

// isValidTag 判断 JSON tag 中的名字是否合法：空名不合法，反斜杠与引号保留，
// 其余标点与字母数字允许。
func isValidTag(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case strings.ContainsRune("!#$%&()*+-./:<=>?@[]^_{|}~ ", c):
			// 反斜杠和引号是保留字符，tag 名称中其它标点均允许。
		default:
			if !unicode.IsLetter(c) && !unicode.IsDigit(c) {
				return false
			}
		}
	}
	return true
}

const (
	caseMask     = ^byte(0x20) // Mask to ignore case in ASCII.
	kelvin       = '\u212a'
	smallLongEss = '\u017f'
)

// foldFunc 返回四种大小写折叠等价函数之一，按通用程度递减、速度递增排列：
//
// 1) key s 含非 ASCII UTF-8 时使用 bytes.EqualFold
// 2) s 含特殊折叠 ASCII 字符（'k'、'K'、's'、'S'）时使用 equalFoldRight
// 3) 无特殊字符但含非字母（含 _）时使用 asciiEqualFold
// 4) 既无特殊字符也无非字母时使用 simpleLetterEqualFold。
//
// 字母 S 和 K 特殊，因为它们映射到 3 个 rune 而非 2 个：
//   - S maps to s and to U+017F 'ſ' Latin small letter long s
//   - k maps to K and to U+212A 'K' Kelvin sign
//
// See http://play.golang.org/p/tTxjOc0OGo
//
// 根据 s 的字符特征选择最廉价的大小写折叠实现；返回的函数针对与 s 的匹配做了特化，
// 只应传入 s；不做柯里化（curry）是出于性能考虑。
func foldFunc(s []byte) func(s, t []byte) bool {
	nonLetter := false
	special := false // special letter
	for _, b := range s {
		if b >= utf8.RuneSelf {
			return bytes.EqualFold
		}
		upper := b & caseMask
		if upper < 'A' || upper > 'Z' {
			nonLetter = true
		} else if upper == 'K' || upper == 'S' {
			// 这些字母为何特殊参见上文。
			special = true
		}
	}
	if special {
		return equalFoldRight
	}
	if nonLetter {
		return asciiEqualFold
	}
	return simpleLetterEqualFold
}

// equalFoldRight 是 bytes.EqualFold 的特化版本：已知 s 全为 ASCII（含标点符号），
// 但含有 's'、'S'、'k' 或 'K'，需要对 t 中的字节做 Unicode 折叠。见 foldFunc 的注释。
// 因为 s 含特殊字母，t 中对应位置可能是开尔文符号或长 s，需要逐 rune 比较。
func equalFoldRight(s, t []byte) bool {
	for _, sb := range s {
		if len(t) == 0 {
			return false
		}
		tb := t[0]
		if tb < utf8.RuneSelf {
			if sb != tb {
				sbUpper := sb & caseMask
				if 'A' <= sbUpper && sbUpper <= 'Z' {
					if sbUpper != tb&caseMask {
						return false
					}
				} else {
					return false
				}
			}
			t = t[1:]
			continue
		}
		// sb 是 ASCII 而 t 不是。t 只能是开尔文符号或长 s；
		// sb 只能是 s、S、k 或 K。
		tr, size := utf8.DecodeRune(t)
		switch sb {
		case 's', 'S':
			if tr != smallLongEss {
				return false
			}
		case 'k', 'K':
			if tr != kelvin {
				return false
			}
		default:
			return false
		}
		t = t[size:]

	}
	if len(t) > 0 {
		return false
	}
	return true
}

// asciiEqualFold 是 bytes.EqualFold 的特化版本，适用于 s 全为 ASCII
// （可含非字母）且不含特殊折叠字母的情况。说明见 foldFunc 的注释。
func asciiEqualFold(s, t []byte) bool {
	if len(s) != len(t) {
		return false
	}
	for i, sb := range s {
		tb := t[i]
		if sb == tb {
			continue
		}
		if ('a' <= sb && sb <= 'z') || ('A' <= sb && sb <= 'Z') {
			if sb&caseMask != tb&caseMask {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// simpleLetterEqualFold 是 bytes.EqualFold 的特化版本：s 全为 ASCII 字母
// （无下划线等），且不含 'k'、'K'、's'、'S'。见 foldFunc 的注释。
// 这是最快的路径：长度相等且逐字节只比较大小写位。
func simpleLetterEqualFold(s, t []byte) bool {
	if len(s) != len(t) {
		return false
	}
	for i, b := range s {
		if b&caseMask != t[i]&caseMask {
			return false
		}
	}
	return true
}

// tagOptions 是结构体字段 "json" tag 中逗号后的部分，没有则为字符串。
// 不包含前导逗号。
type tagOptions string

// parseTag 把结构体字段的 json tag 拆成名称与逗号分隔的选项。
func parseTag(tag string) (string, tagOptions) {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx], tagOptions(tag[idx+1:])
	}
	return tag, tagOptions("")
}

// Contains 判断逗号分隔的选项列表中是否包含某个 substr 标志；
// substr 两侧必须是字符串边界或逗号。
// 逐项比较而非子串匹配，避免 "omitemptyx" 被误判为包含 "omitempty"。
func (o tagOptions) Contains(optionName string) bool {
	if len(o) == 0 {
		return false
	}
	s := string(o)
	for s != "" {
		var next string
		i := strings.Index(s, ",")
		if i >= 0 {
			s, next = s[:i], s[i+1:]
		}
		if s == optionName {
			return true
		}
		s = next
	}
	return false
}
