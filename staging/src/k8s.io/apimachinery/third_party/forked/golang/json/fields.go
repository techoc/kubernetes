// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package json is forked from the Go standard library to enable us to find the
// field of a struct that a given JSON key maps to.
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
	// Find the field that the JSON library would use.
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
		// Find the reflect.Value of the most preferential struct field.
		tjf := t.Field(f.index[0])
		// we must navigate down all the anonymously included structs in the chain
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
	// index is the sequence of indexes from the containing type fields to this field.
	// it is a slice because anonymous structs will need multiple navigation steps to correctly
	// resolve the proper fields
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
	// Anonymous fields to explore at the current level and the next.
	current := []field{}
	next := []field{{typ: t}}

	// Count of queued names for current level and the next.
	count := map[reflect.Type]int{}
	nextCount := map[reflect.Type]int{}

	// Types already visited at an earlier level.
	visited := map[reflect.Type]bool{}

	// Fields found.
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

			// Scan f.typ for fields to include.
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
					// Follow pointer.
					ft = ft.Elem()
				}

				// Record found field and index sequence.
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
						// If there were multiple instances, add a second,
						// so that the annihilation code will see a duplicate.
						// It only cares about the distinction between 1 or 2,
						// so don't bother generating any more copies.
						fields = append(fields, fields[len(fields)-1])
					}
					continue
				}

				// Record new anonymous struct to explore in next round.
				nextCount[ft]++
				if nextCount[ft] == 1 {
					next = append(next, fillField(field{name: ft.Name(), index: index, typ: ft}))
				}
			}
		}
	}

	sort.Sort(byName(fields))

	// Delete all fields that are hidden by the Go rules for embedded fields,
	// except that fields with JSON tags are promoted.

	// 字段按名字为主序、index 长度为次序排序。逐个名字循环，选出存活的 dominant 字段
	// 以删除被遮蔽字段；同名冲突时只保留 dominant 字段，排序后同一名字的字段必然相邻。
	out := fields[:0]
	for advance, i := 0, 0; i < len(fields); i += advance {
		// One iteration per name.
		// Find the sequence of fields with the name of this first field.
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

// dominantField looks through the fields, all of which are known to
// have the same name, to find the single field that dominates the
// others using Go's embedding rules, modified by the presence of
// JSON tags. If there are multiple top-level fields, the boolean
// will be false: This condition is an error in Go and we skip all
// the fields.
func dominantField(fields []field) (field, bool) {
	// The fields are sorted in increasing index-length order. The winner
	// must therefore be one with the shortest index length. Drop all
	// longer entries, which is easy: just truncate the slice.
	length := len(fields[0].index)
	tagged := -1 // Index of first tagged field.
	for i, f := range fields {
		if len(f.index) > length {
			fields = fields[:i]
			break
		}
		if f.tag {
			if tagged >= 0 {
				// Multiple tagged fields at the same level: conflict.
				// Return no field.
				return field{}, false
			}
			tagged = i
		}
	}
	if tagged >= 0 {
		return fields[tagged], true
	}
	// All remaining fields have the same length. If there's more than one,
	// we have a conflict (two fields named "X" at the same level) and we
	// return no field.
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

// cachedTypeFields is like typeFields but uses a cache to avoid repeated work.
func cachedTypeFields(t reflect.Type) []field {
	fieldCache.RLock()
	f := fieldCache.m[t]
	fieldCache.RUnlock()
	if f != nil {
		return f
	}

	// Compute fields without lock.
	// Might duplicate effort but won't hold other computations back.
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
			// Backslash and quote chars are reserved, but
			// otherwise any punctuation chars are allowed
			// in a tag name.
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

// foldFunc returns one of four different case folding equivalence
// functions, from most general (and slow) to fastest:
//
// 1) bytes.EqualFold, if the key s contains any non-ASCII UTF-8
// 2) equalFoldRight, if s contains special folding ASCII ('k', 'K', 's', 'S')
// 3) asciiEqualFold, no special, but includes non-letters (including _)
// 4) simpleLetterEqualFold, no specials, no non-letters.
//
// The letters S and K are special because they map to 3 runes, not just 2:
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
			// See above for why these letters are special.
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
		// sb is ASCII and t is not. t must be either kelvin
		// sign or long s; sb must be s, S, k, or K.
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

// asciiEqualFold is a specialization of bytes.EqualFold for use when
// s is all ASCII (but may contain non-letters) and contains no
// special-folding letters.
// See comments on foldFunc.
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

// tagOptions is the string following a comma in a struct field's "json"
// tag, or the empty string. It does not include the leading comma.
type tagOptions string

// parseTag splits a struct field's json tag into its name and
// comma-separated options.
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
