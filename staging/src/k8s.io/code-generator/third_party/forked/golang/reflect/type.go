// This package is copied from Go library reflect/type.go.
// The struct tag library provides no way to extract the list of struct tags, only
// a specific tag
package reflect

import (
	"fmt"

	"strconv"
	"strings"
)

// StructTag 表示结构体 tag 中的一个 key:"value" 对。
type StructTag struct {
	Name  string
	Value string
}

// String 还原为 key:"value" 形式（Value 使用 Go 引号转义）。
func (t StructTag) String() string {
	return fmt.Sprintf("%s:%q", t.Name, t.Value)
}

// StructTags 按源码中出现的顺序保存全部 tag。
type StructTags []StructTag

// String 还原整个反引号包围的 tag 字面量。
func (tags StructTags) String() string {
	s := make([]string, 0, len(tags))
	for _, tag := range tags {
		s = append(s, tag.String())
	}
	return "`" + strings.Join(s, " ") + "`"
}

// Has 判断是否存在指定名字的 tag（只比较名字，不比较值）。
func (tags StructTags) Has(name string) bool {
	for i := range tags {
		if tags[i].Name == name {
			return true
		}
	}
	return false
}

// ParseStructTags returns the full set of fields in a struct tag in the order they appear in
// the struct tag.
// 与标准库 reflect.StructTag.Lookup 不同，这里保留顺序并可枚举全部 tag；
// 解析规则刻意与 reflect 一致，遇到非法片段就停止（而非报错），最后只返回已解析部分。
func ParseStructTags(tag string) (StructTags, error) {
	tags := StructTags{}
	for tag != "" {
		// Skip leading space.
		i := 0
		for i < len(tag) && tag[i] == ' ' {
			i++
		}
		tag = tag[i:]
		if tag == "" {
			break
		}

		// Scan to colon. A space, a quote or a control character is a syntax error.
		// Strictly speaking, control chars include the range [0x7f, 0x9f], not just
		// [0x00, 0x1f], but in practice, we ignore the multi-byte control characters
		// as it is simpler to inspect the tag's bytes than the tag's runes.
		// 查找 name 结束的冒号；不满足 key:"value" 结构时直接停止解析。
		i = 0
		for i < len(tag) && tag[i] > ' ' && tag[i] != ':' && tag[i] != '"' && tag[i] != 0x7f {
			i++
		}
		if i == 0 || i+1 >= len(tag) || tag[i] != ':' || tag[i+1] != '"' {
			break
		}
		name := string(tag[:i])
		tag = tag[i+1:]

		// Scan quoted string to find value.
		i = 1
		for i < len(tag) && tag[i] != '"' {
			if tag[i] == '\\' {
				i++
			}
			i++
		}
		if i >= len(tag) {
			break
		}
		qvalue := string(tag[:i+1])
		tag = tag[i+1:]

		value, err := strconv.Unquote(qvalue)
		if err != nil {
			return nil, err
		}
		tags = append(tags, StructTag{Name: name, Value: value})
	}
	return tags, nil
}
