// 本包复制自 Go 标准库 reflect/type.go。标准库只支持按名查询单个 struct tag，
// 无法列出标签全集，这里补齐这一能力。
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

// ParseStructTags 按 struct tag 中出现的顺序返回全部字段。
// 与标准库 reflect.StructTag.Lookup 不同，这里保留顺序并可枚举全部 tag；
// 解析规则刻意与 reflect 一致，遇到非法片段就停止（而非报错），最后只返回已解析部分。
func ParseStructTags(tag string) (StructTags, error) {
	tags := StructTags{}
	for tag != "" {
		// 跳过前导空格。
		i := 0
		for i < len(tag) && tag[i] == ' ' {
			i++
		}
		tag = tag[i:]
		if tag == "" {
			break
		}

		// 扫描到冒号为止；空格、引号或控制字符都是语法错误。
		// 严格来说控制字符还包括 [0x7f, 0x9f]，但按字节而非 rune 解析更简单，
		// 因此实际忽略多字节控制字符。
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

		// 扫描带引号的字符串以取出值。
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
