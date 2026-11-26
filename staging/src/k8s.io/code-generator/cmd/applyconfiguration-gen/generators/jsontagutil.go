/*
Copyright 2021 The Kubernetes Authors.

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

package generators

import (
	"reflect"
	"strings"

	"k8s.io/gengo/v2/types"
)

// TODO: 功能与 https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apimachinery/pkg/runtime/converter.go#L236 相同，
// 但采用 https://golang.org/src/encoding/json/encode.go 中更高效的解析方式。

// JSONTags 表示 Go 结构体的 json 字段标签。
// 生成器只关心影响 apply configuration 序列化的部分：字段名、内联和 omitempty；
// omit 字段保留用于将来支持 "-"，目前 lookupJSONTags 已直接返回 false 剔除。
type JSONTags struct {
	name      string // JSON 字段名；内联时为生成器合成的名字
	omit      bool   // json:"-"
	inline    bool   // 匿名嵌入且无显式 tag 名
	omitempty bool   // 序列化时零值省略
}

// String 渲染 json tag 的值部分（不含引号），与 encoding/json 的语法一致。
func (t JSONTags) String() string {
	var tag string
	if !t.inline {
		tag += t.name
	}
	if t.omitempty {
		tag += ",omitempty"
	}
	return tag
}

// lookupJSONTags 解析成员的 json tag；无 tag 或显式 "-" 表示该字段不参与生成。
// 无名标签的匿名嵌入视为 inline，此时字段名沿用嵌入类型自身的展开字段，
// 因此 inner 名字留空，无需伪造。
func lookupJSONTags(m types.Member) (JSONTags, bool) {
	tag, exists := reflect.StructTag(m.Tags).Lookup("json")
	if !exists || tag == "-" {
		return JSONTags{}, false
	}
	name, opts := parseTag(tag)
	inline := m.Embedded && name == ""
	if name == "" {
		name = m.Name
	}
	return JSONTags{
		name:      name,
		omit:      false,
		inline:    inline,
		omitempty: opts.Contains("omitempty"),
	}, true
}

// tagOptions 保存 json tag 中逗号之后的部分，例如 "omitempty,string"。
type tagOptions string

// parseTag 把 json tag 拆成字段名与逗号分隔的选项列表。
func parseTag(tag string) (string, tagOptions) {
	if idx := strings.Index(tag, ","); idx != -1 {
		return tag[:idx], tagOptions(tag[idx+1:])
	}
	return tag, ""
}

// Contains 报告逗号分隔的选项列表中是否含指定选项名。
// 选项名必须位于字符串边界或逗号之间，避免前缀误匹配。
// 逐段扫描而非 split，避免为短小的 tag 分配切片。
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
