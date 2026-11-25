/*
Copyright 2017 The Kubernetes Authors.

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

package flag

import (
	"fmt"
	"sort"
	"strings"
)

// ColonSeparatedMultimapStringString 从命令行编码解析 map[string][]string：
// 用 ':' 分隔键和值，用 ',' 分隔键值对。同一个键可以重复，值按出现顺序追加。
// 例如：`a:hello,b:again,c:world,b:beautiful` 解析为
// `{"a": ["hello"], "b": ["again", "beautiful"], "c": ["world"]}`。
// 首次调用 Set 会先清空 map，后续调用只追加。因此命令行选项可以覆盖默认值，
// 同时仍可把键值对分散到多次标志解析中。例如：
// `--flag "a:hello" --flag "b:again" --flag "b:beautiful" --flag "c:world"`
// 解析为 `{"a": ["hello"], "b": ["again", "beautiful"], "c": ["world"]}`。
type ColonSeparatedMultimapStringString struct {
	Multimap *map[string][]string
	// initialized 记录是否已发生过一次 Set；首次 Set 会先清空默认值。
	initialized          bool // 首次调用 Set 后设为 true
	allowDefaultEmptyKey bool
}

// NewColonSeparatedMultimapStringString 接收 map[string][]string 指针，
// 返回用于解析该 map 的 ColonSeparatedMultimapStringString。
// 接收指针而非 map 值，使 pflag 在解析时能就地替换整个 map。
func NewColonSeparatedMultimapStringString(m *map[string][]string) *ColonSeparatedMultimapStringString {
	return &ColonSeparatedMultimapStringString{Multimap: m}
}

// NewColonSeparatedMultimapStringStringAllowDefaultEmptyKey 接收 map[string][]string 指针，
// 返回允许缺省空键的 ColonSeparatedMultimapStringString：标志中没有冒号时使用空字符串键。
// 用于那些“无键值”本身有意义的场景：缺省键写成单个值，归入空字符串键。
func NewColonSeparatedMultimapStringStringAllowDefaultEmptyKey(m *map[string][]string) *ColonSeparatedMultimapStringString {
	return &ColonSeparatedMultimapStringString{Multimap: m, allowDefaultEmptyKey: true}
}

// Set 实现 github.com/spf13/pflag.Value。
//
// 首次调用清空默认值，之后追加；这样单次 --flag 覆盖默认，而多次 --flag 又能合并。
func (m *ColonSeparatedMultimapStringString) Set(value string) error {
	// 未绑定 map 时无法解析，尽早报错而不是静默丢弃。
	if m.Multimap == nil {
		return fmt.Errorf("no target (nil pointer to map[string][]string)")
	}
	if !m.initialized || *m.Multimap == nil {
		// 清空默认值；若尚无 map，则分配一个。
		*m.Multimap = make(map[string][]string)
		m.initialized = true
	}
	for _, pair := range strings.Split(value, ",") {
		// 容忍尾部逗号等空项。
		if len(pair) == 0 {
			continue
		}
		kv := strings.SplitN(pair, ":", 2)
		var k, v string
		if m.allowDefaultEmptyKey && len(kv) == 1 {
			// 无冒号：值归入空键，兼容只需值列表的调用方。
			v = strings.TrimSpace(kv[0])
		} else {
			if len(kv) != 2 {
				return fmt.Errorf("malformed pair, expect string:string")
			}
			k = strings.TrimSpace(kv[0])
			v = strings.TrimSpace(kv[1])
		}
		// 同名键的顺序即命令行出现顺序。
		(*m.Multimap)[k] = append((*m.Multimap)[k], v)
	}
	return nil
}

// String 实现 github.com/spf13/pflag.Value。
//
// 输出按键排序、键内保持值顺序，保证同一配置的字符串表示稳定（便于测试与日志比对）。
func (m *ColonSeparatedMultimapStringString) String() string {
	type kv struct {
		k string
		v string
	}
	kvs := make([]kv, 0, len(*m.Multimap))
	for k, vs := range *m.Multimap {
		for i := range vs {
			kvs = append(kvs, kv{k: k, v: vs[i]})
		}
	}
	// 按键稳定排序，同时保留键内值的顺序。
	sort.SliceStable(kvs, func(i, j int) bool {
		return kvs[i].k < kvs[j].k
	})
	pairs := make([]string, 0, len(kvs))
	for i := range kvs {
		pairs = append(pairs, fmt.Sprintf("%s:%s", kvs[i].k, kvs[i].v))
	}
	return strings.Join(pairs, ",")
}

// Type 实现 github.com/spf13/pflag.Value。
// 名称会出现在帮助文本和错误信息中。
func (m *ColonSeparatedMultimapStringString) Type() string {
	return "colonSeparatedMultimapStringString"
}

// Empty 实现 OmitEmpty：空 map 视为未设置，避免默认空值进入序列化输出。
func (m *ColonSeparatedMultimapStringString) Empty() bool {
	return len(*m.Multimap) == 0
}
