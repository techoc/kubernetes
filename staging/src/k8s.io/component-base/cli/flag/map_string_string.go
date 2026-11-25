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

// MapStringString 可通过 `--flag "string=string"` 从命令行设置，把 `k=v` 解析进 map。
// `NoSplit` 为 false 时，单次调用支持逗号分隔的多个键值对，
// 例如：`--flag "a=foo,b=bar"`；为 true 时只接受一对，并支持多次调用，
// 例如：`--flag "a=foo" --flag "b=bar"`。
// 首次 Set 会清空调用方传入的默认值——默认值只用于帮助文本展示，
// 一旦用户显式指定就以用户输入为准。
type MapStringString struct {
	Map         *map[string]string
	initialized bool
	NoSplit     bool
}

// NewMapStringString 接收 map[string]string 指针，返回用于解析该 map 的 MapStringString。
// 单次调用允许包含逗号分隔的多个键值对。
func NewMapStringString(m *map[string]string) *MapStringString {
	return &MapStringString{Map: m}
}

// NewMapStringStringNoSplit 接收 map[string]string 指针，将 `NoSplit` 设为 true，
// 并返回用于解析该 map 的 MapStringString。禁用逗号切分后，值本身可以包含逗号，
// 适合 path、命令行等原样透传的场景。
func NewMapStringStringNoSplit(m *map[string]string) *MapStringString {
	return &MapStringString{
		Map:     m,
		NoSplit: true,
	}
}

// String 实现 github.com/spf13/pflag.Value。
//
// 输出按键排序，保证同一 map 的字符串表示稳定（便于测试与帮助信息去抖动）。
func (m *MapStringString) String() string {
	if m == nil || m.Map == nil {
		return ""
	}
	pairs := []string{}
	for k, v := range *m.Map {
		pairs = append(pairs, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

// Set 实现 github.com/spf13/pflag.Value。
//
// Set 解析一次标志值；Map 为 nil 时立即报错，避免调用方忘记初始化。
func (m *MapStringString) Set(value string) error {
	if m.Map == nil {
		return fmt.Errorf("no target (nil pointer to map[string]string)")
	}
	if !m.initialized || *m.Map == nil {
		// 清空默认值；若尚无 map，则分配一个。
		*m.Map = make(map[string]string)
		m.initialized = true
	}

	// 处理单次调用中逗号分隔的多个键值对。
	if !m.NoSplit {
		for _, s := range strings.Split(value, ",") {
			// 允许尾随逗号等空片段，直接跳过。
			if len(s) == 0 {
				continue
			}
			// 只按第一个 "=" 切分，值中允许出现 "="。
			arr := strings.SplitN(s, "=", 2)
			if len(arr) != 2 {
				return fmt.Errorf("malformed pair, expect string=string")
			}
			k := strings.TrimSpace(arr[0])
			v := strings.TrimSpace(arr[1])
			(*m.Map)[k] = v
		}
		return nil
	}

	// 禁用切分时，单次调用只处理一个键值对。
	arr := strings.SplitN(value, "=", 2)
	if len(arr) != 2 {
		return fmt.Errorf("malformed pair, expect string=string")
	}
	k := strings.TrimSpace(arr[0])
	v := strings.TrimSpace(arr[1])
	(*m.Map)[k] = v
	return nil

}

// Type 实现 github.com/spf13/pflag.Value。
func (*MapStringString) Type() string {
	return "mapStringString"
}

// Empty 实现 OmitEmpty，用于 pflag 判断是否应从帮助信息中省略默认值。
func (m *MapStringString) Empty() bool {
	return len(*m.Map) == 0
}
