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

// MapStringString can be set from the command line with the format `--flag "string=string"`.
// Multiple flag invocations are supported. For example: `--flag "a=foo" --flag "b=bar"`. If this is desired
// to be the only type invocation `NoSplit` should be set to true.
// Multiple comma-separated key-value pairs in a single invocation are supported if `NoSplit`
// is set to false. For example: `--flag "a=foo,b=bar"`.
//
// MapStringString 是 pflag.Value 的实现，把 `k=v` 形式解析进 map。
// 首次 Set 会清空调用方传入的默认值——默认值只用于帮助文本展示，
// 一旦用户显式指定就以用户输入为准。
type MapStringString struct {
	Map         *map[string]string
	initialized bool
	NoSplit     bool
}

// NewMapStringString takes a pointer to a map[string]string and returns the
// MapStringString flag parsing shim for that map
//
// NewMapStringString 允许单次调用中包含逗号分隔的多个键值对。
func NewMapStringString(m *map[string]string) *MapStringString {
	return &MapStringString{Map: m}
}

// NewMapStringString takes a pointer to a map[string]string and sets `NoSplit`
// value to `true` and returns the MapStringString flag parsing shim for that map
//
// NewMapStringStringNoSplit 禁用逗号切分：值本身可以包含逗号，
// 适合 path、命令行等原样透传的场景。
func NewMapStringStringNoSplit(m *map[string]string) *MapStringString {
	return &MapStringString{
		Map:     m,
		NoSplit: true,
	}
}

// String implements github.com/spf13/pflag.Value
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

// Set implements github.com/spf13/pflag.Value
//
// Set 解析一次标志值；Map 为 nil 时立即报错，避免调用方忘记初始化。
func (m *MapStringString) Set(value string) error {
	if m.Map == nil {
		return fmt.Errorf("no target (nil pointer to map[string]string)")
	}
	if !m.initialized || *m.Map == nil {
		// clear default values, or allocate if no existing map
		*m.Map = make(map[string]string)
		m.initialized = true
	}

	// account for comma-separated key-value pairs in a single invocation
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

	// account for only one key-value pair in a single invocation
	arr := strings.SplitN(value, "=", 2)
	if len(arr) != 2 {
		return fmt.Errorf("malformed pair, expect string=string")
	}
	k := strings.TrimSpace(arr[0])
	v := strings.TrimSpace(arr[1])
	(*m.Map)[k] = v
	return nil

}

// Type implements github.com/spf13/pflag.Value
func (*MapStringString) Type() string {
	return "mapStringString"
}

// Empty implements OmitEmpty
//
// Empty 用于 pflag 判断是否应从帮助信息中省略默认值。
func (m *MapStringString) Empty() bool {
	return len(*m.Map) == 0
}
