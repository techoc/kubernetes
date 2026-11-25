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

// LangleSeparatedMapStringString 可通过 `--flag "string<string"` 从命令行设置。
// 单次调用支持逗号分隔的多个键值对，例如：`--flag "a<foo,b<bar"`；
// 也支持多次调用，例如：`--flag "a<foo" --flag "b<foo"`。
// 它用 "<" 而非 "=" 分隔键值，适用于值中可能包含 "=" 的场合（例如 HTTP 头之类的映射）。
type LangleSeparatedMapStringString struct {
	Map         *map[string]string
	initialized bool // 首次调用 Set 后设为 true
}

// NewLangleSeparatedMapStringString 接收 map[string]string 指针，
// 返回用于解析该 map 的 LangleSeparatedMapStringString。
// 与 MapStringString 一样，首次 Set 会清空调用方提供的默认值。
func NewLangleSeparatedMapStringString(m *map[string]string) *LangleSeparatedMapStringString {
	return &LangleSeparatedMapStringString{Map: m}
}

// String 实现 github.com/spf13/pflag.Value。
//
// 按键排序输出，保证帮助信息与测试结果稳定。
func (m *LangleSeparatedMapStringString) String() string {
	pairs := []string{}
	for k, v := range *m.Map {
		pairs = append(pairs, fmt.Sprintf("%s<%s", k, v))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

// Set 实现 github.com/spf13/pflag.Value。
//
// 支持逗号分隔的多对；每对只按第一个 "<" 切分，值中可以继续包含 "<"。
func (m *LangleSeparatedMapStringString) Set(value string) error {
	if m.Map == nil {
		return fmt.Errorf("no target (nil pointer to map[string]string)")
	}
	if !m.initialized || *m.Map == nil {
		// 清空默认值；若尚无 map，则分配一个。
		*m.Map = make(map[string]string)
		m.initialized = true
	}
	for _, s := range strings.Split(value, ",") {
		// 跳过空片段（例如尾随逗号）。
		if len(s) == 0 {
			continue
		}
		arr := strings.SplitN(s, "<", 2)
		if len(arr) != 2 {
			return fmt.Errorf("malformed pair, expect string<string")
		}
		k := strings.TrimSpace(arr[0])
		v := strings.TrimSpace(arr[1])
		(*m.Map)[k] = v
	}
	return nil
}

// Type 实现 github.com/spf13/pflag.Value。
func (*LangleSeparatedMapStringString) Type() string {
	return "mapStringString"
}

// Empty 实现 OmitEmpty，决定帮助信息是否省略默认值。
func (m *LangleSeparatedMapStringString) Empty() bool {
	return len(*m.Map) == 0
}
