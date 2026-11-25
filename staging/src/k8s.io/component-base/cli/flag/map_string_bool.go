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
	"strconv"
	"strings"
)

// MapStringBool 可通过 `--flag "string=bool"` 从命令行设置。
// 单次调用支持逗号分隔的多个键值对，例如：`--flag "a=true,b=false"`；
// 也支持多次调用，例如：`--flag "a=true" --flag "b=false"`。
type MapStringBool struct {
	Map         *map[string]bool
	initialized bool
}

// NewMapStringBool 接收 map[string]bool 指针，返回用于解析该 map 的 MapStringBool。
func NewMapStringBool(m *map[string]bool) *MapStringBool {
	return &MapStringBool{Map: m}
}

// String 实现 github.com/spf13/pflag.Value。
// String 以排序后的 key=value 形式输出，保证同一 map 的 String 结果稳定，
// 便于测试与 flag 默认值展示。
func (m *MapStringBool) String() string {
	if m == nil || m.Map == nil {
		return ""
	}
	pairs := []string{}
	for k, v := range *m.Map {
		pairs = append(pairs, fmt.Sprintf("%s=%t", k, v))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

// Set 实现 github.com/spf13/pflag.Value。
// Set 支持逗号分隔的多对 key=value；首次调用会清空默认值。
func (m *MapStringBool) Set(value string) error {
	if m.Map == nil {
		return fmt.Errorf("no target (nil pointer to map[string]bool)")
	}
	if !m.initialized || *m.Map == nil {
		// 首次显式设置时清空默认值；若尚无 map，则分配一个。后续调用保留累积的键值。
		*m.Map = make(map[string]bool)
		m.initialized = true
	}
	for _, s := range strings.Split(value, ",") {
		// 容忍尾随逗号等空项。
		if len(s) == 0 {
			continue
		}
		arr := strings.SplitN(s, "=", 2)
		if len(arr) != 2 {
			return fmt.Errorf("malformed pair, expect string=bool")
		}
		k := strings.TrimSpace(arr[0])
		v := strings.TrimSpace(arr[1])
		boolValue, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid value of %s: %s, err: %v", k, v, err)
		}
		(*m.Map)[k] = boolValue
	}
	return nil
}

// Type 实现 github.com/spf13/pflag.Value。
func (*MapStringBool) Type() string {
	return "mapStringBool"
}

// Empty 实现 OmitEmpty，报告 map 是否为空，供 pflag 决定是否在 usage 中省略零值。
func (m *MapStringBool) Empty() bool {
	return len(*m.Map) == 0
}
