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

package flag

import (
	goflag "flag"
	"fmt"
	"strings"

	"github.com/spf13/pflag"
)

// StringSlice 实现 goflag.Value 和 pflag.Value，允许重复调用 Set 累积值。
// 首次 Set 会清空初始值，之后每次追加；这使得“默认值 + 多次命令行参数”语义可预期。
type StringSlice struct {
	value   *[]string
	changed bool
}

// NewStringSlice 返回绑定到 s 的标志值；写入发生在 Set 调用时，而非构造时。
func NewStringSlice(s *[]string) *StringSlice {
	return &StringSlice{value: s}
}

var _ goflag.Value = &StringSlice{}
var _ pflag.Value = &StringSlice{}

// String 以空格拼接当前值，仅用于展示和 Usage 输出，不保证可反向解析。
func (s *StringSlice) String() string {
	if s == nil || s.value == nil {
		return ""
	}
	return strings.Join(*s.value, " ")
}

// Set 追加一个元素；首次调用时丢弃目标切片的原有内容，
// 避免默认值混入用户显式指定的结果。
func (s *StringSlice) Set(val string) error {
	if s.value == nil {
		return fmt.Errorf("no target (nil pointer to []string)")
	}
	if !s.changed {
		*s.value = make([]string, 0)
	}
	*s.value = append(*s.value, val)
	s.changed = true
	return nil
}

// Type 返回 pflag 中显示的类型名。
func (StringSlice) Type() string {
	return "sliceString"
}
