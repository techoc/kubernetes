/*
Copyright 2014 The Kubernetes Authors.

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

// StringFlag 是兼容 flag/pflag 的字符串标志，额外记录用户是否显式设置过：
// 组件常需要区分“未提供”与“提供了默认值”，二者在配置合并时含义不同。
type StringFlag struct {
	// 调用过 Set 时为 true。
	provided bool
	// 标志中提供的原始值。
	value string
}

// NewStringFlag 返回以 defaultVal 作为初始值的 StringFlag。
// 初始值不算“已提供”，Provided 仍为 false。
func NewStringFlag(defaultVal string) StringFlag {
	return StringFlag{value: defaultVal}
}

// Default 在解析前改写默认值；不影响 provided 标志。
func (f *StringFlag) Default(value string) {
	f.value = value
}

// String 实现 pflag.Value 接口，返回当前值。
func (f StringFlag) String() string {
	return f.value
}

// Value 返回当前字符串值。
func (f StringFlag) Value() string {
	return f.value
}

// Set 实现 pflag.Value：记录值并标记用户已提供该标志。
func (f *StringFlag) Set(value string) error {
	f.value = value
	f.provided = true

	return nil
}

// Provided 报告用户是否显式设置过该标志。
func (f StringFlag) Provided() bool {
	return f.provided
}

// Type 实现 pflag.Value 接口，用于帮助信息中显示类型名。
func (f *StringFlag) Type() string {
	return "string"
}
