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

import (
	"fmt"
	"strconv"
)

// Tristate 是兼容 flag/pflag 的标志，记录用户是否提供过值。
// 三态（未设置/真/假）用于区分“用户显式关闭”和“未配置，采用默认策略”；
// 普通 bool 标志无法表达这一差异。
type Tristate int

const (
	// Unset 表示标志未出现，零值即为未设置，便于结构体直接嵌入。
	Unset Tristate = iota // 0
	// True 表示显式设置为真。
	True
	// False 表示显式设置为假。
	False
)

// Default 把标志的默认值初始化为 value；调用后 Provided 仍返回 true。
func (f *Tristate) Default(value bool) {
	*f = triFromBool(value)
}

// String 实现 pflag.Value；Unset 与 False 都显示为 false。
func (f Tristate) String() string {
	b := boolFromTri(f)
	return fmt.Sprintf("%t", b)
}

// Value 返回布尔视图；注意 Unset 也被当作 false。
func (f Tristate) Value() bool {
	b := boolFromTri(f)
	return b
}

// Set 解析 --flag=true/false 并转为对应三态；任何已设置的值都离开 Unset。
func (f *Tristate) Set(value string) error {
	boolVal, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}

	*f = triFromBool(boolVal)
	return nil
}

// Provided 报告标志是否被显式赋值过（包括通过 Default 设置）。
func (f Tristate) Provided() bool {
	if f != Unset {
		return true
	}
	return false
}

// Type 实现 pflag.Value，返回帮助文本中显示的类型名。
func (f *Tristate) Type() string {
	return "tristate"
}

// boolFromTri 把三态投影为普通 bool，Unset 视为 false。
func boolFromTri(t Tristate) bool {
	if t == True {
		return true
	} else {
		return false
	}
}

// triFromBool 把 bool 转为显式三态，用于默认值和解析结果。
func triFromBool(b bool) Tristate {
	if b {
		return True
	} else {
		return False
	}
}
