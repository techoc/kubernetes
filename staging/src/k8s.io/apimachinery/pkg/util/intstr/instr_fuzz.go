//go:build !notest

/*
Copyright 2020 The Kubernetes Authors.

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

package intstr

import (
	"sigs.k8s.io/randfill"
)

// RandFill 满足 randfill.NativeSelfFiller 接口，为 IntOrString 生成随机值。
// 它会同时维护 Type 与对应字段的一致性：选择 Int 时清空 StrVal，选择 String 时清空 IntVal，
// 避免产生"类型与值不匹配"的非法对象。
func (intstr *IntOrString) RandFill(c randfill.Continue) {
	if intstr == nil {
		return
	}
	// 以 50% 概率生成整数或字符串；两个分支都显式清零另一侧字段。
	if c.Bool() {
		intstr.Type = Int
		c.Fill(&intstr.IntVal)
		intstr.StrVal = ""
	} else {
		intstr.Type = String
		intstr.IntVal = 0
		c.Fill(&intstr.StrVal)
	}
}

// 编译期断言：IntOrString 实现 randfill.NativeSelfFiller。
var _ randfill.NativeSelfFiller = &IntOrString{}
