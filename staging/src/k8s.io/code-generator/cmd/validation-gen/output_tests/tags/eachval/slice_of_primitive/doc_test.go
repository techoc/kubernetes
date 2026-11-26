/*
Copyright 2024 The Kubernetes Authors.

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

package sliceofprimitive

import (
	"testing"
)

// Test 验证 +k8s:eachVal 对“元素为基本类型/typedef 的切片”的逐元素校验，
// 以及校验 ratcheting（更新时复用旧值中已存在的错误）。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 零值列表不触发任何校验。
	st.Value(&Struct{
		// 全部为零值。
	}).ExpectValid()

	// 非空列表的每个元素都应报错，错误路径带 [index]。
	st.Value(&Struct{
		ListField:        []string{"zero", "one"},
		ListTypedefField: []StringType{StringType("zero"), StringType("one")},
	}).ExpectValidateFalseByPath(map[string][]string{
		"listField[0]":        {"field Struct.ListField[*]"},
		"listField[1]":        {"field Struct.ListField[*]"},
		"listTypedefField[0]": {"field Struct.ListTypedefField[*]"},
		"listTypedefField[1]": {"field Struct.ListTypedefField[*]"},
	})

	// 校验 ratcheting：新旧值元素顺序不同但内容相同，仍视为“未修复”，继续报错。
	st.Value(&Struct{
		ListField:        []string{"zero", "one"},
		ListTypedefField: []StringType{StringType("zero"), StringType("one")},
	}).OldValue(&Struct{
		// 数据相同、顺序不同 — 仍应报错。
		ListField:        []string{"one", "zero"},
		ListTypedefField: []StringType{StringType("one"), StringType("zero")},
	}).ExpectValidateFalseByPath(map[string][]string{
		"listField[0]":        {"field Struct.ListField[*]"},
		"listField[1]":        {"field Struct.ListField[*]"},
		"listTypedefField[0]": {"field Struct.ListTypedefField[*]"},
		"listTypedefField[1]": {"field Struct.ListTypedefField[*]"},
	})

	// 新旧值完全一致时，旧值中的错误被 ratcheting 吸收，结果为合法。
	st.Value(&Struct{
		ListField:        []string{"zero", "one"},
		ListTypedefField: []StringType{StringType("zero"), StringType("one")},
	}).OldValue(&Struct{
		ListField:        []string{"zero", "one"},
		ListTypedefField: []StringType{StringType("zero"), StringType("one")},
	}).ExpectValid()
}
