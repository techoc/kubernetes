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

package typedeftoslice

import (
	"testing"
)

// Test 验证 +k8s:eachVal 对 typedef 切片的传播：
// 空值与空切片合法；非空时每个元素都同时收到类型级与字段级错误，
// 错误路径按元素下标渲染，如 listField[0]。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{
		// 全部为零值。
	}).ExpectValid()

	st.Value(&Struct{
		ListField:        ListType{"zero", "one"},
		ListTypedefField: ListTypedefType{StringType("zero"), StringType("one")},
	}).ExpectValidateFalseByPath(map[string][]string{
		"listField[0]":        {"type ListType[*]", "field Struct.ListField[*]"},
		"listField[1]":        {"type ListType[*]", "field Struct.ListField[*]"},
		"listTypedefField[0]": {"type ListTypedefType[*]", "field Struct.ListTypedefField[*]"},
		"listTypedefField[1]": {"type ListTypedefType[*]", "field Struct.ListTypedefField[*]"},
	})
}
