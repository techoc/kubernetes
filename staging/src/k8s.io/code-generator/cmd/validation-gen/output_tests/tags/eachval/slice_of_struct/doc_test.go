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

package sliceofstruct

import (
	"testing"
)

// Test 验证 eachVal 作用于 slice 的值：按索引报错；不可比较元素类型在
// ratcheting 时按语义比较，旧值中已存在的元素不重复报错。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{
		// 全部为零值。
	}).ExpectValid()

	st.Value(&Struct{
		ListField:        []OtherStruct{{}, {}},
		ListTypedefField: []OtherTypedefStruct{{}, {}},
	}).ExpectValidateFalseByPath(map[string][]string{
		"listField[0]":        {"field Struct.ListField[*]"},
		"listField[1]":        {"field Struct.ListField[*]"},
		"listTypedefField[0]": {"field Struct.ListTypedefField[*]"},
		"listTypedefField[1]": {"field Struct.ListTypedefField[*]"},
	})
	st.Value(&Struct{
		ListNonComparableField: []NonComparableStruct{{SliceField: []string{"zero", "one"}}, {SliceField: []string{"three", "four"}}},
	}).ExpectValidateFalseByPath(map[string][]string{
		"listNonComparableField[0]": {"field Struct.ListNonComparableField[*]"},
		"listNonComparableField[1]": {"field Struct.ListNonComparableField[*]"},
	})

	// 测试校验的 ratcheting 行为。
	st.Value(&Struct{
		ListField:        []OtherStruct{{}, {}},
		ListTypedefField: []OtherTypedefStruct{{}, {}},
	}).OldValue(&Struct{
		ListField:        []OtherStruct{{}, {}},
		ListTypedefField: []OtherTypedefStruct{{}, {}},
	}).ExpectValid()

	st.Value(&Struct{
		// 新元素在旧值中已存在，但这不是 set。
		ListNonComparableField: []NonComparableStruct{{SliceField: []string{"three", "four"}}},
	}).OldValue(&Struct{
		ListNonComparableField: []NonComparableStruct{{SliceField: []string{"zero", "one"}}, {SliceField: []string{"three", "four"}}},
	}).ExpectValidateFalseByPath(map[string][]string{
		"listNonComparableField[0]": {"field Struct.ListNonComparableField[*]"},
	})
}
