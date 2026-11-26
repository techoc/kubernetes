/*
Copyright The Kubernetes Authors.

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

package sliceofpointers

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证 eachVal 对指针切片元素的处理：
// 元素非 nil 时对解引用值执行校验；元素为 nil 时由 PtrSliceVal
// 直接报 Required，而不是解引用导致崩溃。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 1. 零值应当合法
	st.Value(&Struct{
		// 全部为零值。
	}).ExpectValid()

	// 2. 非 nil 元素触发校验错误
	st.Value(&Struct{
		ListField:          []*OtherStruct{{}, {}},
		ListPrimitiveField: []*string{new("a"), new("b")},
	}).ExpectValidateFalseByPath(map[string][]string{
		"listField[0]":          {"field Struct.ListField[*]", "type OtherStruct"},
		"listField[1]":          {"field Struct.ListField[*]", "type OtherStruct"},
		"listPrimitiveField[0]": {"field Struct.ListPrimitiveField[*]"},
		"listPrimitiveField[1]": {"field Struct.ListPrimitiveField[*]"},
	})

	// 3. nil 元素触发 EachPtrSliceVal 的 Required 错误
	st.Value(&Struct{
		ListField:          []*OtherStruct{nil},
		ListPrimitiveField: []*string{nil},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Required(field.NewPath("listField").Index(0), ""),
		field.Required(field.NewPath("listPrimitiveField").Index(0), ""),
	})
}
