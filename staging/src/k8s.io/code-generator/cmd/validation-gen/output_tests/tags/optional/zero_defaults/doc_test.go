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

package zerodefaults

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// Test 验证 optional 的零值语义：带 default=0 的值类型字段仍是 optional，
// 而指针字段的 nil 被视为“未设置”，因此必需（required）报错。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{
		// 全部为零值。
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		// "stringField"：默认值零值的可选值字段仅作文档说明
		// "intField"：默认值零值的可选值字段仅作文档说明
		// "boolField"：默认值零值的可选值字段仅作文档说明
		//
		// 值类型的零值可被 default=0 覆盖，因此不算缺失；
		// 指针为 nil 时无法区分“零值”和“未设置”，必须显式赋值。
		field.Required(field.NewPath("stringPtrField"), ""),
		field.Required(field.NewPath("intPtrField"), ""),
		field.Required(field.NewPath("boolPtrField"), ""),
	})

	// 全部字段显式赋值（包括指针指向零值），整体合法。
	st.Value(&Struct{
		StringField:    "abc",
		StringPtrField: ptr.To(""),
		IntField:       123,
		IntPtrField:    ptr.To(0),
		BoolField:      true,
		BoolPtrField:   ptr.To(false),
	}).ExpectValid()
}
