/*
Copyright 2025 The Kubernetes Authors.

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

package format

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// TestCaseless 验证 +k8s:format=k8s-long-name-caseless：
// 该格式接受大小写混合的 DNS subdomain；空串、含空格或非 DNS 字符的值不合法。
func TestCaseless(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法：普通点分小写名。
	st.Value(&Struct{
		LongNameField:        "foo.bar",
		LongNamePtrField:     ptr.To("foo.bar"),
		LongNameTypedefField: "foo.bar",
	}).ExpectValid()

	// 合法：纯数字点分名。
	st.Value(&Struct{
		LongNameField:        "1.2.3.4",
		LongNamePtrField:     ptr.To("1.2.3.4"),
		LongNameTypedefField: "1.2.3.4",
	}).ExpectValid()

	// 合法：caseless 允许大写字母。
	st.Value(&Struct{
		LongNameField:        "Foo.Bar",
		LongNamePtrField:     ptr.To("Foo.Bar"),
		LongNameTypedefField: "Foo.Bar",
	}).ExpectValid()

	// 非法：空串；错误 origin 应标注为 format=k8s-long-name-caseless。
	invalidStruct := &Struct{
		LongNameField:        "",
		LongNamePtrField:     ptr.To(""),
		LongNameTypedefField: "",
	}
	st.Value(invalidStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("longNameField"), nil, "").WithOrigin("format=k8s-long-name-caseless"),
		field.Invalid(field.NewPath("longNamePtrField"), nil, "").WithOrigin("format=k8s-long-name-caseless"),
		field.Invalid(field.NewPath("longNameTypedefField"), nil, "").WithOrigin("format=k8s-long-name-caseless"),
	})
	// 校验 ratcheting：旧值同样非法时，错误被吸收。
	// Test validation ratcheting
	st.Value(invalidStruct).OldValue(invalidStruct).ExpectValid()

	// 非法：包含空格。
	invalidStruct = &Struct{
		LongNameField:        "Not a LongName",
		LongNamePtrField:     ptr.To("Not a LongName"),
		LongNameTypedefField: "Not a LongName",
	}
	st.Value(invalidStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("longNameField"), nil, "").WithOrigin("format=k8s-long-name-caseless"),
		field.Invalid(field.NewPath("longNamePtrField"), nil, "").WithOrigin("format=k8s-long-name-caseless"),
		field.Invalid(field.NewPath("longNameTypedefField"), nil, "").WithOrigin("format=k8s-long-name-caseless"),
	})
	// Test validation ratcheting
	st.Value(invalidStruct).OldValue(invalidStruct).ExpectValid()
}
