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

// Test 覆盖 k8s-short-name（单段 DNS label，最长 63）的合法与非法取值。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法：小写字母数字与连字符。
	st.Value(&Struct{
		ShortNameField:        "foo-bar",
		ShortNamePtrField:     ptr.To("foo-bar"),
		ShortNameTypedefField: "foo-bar",
	}).ExpectValid()

	st.Value(&Struct{
		ShortNameField:        "1234",
		ShortNamePtrField:     ptr.To("1234"),
		ShortNameTypedefField: "1234",
	}).ExpectValid()

	// 非法：空串与包含空格/大写等非 DNS label 字符的值。
	st.Value(&Struct{
		ShortNameField:        "",
		ShortNamePtrField:     ptr.To(""),
		ShortNameTypedefField: "",
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("shortNameField"), nil, "").WithOrigin("format=k8s-short-name"),
		field.Invalid(field.NewPath("shortNamePtrField"), nil, "").WithOrigin("format=k8s-short-name"),
		field.Invalid(field.NewPath("shortNameTypedefField"), nil, "").WithOrigin("format=k8s-short-name"),
	})

	st.Value(&Struct{
		ShortNameField:        "Not a DNS label",
		ShortNamePtrField:     ptr.To("Not a DNS label"),
		ShortNameTypedefField: "Not a DNS label",
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("shortNameField"), nil, "").WithOrigin("format=k8s-short-name"),
		field.Invalid(field.NewPath("shortNamePtrField"), nil, "").WithOrigin("format=k8s-short-name"),
		field.Invalid(field.NewPath("shortNameTypedefField"), nil, "").WithOrigin("format=k8s-short-name"),
	})
}
