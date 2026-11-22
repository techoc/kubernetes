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

// Test 覆盖 k8s-long-name（小写 DNS 子域）的合法/非法取值，
// 并验证旧值非法时 ratcheting 不再报错。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法：小写字母数字与点，点分段非空。
	st.Value(&Struct{
		LongNameField:        "foo.bar",
		LongNamePtrField:     ptr.To("foo.bar"),
		LongNameTypedefField: "foo.bar",
	}).ExpectValid()

	st.Value(&Struct{
		LongNameField:        "1.2.3.4",
		LongNamePtrField:     ptr.To("1.2.3.4"),
		LongNameTypedefField: "1.2.3.4",
	}).ExpectValid()

	// 非法：空串与大写/空格等不满足小写 DNS 子域规则的值。
	invalidStruct := &Struct{
		LongNameField:        "",
		LongNamePtrField:     ptr.To(""),
		LongNameTypedefField: "",
	}
	st.Value(invalidStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("longNameField"), nil, "").WithOrigin("format=k8s-long-name"),
		field.Invalid(field.NewPath("longNamePtrField"), nil, "").WithOrigin("format=k8s-long-name"),
		field.Invalid(field.NewPath("longNameTypedefField"), nil, "").WithOrigin("format=k8s-long-name"),
	})
	// Test validation ratcheting
	st.Value(invalidStruct).OldValue(invalidStruct).ExpectValid()

	invalidStruct = &Struct{
		LongNameField:        "Not a LongName",
		LongNamePtrField:     ptr.To("Not a LongName"),
		LongNameTypedefField: "Not a LongName",
	}
	st.Value(invalidStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("longNameField"), nil, "").WithOrigin("format=k8s-long-name"),
		field.Invalid(field.NewPath("longNamePtrField"), nil, "").WithOrigin("format=k8s-long-name"),
		field.Invalid(field.NewPath("longNameTypedefField"), nil, "").WithOrigin("format=k8s-long-name"),
	})
	// Test validation ratcheting
	st.Value(invalidStruct).OldValue(invalidStruct).ExpectValid()
}
