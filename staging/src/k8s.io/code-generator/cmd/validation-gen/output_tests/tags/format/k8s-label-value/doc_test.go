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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// Test 覆盖 k8s-label-value 的合法与非法取值，并确认值、指针、类型定义
// 三种字段形态得到相同的错误路径与 origin，且旧值非法时允许 ratcheting。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法用例：字母数字、. _ -，首尾为字母数字，长度 <= 63。
	validCases := []struct {
		name  string
		value string
	}{
		{"valid value", "valid-value"},
		{"valid value with dots", "valid.value"},
		{"valid value with underscores", "valid_value"},
		{"valid single character value", "a"},
		{"valid value with numbers", "123-abc"},
		{"valid uppercase characters", "Valid-Value"},
		{"valid: max length", "a" + strings.Repeat("b", 61) + "c"}, // 63 characters
		{"valid: empty string", ""},
	}

	for _, tc := range validCases {
		t.Run(tc.name, func(t *testing.T) {
			st.Value(&Struct{
				LabelValueField:        tc.value,
				LabelValuePtrField:     ptr.To(tc.value),
				LabelValueTypedefField: LabelValueStringType(tc.value),
			}).ExpectValid()
		})
	}

	// 非法用例：首尾出现分隔符、含非法字符、超长。
	invalidCases := []struct {
		name  string
		value string
	}{
		{"invalid: starts with dash", "-invalid-value"},
		{"invalid: ends with dash", "invalid-value-"},
		{"invalid: starts with dot", ".invalid.value"},
		{"invalid: ends with dot", "invalid.value."},
		{"invalid: starts with underscore", "_invalid_value"},
		{"invalid: ends with underscore", "invalid_value_"},
		{"invalid: contains special characters", "invalid@value"},
		{"invalid: contains spaces", "Not a LabelValue"},
		{"invalid: too long", "a" + strings.Repeat("b", 62) + "c"}, // 64 characters
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			invalidStruct := &Struct{
				LabelValueField:        tc.value,
				LabelValuePtrField:     ptr.To(tc.value),
				LabelValueTypedefField: LabelValueStringType(tc.value),
			}
			st.Value(invalidStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin(), field.ErrorList{
				field.Invalid(field.NewPath("labelValueField"), nil, "").WithOrigin("format=k8s-label-value"),
				field.Invalid(field.NewPath("labelValuePtrField"), nil, "").WithOrigin("format=k8s-label-value"),
				field.Invalid(field.NewPath("labelValueTypedefField"), nil, "").WithOrigin("format=k8s-label-value"),
			})
			// 测试 ratcheting 行为
			// 旧值同样非法时不应重新报错（未变化的数据不重复校验）。
			st.Value(invalidStruct).OldValue(invalidStruct).ExpectValid()
		})
	}
}
