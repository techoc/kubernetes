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
)

// Test 覆盖 k8s-prefixed-label-key 的合法与非法取值，并确认普通字段、
// 指针字段、typedef 字段三者行为一致；ratcheting 由 format 标签统一处理。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法样例覆盖长度边界：前缀最多 253 字符、名称最多 63 字符。
	validCases := []string{
		"simple.com/simple",
		"now-with-dashes.com/simple",
		"now.with.dots.com/simple",
		"now-with.dashes-and.dots.com/simple",
		"1-num.2-num.com/3-num",
		"1234.com/5678",
		"1.2.3.4/5678",
		"example.com/Uppercase_Is_OK_123",
		strings.Repeat("a", 253) + "/" + strings.Repeat("b", 63),
	}

	for _, s := range validCases {
		st.Value(&Struct{
			PrefixedLabelKeyField:        s,
			PrefixedLabelKeyPtrField:     new(s),
			PrefixedLabelKeyTypedefField: PrefixedLabelKeyStringType(s),
		}).ExpectValid()
	}

	// 非法样例包含缺少前缀、大写前缀、多余斜杠以及超长等典型失败模式。
	invalidCases := []string{
		"simple",
		"now-with-dashes",
		"1-starts-with-num",
		"1234",
		"Uppercase_Is_OK_123",
		"requests.storage-foo",
		strings.Repeat("a", 63),
		"nospecialchars%^=@",
		"cantendwithadash-",
		"-cantstartwithadash-",
		"only/one/slash",
		"Example.com/abc",
		"example_com/abc",
		"example.com/",
		"/simple",
		strings.Repeat("a", 64),
		strings.Repeat("a", 254) + "/abc",
	}

	for _, s := range invalidCases {
		st.Value(&Struct{
			PrefixedLabelKeyField:        s,
			PrefixedLabelKeyPtrField:     new(s),
			PrefixedLabelKeyTypedefField: PrefixedLabelKeyStringType(s),
		}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin(), field.ErrorList{
			field.Invalid(field.NewPath("prefixedLabelKeyField"), nil, "").WithOrigin("format=k8s-prefixed-label-key"),
			field.Invalid(field.NewPath("prefixedLabelKeyPtrField"), nil, "").WithOrigin("format=k8s-prefixed-label-key"),
			field.Invalid(field.NewPath("prefixedLabelKeyTypedefField"), nil, "").WithOrigin("format=k8s-prefixed-label-key"),
		})
	}
}
