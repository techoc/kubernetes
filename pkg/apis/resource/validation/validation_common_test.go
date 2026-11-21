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

package validation

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/dynamic-resource-allocation/api"
)

// assertFailures 比较期望错误与实际错误，

// 不一致时同时打印格式化后的错误信息，便于判断提示是否对用户有帮助。
func assertFailures(tb testing.TB, want, got field.ErrorList) bool {
	tb.Helper()
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(field.Error{}, "Origin"), cmp.AllowUnexported(api.UniqueString{})); diff != "" {
		tb.Errorf("unexpected field errors (-want, +got):\n%s", diff)
		return false
	}
	return true
}

// TestTruncateIfTooLong 覆盖截断在未超限、达到上限与超过内置下限时的行为。
func TestTruncateIfTooLong(t *testing.T) {
	for name, tc := range map[string]struct {
		str       string
		maxLen    int
		expectStr string
	}{
		// 未超过上限时原样返回。
		"nop": {
			str:       "hello",
			maxLen:    10,
			expectStr: "hello",
		},
		// 超过上限时用省略号替换中间内容。
		"truncate-to-limit": {
			str:       "hello world how are you",
			maxLen:    18,
			expectStr: "hello wo...are you",
		},
		// 上限过小时会被抬高到内置下限。
		"truncate-to-builtin-limit": {
			str:       "hello world how are you",
			maxLen:    1, // 过小，会被抬高到内置下限。
			expectStr: "hello w...re you",
		},
		// 以下几组覆盖字符串长度与上限的奇偶组合，校验省略号的替换位置。
		"truncate-odd-string-even-limit": {
			str:       "abcdefghijklmnopqrs",
			maxLen:    16,
			expectStr: "abcdefg...nopqrs",
		},
		"truncate-even-string-even-limit": {
			str:       "abcdefghijklmnopqrst",
			maxLen:    16,
			expectStr: "abcdefg...opqrst",
		},
		"truncate-odd-string-odd-limit": {
			str:       "abcdefghijklmnopqrs",
			maxLen:    17,
			expectStr: "abcdefg...mnopqrs",
		},
		"truncate-even-string-odd-limit": {
			str:       "abcdefghijklmnopqrst",
			maxLen:    17,
			expectStr: "abcdefg...nopqrst",
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expectStr, truncateIfTooLong(tc.str, tc.maxLen))
		})
	}
}
