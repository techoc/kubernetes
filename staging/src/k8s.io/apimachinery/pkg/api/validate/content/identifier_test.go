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

package content

import (
	"testing"
)

// TestIsCIdentifier 覆盖 C 标识符校验：字母/下划线开头，后续仅允许
// 字母、数字和下划线；空串、数字开头、含 '-'/'.'/'#'/空格等均判为非法。
func TestIsCIdentifier(t *testing.T) {
	// 合法样例同时覆盖大小写、首尾下划线以及中间连续下划线。
	goodValues := []string{
		"a", "ab", "abc", "a1", "_a", "a_", "a_b", "a_1", "a__1__2__b", "__abc_123",
		"A", "AB", "AbC", "A1", "_A", "A_", "A_B", "A_1", "A__1__2__B", "__123_ABC",
	}
	for _, val := range goodValues {
		if msgs := IsCIdentifier(val); len(msgs) != 0 {
			t.Errorf("expected true for '%s': %v", val, msgs)
		}
	}

	// 非法样例覆盖空串、数字开头、非标识符字符（'-' '.' '#'）、空白等。
	badValues := []string{
		"", "1", "123", "1a",
		"-", "a-", "-a", "1-", "-1", "1_", "1_2",
		".", "a.", ".a", "a.b", "1.", ".1", "1.2",
		" ", "a ", " a", "a b", "1 ", " 1", "1 2",
		"#a#",
	}
	for _, val := range badValues {
		if msgs := IsCIdentifier(val); len(msgs) == 0 {
			t.Errorf("expected false for '%s'", val)
		}
	}
}
