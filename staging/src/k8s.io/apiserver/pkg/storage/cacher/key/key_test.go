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

package key

import (
	"testing"
)

// TestHasPathPrefix 用正反用例固定路径前缀的边界语义：
// 精确匹配、斜杠边界、UTF-8 多字节前缀，以及不做路径归一化。
func TestHasPathPrefix(t *testing.T) {
	validTestcases := []struct {
		s      string
		prefix string
	}{
		// Exact matches
		{"", ""},
		{"a", "a"},
		{"a/", "a/"},
		{"a/../", "a/../"},

		// Path prefix matches
		{"a/b", "a"},
		{"a/b", "a/"},
		{"中文/", "中文"},
	}
	for i, tc := range validTestcases {
		if !HasPathPrefix(tc.s, tc.prefix) {
			t.Errorf(`%d: Expected HasPathPrefix("%s","%s") to be true`, i, tc.s, tc.prefix)
		}
	}

	invalidTestcases := []struct {
		s      string
		prefix string
	}{
		// Mismatch
		{"a", "b"},

		// Dir requirement
		{"a", "a/"},

		// Prefix mismatch
		{"ns2", "ns"},
		{"ns2", "ns/"},
		{"中文文", "中文"},

		// Ensure no normalization is applied
		{"a/c/../b/", "a/b/"},
		{"a/", "a/b/.."},
	}
	for i, tc := range invalidTestcases {
		if HasPathPrefix(tc.s, tc.prefix) {
			t.Errorf(`%d: Expected HasPathPrefix("%s","%s") to be false`, i, tc.s, tc.prefix)
		}
	}
}
