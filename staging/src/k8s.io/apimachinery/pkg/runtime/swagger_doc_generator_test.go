/*
Copyright 2015 The Kubernetes Authors.

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

package runtime

import (
	"testing"
)

// TestFmtRawDoc 验证 swagger 文档注释的规范化：
// 去掉 TODO 行、空行转换为字面量 \n、保留缩进，并折叠为单行。
func TestFmtRawDoc(t *testing.T) {
	tests := []struct {
		t, expected string
	}{
		// 注释中出现 "---" 或 TODO 时，其后的内容被截断/忽略。
		{"aaa\n  --- asd\n TODO: tooooodo\n toooodoooooo\n", "aaa"},
		{"aaa\nasd\n TODO: tooooodo\nbbbb\n --- toooodoooooo\n", "aaa asd bbbb"},
		{" TODO: tooooodo\n", ""},
		// 空行转为字面量 \n，以保留段落结构。
		{"Par1\n\nPar2\n\n", "Par1\\n\\nPar2"},
		{"", ""},
		{" ", ""},
		{" \n", ""},
		{" \n\n ", ""},
		// 缩进（制表符）需要保留。
		{"Example:\n\tl1\n\t\tl2\n", "Example:\\n\\tl1\\n\\t\\tl2"},
	}

	for _, test := range tests {
		if o := fmtRawDoc(test.t); o != test.expected {
			t.Fatalf("Expected: %q, got %q", test.expected, o)
		}
	}
}
