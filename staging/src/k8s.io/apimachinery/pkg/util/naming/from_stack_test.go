/*
Copyright 2018 The Kubernetes Authors.

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

package naming

import (
	"strings"
	"testing"
)

// TestGetNameFromCallsite 验证 GetNameFromCallsite 能跳过指定的包或文件，
// 沿调用栈继续向上找到第一个未被忽略的帧。
//
// 忽略规则匹配的是帧的文件路径前缀，因此既能按包目录忽略，也能按单个文件忽略；
// 当所有帧都被忽略时返回 "????"，调用方据此仍可得到一个可用的名称。
func TestGetNameFromCallsite(t *testing.T) {
	tests := []struct {
		name            string
		ignoredPackages []string
		expected        string
	}{
		{
			// 不忽略任何包时，最近的帧就是本测试文件。
			name:     "simple",
			expected: "k8s.io/apimachinery/pkg/util/naming/from_stack_test.go:",
		},
		{
			// 忽略整个 naming 包后，栈顶应落到 testing 包。
			name:            "ignore-package",
			ignoredPackages: []string{"k8s.io/apimachinery/pkg/util/naming"},
			expected:        "testing/testing.go:",
		},
		{
			// 只忽略本测试文件，效果与忽略整个包相同，因为调用链只经过该文件。
			name:            "ignore-file",
			ignoredPackages: []string{"k8s.io/apimachinery/pkg/util/naming/from_stack_test.go"},
			expected:        "testing/testing.go:",
		},
		{
			// 测试文件与 testing 包都被忽略，栈中已无可用的帧，回退为占位符。
			name:            "ignore-multiple",
			ignoredPackages: []string{"k8s.io/apimachinery/pkg/util/naming/from_stack_test.go", "testing/testing.go"},
			expected:        "????",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := GetNameFromCallsite(tc.ignoredPackages...)
			// 只比较前缀：行号与构建路径随环境变化，不应纳入断言。
			if !strings.HasPrefix(actual, tc.expected) {
				t.Fatalf("expected string with prefix %q, got %q", tc.expected, actual)
			}
		})
	}
}
