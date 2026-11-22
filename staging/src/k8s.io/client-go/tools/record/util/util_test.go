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

package util

import (
	"strings"
	"testing"

	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
)

// TestGenerateEventName 验证事件名的生成：合法引用名拼接时间戳后仍是合法的 DNS 子域名，
// 而超长、含大写或特殊字符的引用名也必须被规整成合法名称（不返回错误）。
func TestGenerateEventName(t *testing.T) {
	// 固定时间戳使 golden 期望值可重复。
	timestamp := int64(105999103295324396)
	testCases := []struct {
		name     string
		refName  string
		expected string
	}{
		{
			name:     "valid name",
			refName:  "test-pod",
			expected: "test-pod.178959f726d80ec",
		},
		{
			name:    "invalid name - too long",
			refName: strings.Repeat("x", 300),
		},
		{
			name:    "invalid name - upper case",
			refName: "test.POD",
		},
		{
			name:    "invalid name - special chars",
			refName: "test.pod/invalid!chars?",
		},
		{
			name:    "invalid name - special chars and non alphanumeric starting character",
			refName: "--test.pod/invalid!chars?",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := GenerateEventName(tc.refName, timestamp)

			// 核心不变量：无论输入多离谱，输出都必须能作为对象的名称使用。
			if errs := apimachineryvalidation.NameIsDNSSubdomain(actual, false); len(errs) > 0 {
				t.Errorf("generateEventName(%s) = %s; not a valid name: %v", tc.refName, actual, errs)

			}

			if tc.expected != "" && (actual != tc.expected) {
				t.Errorf("generateEventName(%s) returned %s expected %s", tc.refName, actual, tc.expected)
			}

		})

	}
}
