/*
Copyright 2019 The Kubernetes Authors.

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

package handlers

import (
	"fmt"
	"testing"
)

// TestManagerOrUserAgent 验证 field manager 缺失时回退到 User-Agent 首段的逻辑，
// 重点覆盖非 ASCII（emoji）以及超长 User-Agent 的截断边界。
func TestManagerOrUserAgent(t *testing.T) {
	tests := []struct {
		manager   string
		userAgent string
		expected  string
	}{
		{
			// 常规浏览器 UA：取空格分隔的第一段 "Mozilla"。
			manager:   "",
			userAgent: "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
			expected:  "Mozilla",
		},
		{
			// 超过长度上限的 ASCII UA：必须截断到 128 字节，不能整段保留。
			manager:   "",
			userAgent: "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff/Something",
			expected:  "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		},
		{
			// 多字节字符按 rune 边界截断，不能产生非法 UTF-8 字节序列。
			manager:   "",
			userAgent: "🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔",
			expected:  "🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔🍔",
		},
		{
			// 显式 manager 优先，User-Agent 完全不参与。
			manager:   "manager",
			userAgent: "userAgent",
			expected:  "manager",
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v-%v", test.manager, test.userAgent), func(t *testing.T) {
			got := managerOrUserAgent(test.manager, test.userAgent)
			if got != test.expected {
				t.Errorf("Wanted %#v, got %#v", test.expected, got)
			}
		})
	}
}
