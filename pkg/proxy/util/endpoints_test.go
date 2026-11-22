/*
Copyright 2017 The Kubernetes Authors.

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
	"testing"
)

// TestIPPart 验证 IPPart 能从 IP、IP:port 和 endpoint 字符串中提取 IP，并正确处理格式错误。
func TestIPPart(t *testing.T) {
	// 用空串表示“期望成功”，以便与“期望解析失败”区分开。
	const noError = ""

	// 覆盖纯 IP、IPv4/IPv6 带端口及多种畸形输入，确保错误分支也被测到。
	testCases := []struct {
		endpoint      string
		expectedIP    string
		expectedError string
	}{
		{"1.2.3.4", "1.2.3.4", noError},
		{"1.2.3.4:9999", "1.2.3.4", noError},
		{"2001:db8::1:1", "2001:db8::1:1", noError},
		{"[2001:db8::2:2]:9999", "2001:db8::2:2", noError},
		{"1.2.3.4::9999", "", "too many colons"},
		{"1.2.3.4:[0]", "", "unexpected '[' in address"},
		{"1.2.3:8080", "", "invalid ip part"},
	}

	// 期望失败时只校验返回空串，具体错误信息由日志给出。
	for _, tc := range testCases {
		ip := IPPart(tc.endpoint)
		if tc.expectedError == noError {
			if ip != tc.expectedIP {
				t.Errorf("Unexpected IP for %s: Expected: %s, Got %s", tc.endpoint, tc.expectedIP, ip)
			}
		} else if ip != "" {
			t.Errorf("Error did not occur for %s, expected: '%s' error", tc.endpoint, tc.expectedError)
		}
	}
}
