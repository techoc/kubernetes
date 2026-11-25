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

package options

import (
	"testing"
)

// TestGetServiceIPAndRanges 覆盖默认网段、双栈顺序及非法网段的解析结果。
func TestGetServiceIPAndRanges(t *testing.T) {
	tests := []struct {
		body                    string
		apiServerServiceIP      string
		primaryServiceIPRange   string
		secondaryServiceIPRange string
		expectedError           bool
	}{
		{"", "10.0.0.1", "10.0.0.0/24", "<nil>", false},
		{"192.0.2.1/24", "192.0.2.1", "192.0.2.0/24", "<nil>", false},
		{"192.0.2.1/24,192.168.128.0/17", "192.0.2.1", "192.0.2.0/24", "192.168.128.0/17", false},
		// 双栈：IPv4 主网段、IPv6 备网段。
		{"192.0.2.1/24,2001:db2:1:3:4::1/112", "192.0.2.1", "192.0.2.0/24", "2001:db2:1:3:4::/112", false},
		// 双栈：IPv6 主网段、IPv4 备网段。
		{"2001:db2:1:3:4::1/112,192.0.2.1/24", "2001:db2:1:3:4::1", "2001:db2:1:3:4::/112", "192.0.2.0/24", false},

		{"192.0.2.1/30,192.168.128.0/17", "<nil>", "<nil>", "<nil>", true},
		// 主网段的 IPv4 掩码无效。
		{"192.0.2.1/33,192.168.128.0/17", "<nil>", "<nil>", "<nil>", true},
		// 备网段的 IPv4 掩码无效。
		{"192.0.2.1/24,192.168.128.0/33", "<nil>", "<nil>", "<nil>", true},
		// 主网段的 IPv6 掩码无效。
		{"2001:db2:1:3:4::1/129,192.0.2.1/24", "<nil>", "<nil>", "<nil>", true},
		// 备网段的 IPv6 掩码无效。
		{"192.0.2.1/24,2001:db2:1:3:4::1/129", "<nil>", "<nil>", "<nil>", true},
		// 主网段缺少 IPv4 掩码。
		{"192.0.2.1,192.168.128.0/17", "<nil>", "<nil>", "<nil>", true},
		// 备网段缺少 IPv4 掩码。
		{"192.0.2.1/24,192.168.128.1", "<nil>", "<nil>", "<nil>", true},
		// 主网段缺少 IPv6 掩码。
		{"2001:db2:1:3:4::1,192.0.2.1/24", "<nil>", "<nil>", "<nil>", true},
		// 备网段缺少 IPv6 掩码。
		{"192.0.2.1/24,2001:db2:1:3:4::1", "<nil>", "<nil>", "<nil>", true},
		// 主网段的 IP 地址格式无效。
		{"bad.ip.range,192.168.0.2/24", "<nil>", "<nil>", "<nil>", true},
		// 备网段的 IP 地址格式无效。
		{"192.168.0.2/24,bad.ip.range", "<nil>", "<nil>", "<nil>", true},
	}

	for _, test := range tests {
		apiServerServiceIP, primaryServiceIPRange, secondaryServiceIPRange, err := getServiceIPAndRanges(test.body)

		if apiServerServiceIP.String() != test.apiServerServiceIP {
			t.Errorf("expected apiServerServiceIP: %s, got: %s", test.apiServerServiceIP, apiServerServiceIP.String())
		}

		if primaryServiceIPRange.String() != test.primaryServiceIPRange {
			t.Errorf("expected primaryServiceIPRange: %s, got: %s", test.primaryServiceIPRange, primaryServiceIPRange.String())
		}

		if secondaryServiceIPRange.String() != test.secondaryServiceIPRange {
			t.Errorf("expected secondaryServiceIPRange: %s, got: %s", test.secondaryServiceIPRange, secondaryServiceIPRange.String())
		}

		if (err == nil) == test.expectedError {
			t.Errorf("expected err to be: %t, but it was %t", test.expectedError, !test.expectedError)
		}
	}
}
