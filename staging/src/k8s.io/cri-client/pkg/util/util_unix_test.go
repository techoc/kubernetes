//go:build freebsd || linux || darwin

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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParseEndpoint 验证 parseEndpoint 对各类端点 URL 的解析结果。
// 覆盖合法的 unix/tcp URL，以及 npipe、未知协议、含空格的非法 URL 等错误场景。
func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		endpoint         string // 输入端点
		expectError      bool   // 是否期望解析出错
		expectedProtocol string // 期望的协议名
		expectedAddr     string // 期望的地址
	}{
		{
			endpoint:         "unix:///tmp/s1.sock",
			expectedProtocol: "unix",
			expectedAddr:     "/tmp/s1.sock",
		},
		{
			endpoint:         "tcp://localhost:15880",
			expectedProtocol: "tcp",
			expectedAddr:     "localhost:15880",
		},
		{
			endpoint:         "npipe://./pipe/mypipe",
			expectedProtocol: "npipe",
			expectError:      true,
		},
		{
			endpoint:         "tcp1://abc",
			expectedProtocol: "tcp1",
			expectError:      true,
		},
		{
			endpoint:    "a b c",
			expectError: true,
		},
	}

	// 逐个用例解析：先断言协议名，再按是否期望错误分别断言 err 与地址。
	for _, test := range tests {
		protocol, addr, err := parseEndpoint(test.endpoint)
		assert.Equal(t, test.expectedProtocol, protocol)
		if test.expectError {
			assert.Error(t, err, "Expect error during parsing %q", test.endpoint)
			continue
		}
		assert.NoError(t, err, "Expect no error during parsing %q", test.endpoint)
		assert.Equal(t, test.expectedAddr, addr)
	}

}

// TestGetAddressAndDialer 验证 GetAddressAndDialer 对 unix 端点返回正确路径，
// 并对 tcp/https/http 以及拼写错误的 scheme 返回错误（类 Unix 平台只支持 unix socket）。
func TestGetAddressAndDialer(t *testing.T) {
	tests := []struct {
		endpoint     string // 输入端点
		expectError  bool   // 是否期望出错
		expectedAddr string // 期望返回的地址
	}{
		{
			endpoint:     "unix:///tmp/s1.sock",
			expectError:  false,
			expectedAddr: "/tmp/s1.sock",
		},
		{
			endpoint:     "unix:///tmp/f6.sock",
			expectError:  false,
			expectedAddr: "/tmp/f6.sock",
		},
		{
			endpoint:    "tcp://localhost:9090",
			expectError: true,
		},
		{
			// The misspelling is intentional to make it error
			endpoint:    "htta://free-test.com",
			expectError: true,
		},
		{
			endpoint:    "https://www.youtube.com/",
			expectError: true,
		},
		{
			endpoint:    "http://www.baidu.com/",
			expectError: true,
		},
	}
	for _, test := range tests {
		// just test addr and err
		// 这里只关心返回的地址和错误，忽略拨号函数本身。
		addr, _, err := GetAddressAndDialer(test.endpoint)
		if test.expectError {
			assert.Error(t, err, "expected error during parsing %s", test.endpoint)
			continue
		}
		assert.NoError(t, err, "expected no error during parsing %s", test.endpoint)
		assert.Equal(t, test.expectedAddr, addr)
	}
}
