//go:build windows

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
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetAddressAndDialer 验证 Windows 平台下 GetAddressAndDialer 返回的拨号函数是否正确。
// 由于函数无法直接比较，这里通过反射取得函数指针，再与预期的 tcpDial/npipeDial 指针比对。
func TestGetAddressAndDialer(t *testing.T) {

	// Compare dialer function by pointer
	// 取两个拨号函数的入口地址作为比较基准；0 表示期望返回 nil 拨号函数。
	tcpDialPointer := reflect.ValueOf(tcpDial).Pointer()
	npipeDialPointer := reflect.ValueOf(npipeDial).Pointer()
	var nilDialPointer uintptr = 0x0

	tests := []struct {
		endpoint      string  // 输入端点
		expectedAddr  string  // 期望地址
		expectedDial  uintptr // 期望的拨号函数指针
		expectedError bool    // 是否期望出错
	}{
		{
			endpoint:      "tcp://localhost:15880",
			expectedAddr:  "localhost:15880",
			expectedDial:  tcpDialPointer,
			expectedError: false,
		},
		{
			endpoint:      "npipe://./pipe/mypipe",
			expectedAddr:  "//./pipe/mypipe",
			expectedDial:  npipeDialPointer,
			expectedError: false,
		},
		{
			endpoint:      "npipe:\\\\.\\pipe\\mypipe",
			expectedAddr:  "//./pipe/mypipe",
			expectedDial:  npipeDialPointer,
			expectedError: false,
		},
		{
			endpoint:      "unix:///tmp/s1.sock",
			expectedAddr:  "",
			expectedDial:  nilDialPointer,
			expectedError: true,
		},
		{
			endpoint:      "tcp1://abc",
			expectedAddr:  "",
			expectedDial:  nilDialPointer,
			expectedError: true,
		},
		{
			endpoint:      "a b c",
			expectedAddr:  "",
			expectedDial:  nilDialPointer,
			expectedError: true,
		},
	}

	for _, test := range tests {
		// 为了失败信息可读，把函数指针转换回函数名；0 指针显示为 nilDial。
		expectedDialerName := runtime.FuncForPC(test.expectedDial).Name()
		if expectedDialerName == "" {
			expectedDialerName = "nilDial"
		}
		// 用端点、地址和拨号函数名拼出子测试名，便于定位失败用例。
		t.Run(fmt.Sprintf("Endpoint is %s, addr is %s and dialer is %s",
			test.endpoint, test.expectedAddr, expectedDialerName),
			func(t *testing.T) {
				address, dialer, err := GetAddressAndDialer(test.endpoint)

				// 用反射取实际返回的拨号函数指针，并同样转为名字用于报错。
				dialerPointer := reflect.ValueOf(dialer).Pointer()
				actualDialerName := runtime.FuncForPC(dialerPointer).Name()
				if actualDialerName == "" {
					actualDialerName = "nilDial"
				}

				// 断言拨号函数指针一致，从而确认选择了正确的协议实现。
				assert.Equalf(t, test.expectedDial, dialerPointer,
					"Expected dialer %s, but get %s", expectedDialerName, actualDialerName)

				assert.Equal(t, test.expectedAddr, address)

				if test.expectedError {
					assert.NotNil(t, err, "Expect error during parsing %q", test.endpoint)
				} else {
					assert.Nil(t, err, "Expect no error during parsing %q", test.endpoint)
				}
			})
	}
}

// TestParseEndpoint 验证 Windows 下 parseEndpoint 对 tcp/npipe 各种写法的解析结果，
// 包括反斜杠路径、缺省 host 的 npipe URL，以及 unix 端点在 Windows 上应报错的场景。
func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		endpoint         string // 输入端点
		expectedError    bool   // 是否期望出错
		expectedProtocol string // 期望协议
		expectedAddr     string // 期望地址
	}{
		{
			endpoint:         "unix:///tmp/s1.sock",
			expectedProtocol: "unix",
			expectedError:    true,
		},
		{
			endpoint:         "tcp://localhost:15880",
			expectedProtocol: "tcp",
			expectedAddr:     "localhost:15880",
		},
		{
			endpoint:         "npipe://./pipe/mypipe",
			expectedProtocol: "npipe",
			expectedAddr:     "//./pipe/mypipe",
		},
		{
			endpoint:         "npipe:////./pipe/mypipe2",
			expectedProtocol: "npipe",
			expectedAddr:     "//./pipe/mypipe2",
		},
		{
			endpoint:         "npipe:/pipe/mypipe3",
			expectedProtocol: "npipe",
			expectedAddr:     "//./pipe/mypipe3",
		},
		{
			endpoint:         "npipe:\\\\.\\pipe\\mypipe4",
			expectedProtocol: "npipe",
			expectedAddr:     "//./pipe/mypipe4",
		},
		{
			endpoint:         "npipe:\\pipe\\mypipe5",
			expectedProtocol: "npipe",
			expectedAddr:     "//./pipe/mypipe5",
		},
		{
			endpoint:         "tcp1://abc",
			expectedProtocol: "tcp1",
			expectedError:    true,
		},
		{
			endpoint:      "a b c",
			expectedError: true,
		},
	}

	// 逐个解析：先断言协议名，再按是否期望错误分别检查 err 与地址。
	for _, test := range tests {
		protocol, addr, err := parseEndpoint(test.endpoint)
		assert.Equal(t, test.expectedProtocol, protocol)
		if test.expectedError {
			assert.NotNil(t, err, "Expect error during parsing %q", test.endpoint)
			continue
		}
		require.Nil(t, err, "Expect no error during parsing %q", test.endpoint)
		assert.Equal(t, test.expectedAddr, addr)
	}
}
