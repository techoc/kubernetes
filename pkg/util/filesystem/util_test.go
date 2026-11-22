/*
Copyright 2023 The Kubernetes Authors.

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

package filesystem

import (
	"net"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIsUnixDomainSocket 覆盖 Unix 套接字文件、普通文件和不存在的文件三种情况，
// 验证检测结果与错误行为。
func TestIsUnixDomainSocket(t *testing.T) {
	tests := []struct {
		label          string
		listenOnSocket bool
		expectSocket   bool
		expectError    bool
		invalidFile    bool
	}{
		{
			label:          "Domain Socket file",
			listenOnSocket: true,
			expectSocket:   true,
			expectError:    false,
		},
		{
			label:       "Non Existent file",
			invalidFile: true,
			expectError: true,
		},
		{
			label:          "Regular file",
			listenOnSocket: false,
			expectSocket:   false,
			expectError:    false,
		},
	}
	for _, test := range tests {
		f, err := os.CreateTemp("", "test-domain-socket")
		require.NoErrorf(t, err, "Failed to create file for test purposes: %v while setting up: %s", err, test.label)
		addr := f.Name()
		f.Close()
		var ln *net.UnixListener
		if test.listenOnSocket {
			os.Remove(addr)
			ta, err := net.ResolveUnixAddr("unix", addr)
			require.NoErrorf(t, err, "Failed to ResolveUnixAddr: %v while setting up: %s", err, test.label)
			ln, err = net.ListenUnix("unix", ta)
			require.NoErrorf(t, err, "Failed to ListenUnix: %v while setting up: %s", err, test.label)
		}
		fileToTest := addr
		if test.invalidFile {
			fileToTest = fileToTest + ".invalid"
		}
		result, err := IsUnixDomainSocket(fileToTest)
		if test.listenOnSocket {
			// 关闭监听器会同时删除套接字对应的文件
			ln.Close()
		} else {
			// 普通文件需要显式删除
			os.Remove(addr)
		}
		if test.expectError {
			assert.Errorf(t, err, "Unexpected nil error from IsUnixDomainSocket for %s", test.label)
		} else {
			assert.NoErrorf(t, err, "Unexpected error invoking IsUnixDomainSocket for %s", test.label)
		}
		assert.Equal(t, result, test.expectSocket, "Unexpected result from IsUnixDomainSocket: %v for %s", result, test.label)
	}
}

// TestIsCleanPath 验证 IsPathClean 对统一形式与含 ".."、"//" 等非规范路径的判断，
// 并在 Windows 上追加盘符和反斜杠相关的用例。
func TestIsCleanPath(t *testing.T) {
	type Case struct {
		path     string
		expected bool
	}

	// 参考 https://github.com/kubernetes/kubernetes/pull/124084/files#r1557566941
	cases := []Case{
		{path: "/logs", expected: true},
		{path: "/var/lib/something", expected: true},
		{path: "var/lib/something", expected: true},
		{path: "var\\lib\\something", expected: true},
		{path: "/", expected: true},
		{path: ".", expected: true},
		{path: "/var/../something", expected: false},
		{path: "/var//lib/something", expected: false},
		{path: "/var/./lib/something", expected: false},
	}

	// 仅在 Windows 上适用的额外用例
	if runtime.GOOS == "windows" {
		cases = append(cases, []Case{
			{path: "\\", expected: true},
			{path: "C:/var/lib/something", expected: true},
			{path: "C:\\var\\lib\\something", expected: true},
			{path: "C:/", expected: true},
			{path: "C:\\", expected: true},
			{path: "C:/var//lib/something", expected: false},
			{path: "\\var\\\\lib\\something", expected: false},
			{path: "C:\\var\\\\lib\\something", expected: false},
			{path: "C:\\var\\..\\something", expected: false},
			{path: "\\var\\.\\lib\\something", expected: false},
			{path: "C:\\var\\.\\lib\\something", expected: false},
		}...)
	}

	for _, tc := range cases {
		actual := IsPathClean(tc.path)
		if actual != tc.expected {
			t.Errorf("actual: %t, expected: %t, for path: %s\n", actual, tc.expected, tc.path)
		}
	}

}
