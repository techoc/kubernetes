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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/cri-client/pkg/util"
)

// TestNormalizePath 验证 Windows 路径归一化会补齐盘符并统一分隔符。
func TestNormalizePath(t *testing.T) {
	tests := []struct {
		originalpath   string
		normalizedPath string
	}{
		{
			originalpath:   "\\path\\to\\file",
			normalizedPath: "c:\\path\\to\\file",
		},
		{
			originalpath:   "/path/to/file",
			normalizedPath: "c:\\path\\to\\file",
		},
		{
			originalpath:   "/path/to/dir/",
			normalizedPath: "c:\\path\\to\\dir\\",
		},
		{
			originalpath:   "\\path\\to\\dir\\",
			normalizedPath: "c:\\path\\to\\dir\\",
		},
		{
			originalpath:   "/file",
			normalizedPath: "c:\\file",
		},
		{
			originalpath:   "\\file",
			normalizedPath: "c:\\file",
		},
		{
			originalpath:   "fileonly",
			normalizedPath: "fileonly",
		},
	}

	// 逐个用例校验路径归一化结果。
	for _, test := range tests {
		assert.Equal(t, test.normalizedPath, NormalizePath(test.originalpath))
	}
}

// TestLocalEndpoint 验证本地 endpoint 会被转换为 Windows npipe 地址。
func TestLocalEndpoint(t *testing.T) {
	tests := []struct {
		path             string
		file             string
		expectError      bool
		expectedFullPath string
	}{
		{
			path:             "/var/lib/kubelet/pod-resources",
			file:             "kube.sock", // 这不是默认文件名，但此处无关紧要。
			expectError:      false,
			expectedFullPath: `npipe://\\.\pipe\kubelet-pod-resources`,
		},
	}
	// 逐个用例校验本地 endpoint 的解析结果。
	for _, test := range tests {
		// 先构造本地 endpoint，再解析回地址与 dialer。
		fullPath, err := LocalEndpoint(test.path, test.file)
		if test.expectError {
			assert.NotNil(t, err, "expected error")
			continue
		}
		assert.Nil(t, err, "expected no error")
		assert.Equal(t, test.expectedFullPath, fullPath)
	}
}

// TestLocalEndpointRoundTrip 验证 endpoint 构造与解析的往返一致性。
func TestLocalEndpointRoundTrip(t *testing.T) {
	// 通过反射取得预期 dialer 的函数名。
	functionPointer := reflect.ValueOf(util.GetAddressAndDialer).Pointer()
	functionName := runtime.FuncForPC(functionPointer).Name()
	expectedDialerName := fmt.Sprintf("%s.npipeDial", functionName[:strings.LastIndex(functionName, ".")])
	expectedAddress := "//./pipe/kubelet-pod-resources"

	fullPath, err := LocalEndpoint(`pod-resources`, "kubelet")
	require.NoErrorf(t, err, "Failed to create the local endpoint path")

	address, dialer, err := util.GetAddressAndDialer(fullPath)
	require.NoErrorf(t, err, "Failed to parse the endpoint path and get back address and dialer (path=%q)", fullPath)

	dialerPointer := reflect.ValueOf(dialer).Pointer()
	actualDialerName := runtime.FuncForPC(dialerPointer).Name()

	assert.Equal(t, expectedDialerName, actualDialerName)
	assert.Equal(t, expectedAddress, address)
}
