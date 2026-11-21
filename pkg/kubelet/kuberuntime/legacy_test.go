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

package kuberuntime

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// letterBytes 是随机字符串使用的字母表。
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// randStringBytes 生成指定长度的随机字符串。
func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// TestLogSymLink 验证日志软链接路径的构造与长度截断。
func TestLogSymLink(t *testing.T) {
	as := assert.New(t)
	containerLogsDir := "/foo/bar"
	podFullName := randStringBytes(128)
	containerName := randStringBytes(70)
	containerID := randStringBytes(80)
	// 文件名不能超过 255 字符；由于必须保留 .log 后缀，前缀最长 251 字符。
	expectedPath := filepath.Join(containerLogsDir, fmt.Sprintf("%s_%s-%s", podFullName, containerName, containerID)[:251]+".log")
	as.Equal(expectedPath, logSymlink(containerLogsDir, podFullName, containerName, containerID))
}

// TestLegacyLogSymLink 验证旧版日志软链接路径的构造与长度截断。
func TestLegacyLogSymLink(t *testing.T) {
	as := assert.New(t)
	containerID := randStringBytes(80)
	containerName := randStringBytes(70)
	podName := randStringBytes(128)
	podNamespace := randStringBytes(10)
	// 文件名不能超过 255 字符；由于必须保留 .log 后缀，前缀最长 251 字符。
	expectedPath := filepath.Join(legacyContainerLogsDir, fmt.Sprintf("%s_%s_%s-%s", podName, podNamespace, containerName, containerID)[:251]+".log")
	as.Equal(expectedPath, legacyLogSymlink(containerID, containerName, podName, podNamespace))
}

// TestGetContainerIDFromLegacyLogSymLink 验证从旧版日志软链接中解析容器 ID。
func TestGetContainerIDFromLegacyLogSymLink(t *testing.T) {
	containerID := randStringBytes(80)
	containerName := randStringBytes(70)
	podName := randStringBytes(128)
	podNamespace := randStringBytes(10)

	// 覆盖缺少分隔符、非法后缀、过短容器 ID 与合法路径。
	for _, test := range []struct {
		name        string
		logSymLink  string
		expected    string
		shouldError bool
	}{
		{
			name:        "unable to find separator",
			logSymLink:  "dummy.log",
			expected:    "",
			shouldError: true,
		},
		{
			name:        "invalid suffix",
			logSymLink:  filepath.Join(legacyContainerLogsDir, fmt.Sprintf("%s_%s_%s-%s", podName, podNamespace, containerName, containerID)[:251]+".invalidsuffix"),
			expected:    "",
			shouldError: true,
		},
		{
			name:        "container ID too short",
			logSymLink:  filepath.Join(legacyContainerLogsDir, fmt.Sprintf("%s_%s_%s-%s", podName, podNamespace, containerName, containerID[:5])+".log"),
			expected:    "",
			shouldError: true,
		},
		{
			name:        "valid path",
			logSymLink:  filepath.Join(legacyContainerLogsDir, fmt.Sprintf("%s_%s_%s-%s", podName, podNamespace, containerName, containerID)[:251]+".log"),
			expected:    containerID[:40],
			shouldError: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// 仅合法路径能解析出容器 ID。
			containerID, err := getContainerIDFromLegacyLogSymlink(test.logSymLink)
			if test.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, test.expected, containerID)
		})
	}
}
