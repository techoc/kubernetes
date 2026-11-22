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

package hostutil

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"

	"k8s.io/utils/exec"
)

// createSocketFile 在 socketDir 中创建测试用的 Unix socket。
func createSocketFile(socketDir string) (string, error) {
	testSocketFile := filepath.Join(socketDir, "mt.sock")

	// 切到目标目录再创建 socket：socket 路径不能超过 108 个字符，只能用相对路径。
	oldDir, _ := os.Getwd()

	err := os.Chdir(socketDir)
	if err != nil {
		return "", err
	}
	defer func() {
		os.Chdir(oldDir)
	}()
	_, socketCreateError := net.Listen("unix", "mt.sock")
	return testSocketFile, socketCreateError
}

// TestGetFileType 验证 GetFileType 能正确识别目录、普通文件、socket 和设备文件。
func TestGetFileType(t *testing.T) {
	hu := NewHostUtil()

	testCase := []struct {
		name         string
		skipWindows  bool
		expectedType FileType
		setUp        func() (string, string, error)
	}{
		{
			"Directory Test",
			false,
			FileTypeDirectory,
			func() (string, string, error) {
				tempDir, err := os.MkdirTemp("", "test-get-filetype-")
				return tempDir, tempDir, err
			},
		},
		{
			"File Test",
			false,
			FileTypeFile,
			func() (string, string, error) {
				tempFile, err := os.CreateTemp("", "test-get-filetype")
				if err != nil {
					return "", "", err
				}
				tempFile.Close()
				return tempFile.Name(), tempFile.Name(), nil
			},
		},
		{
			"Socket Test",
			false,
			FileTypeSocket,
			func() (string, string, error) {
				tempDir, err := os.MkdirTemp("", "test-get-filetype-")
				if err != nil {
					return "", "", err
				}
				tempSocketFile, err := createSocketFile(tempDir)
				return tempSocketFile, tempDir, err
			},
		},
		{
			"Block Device Test",
			true,
			FileTypeBlockDev,
			func() (string, string, error) {
				tempDir, err := os.MkdirTemp("", "test-get-filetype-")
				if err != nil {
					return "", "", err
				}

				tempBlockFile := filepath.Join(tempDir, "test_blk_dev")
				outputBytes, err := exec.New().Command("mknod", tempBlockFile, "b", "89", "1").CombinedOutput()
				if err != nil {
					err = fmt.Errorf("%v: %s ", err, outputBytes)
				}
				return tempBlockFile, tempDir, err
			},
		},
		{
			"Character Device Test",
			true,
			FileTypeCharDev,
			func() (string, string, error) {
				tempDir, err := os.MkdirTemp("", "test-get-filetype-")
				if err != nil {
					return "", "", err
				}

				tempCharFile := filepath.Join(tempDir, "test_char_dev")
				outputBytes, err := exec.New().Command("mknod", tempCharFile, "c", "89", "1").CombinedOutput()
				if err != nil {
					err = fmt.Errorf("%v: %s ", err, outputBytes)
				}
				return tempCharFile, tempDir, err
			},
		},
	}

	for idx, tc := range testCase {
		if goruntime.GOOS == "windows" && tc.skipWindows {
			continue
		}
		path, cleanUpPath, err := tc.setUp()
		defer os.RemoveAll(cleanUpPath) // RemoveAll 能处理空路径 ""
		if err != nil {
			// 本地能通过，但上游 CI 不允许创建这类设备文件。
			// 跳过 "Operation not permitted"，留给 e2e 测试覆盖。
			if isOperationNotPermittedError(err) {
				continue
			}
			t.Fatalf("[%d-%s] unexpected error : %v", idx, tc.name, err)
		}

		fileType, err := hu.GetFileType(path)
		if err != nil {
			t.Fatalf("[%d-%s] unexpected error : %v", idx, tc.name, err)
		}
		if fileType != tc.expectedType {
			t.Fatalf("[%d-%s] expected %s, but got %s", idx, tc.name, tc.expectedType, fileType)
		}
	}
}

// isOperationNotPermittedError 判断错误是否因权限不足被拒绝。
func isOperationNotPermittedError(err error) bool {
	return strings.Contains(err.Error(), "Operation not permitted")
}
