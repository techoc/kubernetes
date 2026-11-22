//go:build windows

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

package subpath

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// makeLink 通过 cmd 的 mklink 创建目录符号链接。
func makeLink(link, target string) error {
	if output, err := exec.Command("cmd", "/c", "mklink", "/D", link, target).CombinedOutput(); err != nil {
		return fmt.Errorf("mklink failed: %v, link(%q) target(%q) output: %q", err, link, target, string(output))
	}
	return nil
}

// TestDoSafeMakeDir 验证创建子目录时会拒绝逃逸卷根的符号链接路径。
func TestDoSafeMakeDir(t *testing.T) {
	base, err := os.MkdirTemp("", "TestDoSafeMakeDir")
	if err != nil {
		t.Fatalf("failed to create temporary directory: %v", err)
	}

	defer os.RemoveAll(base)

	testingVolumePath := filepath.Join(base, "testingVolumePath")
	os.MkdirAll(testingVolumePath, 0755)
	defer os.RemoveAll(testingVolumePath)

	tests := []struct {
		volumePath    string
		subPath       string
		expectError   bool
		symlinkTarget string
	}{
		{
			volumePath:    testingVolumePath,
			subPath:       ``,
			expectError:   true,
			symlinkTarget: "",
		},
		{
			volumePath:    testingVolumePath,
			subPath:       filepath.Join(testingVolumePath, `x`),
			expectError:   false,
			symlinkTarget: "",
		},
		{
			volumePath:    testingVolumePath,
			subPath:       filepath.Join(testingVolumePath, `a\b\c\d`),
			expectError:   false,
			symlinkTarget: "",
		},
		{
			volumePath:    testingVolumePath,
			subPath:       filepath.Join(testingVolumePath, `symlink`),
			expectError:   false,
			symlinkTarget: base,
		},
		{
			volumePath:    testingVolumePath,
			subPath:       filepath.Join(testingVolumePath, `symlink\c\d`),
			expectError:   true,
			symlinkTarget: "",
		},
		{
			volumePath:    testingVolumePath,
			subPath:       filepath.Join(testingVolumePath, `symlink\y926`),
			expectError:   true,
			symlinkTarget: "",
		},
		{
			volumePath:    testingVolumePath,
			subPath:       filepath.Join(testingVolumePath, `a\b\symlink`),
			expectError:   false,
			symlinkTarget: base,
		},
		{
			volumePath:    testingVolumePath,
			subPath:       filepath.Join(testingVolumePath, `a\x\symlink`),
			expectError:   false,
			symlinkTarget: filepath.Join(testingVolumePath, `a`),
		},
	}

	for _, test := range tests {
		if len(test.volumePath) > 0 && len(test.subPath) > 0 && len(test.symlinkTarget) > 0 {
			// 先创建所有父目录。
			if parent := filepath.Dir(test.subPath); parent != "." {
				os.MkdirAll(parent, 0755)
			}

			// 将路径最后一段创建为符号链接。
			linkPath := test.subPath
			if _, err := os.Stat(linkPath); err != nil && os.IsNotExist(err) {
				if err := makeLink(linkPath, test.symlinkTarget); err != nil {
					t.Fatalf("unexpected error: %v", fmt.Errorf("mklink link(%q) target(%q) error: %q", linkPath, test.symlinkTarget, err))
				}
			}
		}

		err := doSafeMakeDir(test.subPath, test.volumePath, os.FileMode(0755))
		if test.expectError {
			assert.NotNil(t, err, "Expect error during doSafeMakeDir(%s, %s)", test.subPath, test.volumePath)
			continue
		}
		assert.Nil(t, err, "Expect no error during doSafeMakeDir(%s, %s)", test.subPath, test.volumePath)
		if _, err := os.Stat(test.subPath); os.IsNotExist(err) {
			t.Errorf("subPath should exists after doSafeMakeDir(%s, %s)", test.subPath, test.volumePath)
		}
	}
}

// TestLockAndCheckSubPath 验证路径锁定和符号链接逃逸检查返回的句柄数。
func TestLockAndCheckSubPath(t *testing.T) {
	base, err := os.MkdirTemp("", "TestLockAndCheckSubPath")
	if err != nil {
		t.Fatalf("failed to create temporary directory: %v", err)
	}

	defer os.RemoveAll(base)

	testingVolumePath := filepath.Join(base, "testingVolumePath")

	tests := []struct {
		volumePath          string
		subPath             string
		expectedHandleCount int
		expectError         bool
		symlinkTarget       string
	}{
		{
			volumePath:          `c:\`,
			subPath:             ``,
			expectedHandleCount: 0,
			expectError:         false,
			symlinkTarget:       "",
		},
		{
			volumePath:          ``,
			subPath:             `a`,
			expectedHandleCount: 0,
			expectError:         false,
			symlinkTarget:       "",
		},
		{
			volumePath:          testingVolumePath,
			subPath:             filepath.Join(testingVolumePath, `a`),
			expectedHandleCount: 1,
			expectError:         false,
			symlinkTarget:       "",
		},
		{
			volumePath:          testingVolumePath,
			subPath:             filepath.Join(testingVolumePath, `a\b\c\d`),
			expectedHandleCount: 4,
			expectError:         false,
			symlinkTarget:       "",
		},
		{
			volumePath:          testingVolumePath,
			subPath:             filepath.Join(testingVolumePath, `symlink`),
			expectedHandleCount: 0,
			expectError:         true,
			symlinkTarget:       base,
		},
		{
			volumePath:          testingVolumePath,
			subPath:             filepath.Join(testingVolumePath, `a\b\c\symlink`),
			expectedHandleCount: 0,
			expectError:         true,
			symlinkTarget:       base,
		},
		{
			volumePath:          testingVolumePath,
			subPath:             filepath.Join(testingVolumePath, `a\b\c\d\symlink`),
			expectedHandleCount: 2,
			expectError:         false,
			symlinkTarget:       filepath.Join(testingVolumePath, `a\b`),
		},
	}

	for _, test := range tests {
		if len(test.volumePath) > 0 && len(test.subPath) > 0 {
			os.MkdirAll(test.volumePath, 0755)
			if len(test.symlinkTarget) == 0 {
				// 创建所有中间目录。
				os.MkdirAll(test.subPath, 0755)
			} else {
				// 先创建所有父目录。
				if parent := filepath.Dir(test.subPath); parent != "." {
					os.MkdirAll(parent, 0755)
				}

				// 将路径最后一段创建为符号链接。
				linkPath := test.subPath
				if _, err := os.Stat(linkPath); err != nil && os.IsNotExist(err) {
					if err := makeLink(linkPath, test.symlinkTarget); err != nil {
						t.Fatalf("unexpected error: %v", fmt.Errorf("mklink link(%q) target(%q) error: %q", linkPath, test.symlinkTarget, err))
					}
				}
			}
		}

		fileHandles, err := lockAndCheckSubPath(test.volumePath, test.subPath)
		unlockPath(fileHandles)
		assert.Equal(t, test.expectedHandleCount, len(fileHandles))
		if test.expectError {
			assert.NotNil(t, err, "Expect error during LockAndCheckSubPath(%s, %s)", test.volumePath, test.subPath)
			continue
		}
		assert.Nil(t, err, "Expect no error during LockAndCheckSubPath(%s, %s)", test.volumePath, test.subPath)
	}

	// 先关闭所有文件句柄，才能删除目录。
	assert.Nil(t, os.RemoveAll(testingVolumePath), "Expect no error during remove dir %s", testingVolumePath)
}

// TestLockAndCheckSubPathWithoutSymlink 验证不解析符号链接时仍返回各路径分量的句柄。
func TestLockAndCheckSubPathWithoutSymlink(t *testing.T) {
	base, err := os.MkdirTemp("", "TestLockAndCheckSubPathWithoutSymlink")
	if err != nil {
		t.Fatalf("failed to create temporary directory: %v", err)
	}

	defer os.RemoveAll(base)

	testingVolumePath := filepath.Join(base, "testingVolumePath")

	tests := []struct {
		volumePath          string
		subPath             string
		expectedHandleCount int
		expectError         bool
		symlinkTarget       string
	}{
		{
			volumePath:          `c:\`,
			subPath:             ``,
			expectedHandleCount: 0,
			expectError:         false,
			symlinkTarget:       "",
		},
		{
			volumePath:          ``,
			subPath:             `a`,
			expectedHandleCount: 0,
			expectError:         false,
			symlinkTarget:       "",
		},
		{
			volumePath:          testingVolumePath,
			subPath:             filepath.Join(testingVolumePath, `a`),
			expectedHandleCount: 1,
			expectError:         false,
			symlinkTarget:       "",
		},
		{
			volumePath:          testingVolumePath,
			subPath:             filepath.Join(testingVolumePath, `a\b\c\d`),
			expectedHandleCount: 4,
			expectError:         false,
			symlinkTarget:       "",
		},
		{
			volumePath:          testingVolumePath,
			subPath:             filepath.Join(testingVolumePath, `symlink`),
			expectedHandleCount: 1,
			expectError:         true,
			symlinkTarget:       base,
		},
		{
			volumePath:          testingVolumePath,
			subPath:             filepath.Join(testingVolumePath, `a\b\c\symlink`),
			expectedHandleCount: 4,
			expectError:         true,
			symlinkTarget:       base,
		},
		{
			volumePath:          testingVolumePath,
			subPath:             filepath.Join(testingVolumePath, `a\b\c\d\symlink`),
			expectedHandleCount: 5,
			expectError:         true,
			symlinkTarget:       filepath.Join(testingVolumePath, `a\b`),
		},
	}

	for _, test := range tests {
		if len(test.volumePath) > 0 && len(test.subPath) > 0 {
			os.MkdirAll(test.volumePath, 0755)
			if len(test.symlinkTarget) == 0 {
				// 创建所有中间目录。
				os.MkdirAll(test.subPath, 0755)
			} else {
				// 先创建所有父目录。
				if parent := filepath.Dir(test.subPath); parent != "." {
					os.MkdirAll(parent, 0755)
				}

				// 将路径最后一段创建为符号链接。
				linkPath := test.subPath
				if _, err := os.Stat(linkPath); err != nil && os.IsNotExist(err) {
					if err := makeLink(linkPath, test.symlinkTarget); err != nil {
						t.Fatalf("unexpected error: %v", fmt.Errorf("mklink link(%q) target(%q) error: %q", linkPath, test.symlinkTarget, err))
					}
				}
			}
		}

		fileHandles, err := lockAndCheckSubPathWithoutSymlink(test.volumePath, test.subPath)
		unlockPath(fileHandles)
		assert.Equal(t, test.expectedHandleCount, len(fileHandles))
		if test.expectError {
			assert.NotNil(t, err, "Expect error during LockAndCheckSubPath(%s, %s)", test.volumePath, test.subPath)
			continue
		}
		assert.Nil(t, err, "Expect no error during LockAndCheckSubPath(%s, %s)", test.volumePath, test.subPath)
	}

	// 先关闭所有文件句柄，才能删除目录。
	assert.Nil(t, os.RemoveAll(testingVolumePath), "Expect no error during remove dir %s", testingVolumePath)
}

// TestFindExistingPrefix 验证查找已有路径前缀和待创建目录列表。
func TestFindExistingPrefix(t *testing.T) {
	base, err := os.MkdirTemp("", "TestFindExistingPrefix")
	if err != nil {
		t.Fatalf("failed to create temporary directory: %v", err)
	}

	defer os.RemoveAll(base)

	testingVolumePath := filepath.Join(base, "testingVolumePath")

	tests := []struct {
		base                    string
		pathname                string
		expectError             bool
		expectedExistingPath    string
		expectedToCreateDirs    []string
		createSubPathBeforeTest bool
	}{
		{
			base:                    `c:\tmp\a`,
			pathname:                `c:\tmp\b`,
			expectError:             true,
			expectedExistingPath:    "",
			expectedToCreateDirs:    []string{},
			createSubPathBeforeTest: false,
		},
		{
			base:                    ``,
			pathname:                `c:\tmp\b`,
			expectError:             true,
			expectedExistingPath:    "",
			expectedToCreateDirs:    []string{},
			createSubPathBeforeTest: false,
		},
		{
			base:                    `c:\tmp\a`,
			pathname:                `d:\tmp\b`,
			expectError:             true,
			expectedExistingPath:    "",
			expectedToCreateDirs:    []string{},
			createSubPathBeforeTest: false,
		},
		{
			base:                    testingVolumePath,
			pathname:                testingVolumePath,
			expectError:             false,
			expectedExistingPath:    testingVolumePath,
			expectedToCreateDirs:    []string{},
			createSubPathBeforeTest: false,
		},
		{
			base:                    testingVolumePath,
			pathname:                filepath.Join(testingVolumePath, `a\b`),
			expectError:             false,
			expectedExistingPath:    filepath.Join(testingVolumePath, `a\b`),
			expectedToCreateDirs:    []string{},
			createSubPathBeforeTest: true,
		},
		{
			base:                    testingVolumePath,
			pathname:                filepath.Join(testingVolumePath, `a\b\c\`),
			expectError:             false,
			expectedExistingPath:    filepath.Join(testingVolumePath, `a\b`),
			expectedToCreateDirs:    []string{`c`},
			createSubPathBeforeTest: false,
		},
		{
			base:                    testingVolumePath,
			pathname:                filepath.Join(testingVolumePath, `a\b\c\d`),
			expectError:             false,
			expectedExistingPath:    filepath.Join(testingVolumePath, `a\b`),
			expectedToCreateDirs:    []string{`c`, `d`},
			createSubPathBeforeTest: false,
		},
	}

	for _, test := range tests {
		if test.createSubPathBeforeTest {
			os.MkdirAll(test.pathname, 0755)
		}

		existingPath, toCreate, err := findExistingPrefix(test.base, test.pathname)
		if test.expectError {
			assert.NotNil(t, err, "Expect error during findExistingPrefix(%s, %s)", test.base, test.pathname)
			continue
		}
		assert.Nil(t, err, "Expect no error during findExistingPrefix(%s, %s)", test.base, test.pathname)

		assert.Equal(t, test.expectedExistingPath, existingPath, "Expect result not equal with findExistingPrefix(%s, %s) return: %q, expected: %q",
			test.base, test.pathname, existingPath, test.expectedExistingPath)

		assert.Equal(t, test.expectedToCreateDirs, toCreate, "Expect result not equal with findExistingPrefix(%s, %s) return: %q, expected: %q",
			test.base, test.pathname, toCreate, test.expectedToCreateDirs)

	}
	// 先关闭所有文件句柄，才能删除目录。
	assert.Nil(t, os.RemoveAll(testingVolumePath), "Expect no error during remove dir %s", testingVolumePath)
}

// TestIsDriveLetterorEmptyPath 验证盘符路径和空路径的识别。
func TestIsDriveLetterorEmptyPath(t *testing.T) {
	tests := []struct {
		path           string
		expectedResult bool
	}{
		{
			path:           ``,
			expectedResult: true,
		},
		{
			path:           `\tmp`,
			expectedResult: false,
		},
		{
			path:           `c:\tmp`,
			expectedResult: false,
		},
		{
			path:           `c:\\`,
			expectedResult: true,
		},
		{
			path:           `c:\`,
			expectedResult: true,
		},
		{
			path:           `c:`,
			expectedResult: true,
		},
	}

	for _, test := range tests {
		result := isDriveLetterorEmptyPath(test.path)
		assert.Equal(t, test.expectedResult, result, "Expect result not equal with isDriveLetterorEmptyPath(%s) return: %t, expected: %t",
			test.path, result, test.expectedResult)
	}
}

// TestIsDeviceOrUncPath 验证设备路径和 UNC 路径的识别，防止被解析触发 NTLM 认证。
func TestIsDeviceOrUncPath(t *testing.T) {
	tests := []struct {
		path           string
		expectedResult bool
	}{
		{
			// 普通本地路径应可正常解析。
			path:           `c:\tmp\foo`,
			expectedResult: false,
		},
		{
			// 空路径不是设备/UNC 路径。
			path:           ``,
			expectedResult: false,
		},
		{
			// UNC 网络路径必须拒绝解析，否则解析时会触发强制 NTLM 认证。
			path:           `\\attacker\share`,
			expectedResult: true,
		},
		{
			// 使用 IP 的 UNC 网络路径。
			path:           `\\127.0.0.1\share\dir`,
			expectedResult: true,
		},
		{
			// 正斜杠 UNC 形式：evalSymlink 在正常化时会将 "/" 转为 "\\"，
			// 此处显式断言可防止调用方传入未规范化的目标。
			path:           `//attacker/share`,
			expectedResult: true,
		},
		{
			// 设备形式的 UNC 路径（扩展长度前缀）。
			path:           `\\?\UNC\server\share`,
			expectedResult: true,
		},
		{
			// 设备命名空间路径。
			path:           `\\.\PhysicalDrive0`,
			expectedResult: true,
		},
		{
			// 扩展长度的本地路径。
			path:           `\\?\c:\tmp`,
			expectedResult: true,
		},
		{
			// 去掉前缀的设备形式 UNC 路径。
			path:           `UNC\server\share`,
			expectedResult: true,
		},
		{
			// 卷 GUID 路径。
			path:           `Volume{00000000-0000-0000-0000-000000000000}\`,
			expectedResult: true,
		},
	}

	for _, test := range tests {
		result := isDeviceOrUncPath(test.path)
		assert.Equal(t, test.expectedResult, result, "Expect result not equal with isDeviceOrUncPath(%s) return: %t, expected: %t",
			test.path, result, test.expectedResult)
	}
}
