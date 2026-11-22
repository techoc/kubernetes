/*
Copyright 2021 The Kubernetes Authors.

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

package routes

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestPreCheckLogFileNameLength 验证 logFileNameIsTooLong 函数能正确区分超长文件名和正常文件名。
// 测试三种情况：
//
//  1. 超长文件名（32768字符）应返回 true
//  2. 正常长度但不存在的文件名应返回 false（os.Stat 返回 ENOENT 而非 ENAMETOOLONG）
//  3. 正常长度且存在的文件应返回 false
func TestPreCheckLogFileNameLength(t *testing.T) {
	// Windows 启用长文件名支持后，文件名可达 32,767 字符，所以用 32768 字符确保超长
	oversizeFileName := fmt.Sprintf("%032768s", "a")
	normalFileName := fmt.Sprintf("%0255s", "a")

	// 检查超长文件名应被识别
	if !logFileNameIsTooLong(oversizeFileName) {
		t.Error("failed to check oversize filename")
	}

	// 检查正常长度但不存在的文件名不应被识别为超长
	if logFileNameIsTooLong(normalFileName) {
		t.Error("failed to check normal filename")
	}

	// 检查正常长度且实际存在的文件不应被识别为超长
	dir, err := os.MkdirTemp("", "logs")
	if err != nil {
		t.Fatal("failed to create temp dir")
	}
	defer os.RemoveAll(dir)

	normalFileName = filepath.Join(dir, normalFileName)
	f, err := os.Create(normalFileName)
	if err != nil {
		t.Error("failed to create test file")
	}
	defer os.Remove(normalFileName)
	defer f.Close()
	if logFileNameIsTooLong(normalFileName) {
		t.Error("failed to check normal filename")
	}
}
