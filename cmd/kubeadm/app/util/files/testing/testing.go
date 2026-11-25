/*
Copyright The Kubernetes Authors.

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

// Package testing 包含在测试中管理 kubeadm 文件的工具。
package testing

import (
	"os"
	"path/filepath"
	"testing"
)

// SetupEmptyFiles 创建一个或多个空文件（touch），供 kubeadm 测试使用。
func SetupEmptyFiles(t *testing.T, tmpdir string, fileNames ...string) {
	for _, fileName := range fileNames {
		newFile, err := os.Create(filepath.Join(tmpdir, fileName))
		if err != nil {
			t.Fatalf("Error creating file %s in %s: %v", fileName, tmpdir, err)
		}
		newFile.Close()
	}
}

// AssertFilesCount 断言给定目录包含指定数量的文件，供 kubeadm 测试使用。
func AssertFilesCount(t *testing.T, dirName string, count int) {
	files, err := os.ReadDir(dirName)
	if err != nil {
		t.Fatalf("Couldn't read files from tmpdir: %s", err)
	}

	countFiles := 0
	for _, f := range files {
		if !f.IsDir() {
			countFiles++
		}
	}

	if countFiles != count {
		t.Errorf("dir does contains %d, %d expected", len(files), count)
		for _, f := range files {
			t.Error(f.Name())
		}
	}
}

// AssertFileExists 断言给定目录包含指定文件，供 kubeadm 测试使用。
func AssertFileExists(t *testing.T, dirName string, fileNames ...string) {
	for _, fileName := range fileNames {
		path := filepath.Join(dirName, fileName)

		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("file %s does not exist", fileName)
		}
	}
}
