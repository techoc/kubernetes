/*
Copyright 2016 The Kubernetes Authors.

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

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestExtractFromBadDataFile 验证无法解析的配置文件会返回错误且不发布更新。
func TestExtractFromBadDataFile(t *testing.T) {
	dirName, err := mkTempDir("file-test")
	if err != nil {
		t.Fatalf("unable to create temp dir: %v", err)
	}
	defer removeAll(dirName, t)

	logger, _ := ktesting.NewTestContext(t)
	fileName := filepath.Join(dirName, "test_pod_config")
	err = os.WriteFile(fileName, []byte{1, 2, 3}, 0555)
	if err != nil {
		t.Fatalf("unable to write test file %#v", err)
	}

	// 使用带缓冲通道，避免发送阻塞测试。
	ch := make(chan sourceUpdate, 1)
	// 以毫秒为周期创建文件配置源。
	lw := newSourceFile(fileName, "localhost", time.Millisecond, ch)
	err = lw.listConfig(logger)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	expectEmptyChannel(t, ch)
}

// TestExtractFromEmptyDir 验证空目录会发布一个空的配置更新。
func TestExtractFromEmptyDir(t *testing.T) {
	dirName, err := mkTempDir("file-test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer removeAll(dirName, t)

	logger, _ := ktesting.NewTestContext(t)
	ch := make(chan sourceUpdate, 1)
	// 以毫秒为周期创建文件配置源。
	lw := newSourceFile(dirName, "localhost", time.Millisecond, ch)
	err = lw.listConfig(logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	update, ok := <-ch
	if !ok {
		t.Fatalf("unexpected type: %#v", update)
	}
	expected := createSourceUpdate() // 预期收到空更新。
	if !apiequality.Semantic.DeepEqual(expected, update) {
		t.Fatalf("expected %#v, got %#v", expected, update)
	}
}

// mkTempDir 创建带指定前缀的临时目录。
func mkTempDir(prefix string) (string, error) {
	return os.MkdirTemp(os.TempDir(), prefix)
}

// removeAll 删除临时目录，失败时终止测试。
func removeAll(dir string, t *testing.T) {
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("unable to remove dir %s: %v", dir, err)
	}
}
