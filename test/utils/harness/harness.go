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

package harness

import (
	"os"
	"testing"

	"k8s.io/klog/v2"
)

// Harness 为 testing.T 添加了一些功能，特别是资源清理。
// 它内嵌了 testing.T，因此应具有相同的签名。
//
// 用法示例：
// ```
//
//	func MyTest(tt *testing.T) {
//	  t := harness.For(tt)
//	  defer t.Close()
//	  ...
//	}
//
// ```
type Harness struct {
	*testing.T
	defers []func() error
}

// For 从一个 testing.T 创建 Harness。
// 调用者必须在 Harness 上调用 Close，以便清理资源。
func For(t *testing.T) *Harness {
	h := &Harness{T: t}
	return h
}

// Close 清理所有持有的资源，应在 For 之后通过 defer 调用。
func (h *Harness) Close() {
	for _, d := range h.defers {
		if err := d(); err != nil {
			klog.Warningf("error closing harness: %v", err)
		}
	}
}

// TempDir 是 os.MkdirTemp 面向测试的包装。
// 如果无法创建临时文件，它会自动令测试失败，
// 并在 Harness 上调用 Close 时删除该临时目录。
func (h *Harness) TempDir(baseDir string, prefix string) string {
	tempDir, err := os.MkdirTemp(baseDir, prefix)
	if err != nil {
		h.Fatalf("unable to create tempdir: %v", err)
	}
	h.defers = append(h.defers, func() error {
		return os.RemoveAll(tempDir)
	})
	return tempDir
}
