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

package testing

import (
	"os"
	"testing"
)

// CloseAndRemove 关闭并删除测试文件。
//
// nil 文件会被跳过，方便 defer 中传入可能未创建成功的文件句柄。
func CloseAndRemove(t *testing.T, files ...*os.File) {
	t.Helper()
	// 删除前必须先关闭；这不只是良好习惯，也能避免 Windows 上删除失败。
	for _, f := range files {
		if f == nil {
			continue
		}
		if err := f.Close(); err != nil {
			t.Fatalf("Error closing %s: %v", f.Name(), err)
		}
		if err := os.Remove(f.Name()); err != nil {
			t.Fatalf("Error removing %s: %v", f.Name(), err)
		}
	}
}
