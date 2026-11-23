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

package deviceattribute

import (
	"io/fs"
	"testing"
	"testing/fstest"
)

// TestWithFS 验证 WithFS 选项确实把调用方提供的内存文件系统注入到 machine 中。
// 测试输入为一个包含单文件（带指定内容与文件模式）的 fstest.MapFS；
// 断言通过 mc.sysfs.Lstat 能读到该文件，且大小与模式与注入值一致。
func TestWithFS(t *testing.T) {
	mc := machine{}
	path := "dra.test"
	content := "DRA deviceattribute"
	mode := fs.FileMode(0o640) // any random mode pattern which is not the default
	// 构造仅含一个文件的内存文件系统，内容与模式均作为校验基准。
	fakefs := fstest.MapFS{
		path: &fstest.MapFile{
			Data: []byte(content),
			Mode: mode,
		},
	}

	// 应用选项后，machine 的 sysfs 应指向注入的内存文件系统。
	mod := WithFS(fakefs)
	mod(&mc)

	got, err := mc.sysfs.Lstat(path)
	if err != nil {
		t.Fatalf("cannot Lstat %q: %v", path, err)
	}
	// 断言文件大小与内容长度一致。
	if got.Size() != int64(len(content)) {
		t.Fatalf("did not found content: got %v expected %v", got.Size(), content)
	}
	// 断言文件模式未被改变。
	if got.Mode() != mode {
		t.Fatalf("found mismatched file mode: got %v expected %v", got.Mode(), mode)
	}
}
