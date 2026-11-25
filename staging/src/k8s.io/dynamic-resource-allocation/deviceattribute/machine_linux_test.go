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
	"path/filepath"
	"testing"
)

// TestWithFSFromRoot 验证 WithFSFromRoot 以真实 /sys 为根创建的文件系统可读。
// 测试输入为固定的 /sys 路径，读取其中自 sysfs 早期就存在的标准文件 devices/system/cpu/online；
// 断言读取成功且内容非空。
func TestWithFSFromRoot(t *testing.T) {
	mc := machine{}
	// 使用真实 /sys 作为根，仅读取长期稳定存在的标准文件以避免环境差异。
	mod := WithFSFromRoot("/sys")
	mod(&mc)

	srcPath := filepath.Join("devices", "system", "cpu", "online")
	got, err := fs.ReadFile(mc.sysfs, srcPath)
	if err != nil {
		t.Fatalf("cannot read %q: %v", srcPath, err)
	}
	// 断言读到的 CPU 在线列表非空。
	if len(got) == 0 {
		t.Fatalf("empty content in %q", srcPath)
	}
}
