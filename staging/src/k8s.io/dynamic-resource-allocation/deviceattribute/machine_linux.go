/*
Copyright 2025 The Kubernetes Authors.

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
	"os"
)

const (
	// SysfsRoot 是 Linux 上 sysfs 伪文件系统的默认挂载点。
	SysfsRoot = "/sys"
)

// initDefaultMachine 把 mc 的 sysfs 初始化为以 SysfsRoot 为根的只读文件系统。
func initDefaultMachine(mc *machine) {
	// 通过 os.DirFS 打开 /sys，并断言其实现 ReadLinkFS 以支持读取符号链接。
	mc.sysfs = os.DirFS(SysfsRoot).(fs.ReadLinkFS)
}
