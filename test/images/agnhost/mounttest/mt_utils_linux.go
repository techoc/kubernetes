//go:build linux

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

package mounttest

import (
	"strconv"

	"golang.org/x/sys/unix"
)

// formatFsType 返回文件系统类型的字符串表示。
// 它会把已知的文件系统魔数转换为便于阅读的名称。
func formatFsType(fsType int64) string {
	if fsType == unix.TMPFS_MAGIC {
		return "tmpfs"
	}
	return strconv.FormatInt(fsType, 10)
}
