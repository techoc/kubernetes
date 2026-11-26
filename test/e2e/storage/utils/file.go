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

package utils

import (
	"fmt"
	"hash/crc32"
)

// ntfs、ext4、xfs 和 btrfs 支持的最大长度。
const maxFileNameLength = 255

// 把文件名缩短到最常见文件系统允许的长度。
// 文件名过长时截断，并加上短 hash（crc32）保证唯一。
// 注意输入应是单个文件/目录名，而不是由多个目录
// 组成的路径。
func ShortenFileName(filename string) string {
	if len(filename) <= maxFileNameLength {
		return filename
	}

	hash := crc32.ChecksumIEEE([]byte(filename))
	hashString := fmt.Sprintf("%x", hash)
	hashLen := len(hashString)

	return fmt.Sprintf("%s-%s", filename[:maxFileNameLength-1-hashLen], hashString)
}
