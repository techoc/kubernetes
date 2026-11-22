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

package sharding

import (
	"fmt"
	"hash/fnv"
)

// HashField 计算 value 的哈希，并以 16 字符的小写十六进制字符串返回（不带 "0x" 前缀）。
// 使用 FNV-1a 64 位哈希，追求的是计算快、分布均匀，而非密码学安全性；
// 固定宽度（%016x 左补零）保证不同长度的哈希可以直接按字符串比较或作为分片键。
func HashField(value string) string {
	h := fnv.New64a()
	// 写入字符串的字节表示；Hash.Write 不会返回错误，因此忽略其返回值。
	h.Write([]byte(value))
	return fmt.Sprintf("%016x", h.Sum64())
}
