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

// Package resourceversion 提供对 etcd resourceVersion 字符串的比较工具。
//
// resourceVersion 是十进制正整数，且不保证连续；由于可能超过 int64，
// 这里按"先比长度、再按字典序"的方式比较，避免整数溢出。
package resourceversion

import (
	"fmt"
	"strings"
)

// InvalidResourceVersion 表示传入的 resourceVersion 不满足格式要求。
// 错误中保留原始字符串，方便排查客户端传参问题。
type InvalidResourceVersion struct {
	rv string
}

func (i InvalidResourceVersion) Error() string {
	return fmt.Sprintf("resource version is not well formed: %s", i.rv)
}

// CompareResourceVersion 比较两个 ResourceVersion 的大小。只有两个版本号属于同一种资源时，
// 比较结果才具有语义意义。返回值含义如下：
//
//	-1：a 小于 b
//	 0：a 等于 b
//	+1：a 大于 b
//
// resourceVersion 必须是格式正确的正十进制整数，否则返回错误；长度不设上限。
// 合法值不能包含前导零或非数字字符。0 也视为非法，因为它在 list/watch 事件中
// 用作特殊值，不会作为真实资源版本号出现。
//
// 关键不变量：格式良好的十进制串没有前导零，因此"位数更多 = 数值更大"，
// 位数相同则字典序与数值序一致；这样无需 strconv 就能支持任意长度。
func CompareResourceVersion(a, b string) (int, error) {
	if !isWellFormed(a) {
		return 0, InvalidResourceVersion{rv: a}
	}
	if !isWellFormed(b) {
		return 0, InvalidResourceVersion{rv: b}
	}
	// 此时 a 和 b 都是格式正确且不带前导零的十进制数字串。
	aLen := len(a)
	bLen := len(b)
	switch {
	case aLen < bLen:
		// 位数较少的数字串表示的数值更小。
		return -1, nil
	case aLen > bLen:
		// 位数较多的数字串表示的数值更大。
		return 1, nil
	default:
		// 位数相同时，字典序与十进制数值顺序一致。
		return strings.Compare(a, b), nil
	}
}

// isWellFormed 要求非空、无前导零（因此 "0" 本身也非法）且全为数字。
func isWellFormed(s string) bool {
	if len(s) == 0 {
		return false
	}
	if s[0] == '0' {
		return false
	}
	for i := range s {
		if !isDigit(s[i]) {
			return false
		}
	}
	return true
}

// isDigit 手写判断而非 unicode.IsDigit，避免为 ASCII 场景引入额外依赖与开销。
func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}
