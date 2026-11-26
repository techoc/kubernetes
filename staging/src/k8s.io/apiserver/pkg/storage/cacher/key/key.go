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

package key

import (
	"strings"
)

// HasPathPrefix 判断字符串 s 是否与 pathPrefix 完全相等，或是否在路径分段边界处以 pathPrefix 开头。
// 必须以路径分隔符为边界，避免 "ns" 误匹配 "ns2"；整个函数不分配内存，供缓存热路径调用。
func HasPathPrefix(s, pathPrefix string) bool {
	// 若 s 根本不包含该前缀，直接短路返回。
	if !strings.HasPrefix(s, pathPrefix) {
		return false
	}

	pathPrefixLength := len(pathPrefix)

	if len(s) == pathPrefixLength {
		// 完全相等
		return true
	}
	if strings.HasSuffix(pathPrefix, "/") {
		// pathPrefix 已确保位于路径分段边界。
		return true
	}
	if s[pathPrefixLength:pathPrefixLength+1] == "/" {
		// s 的下一个字符是路径分段边界。
		// 检查这个条件而不是规范化 pathPrefix，避免每次调用都分配内存。
		return true
	}
	return false
}
