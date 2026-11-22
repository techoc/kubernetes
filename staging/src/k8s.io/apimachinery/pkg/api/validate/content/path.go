/*
Copyright 2015 The Kubernetes Authors.

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

package content

import (
	"fmt"
	"strings"
)

// 不能作为路径段名称（REST API 或 etcd 存储）出现的完整字符串。
// "." 与 ".." 在文件系统/URL 归一化中具有特殊含义，直接使用会导致路径穿越。
var pathSegmentNameMayNotBe = []string{".", ".."}

// 不能出现在以路径段形式使用（REST API 或 etcd 存储）的名称中的子串。
// "/" 会改变路径层级，"%"" 可能被 URL 解码逻辑二次解释。
var pathSegmentNameMayNotContain = []string{"/", "%"}

// IsPathSegmentName 校验 name 可以被安全地编码为单个路径段。
//
// 注意：出于历史兼容原因，该函数不检查空字符串，也不限制名称长度；
// 调用方若需要这些约束需自行叠加。
func IsPathSegmentName(name string) []string {
	for _, illegalName := range pathSegmentNameMayNotBe {
		if name == illegalName {
			return []string{fmt.Sprintf(`may not be '%s'`, illegalName)}
		}
	}

	return IsPathSegmentPrefix(name)
}

// IsPathSegmentPrefix 校验 name 可以作为将以路径段形式编码的名称前缀。
// 它不检查与禁用名称的精确匹配，因为任意后缀可能使该名称重新变得合法。
//
// 注意：出于历史兼容原因，该函数不检查空字符串，也不限制名称长度。
func IsPathSegmentPrefix(name string) []string {
	var errors []string
	for _, illegalContent := range pathSegmentNameMayNotContain {
		if strings.Contains(name, illegalContent) {
			errors = append(errors, fmt.Sprintf(`may not contain '%s'`, illegalContent))
		}
	}

	return errors
}
