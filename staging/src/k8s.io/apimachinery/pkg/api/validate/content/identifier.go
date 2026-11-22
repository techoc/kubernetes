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

package content

import (
	"regexp"
)

// C 标识符的格式：以字母或下划线开头，后接字母、数字或下划线。
const cIdentifierFmt string = "[A-Za-z_][A-Za-z0-9_]*"

// identifierErrMsg 是给最终用户看的错误描述，需与 cIdentifierFmt 的规则保持一致。
const identifierErrMsg string = "a valid C identifier must start with alphabetic character or '_', followed by a string of alphanumeric characters or '_'"

// 编译期预编译正则，避免每次校验都重新编译。
var cIdentifierRegexp = regexp.MustCompile("^" + cIdentifierFmt + "$")

// IsCIdentifier 校验字符串是否符合 C 语言标识符的定义。
// 只检查格式，不检查长度。
// 返回 nil 表示通过，否则返回包含一条可读错误信息的切片。
func IsCIdentifier(value string) []string {
	if !cIdentifierRegexp.MatchString(value) {
		return []string{RegexError(identifierErrMsg, cIdentifierFmt, "my_name", "MY_NAME", "MyName")}
	}
	return nil
}
