/*
Copyright 2014 The Kubernetes Authors.

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

package validation

import (
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// 中文补充：多处最小值校验共用该文案，保证客户端看到的提示一致。
const IsNegativeErrorMsg string = `must be greater than or equal to 0`

// 中文补充：prefix 为 true 表示名称后面还会拼接生成的后缀，此时允许以 '-' 结尾，
// 因为拼接后不会再以 '-' 结尾。
type ValidateNameFunc func(name string, prefix bool) []string

// 中文补充：新版校验直接携带字段路径，便于结构化错误输出；旧接口仅有字符串描述。
type ValidateNameFuncWithErrors func(fldPath *field.Path, name string) field.ErrorList

// 中文补充：最长 253 字符，允许以 '.' 分段的域名形式。
func NameIsDNSSubdomain(name string, prefix bool) []string {
	if prefix {
		name = maskTrailingDash(name)
	}
	return validation.IsDNS1123Subdomain(name)
}

// 中文补充：最长 63 字符，不允许 '.'。
func NameIsDNSLabel(name string, prefix bool) []string {
	if prefix {
		name = maskTrailingDash(name)
	}
	return validation.IsDNS1123Label(name)
}

// 中文补充：RFC 1035 label 要求必须以字母开头，比 1123 label 更严格；
// 原注释中的 "DNS 952" 是历史笔误。
func NameIsDNS1035Label(name string, prefix bool) []string {
	if prefix {
		name = maskTrailingDash(name)
	}
	return validation.IsDNS1035Label(name)
}

// ValidateNamespaceName 校验给定 namespace 名称是否合法。
// Prefix 表示名称将用于生成，此时允许以截断方式适配最大长度。
// 允许尾随连字符。
var ValidateNamespaceName = NameIsDNSLabel

// ValidateServiceAccountName 校验给定 service account 名称是否合法。
// Prefix 表示名称将用于生成，此时允许以截断方式适配最大长度。
// 允许尾随连字符。
var ValidateServiceAccountName = NameIsDNSSubdomain

// 中文补充：目的是在“名称+生成后缀”场景下避免出现以 '-' 结尾的非法名称；
// 实际实现是丢弃倒数第二个字符并追加 'a'。
func maskTrailingDash(name string) string {
	if len(name) > 1 && strings.HasSuffix(name, "-") {
		return name[:len(name)-2] + "a"
	}
	return name
}

// 中文补充：origin 标记为 minimum，便于与 validation-gen 生成的最小值校验统一。
func ValidateNonnegativeField(value int64, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if value < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, value, IsNegativeErrorMsg).WithOrigin("minimum"))
	}
	return allErrs
}
