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
	"strings"
)

// labelKeyCharFmt 是标签键 name 部分允许的首尾字符（必须为字母或数字）。
const labelKeyCharFmt string = "[A-Za-z0-9]"

// labelKeyExtCharFmt 是 name 中间位置允许的字符，比首尾宽松，额外接受 -_.。
const labelKeyExtCharFmt string = "[-A-Za-z0-9_.]"
const labelKeyFmt string = "(" + labelKeyCharFmt + labelKeyExtCharFmt + "*)?" + labelKeyCharFmt
const labelKeyErrMsg string = "must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character"
const labelKeyMaxLength int = 63

// labelKeyRegexp 只校验 name 部分（不含可选的前缀），前缀由 IsDNS1123Subdomain 单独校验。
var labelKeyRegexp = regexp.MustCompile("^" + labelKeyFmt + "$")

// Deprecated: 请改用 IsLabelKey。
// 中文补充：仅为向后兼容保留的别名。
var IsQualifiedName = IsLabelKey

// 中文补充：前缀必须是合法的 DNS 子域；name 长度上限 63 且必须以字母数字开头和结尾。
func IsLabelKey(value string) []string {
	var errs []string
	// 最多只允许一个 '/'：省略形式 "key"，或带域形式 "prefix/key"。
	parts := strings.Split(value, "/")
	var name string
	switch len(parts) {
	case 1:
		name = parts[0]
	case 2:
		var prefix string
		prefix, name = parts[0], parts[1]
		if len(prefix) == 0 {
			errs = append(errs, "prefix part "+EmptyError())
		} else if msgs := IsDNS1123Subdomain(prefix); len(msgs) != 0 {
			errs = append(errs, prefixEach(msgs, "prefix part ")...)
		}
	default:
		// 出现多个 '/' 时无法拆分前缀与名称，直接返回格式错误。
		return append(errs, "a valid label key "+RegexError(labelKeyErrMsg, labelKeyFmt, "MyName", "my.name", "123-abc")+
			" with an optional DNS subdomain prefix and '/' (e.g. 'example.com/MyName')")
	}

	if len(name) == 0 {
		errs = append(errs, "name part "+EmptyError())
	} else if len(name) > labelKeyMaxLength {
		errs = append(errs, "name part "+MaxLenError(labelKeyMaxLength))
	}
	if !labelKeyRegexp.MatchString(name) {
		errs = append(errs, "name part "+RegexError(labelKeyErrMsg, labelKeyFmt, "MyName", "my.name", "123-abc"))
	}
	return errs
}

// labelValueFmt 允许空字符串；否则与标签 key 的 name 规则相同。
const labelValueFmt string = "(" + labelKeyFmt + ")?"

// labelValueErrMsg 与 regex 配套使用，提示调用方合法取值。
const labelValueErrMsg string = "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character"

// 中文补充：与标签键 name 部分的长度上限保持一致。
const LabelValueMaxLength int = 63

var labelValueRegexp = regexp.MustCompile("^" + labelValueFmt + "$")

// 中文补充：空字符串合法，表示“无值”的标签。
func IsLabelValue(value string) []string {
	var errs []string
	if len(value) > LabelValueMaxLength {
		errs = append(errs, MaxLenError(LabelValueMaxLength))
	}
	if !labelValueRegexp.MatchString(value) {
		errs = append(errs, RegexError(labelValueErrMsg, labelValueFmt, "MyValue", "my_value", "12345"))
	}
	return errs
}

// prefixEach 给每条错误信息加上字段前缀（本轮在原切片上就地修改）。
func prefixEach(msgs []string, prefix string) []string {
	for i := range msgs {
		msgs[i] = prefix + msgs[i]
	}
	return msgs
}

// 中文补充：先复用 IsLabelKey 做完整校验，再要求必须存在而不仅是有可选的域名前缀。
func IsPrefixedLabelKey(value string) []string {
	if errs := IsLabelKey(value); len(errs) > 0 {
		return errs
	}

	segments := strings.Split(value, "/")
	if len(segments) != 2 {
		return []string{"must include a prefix (e.g. 'example.com/key')"}
	}
	return nil
}
