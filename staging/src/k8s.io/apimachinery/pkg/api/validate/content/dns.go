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

// dns1123LabelFmt 是 RFC 1123 label 的正则片段：以字母数字开头结尾，中间可含 '-'。
const dns1123LabelFmt string = "[a-z0-9]([-a-z0-9]*[a-z0-9])?"

// dns1123LabelErrMsg 是 label 校验失败时展示给用户的说明。
const dns1123LabelErrMsg string = "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character"

// DNS1123LabelMaxLength 是 RFC 1123 中 DNS 标签的最大长度。
const DNS1123LabelMaxLength int = 63

// dns1123LabelRegexp 预编译带锚点的 label 正则，避免每次校验都重新编译。
var dns1123LabelRegexp = regexp.MustCompile("^" + dns1123LabelFmt + "$")

// IsDNS1123Label 校验字符串是否符合 DNS（RFC 1123）对 label 的定义。
// 返回错误信息列表，为空表示通过。
func IsDNS1123Label(value string) []string {
	var errs []string
	if len(value) > DNS1123LabelMaxLength {
		errs = append(errs, MaxLenError(DNS1123LabelMaxLength))
	}
	if !dns1123LabelRegexp.MatchString(value) {
		if dns1123SubdomainRegexp.MatchString(value) {
			// 能通过 subdomain 校验却过不了 label 校验，只可能是含有点号，给出针对性提示。
			errs = append(errs, "must not contain dots")
		} else {
			errs = append(errs, RegexError(dns1123LabelErrMsg, dns1123LabelFmt, "my-name", "123-abc"))
		}
	}
	return errs
}

// subdomain 由若干以 '.' 连接的 label 组成，因此直接复用 label 的格式片段。
const dns1123SubdomainFmt string = dns1123LabelFmt + "(\\." + dns1123LabelFmt + ")*"
const dns1123SubdomainFmtCaseless string = "(?i)" + dns1123SubdomainFmt
const dns1123SubdomainErrorMsg string = "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character"
const dns1123SubdomainCaselessErrorMsg string = "an RFC 1123 subdomain must consist of alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character"

// DNS1123SubdomainMaxLength 是 RFC 1123 中 DNS 子域名的最大长度。
const DNS1123SubdomainMaxLength int = 253

var dns1123SubdomainRegexp = regexp.MustCompile("^" + dns1123SubdomainFmt + "$")
var dns1123SubdomainCaselessRegexp = regexp.MustCompile("^" + dns1123SubdomainFmtCaseless + "$")

// IsDNS1123Subdomain 校验字符串是否符合 DNS（RFC 1123）对 subdomain 的定义，且必须为小写。
func IsDNS1123Subdomain(value string) []string {
	return isDNS1123Subdomain(value, false)
}

// IsDNS1123SubdomainCaseless 校验字符串是否符合 DNS（RFC 1123）对 subdomain 的定义，
// 但允许大小写混写。
//
// Deprecated: API 校验不应忽略大小写；忽略大小写会为伪装攻击提供可乘之机，
//
// 译注：该函数允许大写字母，但大小写仍被视为不同的值，并非真正的大小写无关比较。
// 新代码应使用 IsDNS1123Subdomain 做严格的小写校验。
func IsDNS1123SubdomainCaseless(value string) []string {
	return isDNS1123Subdomain(value, true)
}

// isDNS1123Subdomain 是 subdomain 校验的公共实现，caseless 决定是否允许大写。
func isDNS1123Subdomain(value string, caseless bool) []string {
	var errs []string
	if len(value) > DNS1123SubdomainMaxLength {
		errs = append(errs, MaxLenError(DNS1123SubdomainMaxLength))
	}
	// 大小写两种模式只在错误文案、示例与所用正则上不同，逻辑完全一致。
	errorMsg := dns1123SubdomainErrorMsg
	example := "example.com"
	regexp := dns1123SubdomainRegexp
	if caseless {
		errorMsg = dns1123SubdomainCaselessErrorMsg
		example = "Example.com"
		regexp = dns1123SubdomainCaselessRegexp
	}
	if !regexp.MatchString(value) {
		errs = append(errs, RegexError(errorMsg, dns1123SubdomainFmt, example))
	}
	return errs
}
