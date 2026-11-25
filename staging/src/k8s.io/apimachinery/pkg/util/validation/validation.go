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

// Package validation 提供 Kubernetes API 通用的字符串格式校验（DNS、标签、端口、
// 环境变量等）。函数返回人类可读的错误描述列表，空列表表示合法。
package validation

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"unicode"

	"k8s.io/apimachinery/pkg/api/validate/content"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// IsQualifiedName 验证值是否符合 Kubernetes 的 "qualified name" 格式。
// 无效时返回错误字符串列表，否则返回空列表或 nil。
// Deprecated: 请改用 k8s.io/apimachinery/pkg/api/validate/content.IsQualifiedName。
var IsQualifiedName = content.IsLabelKey

// IsFullyQualifiedName 检查名称是否为完整限定名称。它与 IsFullyQualifiedDomainName 类似，
// 但至少需要三段而不是两段，且不接受末尾的 "."。
// TODO: 此函数已弃用，保留至所有调用方迁移到 IsFullyQualifiedDomainName；请勿新增调用方。
// 中文补充：校验顺序为“非空 -> DNS 子域格式 -> 至少 3 段”，返回首个失败原因。
func IsFullyQualifiedName(fldPath *field.Path, name string) field.ErrorList {
	var allErrors field.ErrorList
	if len(name) == 0 {
		return append(allErrors, field.Required(fldPath, ""))
	}
	if errs := IsDNS1123Subdomain(name); len(errs) > 0 {
		return append(allErrors, field.Invalid(fldPath, name, strings.Join(errs, ",")))
	}
	if len(strings.Split(name, ".")) < 3 {
		return append(allErrors, field.Invalid(fldPath, name, "should be a domain with at least three segments separated by dots"))
	}
	return allErrors
}

// IsFullyQualifiedDomainName 检查域名是否为完整限定域名。
// 它与 IsFullyQualifiedName 类似，但至少需要两段，且接受末尾的 "."。
// 中文补充：末尾的 '.'（FQDN 根标记）在校验前被去掉；除子域整体校验外，
// 还要求每一段单独满足 RFC 1123 label 规则，以拦截超长单段。
func IsFullyQualifiedDomainName(fldPath *field.Path, name string) field.ErrorList {
	var allErrors field.ErrorList
	if len(name) == 0 {
		return append(allErrors, field.Required(fldPath, ""))
	}
	if strings.HasSuffix(name, ".") {
		name = name[:len(name)-1]
	}
	if errs := IsDNS1123Subdomain(name); len(errs) > 0 {
		return append(allErrors, field.Invalid(fldPath, name, strings.Join(errs, ",")))
	}
	if len(strings.Split(name, ".")) < 2 {
		return append(allErrors, field.Invalid(fldPath, name, "should be a domain with at least two segments separated by dots"))
	}
	for _, label := range strings.Split(name, ".") {
		if errs := IsDNS1123Label(label); len(errs) > 0 {
			return append(allErrors, field.Invalid(fldPath, label, strings.Join(errs, ",")))
		}
	}
	return allErrors
}

// RFC 3986 定义的 HTTP 路径允许字符，路径可包含：
// * 非保留字符（字母、数字、'-'、'.'、'_'、'~'）。
// * 百分号编码的八位组。
// * sub-delims（"!"、"$"、"&"、"'"、"("、")"、"*"、"+"、","、";"、"="）。
// * 冒号（":"）。
// 中文补充：该正则只做字符集检查，不验证百分号编码的合法性。
const httpPathFmt string = `[A-Za-z0-9/\-._~%!$&'()*+,;=:]+`

var httpPathRegexp = regexp.MustCompile("^" + httpPathFmt + "$")

// IsDomainPrefixedPath 检查字符串是否为带域名前缀的路径（如 acme.io/foo）。
// 第一个 "/" 之前必须是 RFC 1123 定义的合法子域名，
// 之后必须非空并匹配 ^([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]$。
func IsDomainPrefixedPath(fldPath *field.Path, dpPath string) field.ErrorList {
	var allErrs field.ErrorList
	if len(dpPath) == 0 {
		return append(allErrs, field.Required(fldPath, ""))
	}

	// 只按第一个 '/' 切分：路径部分允许再包含 '/'。
	segments := strings.SplitN(dpPath, "/", 2)
	if len(segments) != 2 || len(segments[0]) == 0 || len(segments[1]) == 0 {
		return append(allErrs, field.Invalid(fldPath, dpPath, "must be a domain-prefixed path (such as \"acme.io/foo\")"))
	}

	host := segments[0]
	for _, err := range IsDNS1123Subdomain(host) {
		allErrs = append(allErrs, field.Invalid(fldPath, host, err))
	}

	path := segments[1]
	if !httpPathRegexp.MatchString(path) {
		return append(allErrs, field.Invalid(fldPath, path, RegexError("Invalid path", httpPathFmt)))
	}

	return allErrs
}

// IsDomainPrefixedKey 检查 key 是否为带域名前缀的键（如 acme.io/foo）。
// 第一个 "/" 之前必须是 RFC 1123 定义的合法子域名，
// 之后必须非空并匹配 ^([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]$。
func IsDomainPrefixedKey(fldPath *field.Path, key string) field.ErrorList {
	var allErrs field.ErrorList
	if len(key) == 0 {
		return append(allErrs, field.Required(fldPath, ""))
	}
	for _, errMessages := range content.IsLabelKey(key) {
		allErrs = append(allErrs, field.Invalid(fldPath, key, errMessages))
	}

	// 格式不合法时不再检查前缀，避免在错误信息上叠加无关内容。
	if len(allErrs) > 0 {
		return allErrs
	}

	// IsLabelKey 允许省略前缀，这里额外要求必须恰好有一段前缀。
	segments := strings.Split(key, "/")
	if len(segments) != 2 {
		return append(allErrs, field.Invalid(fldPath, key, "must be a domain-prefixed key (such as \"acme.io/foo\")"))
	}

	return allErrs
}

// LabelValueMaxLength 是标签值的最大长度。
// Deprecated: 请改用 k8s.io/apimachinery/pkg/api/validate/content.LabelValueMaxLength。
const LabelValueMaxLength int = content.LabelValueMaxLength

// IsValidLabelValue 验证值是否为合法标签值；无效时返回错误字符串列表，否则返回空列表或 nil。
// Deprecated: 请改用 k8s.io/apimachinery/pkg/api/validate/content.IsLabelValue。
var IsValidLabelValue = content.IsLabelValue

// dns1123LabelFmt 是 RFC 1123 label 的正则：小写字母数字，中间可含 '-'，首尾必须是字母数字。
const dns1123LabelFmt string = "[a-z0-9]([-a-z0-9]*[a-z0-9])?"

// dns1123LabelFmtWithUnderscore 是放宽版本，额外允许 '_'（用于某些历史字段）。
const dns1123LabelFmtWithUnderscore string = "_?[a-z0-9]([-_a-z0-9]*[a-z0-9])?"

const dns1123LabelErrMsg string = "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character"

// DNS1123LabelMaxLength 是 DNS 标签的最大长度（RFC 1123）。
const DNS1123LabelMaxLength int = 63

var dns1123LabelRegexp = regexp.MustCompile("^" + dns1123LabelFmt + "$")

// IsDNS1123Label 验证字符串是否符合 DNS（RFC 1123）标签定义。
// DNS (RFC 1123).
func IsDNS1123Label(value string) []string {
	var errs []string
	if len(value) > DNS1123LabelMaxLength {
		errs = append(errs, MaxLenError(DNS1123LabelMaxLength))
	}
	if !dns1123LabelRegexp.MatchString(value) {
		if dns1123SubdomainRegexp.MatchString(value) {
			//它原本是合法子域名但不是合法标签；长度已检查，因此问题一定是点号。
			// 中文补充：给出“不能包含点”的精确提示，比通用正则错误更易定位问题。
			errs = append(errs, "must not contain dots")
		} else {
			errs = append(errs, RegexError(dns1123LabelErrMsg, dns1123LabelFmt, "my-name", "123-abc"))
		}
	}
	return errs
}

const dns1123SubdomainFmt string = dns1123LabelFmt + "(\\." + dns1123LabelFmt + ")*"
const dns1123SubdomainErrorMsg string = "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character"

const dns1123SubdomainFmtWithUnderscore string = dns1123LabelFmtWithUnderscore + "(\\." + dns1123LabelFmtWithUnderscore + ")*"
const dns1123SubdomainErrorMsgFG string = "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '_', '-' or '.', and must start and end with an alphanumeric character"

// DNS1123SubdomainMaxLength 是 DNS 子域名的最大长度（RFC 1123）。
const DNS1123SubdomainMaxLength int = 253

var dns1123SubdomainRegexp = regexp.MustCompile("^" + dns1123SubdomainFmt + "$")
var dns1123SubdomainRegexpWithUnderscore = regexp.MustCompile("^" + dns1123SubdomainFmtWithUnderscore + "$")

// IsDNS1123Subdomain 验证字符串是否符合 DNS（RFC 1123）子域名定义。
func IsDNS1123Subdomain(value string) []string {
	var errs []string
	if len(value) > DNS1123SubdomainMaxLength {
		errs = append(errs, MaxLenError(DNS1123SubdomainMaxLength))
	}
	if !dns1123SubdomainRegexp.MatchString(value) {
		errs = append(errs, RegexError(dns1123SubdomainErrorMsg, dns1123SubdomainFmt, "example.com"))
	}
	return errs
}

// IsDNS1123SubdomainWithUnderscore 验证字符串是否符合 DNS（RFC 1123）子域名定义，但允许下划线。
func IsDNS1123SubdomainWithUnderscore(value string) []string {
	var errs []string
	if len(value) > DNS1123SubdomainMaxLength {
		errs = append(errs, MaxLenError(DNS1123SubdomainMaxLength))
	}
	if !dns1123SubdomainRegexpWithUnderscore.MatchString(value) {
		errs = append(errs, RegexError(dns1123SubdomainErrorMsgFG, dns1123SubdomainFmtWithUnderscore, "example.com"))
	}
	return errs
}

// dns1035LabelFmt 与 1123 的区别是必须以字母开头（不能以数字开头）。
const dns1035LabelFmt string = "[a-z]([-a-z0-9]*[a-z0-9])?"
const dns1035LabelErrMsg string = "a DNS-1035 label must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character"

// DNS1035LabelMaxLength 是 DNS 标签的最大长度（RFC 1035）。
const DNS1035LabelMaxLength int = 63

var dns1035LabelRegexp = regexp.MustCompile("^" + dns1035LabelFmt + "$")

// IsDNS1035Label 验证字符串是否符合 DNS（RFC 1035）标签定义。
// DNS (RFC 1035).
func IsDNS1035Label(value string) []string {
	var errs []string
	if len(value) > DNS1035LabelMaxLength {
		errs = append(errs, MaxLenError(DNS1035LabelMaxLength))
	}
	if !dns1035LabelRegexp.MatchString(value) {
		errs = append(errs, RegexError(dns1035LabelErrMsg, dns1035LabelFmt, "my-name", "abc-123"))
	}
	return errs
}

// 通配符定义见 RFC 1034 4.3.3。
// 示例：
// - 有效：*.bar.com、*.foo.bar.com。
// - 无效：*.*.bar.com、*.foo.*.com、*bar.com、f*.bar.com、*。
const wildcardDNS1123SubdomainFmt = "\\*\\." + dns1123SubdomainFmt
const wildcardDNS1123SubdomainErrMsg = "a wildcard DNS-1123 subdomain must start with '*.', followed by a valid DNS subdomain, which must consist of lower case alphanumeric characters, '-' or '.' and end with an alphanumeric character"

// IsWildcardDNS1123Subdomain 验证字符串是否符合 DNS 通配符子域名定义（RFC 1034 4.3.3）。
// 中文补充：正则每次调用时构建，因为 wildcard 校验不是热路径；通配符只能出现在最左侧。
func IsWildcardDNS1123Subdomain(value string) []string {
	wildcardDNS1123SubdomainRegexp := regexp.MustCompile("^" + wildcardDNS1123SubdomainFmt + "$")

	var errs []string
	if len(value) > DNS1123SubdomainMaxLength {
		errs = append(errs, MaxLenError(DNS1123SubdomainMaxLength))
	}
	if !wildcardDNS1123SubdomainRegexp.MatchString(value) {
		errs = append(errs, RegexError(wildcardDNS1123SubdomainErrMsg, wildcardDNS1123SubdomainFmt, "*.example.com"))
	}
	return errs
}

// IsCIdentifier 验证字符串是否符合 C 标识符定义，只检查格式而不检查长度。
// Deprecated: 请改用 k8s.io/apimachinery/pkg/api/validate/content.IsCIdentifier。
var IsCIdentifier = content.IsCIdentifier

// IsValidPortNum 验证参数是否为有效的非零端口号。
func IsValidPortNum(port int) []string {
	if 1 <= port && port <= 65535 {
		return nil
	}
	return []string{InclusiveRangeError(1, 65535)}
}

// IsInRange 验证参数是否在闭区间内。
func IsInRange(value int, min int, max int) []string {
	if value >= min && value <= max {
		return nil
	}
	return []string{InclusiveRangeError(min, max)}
}

// libcontainer 的 UID/GID 范围目前为 0 到 1<<31-1。
// TODO: 有了 UID/GID 类型后，应使用该类型。
// 中文补充：上限取 MaxInt32 是为了与 libcontainer 的 UID/GID 映射范围保持一致。
const (
	minUserID  = 0
	maxUserID  = math.MaxInt32
	minGroupID = 0
	maxGroupID = math.MaxInt32
)

// IsValidGroupID 验证参数是否为有效的 Unix GID。
func IsValidGroupID(gid int64) []string {
	if minGroupID <= gid && gid <= maxGroupID {
		return nil
	}
	return []string{InclusiveRangeError(minGroupID, maxGroupID)}
}

// IsValidUserID 验证参数是否为有效的 Unix UID。
func IsValidUserID(uid int64) []string {
	if minUserID <= uid && uid <= maxUserID {
		return nil
	}
	return []string{InclusiveRangeError(minUserID, maxUserID)}
}

var portNameCharsetRegex = regexp.MustCompile("^[-a-z0-9]+$")
var portNameOneLetterRegexp = regexp.MustCompile("[a-z]")

// IsValidPortName 验证端口名语法：必须非空、不超过 15 个字符，
// 只能包含 [-a-z0-9]，至少含一个字母，
// 不能以连字符开头或结尾，也不能含连续连字符。
//
// 注意：即使 RFC 6335 不区分大小写，这里也只允许小写字母。
func IsValidPortName(port string) []string {
	var errs []string
	// 长度上限来自 RFC 6335 对服务名的限制。
	if len(port) > 15 {
		errs = append(errs, MaxLenError(15))
	}
	if !portNameCharsetRegex.MatchString(port) {
		errs = append(errs, "must contain only alpha-numeric characters (a-z, 0-9), and hyphens (-)")
	}
	if !portNameOneLetterRegexp.MatchString(port) {
		errs = append(errs, "must contain at least one letter (a-z)")
	}
	if strings.Contains(port, "--") {
		errs = append(errs, "must not contain consecutive hyphens")
	}
	// 空串的边界单独判断，避免索引越界；空串本身已由字符集正则报错。
	if len(port) > 0 && (port[0] == '-' || port[len(port)-1] == '-') {
		errs = append(errs, "must not begin or end with a hyphen")
	}
	return errs
}

const percentFmt string = "[0-9]+%"
const percentErrMsg string = "a valid percent string must be a numeric string followed by an ending '%'"

var percentRegexp = regexp.MustCompile("^" + percentFmt + "$")

// IsValidPercent 验证字符串是否为百分比形式。
func IsValidPercent(percent string) []string {
	if !percentRegexp.MatchString(percent) {
		return []string{RegexError(percentErrMsg, percentFmt, "1%", "93%")}
	}
	return nil
}

const httpHeaderNameFmt string = "[-A-Za-z0-9]+"
const httpHeaderNameErrMsg string = "a valid HTTP header must consist of alphanumeric characters or '-'"

var httpHeaderNameRegexp = regexp.MustCompile("^" + httpHeaderNameFmt + "$")

// IsHTTPHeaderName 验证字符串是否符合 Go HTTP 库的合法头字段名定义，
// 其范围比 RFC 7230 更严格。
func IsHTTPHeaderName(value string) []string {
	if !httpHeaderNameRegexp.MatchString(value) {
		return []string{RegexError(httpHeaderNameErrMsg, httpHeaderNameFmt, "X-Header-Name")}
	}
	return nil
}

const envVarNameFmt = "[-._a-zA-Z][-._a-zA-Z0-9]*"
const envVarNameFmtErrMsg string = "a valid environment variable name must consist of alphabetic characters, digits, '_', '-', or '.', and must not start with a digit"

// TODO(hirazawaui): 移除 RelaxedEnvironmentVariableValidation 特性门后重命名。
const relaxedEnvVarNameFmtErrMsg string = "a valid environment variable name must consist only of printable ASCII characters other than '='"

var envVarNameRegexp = regexp.MustCompile("^" + envVarNameFmt + "$")

// IsEnvVarName 验证字符串是否为合法环境变量名。
// 中文补充：除正则外还拒绝 "."、".." 及以 ".." 开头的名称，防止路径穿越语义。
func IsEnvVarName(value string) []string {
	var errs []string
	if !envVarNameRegexp.MatchString(value) {
		errs = append(errs, RegexError(envVarNameFmtErrMsg, envVarNameFmt, "my.env-name", "MY_ENV.NAME", "MyEnvName1"))
	}

	errs = append(errs, hasChDirPrefix(value)...)
	return errs
}

// IsRelaxedEnvVarName 按宽松规则验证字符串是否为合法环境变量名。
// 中文补充：放宽版只要求非空且为可打印 ASCII（排除 '='），用于特性门控开启后的场景。
func IsRelaxedEnvVarName(value string) []string {
	var errs []string

	if len(value) == 0 {
		errs = append(errs, "environment variable name "+EmptyError())
	}

	for _, r := range value {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) || r == '=' {
			errs = append(errs, relaxedEnvVarNameFmtErrMsg)
			break
		}
	}

	return errs
}

const configMapKeyFmt = `[-._a-zA-Z0-9]+`
const configMapKeyErrMsg string = "a valid config key must consist of alphanumeric characters, '-', '_' or '.'"

var configMapKeyRegexp = regexp.MustCompile("^" + configMapKeyFmt + "$")

// IsConfigMapKey 验证字符串是否为 ConfigMap 或 Secret 的合法键。
// 中文补充：键长上限复用 subdomain 的 253，并额外拒绝 ".." 前缀以防挂载路径穿越。
func IsConfigMapKey(value string) []string {
	var errs []string
	if len(value) > DNS1123SubdomainMaxLength {
		errs = append(errs, MaxLenError(DNS1123SubdomainMaxLength))
	}
	if !configMapKeyRegexp.MatchString(value) {
		errs = append(errs, RegexError(configMapKeyErrMsg, configMapKeyFmt, "key.name", "KEY_NAME", "key-name"))
	}
	errs = append(errs, hasChDirPrefix(value)...)
	return errs
}

// MaxLenError 返回 "string too long" 校验失败的说明字符串。
func MaxLenError(length int) string {
	return fmt.Sprintf("must be no more than %d characters", length)
}

// RegexError 返回正则校验失败的说明字符串。
// 中文补充：把示例值一并写进错误信息，让用户无需查源码即可知道期望格式。
func RegexError(msg string, fmt string, examples ...string) string {
	if len(examples) == 0 {
		return msg + " (regex used for validation is '" + fmt + "')"
	}
	msg += " (e.g. "
	for i := range examples {
		if i > 0 {
			msg += " or "
		}
		msg += "'" + examples[i] + "', "
	}
	msg += "regex used for validation is '" + fmt + "')"
	return msg
}

// EmptyError 返回 "must not be empty" 校验失败的说明字符串。
func EmptyError() string {
	return "must be non-empty"
}

// InclusiveRangeError 返回数值必须在指定闭区间内的校验失败说明。
func InclusiveRangeError(lo, hi int) string {
	return fmt.Sprintf(`must be between %d and %d, inclusive`, lo, hi)
}

// hasChDirPrefix 拒绝 "."、".." 以及以 ".." 开头的值，
// 避免这些名称在被当作相对路径拼接时逃逸出预期目录。
func hasChDirPrefix(value string) []string {
	var errs []string
	switch {
	case value == ".":
		errs = append(errs, `must not be '.'`)
	case value == "..":
		errs = append(errs, `must not be '..'`)
	case strings.HasPrefix(value, ".."):
		errs = append(errs, `must not start with '..'`)
	}
	return errs
}
