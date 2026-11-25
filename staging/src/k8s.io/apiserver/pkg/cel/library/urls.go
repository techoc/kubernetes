/*
Copyright 2022 The Kubernetes Authors.

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

package library

import (
	"net/url"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	apiservercel "k8s.io/apiserver/pkg/cel"
)

// URLs provides a CEL function library extension of URL parsing functions.
//
// url
//
// Converts a string to a URL or results in an error if the string is not a valid URL. The URL must be an absolute URI
// or an absolute path.
//
//	url(<string>) <URL>
//
// Examples:
//
//	url('https://user:pass@example.com:80/path?query=val#fragment') // returns a URL
//	url('/absolute-path') // returns a URL
//	url('https://a:b:c/') // error
//	url('../relative-path') // error
//
// isURL
//
// Returns true if a string is a valid URL. The URL must be an absolute URI or an absolute path.
//
//	isURL( <string>) <bool>
//
// Examples:
//
//	isURL('https://user:pass@example.com:80/path?query=val#fragment') // returns true
//	isURL('/absolute-path') // returns true
//	isURL('https://a:b:c/') // returns false
//	isURL('../relative-path') // returns false
//
// getScheme / getHost / getHostname / getPort / getEscapedPath / getQuery
//
// Return the parsed components of a URL.
//
//   - getScheme: If absent in the URL, returns an empty string.
//
//   - getHostname: IPv6 addresses are returned without braces, e.g. "::1". If absent in the URL, returns an empty string.
//
//   - getHost: IPv6 addresses are returned with braces, e.g. "[::1]". If absent in the URL, returns an empty string.
//
//   - getEscapedPath: The string returned by getEscapedPath is URL escaped, e.g. "with space" becomes "with%20space".
//     If absent in the URL, returns an empty string.
//
//   - getPort: If absent in the URL, returns an empty string.
//
//   - getQuery: Returns the query parameters in "matrix" form where a repeated query key is interpreted to
//     mean that there are multiple values for that key. The keys and values are returned unescaped.
//     If absent in the URL, returns an empty map.
//
//     <URL>.getScheme() <string>
//     <URL>.getHost() <string>
//     <URL>.getHostname() <string>
//     <URL>.getPort() <string>
//     <URL>.getEscapedPath() <string>
//     <URL>.getQuery() <map <string>, <list <string>>
//
// Examples:
//
//	url('/path').getScheme() // returns ''
//	url('https://example.com/').getScheme() // returns 'https'
//	url('https://example.com:80/').getHost() // returns 'example.com:80'
//	url('https://example.com/').getHost() // returns 'example.com'
//	url('https://[::1]:80/').getHost() // returns '[::1]:80'
//	url('https://[::1]/').getHost() // returns '[::1]'
//	url('/path').getHost() // returns ''
//	url('https://example.com:80/').getHostname() // returns 'example.com'
//	url('https://127.0.0.1:80/').getHostname() // returns '127.0.0.1'
//	url('https://[::1]:80/').getHostname() // returns '::1'
//	url('/path').getHostname() // returns ''
//	url('https://example.com:80/').getPort() // returns '80'
//	url('https://example.com/').getPort() // returns ''
//	url('/path').getPort() // returns ''
//	url('https://example.com/path').getEscapedPath() // returns '/path'
//	url('https://example.com/path with spaces/').getEscapedPath() // returns '/path%20with%20spaces/'
//	url('https://example.com').getEscapedPath() // returns ''
//	url('https://example.com/path?k1=a&k2=b&k2=c').getQuery() // returns { 'k1': ['a'], 'k2': ['b', 'c']}
//	url('https://example.com/path?key with spaces=value with spaces').getQuery() // returns { 'key with spaces': ['value with spaces']}
//	url('https://example.com/path?').getQuery() // returns {}
//	url('https://example.com/path').getQuery() // returns {}
//
// URL 库把字符串转换为 kubernetes.URL 类型并暴露解析后的组件。
// 只接受绝对 URI 或绝对路径，相对路径与畸形 URI 会被拒绝，以免策略中
// 出现歧义的 URL 判断。IP 地址在 getHost 中保留方括号，便于直接用作
// HTTP 主机；getHostname 则去掉方括号。
func URLs() cel.EnvOption {
	return cel.Lib(urlsLib)
}

// urlsLib 是单例；库无状态，可安全共享。
var urlsLib = &urls{}

// urls 实现 cel.Library 接口。
type urls struct{}

// LibraryName 返回稳定的库名，供兼容性与去重使用。
func (*urls) LibraryName() string {
	return "kubernetes.urls"
}

// Types 返回库引入的自定义 CEL 类型 kubernetes.URL。
func (*urls) Types() []*cel.Type {
	return []*cel.Type{apiservercel.URLType}
}

// declarations 暴露函数声明供外部检查使用。
func (*urls) declarations() map[string][]cel.FunctionOpt {
	return urlLibraryDecls
}

// urlLibraryDecls 定义 url() 转换函数及各组件访问器。
var urlLibraryDecls = map[string][]cel.FunctionOpt{
	"url": {
		cel.Overload("string_to_url", []*cel.Type{cel.StringType}, apiservercel.URLType,
			cel.UnaryBinding(stringToUrl))},
	"getScheme": {
		cel.MemberOverload("url_get_scheme", []*cel.Type{apiservercel.URLType}, cel.StringType,
			cel.UnaryBinding(getScheme))},
	"getHost": {
		cel.MemberOverload("url_get_host", []*cel.Type{apiservercel.URLType}, cel.StringType,
			cel.UnaryBinding(getHost))},
	"getHostname": {
		cel.MemberOverload("url_get_hostname", []*cel.Type{apiservercel.URLType}, cel.StringType,
			cel.UnaryBinding(getHostname))},
	"getPort": {
		cel.MemberOverload("url_get_port", []*cel.Type{apiservercel.URLType}, cel.StringType,
			cel.UnaryBinding(getPort))},
	"getEscapedPath": {
		cel.MemberOverload("url_get_escaped_path", []*cel.Type{apiservercel.URLType}, cel.StringType,
			cel.UnaryBinding(getEscapedPath))},
	"getQuery": {
		cel.MemberOverload("url_get_query", []*cel.Type{apiservercel.URLType},
			cel.MapType(cel.StringType, cel.ListType(cel.StringType)),
			cel.UnaryBinding(getQuery))},
	"isURL": {
		cel.Overload("is_url_string", []*cel.Type{cel.StringType}, cel.BoolType,
			cel.UnaryBinding(isURL))},
}

// CompileOptions 把函数注册进 CEL 编译环境。
func (*urls) CompileOptions() []cel.EnvOption {
	options := []cel.EnvOption{}
	for name, overloads := range urlLibraryDecls {
		options = append(options, cel.Function(name, overloads...))
	}
	return options
}

// ProgramOptions 无需运行期优化，URL 解析本身不是热点。
func (*urls) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{}
}

// stringToUrl 把字符串转换为 kubernetes.URL。先 ParseRequestURI 做严格
// 校验，再用 Parse 得到正确保留 fragment 的 URL 对象。
func stringToUrl(arg ref.Val) ref.Val {
	s, ok := arg.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}
	// Use ParseRequestURI to check the URL before conversion.
	// ParseRequestURI requires absolute URLs and is used by the OpenAPIv3 'uri' format.
	// 用与 OpenAPIv3 'uri' 格式一致的严格校验，保持策略语义与 CRD
	// schema 校验一致。
	_, err := url.ParseRequestURI(s)
	if err != nil {
		return types.NewErr("URL parse error during conversion from string: %v", err)
	}
	// We must parse again with Parse since ParseRequestURI incorrectly parses URLs that contain a fragment
	// part and will incorrectly append the fragment to either the path or the query, depending on which it was adjacent to.
	// ParseRequestURI 对 fragment 的处理有缺陷，因此校验通过后必须用
	// url.Parse 重新解析才能得到正确的组件划分。
	u, err := url.Parse(s)
	if err != nil {
		// Errors are not expected here since Parse is a more lenient parser than ParseRequestURI.
		return types.NewErr("URL parse error during conversion from string: %v", err)
	}
	return apiservercel.URL{URL: u}
}

// getScheme 返回 scheme，缺省为空串。
func getScheme(arg ref.Val) ref.Val {
	u, ok := arg.Value().(*url.URL)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}
	return types.String(u.Scheme)
}

// getHost 返回 host（含端口；IPv6 地址带方括号），缺省为空串。
func getHost(arg ref.Val) ref.Val {
	u, ok := arg.Value().(*url.URL)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}
	return types.String(u.Host)
}

// getHostname 返回不含端口的主机名；IPv6 地址去掉方括号。
func getHostname(arg ref.Val) ref.Val {
	u, ok := arg.Value().(*url.URL)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}
	return types.String(u.Hostname())
}

// getPort 返回端口字符串；缺省为空串而非 "0"。
func getPort(arg ref.Val) ref.Val {
	u, ok := arg.Value().(*url.URL)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}
	return types.String(u.Port())
}

// getEscapedPath 返回 URL 转义后的路径，保证结果可直接拼接进 URL。
func getEscapedPath(arg ref.Val) ref.Val {
	u, ok := arg.Value().(*url.URL)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}
	return types.String(u.EscapedPath())
}

// getQuery 返回未转义的查询参数映射，重复键聚合为字符串列表；
// 无查询参数时返回空 map 而非 nil，避免 CEL 侧的空值判断差异。
func getQuery(arg ref.Val) ref.Val {
	u, ok := arg.Value().(*url.URL)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}

	result := map[ref.Val]ref.Val{}
	for k, v := range u.Query() {
		result[types.String(k)] = types.NewStringList(types.DefaultTypeAdapter, v)
	}
	return types.NewRefValMap(types.DefaultTypeAdapter, result)
}

// isURL 用与 stringToUrl 相同的严格规则判断字符串是否是合法 URL。
func isURL(arg ref.Val) ref.Val {
	s, ok := arg.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}
	_, err := url.ParseRequestURI(s)
	return types.Bool(err == nil)
}
