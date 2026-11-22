/*
Copyright 2016 The Kubernetes Authors.

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

// Package headerrequest 从 HTTP 请求头中提取用户身份，供前置代理（如聚合层、
// kube-apiserver 的 front-proxy）认证使用。
//
// 安全边界：这些请求头只在客户端证书已通过前置代理校验时才可信；
// 认证完成后必须清除相关请求头，防止身份信息被下游组件误用。
package headerrequest

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	x509request "k8s.io/apiserver/pkg/authentication/request/x509"
	"k8s.io/apiserver/pkg/authentication/user"
)

// StringSliceProvider is a way to get a string slice value.  It is heavily used for authentication headers among other places.
// 通过接口延迟取值，使配置可以在运行期动态更新（例如热加载的请求头列表）。
type StringSliceProvider interface {
	// Value returns the current string slice.  Callers should never mutate the returned value.
	// 返回的切片可能被多个请求共享，调用方不得修改。
	Value() []string
}

// StringSliceProviderFunc is a function that matches the StringSliceProvider interface
type StringSliceProviderFunc func() []string

// Value returns the current string slice.  Callers should never mutate the returned value.
func (d StringSliceProviderFunc) Value() []string {
	return d()
}

// StaticStringSlice a StringSliceProvider that returns a fixed value
type StaticStringSlice []string

// Value returns the current string slice.  Callers should never mutate the returned value.
func (s StaticStringSlice) Value() []string {
	return s
}

// requestHeaderAuthRequestHandler 按配置的请求头列表构造用户身份。
type requestHeaderAuthRequestHandler struct {
	// nameHeaders are the headers to check (in order, case-insensitively) for an identity. The first header with a value wins.
	// 用户名请求头，按顺序取第一个非空值。
	nameHeaders StringSliceProvider

	// nameHeaders are the headers to check (in order, case-insensitively) for an identity UID. The first header with a value wins.
	// UID 请求头，按顺序取第一个非空值。
	uidHeaders StringSliceProvider

	// groupHeaders are the headers to check (case-insensitively) for group membership.  All values of all headers will be added.
	// 组请求头，所有头部的所有值都会被合并。
	groupHeaders StringSliceProvider

	// extraHeaderPrefixes are the head prefixes to check (case-insensitively) for filling in
	// the user.Info.Extra.  All values of all matching headers will be added.
	// Extra 信息以请求头前缀表示，前缀之后的部分（小写、URL 解码）作为 key。
	extraHeaderPrefixes StringSliceProvider
}

// New 校验并规范化请求头名称后构造认证器；空请求头名称视为配置错误。
func New(nameHeaders, uidHeaders, groupHeaders, extraHeaderPrefixes []string) (authenticator.Request, error) {
	trimmedNameHeaders, err := trimHeaders(nameHeaders...)
	if err != nil {
		return nil, err
	}
	trimmedUIDHeaders, err := trimHeaders(uidHeaders...)
	if err != nil {
		return nil, err
	}
	trimmedGroupHeaders, err := trimHeaders(groupHeaders...)
	if err != nil {
		return nil, err
	}
	trimmedExtraHeaderPrefixes, err := trimHeaders(extraHeaderPrefixes...)
	if err != nil {
		return nil, err
	}

	return NewDynamic(
		StaticStringSlice(trimmedNameHeaders),
		StaticStringSlice(trimmedUIDHeaders),
		StaticStringSlice(trimmedGroupHeaders),
		StaticStringSlice(trimmedExtraHeaderPrefixes),
	), nil
}

// NewDynamic 使用可在运行期变化的请求头列表构造认证器。
func NewDynamic(nameHeaders, uidHeaders, groupHeaders, extraHeaderPrefixes StringSliceProvider) authenticator.Request {
	return &requestHeaderAuthRequestHandler{
		nameHeaders:         nameHeaders,
		uidHeaders:          uidHeaders,
		groupHeaders:        groupHeaders,
		extraHeaderPrefixes: extraHeaderPrefixes,
	}
}

// trimHeaders 去除请求头名称两端空白；空名称会导致请求头匹配歧义，直接报错。
func trimHeaders(headerNames ...string) ([]string, error) {
	ret := []string{}
	for _, headerName := range headerNames {
		trimmedHeader := strings.TrimSpace(headerName)
		if len(trimmedHeader) == 0 {
			return nil, fmt.Errorf("empty header %q", headerName)
		}
		ret = append(ret, trimmedHeader)
	}

	return ret, nil
}

// NewDynamicVerifyOptionsSecure 先校验客户端证书（前置代理），再根据请求头提取身份。
// 只有通过代理证书校验的请求才会被解析请求头，防止外部客户端伪造身份。
func NewDynamicVerifyOptionsSecure(verifyOptionFn x509request.VerifyOptionFunc, proxyClientNames, nameHeaders, uidHeaders, groupHeaders, extraHeaderPrefixes StringSliceProvider) authenticator.Request {
	headerAuthenticator := NewDynamic(nameHeaders, uidHeaders, groupHeaders, extraHeaderPrefixes)

	return x509request.NewDynamicCAVerifier(verifyOptionFn, headerAuthenticator, proxyClientNames)
}

// AuthenticateRequest 从请求头解析用户身份；用户名缺失时视为未认证（不返回错误）。
func (a *requestHeaderAuthRequestHandler) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	name := headerValue(req.Header, a.nameHeaders.Value())
	if len(name) == 0 {
		return nil, false, nil
	}
	uid := headerValue(req.Header, a.uidHeaders.Value())
	groups := allHeaderValues(req.Header, a.groupHeaders.Value())
	extra := newExtra(req.Header, a.extraHeaderPrefixes.Value())

	// clear headers used for authentication
	// 认证信息已提取完毕，立即清除请求头，避免被后续 handler 或代理当作可信输入。
	ClearAuthenticationHeaders(req.Header, a.nameHeaders, a.uidHeaders, a.groupHeaders, a.extraHeaderPrefixes)

	return &authenticator.Response{
		User: &user.DefaultInfo{
			Name:   name,
			UID:    uid,
			Groups: groups,
			Extra:  extra,
		},
	}, true, nil
}

// ClearAuthenticationHeaders 删除所有用于认证的请求头。
// Extra 请求头按前缀匹配，遍历原始 map 删除，避免依赖头部名称规范化。
func ClearAuthenticationHeaders(h http.Header, nameHeaders, uidHeaders, groupHeaders, extraHeaderPrefixes StringSliceProvider) {
	for _, headerName := range nameHeaders.Value() {
		h.Del(headerName)
	}
	for _, headerName := range uidHeaders.Value() {
		h.Del(headerName)
	}
	for _, headerName := range groupHeaders.Value() {
		h.Del(headerName)
	}
	for _, prefix := range extraHeaderPrefixes.Value() {
		for k := range h {
			if hasPrefixIgnoreCase(k, prefix) {
				delete(h, k) // we have the raw key so avoid relying on canonicalization
			}
		}
	}
}

// hasPrefixIgnoreCase 以大小写不敏感的方式判断前缀，符合 HTTP 头名称的语义。
func hasPrefixIgnoreCase(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

// headerValue 返回第一个非空的请求头值。
func headerValue(h http.Header, headerNames []string) string {
	for _, headerName := range headerNames {
		headerValue := h.Get(headerName)
		if len(headerValue) > 0 {
			return headerValue
		}
	}
	return ""
}

// allHeaderValues 合并所有指定请求头的全部非空值。
func allHeaderValues(h http.Header, headerNames []string) []string {
	ret := []string{}
	for _, headerName := range headerNames {
		headerKey := http.CanonicalHeaderKey(headerName)
		values, ok := h[headerKey]
		if !ok {
			continue
		}

		for _, headerValue := range values {
			if len(headerValue) > 0 {
				ret = append(ret, headerValue)
			}
		}
	}
	return ret
}

// unescapeExtraKey 对 Extra 的 key 做 URL 解码；解码失败时保留原值，
// 宁可保留可能畸形的 key，也不丢弃调用方传入的信息。
func unescapeExtraKey(encodedKey string) string {
	key, err := url.PathUnescape(encodedKey) // Decode %-encoded bytes.
	if err != nil {
		return encodedKey // Always record extra strings, even if malformed/unencoded.
	}
	return key
}

// newExtra 根据前缀收集 Extra 信息。
func newExtra(h http.Header, headerPrefixes []string) map[string][]string {
	ret := map[string][]string{}

	// we have to iterate over prefixes first in order to have proper ordering inside the value slices
	// 先遍历前缀，保证同一前缀下多个请求头的值按配置顺序排列。
	for _, prefix := range headerPrefixes {
		for headerName, vv := range h {
			if !hasPrefixIgnoreCase(headerName, prefix) {
				continue
			}

			extraKey := unescapeExtraKey(strings.ToLower(headerName[len(prefix):]))
			ret[extraKey] = append(ret[extraKey], vv...)
		}
	}

	return ret
}
