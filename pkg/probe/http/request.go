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

// Package http 实现 HTTP GET 探针的请求构建逻辑。
// 提供从 v1.HTTPGetAction 配置创建 http.Request 的功能，
// 处理端口解析、URL 格式化、header 设置和 User-Agent 生成。
package http

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/component-base/version"
	"k8s.io/kubernetes/pkg/probe"
)

// NewProbeRequest 返回一个可用于探针的 http.Request。
// 设置默认的 User-Agent 和 Accept header。
func NewProbeRequest(url *url.URL, headers http.Header) (*http.Request, error) {
	return newProbeRequest(url, headers, "probe")
}

// NewRequestForHTTPGetAction 根据 HTTPGetAction 配置创建 http.Request。
// 如果 httpGet.Host 为空，则使用 podIP 作为主机。
// 端口通过 probe.ResolveContainerPort 解析，支持数字和命名端口。
func NewRequestForHTTPGetAction(httpGet *v1.HTTPGetAction, container *v1.Container, podIP string, userAgentFragment string) (*http.Request, error) {
	scheme := strings.ToLower(string(httpGet.Scheme))
	if scheme == "" {
		scheme = "http" // 默认使用 HTTP
	}

	host := httpGet.Host
	if host == "" {
		host = podIP // 未指定 Host 时使用 Pod IP，确保探针请求发送到本 Pod
	}

	// 解析端口：支持数字或命名端口
	port, err := probe.ResolveContainerPort(httpGet.Port, container)
	if err != nil {
		return nil, err
	}

	path := httpGet.Path
	url := formatURL(scheme, host, port, path)
	headers := v1HeaderToHTTPHeader(httpGet.HTTPHeaders)

	return newProbeRequest(url, headers, userAgentFragment)
}

// newProbeRequest 创建带有默认 header 的 GET 请求。
// 如果未设置 User-Agent，则使用 kube-probe/<version> 格式。
// 如果未设置 Accept，则默认为 */*（接受所有类型）。
func newProbeRequest(url *url.URL, headers http.Header, userAgentFragment string) (*http.Request, error) {
	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}

	if headers == nil {
		headers = http.Header{}
	}
	// 设置默认 User-Agent，格式为 kube-<purpose>/<major>.<minor>
	if _, ok := headers["User-Agent"]; !ok {
		headers.Set("User-Agent", userAgent(userAgentFragment))
	}
	// 设置默认 Accept header
	if _, ok := headers["Accept"]; !ok {
		headers.Set("Accept", "*/*") // 接受所有响应类型
	} else if headers.Get("Accept") == "" {
		// Accept header 被显式设置为空值，移除它
		headers.Del("Accept")
	}
	req.Header = headers
	// 如果 header 中设置了 Host，覆盖 req.Host，支持自定义虚拟主机
	req.Host = headers.Get("Host")

	return req, nil
}

// userAgent 生成探针的 User-Agent header 值。
// 格式为 kube-<purpose>/<major>.<minor>，如 kube-probe/1.28
func userAgent(purpose string) string {
	v := version.Get()
	return fmt.Sprintf("kube-%s/%s.%s", purpose, v.Major, v.Minor)
}

// formatURL 根据参数构建 URL，用于测试。
// 如果 path 解析失败，仍然保留原始 path（兼容旧版探针行为）。
func formatURL(scheme string, host string, port int, path string) *url.URL {
	u, err := url.Parse(path)
	// path 格式有问题，但已经太晚拒绝它了，直接传递原始值
	// 这种 URL 构建方式在某些情况下可能不正确，但保留了旧版探针的行为
	if err != nil {
		u = &url.URL{
			Path: path,
		}
	}
	u.Scheme = scheme
	u.Host = net.JoinHostPort(host, strconv.Itoa(port))
	return u
}

// v1HeaderToHTTPHeader 将 v1.HTTPHeader 列表转换为 http.Header 映射。
// 用于将 Pod 配置中的 HTTPHeader 字段转换为 Go 标准库的 header 格式。
func v1HeaderToHTTPHeader(headerList []v1.HTTPHeader) http.Header {
	headers := make(http.Header)
	for _, header := range headerList {
		headers.Add(header.Name, header.Value)
	}
	return headers
}
