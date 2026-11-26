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

// Package rest 实现 Kubernetes 的 REST 客户端：负责请求构造、序列化、认证与传输层配置。
package rest

import (
	"fmt"
	"net/url"
	"path"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// DefaultServerURL 按 Kubernetes API 约定，把 host、host:port 或 URL 字符串转换为客户端
// 在给定 API 版本下使用的默认 base 路径。
//
// 三个返回值分别是：带（可选）前缀路径的 base URL、相对 base 的版本化 API 路径、错误。
// 若传入的 host 不含 scheme，则按 defaultTLS 补上 http:// 或 https://。
func DefaultServerURL(host, apiPath string, groupVersion schema.GroupVersion, defaultTLS bool) (*url.URL, string, error) {
	if host == "" {
		return nil, "", fmt.Errorf("host must be a URL or a host:port pair")
	}
	// 先按完整 URL 解析；失败或缺少 scheme/host 时才把它当作 host:port 处理。
	base := host
	hostURL, err := url.Parse(base)
	if err != nil || hostURL.Scheme == "" || hostURL.Host == "" {
		scheme := "http://"
		if defaultTLS {
			scheme = "https://"
		}
		hostURL, err = url.Parse(scheme + base)
		if err != nil {
			return nil, "", err
		}
		// 补 scheme 之后若仍带有非根路径，说明输入既不是纯 host:port 也不是合法 URL。
		if hostURL.Path != "" && hostURL.Path != "/" {
			return nil, "", fmt.Errorf("host must be a URL or a host:port pair: %q", base)
		}
	}

	// hostURL.Path 可选，非空时作为访问该主机所有 URI 的前缀。适用于 apiserver 前有反向代理
	// 改写端点路径的场景：例如代理把 /a/b/c 的请求转发给 apiserver，此时 Path 应为 /a/b/c。
	// 没有这类代理时应留空。
	//
	// versionedAPIPath 是相对 baseURL.Path 的版本化 API 基路径。
	versionedAPIPath := DefaultVersionedAPIPath(apiPath, groupVersion)

	return hostURL, versionedAPIPath, nil
}

// DefaultVersionedAPIPath 按 Kubernetes API 约定，基于给定的 API 路径为指定 group version 构造默认路径。
//
// 核心组不带 group 段（/api/v1），其他组形如 /apis/<group>/<version>；这里只负责把
// apiPath、group、version 拼接起来，具体是 api 还是 apis 前缀由调用方传入的 apiPath 决定。
func DefaultVersionedAPIPath(apiPath string, groupVersion schema.GroupVersion) string {
	versionedAPIPath := path.Join("/", apiPath)

	// 在路径末尾追加版本。
	if len(groupVersion.Group) > 0 {
		versionedAPIPath = path.Join(versionedAPIPath, groupVersion.Group, groupVersion.Version)

	} else {
		versionedAPIPath = path.Join(versionedAPIPath, groupVersion.Version)
	}

	return versionedAPIPath
}

// DefaultServerUrlFor 由 IsConfigTransportTLS 与 RESTClientFor 共用，调用前必须设置 Host 与 Version。
//
// 是否默认使用 TLS 取决于配置里是否出现任何凭据材料（CA 或客户端证书），或者显式设置了
// Insecure。Insecure 的语义是“仍用 HTTPS，但不校验证书”，并非改用明文 HTTP。
func DefaultServerUrlFor(config *Config) (*url.URL, string, error) {
	// TODO: 待 apiserver 默认支持 TLS 后，把默认行为改为安全连接。
	// config.Insecure 的语义是“仍用 HTTPS，但不校验证书”。
	hasCA := len(config.CAFile) != 0 || len(config.CAData) != 0
	hasCert := len(config.CertFile) != 0 || len(config.CertData) != 0
	defaultTLS := hasCA || hasCert || config.Insecure
	host := config.Host
	if host == "" {
		host = "localhost"
	}

	if config.GroupVersion != nil {
		return DefaultServerURL(host, config.APIPath, *config.GroupVersion, defaultTLS)
	}
	return DefaultServerURL(host, config.APIPath, schema.GroupVersion{}, defaultTLS)
}
