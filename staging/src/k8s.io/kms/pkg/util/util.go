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

package util

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	// unixProtocol 是远程 KMS 插件唯一支持的协议（Unix 域套接字）。
	unixProtocol = "unix"
)

// ParseEndpoint 解析 KMS 插件端点 URL，提取并返回 socket 路径。
// 支持形式：
//   - unix:///path/to/socket   -> 返回 /path/to/socket
//   - unix:///@abstract        -> 返回 @abstract（Linux 抽象命名空间 socket）
//
// 校验规则：端点不能为空、必须是合法 URL、scheme 必须为 unix。
func ParseEndpoint(endpoint string) (string, error) {
	// 空端点直接拒绝。
	if len(endpoint) == 0 {
		return "", fmt.Errorf("remote KMS provider can't use empty string as endpoint")
	}

	// 按 URL 解析；语法错误（如含空格）返回错误。
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint %q for remote KMS provider, error: %v", endpoint, err)
	}

	// 只允许 unix scheme，其他（tcp/http 等）一律拒绝。
	if u.Scheme != unixProtocol {
		return "", fmt.Errorf("unsupported scheme %q for remote KMS provider", u.Scheme)
	}

	// Linux 抽象命名空间 socket 不需要实体文件，路径以 @ 开头。
	// 注意：与基于文件的 socket 不同，抽象 socket 没有 ACL 概念；
	// 但它受 Linux 网络命名空间约束，因此只有同一 Pod 内的容器能访问（除非使用 host 网络）。
	// URL 中的 /@xxx 需要去掉前导 /，还原为抽象命名空间地址 @xxx。
	if strings.HasPrefix(u.Path, "/@") {
		return strings.TrimPrefix(u.Path, "/"), nil
	}

	// 普通文件 socket，直接返回路径。
	return u.Path, nil
}
