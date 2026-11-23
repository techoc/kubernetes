//go:build freebsd || linux || darwin

/*
Copyright 2017 The Kubernetes Authors.

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
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const (
	// unixProtocol is the network protocol of unix socket.
	// unixProtocol 是 unix socket 在网络层的协议名，用于 net.Listen / net.Dialer。
	unixProtocol = "unix"
)

// CreateListener creates a listener on the specified endpoint.
// CreateListener 在给定端点上创建一个 unix socket 监听器。
//
// 关键步骤：
//  1. 解析端点，只接受 unix socket（tcp 等协议在此平台不支持）；
//  2. 先 unlink 掉旧的 socket 文件，避免 "address already in use"；
//  3. 创建 socket 文件所在的目录，权限 0750；
//  4. 在临时文件上建立监听，再 rename 到目标路径——这样即使中途失败也不会留下损坏的 socket 文件；
//  5. 返回监听器。
func CreateListener(endpoint string) (net.Listener, error) {
	protocol, addr, err := parseEndpointWithFallbackProtocol(endpoint, unixProtocol)
	// 解析失败直接返回错误。
	if err != nil {
		return nil, err
	}
	// 类 Unix 平台只支持 unix socket。
	if protocol != unixProtocol {
		return nil, fmt.Errorf("only support unix socket endpoint")
	}

	// Unlink to cleanup the previous socket file.
	// 清理可能残留的旧 socket 文件；文件不存在是正常情况，不视为错误。
	err = unix.Unlink(addr)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to unlink socket file %q: %w", addr, err)
	}

	// 确保 socket 所在目录存在，权限设为 0750（仅属主及同组可访问）。
	if err := os.MkdirAll(filepath.Dir(addr), 0750); err != nil {
		return nil, fmt.Errorf("error creating socket directory %q: %w", filepath.Dir(addr), err)
	}

	// Create the socket on a tempfile and move it to the destination socket to handle improper cleanup
	// 先在同目录创建一个临时文件，随后删掉它以腾出路径；
	// 这样即使监听建立失败，也不会污染最终的目标 socket 路径。
	file, err := os.CreateTemp(filepath.Dir(addr), "")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file: %w", err)
	}

	if err := os.Remove(file.Name()); err != nil {
		return nil, fmt.Errorf("failed to remove temporary file: %w", err)
	}

	// 在临时文件路径上真正建立监听。
	l, err := net.Listen(protocol, file.Name())
	if err != nil {
		return nil, err
	}

	// 监听成功后原子地把临时文件改名为目标地址，完成 socket 的发布。
	if err = os.Rename(file.Name(), addr); err != nil {
		return nil, fmt.Errorf("failed to move temporary file to addr %q: %w", addr, err)
	}

	return l, nil
}

// GetAddressAndDialer returns the address parsed from the given endpoint and a context dialer.
// GetAddressAndDialer 解析端点，返回可用于 gRPC 拨号的 socket 路径以及支持 context 取消的拨号函数。
// 与 CreateListener 一样，此平台只接受 unix socket。
func GetAddressAndDialer(endpoint string) (string, func(ctx context.Context, addr string) (net.Conn, error), error) {
	protocol, addr, err := parseEndpointWithFallbackProtocol(endpoint, unixProtocol)
	if err != nil {
		return "", nil, err
	}
	if protocol != unixProtocol {
		return "", nil, fmt.Errorf("only support unix socket endpoint")
	}

	return addr, dial, nil
}

// dial 使用 unix 协议建立连接，并把 ctx 传给 DialContext 以支持取消与超时。
func dial(ctx context.Context, addr string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, unixProtocol, addr)
}

// parseEndpointWithFallbackProtocol 先按完整 URL 解析端点；
// 若解析失败且没有识别出协议（protocol 为空），则认为是旧式的裸路径，
// 自动补上 fallbackProtocol + "://" 后再解析一次，以兼容历史配置。
func parseEndpointWithFallbackProtocol(endpoint string, fallbackProtocol string) (protocol string, addr string, err error) {
	if protocol, addr, err = parseEndpoint(endpoint); err != nil && protocol == "" {
		fallbackEndpoint := fallbackProtocol + "://" + endpoint
		protocol, addr, err = parseEndpoint(fallbackEndpoint)
	}
	return
}

// parseEndpoint 按 URL scheme 解析运行时端点，返回协议名与实际地址。
//
// 支持的 scheme：
//   - tcp：返回 host（形如 host:port）；
//   - unix：返回 URL 中的路径部分，即 socket 文件路径；
//   - 空 scheme：视为已弃用的写法，返回错误并提示使用完整 URL；
//   - 其他 scheme：协议不支持错误。
func parseEndpoint(endpoint string) (string, string, error) {
	u, err := url.Parse(endpoint)
	// URL 语法本身非法（如包含空格）时直接返回。
	if err != nil {
		return "", "", err
	}

	switch u.Scheme {
	case "tcp":
		return "tcp", u.Host, nil

	case "unix":
		return "unix", u.Path, nil

	case "":
		// 兼容提示：裸端点已弃用，应使用 unix:///path 形式。
		return "", "", fmt.Errorf("using %q as endpoint is deprecated, please consider using full url format", endpoint)

	default:
		return u.Scheme, "", fmt.Errorf("protocol %q not supported", u.Scheme)
	}
}
