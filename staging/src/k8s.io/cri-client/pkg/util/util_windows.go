//go:build windows

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
	"strings"

	"github.com/Microsoft/go-winio"
)

const (
	// tcpProtocol 表示 TCP 网络协议。
	tcpProtocol = "tcp"
	// npipeProtocol 表示 Windows 命名管道（named pipe）协议。
	npipeProtocol = "npipe"
)

// CreateListener creates a listener on the specified endpoint.
// CreateListener 在 Windows 平台上根据端点协议创建监听器，支持 tcp 与 npipe（命名管道）两种。
func CreateListener(endpoint string) (net.Listener, error) {
	protocol, addr, err := parseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	switch protocol {
	case tcpProtocol:
		// TCP 端点直接交给标准库监听。
		return net.Listen(tcpProtocol, addr)

	case npipeProtocol:
		// 命名管道需要借助 go-winio 库实现。
		return winio.ListenPipe(addr, nil)

	default:
		return nil, fmt.Errorf("only support tcp and npipe endpoint")
	}
}

// GetAddressAndDialer returns the address parsed from the given endpoint and a context dialer.
// GetAddressAndDialer 解析端点并返回对应的拨号函数：TCP 用标准库拨号，npipe 用 winio 拨号。
// 两个拨号函数都接受 context，以便 gRPC 取消连接时及时中止。
func GetAddressAndDialer(endpoint string) (string, func(ctx context.Context, addr string) (net.Conn, error), error) {
	protocol, addr, err := parseEndpoint(endpoint)
	if err != nil {
		return "", nil, err
	}

	if protocol == tcpProtocol {
		return addr, tcpDial, nil
	}

	if protocol == npipeProtocol {
		return addr, npipeDial, nil
	}

	return "", nil, fmt.Errorf("only support tcp and npipe endpoint")
}

// tcpDial 用标准库的 Dialer 建立 TCP 连接，并传入 ctx 支持取消/超时。
func tcpDial(ctx context.Context, addr string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, tcpProtocol, addr)
}

// npipeDial 通过 go-winio 建立 Windows 命名管道连接。
func npipeDial(ctx context.Context, addr string) (net.Conn, error) {
	return winio.DialPipeContext(ctx, addr)
}

// parseEndpoint 解析 Windows 平台的运行时端点，返回协议与地址。
//
// 关键处理：
//   - url.Parse 不认识反斜杠 "\"，因此先把所有 "\" 替换为 "/"，避免解析失败；
//   - tcp：取 host（形如 host:port）；
//   - npipe：若路径已经是 //./pipe 开头则直接返回；否则用 host（缺省为 "."）与路径拼出
//     规范形式 //<host><path>，例如 npipe://./pipe/foo -> //./pipe/foo；
//   - 空 scheme 视为已弃用写法；其他 scheme 不支持。
func parseEndpoint(endpoint string) (string, string, error) {
	// url.Parse doesn't recognize \, so replace with / first.
	// Windows 路径用反斜杠分隔，URL 解析器不支持，先统一替换为正斜杠。
	endpoint = strings.Replace(endpoint, "\\", "/", -1)
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", "", err
	}

	if u.Scheme == "tcp" {
		return "tcp", u.Host, nil
	} else if u.Scheme == "npipe" {
		// 已经是规范形式 //./pipe/... 时直接返回。
		if strings.HasPrefix(u.Path, "//./pipe") {
			return "npipe", u.Path, nil
		}

		// fallback host if not provided.
		// 未提供 host 时默认使用 "."（本机），拼出标准命名管道路径。
		host := u.Host
		if host == "" {
			host = "."
		}
		return "npipe", fmt.Sprintf("//%s%s", host, u.Path), nil
	} else if u.Scheme == "" {
		return "", "", fmt.Errorf("Using %q as endpoint is deprecated, please consider using full url format", endpoint)
	} else {
		return u.Scheme, "", fmt.Errorf("protocol %q not supported", u.Scheme)
	}
}
