/*
Copyright The Kubernetes Authors.

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

package cri

import "strings"

// clientTargetForAddress 把运行时地址转换为 gRPC 可以正确解析的 target 字符串。
//
// 背景：gRPC 对不带 scheme 的 target 默认使用 DNS 解析器，而 unix socket 路径并不是域名。
// 因此对于 socket 风格的地址，需要使用 passthrough 解析器，让自定义的 dialer 拿到原始路径。
func clientTargetForAddress(addr string) string {
	// Defensive: normalize callers that pass a unix endpoint instead of the
	// parsed socket path.
	// 防御性处理：有些调用方会直接传入 unix:/// 形式的端点而不是已解析出的 socket 路径，
	// 这里统一去掉 unix:// 前缀，只保留路径部分。
	if strings.HasPrefix(addr, "unix:///") {
		addr = strings.TrimPrefix(addr, "unix://")
	}

	// grpc defaults to the DNS resolver for bare targets. Use the passthrough
	// resolver for socket-style addresses so the custom dialer gets the raw path.
	// 以 / 开头的地址视为 unix socket（或 Windows 命名管道的绝对路径），
	// 加上 passthrough:/// 前缀，避免 gRPC 走 DNS 解析导致连接失败。
	if strings.HasPrefix(addr, "/") {
		return "passthrough:///" + addr
	}
	// TCP 地址（如 127.0.0.1:1234）保持原样，交给 gRPC 默认解析器处理。
	return addr
}
