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

import "testing"

// TestClientTargetForAddress 验证 clientTargetForAddress 对各类地址的转换结果。
// 重点覆盖：unix socket 路径、带 unix:// 前缀的端点、Windows 命名管道路径，以及普通的 TCP 地址。
func TestClientTargetForAddress(t *testing.T) {
	tests := []struct {
		name string // 用例名
		addr string // 输入地址
		want string // 期望的 gRPC target
	}{
		{
			// 原生 socket 路径：直接加 passthrough:/// 前缀。
			name: "unix socket path",
			addr: "/run/containerd/containerd.sock",
			want: "passthrough:////run/containerd/containerd.sock",
		},
		{
			// 带 unix:/// 前缀的端点：先剥掉前缀，再按路径处理，结果应与上一用例一致。
			name: "unix socket endpoint",
			addr: "unix:///run/containerd/containerd.sock",
			want: "passthrough:////run/containerd/containerd.sock",
		},
		{
			// Windows 命名管道以 // 开头，同样走 passthrough 分支。
			name: "windows pipe path",
			addr: "//./pipe/containerd-containerd",
			want: "passthrough://///./pipe/containerd-containerd",
		},
		{
			// TCP 地址不含 scheme 开头路径，应保持原样返回。
			name: "tcp address",
			addr: "127.0.0.1:1234",
			want: "127.0.0.1:1234",
		},
	}

	// 遍历用例，逐个以子测试运行，便于定位失败场景。
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := clientTargetForAddress(tc.addr); got != tc.want {
				t.Fatalf("clientTargetForAddress(%q) = %q, want %q", tc.addr, got, tc.want)
			}
		})
	}
}
