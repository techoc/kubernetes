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

// Package portforward 定义 port-forward 子协议相关的常量。
package portforward

const (
	// PortForwardV1Name 是 port-forward v1 子协议的注册名称。
	PortForwardV1Name = "portforward.k8s.io"
	// WebsocketsSPDYTunnelingPrefix 是 WebSocket 上承载 SPDY 隧道的子协议前缀。
	WebsocketsSPDYTunnelingPrefix = "SPDY/3.1+"
	// KubernetesSuffix 是 Kubernetes 相关域名使用的后缀。
	KubernetesSuffix = ".k8s.io"
	// WebsocketsSPDYTunnelingPortForwardV1 是 WebSocket 上承载 port-forward v1 的完整子协议名，
	// 由前缀与协议名拼接而成。
	WebsocketsSPDYTunnelingPortForwardV1 = WebsocketsSPDYTunnelingPrefix + PortForwardV1Name
)
