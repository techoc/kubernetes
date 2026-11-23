/*
Copyright 2015 The Kubernetes Authors.

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

// Package portforward contains server-side logic for handling port forwarding requests.
// Package portforward 提供服务端处理端口转发请求的逻辑。
package portforward

// ProtocolV1Name is the name of the subprotocol used for port forwarding.
// ProtocolV1Name 是端口转发所使用的子协议名称。
const ProtocolV1Name = "portforward.k8s.io"

// SupportedProtocols are the supported port forwarding protocols.
// SupportedProtocols 列出服务端支持的端口转发协议。
var SupportedProtocols = []string{ProtocolV1Name}

const (
	// 以下常量定义端口转发的协议名称，以及在 WebSocket 上隧道化 SPDY 时使用的子协议前缀与组合值。
	PortForwardV1Name                    = ProtocolV1Name
	WebsocketsSPDYTunnelingPrefix        = "SPDY/3.1+"
	KubernetesSuffix                     = ".k8s.io"
	WebsocketsSPDYTunnelingPortForwardV1 = WebsocketsSPDYTunnelingPrefix + PortForwardV1Name

	// 以下常量定义请求中用于传递目标端口和请求 ID 的头（或查询参数）名称。
	PortHeader                 = "port"
	PortForwardRequestIDHeader = "requestID"

	// 以下常量定义流的类型标识：流类型头名称、数据流的取值以及错误流的取值。
	StreamType      = "streamType"
	StreamTypeData  = "data"
	StreamTypeError = "error"
)
