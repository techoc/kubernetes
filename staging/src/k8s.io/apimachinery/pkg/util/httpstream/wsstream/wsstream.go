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

package wsstream

import (
	"io"
	"net/http"
	"time"

	"golang.org/x/net/websocket"

	"k8s.io/klog/v2"
	streamws "k8s.io/streaming/pkg/httpstream/wsstream"
)

// 这些常量是上游实现中协议头与通道协议的别名。
const (
	WebSocketProtocolHeader        = streamws.WebSocketProtocolHeader
	ChannelWebSocketProtocol       = streamws.ChannelWebSocketProtocol
	Base64ChannelWebSocketProtocol = streamws.Base64ChannelWebSocketProtocol
)

// ChannelType 是上游通道类型的别名。
type ChannelType = streamws.ChannelType

// 通道方向常量，与上游 ChannelType 定义一一对应。
const (
	IgnoreChannel    = streamws.IgnoreChannel
	ReadChannel      = streamws.ReadChannel
	WriteChannel     = streamws.WriteChannel
	ReadWriteChannel = streamws.ReadWriteChannel
)

// IsWebSocketRequest 判断请求是否为 WebSocket 升级请求。
func IsWebSocketRequest(req *http.Request) bool {
	return streamws.IsWebSocketRequest(req)
}

// IsWebSocketRequestWithStreamCloseProtocol 判断请求是否协商了流关闭协议。
func IsWebSocketRequestWithStreamCloseProtocol(req *http.Request) bool {
	return streamws.IsWebSocketRequestWithStreamCloseProtocol(req)
}

// IsWebSocketRequestWithTunnelingProtocol 判断请求是否协商了隧道协议（用于 SPDY 等）。
func IsWebSocketRequestWithTunnelingProtocol(req *http.Request) bool {
	return streamws.IsWebSocketRequestWithTunnelingProtocol(req)
}

// IgnoreReceives 在 timeout 内持续丢弃连接上的数据，用于防止对端因写入阻塞。
func IgnoreReceives(ws *websocket.Conn, timeout time.Duration) {
	streamws.IgnoreReceives(ws, timeout)
}

// IgnoreReceivesWithLogger 与 IgnoreReceives 相同，但使用指定的 logger 记录日志。
func IgnoreReceivesWithLogger(logger klog.Logger, ws *websocket.Conn, timeout time.Duration) {
	streamws.IgnoreReceivesWithLogger(logger, ws, timeout)
}

// ChannelProtocolConfig 是上游通道协议配置的别名。
type ChannelProtocolConfig = streamws.ChannelProtocolConfig

// NewDefaultChannelProtocols 为给定通道列表构造默认的协议映射。
func NewDefaultChannelProtocols(channels []ChannelType) map[string]ChannelProtocolConfig {
	return streamws.NewDefaultChannelProtocols(channels)
}

// Conn 是上游多路复用 WebSocket 连接的别名。
type Conn = streamws.Conn

// NewConn 基于协议映射创建一个多路复用的 WebSocket 连接。
func NewConn(protocols map[string]ChannelProtocolConfig) *Conn {
	return streamws.NewConn(protocols)
}

// ReaderProtocolConfig 是上游读取端协议配置的别名。
type ReaderProtocolConfig = streamws.ReaderProtocolConfig

// NewDefaultReaderProtocols 返回默认的读取端协议映射。
func NewDefaultReaderProtocols() map[string]ReaderProtocolConfig {
	return streamws.NewDefaultReaderProtocols()
}

// Reader 是上游 WebSocket 协议读取器的别名。
type Reader = streamws.Reader

// NewReader 从 io.Reader 构造协议读取器；ping 为 true 时响应 ping 帧。
func NewReader(r io.Reader, ping bool, protocols map[string]ReaderProtocolConfig) *Reader {
	//nolint:logcheck // 此处有意使用不带 context 的变体。
	return streamws.NewReader(r, ping, protocols)
}

// NewReaderWithLogger 与 NewReader 相同，但使用指定的 logger。
func NewReaderWithLogger(logger klog.Logger, r io.Reader, ping bool, protocols map[string]ReaderProtocolConfig) *Reader {
	return streamws.NewReaderWithLogger(logger, r, ping, protocols)
}
