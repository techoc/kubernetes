/*
Copyright 2023 The Kubernetes Authors.

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

package portforward

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/httpstream"
	httpstreamspdy "k8s.io/apimachinery/pkg/util/httpstream/spdy"
	constants "k8s.io/apimachinery/pkg/util/portforward"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/transport/websocket"
	"k8s.io/klog/v2"
	streamhttp "k8s.io/streaming/pkg/httpstream"
	streamspdy "k8s.io/streaming/pkg/httpstream/spdy"
)

// PingPeriod 是 SPDY 连接上发送 ping 的间隔，用于及时探测并清理失效的隧道连接。
const PingPeriod = 10 * time.Second

// tunnelingDialer implements "httpstream.Dial" interface
//
// tunnelingDialer 先用 WebSocket 建立外层隧道，再在其上承载 SPDY 多路复用，
// 从而绕过某些代理对原生 SPDY 升级的限制。
type tunnelingDialer struct {
	logger    klog.Logger
	url       *url.URL
	transport http.RoundTripper
	// holder 跟踪 WebSocket 连接，供 transport 在关闭时统一清理，避免泄漏。
	holder websocket.ConnectionHolder
}

// streamingTunnelingDialer implements "k8s.io/streaming/pkg/httpstream.Dialer"
// for in-tree callers.
//
// 与 tunnelingDialer 行为一致，只是使用 k8s.io/streaming 中的类型，
// 便于 in-tree 调用方逐步迁移到新接口。
type streamingTunnelingDialer struct {
	logger    klog.Logger
	url       *url.URL
	transport http.RoundTripper
	holder    websocket.ConnectionHolder
}

// NewTunnelingDialer creates and returns the tunnelingDialer structure which implemements the "httpstream.Dialer"
// interface. The dialer can upgrade a websocket request, creating a websocket connection. This function
// returns an error if one occurs.
//
//logcheck:context // NewSPDYOverWebsocketDialerWithLogger should be used instead of NewSPDYOverWebsocketDialer in code which supports contextual logging.
func NewSPDYOverWebsocketDialer(url *url.URL, config *restclient.Config) (httpstream.Dialer, error) {
	return NewSPDYOverWebsocketDialerWithLogger(klog.Background(), url, config)
}

// NewTunnelingDialer creates and returns the tunnelingDialer structure which implemements the "httpstream.Dialer"
// interface. The dialer can upgrade a websocket request, creating a websocket connection. This function
// returns an error if one occurs.
func NewSPDYOverWebsocketDialerWithLogger(logger klog.Logger, url *url.URL, config *restclient.Config) (httpstream.Dialer, error) {
	// RoundTripperFor 根据 rest.Config 构造可执行 WebSocket 升级的 transport，
	// 并返回 ConnectionHolder 以便统一管理连接生命周期。
	transport, holder, err := websocket.RoundTripperFor(config)
	if err != nil {
		return nil, err
	}
	return &tunnelingDialer{
		logger:    logger,
		url:       url,
		transport: transport,
		holder:    holder,
	}, nil
}

// NewSPDYOverWebsocketDialerForStreaming creates a SPDY-over-websocket dialer
// for in-tree callers that use k8s.io/streaming/pkg/httpstream types.
//
//logcheck:context // NewSPDYOverWebsocketDialerForStreamingWithLogger should be used instead of NewSPDYOverWebsocketDialerForStreaming in code which supports contextual logging.
func NewSPDYOverWebsocketDialerForStreaming(url *url.URL, config *restclient.Config) (streamhttp.Dialer, error) {
	return NewSPDYOverWebsocketDialerForStreamingWithLogger(klog.Background(), url, config)
}

// NewSPDYOverWebsocketDialerForStreamingWithLogger creates a SPDY-over-websocket dialer
// for in-tree callers that use k8s.io/streaming/pkg/httpstream types.
func NewSPDYOverWebsocketDialerForStreamingWithLogger(logger klog.Logger, url *url.URL, config *restclient.Config) (streamhttp.Dialer, error) {
	transport, holder, err := websocket.RoundTripperFor(config)
	if err != nil {
		return nil, err
	}
	return &streamingTunnelingDialer{
		logger:    logger,
		url:       url,
		transport: transport,
		holder:    holder,
	}, nil
}

// negotiateSPDYOverWebsocket 完成 WebSocket 握手并协商出隧道协议子协议。
// 协议名会被加上 tunneling 前缀，以便服务端识别这是隧道式 SPDY 而非普通 SPDY。
func negotiateSPDYOverWebsocket(logger klog.Logger, url *url.URL, transport http.RoundTripper, holder websocket.ConnectionHolder, protocols ...string) (*TunnelingConnection, string, error) {
	// There is no passed context, so use the background context when creating request for now.
	// 该层没有来自调用方的 ctx（Dial 接口未接收 context），只能使用 background 派生。
	ctx := klog.NewContext(context.Background(), logger)

	// Websockets requires "GET" method: RFC 6455 Sec. 4.1 (page 17).
	req, err := http.NewRequestWithContext(ctx, "GET", url.String(), nil)
	if err != nil {
		return nil, "", err
	}
	// Add the spdy tunneling prefix to the requested protocols. The tunneling
	// handler will know how to negotiate these protocols.
	tunnelingProtocols := make([]string, 0, len(protocols))
	for _, protocol := range protocols {
		tunnelingProtocol := constants.WebsocketsSPDYTunnelingPrefix + protocol
		tunnelingProtocols = append(tunnelingProtocols, tunnelingProtocol)
	}
	logger.V(4).Info("Before WebSocket Upgrade Connection...")
	conn, err := websocket.Negotiate(transport, holder, req, tunnelingProtocols...)
	if err != nil {
		return nil, "", err
	}
	if conn == nil {
		return nil, "", fmt.Errorf("negotiated websocket connection is nil")
	}
	// 服务端返回的子协议带前缀，去掉后才是调用方可见的 SPDY 协议名。
	protocol := conn.Subprotocol()
	protocol = strings.TrimPrefix(protocol, constants.WebsocketsSPDYTunnelingPrefix)
	logger.V(4).Info("Negotiation complete", "protocol", protocol)

	return NewTunnelingConnectionWithLogger(klog.LoggerWithName(logger, "client"), conn), protocol, nil
}

// Dial upgrades to a tunneling streaming connection, returning a SPDY connection
// containing a WebSockets connection (which implements "net.Conn"). Also
// returns the protocol negotiated, or an error.
func (d *tunnelingDialer) Dial(protocols ...string) (httpstream.Connection, string, error) {
	tConn, protocol, err := negotiateSPDYOverWebsocket(d.logger, d.url, d.transport, d.holder, protocols...)
	if err != nil {
		return nil, "", err
	}
	// Create SPDY connection injecting the previously created tunneling connection.
	// 以 PingPeriod 周期性发送 ping，用于快速发现半开连接。
	spdyConn, err := httpstreamspdy.NewClientConnectionWithPings(tConn, PingPeriod)

	return spdyConn, protocol, err
}

// Dial upgrades to a tunneling streaming connection for callers using
// k8s.io/streaming/pkg/httpstream types.
func (d *streamingTunnelingDialer) Dial(protocols ...string) (streamhttp.Connection, string, error) {
	tConn, protocol, err := negotiateSPDYOverWebsocket(d.logger, d.url, d.transport, d.holder, protocols...)
	if err != nil {
		return nil, "", err
	}
	// Create SPDY connection injecting the previously created tunneling connection.
	spdyConn, err := streamspdy.NewClientConnectionWithPings(tConn, PingPeriod)

	return spdyConn, protocol, err
}
