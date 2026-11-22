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

package spdy

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"

	apihttpstream "k8s.io/apimachinery/pkg/util/httpstream"
	streamhttp "k8s.io/streaming/pkg/httpstream"
	streamspdy "k8s.io/streaming/pkg/httpstream/spdy"
)

// HeaderSpdy31 是协商使用 SPDY/3.1 协议时使用的 HTTP 头名字。
const HeaderSpdy31 = streamspdy.HeaderSpdy31

// SpdyRoundTripper 是 streaming 模块 SPDY RoundTripper 的兼容性包装。
// 保留这一层是为了让既有调用方无需立即迁移到新模块。
type SpdyRoundTripper struct {
	delegate *streamspdy.SpdyRoundTripper
}

// NewRoundTripper 用给定的 TLS 配置构造 SPDY RoundTripper。
func NewRoundTripper(tlsConfig *tls.Config) (*SpdyRoundTripper, error) {
	delegate, err := streamspdy.NewRoundTripper(tlsConfig)
	if err != nil {
		return nil, err
	}
	return &SpdyRoundTripper{delegate: delegate}, nil
}

// NewRoundTripperWithProxy 构造支持代理函数的 SPDY RoundTripper。
func NewRoundTripperWithProxy(tlsConfig *tls.Config, proxier func(*http.Request) (*url.URL, error)) (*SpdyRoundTripper, error) {
	delegate, err := streamspdy.NewRoundTripperWithProxy(tlsConfig, proxier)
	if err != nil {
		return nil, err
	}
	return &SpdyRoundTripper{delegate: delegate}, nil
}

// RoundTripperConfig 是 SpdyRoundTripper 的选项集合。
type RoundTripperConfig struct {
	// TLS configuration used by the round tripper if UpgradeTransport not present.
	// UpgradeTransport 未设置时使用的 TLS 配置。
	TLS *tls.Config
	// Proxier is a proxy function invoked on each request. Optional.
	// 每次请求时调用的代理函数，可选。
	Proxier func(*http.Request) (*url.URL, error)
	// PingPeriod is a period for sending SPDY Pings on the connection.
	// Optional.
	// 在连接上发送 SPDY Ping 的周期，可选。
	PingPeriod time.Duration
	// UpgradeTransport is a subtitute transport used for dialing. If set,
	// this field will be used instead of "TLS" and "Proxier" for connection creation.
	// Optional.
	// 用于拨号的替代 transport；设置后将取代 TLS 与 Proxier 建立连接。可选。
	UpgradeTransport http.RoundTripper
}

// NewRoundTripperWithConfig 按选项集合构造 RoundTripper，并转发到 streaming 模块的实现。
func NewRoundTripperWithConfig(cfg RoundTripperConfig) (*SpdyRoundTripper, error) {
	delegate, err := streamspdy.NewRoundTripperWithConfig(streamspdy.RoundTripperConfig{
		TLS:              cfg.TLS,
		Proxier:          cfg.Proxier,
		PingPeriod:       cfg.PingPeriod,
		UpgradeTransport: cfg.UpgradeTransport,
	})
	if err != nil {
		return nil, err
	}
	return &SpdyRoundTripper{delegate: delegate}, nil
}

// TLSClientConfig 实现 pkg/util/net.TLSClientConfigHolder，
// 使代理场景下能够正确检查 SPDY 连接的 TLS 配置。
func (s *SpdyRoundTripper) TLSClientConfig() *tls.Config {
	return s.delegate.TLSClientConfig()
}

// Dial 为升级请求建立网络连接。
func (s *SpdyRoundTripper) Dial(req *http.Request) (net.Conn, error) {
	return s.delegate.Dial(req)
}

// RoundTrip 执行请求并把连接升级为 SPDY。
func (s *SpdyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return s.delegate.RoundTrip(req)
}

// NewConnection 校验服务端的升级响应并准备传输层，返回值适配为本包的 Connection 接口。
func (s *SpdyRoundTripper) NewConnection(resp *http.Response) (apihttpstream.Connection, error) {
	conn, err := s.delegate.NewConnection(resp)
	if err != nil {
		return nil, err
	}
	return wrapConnection(conn), nil
}

// responseUpgraderAdapter 把本包的 ResponseUpgrader 接口适配到 streaming 模块的实现，
// 主要是把流处理回调与新接口类型互相转换。
type responseUpgraderAdapter struct {
	delegate streamhttp.ResponseUpgrader
}

func (r *responseUpgraderAdapter) UpgradeResponse(w http.ResponseWriter, req *http.Request, newStreamHandler apihttpstream.NewStreamHandler) apihttpstream.Connection {
	conn := r.delegate.UpgradeResponse(w, req, wrapNewStreamHandler(newStreamHandler))
	return wrapConnection(conn)
}

// NewResponseUpgrader 返回服务端的响应升级器。
func NewResponseUpgrader() apihttpstream.ResponseUpgrader {
	return &responseUpgraderAdapter{delegate: streamspdy.NewResponseUpgrader()}
}

// NewResponseUpgraderWithPings 返回定期发送 Ping 的服务端响应升级器。
func NewResponseUpgraderWithPings(pingPeriod time.Duration) apihttpstream.ResponseUpgrader {
	return &responseUpgraderAdapter{delegate: streamspdy.NewResponseUpgraderWithPings(pingPeriod)}
}

// NewClientConnection 从已建立的网络连接创建客户端 SPDY 连接。
func NewClientConnection(conn net.Conn) (apihttpstream.Connection, error) {
	c, err := streamspdy.NewClientConnection(conn)
	if err != nil {
		return nil, err
	}
	return wrapConnection(c), nil
}

// NewClientConnectionWithPings 创建带定时 Ping 的客户端 SPDY 连接。
func NewClientConnectionWithPings(conn net.Conn, pingPeriod time.Duration) (apihttpstream.Connection, error) {
	c, err := streamspdy.NewClientConnectionWithPings(conn, pingPeriod)
	if err != nil {
		return nil, err
	}
	return wrapConnection(c), nil
}

// NewServerConnection 从已建立的网络连接创建服务端 SPDY 连接，并注册新流处理回调。
func NewServerConnection(conn net.Conn, newStreamHandler apihttpstream.NewStreamHandler) (apihttpstream.Connection, error) {
	c, err := streamspdy.NewServerConnection(conn, wrapNewStreamHandler(newStreamHandler))
	if err != nil {
		return nil, err
	}
	return wrapConnection(c), nil
}

// NewServerConnectionWithPings 创建带定时 Ping 的服务端 SPDY 连接。
func NewServerConnectionWithPings(conn net.Conn, newStreamHandler apihttpstream.NewStreamHandler, pingPeriod time.Duration) (apihttpstream.Connection, error) {
	c, err := streamspdy.NewServerConnectionWithPings(conn, wrapNewStreamHandler(newStreamHandler), pingPeriod)
	if err != nil {
		return nil, err
	}
	return wrapConnection(c), nil
}

// streamAdapter 把 streaming 模块的 Stream 适配为本包的 Stream 接口。
type streamAdapter struct {
	delegate streamhttp.Stream
}

func (s *streamAdapter) Read(p []byte) (int, error) {
	return s.delegate.Read(p)
}

func (s *streamAdapter) Write(p []byte) (int, error) {
	return s.delegate.Write(p)
}

func (s *streamAdapter) Close() error {
	return s.delegate.Close()
}

func (s *streamAdapter) Reset() error {
	return s.delegate.Reset()
}

func (s *streamAdapter) Headers() http.Header {
	return s.delegate.Headers()
}

func (s *streamAdapter) Identifier() uint32 {
	return s.delegate.Identifier()
}

// connectionAdapter 把 streaming 模块的 Connection 适配为本包的 Connection 接口。
type connectionAdapter struct {
	delegate streamhttp.Connection
}

func (c *connectionAdapter) CreateStream(headers http.Header) (apihttpstream.Stream, error) {
	stream, err := c.delegate.CreateStream(headers)
	if err != nil {
		return nil, err
	}
	return &streamAdapter{delegate: stream}, nil
}

func (c *connectionAdapter) Close() error {
	return c.delegate.Close()
}

func (c *connectionAdapter) CloseChan() <-chan bool {
	return c.delegate.CloseChan()
}

func (c *connectionAdapter) SetIdleTimeout(timeout time.Duration) {
	c.delegate.SetIdleTimeout(timeout)
}

// RemoveStreams 只转发实现了 streaming 接口的流，跳过 nil 与外来实现，
// 避免类型断言失败导致 panic。
func (c *connectionAdapter) RemoveStreams(streams ...apihttpstream.Stream) {
	streamingStreams := make([]streamhttp.Stream, 0, len(streams))
	for _, stream := range streams {
		if stream == nil {
			continue
		}
		if s, ok := stream.(streamhttp.Stream); ok {
			streamingStreams = append(streamingStreams, s)
		}
	}
	c.delegate.RemoveStreams(streamingStreams...)
}

// wrapConnection 把 streaming 连接包装为本包接口；nil 原样返回，保持 nil 语义。
func wrapConnection(conn streamhttp.Connection) apihttpstream.Connection {
	if conn == nil {
		return nil
	}
	return &connectionAdapter{delegate: conn}
}

// wrapNewStreamHandler 把本包的新流回调适配为 streaming 模块的回调；
// nil 原样返回，避免把空回调误当作有效处理器。
func wrapNewStreamHandler(newStreamHandler apihttpstream.NewStreamHandler) streamhttp.NewStreamHandler {
	if newStreamHandler == nil {
		return nil
	}
	return func(stream streamhttp.Stream, replySent <-chan struct{}) error {
		return newStreamHandler(&streamAdapter{delegate: stream}, replySent)
	}
}
