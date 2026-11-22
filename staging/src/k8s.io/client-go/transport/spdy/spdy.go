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

// Package spdy 提供把普通 HTTP 连接升级为 SPDY 流的拨号与协商逻辑，
// 并在 k8s.io/apimachinery 的 httpstream 与 k8s.io/streaming 的新接口之间做适配。
package spdy

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"k8s.io/apimachinery/pkg/util/httpstream"
	httpstreamspdy "k8s.io/apimachinery/pkg/util/httpstream/spdy"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	streamhttp "k8s.io/streaming/pkg/httpstream"
)

// Upgrader validates a response from the server after a SPDY upgrade.
type Upgrader interface {
	// NewConnection validates the response and creates a new Connection.
	NewConnection(resp *http.Response) (httpstream.Connection, error)
}

// RoundTripperFor returns a round tripper and upgrader to use with SPDY.
//
// 返回的 upgrader 与 round tripper 必须配套使用：请求经 round tripper 发出后，
// 由 upgrader 校验 101 响应并从连接中接管底层流。
func RoundTripperFor(config *restclient.Config) (http.RoundTripper, Upgrader, error) {
	tlsConfig, err := restclient.TLSConfigFor(config)
	if err != nil {
		return nil, nil, err
	}
	proxy := http.ProxyFromEnvironment
	if config.Proxy != nil {
		proxy = config.Proxy
	}
	// 5 秒的 ping 周期用于及时发现半开连接，避免 exec/attach 会话静默悬挂。
	upgradeRoundTripper, err := httpstreamspdy.NewRoundTripperWithConfig(httpstreamspdy.RoundTripperConfig{
		TLS:              tlsConfig,
		Proxier:          proxy,
		PingPeriod:       time.Second * 5,
		UpgradeTransport: nil,
	})
	if err != nil {
		return nil, nil, err
	}
	// wrapper 叠加认证/限速等 rest.Config 中的传输层包装，使升级请求也经过它们。
	wrapper, err := restclient.HTTPWrappersForConfig(config, upgradeRoundTripper)
	if err != nil {
		return nil, nil, err
	}
	return wrapper, upgradeRoundTripper, nil
}

// dialer implements the httpstream.Dialer interface.
type dialer struct {
	client   *http.Client
	upgrader Upgrader
	method   string
	url      *url.URL
}

// 编译期断言拨号器实现标准接口，便于在 remotecommand 等通用代码中复用。
var _ httpstream.Dialer = &dialer{}

// NewDialer will create a dialer that connects to the provided URL and upgrades the connection to SPDY.
func NewDialer(upgrader Upgrader, client *http.Client, method string, url *url.URL) httpstream.Dialer {
	return &dialer{
		client:   client,
		upgrader: upgrader,
		method:   method,
		url:      url,
	}
}

// NewDialerForStreaming creates a SPDY dialer for in-tree callers that use
// k8s.io/streaming/pkg/httpstream types.
func NewDialerForStreaming(upgrader Upgrader, client *http.Client, method string, url *url.URL) streamhttp.Dialer {
	return &streamingDialerAdapter{delegate: NewDialer(upgrader, client, method, url)}
}

// NewUpgraderForStreaming adapts a streaming upgrader for callers that need
// the compatibility Upgrader interface.
func NewUpgraderForStreaming(upgrader streamhttp.UpgradeRoundTripper) Upgrader {
	return &compatUpgraderAdapter{delegate: upgrader}
}

// Dial 用预设的 method/url 构造请求并进入协议协商；protocols 是客户端支持的子协议列表。
func (d *dialer) Dial(protocols ...string) (httpstream.Connection, string, error) {
	req, err := http.NewRequest(d.method, d.url.String(), nil)
	if err != nil {
		return nil, "", fmt.Errorf("error creating request: %v", err)
	}
	return Negotiate(d.upgrader, d.client, req, protocols...)
}

// Negotiate opens a connection to a remote server and attempts to negotiate
// a SPDY connection. Upon success, it returns the connection and the protocol selected by
// the server. The client transport must use the upgradeRoundTripper - see RoundTripperFor.
func Negotiate(upgrader Upgrader, client *http.Client, req *http.Request, protocols ...string) (httpstream.Connection, string, error) {
	// 同一 header 可重复添加，等价于逗号分隔的候选协议列表。
	for i := range protocols {
		req.Header.Add(httpstream.HeaderProtocolVersion, protocols[i])
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()
	// 服务端在响应头中回选一个协议；调用方需据此决定使用哪套流语义。
	conn, err := upgrader.NewConnection(resp)
	if err != nil {
		return nil, "", err
	}
	return conn, resp.Header.Get(httpstream.HeaderProtocolVersion), nil
}

// NegotiateStreaming is for in-tree callers that still operate on
// k8s.io/streaming/pkg/httpstream types.
func NegotiateStreaming(upgrader Upgrader, client *http.Client, req *http.Request, protocols ...string) (streamhttp.Connection, string, error) {
	conn, protocol, err := Negotiate(upgrader, client, req, protocols...)
	if err != nil {
		return nil, "", err
	}
	return wrapStreamingConnection(conn), protocol, nil
}

// 适配层保持两套接口（apimachinery 与 streaming）双向可用，避免调用方因迁移而重复实现。
type streamingDialerAdapter struct {
	delegate httpstream.Dialer
}

func (d *streamingDialerAdapter) Dial(protocols ...string) (streamhttp.Connection, string, error) {
	conn, protocol, err := d.delegate.Dial(protocols...)
	if err != nil {
		return nil, "", err
	}
	return wrapStreamingConnection(conn), protocol, nil
}

type compatUpgraderAdapter struct {
	delegate streamhttp.UpgradeRoundTripper
}

func (u *compatUpgraderAdapter) NewConnection(resp *http.Response) (httpstream.Connection, error) {
	conn, err := u.delegate.NewConnection(resp)
	if err != nil {
		return nil, err
	}
	return wrapCompatConnection(conn), nil
}

type streamingStreamAdapter struct {
	delegate httpstream.Stream
}

func (s *streamingStreamAdapter) Read(p []byte) (int, error) {
	return s.delegate.Read(p)
}

func (s *streamingStreamAdapter) Write(p []byte) (int, error) {
	return s.delegate.Write(p)
}

func (s *streamingStreamAdapter) Close() error {
	return s.delegate.Close()
}

func (s *streamingStreamAdapter) Reset() error {
	return s.delegate.Reset()
}

func (s *streamingStreamAdapter) Headers() http.Header {
	return s.delegate.Headers()
}

func (s *streamingStreamAdapter) Identifier() uint32 {
	return s.delegate.Identifier()
}

type streamingConnectionAdapter struct {
	delegate httpstream.Connection
}

func (c *streamingConnectionAdapter) CreateStream(headers http.Header) (streamhttp.Stream, error) {
	stream, err := c.delegate.CreateStream(headers)
	if err != nil {
		return nil, err
	}
	return &streamingStreamAdapter{delegate: stream}, nil
}

func (c *streamingConnectionAdapter) Close() error {
	return c.delegate.Close()
}

func (c *streamingConnectionAdapter) CloseChan() <-chan bool {
	return c.delegate.CloseChan()
}

func (c *streamingConnectionAdapter) SetIdleTimeout(timeout time.Duration) {
	c.delegate.SetIdleTimeout(timeout)
}

func (c *streamingConnectionAdapter) RemoveStreams(streams ...streamhttp.Stream) {
	compatStreams := make([]httpstream.Stream, 0, len(streams))
	for _, stream := range streams {
		if stream == nil {
			continue
		}
		if s, ok := stream.(*streamingStreamAdapter); ok {
			compatStreams = append(compatStreams, s.delegate)
			continue
		}
		if s, ok := stream.(httpstream.Stream); ok {
			compatStreams = append(compatStreams, s)
			continue
		}
		//nolint:logcheck // Extending the httpstream.Connection API is not worth it for contextual, structured logging of this.
		klog.V(5).Infof("dropping unadaptable streaming stream %T in RemoveStreams", stream)
	}
	c.delegate.RemoveStreams(compatStreams...)
}

func wrapStreamingConnection(conn httpstream.Connection) streamhttp.Connection {
	if conn == nil {
		return nil
	}
	if wrapped, ok := conn.(*compatConnectionAdapter); ok {
		return wrapped.delegate
	}
	return &streamingConnectionAdapter{delegate: conn}
}

type compatStreamAdapter struct {
	delegate streamhttp.Stream
}

func (s *compatStreamAdapter) Read(p []byte) (int, error) {
	return s.delegate.Read(p)
}

func (s *compatStreamAdapter) Write(p []byte) (int, error) {
	return s.delegate.Write(p)
}

func (s *compatStreamAdapter) Close() error {
	return s.delegate.Close()
}

func (s *compatStreamAdapter) Reset() error {
	return s.delegate.Reset()
}

func (s *compatStreamAdapter) Headers() http.Header {
	return s.delegate.Headers()
}

func (s *compatStreamAdapter) Identifier() uint32 {
	return s.delegate.Identifier()
}

type compatConnectionAdapter struct {
	delegate streamhttp.Connection
}

func (c *compatConnectionAdapter) CreateStream(headers http.Header) (httpstream.Stream, error) {
	stream, err := c.delegate.CreateStream(headers)
	if err != nil {
		return nil, err
	}
	return &compatStreamAdapter{delegate: stream}, nil
}

func (c *compatConnectionAdapter) Close() error {
	return c.delegate.Close()
}

func (c *compatConnectionAdapter) CloseChan() <-chan bool {
	return c.delegate.CloseChan()
}

func (c *compatConnectionAdapter) SetIdleTimeout(timeout time.Duration) {
	c.delegate.SetIdleTimeout(timeout)
}

func (c *compatConnectionAdapter) RemoveStreams(streams ...httpstream.Stream) {
	streamingStreams := make([]streamhttp.Stream, 0, len(streams))
	for _, stream := range streams {
		if stream == nil {
			continue
		}
		if s, ok := stream.(*compatStreamAdapter); ok {
			streamingStreams = append(streamingStreams, s.delegate)
			continue
		}
		if s, ok := stream.(streamhttp.Stream); ok {
			streamingStreams = append(streamingStreams, s)
			continue
		}
		//nolint:logcheck // Extending the API is not worth it for contextual, structured logging of this.
		klog.V(5).Infof("dropping unadaptable compat stream %T in RemoveStreams", stream)
	}
	c.delegate.RemoveStreams(streamingStreams...)
}

func wrapCompatConnection(conn streamhttp.Connection) httpstream.Connection {
	if conn == nil {
		return nil
	}
	if wrapped, ok := conn.(*streamingConnectionAdapter); ok {
		return wrapped.delegate
	}
	return &compatConnectionAdapter{delegate: conn}
}
