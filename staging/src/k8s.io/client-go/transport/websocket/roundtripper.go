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

// Package websocket 把 HTTP 请求升级为 WebSocket 连接，供 exec/port-forward 等流式接口使用。
// 它复用 rest.Config 的 TLS/代理配置，并在握手失败时把错误转换为
// httpstream.UpgradeFailureError，以便上层决定是否回退到 SPDY。
package websocket

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	gwebsocket "github.com/gorilla/websocket"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/streaming/pkg/httpstream/wsstream"
)

// 编译期断言：RoundTripper 同时满足 TLS 配置持有者与标准 RoundTripper 接口。
var (
	_ utilnet.TLSClientConfigHolder = &RoundTripper{}
	_ http.RoundTripper             = &RoundTripper{}
)

// statusScheme 只注册 metav1.Status，用于把握手失败响应体解析成 StatusError。
var (
	statusScheme = runtime.NewScheme()
	statusCodecs = serializer.NewCodecFactory(statusScheme)
)

func init() {
	statusScheme.AddUnversionedTypes(metav1.SchemeGroupVersion,
		&metav1.Status{},
	)
}

// ConnectionHolder defines functions for structure providing
// access to the websocket connection.
type ConnectionHolder interface {
	DataBufferSize() int
	Connection() *gwebsocket.Conn
}

// RoundTripper knows how to establish a connection to a remote WebSocket endpoint and make it available for use.
// RoundTripper must not be reused.
//
// 因为一次成功握手后连接被保存在 rt.Conn 中，复用同一个实例会让第二次请求
// 覆盖前者，所以调用方必须为每个连接创建独立实例。
type RoundTripper struct {
	// TLSConfig holds the TLS configuration settings to use when connecting
	// to the remote server.
	TLSConfig *tls.Config

	// Proxier specifies a function to return a proxy for a given
	// Request. If the function returns a non-nil error, the
	// request is aborted with the provided error.
	// If Proxy is nil or returns a nil *URL, no proxy is used.
	Proxier func(req *http.Request) (*url.URL, error)

	// Conn holds the WebSocket connection after a round trip.
	Conn *gwebsocket.Conn
}

// Connection returns the stored websocket connection.
func (rt *RoundTripper) Connection() *gwebsocket.Conn {
	return rt.Conn
}

// DataBufferSize returns the size of buffers for the
// websocket connection.
//
// 32KB 足以容纳常规 exec 数据帧，同时避免为每条流分配过大内存。
func (rt *RoundTripper) DataBufferSize() int {
	return 32 * 1024
}

// TLSClientConfig implements pkg/util/net.TLSClientConfigHolder.
func (rt *RoundTripper) TLSClientConfig() *tls.Config {
	return rt.TLSConfig
}

// RoundTrip connects to the remote websocket using the headers in the request and the TLS
// configuration from the config
func (rt *RoundTripper) RoundTrip(request *http.Request) (retResp *http.Response, retErr error) {
	defer func() {
		if request.Body != nil {
			err := request.Body.Close()
			if retErr == nil {
				retErr = err
			}
		}
	}()

	// set the protocol version directly on the dialer from the header
	// 子协议通过请求头传入，但 gorilla/websocket 要求通过 Dialer.Subprotocols 指定，
	// 因此取出后要从 Header 中删除，避免作为普通头重复发送。
	protocolVersions := request.Header[wsstream.WebSocketProtocolHeader]
	delete(request.Header, wsstream.WebSocketProtocolHeader)

	dialer := gwebsocket.Dialer{
		Proxy:           rt.Proxier,
		TLSClientConfig: rt.TLSConfig,
		Subprotocols:    protocolVersions,
		ReadBufferSize:  rt.DataBufferSize() + 1024, // add space for the protocol byte indicating which channel the data is for
		WriteBufferSize: rt.DataBufferSize() + 1024, // add space for the protocol byte indicating which channel the data is for
	}
	// http/https 需要转换成 ws/wss，其他 scheme 无法升级。
	switch request.URL.Scheme {
	case "https":
		request.URL.Scheme = "wss"
	case "http":
		request.URL.Scheme = "ws"
	default:
		return nil, fmt.Errorf("unknown url scheme: %s", request.URL.Scheme)
	}
	wsConn, resp, err := dialer.DialContext(request.Context(), request.URL.String(), request.Header)
	if err != nil {
		// BadHandshake error becomes an "UpgradeFailureError" (used for streaming fallback).
		if errors.Is(err, gwebsocket.ErrBadHandshake) {
			cause := err
			// Enhance the error message with the error response if possible.
			if resp != nil && len(resp.Status) > 0 {
				defer resp.Body.Close()                         //nolint:errcheck
				cause = fmt.Errorf("%w (%s)", err, resp.Status) // Always add the response status
				responseError := ""
				responseErrorBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
				if readErr != nil {
					cause = fmt.Errorf("%w: unable to read error from server response", cause)
				} else {
					// If returned error can be decoded as "metav1.Status", return a "StatusError".
					// 服务端通常以 Status 描述拒绝原因，保留它可以向上层暴露 403/404 等语义。
					responseError = strings.TrimSpace(string(responseErrorBytes))
					if len(responseError) > 0 {
						if obj, _, decodeErr := statusCodecs.UniversalDecoder().Decode(responseErrorBytes, nil, &metav1.Status{}); decodeErr == nil {
							if status, ok := obj.(*metav1.Status); ok {
								cause = &apierrors.StatusError{ErrStatus: *status}
							}
						} else {
							// Otherwise, append the responseError string.
							cause = fmt.Errorf("%w: %s", cause, responseError)
						}
					}
				}
			}
			return nil, &httpstream.UpgradeFailureError{Cause: cause}
		}
		return nil, err
	}

	// Ensure we got back a protocol we understand
	// 服务端可能选择我们未请求的子协议，此时连接不可用，必须关闭并按升级失败处理。
	foundProtocol := false
	for _, protocolVersion := range protocolVersions {
		if protocolVersion == wsConn.Subprotocol() {
			foundProtocol = true
			break
		}
	}
	if !foundProtocol {
		wsConn.Close() // nolint:errcheck
		return nil, &httpstream.UpgradeFailureError{Cause: fmt.Errorf("invalid protocol, expected one of %q, got %q", protocolVersions, wsConn.Subprotocol())}
	}

	rt.Conn = wsConn

	return resp, nil
}

// RoundTripperFor transforms the passed rest config into a wrapped roundtripper, as well
// as a pointer to the websocket RoundTripper. The websocket RoundTripper contains the
// websocket connection after RoundTrip() on the wrapper. Returns an error if there is
// a problem creating the round trippers.
//
// 返回的 wrapper 负责认证等 HTTP 层逻辑，真正的 WebSocket 握手仍由第二个返回值完成；
// 两者必须配套使用。
func RoundTripperFor(config *restclient.Config) (http.RoundTripper, ConnectionHolder, error) {
	transportCfg, err := config.TransportConfig()
	if err != nil {
		return nil, nil, err
	}
	tlsConfig, err := transport.TLSConfigFor(transportCfg)
	if err != nil {
		return nil, nil, err
	}
	proxy := config.Proxy
	if proxy == nil {
		// 默认代理函数需遵守 NO_PROXY/CIDR 排除规则，与普通 REST 客户端保持一致。
		proxy = utilnet.NewProxierWithNoProxyCIDR(http.ProxyFromEnvironment)
	}

	upgradeRoundTripper := &RoundTripper{
		TLSConfig: tlsConfig,
		Proxier:   proxy,
	}
	wrapper, err := transport.HTTPWrappersForConfig(transportCfg, upgradeRoundTripper)
	if err != nil {
		return nil, nil, err
	}
	return wrapper, upgradeRoundTripper, nil
}

// Negotiate opens a connection to a remote server and attempts to negotiate
// a WebSocket connection. Upon success, it returns the negotiated connection.
// The round tripper rt must use the WebSocket round tripper wsRt - see RoundTripperFor.
func Negotiate(rt http.RoundTripper, connectionInfo ConnectionHolder, req *http.Request, protocols ...string) (*gwebsocket.Conn, error) {
	// Plumb protocols to RoundTripper#RoundTrip
	req.Header[wsstream.WebSocketProtocolHeader] = protocols
	resp, err := rt.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	// 握手的 HTTP 响应体此时已无用途，关闭它以释放资源；
	// 失败时连已建立的 WebSocket 一并关闭，避免连接泄漏。
	err = resp.Body.Close()
	if err != nil {
		connectionInfo.Connection().Close()
		return nil, fmt.Errorf("error closing response body: %v", err)
	}
	return connectionInfo.Connection(), nil
}
