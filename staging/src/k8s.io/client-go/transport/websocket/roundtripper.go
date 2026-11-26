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

// ConnectionHolder 定义提供连接的结构所需的方法。
// 访问 websocket 连接。
type ConnectionHolder interface {
	DataBufferSize() int
	Connection() *gwebsocket.Conn
}

// RoundTripper 负责建立与远端 WebSocket 端点的连接并供上层使用，不得复用：
// 一次成功握手后连接保存在 rt.Conn 中，复用同一个实例会让第二次请求
// 覆盖前者，所以调用方必须为每个连接创建独立实例。
type RoundTripper struct {
	// TLSConfig 保存连接时使用的 TLS 配置。
	// 到远端服务端。
	TLSConfig *tls.Config

	// Proxier 指定为给定请求返回代理的函数。
	// 请求。函数返回非 nil 错误时，请求以该错误中止。Proxy 为 nil 或返回 nil *URL
	// 时不使用代理。
	Proxier func(req *http.Request) (*url.URL, error)

	// Conn 保存 round trip 之后得到的 WebSocket 连接。
	Conn *gwebsocket.Conn
}

// Connection 返回保存的 WebSocket 连接。
func (rt *RoundTripper) Connection() *gwebsocket.Conn {
	return rt.Conn
}

// DataBufferSize 返回 WebSocket 连接的缓冲区大小。
// 32KB 足以容纳常规 exec 数据帧，同时避免为每条流分配过大内存。
func (rt *RoundTripper) DataBufferSize() int {
	return 32 * 1024
}

// TLSClientConfig 实现 pkg/util/net.TLSClientConfigHolder。
func (rt *RoundTripper) TLSClientConfig() *tls.Config {
	return rt.TLSConfig
}

// RoundTrip 用请求中的 header 与配置中的 TLS 设置连接远端 websocket。
func (rt *RoundTripper) RoundTrip(request *http.Request) (retResp *http.Response, retErr error) {
	defer func() {
		if request.Body != nil {
			err := request.Body.Close()
			if retErr == nil {
				retErr = err
			}
		}
	}()

	// 从请求头取出协议版本并直接设置到 dialer 上：
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
		// BadHandshake 错误会变成 "UpgradeFailureError"（用于流式回退）。
		if errors.Is(err, gwebsocket.ErrBadHandshake) {
			cause := err
			// 若可能，用错误响应增强错误消息。
			if resp != nil && len(resp.Status) > 0 {
				defer resp.Body.Close()                         //nolint:errcheck
				cause = fmt.Errorf("%w (%s)", err, resp.Status) // Always add the response status
				responseError := ""
				responseErrorBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
				if readErr != nil {
					cause = fmt.Errorf("%w: unable to read error from server response", cause)
				} else {
					// 若返回的错误体能解码为 "metav1.Status"，就返回 "StatusError"：
					// 服务端通常以 Status 描述拒绝原因，保留它可以向上层暴露 403/404 等语义。
					responseError = strings.TrimSpace(string(responseErrorBytes))
					if len(responseError) > 0 {
						if obj, _, decodeErr := statusCodecs.UniversalDecoder().Decode(responseErrorBytes, nil, &metav1.Status{}); decodeErr == nil {
							if status, ok := obj.(*metav1.Status); ok {
								cause = &apierrors.StatusError{ErrStatus: *status}
							}
						} else {
							// 否则追加 responseError 字符串。
							cause = fmt.Errorf("%w: %s", cause, responseError)
						}
					}
				}
			}
			return nil, &httpstream.UpgradeFailureError{Cause: cause}
		}
		return nil, err
	}

	// 确认协商回的是我们认识的子协议：
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

// RoundTripperFor 把传入的 rest config 转换为一个包装 roundtripper，并同时返回
// WebSocket RoundTripper 的指针；对包装 roundtripper 调用 RoundTrip() 之后，
// WebSocket RoundTripper 即持有建立的 WebSocket 连接。创建 round tripper 出错时返回错误。
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

// Negotiate 连接远端服务端并尝试协商协议。
// WebSocket 连接。成功时返回协商好的连接。
// round tripper rt 必须使用 WebSocket round tripper wsRt——见 RoundTripperFor。
func Negotiate(rt http.RoundTripper, connectionInfo ConnectionHolder, req *http.Request, protocols ...string) (*gwebsocket.Conn, error) {
	// 把协议透传给 RoundTripper#RoundTrip。
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
