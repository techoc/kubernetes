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
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/net/proxy"
	"k8s.io/streaming/pkg/httpstream"
	utilnet "k8s.io/utils/net"
)

// SpdyRoundTripper 将 HTTP 请求升级为支持多路复用的连接，并实现 UpgradeRoundTripper。
// 调用 RoundTrip 后，conn 会被设置并可交给 NewConnection 使用。
// 本类型承载一次升级的状态；每个实例只应服务一个客户端请求，不能并发复用。
type SpdyRoundTripper struct {
	// tlsConfig 保存连接远端服务端时使用的 TLS 配置。
	// 补全 ServerName 时会先克隆，调用方传入的配置不会被就地改写。
	tlsConfig *tls.Config

	/* 待办：根据 http://golang.org/pkg/net/http/#RoundTripper，RoundTripper 必须支持并发调用。
	   如果确有必要，可以用 http.Request 到 net.Conn 的映射隔离每次连接。
	   实际调用方通常只创建一个 SpdyRoundTripper 实例并使用一次，因此当前实现未保证并发安全。
	*/
	// conn 是到远端服务端的底层网络连接，由 RoundTrip 建立、由 NewConnection 消费。
	conn net.Conn

	// Dialer 是连接所用的拨号器；非零时使用调用方提供的实例。
	// 留空时各拨号路径自建默认 dialer；注入主要用于定制超时或绕过默认网络栈。
	Dialer *net.Dialer

	// proxier 根据请求选择代理，默认保留 NO_PROXY 的 CIDR 匹配行为，
	// 并将其他判断委托给 http.ProxyFromEnvironment；测试也可用它替换代理解析。
	proxier func(req *http.Request) (*url.URL, error)

	// pingPeriod 是已建立连接发送 Ping 的周期；零值表示不发送保活 Ping。
	pingPeriod time.Duration

	// upgradeTransport 可替代默认拨号路径，与 tlsConfig、Dialer 和 proxier 互斥。
	// 注入的 transport 用于复用其拨号逻辑与 TLS 配置，而不是重新建链。
	upgradeTransport http.RoundTripper
}

// 只暴露读取 TLS 配置的能力，便于从任意 transport 中抽取证书设置而不依赖具体类型。
type tlsClientConfigHolder interface {
	TLSClientConfig() *tls.Config
}

// 递归展开被包装的 RoundTripper，直到找到携带 TLS 配置或拨号器的那一层。
type roundTripperWrapper interface {
	http.RoundTripper
	WrappedRoundTripper() http.RoundTripper
}

// 与 net.Dialer.DialContext 同签名，便于从自定义 transport 中直接借用拨号函数。
type dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

var _ tlsClientConfigHolder = &SpdyRoundTripper{}
var _ httpstream.UpgradeRoundTripper = &SpdyRoundTripper{}

// NewRoundTripper 使用指定 TLS 配置创建 SpdyRoundTripper。
// 历史签名保留给只需 TLS 的调用方；新代码优先用 WithConfig 显式声明各项配置。
func NewRoundTripper(tlsConfig *tls.Config) (*SpdyRoundTripper, error) {
	return NewRoundTripperWithConfig(RoundTripperConfig{
		TLS:              tlsConfig,
		UpgradeTransport: nil,
	})
}

// NewRoundTripperWithProxy 使用指定 TLS 配置和代理选择函数创建 SpdyRoundTripper。
// proxier 为 nil 时按环境规则自动选择代理。
func NewRoundTripperWithProxy(tlsConfig *tls.Config, proxier func(*http.Request) (*url.URL, error)) (*SpdyRoundTripper, error) {
	return NewRoundTripperWithConfig(RoundTripperConfig{
		TLS:              tlsConfig,
		Proxier:          proxier,
		UpgradeTransport: nil,
	})
}

// NewRoundTripperWithConfig 按指定配置创建 SpdyRoundTripper，配置互斥时返回错误。
// 其余构造函数最终都汇聚到这个入口。
func NewRoundTripperWithConfig(cfg RoundTripperConfig) (*SpdyRoundTripper, error) {
	// UpgradeTransport 与 TLS、Proxier 互斥；已有 transport 负责建链时，配置必须从它继承。
	if cfg.UpgradeTransport != nil {
		if cfg.TLS != nil || cfg.Proxier != nil {
			return nil, fmt.Errorf("SpdyRoundTripper: UpgradeTransport is mutually exclusive to TLSConfig or Proxier")
		}
		tlsConfig, err := tlsConfigForTransport(cfg.UpgradeTransport)
		if err != nil {
			return nil, fmt.Errorf("SpdyRoundTripper: unable to retrieve TLS config from UpgradeTransport: %w", err)
		}
		cfg.TLS = tlsConfig
	}
	if cfg.Proxier == nil {
		cfg.Proxier = newProxierWithNoProxyCIDR(http.ProxyFromEnvironment)
	}
	return &SpdyRoundTripper{
		tlsConfig:        cfg.TLS,
		proxier:          cfg.Proxier,
		pingPeriod:       cfg.PingPeriod,
		upgradeTransport: cfg.UpgradeTransport,
	}, nil
}

// newProxierWithNoProxyCIDR 保留 NO_PROXY/no_proxy 的 CIDR 匹配，
// 并将其他代理解析行为委托给 delegate，避免改变标准库的既有语义。
func newProxierWithNoProxyCIDR(delegate func(req *http.Request) (*url.URL, error)) func(req *http.Request) (*url.URL, error) {
	// 两个大小写写法都读，遵循环境变量惯例：大写优先。
	noProxyEnv := os.Getenv("NO_PROXY")
	if noProxyEnv == "" {
		noProxyEnv = os.Getenv("no_proxy")
	}
	noProxyRules := strings.Split(noProxyEnv, ",")

	cidrs := make([]*net.IPNet, 0, len(noProxyRules))
	for _, noProxyRule := range noProxyRules {
		noProxyRule = strings.TrimSpace(noProxyRule)
		if noProxyRule == "" {
			continue
		}
		_, cidr, err := utilnet.ParseCIDRSloppy(noProxyRule)
		if err == nil {
			// 解析失败的规则忽略，交给 delegate 按普通 NO_PROXY 语义处理。
			cidrs = append(cidrs, cidr)
		}
	}

	if len(cidrs) == 0 {
		// 没有 CIDR 时无需包装，直接复用原函数。
		return delegate
	}

	return func(req *http.Request) (*url.URL, error) {
		ip := utilnet.ParseIPSloppy(req.URL.Hostname())
		if ip == nil {
			return delegate(req)
		}

		for _, cidr := range cidrs {
			if cidr.Contains(ip) {
				// nil URL 是调用方识别“直连”的约定。
				return nil, nil
			}
		}

		return delegate(req)
	}
}

// RoundTripperConfig 集中表示 SpdyRoundTripper 的构造选项。
type RoundTripperConfig struct {
	// TLS 是未设置 UpgradeTransport 时使用的 TLS 配置；两者不能同时指定。
	TLS *tls.Config
	// Proxier 是每个请求都会调用的代理选择函数；为 nil 时使用支持 NO_PROXY CIDR 的默认实现。
	Proxier func(*http.Request) (*url.URL, error)
	// PingPeriod 是连接发送 SPDY Ping 的周期；为零表示关闭 Ping。
	PingPeriod time.Duration
	// UpgradeTransport 用于复用既有传输层的拨号器与 TLS 配置，可选项，与 TLS、Proxier 互斥。
	UpgradeTransport http.RoundTripper
}

// TLSClientConfig 返回 TLS 配置，供代理协商阶段执行一致的证书校验。
func (s *SpdyRoundTripper) TLSClientConfig() *tls.Config {
	return s.tlsConfig
}

// Dial 为升级请求建立网络连接，并在建链后立即写出原始 HTTP 请求。
// 调用方随后只需读取响应即可完成升级。
func (s *SpdyRoundTripper) Dial(req *http.Request) (net.Conn, error) {
	// 优先用注入的 transport 建链，以复用其拨号逻辑和 TLS 配置。
	var conn net.Conn
	var err error
	if s.upgradeTransport != nil {
		conn, err = dialURLWithTransport(req.Context(), req.URL, s.upgradeTransport)
	} else {
		conn, err = s.dial(req)
	}
	if err != nil {
		return nil, err
	}

	if err := req.Write(conn); err != nil {
		// 请求写失败时连接已不可用，立即关闭，避免泄漏给调用方。
		conn.Close()
		return nil, err
	}

	return conn, nil
}

// dial 按需使用 TLS 和环境变量指定的代理连接 req 中的主机。
// 按代理协议分派到不同实现；未知 scheme 直接报错，避免悄悄直连暴露目标地址。
func (s *SpdyRoundTripper) dial(req *http.Request) (net.Conn, error) {
	proxyURL, err := s.proxier(req)
	if err != nil {
		return nil, err
	}

	if proxyURL == nil {
		// 代理函数返回 nil 表示直连。
		return s.dialWithoutProxy(req.Context(), req.URL)
	}

	switch proxyURL.Scheme {
	case "socks5":
		return s.dialWithSocks5Proxy(req, proxyURL)
	case "https", "http", "":
		return s.dialWithHttpProxy(req, proxyURL)
	}

	return nil, fmt.Errorf("proxy URL scheme not supported: %s", proxyURL.Scheme)
}

// dialWithHttpProxy 通过 HTTP 或 HTTPS 代理连接目标主机。
// 先建立 CONNECT 隧道，再在隧道内按需完成到目标主机的 TLS 握手。
func (s *SpdyRoundTripper) dialWithHttpProxy(req *http.Request, proxyURL *url.URL) (net.Conn, error) {
	// 代理请求必须使用补全端口后的规范主机地址。
	targetHost := canonicalAddr(req.URL)

	// 代理处理逻辑参考 http://blog.h6t.eu/post/74098062923/golang-websocket-with-http-proxy-support
	proxyReq := http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{},
		Host:   targetHost,
	}

	proxyReq = *proxyReq.WithContext(req.Context())

	if pa := s.proxyAuth(proxyURL); pa != "" {
		// 凭据只放在与代理的连接上，不进入发给目标服务器的请求头。
		proxyReq.Header = http.Header{}
		proxyReq.Header.Set("Proxy-Authorization", pa)
	}

	proxyDialConn, err := s.dialWithoutProxy(proxyReq.Context(), proxyURL)
	if err != nil {
		return nil, err
	}

	//nolint:staticcheck // SA1019：httputil.NewProxyClientConn 已废弃，但此路径仍需要它。
	proxyClientConn := httputil.NewProxyClientConn(proxyDialConn, nil)
	response, err := proxyClientConn.Do(&proxyReq)
	//nolint:staticcheck // SA1019：proxyClientConn.Do 可能返回已废弃的 httputil.ErrPersistEOF。
	if err != nil && err != httputil.ErrPersistEOF {
		return nil, err
	}
	if response != nil && response.StatusCode >= 300 || response.StatusCode < 200 {
		// 只用 Redacted() 拼错误信息，避免代理 URL 中的密码被写进日志。
		return nil, fmt.Errorf("CONNECT request to %s returned response: %s", proxyURL.Redacted(), response.Status)
	}

	rwc, _ := proxyClientConn.Hijack()

	if req.URL.Scheme == "https" {
		return s.tlsConn(proxyReq.Context(), rwc, targetHost)
	}
	return rwc, nil
}

// dialWithSocks5Proxy 通过 SOCKS5 代理连接目标主机。
// SOCKS5 自身完成目标地址转发，握手方式与 HTTP 代理的 CONNECT 隧道不同，因此单独实现。
func (s *SpdyRoundTripper) dialWithSocks5Proxy(req *http.Request, proxyURL *url.URL) (net.Conn, error) {
	// 代理握手必须使用补全端口后的规范主机地址。
	targetHost := canonicalAddr(req.URL)
	proxyDialAddr := canonicalAddr(proxyURL)

	var auth *proxy.Auth
	if proxyURL.User != nil {
		pass, _ := proxyURL.User.Password()
		auth = &proxy.Auth{
			User:     proxyURL.User.Username(),
			Password: pass,
		}
	}

	dialer := s.Dialer
	if dialer == nil {
		// 到代理的连接需要一个超时上限，避免 SOCKS5 握手长期挂起。
		dialer = &net.Dialer{
			Timeout: 30 * time.Second,
		}
	}

	proxyDialer, err := proxy.SOCKS5("tcp", proxyDialAddr, auth, dialer)
	if err != nil {
		return nil, err
	}

	// 按 proxy.SOCKS5 的实现，该类型断言始终会成功；保留检查以防上游行为变化。
	contextDialer, ok := proxyDialer.(proxy.ContextDialer)
	if !ok {
		return nil, errors.New("SOCKS5 Dialer must implement ContextDialer")
	}

	proxyDialConn, err := contextDialer.DialContext(req.Context(), "tcp", targetHost)
	if err != nil {
		return nil, err
	}

	if req.URL.Scheme == "https" {
		return s.tlsConn(req.Context(), proxyDialConn, targetHost)
	}
	return proxyDialConn, nil
}

// tlsConn 以 rwc 为底层连接创建 TLS 客户端连接。
// ServerName 取自目标主机而非代理地址，确保证书校验针对最终服务端。
func (s *SpdyRoundTripper) tlsConn(ctx context.Context, rwc net.Conn, targetHost string) (net.Conn, error) {

	host, _, err := net.SplitHostPort(targetHost)
	if err != nil {
		return nil, err
	}

	tlsConfig := s.tlsConfig
	switch {
	case tlsConfig == nil:
		tlsConfig = &tls.Config{ServerName: host}
	case len(tlsConfig.ServerName) == 0:
		// 克隆后再补 ServerName，防止修改共享的调用方配置。
		tlsConfig = tlsConfig.Clone()
		tlsConfig.ServerName = host
	}

	tlsConn := tls.Client(rwc, tlsConfig)

	if err := tlsConn.HandshakeContext(ctx); err != nil {
		// 握手失败时底层 rwc 已被 tlsConn 接管，关闭它即可释放连接。
		tlsConn.Close()
		return nil, err
	}

	return tlsConn, nil
}

// dialWithoutProxy 按需使用 TLS 直连 url 中的主机，不经过代理。
func (s *SpdyRoundTripper) dialWithoutProxy(ctx context.Context, url *url.URL) (net.Conn, error) {
	dialAddr := canonicalAddr(url)
	dialer := s.Dialer
	if dialer == nil {
		dialer = &net.Dialer{}
	}

	if url.Scheme == "http" {
		return dialer.DialContext(ctx, "tcp", dialAddr)
	}

	tlsDialer := tls.Dialer{
		NetDialer: dialer,
		Config:    s.tlsConfig,
	}
	return tlsDialer.DialContext(ctx, "tcp", dialAddr)
}

// proxyAuth 返回用于 Proxy-Authorization 头的值，按 Basic 方案编码代理凭据。
// 没有用户信息时返回空串，由调用方跳过设置该头。
func (s *SpdyRoundTripper) proxyAuth(proxyURL *url.URL) string {
	if proxyURL == nil || proxyURL.User == nil {
		return ""
	}
	username := proxyURL.User.Username()
	password, _ := proxyURL.User.Password()
	auth := username + ":" + password
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
}

// RoundTrip 执行请求并完成 HTTP 升级。成功后可通过 NewConnection 获取升级后的连接。
// 克隆请求与头后再加升级头，绝不污染调用方持有的原请求。
func (s *SpdyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header = req.Header.Clone()
	req.Header.Add(httpstream.HeaderConnection, httpstream.HeaderUpgrade)
	req.Header.Add(httpstream.HeaderUpgrade, HeaderSpdy31)

	conn, err := s.Dial(req)
	if err != nil {
		return nil, err
	}

	responseReader := bufio.NewReader(conn)

	resp, err := http.ReadResponse(responseReader, nil)
	if err != nil {
		// 读不到合法响应时连接留在半开状态没有意义，直接关闭。
		conn.Close()
		return nil, err
	}

	s.conn = conn

	return resp, nil
}

// NewConnection 校验升级响应，校验通过时创建并返回 httpstream.Connection。
// 校验失败时优先返回服务端响应体中的可读错误，便于定位具体原因。
func (s *SpdyRoundTripper) NewConnection(resp *http.Response) (httpstream.Connection, error) {
	connectionHeader := strings.ToLower(resp.Header.Get(httpstream.HeaderConnection))
	upgradeHeader := strings.ToLower(resp.Header.Get(httpstream.HeaderUpgrade))
	if (resp.StatusCode != http.StatusSwitchingProtocols) || !strings.Contains(connectionHeader, strings.ToLower(httpstream.HeaderUpgrade)) || !strings.Contains(upgradeHeader, strings.ToLower(HeaderSpdy31)) {
		defer resp.Body.Close()
		responseErrorBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("unable to upgrade connection: unable to read error from server response")
		}
		return nil, fmt.Errorf("unable to upgrade connection: %s", upgradeErrorMessage(responseErrorBytes))
	}

	return NewClientConnectionWithPings(s.conn, s.pingPeriod)
}

// 从 transport 中提取 TLS 配置：先试已知具体类型，再试能力接口，最后沿包装链递归。
// 任一层都无法提供时返回错误，调用方据此决定是否回退到默认配置。
func tlsConfigForTransport(transport http.RoundTripper) (*tls.Config, error) {
	if transport == nil {
		return nil, nil
	}
	switch transport := transport.(type) {
	case *http.Transport:
		return transport.TLSClientConfig, nil
	case tlsClientConfigHolder:
		return transport.TLSClientConfig(), nil
	case roundTripperWrapper:
		return tlsConfigForTransport(transport.WrappedRoundTripper())
	default:
		return nil, fmt.Errorf("transport %T does not expose TLS client config", transport)
	}
}

// 按 scheme 补全默认端口，得到可直接传给 Dial 的 host:port。
// WebSocket 系列 scheme 也一并处理，因为升级场景里目标 URL 常以 ws/wss 出现。
func canonicalAddr(url *url.URL) string {
	host := url.Hostname()
	port := url.Port()
	if len(port) == 0 {
		switch strings.ToLower(url.Scheme) {
		case "http", "ws":
			port = "80"
		case "https", "wss":
			port = "443"
		}
	}
	return net.JoinHostPort(host, port)
}

// 升级失败时优先取 K8s Status 风格响应里的 message/error 字段，没有则用原始文本。
// 这样客户端日志里呈现的是服务端给出的可读原因，而不是整段 JSON。
func upgradeErrorMessage(responseErrorBytes []byte) string {
	type statusLike struct {
		Message string `json:"message"`
		Error   string `json:"error"`
	}

	var status statusLike
	if err := json.Unmarshal(responseErrorBytes, &status); err == nil {
		if msg := strings.TrimSpace(status.Message); msg != "" {
			return msg
		}
		if msg := strings.TrimSpace(status.Error); msg != "" {
			return msg
		}
	}

	msg := strings.TrimSpace(string(responseErrorBytes))
	if msg == "" {
		return "empty server response"
	}
	return msg
}

// 使用注入 transport 的拨号器与 TLS 配置建链，供 UpgradeTransport 路径复用。
// 抽取配置失败时静默回退到零值 dialer/TLS，尽量不阻断已有连接。
func dialURLWithTransport(ctx context.Context, url *url.URL, transport http.RoundTripper) (net.Conn, error) {
	dialAddr := canonicalAddr(url)

	dialer, err := dialerFor(transport)
	if err != nil {
		dialer = nil
	}

	switch url.Scheme {
	case "http":
		if dialer != nil {
			return dialer(ctx, "tcp", dialAddr)
		}
		var d net.Dialer
		return d.DialContext(ctx, "tcp", dialAddr)
	case "https":
		tlsConfig, err := tlsConfigForTransport(transport)
		if err != nil {
			tlsConfig = nil
		}

		if dialer != nil {
			netConn, err := dialer(ctx, "tcp", dialAddr)
			if err != nil {
				return nil, err
			}

			if tlsConfig == nil {
				// 无法从 transport 提取配置时跳过证书校验，优先保持已有升级路径可用。
				tlsConfig = &tls.Config{InsecureSkipVerify: true}
			} else if len(tlsConfig.ServerName) == 0 && !tlsConfig.InsecureSkipVerify {
				inferredHost := dialAddr
				if host, _, err := net.SplitHostPort(dialAddr); err == nil {
					inferredHost = host
				}
				// 只克隆后补 ServerName，不修改 transport 原有的配置。
				tlsConfigCopy := tlsConfig.Clone()
				tlsConfigCopy.ServerName = inferredHost
				tlsConfig = tlsConfigCopy
			}

			// SPDY 升级要求 HTTP/1.1；未声明 ALPN 或已包含该协议时，统一收敛为单一协议。
			if supportsHTTP11(tlsConfig.NextProtos) {
				tlsConfig = tlsConfig.Clone()
				tlsConfig.NextProtos = []string{"http/1.1"}
			}

			tlsConn := tls.Client(netConn, tlsConfig)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				netConn.Close()
				return nil, err
			}
			return tlsConn, nil
		}

		tlsDialer := tls.Dialer{Config: tlsConfig}
		return tlsDialer.DialContext(ctx, "tcp", dialAddr)
	default:
		return nil, fmt.Errorf("unknown scheme: %s", url.Scheme)
	}
}

func dialerFor(transport http.RoundTripper) (dialFunc, error) {
	if transport == nil {
		return nil, nil
	}

	switch transport := transport.(type) {
	case *http.Transport:
		if transport.DialContext != nil {
			return transport.DialContext, nil
		}
		if transport.Dial != nil {
			return func(ctx context.Context, network, addr string) (net.Conn, error) {
				return transport.Dial(network, addr)
			}, nil
		}
		return nil, nil
	case roundTripperWrapper:
		return dialerFor(transport.WrappedRoundTripper())
	default:
		return nil, fmt.Errorf("unknown transport type: %T", transport)
	}
}

func supportsHTTP11(nextProtos []string) bool {
	if len(nextProtos) == 0 {
		return true
	}
	for _, proto := range nextProtos {
		if proto == "http/1.1" {
			return true
		}
	}
	return false
}
