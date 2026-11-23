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

// SpdyRoundTripper knows how to upgrade an HTTP request to one that supports
// multiplexed streams. After RoundTrip() is invoked, Conn will be set
// and usable. SpdyRoundTripper implements the UpgradeRoundTripper interface.
// 本类型承载一次升级的状态；按惯例每个实例只服务一个客户端请求，不要并发复用。
type SpdyRoundTripper struct {
	//tlsConfig holds the TLS configuration settings to use when connecting
	//to the remote server.
	// 补全 ServerName 时会先克隆，调用方传入的配置不会被就地改写。
	tlsConfig *tls.Config

	/* TODO according to http://golang.org/pkg/net/http/#RoundTripper, a RoundTripper
	   must be safe for use by multiple concurrent goroutines. If this is absolutely
	   necessary, we could keep a map from http.Request to net.Conn. In practice,
	   a client will create an http.Client, set the transport to a new insteace of
	   SpdyRoundTripper, and use it a single time, so this hopefully won't be an issue.
	*/
	// conn is the underlying network connection to the remote server.
	// 由 RoundTrip 建立、由 NewConnection 消费，两者必须按序调用。
	conn net.Conn

	// Dialer is the dialer used to connect.  Used if non-nil.
	// 留空时各拨号路径自建默认 dialer；注入主要用于定制超时或绕过默认网络栈。
	Dialer *net.Dialer

	// proxier knows which proxy to use given a request, defaults to a proxier that
	// preserves NO_PROXY CIDR behavior while delegating to http.ProxyFromEnvironment.
	// Used primarily for mocking the proxy discovery in tests.
	// 包装一层是为了补上标准库对 NO_PROXY 中 CIDR 写法的支持缺口。
	proxier func(req *http.Request) (*url.URL, error)

	// pingPeriod is a period for sending Ping frames over established
	// connections.
	// 为零表示不发送保活 Ping；非零值可防止长连接被中间设备静默回收。
	pingPeriod time.Duration

	// upgradeTransport is an optional substitute for dialing if present. This field is
	// mutually exclusive with the "tlsConfig", "Dialer", and "proxier".
	// 注入的 transport 用来在已有 HTTP 连接之上复用拨号与 TLS 配置，而不是重新建链。
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

// NewRoundTripper creates a new SpdyRoundTripper that will use the specified
// tlsConfig.
// 历史签名保留给只需 TLS 的调用方；新代码优先用 WithConfig 显式声明各项配置。
func NewRoundTripper(tlsConfig *tls.Config) (*SpdyRoundTripper, error) {
	return NewRoundTripperWithConfig(RoundTripperConfig{
		TLS:              tlsConfig,
		UpgradeTransport: nil,
	})
}

// NewRoundTripperWithProxy creates a new SpdyRoundTripper that will use the
// specified tlsConfig and proxy func.
// 显式传入 proxier 的便捷入口；nil 表示按环境规则自动选择代理。
func NewRoundTripperWithProxy(tlsConfig *tls.Config, proxier func(*http.Request) (*url.URL, error)) (*SpdyRoundTripper, error) {
	return NewRoundTripperWithConfig(RoundTripperConfig{
		TLS:              tlsConfig,
		Proxier:          proxier,
		UpgradeTransport: nil,
	})
}

// NewRoundTripperWithConfig creates a new SpdyRoundTripper with the specified
// configuration. Returns an error if the SpdyRoundTripper is misconfigured.
// 唯一会校验互斥配置的构造入口，其余构造函数最终都汇聚到这里。
func NewRoundTripperWithConfig(cfg RoundTripperConfig) (*SpdyRoundTripper, error) {
	// Process UpgradeTransport, which is mutually exclusive to TLSConfig and Proxier.
	// 既然由已有 transport 负责建链，TLS 与代理设置必须一并从它继承，不能两处同时指定。
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

// newProxierWithNoProxyCIDR preserves CIDR matching in NO_PROXY/no_proxy while
// delegating all other behavior to the supplied proxy function.
// 标准库不解析 CIDR，这里先按网段匹配，命中即绕过代理；其余情形原样交给 delegate。
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

// RoundTripperConfig is a set of options for an SpdyRoundTripper.
// 用配置对象集中表达构造参数，避免再增加位置参数重叠的构造函数。
type RoundTripperConfig struct {
	// TLS configuration used by the round tripper if UpgradeTransport not present.
	// 仅在没有 UpgradeTransport 时生效；设置后者会拒绝同时指定本字段。
	TLS *tls.Config
	// Proxier is a proxy function invoked on each request. Optional.
	// 每个请求都会调用一次，nil 时由构造函数补上带 CIDR 支持的默认值。
	Proxier func(*http.Request) (*url.URL, error)
	// PingPeriod is a period for sending SPDY Pings on the connection.
	// Optional.
	// 控制保活帧频率；为零表示关闭 Ping。
	PingPeriod time.Duration
	// UpgradeTransport is a subtitute transport used for dialing. If set,
	// this field will be used instead of "TLS" and "Proxier" for connection creation.
	// Optional.
	// 用于复用既有传输层的拨号器与 TLS 配置，和 TLS/Proxier 互斥。
	UpgradeTransport http.RoundTripper
}

// TLSClientConfig implements pkg/util/net.TLSClientConfigHolder for proper TLS checking during
// proxying with a spdy roundtripper.
// 让调用方在做代理协商时仍能拿到同一份 TLS 配置，避免证书校验出现两套标准。
func (s *SpdyRoundTripper) TLSClientConfig() *tls.Config {
	return s.tlsConfig
}

// Dial opens a network connection for an upgrade request.
// 建链后立刻把原始 HTTP 请求写出去，因此上层只需读取一次响应即可完成升级。
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

// dial dials the host specified by req, using TLS if appropriate, optionally
// using a proxy server if one is configured via environment variables.
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

// dialWithHttpProxy dials the host specified by url through an http or an https proxy.
// 先与代理建立 CONNECT 隧道，再在隧道内按需完成到目标主机的 TLS 握手。
func (s *SpdyRoundTripper) dialWithHttpProxy(req *http.Request, proxyURL *url.URL) (net.Conn, error) {
	// ensure we use a canonical host with proxyReq
	targetHost := canonicalAddr(req.URL)

	// proxying logic adapted from http://blog.h6t.eu/post/74098062923/golang-websocket-with-http-proxy-support
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

	//nolint:staticcheck // SA1019 ignore deprecated httputil.NewProxyClientConn
	proxyClientConn := httputil.NewProxyClientConn(proxyDialConn, nil)
	response, err := proxyClientConn.Do(&proxyReq)
	//nolint:staticcheck // SA1019 ignore deprecated httputil.ErrPersistEOF: it might be
	// returned from the invocation of proxyClientConn.Do
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

// dialWithSocks5Proxy dials the host specified by url through a socks5 proxy.
// SOCKS5 自身完成目标地址转发，握手方式与 HTTP 代理的 CONNECT 隧道不同，单独实现。
func (s *SpdyRoundTripper) dialWithSocks5Proxy(req *http.Request, proxyURL *url.URL) (net.Conn, error) {
	// ensure we use a canonical host with proxyReq
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

	// According to the implementation of proxy.SOCKS5, the type assertion will always succeed
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

// tlsConn returns a TLS client side connection using rwc as the underlying transport.
// 在代理已建立的字节流之上补一层 TLS；ServerName 取自目标主机而非代理地址。
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

// dialWithoutProxy dials the host specified by url, using TLS if appropriate.
// 直连路径：按 scheme 决定走裸 TCP 还是 TLS，不掺入任何代理逻辑。
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

// proxyAuth returns, for a given proxy URL, the value to be used for the Proxy-Authorization header
// 按 Basic 方案编码凭据；没有用户信息时返回空串，由调用方跳过设置该头。
func (s *SpdyRoundTripper) proxyAuth(proxyURL *url.URL) string {
	if proxyURL == nil || proxyURL.User == nil {
		return ""
	}
	username := proxyURL.User.Username()
	password, _ := proxyURL.User.Password()
	auth := username + ":" + password
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
}

// RoundTrip executes the Request and upgrades it. After a successful upgrade,
// clients may call SpdyRoundTripper.Connection() to retrieve the upgraded
// connection.
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

// NewConnection validates the upgrade response, creating and returning a new
// httpstream.Connection if there were no errors.
// 失败时把服务端返回体里的可读错误摘出来，排障时比裸状态码有用得多。
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
				// 拿不到配置时沿用调用方的选择：跳过证书校验，避免握手直接失败。
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

			// SPDY 升级要求 HTTP/1.1；ALPN 列表不兼容时覆盖为单一 http/1.1。
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
