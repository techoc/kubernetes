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

package net

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/http2"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
)

// JoinPreservingTrailingSlash 对指定元素执行 path.Join，
// 并保留最后一个非空片段末尾的斜杠。
//
// path.Join 会吞掉末尾斜杠，而 URL 语义中 "/foo/" 与 "/foo" 可能不同，
// 因此这里在拼接后按需补回。
func JoinPreservingTrailingSlash(elem ...string) string {
	//先执行基本的路径拼接
	result := path.Join(elem...)

	//查找最后一个非空片段
	for i := len(elem) - 1; i >= 0; i-- {
		if len(elem[i]) > 0 {
			//如果最后一个片段以斜杠结尾，确保结果也如此
			if strings.HasSuffix(elem[i], "/") && !strings.HasSuffix(result, "/") {
				result += "/"
			}
			break
		}
	}

	return result
}

// 如果给定错误是网络超时错误，IsTimeout 返回 true。
//
// 用 errors.As 而非类型断言，保证被包装的 net.Error 也能识别。
func IsTimeout(err error) bool {
	var neterr net.Error
	if errors.As(err, &neterr) {
		return neterr != nil && neterr.Timeout()
	}
	return false
}

// 如果给定错误类似于连接终止，IsProbableEOF 返回 true，
// 此时可以合理地认为 watch 已为空。
// Go HTTP 栈返回这些一般性连接关闭错误；
// 调用方需要区分正常断开与异常连接错误时，
// 应使用此方法。
//
// 判定依赖错误字符串，这是有意为之：底层库没有导出可判定的错误类型。
// 因此这里的匹配集合应保守，只收录已知的"对端正常关闭/连接被重置"文案。
func IsProbableEOF(err error) bool {
	if err == nil {
		return false
	}
	var uerr *url.Error
	if errors.As(err, &uerr) {
		err = uerr.Err
	}
	msg := err.Error()
	switch {
	case err == io.EOF:
		return true
	case err == io.ErrUnexpectedEOF:
		return true
	case msg == "http: can't write HTTP request on broken connection":
		return true
	case strings.Contains(msg, "http2: server sent GOAWAY and closed the connection"):
		return true
	case strings.Contains(msg, "connection reset by peer"):
		return true
	case strings.Contains(strings.ToLower(msg), "use of closed network connection"):
		return true
	}
	return false
}

// defaultTransport 是标准库默认 Transport，用于给自定义 Transport 补齐零值字段。
var defaultTransport = http.DefaultTransport.(*http.Transport)

// SetOldTransportDefaults 为未设置的 Proxy、Dial 和 TLSHandshakeTimeout 字段应用 http.DefaultTransport 的默认值。
//
// "Old" 指不涉及 HTTP/2 的旧默认值；SetTransportDefaults 在此基础上再配置 h2。
func SetOldTransportDefaults(t *http.Transport) *http.Transport {
	if t.Proxy == nil || isDefault(t.Proxy) {
		//http.ProxyFromEnvironment 不尊重 CIDR，因此无法从代理设置中排除 Pod 和 Service IP 等地址。
		//ProxierWithNoProxyCIDR 允许在 NO_PROXY 中使用 CIDR 规则。
		t.Proxy = NewProxierWithNoProxyCIDR(http.ProxyFromEnvironment)
	}
	//未设置自定义 dialer 时使用默认的 context dialer。
	//lint:file-ignore SA1019 继续支持自定义传输器已弃用的 Dial 方法。
	if t.DialContext == nil && t.Dial == nil {
		t.DialContext = defaultTransport.DialContext
	}
	if t.TLSHandshakeTimeout == 0 {
		t.TLSHandshakeTimeout = defaultTransport.TLSHandshakeTimeout
	}
	if t.IdleConnTimeout == 0 {
		t.IdleConnTimeout = defaultTransport.IdleConnTimeout
	}
	return t
}

// SetTransportDefaults 为未设置的 Proxy、Dial 和 TLSHandshakeTimeout 字段应用 http.DefaultTransport 的默认值。
//
// 在旧默认值之上按需启用 HTTP/2；环境变量 DISABLE_HTTP2 提供全局逃生开关，
// 用于排查 h2 相关问题。配置失败只告警不失败，保证客户端仍可用 HTTP/1.1。
func SetTransportDefaults(t *http.Transport) *http.Transport {
	t = SetOldTransportDefaults(t)
	//允许客户端按需禁用 HTTP/2。
	if s := os.Getenv("DISABLE_HTTP2"); len(s) > 0 {
		//nolint:logcheck // 此情况应很少见，不值得转换。
		klog.Info("HTTP2 has been explicitly disabled")
	} else if allowsHTTP2(t) {
		if err := configureHTTP2Transport(t); err != nil {
			//nolint:logcheck // 此情况应很少见，不值得转换。
			klog.Warningf("Transport failed http2 configuration: %v", err)
		}
	}
	return t
}

// readIdleTimeoutSeconds 读取 HTTP/2 连接健康检查的空闲超时，默认 30 秒；
// 设为 0 可关闭检查。非法值回退默认值并告警。
func readIdleTimeoutSeconds() int {
	ret := 30
	//用户可将 readIdleTimeout 设为 0 来禁用 HTTP/2
	//连接健康检查。
	if s := os.Getenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS"); len(s) > 0 {
		i, err := strconv.Atoi(s)
		if err != nil {
			//nolint:logcheck // 此情况应很少见，不值得转换。
			klog.Warningf("Illegal HTTP2_READ_IDLE_TIMEOUT_SECONDS(%q): %v."+
				" Default value %d is used", s, err, ret)
			return ret
		}
		ret = i
	}
	return ret
}

// pingTimeoutSeconds 读取 HTTP/2 PING 超时，默认 15 秒；非法值回退默认值。
func pingTimeoutSeconds() int {
	ret := 15
	if s := os.Getenv("HTTP2_PING_TIMEOUT_SECONDS"); len(s) > 0 {
		i, err := strconv.Atoi(s)
		if err != nil {
			//nolint:logcheck // 此情况应很少见，不值得转换。
			klog.Warningf("Illegal HTTP2_PING_TIMEOUT_SECONDS(%q): %v."+
				" Default value %d is used", s, err, ret)
			return ret
		}
		ret = i
	}
	return ret
}

// configureHTTP2Transport 把 h2 支持挂到 transport 上，并启用连接健康检查。
func configureHTTP2Transport(t *http.Transport) error {
	t2, err := http2.ConfigureTransports(t)
	if err != nil {
		return err
	}
	//以下代码启用 golang/net/pull/55 新增的 HTTP/2 连接健康检查。
	//https://github.com/golang/net/pull/55。健康检查会检测并
	//关闭损坏的传输层连接。没有健康检查时，
	//损坏的连接可能存活过久，例如损坏的 TCP 连接默认要等 Linux 内核
	//经过 13 到 30 分钟才关闭，从而导致
	// https://github.com/kubernetes/client-go/issues/374 and
	// https://github.com/kubernetes/kubernetes/issues/87615.
	t2.ReadIdleTimeout = time.Duration(readIdleTimeoutSeconds()) * time.Second
	t2.PingTimeout = time.Duration(pingTimeoutSeconds()) * time.Second
	return nil
}

// allowsHTTP2 判断是否应启用 h2：
// 未显式设置 NextProtos 时允许（采用 Go 默认行为），显式设置时尊重调用方选择。
func allowsHTTP2(t *http.Transport) bool {
	if t.TLSClientConfig == nil || len(t.TLSClientConfig.NextProtos) == 0 {
		//传输器未表达 NextProto 偏好时允许 HTTP/2
		return true
	}
	for _, p := range t.TLSClientConfig.NextProtos {
		if p == http2.NextProtoTLS {
			//传输器明确允许 HTTP/2
			return true
		}
	}
	//传输器明确设置 NextProtos 且排除了 HTTP/2
	return false
}

// RoundTripperWrapper 由自定义 RoundTripper 实现，用于暴露被包装的底层
// RoundTripper，使 DialerFor/TLSClientConfig/CloseIdleConnectionsFor 能递归穿透。
type RoundTripperWrapper interface {
	http.RoundTripper
	WrappedRoundTripper() http.RoundTripper
}

// DialFunc 与 net.Dialer.DialContext 签名一致，便于直接传递。
type DialFunc func(ctx context.Context, net, addr string) (net.Conn, error)

// DialerFor 从任意 RoundTripper 中提取拨号函数。
// 优先级：DialContext > Dial；都没有则返回 nil（表示使用默认拨号）。
func DialerFor(transport http.RoundTripper) (DialFunc, error) {
	if transport == nil {
		return nil, nil
	}

	switch transport := transport.(type) {
	case *http.Transport:
		//transport.DialContext 优先于 transport.Dial。
		if transport.DialContext != nil {
			return transport.DialContext, nil
		}
		//将 transport.Dial 适配为 DialWithContext 签名。
		if transport.Dial != nil {
			return func(ctx context.Context, net, addr string) (net.Conn, error) {
				return transport.Dial(net, addr)
			}, nil
		}
		//否则返回 nil
		return nil, nil
	case RoundTripperWrapper:
		return DialerFor(transport.WrappedRoundTripper())
	default:
		return nil, fmt.Errorf("unknown transport type: %T", transport)
	}
}

// CloseIdleConnectionsFor 关闭 Transport 的空闲连接。
// 如果 Transport 被包装，则遍历包装的 RoundTripper，
// 直到找到实现 CloseIdleConnections 方法的实例。
// 如果 Transport 没有 CloseIdleConnections 方法，
// 则此函数不执行任何操作。
//
// 先检查 closeIdler 再检查 wrapper：外层 wrapper 自己实现了该方法时优先用外层。
func CloseIdleConnectionsFor(transport http.RoundTripper) {
	if transport == nil {
		return
	}
	type closeIdler interface {
		CloseIdleConnections()
	}

	switch transport := transport.(type) {
	case closeIdler:
		transport.CloseIdleConnections()
	case RoundTripperWrapper:
		CloseIdleConnectionsFor(transport.WrappedRoundTripper())
	default:
		//nolint:logcheck // 此情况应很少见，不值得转换。
		klog.Warningf("unknown transport type: %T", transport)
	}
}

// TLSClientConfigHolder 允许自定义 RoundTripper 暴露其 TLS 配置。
type TLSClientConfigHolder interface {
	TLSClientConfig() *tls.Config
}

// TLSClientConfig 从任意 RoundTripper 中提取 TLS 配置，必要时穿透 wrapper。
func TLSClientConfig(transport http.RoundTripper) (*tls.Config, error) {
	if transport == nil {
		return nil, nil
	}

	switch transport := transport.(type) {
	case *http.Transport:
		return transport.TLSClientConfig, nil
	case TLSClientConfigHolder:
		return transport.TLSClientConfig(), nil
	case RoundTripperWrapper:
		return TLSClientConfig(transport.WrappedRoundTripper())
	default:
		return nil, fmt.Errorf("unknown transport type: %T", transport)
	}
}

// FormatURL 用 net.JoinHostPort 构造 URL，正确处理 IPv6 地址的方括号。
func FormatURL(scheme string, host string, port int, path string) *url.URL {
	return &url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
		Path:   path,
	}
}

// GetHTTPClient 返回请求的 User-Agent；缺失时返回 "unknown" 以便审计日志可读。
func GetHTTPClient(req *http.Request) string {
	if ua := req.UserAgent(); len(ua) != 0 {
		return ua
	}
	return "unknown"
}

// SourceIPs 拆分逗号分隔的 X-Forwarded-For 头，并与 X-Real-Ip 头和 req.RemoteAddr 合并，忽略无效 IP。
// 如果 X-Real-Ip 已存在于 X-Forwarded-For 链中，则不再重复添加。
// req.RemoteAddr 始终是返回列表中的最后一个 IP。
// 如果这些值都为空或无效，则返回 nil。
//
// 返回顺序即"从最远到最近"的代理链；调用方通常关心第一个（GetClientIP），
// 但审计日志需要完整链路。RemoteAddr 无法被客户端伪造，故始终追加在末尾。
func SourceIPs(req *http.Request) []net.IP {
	var srcIPs []net.IP

	hdr := req.Header
	//对于经过代理的请求，先检查 X-Forwarded-For 头。
	hdrForwardedFor := hdr.Get("X-Forwarded-For")
	if hdrForwardedFor != "" {
		//经过多个代理时，X-Forwarded-For 可能是逗号分隔的 IP 列表。
		//使用第一个有效值。
		parts := strings.Split(hdrForwardedFor, ",")
		for _, part := range parts {
			// ParseIPSloppy 兼容旧数据中带前导零等非严格格式的 IPv4。
			ip := netutils.ParseIPSloppy(strings.TrimSpace(part))
			if ip != nil {
				srcIPs = append(srcIPs, ip)
			}
		}
	}

	//尝试使用 X-Real-Ip 头。
	hdrRealIp := hdr.Get("X-Real-Ip")
	if hdrRealIp != "" {
		ip := netutils.ParseIPSloppy(hdrRealIp)
		//仅当 X-Real-Ip 不在 X-Forwarded-For 链中时才追加。
		if ip != nil && !containsIP(srcIPs, ip) {
			srcIPs = append(srcIPs, ip)
		}
	}

	//始终包含请求的 Remote Address，因为它不易被伪造。
	var remoteIP net.IP
	//Go HTTP 服务器中的 Remote Address 格式为 host:port，因此需要先拆分。
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err == nil {
		remoteIP = netutils.ParseIPSloppy(host)
	}
	//如果 Remote Address 只是 IP，则直接使用。
	if remoteIP == nil {
		remoteIP = netutils.ParseIPSloppy(req.RemoteAddr)
	}

	//如果远端 IP 已是链中最后一个地址，则不重复添加。
	if remoteIP != nil && (len(srcIPs) == 0 || !remoteIP.Equal(srcIPs[len(srcIPs)-1])) {
		srcIPs = append(srcIPs, remoteIP)
	}

	return srcIPs
}

// 检查给定 IP 是否包含在 IP 列表中。
//
// 使用 net.IP.Equal 而非字符串比较，正确处理 IPv4/IPv6 的不同表示形式。
func containsIP(ips []net.IP, ip net.IP) bool {
	for _, v := range ips {
		if v.Equal(ip) {
			return true
		}
	}
	return false
}

// 从给定请求中提取并返回客户端 IP。
// 依次检查 X-Forwarded-For、X-Real-Ip 和 request.RemoteAddr。
// 如果都未设置或值无效，则返回 nil。
func GetClientIP(req *http.Request) net.IP {
	ips := SourceIPs(req)
	if len(ips) == 0 {
		return nil
	}
	return ips[0]
}

// 为下一跳准备 X-Forwarded-For 头，将前一个发送方的
// IP 追加到 X-Forwarded-For 链。
//
// 仅在 RemoteAddr 可解析时修改请求；已有多个 X-Forwarded-For 头会被折叠为一个。
func AppendForwardedForHeader(req *http.Request) {
	//复制自 net/http/httputil/reverseproxy.go：
	if clientIP, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
		//如果不是第一个代理，则保留已有的
		//X-Forwarded-For 信息，以逗号加空格分隔，
		//并将多个请求头合并为一个。
		if prior, ok := req.Header["X-Forwarded-For"]; ok {
			clientIP = strings.Join(prior, ", ") + ", " + clientIP
		}
		req.Header.Set("X-Forwarded-For", clientIP)
	}
}

// defaultProxyFuncPointer 记录标准库 ProxyFromEnvironment 的函数地址，
// 用于判断调用方是否只是沿用了默认值（函数无法直接比较）。
var defaultProxyFuncPointer = fmt.Sprintf("%p", http.ProxyFromEnvironment)

// isDefault 检查 transportProxierFunc 是否指向默认实现。
func isDefault(transportProxier func(*http.Request) (*url.URL, error)) bool {
	transportProxierPointer := fmt.Sprintf("%p", transportProxier)
	return transportProxierPointer == defaultProxyFuncPointer
}

// NewProxierWithNoProxyCIDR 创建尊重 NO_PROXY 中 CIDR 的 Proxier；未匹配 CIDR 时委托默认实现。
//
// 标准库只支持域名后缀匹配；这里补齐 CIDR，使集群内 Pod/Service 网段可被排除。
// 若 NO_PROXY 中没有 CIDR，则直接返回 delegate，零开销。
func NewProxierWithNoProxyCIDR(delegate func(req *http.Request) (*url.URL, error)) func(req *http.Request) (*url.URL, error) {
	//已包装默认方法，因此仅当 NO_PROXY（或 no_proxy）环境变量包含 CIDR 时才需要检查。
	noProxyEnv := os.Getenv("NO_PROXY")
	if noProxyEnv == "" {
		noProxyEnv = os.Getenv("no_proxy")
	}
	noProxyRules := strings.Split(noProxyEnv, ",")

	cidrs := []*net.IPNet{}
	for _, noProxyRule := range noProxyRules {
		// 非 CIDR 的规则解析失败会返回 nil，由 delegate 继续处理。
		_, cidr, _ := netutils.ParseCIDRSloppy(noProxyRule)
		if cidr != nil {
			cidrs = append(cidrs, cidr)
		}
	}

	if len(cidrs) == 0 {
		return delegate
	}

	return func(req *http.Request) (*url.URL, error) {
		ip := netutils.ParseIPSloppy(req.URL.Hostname())
		if ip == nil {
			return delegate(req)
		}

		for _, cidr := range cidrs {
			if cidr.Contains(ip) {
				// 命中 NO_PROXY CIDR：返回 nil 表示直连。
				return nil, nil
			}
		}

		return delegate(req)
	}
}

// DialerFunc 为给定函数实现 Dialer。
type DialerFunc func(req *http.Request) (net.Conn, error)

// Dial 调用 fn 执行拨号并返回结果。
func (fn DialerFunc) Dial(req *http.Request) (net.Conn, error) {
	return fn(req)
}

// Dialer 连接主机并写入请求。
type Dialer interface {
	//Dial 连接 req URL 指定的主机，将请求写入连接，
	//并返回建立的 net.Conn。
	Dial(req *http.Request) (net.Conn, error)
}

// CloneRequest 创建请求的浅拷贝，并深拷贝 Headers。
//
// 浅拷贝意味着 Body、URL 等指针字段仍与源请求共享；Headers 单独深拷贝，
// 以便代理修改头而不影响原请求。
func CloneRequest(req *http.Request) *http.Request {
	r := new(http.Request)

	//浅拷贝
	*r = *req

	//深拷贝请求头
	r.Header = CloneHeader(req.Header)

	return r
}

// CloneHeader 创建 http.Header 的深拷贝。
//
// 逐值复制切片，避免 append 时共享底层数组导致源 header 被意外修改。
func CloneHeader(in http.Header) http.Header {
	out := make(http.Header, len(in))
	for key, values := range in {
		newValues := make([]string, len(values))
		copy(newValues, values)
		out[key] = newValues
	}
	return out
}

// WarningHeader 包含单个 RFC 2616 14.46 Warning 头。
type WarningHeader struct {
	//Code 表示警告类型；299 表示杂项持久警告。
	Code int
	//Agent 包含添加 Warning 头的服务器名称或别名。
	//代理未知时建议使用单个 "-"。
	Agent string
	//警告文本
	Text string
}

// ParseWarningHeaders 从给定的一组头值中提取 RFC 2616 14.46 Warning 头。
// 每个头支持多个以逗号分隔的警告。
// 如果某个头解析出错，则跳过该头的剩余内容，继续解析后续头。
// 返回成功解析的警告以及遇到的错误。
//
// 解析策略是"尽力而为"：单个 header 出错不影响后续 header，
// 这样客户端不会因为一个损坏的 warning 丢掉全部诊断信息。
func ParseWarningHeaders(headers []string) ([]WarningHeader, []error) {
	var (
		results []WarningHeader
		errs    []error
	)
	for _, header := range headers {
		for len(header) > 0 {
			result, remainder, err := ParseWarningHeader(header)
			if err != nil {
				errs = append(errs, err)
				break
			}
			results = append(results, result)
			header = remainder
		}
	}
	return results, errs
}

var (
	// 警告码必须是三位数字；范围校验（100-299）交由上层语义判断。
	codeMatcher = regexp.MustCompile(`^[0-9]{3}$`)
	// wordDecoder 用于兼容 RFC2047 编码的警告文本（RFC2616 时代产物）。
	wordDecoder = &mime.WordDecoder{}
)

// ParseWarningHeader 从指定头中提取一个 RFC 2616 14.46 警告，
// 格式不正确时返回错误。
// 头中剩余的内容也会返回。
func ParseWarningHeader(header string) (result WarningHeader, remainder string, err error) {
	// https://tools.ietf.org/html/rfc2616#section-14.46
	// 更新者
	// https://tools.ietf.org/html/rfc7234#section-5.5
	//   https://tools.ietf.org/html/rfc7234#appendix-A
	//   Warning 头字段的生产和处理要求已有所放宽，
	//   因为该字段未被广泛实现。
	//   此外，Warning 头不再使用 RFC 2047 编码，
	//   也不允许多语言，因为这些方面未被实现。
	//
	//格式为以下之一：
	// warn-code warn-agent "warn-text"
	// warn-code warn-agent "warn-text" "warn-date"
	//
	//warn-code 是三位数字
	//warn-agent 不加引号且不含空格
	//warn-text 加引号并使用反斜杠转义（按 RFC 2616 使用 RFC 2047 编码，不按 RFC 7234 编码）
	//warn-date 可选、加引号，使用 HTTP-date 格式（不能包含嵌入引号或转义引号）
	//
	//同一头中可用逗号分隔并附加其他警告：
	// warn-code warn-agent "warn-text" "warn-date"[, warn-code warn-agent "warn-text" "warn-date", ...]

	//容忍前导空白
	header = strings.TrimSpace(header)

	// 只切出三段：code、agent 和剩余内容，剩余内容里再解析带引号的 text/date。
	parts := strings.SplitN(header, " ", 3)
	if len(parts) != 3 {
		return WarningHeader{}, "", errors.New("invalid warning header: fewer than 3 segments")
	}
	code, agent, textDateRemainder := parts[0], parts[1], parts[2]

	//校验 code 格式
	if !codeMatcher.Match([]byte(code)) {
		return WarningHeader{}, "", errors.New("invalid warning header: code segment is not 3 digits between 100-299")
	}
	codeInt, _ := strconv.ParseInt(code, 10, 64)

	//校验 agent 是否存在
	if len(agent) == 0 {
		return WarningHeader{}, "", errors.New("invalid warning header: empty agent segment")
	}
	if !utf8.ValidString(agent) || hasAnyRunes(agent, unicode.IsControl) {
		return WarningHeader{}, "", errors.New("invalid warning header: invalid agent")
	}

	//校验 textDateRemainder 是否存在
	if len(textDateRemainder) == 0 {
		return WarningHeader{}, "", errors.New("invalid warning header: empty text segment")
	}

	//提取文本
	text, dateAndRemainder, err := parseQuotedString(textDateRemainder)
	if err != nil {
		return WarningHeader{}, "", fmt.Errorf("invalid warning header: %v", err)
	}
	//容忍按 RFC 2616 生成、使用 RFC 2047 编码的警告文本
	if decodedText, err := wordDecoder.DecodeHeader(text); err == nil {
		text = decodedText
	}
	if !utf8.ValidString(text) || hasAnyRunes(text, unicode.IsControl) {
		return WarningHeader{}, "", errors.New("invalid warning header: invalid text")
	}
	result = WarningHeader{Code: int(codeInt), Agent: agent, Text: text}

	// 可选的 warn-date：只做结构解析，不校验日期格式（宽松兼容）。
	if len(dateAndRemainder) > 0 {
		if dateAndRemainder[0] == '"' {
			//消费日期
			foundEndQuote := false
			for i := 1; i < len(dateAndRemainder); i++ {
				if dateAndRemainder[i] == '"' {
					foundEndQuote = true
					remainder = strings.TrimSpace(dateAndRemainder[i+1:])
					break
				}
			}
			if !foundEndQuote {
				return WarningHeader{}, "", errors.New("invalid warning header: unterminated date segment")
			}
		} else {
			remainder = dateAndRemainder
		}
	}
	// 多条 warning 以逗号分隔；其他残留 token 视为格式错误。
	if len(remainder) > 0 {
		if remainder[0] == ',' {
			//如果存在逗号则消费
			remainder = strings.TrimSpace(remainder[1:])
		} else {
			return WarningHeader{}, "", errors.New("invalid warning header: unexpected token after warn-date")
		}
	}

	return result, remainder, nil
}

// parseQuotedString 解析以 '"' 开头、支持反斜杠转义的字符串，
// 返回解析结果和闭合引号之后剩余的内容。
func parseQuotedString(quotedString string) (string, string, error) {
	if len(quotedString) == 0 {
		return "", "", errors.New("invalid quoted string: 0-length")
	}

	if quotedString[0] != '"' {
		return "", "", errors.New("invalid quoted string: missing initial quote")
	}

	quotedString = quotedString[1:]
	var remainder string
	escaping := false
	closedQuote := false
	result := &strings.Builder{}
loop:
	for i := 0; i < len(quotedString); i++ {
		b := quotedString[i]
		switch b {
		case '"':
			if escaping {
				result.WriteByte(b)
				escaping = false
			} else {
				closedQuote = true
				remainder = strings.TrimSpace(quotedString[i+1:])
				break loop
			}
		case '\\':
			if escaping {
				// "\\" 表示一个字面反斜杠。
				result.WriteByte(b)
				escaping = false
			} else {
				escaping = true
			}
		default:
			result.WriteByte(b)
			// 非转义字符会终止转义状态，例如 "\x" 中的 x 按字面处理。
			escaping = false
		}
	}

	if !closedQuote {
		return "", "", errors.New("invalid quoted string: missing closing quote")
	}
	return result.String(), remainder, nil
}

// NewWarningHeader 按 RFC2616 格式构造 Warning 头。
// code 限制在 0-999；空 agent 用 "-" 表示未知，其他 agent 不得包含
// 空格、引号、反斜杠或控制字符，否则会破坏头部语法。
func NewWarningHeader(code int, agent, text string) (string, error) {
	if code < 0 || code > 999 {
		return "", errors.New("code must be between 0 and 999")
	}
	if len(agent) == 0 {
		agent = "-"
	} else if !utf8.ValidString(agent) || strings.ContainsAny(agent, `\"`) || hasAnyRunes(agent, unicode.IsSpace, unicode.IsControl) {
		return "", errors.New("agent must be valid UTF-8 and must not contain spaces, quotes, backslashes, or control characters")
	}
	if !utf8.ValidString(text) || hasAnyRunes(text, unicode.IsControl) {
		return "", errors.New("text must be valid UTF-8 and must not contain control characters")
	}
	return fmt.Sprintf("%03d %s %s", code, agent, makeQuotedString(text)), nil
}

// hasAnyRunes 判断字符串中是否存在满足任一 checker 的 rune；
// 用于统一实现"禁止控制字符/空白"等校验。
func hasAnyRunes(s string, runeCheckers ...func(rune) bool) bool {
	for _, r := range s {
		for _, checker := range runeCheckers {
			if checker(r) {
				return true
			}
		}
	}
	return false
}

// makeQuotedString 把文本包成带引号的字符串，仅转义 '"' 和 '\\'，
// 与 parseQuotedString 的解析规则严格对称。
func makeQuotedString(s string) string {
	result := &bytes.Buffer{}
	//起始引号
	result.WriteRune('"')
	for _, c := range s {
		switch c {
		case '"', '\\':
			//转义 " 和 \
			result.WriteRune('\\')
			result.WriteRune(c)
		default:
			//其他内容原样写入
			result.WriteRune(c)
		}
	}
	//结束引号
	result.WriteRune('"')
	return result.String()
}
