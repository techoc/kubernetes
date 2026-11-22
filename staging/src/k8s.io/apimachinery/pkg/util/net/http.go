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

// JoinPreservingTrailingSlash does a path.Join of the specified elements,
// preserving any trailing slash on the last non-empty segment
//
// path.Join 会吞掉末尾斜杠，而 URL 语义中 "/foo/" 与 "/foo" 可能不同，
// 因此这里在拼接后按需补回。
func JoinPreservingTrailingSlash(elem ...string) string {
	// do the basic path join
	result := path.Join(elem...)

	// find the last non-empty segment
	for i := len(elem) - 1; i >= 0; i-- {
		if len(elem[i]) > 0 {
			// if the last segment ended in a slash, ensure our result does as well
			if strings.HasSuffix(elem[i], "/") && !strings.HasSuffix(result, "/") {
				result += "/"
			}
			break
		}
	}

	return result
}

// IsTimeout returns true if the given error is a network timeout error
//
// 用 errors.As 而非类型断言，保证被包装的 net.Error 也能识别。
func IsTimeout(err error) bool {
	var neterr net.Error
	if errors.As(err, &neterr) {
		return neterr != nil && neterr.Timeout()
	}
	return false
}

// IsProbableEOF returns true if the given error resembles a connection termination
// scenario that would justify assuming that the watch is empty.
// These errors are what the Go http stack returns back to us which are general
// connection closure errors (strongly correlated) and callers that need to
// differentiate probable errors in connection behavior between normal "this is
// disconnected" should use the method.
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

// SetOldTransportDefaults applies the defaults from http.DefaultTransport
// for the Proxy, Dial, and TLSHandshakeTimeout fields if unset
//
// "Old" 指不涉及 HTTP/2 的旧默认值；SetTransportDefaults 在此基础上再配置 h2。
func SetOldTransportDefaults(t *http.Transport) *http.Transport {
	if t.Proxy == nil || isDefault(t.Proxy) {
		// http.ProxyFromEnvironment doesn't respect CIDRs and that makes it impossible to exclude things like pod and service IPs from proxy settings
		// ProxierWithNoProxyCIDR allows CIDR rules in NO_PROXY
		t.Proxy = NewProxierWithNoProxyCIDR(http.ProxyFromEnvironment)
	}
	// If no custom dialer is set, use the default context dialer
	//lint:file-ignore SA1019 Keep supporting deprecated Dial method of custom transports
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

// SetTransportDefaults applies the defaults from http.DefaultTransport
// for the Proxy, Dial, and TLSHandshakeTimeout fields if unset
//
// 在旧默认值之上按需启用 HTTP/2；环境变量 DISABLE_HTTP2 提供全局逃生开关，
// 用于排查 h2 相关问题。配置失败只告警不失败，保证客户端仍可用 HTTP/1.1。
func SetTransportDefaults(t *http.Transport) *http.Transport {
	t = SetOldTransportDefaults(t)
	// Allow clients to disable http2 if needed.
	if s := os.Getenv("DISABLE_HTTP2"); len(s) > 0 {
		//nolint:logcheck // Should be rare, not worth converting.
		klog.Info("HTTP2 has been explicitly disabled")
	} else if allowsHTTP2(t) {
		if err := configureHTTP2Transport(t); err != nil {
			//nolint:logcheck // Should be rare, not worth converting.
			klog.Warningf("Transport failed http2 configuration: %v", err)
		}
	}
	return t
}

// readIdleTimeoutSeconds 读取 HTTP/2 连接健康检查的空闲超时，默认 30 秒；
// 设为 0 可关闭检查。非法值回退默认值并告警。
func readIdleTimeoutSeconds() int {
	ret := 30
	// User can set the readIdleTimeout to 0 to disable the HTTP/2
	// connection health check.
	if s := os.Getenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS"); len(s) > 0 {
		i, err := strconv.Atoi(s)
		if err != nil {
			//nolint:logcheck // Should be rare, not worth converting.
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
			//nolint:logcheck // Should be rare, not worth converting.
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
	// The following enables the HTTP/2 connection health check added in
	// https://github.com/golang/net/pull/55. The health check detects and
	// closes broken transport layer connections. Without the health check,
	// a broken connection can linger too long, e.g., a broken TCP
	// connection will be closed by the Linux kernel after 13 to 30 minutes
	// by default, which caused
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
		// the transport expressed no NextProto preference, allow
		return true
	}
	for _, p := range t.TLSClientConfig.NextProtos {
		if p == http2.NextProtoTLS {
			// the transport explicitly allowed http/2
			return true
		}
	}
	// the transport explicitly set NextProtos and excluded http/2
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
		// transport.DialContext takes precedence over transport.Dial
		if transport.DialContext != nil {
			return transport.DialContext, nil
		}
		// adapt transport.Dial to the DialWithContext signature
		if transport.Dial != nil {
			return func(ctx context.Context, net, addr string) (net.Conn, error) {
				return transport.Dial(net, addr)
			}, nil
		}
		// otherwise return nil
		return nil, nil
	case RoundTripperWrapper:
		return DialerFor(transport.WrappedRoundTripper())
	default:
		return nil, fmt.Errorf("unknown transport type: %T", transport)
	}
}

// CloseIdleConnectionsFor close idles connections for the Transport.
// If the Transport is wrapped it iterates over the wrapped round trippers
// until it finds one that implements the CloseIdleConnections method.
// If the Transport does not have a CloseIdleConnections method
// then this function does nothing.
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
		//nolint:logcheck // Should be rare, not worth converting.
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

// SourceIPs splits the comma separated X-Forwarded-For header and joins it with
// the X-Real-Ip header and/or req.RemoteAddr, ignoring invalid IPs.
// The X-Real-Ip is omitted if it's already present in the X-Forwarded-For chain.
// The req.RemoteAddr is always the last IP in the returned list.
// It returns nil if all of these are empty or invalid.
//
// 返回顺序即"从最远到最近"的代理链；调用方通常关心第一个（GetClientIP），
// 但审计日志需要完整链路。RemoteAddr 无法被客户端伪造，故始终追加在末尾。
func SourceIPs(req *http.Request) []net.IP {
	var srcIPs []net.IP

	hdr := req.Header
	// First check the X-Forwarded-For header for requests via proxy.
	hdrForwardedFor := hdr.Get("X-Forwarded-For")
	if hdrForwardedFor != "" {
		// X-Forwarded-For can be a csv of IPs in case of multiple proxies.
		// Use the first valid one.
		parts := strings.Split(hdrForwardedFor, ",")
		for _, part := range parts {
			// ParseIPSloppy 兼容旧数据中带前导零等非严格格式的 IPv4。
			ip := netutils.ParseIPSloppy(strings.TrimSpace(part))
			if ip != nil {
				srcIPs = append(srcIPs, ip)
			}
		}
	}

	// Try the X-Real-Ip header.
	hdrRealIp := hdr.Get("X-Real-Ip")
	if hdrRealIp != "" {
		ip := netutils.ParseIPSloppy(hdrRealIp)
		// Only append the X-Real-Ip if it's not already contained in the X-Forwarded-For chain.
		if ip != nil && !containsIP(srcIPs, ip) {
			srcIPs = append(srcIPs, ip)
		}
	}

	// Always include the request Remote Address as it cannot be easily spoofed.
	var remoteIP net.IP
	// Remote Address in Go's HTTP server is in the form host:port so we need to split that first.
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err == nil {
		remoteIP = netutils.ParseIPSloppy(host)
	}
	// Fallback if Remote Address was just IP.
	if remoteIP == nil {
		remoteIP = netutils.ParseIPSloppy(req.RemoteAddr)
	}

	// Don't duplicate remote IP if it's already the last address in the chain.
	if remoteIP != nil && (len(srcIPs) == 0 || !remoteIP.Equal(srcIPs[len(srcIPs)-1])) {
		srcIPs = append(srcIPs, remoteIP)
	}

	return srcIPs
}

// Checks whether the given IP address is contained in the list of IPs.
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

// Extracts and returns the clients IP from the given request.
// Looks at X-Forwarded-For header, X-Real-Ip header and request.RemoteAddr in that order.
// Returns nil if none of them are set or is set to an invalid value.
func GetClientIP(req *http.Request) net.IP {
	ips := SourceIPs(req)
	if len(ips) == 0 {
		return nil
	}
	return ips[0]
}

// Prepares the X-Forwarded-For header for another forwarding hop by appending the previous sender's
// IP address to the X-Forwarded-For chain.
//
// 仅在 RemoteAddr 可解析时修改请求；已有多个 X-Forwarded-For 头会被折叠为一个。
func AppendForwardedForHeader(req *http.Request) {
	// Copied from net/http/httputil/reverseproxy.go:
	if clientIP, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
		// If we aren't the first proxy retain prior
		// X-Forwarded-For information as a comma+space
		// separated list and fold multiple headers into one.
		if prior, ok := req.Header["X-Forwarded-For"]; ok {
			clientIP = strings.Join(prior, ", ") + ", " + clientIP
		}
		req.Header.Set("X-Forwarded-For", clientIP)
	}
}

// defaultProxyFuncPointer 记录标准库 ProxyFromEnvironment 的函数地址，
// 用于判断调用方是否只是沿用了默认值（函数无法直接比较）。
var defaultProxyFuncPointer = fmt.Sprintf("%p", http.ProxyFromEnvironment)

// isDefault checks to see if the transportProxierFunc is pointing to the default one
func isDefault(transportProxier func(*http.Request) (*url.URL, error)) bool {
	transportProxierPointer := fmt.Sprintf("%p", transportProxier)
	return transportProxierPointer == defaultProxyFuncPointer
}

// NewProxierWithNoProxyCIDR constructs a Proxier function that respects CIDRs in NO_PROXY and delegates if
// no matching CIDRs are found
//
// 标准库只支持域名后缀匹配；这里补齐 CIDR，使集群内 Pod/Service 网段可被排除。
// 若 NO_PROXY 中没有 CIDR，则直接返回 delegate，零开销。
func NewProxierWithNoProxyCIDR(delegate func(req *http.Request) (*url.URL, error)) func(req *http.Request) (*url.URL, error) {
	// we wrap the default method, so we only need to perform our check if the NO_PROXY (or no_proxy) envvar has a CIDR in it
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

// DialerFunc implements Dialer for the provided function.
type DialerFunc func(req *http.Request) (net.Conn, error)

func (fn DialerFunc) Dial(req *http.Request) (net.Conn, error) {
	return fn(req)
}

// Dialer dials a host and writes a request to it.
type Dialer interface {
	// Dial connects to the host specified by req's URL, writes the request to the connection, and
	// returns the opened net.Conn.
	Dial(req *http.Request) (net.Conn, error)
}

// CloneRequest creates a shallow copy of the request along with a deep copy of the Headers.
//
// 浅拷贝意味着 Body、URL 等指针字段仍与源请求共享；Headers 单独深拷贝，
// 以便代理修改头而不影响原请求。
func CloneRequest(req *http.Request) *http.Request {
	r := new(http.Request)

	// shallow clone
	*r = *req

	// deep copy headers
	r.Header = CloneHeader(req.Header)

	return r
}

// CloneHeader creates a deep copy of an http.Header.
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

// WarningHeader contains a single RFC2616 14.46 warnings header
type WarningHeader struct {
	// Codeindicates the type of warning. 299 is a miscellaneous persistent warning
	Code int
	// Agent contains the name or pseudonym of the server adding the Warning header.
	// A single "-" is recommended when agent is unknown.
	Agent string
	// Warning text
	Text string
}

// ParseWarningHeaders extract RFC2616 14.46 warnings headers from the specified set of header values.
// Multiple comma-separated warnings per header are supported.
// If errors are encountered on a header, the remainder of that header are skipped and subsequent headers are parsed.
// Returns successfully parsed warnings and any errors encountered.
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

// ParseWarningHeader extracts one RFC2616 14.46 warning from the specified header,
// returning an error if the header does not contain a correctly formatted warning.
// Any remaining content in the header is returned.
func ParseWarningHeader(header string) (result WarningHeader, remainder string, err error) {
	// https://tools.ietf.org/html/rfc2616#section-14.46
	//   updated by
	// https://tools.ietf.org/html/rfc7234#section-5.5
	//   https://tools.ietf.org/html/rfc7234#appendix-A
	//     Some requirements regarding production and processing of the Warning
	//     header fields have been relaxed, as it is not widely implemented.
	//     Furthermore, the Warning header field no longer uses RFC 2047
	//     encoding, nor does it allow multiple languages, as these aspects were
	//     not implemented.
	//
	// Format is one of:
	// warn-code warn-agent "warn-text"
	// warn-code warn-agent "warn-text" "warn-date"
	//
	// warn-code is a three digit number
	// warn-agent is unquoted and contains no spaces
	// warn-text is quoted with backslash escaping (RFC2047-encoded according to RFC2616, not encoded according to RFC7234)
	// warn-date is optional, quoted, and in HTTP-date format (no embedded or escaped quotes)
	//
	// additional warnings can optionally be included in the same header by comma-separating them:
	// warn-code warn-agent "warn-text" "warn-date"[, warn-code warn-agent "warn-text" "warn-date", ...]

	// tolerate leading whitespace
	header = strings.TrimSpace(header)

	// 只切出三段：code、agent 和剩余内容，剩余内容里再解析带引号的 text/date。
	parts := strings.SplitN(header, " ", 3)
	if len(parts) != 3 {
		return WarningHeader{}, "", errors.New("invalid warning header: fewer than 3 segments")
	}
	code, agent, textDateRemainder := parts[0], parts[1], parts[2]

	// verify code format
	if !codeMatcher.Match([]byte(code)) {
		return WarningHeader{}, "", errors.New("invalid warning header: code segment is not 3 digits between 100-299")
	}
	codeInt, _ := strconv.ParseInt(code, 10, 64)

	// verify agent presence
	if len(agent) == 0 {
		return WarningHeader{}, "", errors.New("invalid warning header: empty agent segment")
	}
	if !utf8.ValidString(agent) || hasAnyRunes(agent, unicode.IsControl) {
		return WarningHeader{}, "", errors.New("invalid warning header: invalid agent")
	}

	// verify textDateRemainder presence
	if len(textDateRemainder) == 0 {
		return WarningHeader{}, "", errors.New("invalid warning header: empty text segment")
	}

	// extract text
	text, dateAndRemainder, err := parseQuotedString(textDateRemainder)
	if err != nil {
		return WarningHeader{}, "", fmt.Errorf("invalid warning header: %v", err)
	}
	// tolerate RFC2047-encoded text from warnings produced according to RFC2616
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
			// consume date
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
			// consume comma if present
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
	// opening quote
	result.WriteRune('"')
	for _, c := range s {
		switch c {
		case '"', '\\':
			// escape " and \
			result.WriteRune('\\')
			result.WriteRune(c)
		default:
			// write everything else as-is
			result.WriteRune(c)
		}
	}
	// closing quote
	result.WriteRune('"')
	return result.String()
}
