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

// Package transport 提供 client-go 的 HTTP 传输层：认证、伪装、调试日志与
// 连接缓存等。
//
// 各功能通过 HTTP RoundTripper 逐层包装实现，顺序会影响最终请求头与日志；
// 包装后的 transport 可被缓存复用，因此实现必须并发安全。

package transport

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/oauth2"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/klog/v2"
)

// HTTPWrappersForConfig 给 RoundTripper 套上配置要求的各层行为。暴露出来供需要 HTTP 语义但
// 要劫持底层连接（如 WebSocket/HTTP2）的客户端使用；纯 HTTP 客户端应使用 New 返回的 RoundTripper。
//
// 包装顺序从内到外依次是 Debug、认证、User-Agent、伪装；越靠外的层越晚
// 写入请求头，后写者覆盖先写者。
func HTTPWrappersForConfig(config *Config, rt http.RoundTripper) (http.RoundTripper, error) {
	if config.WrapTransport != nil {
		rt = config.WrapTransport(rt)
	}

	rt = DebugWrappers(rt)

	// 安装认证相关的包装器。
	switch {
	case config.HasBasicAuth() && config.HasTokenAuth():
		return nil, fmt.Errorf("username/password or bearer token may be set, but not both")
	case config.HasTokenAuth():
		var err error
		rt, err = NewBearerAuthWithRefreshRoundTripper(config.BearerToken, config.BearerTokenFile, rt)
		if err != nil {
			return nil, err
		}
	case config.HasBasicAuth():
		rt = NewBasicAuthRoundTripper(config.Username, config.Password, rt)
	}
	if len(config.UserAgent) > 0 {
		rt = NewUserAgentRoundTripper(config.UserAgent, rt)
	}
	if len(config.Impersonate.UserName) > 0 ||
		len(config.Impersonate.UID) > 0 ||
		len(config.Impersonate.Groups) > 0 ||
		len(config.Impersonate.Extra) > 0 {
		rt = NewImpersonatingRoundTripper(config.Impersonate, rt)
	}
	return rt, nil
}

// DebugWrappers 可能给 RoundTripper 包一层，按每个请求上下文中的日志级别输出日志。
// 目前是否包装取决于全局日志详细度（>= 6 时包装），将来可能改变。
func DebugWrappers(rt http.RoundTripper) http.RoundTripper {
	//nolint:logcheck // The actual logging is done with a different logger, so only checking here is okay.
	// 这里只看全局 verbosity 决定是否安装包装器；真正的日志级别在
	// RoundTrip 中按请求上下文重新判定。
	if klog.V(6).Enabled() {
		rt = NewDebuggingRoundTripper(rt, DebugByContext)
	}
	return rt
}

// authProxyRoundTripper 为“终止认证的代理”场景注入已认证用户信息。
type authProxyRoundTripper struct {
	username string
	uid      string
	groups   []string
	extra    map[string][]string

	rt http.RoundTripper
}

var _ utilnet.RoundTripperWrapper = &authProxyRoundTripper{}

// NewAuthProxyRoundTripper 返回一个 RoundTripper，为「认证终止代理」场景在请求上附加认证代理字段。
// 前提是从 context 中取出 user：username 对应 user.Info.GetName()，uid 对应 GetUID()，
// groups 对应 GetGroups()，extra 对应 GetExtra()。extra 可携带认证器认为有用的附加信息（如授权范围）。
// 为能在伪装流程中原样传递，这些键必须是小写。
func NewAuthProxyRoundTripper(username, uid string, groups []string, extra map[string][]string, rt http.RoundTripper) http.RoundTripper {
	return &authProxyRoundTripper{
		username: username,
		uid:      uid,
		groups:   groups,
		extra:    extra,
		rt:       rt,
	}
}

func (rt *authProxyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// 克隆请求后再改头，避免修改调用方共享的 *http.Request。
	req = utilnet.CloneRequest(req)
	SetAuthProxyHeaders(req, rt.username, rt.uid, rt.groups, rt.extra)

	return rt.rt.RoundTrip(req)
}

// SetAuthProxyHeaders 直接覆盖参数中的认证代理头字段。
func SetAuthProxyHeaders(req *http.Request, username, uid string, groups []string, extra map[string][]string) {
	// 先清空所有可能的旧值，防止调用方注入伪造身份；extra 头是动态前缀，
	// 必须遍历已有头而不是只删已知键。
	req.Header.Del("X-Remote-User")
	req.Header.Del("X-Remote-Uid")
	req.Header.Del("X-Remote-Group")
	for key := range req.Header {
		if strings.HasPrefix(strings.ToLower(key), strings.ToLower("X-Remote-Extra-")) {
			req.Header.Del(key)
		}
	}

	req.Header.Set("X-Remote-User", username)
	if len(uid) > 0 {
		req.Header.Set("X-Remote-Uid", uid)
	}
	for _, group := range groups {
		req.Header.Add("X-Remote-Group", group)
	}
	for key, values := range extra {
		for _, value := range values {
			req.Header.Add("X-Remote-Extra-"+headerKeyEscape(key), value)
		}
	}
}

func (rt *authProxyRoundTripper) CancelRequest(req *http.Request) {
	tryCancelRequest(rt.WrappedRoundTripper(), req)
}

func (rt *authProxyRoundTripper) WrappedRoundTripper() http.RoundTripper { return rt.rt }

// userAgentRoundTripper 在请求未设置 User-Agent 时补充配置值。
type userAgentRoundTripper struct {
	agent string
	rt    http.RoundTripper
}

var _ utilnet.RoundTripperWrapper = &userAgentRoundTripper{}

// NewUserAgentRoundTripper 在请求未设置 User-Agent 时补上该头。
func NewUserAgentRoundTripper(agent string, rt http.RoundTripper) http.RoundTripper {
	return &userAgentRoundTripper{agent, rt}
}

func (rt *userAgentRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if len(req.Header.Get("User-Agent")) != 0 {
		return rt.rt.RoundTrip(req)
	}
	req = utilnet.CloneRequest(req)
	req.Header.Set("User-Agent", rt.agent)
	return rt.rt.RoundTrip(req)
}

func (rt *userAgentRoundTripper) CancelRequest(req *http.Request) {
	tryCancelRequest(rt.WrappedRoundTripper(), req)
}

func (rt *userAgentRoundTripper) WrappedRoundTripper() http.RoundTripper { return rt.rt }

// basicAuthRoundTripper 注入 HTTP Basic 认证头。
type basicAuthRoundTripper struct {
	username string
	password string `datapolicy:"password"`
	rt       http.RoundTripper
}

var _ utilnet.RoundTripperWrapper = &basicAuthRoundTripper{}

// NewBasicAuthRoundTripper 在请求未设置 Authorization 时加上 Basic 认证头。
func NewBasicAuthRoundTripper(username, password string, rt http.RoundTripper) http.RoundTripper {
	return &basicAuthRoundTripper{username, password, rt}
}

func (rt *basicAuthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if len(req.Header.Get("Authorization")) != 0 {
		return rt.rt.RoundTrip(req)
	}
	req = utilnet.CloneRequest(req)
	req.SetBasicAuth(rt.username, rt.password)
	return rt.rt.RoundTrip(req)
}

func (rt *basicAuthRoundTripper) CancelRequest(req *http.Request) {
	tryCancelRequest(rt.WrappedRoundTripper(), req)
}

func (rt *basicAuthRoundTripper) WrappedRoundTripper() http.RoundTripper { return rt.rt }

// 这些头与 pkg/apis/authentication 中的定义对应。这里刻意不引入包依赖，但取值绝不能更改。
const (
	// ImpersonateUserHeader 用于在 API server 请求中伪装为指定用户。
	ImpersonateUserHeader = "Impersonate-User"

	// ImpersonateUIDHeader 用于在 API server 请求中伪装为指定 UID。
	ImpersonateUIDHeader = "Impersonate-Uid"

	// ImpersonateGroupHeader 用于在 API server 请求中伪装用户组，可重复出现以指定多个组。
	ImpersonateGroupHeader = "Impersonate-Group"

	// ImpersonateUserExtraHeaderPrefix 是用于伪装 user.Info.extra 中某项的前缀，后缀即 extra map 的键。
	// 同一个键可重复出现，使该键对应多个值，例如：
	// Impersonate-Extra-Foo: one
	// Impersonate-Extra-Foo: two
	// 结果为 extra["Foo"] = []string{"one", "two"}。
	ImpersonateUserExtraHeaderPrefix = "Impersonate-Extra-"
)

// impersonatingRoundTripper 通过 Impersonate-* 头让服务端以目标用户身份鉴权。
type impersonatingRoundTripper struct {
	impersonate ImpersonationConfig
	delegate    http.RoundTripper
}

var _ utilnet.RoundTripperWrapper = &impersonatingRoundTripper{}

// NewImpersonatingRoundTripper 在请求未设置伪装头时补上 Act-As 头。
func NewImpersonatingRoundTripper(impersonate ImpersonationConfig, delegate http.RoundTripper) http.RoundTripper {
	return &impersonatingRoundTripper{impersonate, delegate}
}

func (rt *impersonatingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// 以用户头作为是否已设置伪装信息的标志。
	//
	// 只要已有 Impersonate-User 就认为调用方自行设置了伪装，整体跳过，
	// 避免把两套身份信息拼在一起。
	if len(req.Header.Get(ImpersonateUserHeader)) != 0 {
		return rt.delegate.RoundTrip(req)
	}
	req = utilnet.CloneRequest(req)
	req.Header.Set(ImpersonateUserHeader, rt.impersonate.UserName)
	if rt.impersonate.UID != "" {
		req.Header.Set(ImpersonateUIDHeader, rt.impersonate.UID)
	}
	for _, group := range rt.impersonate.Groups {
		req.Header.Add(ImpersonateGroupHeader, group)
	}
	for k, vv := range rt.impersonate.Extra {
		for _, v := range vv {
			req.Header.Add(ImpersonateUserExtraHeaderPrefix+headerKeyEscape(k), v)
		}
	}

	return rt.delegate.RoundTrip(req)
}

func (rt *impersonatingRoundTripper) CancelRequest(req *http.Request) {
	tryCancelRequest(rt.WrappedRoundTripper(), req)
}

func (rt *impersonatingRoundTripper) WrappedRoundTripper() http.RoundTripper { return rt.delegate }

// bearerAuthRoundTripper 注入 Bearer token，并可选地从文件刷新。
type bearerAuthRoundTripper struct {
	bearer string
	source oauth2.TokenSource
	rt     http.RoundTripper
}

var _ utilnet.RoundTripperWrapper = &bearerAuthRoundTripper{}

// NewBearerAuthRoundTripper 在请求未设置 Authorization 时加上给定的 Bearer token。
func NewBearerAuthRoundTripper(bearer string, rt http.RoundTripper) http.RoundTripper {
	return &bearerAuthRoundTripper{bearer, nil, rt}
}

// NewBearerAuthWithRefreshRoundTripper 在请求未设置 Authorization 时加上给定的 Bearer token。
// tokenFile 非空时会定期读取，最后一次成功读取的内容作为 token；若 bearer 为空则立即读取一次以填充初值。
func NewBearerAuthWithRefreshRoundTripper(bearer string, tokenFile string, rt http.RoundTripper) (http.RoundTripper, error) {
	if len(tokenFile) == 0 {
		return &bearerAuthRoundTripper{bearer, nil, rt}, nil
	}
	source := NewCachedFileTokenSource(tokenFile)
	if len(bearer) == 0 {
		token, err := source.Token()
		if err != nil {
			return nil, err
		}
		bearer = token.AccessToken
	}
	return &bearerAuthRoundTripper{bearer, source, rt}, nil
}

func (rt *bearerAuthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if len(req.Header.Get("Authorization")) != 0 {
		return rt.rt.RoundTrip(req)
	}

	req = utilnet.CloneRequest(req)
	token := rt.bearer
	// 刷新失败时沿用上一次的 token，让服务端决定是否真的失效；
	// 这样短暂的读文件错误不会立即打断请求。
	if rt.source != nil {
		if refreshedToken, err := rt.source.Token(); err == nil {
			token = refreshedToken.AccessToken
		}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return rt.rt.RoundTrip(req)
}

func (rt *bearerAuthRoundTripper) CancelRequest(req *http.Request) {
	tryCancelRequest(rt.WrappedRoundTripper(), req)
}

func (rt *bearerAuthRoundTripper) WrappedRoundTripper() http.RoundTripper { return rt.rt }

// requestInfo 记录一次请求/响应组合的相关信息。
//
// 详细计时字段由 httptrace 回调并发写入，因此用 muTrace 保护；其余字段只在
// RoundTrip 协程内读写。
type requestInfo struct {
	RequestHeaders http.Header `datapolicy:"token"`
	RequestVerb    string
	RequestURL     string

	ResponseStatus  string
	ResponseHeaders http.Header
	ResponseErr     error

	muTrace          sync.Mutex // Protect trace fields
	DNSLookup        time.Duration
	Dialing          time.Duration
	GetConnection    time.Duration
	TLSHandshake     time.Duration
	ServerProcessing time.Duration
	ConnectionReused bool

	Duration time.Duration
}

// newRequestInfo 基于 http 请求创建 RequestInfo。
func newRequestInfo(req *http.Request) *requestInfo {
	return &requestInfo{
		RequestURL:     req.URL.String(),
		RequestVerb:    req.Method,
		RequestHeaders: req.Header,
	}
}

// complete 把响应信息补进 requestInfo。
func (r *requestInfo) complete(response *http.Response, err error) {
	if err != nil {
		r.ResponseErr = err
		return
	}
	r.ResponseStatus = response.Status
	r.ResponseHeaders = response.Header
}

// toCurl 返回可在终端执行的等价 curl 命令（不含 body）。
func (r *requestInfo) toCurl() string {
	headers := ""
	for key, values := range r.RequestHeaders {
		for _, value := range values {
			value = maskValue(key, value)
			headers += fmt.Sprintf(` -H %q`, fmt.Sprintf("%s: %s", key, value))
		}
	}

	// 末尾换行让文本日志输出更美观（这是该方法的唯一用途）：变成无需引号的多行字符串。
	return fmt.Sprintf("curl -v -X%s %s '%s'\n", r.RequestVerb, headers, r.RequestURL)
}

// debuggingRoundTripper 按配置展示经过它的请求信息。
//
// debuggingRoundTripper 按 levels 位掩码输出请求/响应诊断信息。
type debuggingRoundTripper struct {
	delegatedRoundTripper http.RoundTripper
	// levels 是 DebugLevel 的位集合，决定记录哪些字段。
	levels int
}

var _ utilnet.RoundTripperWrapper = &debuggingRoundTripper{}

// DebugLevel 用于让 debuggingRoundTripper 输出指定的 HTTP 请求/响应字段。
type DebugLevel int

const (
	// DebugJustURL 在调试输出中加入请求方法与 URL。
	DebugJustURL DebugLevel = iota
	// DebugURLTiming 在调试输出中加入 HTTP 请求耗时。
	DebugURLTiming
	// DebugCurlCommand 在调试输出中加入等价的 curl 命令。
	DebugCurlCommand
	// DebugRequestHeaders 在调试输出中加入请求头。
	DebugRequestHeaders
	// DebugResponseStatus 在调试输出中加入响应状态码。
	DebugResponseStatus
	// DebugResponseHeaders 在调试输出中加入响应头。
	DebugResponseHeaders
	// DebugDetailedTiming 在调试输出中加入 HTTP 请求各阶段的耗时。
	DebugDetailedTiming
	// DebugByContext 根据请求上下文中 logger 的详细度决定输出上述哪些信息。
	// 可与 NewDebuggingRoundTripper 的其他选项组合：此时既输出选项指定的内容，也输出上下文详细度启用的内容。
	DebugByContext
)

// 不同日志级别包含不同信息集合。该类型未导出，因为日志的确切内容不属于包 API。
const (
	levelsV6 = (1 << DebugURLTiming)
	// 级别 7 比 6 记录更少的响应信息，这是为了复现旧行为：
	//  https://github.com/kubernetes/kubernetes/blob/2b472fe4690c83a2b343995f88050b2a3e9ff0fa/staging/src/k8s.io/client-go/transport/round_trippers.go#L79
	// 大概是因为方法与 URL 已出现在请求日志中，无需重复。
	levelsV7 = (1 << DebugJustURL) | (1 << DebugRequestHeaders) | (1 << DebugResponseStatus)
	levelsV8 = (1 << DebugJustURL) | (1 << DebugRequestHeaders) | (1 << DebugResponseStatus) | (1 << DebugResponseHeaders)
	levelsV9 = (1 << DebugCurlCommand) | (1 << DebugURLTiming) | (1 << DebugDetailedTiming) | (1 << DebugResponseHeaders)
)

// NewDebuggingRoundTripper 让日志输出客户端所发 API 请求的调试信息。
func NewDebuggingRoundTripper(rt http.RoundTripper, levels ...DebugLevel) http.RoundTripper {
	drt := &debuggingRoundTripper{
		delegatedRoundTripper: rt,
	}
	for _, v := range levels {
		drt.levels |= 1 << v
	}
	return drt
}

func (rt *debuggingRoundTripper) CancelRequest(req *http.Request) {
	tryCancelRequest(rt.WrappedRoundTripper(), req)
}

var knownAuthTypes = map[string]bool{
	"bearer":    true,
	"basic":     true,
	"negotiate": true,
}

// maskValue 对 Authorization 头中的凭证脱敏，详见 https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Authorization。
//
// 保留认证类型前缀（如 Bearer）便于排查，但隐藏凭证本身；未知类型整体
// 替换为 <masked>，避免把自定义格式的密钥打进日志。
func maskValue(key string, value string) string {
	if !strings.EqualFold(key, "Authorization") {
		return value
	}
	if len(value) == 0 {
		return ""
	}
	var authType string
	if i := strings.Index(value, " "); i > 0 {
		authType = value[0:i]
	} else {
		authType = value
	}
	if !knownAuthTypes[strings.ToLower(authType)] {
		return "<masked>"
	}
	if len(value) > len(authType)+1 {
		value = authType + " <masked>"
	} else {
		value = authType
	}
	return value
}

func (rt *debuggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	logger := klog.FromContext(req.Context())
	levels := rt.levels

	// 依赖上下文时按每请求 logger 的详细度及硬编码映射决定输出内容，否则所有消息都以 V(0) 记录。
	//
	// 逐级提升以便一次判定；高级别包含低级别信息。
	if levels&(1<<DebugByContext) != 0 {
		if loggerV := logger.V(9); loggerV.Enabled() {
			logger = loggerV
			// 输出 curl 命令时不再单独输出 URL。
			levels |= levelsV9
		} else if loggerV := logger.V(8); loggerV.Enabled() {
			logger = loggerV
			levels |= levelsV8
		} else if loggerV := logger.V(7); loggerV.Enabled() {
			logger = loggerV
			levels |= levelsV7
		} else if loggerV := logger.V(6); loggerV.Enabled() {
			logger = loggerV
			levels |= levelsV6
		}
	}

	reqInfo := newRequestInfo(req)

	kvs := make([]any, 0, 8) // Exactly large enough for all appends below.
	if levels&(1<<DebugJustURL) != 0 {
		kvs = append(kvs,
			"verb", reqInfo.RequestVerb,
			"url", reqInfo.RequestURL,
		)
	}
	if levels&(1<<DebugCurlCommand) != 0 {
		kvs = append(kvs, "curlCommand", reqInfo.toCurl())
	}
	if levels&(1<<DebugRequestHeaders) != 0 {
		kvs = append(kvs, "headers", newHeadersMap(reqInfo.RequestHeaders))
	}
	if len(kvs) > 0 {
		logger.Info("Request", kvs...)
	}

	startTime := time.Now()

	if levels&(1<<DebugDetailedTiming) != 0 {
		var getConn, dnsStart, dialStart, tlsStart, serverStart time.Time
		var host string
		trace := &httptrace.ClientTrace{
			// DNS 解析。
			DNSStart: func(info httptrace.DNSStartInfo) {
				reqInfo.muTrace.Lock()
				defer reqInfo.muTrace.Unlock()
				dnsStart = time.Now()
				host = info.Host
			},
			DNSDone: func(info httptrace.DNSDoneInfo) {
				reqInfo.muTrace.Lock()
				defer reqInfo.muTrace.Unlock()
				reqInfo.DNSLookup = time.Since(dnsStart)
				logger.Info("HTTP Trace: DNS Lookup resolved", "host", host, "address", info.Addrs)
			},
			// 拨号。
			ConnectStart: func(network, addr string) {
				reqInfo.muTrace.Lock()
				defer reqInfo.muTrace.Unlock()
				dialStart = time.Now()
			},
			ConnectDone: func(network, addr string, err error) {
				reqInfo.muTrace.Lock()
				defer reqInfo.muTrace.Unlock()
				reqInfo.Dialing = time.Since(dialStart)
				if err != nil {
					logger.Info("HTTP Trace: Dial failed", "network", network, "address", addr, "err", err)
				} else {
					logger.Info("HTTP Trace: Dial succeed", "network", network, "address", addr)
				}
			},
			// TLS 握手。
			TLSHandshakeStart: func() {
				tlsStart = time.Now()
			},
			TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
				reqInfo.muTrace.Lock()
				defer reqInfo.muTrace.Unlock()
				reqInfo.TLSHandshake = time.Since(tlsStart)
			},
			// 连接建立（可能是 DNS + 拨号，或仅是从连接池取连接的耗时）。
			GetConn: func(hostPort string) {
				getConn = time.Now()
			},
			GotConn: func(info httptrace.GotConnInfo) {
				reqInfo.muTrace.Lock()
				defer reqInfo.muTrace.Unlock()
				reqInfo.GetConnection = time.Since(getConn)
				reqInfo.ConnectionReused = info.Reused
			},
			// 服务端处理（从写完请求到收到首字节的时间）。
			WroteRequest: func(info httptrace.WroteRequestInfo) {
				reqInfo.muTrace.Lock()
				defer reqInfo.muTrace.Unlock()
				serverStart = time.Now()
			},
			GotFirstResponseByte: func() {
				reqInfo.muTrace.Lock()
				defer reqInfo.muTrace.Unlock()
				reqInfo.ServerProcessing = time.Since(serverStart)
			},
		}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	}

	response, err := rt.delegatedRoundTripper.RoundTrip(req)
	reqInfo.Duration = time.Since(startTime)

	reqInfo.complete(response, err)

	kvs = make([]any, 0, 20) // Exactly large enough for all appends below.
	if levels&(1<<DebugURLTiming) != 0 {
		kvs = append(kvs, "verb", reqInfo.RequestVerb, "url", reqInfo.RequestURL)
	}
	if levels&(1<<DebugURLTiming|1<<DebugResponseStatus) != 0 {
		kvs = append(kvs, "status", reqInfo.ResponseStatus)
	}
	if levels&(1<<DebugResponseHeaders) != 0 {
		kvs = append(kvs, "headers", newHeadersMap(reqInfo.ResponseHeaders))
	}
	if levels&(1<<DebugURLTiming|1<<DebugDetailedTiming|1<<DebugResponseStatus) != 0 {
		kvs = append(kvs, "milliseconds", reqInfo.Duration.Nanoseconds()/int64(time.Millisecond))
	}
	if levels&(1<<DebugDetailedTiming) != 0 {
		if !reqInfo.ConnectionReused {
			kvs = append(kvs,
				"dnsLookupMilliseconds", reqInfo.DNSLookup.Nanoseconds()/int64(time.Millisecond),
				"dialMilliseconds", reqInfo.Dialing.Nanoseconds()/int64(time.Millisecond),
				"tlsHandshakeMilliseconds", reqInfo.TLSHandshake.Nanoseconds()/int64(time.Millisecond),
			)
		} else {
			kvs = append(kvs, "getConnectionMilliseconds", reqInfo.GetConnection.Nanoseconds()/int64(time.Millisecond))
		}
		if reqInfo.ServerProcessing != 0 {
			kvs = append(kvs, "serverProcessingMilliseconds", reqInfo.ServerProcessing.Nanoseconds()/int64(time.Millisecond))
		}
	}
	if len(kvs) > 0 {
		logger.Info("Response", kvs...)
	}

	return response, err
}

// headerMap 在字符串输出时排序并分行且不加引号，使用 zapr 时输出为 JSON。
type headersMap http.Header

// newHeadersMap 在把 header map 交给 logger 之前对所有敏感值脱敏；虽然实践中 logger 都会
// 调用 String 或 MarshalLog，但这并非保证。
//
// 复制而不是原地修改，避免把脱敏结果写回真实请求头。
func newHeadersMap(header http.Header) headersMap {
	h := make(headersMap, len(header))
	for key, values := range header {
		maskedValues := make([]string, 0, len(values))
		for _, value := range values {
			maskedValues = append(maskedValues, maskValue(key, value))
		}
		h[key] = maskedValues
	}
	return h
}

var _ fmt.Stringer = headersMap{}
var _ logr.Marshaler = headersMap{}

func (h headersMap) String() string {
	// 预分配足够大的固定容量，通常可避免扩容分配。
	keys := make([]string, 0, 20)
	for key := range h {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var buffer strings.Builder
	for _, key := range keys {
		for _, value := range h[key] {
			_, _ = buffer.WriteString(key)
			_, _ = buffer.WriteString(": ")
			_, _ = buffer.WriteString(value)
			_, _ = buffer.WriteString("\n")
		}
	}
	return buffer.String()
}

func (h headersMap) MarshalLog() any {
	return map[string][]string(h)
}

func (rt *debuggingRoundTripper) WrappedRoundTripper() http.RoundTripper {
	return rt.delegatedRoundTripper
}

func legalHeaderByte(b byte) bool {
	return int(b) < len(legalHeaderKeyBytes) && legalHeaderKeyBytes[b]
}

func shouldEscape(b byte) bool {
	// url.PathUnescape() 遇到 '%' 后不是两位十六进制数会报错，因此这里主动编码 '%'。
	return !legalHeaderByte(b) || b == '%'
}

// headerKeyEscape 把 impersonate extra 的键编码成合法的 HTTP 头后缀。
//
// 规则与 URL path 转义一致，且额外编码 '%'，保证解码时不会产生歧义。
func headerKeyEscape(key string) string {
	buf := strings.Builder{}
	for i := 0; i < len(key); i++ {
		b := key[i]
		if shouldEscape(b) {
			// 对需要转义的字节做 % 编码：
			// https://tools.ietf.org/html/rfc3986#section-2.1
			fmt.Fprintf(&buf, "%%%02X", b)
			continue
		}
		buf.WriteByte(b)
	}
	return buf.String()
}

// legalHeaderKeyBytes 复制自 net/http/lex.go 的 isTokenTable，
// 因为 token 字符集合由 RFC 7230 定义，需要保持完全一致。
// See https://httpwg.github.io/specs/rfc7230.html#rule.token.separators
var legalHeaderKeyBytes = [127]bool{
	'%':  true,
	'!':  true,
	'#':  true,
	'$':  true,
	'&':  true,
	'\'': true,
	'*':  true,
	'+':  true,
	'-':  true,
	'.':  true,
	'0':  true,
	'1':  true,
	'2':  true,
	'3':  true,
	'4':  true,
	'5':  true,
	'6':  true,
	'7':  true,
	'8':  true,
	'9':  true,
	'A':  true,
	'B':  true,
	'C':  true,
	'D':  true,
	'E':  true,
	'F':  true,
	'G':  true,
	'H':  true,
	'I':  true,
	'J':  true,
	'K':  true,
	'L':  true,
	'M':  true,
	'N':  true,
	'O':  true,
	'P':  true,
	'Q':  true,
	'R':  true,
	'S':  true,
	'T':  true,
	'U':  true,
	'W':  true,
	'V':  true,
	'X':  true,
	'Y':  true,
	'Z':  true,
	'^':  true,
	'_':  true,
	'`':  true,
	'a':  true,
	'b':  true,
	'c':  true,
	'd':  true,
	'e':  true,
	'f':  true,
	'g':  true,
	'h':  true,
	'i':  true,
	'j':  true,
	'k':  true,
	'l':  true,
	'm':  true,
	'n':  true,
	'o':  true,
	'p':  true,
	'q':  true,
	'r':  true,
	's':  true,
	't':  true,
	'u':  true,
	'v':  true,
	'w':  true,
	'x':  true,
	'y':  true,
	'z':  true,
	'|':  true,
	'~':  true,
}
