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

package proxy

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/streaming/pkg/httpstream"

	"github.com/mxk/go-flowrate/flowrate"

	"k8s.io/klog/v2"
)

// UpgradeRequestRoundTripper 在向服务端发起协议升级前，提供额外方法来给请求补充
// 认证或其他协议层信息。响应由拦截式代理负责处理。
type UpgradeRequestRoundTripper interface {
	http.RoundTripper
	// WrapRequest 接收一个合法 HTTP 请求，返回补充了完成服务端升级所需 HTTP 层信息后的版本。
	// 它看不到响应，因此实现应跳过任何依赖响应才能完成的请求侧逻辑。
	WrapRequest(*http.Request) (*http.Request, error)
}

// UpgradeAwareHandler 是处理可能需要协议升级（如 SPDY/WebSocket）的代理请求的 handler。
// 非升级请求交给 httputil.ReverseProxy 处理；升级请求则直接拨号后端并双向转发原始连接。
type UpgradeAwareHandler struct {
	// UpgradeRequired 为 true 时，非升级连接会被直接拒绝。
	UpgradeRequired bool
	// Location 是上游代理的地址。除非 UseRequestLocation 为 true，
	// 否则升级请求会拨号到该地址。
	Location *url.URL
	// AppendLocationPath 决定是否把 Location 的原始路径追加到上游代理请求路径中。
	AppendLocationPath bool
	// Transport 是可选的反向代理 RoundTripper；为 nil 时使用默认代理传输层。
	Transport http.RoundTripper
	// UpgradeTransport 若设置，将作为升级请求的后端传输层。
	// 这样调用方可以禁用 HTTP/2——升级连接需要原始 TCP 语义，HTTP/2 多路复用不适用。
	UpgradeTransport UpgradeRequestRoundTripper
	// WrapTransport 表示是否用默认代理行为（URL 改写、设置 X-Forwarded-* 头）包装提供的 Transport。
	WrapTransport bool
	// UseRequestLocation 表示与后端通信时使用入站请求的 URL（而非 Location）。
	UseRequestLocation bool
	// UseLocationHost 把发往后端的请求中的 HTTP Host 头覆盖为 Location 的 Host。
	// 它覆盖请求的 req.Host 字段，而 UseRequestLocation 覆盖 req.URL 字段：
	// req.URL.Host 指定要连接的服务器，req.Host 指定 HTTP 请求中发送的 Host 头值。
	// 为 false 时，入站请求的 req.Host 会原样转发给后端。
	UseLocationHost bool
	// FlushInterval 控制标准 HTTP 代理从上游刷新内容的频率。
	// 对流式响应（如日志）需要较小的间隔，避免内容在缓冲区中滞留。
	FlushInterval time.Duration
	// MaxBytesPerSec 控制上游连接的最大速率；为 0 表示不限速。
	MaxBytesPerSec int64
	// Responder 接收建立代理过程中发生的错误。
	Responder ErrorResponder
	// RejectForwardingRedirects 为 true 时，拒绝把后端的重定向响应转发给客户端。
	RejectForwardingRedirects bool
}

// defaultFlushInterval 是默认的刷新间隔，在吞吐与流式延迟之间折中。
const defaultFlushInterval = 200 * time.Millisecond

// ErrorResponder 抽象了代理 handler 的错误上报，避免把特定错误格式硬编码在代理逻辑中。
type ErrorResponder interface {
	Error(w http.ResponseWriter, req *http.Request, err error)
}

// SimpleErrorResponder 是 ErrorResponder 的旧式实现，
// 适用于每个代理只处理单个请求/响应的调用方。
type SimpleErrorResponder interface {
	Error(err error)
}

// NewErrorResponder 把旧的 SimpleErrorResponder 适配为 ErrorResponder。
func NewErrorResponder(r SimpleErrorResponder) ErrorResponder {
	return simpleResponder{r}
}

// simpleResponder 是 SimpleErrorResponder 到 ErrorResponder 的适配器。
type simpleResponder struct {
	responder SimpleErrorResponder
}

func (r simpleResponder) Error(w http.ResponseWriter, req *http.Request, err error) {
	r.responder.Error(err)
}

// upgradeRequestRoundTripper 实现 UpgradeRequestRoundTripper。
type upgradeRequestRoundTripper struct {
	http.RoundTripper
	upgrader http.RoundTripper
}

// 编译期断言：同时实现 UpgradeRequestRoundTripper 与 RoundTripperWrapper。
var (
	_ UpgradeRequestRoundTripper  = &upgradeRequestRoundTripper{}
	_ utilnet.RoundTripperWrapper = &upgradeRequestRoundTripper{}
)

// WrappedRoundTripper 返回调用方实际应使用的（底层连接）RoundTripper。
func (rt *upgradeRequestRoundTripper) WrappedRoundTripper() http.RoundTripper {
	return rt.RoundTripper
}

// WrapRequest 调用嵌套的 upgrader，然后返回其产出的请求（携带已补充的请求头）。
// 注释中的 WriteToRequest 是历史名称。
func (rt *upgradeRequestRoundTripper) WrapRequest(req *http.Request) (*http.Request, error) {
	resp, err := rt.upgrader.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	return resp.Request, nil
}

// onewayRoundTripper 捕获传入的请求（假定已被其他 RoundTripper 修改），然后返回一个假响应。
// 它用于在 RoundTripper 链的末端"回显"请求，以便取出被链上各层加工后的最终请求。
type onewayRoundTripper struct{}

// RoundTrip 返回一个简单的 200 OK 响应，并把请求挂在响应的 Request 字段上。
func (onewayRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(&bytes.Buffer{}),
		Request:    req,
	}, nil
}

// MirrorRequest 可作为 RoundTripper 链的核心，用于取回被链上各层加工后的请求。
var MirrorRequest http.RoundTripper = onewayRoundTripper{}

// NewUpgradeRequestRoundTripper 接收两个 RoundTripper：
// 一个负责底层 TCP 连接，另一个负责向 HTTP 请求写入请求头。
// 后者用于加工请求头，加工后的请求通过前者写到连接上。
func NewUpgradeRequestRoundTripper(connection, request http.RoundTripper) UpgradeRequestRoundTripper {
	return &upgradeRequestRoundTripper{
		RoundTripper: connection,
		upgrader:     request,
	}
}

// normalizeLocation 解析完整 URL，并在缺少 scheme 时补为 http。
// 这里先 String() 再 Parse，相当于复制一份 URL，避免修改调用方传入的对象。
func normalizeLocation(location *url.URL) *url.URL {
	normalized, _ := url.Parse(location.String())
	if len(normalized.Scheme) == 0 {
		normalized.Scheme = "http"
	}
	return normalized
}

// NewUpgradeAwareHandler 创建带默认刷新间隔的代理 handler。
// Responder 是必需的，用于把错误返回给调用方。
func NewUpgradeAwareHandler(location *url.URL, transport http.RoundTripper, wrapTransport, upgradeRequired bool, responder ErrorResponder) *UpgradeAwareHandler {
	return &UpgradeAwareHandler{
		Location:        normalizeLocation(location),
		Transport:       transport,
		WrapTransport:   wrapTransport,
		UpgradeRequired: upgradeRequired,
		FlushInterval:   defaultFlushInterval,
		Responder:       responder,
	}
}

// proxyRedirectsforRootPath 对根路径请求做 301 重定向，补齐末尾斜杠。
// 返回 true 表示已写响应，调用方应立即返回。
func proxyRedirectsforRootPath(path string, w http.ResponseWriter, req *http.Request) bool {
	redirect := false
	method := req.Method

	// 摘自 pkg/genericapiserver/endpoints/handlers/proxy.go#ServeHTTP：
	// 把空路径请求重定向到以 '/' 结尾的位置。这本质上是针对
	// https://issue.k8s.io/4958 的变通处理。
	// 注意：此逻辑必须放在 tryUpgrade 之后，否则会破坏升级流程。
	// 只对幂等的 GET/HEAD 做重定向，避免改变 POST 等请求的语义。
	if len(path) == 0 && (method == http.MethodGet || method == http.MethodHead) {
		var queryPart string
		if len(req.URL.RawQuery) > 0 {
			queryPart = "?" + req.URL.RawQuery
		}
		w.Header().Set("Location", req.URL.Path+"/"+queryPart)
		w.WriteHeader(http.StatusMovedPermanently)
		redirect = true
	}
	return redirect
}

// ServeHTTP 处理代理请求：优先尝试协议升级，否则走普通反向代理。
func (h *UpgradeAwareHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// 升级请求一旦处理成功就接管连接，必须立即返回。
	if h.tryUpgrade(w, req) {
		return
	}
	if h.UpgradeRequired {
		h.Responder.Error(w, req, errors.NewBadRequest("Upgrade request required"))
		return
	}

	// 基于 Location 构造目标 URL，并沿用原始查询参数。
	loc := *h.Location
	loc.RawQuery = req.URL.RawQuery

	// 若原始请求 URL 以 '/' 结尾，则代理 URL 也补上 '/'，保持目录语义。
	if !strings.HasSuffix(loc.Path, "/") && strings.HasSuffix(req.URL.Path, "/") {
		loc.Path += "/"
	}

	// 根路径重定向必须在升级尝试之后，见 proxyRedirectsforRootPath 中的说明。
	proxyRedirect := proxyRedirectsforRootPath(loc.Path, w, req)
	if proxyRedirect {
		return
	}

	// 未提供 Transport 或要求包装时，构造带 URL 改写与 CORS 清理的默认传输层。
	if h.Transport == nil || h.WrapTransport {
		h.Transport = h.defaultProxyTransport(req.URL, h.Transport)
	}

	// WithContext 创建共享同一 context 的浅拷贝请求；随后替换 Header 以避免并发修改原请求。
	newReq := req.WithContext(req.Context())
	newReq.Header = utilnet.CloneHeader(req.Header)
	if !h.UseRequestLocation {
		newReq.URL = &loc
	}
	if h.UseLocationHost {
		// 对依赖 HTTP Host 头的后端（如 API 网关）必须替换 req.Host：
		// 在填充 Host 头时，req.Host 的优先级高于 req.URL.Host。
		newReq.Host = h.Location.Host
	}

	// 构造反向代理的目标地址；路径是否拼接由 AppendLocationPath 决定。
	reverseProxyLocation := &url.URL{Scheme: h.Location.Scheme, Host: h.Location.Host}
	if h.AppendLocationPath {
		reverseProxyLocation.Path = h.Location.Path
	}

	proxy := httputil.NewSingleHostReverseProxy(reverseProxyLocation)
	proxy.Transport = h.Transport
	proxy.FlushInterval = h.FlushInterval
	proxy.ErrorLog = log.New(noSuppressPanicError{}, "", log.LstdFlags)
	// 拒绝转发重定向：把 3xx 改写成 502，防止后端把客户端引到不可达的内部地址。
	if h.RejectForwardingRedirects {
		oldModifyResponse := proxy.ModifyResponse
		proxy.ModifyResponse = func(response *http.Response) error {
			code := response.StatusCode
			if code >= 300 && code <= 399 && len(response.Header.Get("Location")) > 0 {
				// 丢弃原始响应体，避免连接泄漏。
				response.Body.Close()
				msg := "the backend attempted to redirect this request, which is not permitted"
				// 用 502 响应替换原响应。
				*response = http.Response{
					StatusCode:    http.StatusBadGateway,
					Status:        fmt.Sprintf("%d %s", response.StatusCode, http.StatusText(response.StatusCode)),
					Body:          io.NopCloser(strings.NewReader(msg)),
					ContentLength: int64(len(msg)),
				}
			} else {
				if oldModifyResponse != nil {
					if err := oldModifyResponse(response); err != nil {
						return err
					}
				}
			}
			return nil
		}
	}
	if h.Responder != nil {
		// 若提供了错误拦截器/响应器则接入：
		// 自定义响应器可用于统一错误上报，或通过不把非致命错误发给客户端来支持重试机制。
		proxy.ErrorHandler = h.Responder.Error
	}
	proxy.ServeHTTP(w, newReq)
}

// noSuppressPanicError 过滤 httputil 在测试中输出的 "suppressing panic" 噪声日志。
type noSuppressPanicError struct{}

func (noSuppressPanicError) Write(p []byte) (n int, err error) {
	// 跳过 "suppressing panic for copyResponse error in test; copy error" 消息：
	// 它在 CI 中每次 kube-apiserver 退出时都会出现，纯属噪声，却容易被误认为致命错误。
	if strings.Contains(string(p), "suppressing panic") {
		return len(p), nil
	}
	return os.Stderr.Write(p)
}

// tryUpgrade 在请求是协议升级请求并已处理时返回 true；非升级请求返回 false。
// 升级路径直接操作原始 TCP 连接，绕过 net/http 的请求/响应模型。
func (h *UpgradeAwareHandler) tryUpgrade(w http.ResponseWriter, req *http.Request) bool {
	logger := klog.FromContext(req.Context())
	// 非升级请求交给普通反向代理处理。
	if !httpstream.IsUpgradeRequest(req) {
		logger.V(6).Info("Request was not an upgrade")
		return false
	}

	var (
		backendConn net.Conn
		rawResponse []byte
		err         error
	)

	// 默认拨号到 Location；UseRequestLocation 时改用入站请求的路径，
	// 但仍沿用 Location 的 scheme/host，确保连接的是后端而不是客户端。
	location := *h.Location
	if h.UseRequestLocation {
		location = *req.URL
		location.Scheme = h.Location.Scheme
		location.Host = h.Location.Host
		if h.AppendLocationPath {
			location.Path = singleJoiningSlash(h.Location.Path, location.Path)
		}
	}

	clone := utilnet.CloneRequest(req)
	// 仅在升级路径手动追加 X-Forwarded-For；非升级路径由
	// httputil.NewSingleHostReverseProxy 负责。
	utilnet.AppendForwardedForHeader(clone)
	logger.V(6).Info("Connecting to backend proxy (direct dial)", "location", &location, "headers", clone.Header)
	if h.UseLocationHost {
		clone.Host = h.Location.Host
	}
	clone.URL = &location
	logger.V(6).Info("UpgradeAwareProxy: dialing for SPDY upgrade with headers", "headers", clone.Header)
	// 直接拨号后端并写出请求，之后进入裸连接转发阶段。
	backendConn, err = h.DialForUpgrade(clone)
	if err != nil {
		logger.V(6).Info("Proxy connection error", "err", err)
		h.Responder.Error(w, req, err)
		return true
	}
	defer backendConn.Close()

	// 从 backendConn 读取后端的 HTTP 响应（至少读到响应头），以判断是否同意升级。
	backendHTTPResponse, headerBytes, err := getResponse(io.MultiReader(bytes.NewReader(rawResponse), backendConn))
	if err != nil {
		logger.V(6).Info("Proxy connection error", "err", err)
		h.Responder.Error(w, req, err)
		return true
	}
	if len(headerBytes) > len(rawResponse) {
		// 读取范围超过了 rawResponse 中的字节，更新为从后端读到的完整字节集合，
		// 后续会原样转发给客户端。
		rawResponse = headerBytes
	}

	// 后端未同意升级时向客户端返回错误。若后端返回的是 4xx/5xx，
	// 会在劫持连接后把该错误响应原样转发；否则在此返回通用错误。
	if backendHTTPResponse.StatusCode != http.StatusSwitchingProtocols && backendHTTPResponse.StatusCode < 400 {
		err := fmt.Errorf("invalid upgrade response: status code %d", backendHTTPResponse.StatusCode)
		logger.Error(err, "Proxy upgrade error")
		h.Responder.Error(w, req, err)
		return true
	}

	// 连接一旦被劫持，ErrorResponder 将不再生效，因此劫持必须是升级流程的最后一步。
	requestHijacker, ok := w.(http.Hijacker)
	if !ok {
		logger.Error(nil, "Unable to hijack response writer", "type", fmt.Sprintf("%T", w))
		h.Responder.Error(w, req, fmt.Errorf("request connection cannot be hijacked: %T", w))
		return true
	}
	requestHijackedConn, _, err := requestHijacker.Hijack()
	if err != nil {
		logger.Error(err, "Unable to hijack response")
		h.Responder.Error(w, req, fmt.Errorf("error hijacking connection: %v", err))
		return true
	}
	defer requestHijackedConn.Close()

	if backendHTTPResponse.StatusCode != http.StatusSwitchingProtocols {
		// 后端没有升级请求（通常是错误响应）：把后端的响应回显给客户端后关闭连接。
		logger.V(6).Info("Proxy upgrade error", "statusCode", backendHTTPResponse.StatusCode)
		// 设置读写超时，防止对端不读不写导致连接永久挂起。
		deadline := time.Now().Add(10 * time.Second)
		backendConn.SetReadDeadline(deadline)
		requestHijackedConn.SetWriteDeadline(deadline)
		// 把响应写回客户端。
		err := backendHTTPResponse.Write(requestHijackedConn)
		if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			logger.Error(err, "Error proxying data from backend to client")
		}
		//表明已处理该请求。
		return true
	}

	// 把后端响应头（以及可能多读到的字节）原样转发给客户端。
	if len(rawResponse) > 0 {
		logger.V(6).Info("Writing to hijacked connection", "length", len(rawResponse))
		if _, err = requestHijackedConn.Write(rawResponse); err != nil {
			utilruntime.HandleErrorWithLogger(logger, err, "Error proxying response from backend to client")
		}
	}

	// 代理连接。连接是双向的，因此每个方向各用一个 goroutine 拷贝数据。
	// 任一方向结束后函数即返回，由 defer 关闭另一半连接，从而让另一个 goroutine 退出。
	writerComplete := make(chan struct{})
	readerComplete := make(chan struct{})

	go func() {
		// 客户端 -> 后端；限速时用 flowrate 包装 writer。
		var writer io.WriteCloser
		if h.MaxBytesPerSec > 0 {
			writer = flowrate.NewWriter(backendConn, h.MaxBytesPerSec)
		} else {
			writer = backendConn
		}
		_, err := io.Copy(writer, requestHijackedConn)
		if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			logger.Error(err, "Error proxying data from client to backend")
		}
		close(writerComplete)
	}()

	go func() {
		// 后端 -> 客户端；限速时用 flowrate 包装 reader。
		var reader io.ReadCloser
		if h.MaxBytesPerSec > 0 {
			reader = flowrate.NewReader(backendConn, h.MaxBytesPerSec)
		} else {
			reader = backendConn
		}
		_, err := io.Copy(requestHijackedConn, reader)
		if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			logger.Error(err, "Error proxying data from backend to client")
		}
		close(readerComplete)
	}()

	// 等待任一方向结束；随后 defer 会清理另一半连接。
	select {
	case <-writerComplete:
	case <-readerComplete:
	}
	logger.V(6).Info("Disconnecting from backend proxy", "location", &location, "headers", clone.Header)

	return true
}

// singleJoiningSlash 拼接两个路径，保证中间恰好有一个斜杠。
// FIXME：取自 net/http/httputil/reverseproxy.go，因为该函数未导出，无法直接复用。
// 另请参见：https://github.com/golang/go/issues/44290
func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

// DialForUpgrade 拨号后端以执行升级：若配置了 UpgradeTransport，
// 先由其补充请求头，再通过它拨号；否则使用普通 Transport。
func (h *UpgradeAwareHandler) DialForUpgrade(req *http.Request) (net.Conn, error) {
	if h.UpgradeTransport == nil {
		return dial(req, h.Transport)
	}
	updatedReq, err := h.UpgradeTransport.WrapRequest(req)
	if err != nil {
		return nil, err
	}
	return dial(updatedReq, h.UpgradeTransport)
}

// getResponse 从给定 reader 读取一个 HTTP 响应，返回响应对象、读取过程中消耗的原始字节以及错误。
// 原始字节需要保留，因为升级成功时它们属于要转发给客户端的裸数据流。
// 注释中的 getResponseCode 是历史名称。
func getResponse(r io.Reader) (*http.Response, []byte, error) {
	// 预分配 256 字节，覆盖绝大多数响应头大小。
	rawResponse := bytes.NewBuffer(make([]byte, 0, 256))
	// 用 TeeReader 把读取响应头时消耗的字节同时保存到 rawResponse。
	resp, err := http.ReadResponse(bufio.NewReader(io.TeeReader(r, rawResponse)), nil)
	if err != nil {
		return nil, nil, err
	}
	// 返回响应对象与本次读取消耗的原始字节。
	return resp, rawResponse.Bytes(), nil
}

// dial 拨号到 req.URL 指定的后端，并把 req 写入该连接。
func dial(req *http.Request, transport http.RoundTripper) (net.Conn, error) {
	conn, err := DialURL(req.Context(), req.URL, transport)
	if err != nil {
		return nil, fmt.Errorf("error dialing backend: %v", err)
	}

	// 请求写入失败时必须关闭连接，否则会泄漏文件描述符。
	if err = req.Write(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("error sending request: %v", err)
	}

	return conn, err
}

// defaultProxyTransport 构造默认传输层：用 URL 改写 Transport 包装内部传输层，
// 再套一层 CORS 头清理。
func (h *UpgradeAwareHandler) defaultProxyTransport(url *url.URL, internalTransport http.RoundTripper) http.RoundTripper {
	scheme := url.Scheme
	host := url.Host
	// 通过"去掉 Location 路径后缀"推导出代理前缀；
	// 末尾斜杠需对齐后再做 TrimSuffix，否则会漏掉或误删斜杠。
	suffix := h.Location.Path
	if strings.HasSuffix(url.Path, "/") && !strings.HasSuffix(suffix, "/") {
		suffix += "/"
	}
	pathPrepend := strings.TrimSuffix(url.Path, suffix)
	rewritingTransport := &Transport{
		Scheme:       scheme,
		Host:         host,
		PathPrepend:  pathPrepend,
		RoundTripper: internalTransport,
	}
	return &corsRemovingTransport{
		RoundTripper: rewritingTransport,
	}
}

// corsRemovingTransport 是内部传输层的包装器，用于从响应中移除 CORS 头。
// 实现 pkg/util/net.RoundTripperWrapper。
type corsRemovingTransport struct {
	http.RoundTripper
}

// 编译期断言：corsRemovingTransport 实现 RoundTripperWrapper。
var _ = utilnet.RoundTripperWrapper(&corsRemovingTransport{})

// RoundTrip 转发请求，并在成功时移除响应中的 CORS 头。
func (rt *corsRemovingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := rt.RoundTripper.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	removeCORSHeaders(resp)
	return resp, nil
}

// WrappedRoundTripper 返回被包装的内部传输层。
func (rt *corsRemovingTransport) WrappedRoundTripper() http.RoundTripper {
	return rt.RoundTripper
}

// removeCORSHeaders 移除后端返回的 CORS 头。
// 代理不应把后端的 CORS 策略透传给客户端——CORS 应由暴露代理的 API server 统一决定，
// 否则可能意外放开跨域访问。所有响应在返回前都应调用。
func removeCORSHeaders(resp *http.Response) {
	resp.Header.Del("Access-Control-Allow-Credentials")
	resp.Header.Del("Access-Control-Allow-Headers")
	resp.Header.Del("Access-Control-Allow-Methods")
	resp.Header.Del("Access-Control-Allow-Origin")
}
