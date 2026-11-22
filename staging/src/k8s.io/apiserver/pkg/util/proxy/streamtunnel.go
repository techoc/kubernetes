/*
Copyright 2024 The Kubernetes Authors.

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

// 本文件实现 stream tunnel 代理：部分网络环境不支持 SPDY 升级但支持 WebSocket，
// 这里在两者之间做协议转换。关键是保留 HTTP 响应头语义：后端返回的 101 或
// 错误响应必须先被解析，再决定如何初始化下游连接。

package proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	gwebsocket "github.com/gorilla/websocket"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	constants "k8s.io/apimachinery/pkg/util/portforward"
	"k8s.io/apiserver/pkg/util/proxy/metrics"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/klog/v2"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/streaming/pkg/httpstream/spdy"
	"k8s.io/streaming/pkg/httpstream/wsstream"
)

// TunnelingHandler is a handler which tunnels SPDY through WebSockets.
type TunnelingHandler struct {
	// Used to communicate between upstream SPDY and downstream tunnel.
	upgradeHandler http.Handler
}

// NewTunnelingHandler is used to create the tunnel between an upstream
// SPDY connection and a downstream tunneling connection through the stored
// UpgradeAwareProxy.
func NewTunnelingHandler(upgradeHandler http.Handler) *TunnelingHandler {
	return &TunnelingHandler{upgradeHandler: upgradeHandler}
}

// ServeHTTP uses the upgradeHandler to tunnel between a downstream tunneling
// connection and an upstream SPDY connection. The tunneling connection is
// a wrapped WebSockets connection which communicates SPDY framed data. In the
// case the upstream upgrade fails, we delegate communication to the passed
// in "w" ResponseWriter.
//
// 中文说明：客户端必须在 WebSocket 子协议中声明 SPDY 隧道协议，否则无法确定
// 上游应使用哪个 SPDY 版本，直接 400 拒绝。writer 中的 w 与 conn 互斥：
// 升级成功用 conn，失败则用 w 写普通 HTTP 错误响应。
func (h *TunnelingHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	logger := klog.FromContext(req.Context())
	logger.V(4).Info("TunnelingHandler ServeHTTP")

	spdyProtocols := spdyProtocolsFromWebsocketProtocols(req)
	if len(spdyProtocols) == 0 {
		metrics.IncStreamTunnelRequest(req.Context(), strconv.Itoa(http.StatusBadRequest))
		http.Error(w, "unable to upgrade: no tunneling spdy protocols provided", http.StatusBadRequest)
		return
	}

	spdyRequest := createSPDYRequest(req, spdyProtocols...)

	// The fields "w" and "conn" are mutually exclusive. Either a successful upgrade occurs
	// and the "conn" is hijacked and used in the subsequent upgradeHandler, or
	// the upgrade failed, and "w" is the delegate used for the non-upgrade response.
	writer := &tunnelingResponseWriter{
		logger: logger,
		// "w" is used in the non-upgrade error cases called in the upgradeHandler.
		w: w,
		// "conn" is returned in the successful upgrade case when hijacked in the upgradeHandler.
		conn: &headerInterceptingConn{
			logger: logger,
			initializableConn: &tunnelingWebsocketUpgraderConn{
				w:   w,
				req: req,
			},
		},
	}

	logger.V(4).Info("Tunnel spdy through websockets using the UpgradeAwareProxy")
	h.upgradeHandler.ServeHTTP(writer, spdyRequest)
}

// createSPDYRequest modifies the passed request to remove
// WebSockets headers and add SPDY upgrade information, including
// spdy protocols acceptable to the client.
//
// 中文说明：必须克隆请求——原请求仍被下游 WebSocket 升级使用，直接修改会破坏它。
// 请求体被丢弃：SPDY 升级请求本身无 body，保留它会让上游误读。
func createSPDYRequest(req *http.Request, spdyProtocols ...string) *http.Request {
	clone := utilnet.CloneRequest(req)
	// Clean up the websocket headers from the http request.
	clone.Header.Del(wsstream.WebSocketProtocolHeader)
	clone.Header.Del("Sec-Websocket-Key")
	clone.Header.Del("Sec-Websocket-Version")
	clone.Header.Del(httpstream.HeaderUpgrade)
	// Update the http request for an upstream SPDY upgrade.
	clone.Method = "POST"
	clone.Body = nil // Remove the request body which is unused.
	clone.Header.Set(httpstream.HeaderUpgrade, spdy.HeaderSpdy31)
	clone.Header.Del(httpstream.HeaderProtocolVersion)
	for i := range spdyProtocols {
		clone.Header.Add(httpstream.HeaderProtocolVersion, spdyProtocols[i])
	}
	return clone
}

// spdyProtocolsFromWebsocketProtocols returns a list of spdy protocols by filtering
// to Kubernetes websocket subprotocols prefixed with "SPDY/3.1+", then removing the prefix
//
// 中文说明：只接受同时带 Kubernetes 前缀与后缀的子协议，避免把无关的
// 自定义子协议误当成 SPDY 版本传给上游。
func spdyProtocolsFromWebsocketProtocols(req *http.Request) []string {
	var spdyProtocols []string
	for _, protocol := range gwebsocket.Subprotocols(req) {
		if strings.HasPrefix(protocol, constants.WebsocketsSPDYTunnelingPrefix) && strings.HasSuffix(protocol, constants.KubernetesSuffix) {
			spdyProtocols = append(spdyProtocols, strings.TrimPrefix(protocol, constants.WebsocketsSPDYTunnelingPrefix))
		}
	}
	return spdyProtocols
}

var _ http.ResponseWriter = &tunnelingResponseWriter{}
var _ http.Hijacker = &tunnelingResponseWriter{}

// tunnelingResponseWriter implements the http.ResponseWriter and http.Hijacker interfaces.
// Only non-upgrade responses can be written using WriteHeader() and Write().
// Once Write or WriteHeader is called, Hijack returns an error.
// Once Hijack is called, Write, WriteHeader, and Hijack return errors.
//
// 中文说明：状态机是单向的（written / hijacked 至多一个为真），互斥锁保证并发安全。
// 这种严格性让「升级成功」与「返回普通错误」两条路径不会交叉，避免写出半截响应。
type tunnelingResponseWriter struct {
	logger klog.Logger
	// w is used to delegate Header(), WriteHeader(), and Write() calls
	w http.ResponseWriter
	// conn is returned from Hijack()
	conn net.Conn
	// mu guards writes
	mu sync.Mutex
	// wrote tracks whether WriteHeader or Write has been called
	written bool
	// hijacked tracks whether Hijack has been called
	hijacked bool
}

// Hijack returns a delegate "net.Conn".
// An error is returned if Write(), WriteHeader(), or Hijack() was previously called.
// The returned bufio.ReadWriter is always nil.
func (w *tunnelingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.written {
		w.logger.Error(nil, "Hijack called after write")
		return nil, nil, errors.New("connection has already been written to")
	}
	if w.hijacked {
		w.logger.Error(nil, "Hijack called after hijack")
		return nil, nil, errors.New("connection has already been hijacked")
	}
	w.hijacked = true
	w.logger.V(6).Info("Hijack returning websocket tunneling net.Conn")
	return w.conn, nil, nil
}

// Header is delegated to the stored "http.ResponseWriter".
func (w *tunnelingResponseWriter) Header() http.Header {
	return w.w.Header()
}

// Write is delegated to the stored "http.ResponseWriter".
func (w *tunnelingResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.hijacked {
		w.logger.Error(nil, "Write called after hijack")
		return 0, http.ErrHijacked
	}
	w.written = true
	return w.w.Write(p)
}

// WriteHeader is delegated to the stored "http.ResponseWriter".
//
// 中文说明：101 必须经由 Hijack 后的连接发出，直接 WriteHeader(101) 会绕过
// WebSocket 握手逻辑；这里转换为 500，让配置错误显式失败而非静默产生坏连接。
func (w *tunnelingResponseWriter) WriteHeader(statusCode int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.written {
		w.logger.Error(nil, "WriteHeader called after write")
		return
	}
	if w.hijacked {
		w.logger.Error(nil, "WriteHeader called after hijack")
		return
	}
	w.written = true

	if statusCode == http.StatusSwitchingProtocols {
		// 101 upgrade responses must come via the hijacked connection, not WriteHeader
		w.logger.Error(nil, "WriteHeader called with 101 upgrade")
		http.Error(w.w, "unexpected upgrade", http.StatusInternalServerError)
		return
	}

	// pass through non-upgrade responses we don't need to translate
	w.w.WriteHeader(statusCode)
}

// headerInterceptingConn wraps the tunneling "net.Conn" to drain the
// HTTP response status/headers from the upstream SPDY connection, then use
// that to decide how to initialize the delegate connection for writes.
//
// 中文说明：SPDY 客户端期望先读到完整的 HTTP 响应头，再开始读写帧；
// 因此这里先缓冲并解析头部，解析成功后才初始化真正的下游连接，
// 并把剩余字节（可能已含 body）转发出去。
type headerInterceptingConn struct {
	logger klog.Logger
	// initializableConn is delegated to for all net.Conn methods.
	// initializableConn.Write() is not called until response headers have been read
	// and initializableConn#InitializeWrite() has been called with the result.
	initializableConn

	lock          sync.Mutex
	headerBuffer  []byte
	initialized   bool
	initializeErr error
}

// initializableConn is a connection that will be initialized before any calls to Write are made
type initializableConn interface {
	net.Conn
	// InitializeWrite is called when the backend response headers have been read.
	// backendResponse contains the parsed headers.
	// backendResponseBytes are the raw bytes the headers were parsed from.
	InitializeWrite(backendResponse *http.Response, backendResponseBytes []byte) error
}

const maxHeaderBytes = 1 << 20

// token for normal header / body separation (\r\n\r\n, but go tolerates the leading \r being absent)
var lfCRLF = []byte("\n\r\n")

// token for header / body separation without \r (which go tolerates)
var lfLF = []byte("\n\n")

// Write intercepts to initially swallow the HTTP response, then
// delegate to the tunneling "net.Conn" once the response has been
// seen and processed.
//
// 中文说明：头部可能分多次 Write 到达，因此在未见分隔符前持续累积并返回成功；
// 一旦识别出完整头部，解析后立即把同一批数据里的 body 字节转发。
// 缓冲区上限防止恶意上游发送无限长的「头部」耗尽内存。
func (h *headerInterceptingConn) Write(b []byte) (int, error) {
	h.lock.Lock()
	defer h.lock.Unlock()

	if h.initializeErr != nil {
		return 0, h.initializeErr
	}
	if h.initialized {
		return h.initializableConn.Write(b)
	}

	// Guard against excessive buffering
	if len(h.headerBuffer)+len(b) > maxHeaderBytes {
		return 0, fmt.Errorf("header size limit exceeded")
	}

	// Accumulate into headerBuffer
	h.headerBuffer = append(h.headerBuffer, b...)

	// Attempt to parse http response headers
	var headerBytes, bodyBytes []byte
	if i := bytes.Index(h.headerBuffer, lfCRLF); i != -1 {
		// headers terminated with \n\r\n
		headerBytes = h.headerBuffer[0 : i+len(lfCRLF)]
		bodyBytes = h.headerBuffer[i+len(lfCRLF):]
	} else if i := bytes.Index(h.headerBuffer, lfLF); i != -1 {
		// headers terminated with \n\n (which go tolerates)
		headerBytes = h.headerBuffer[0 : i+len(lfLF)]
		bodyBytes = h.headerBuffer[i+len(lfLF):]
	} else {
		// don't yet have a complete set of headers yet
		return len(b), nil
	}
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(headerBytes)), nil)
	if err != nil {
		h.logger.Error(err, "Invalid headers")
		h.initializeErr = err
		return len(b), err
	}
	resp.Body.Close() //nolint:errcheck

	h.headerBuffer = nil
	h.initialized = true
	h.initializeErr = h.initializableConn.InitializeWrite(resp, headerBytes)
	if h.initializeErr != nil {
		return len(b), h.initializeErr
	}
	if len(bodyBytes) > 0 {
		_, err = h.initializableConn.Write(bodyBytes)
	}
	return len(b), err
}

// tunnelingWebsocketUpgraderConn 在收到后端响应头之前延迟初始化，
// 之后把读写委派给真正的连接（WebSocket 或 hijack 后的原始连接）。
type tunnelingWebsocketUpgraderConn struct {
	// req is the websocket request, used for upgrading
	req *http.Request
	// w is the websocket writer, used for upgrading and writing error responses
	w http.ResponseWriter

	// lock guards conn and err
	lock sync.RWMutex
	// if conn is non-nil, InitializeWrite succeeded
	conn net.Conn
	// if err is non-nil, InitializeWrite failed or Close was called before InitializeWrite
	err error
}

// InitializeWrite 根据后端响应决定下游连接的形态：
//   - 101：校验 Upgrade/Connection 头后，升级为 WebSocket 并包装成隧道连接；
//   - 其他状态：把原始响应字节回放到 hijack 得到的连接上，透传给客户端。
//
// 中文说明：错误路径会关闭可能已打开的连接，避免泄漏；升级后不再向 ResponseWriter
// 写错误（此时 HTTP 响应已提交，再写会产生无效数据）。
func (u *tunnelingWebsocketUpgraderConn) InitializeWrite(backendResponse *http.Response, backendResponseBytes []byte) (err error) {
	// make sure we close a connection we open in error cases
	var conn net.Conn
	defer func() {
		if err != nil && conn != nil {
			conn.Close() //nolint:errcheck
		}
	}()

	u.lock.Lock()
	defer u.lock.Unlock()
	if u.conn != nil {
		return fmt.Errorf("InitializeWrite already called")
	}
	if u.err != nil {
		return u.err
	}

	logger := klog.FromContext(u.req.Context())
	if backendResponse.StatusCode == http.StatusSwitchingProtocols {
		connectionHeader := strings.ToLower(backendResponse.Header.Get(httpstream.HeaderConnection))
		upgradeHeader := strings.ToLower(backendResponse.Header.Get(httpstream.HeaderUpgrade))
		// 校验后端确实同意 SPDY 升级；缺失头说明后端行为异常，不能继续隧道化。
		if !strings.Contains(connectionHeader, strings.ToLower(httpstream.HeaderUpgrade)) || !strings.Contains(upgradeHeader, strings.ToLower(spdy.HeaderSpdy31)) {
			logger.Error(nil, "Unable to upgrade: missing upgrade headers in response", "headers", backendResponse.Header)
			u.err = fmt.Errorf("unable to upgrade: missing upgrade headers in response")
			metrics.IncStreamTunnelRequest(context.Background(), strconv.Itoa(http.StatusInternalServerError))
			http.Error(u.w, u.err.Error(), http.StatusInternalServerError)
			return u.err
		}

		// Translate the server's chosen SPDY protocol into the tunneled websocket protocol for the handshake
		var serverWebsocketProtocols []string
		if backendSPDYProtocol := strings.TrimSpace(backendResponse.Header.Get(httpstream.HeaderProtocolVersion)); backendSPDYProtocol != "" {
			serverWebsocketProtocols = []string{constants.WebsocketsSPDYTunnelingPrefix + backendSPDYProtocol}
		} else {
			serverWebsocketProtocols = []string{}
		}

		// Try to upgrade the websocket connection.
		// Beyond this point, we don't need to write errors to the response.
		var upgrader = gwebsocket.Upgrader{
			CheckOrigin:  func(r *http.Request) bool { return true },
			Subprotocols: serverWebsocketProtocols,
		}
		conn, err := upgrader.Upgrade(u.w, u.req, nil)
		if err != nil {
			logger.Error(err, "Error upgrading websocket connection")
			metrics.IncStreamTunnelRequest(context.Background(), strconv.Itoa(http.StatusInternalServerError))
			u.err = err
			return u.err
		}

		logger.V(4).Info("Websocket connection created", "protocol", conn.Subprotocol())
		metrics.IncStreamTunnelRequest(context.Background(), strconv.Itoa(http.StatusSwitchingProtocols))
		u.conn = portforward.NewTunnelingConnectionWithLogger(klog.LoggerWithName(logger, "server"), conn)
		return nil
	}

	// anything other than an upgrade should pass through the backend response
	logger.Error(nil, "SPDY upgrade failed", "status", backendResponse.Status)
	metrics.IncStreamTunnelRequest(context.Background(), strconv.Itoa(backendResponse.StatusCode))

	// try to hijack
	conn, _, err = u.w.(http.Hijacker).Hijack()
	if err != nil {
		logger.Error(err, "Unable to hijack response")
		u.err = err
		return u.err
	}
	// replay the backend response bytes to the hijacked conn
	//
	// 中文说明：设置写超时，避免客户端不读取时永久阻塞；回放的是原始字节，
	// 保证状态行、头部与 body 与后端完全一致。
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second)) //nolint:errcheck
	_, err = conn.Write(backendResponseBytes)
	if err != nil {
		u.err = err
		return u.err
	}
	u.conn = conn
	return nil
}

// Read 在初始化前返回空读而非阻塞，避免上游在握手完成前卡住。
func (u *tunnelingWebsocketUpgraderConn) Read(b []byte) (n int, err error) {
	u.lock.RLock()
	defer u.lock.RUnlock()
	if u.conn != nil {
		return u.conn.Read(b)
	}
	if u.err != nil {
		return 0, u.err
	}
	// return empty read without blocking until we are initialized
	return 0, nil
}

// Write 在初始化前直接报错：调用方不应在响应头确定之前写数据。
func (u *tunnelingWebsocketUpgraderConn) Write(b []byte) (n int, err error) {
	u.lock.RLock()
	defer u.lock.RUnlock()
	if u.conn != nil {
		return u.conn.Write(b)
	}
	if u.err != nil {
		return 0, u.err
	}
	return 0, fmt.Errorf("Write called before Initialize")
}

// Close 在未初始化时记录关闭状态并写一个 500 响应，防止后续 InitializeWrite 再升级。
func (u *tunnelingWebsocketUpgraderConn) Close() error {
	u.lock.Lock()
	defer u.lock.Unlock()
	if u.conn != nil {
		return u.conn.Close()
	}
	if u.err != nil {
		return u.err
	}
	// record that we closed so we don't write again or try to initialize
	u.err = fmt.Errorf("connection closed")
	// write a response
	http.Error(u.w, u.err.Error(), http.StatusInternalServerError)
	return nil
}

// LocalAddr 在初始化前返回空地址，避免调用方因 nil 解引用而 panic。
func (u *tunnelingWebsocketUpgraderConn) LocalAddr() net.Addr {
	u.lock.RLock()
	defer u.lock.RUnlock()
	if u.conn != nil {
		return u.conn.LocalAddr()
	}
	return noopAddr{}
}

// RemoteAddr 在初始化前返回空地址，理由同 LocalAddr。
func (u *tunnelingWebsocketUpgraderConn) RemoteAddr() net.Addr {
	u.lock.RLock()
	defer u.lock.RUnlock()
	if u.conn != nil {
		return u.conn.RemoteAddr()
	}
	return noopAddr{}
}

// SetDeadline 在初始化前是空操作：尚无底层连接可设置超时。
func (u *tunnelingWebsocketUpgraderConn) SetDeadline(t time.Time) error {
	u.lock.RLock()
	defer u.lock.RUnlock()
	if u.conn != nil {
		return u.conn.SetDeadline(t)
	}
	return nil
}

// SetReadDeadline 在初始化前是空操作。
func (u *tunnelingWebsocketUpgraderConn) SetReadDeadline(t time.Time) error {
	u.lock.RLock()
	defer u.lock.RUnlock()
	if u.conn != nil {
		return u.conn.SetReadDeadline(t)
	}
	return nil
}

// SetWriteDeadline 在初始化前是空操作。
func (u *tunnelingWebsocketUpgraderConn) SetWriteDeadline(t time.Time) error {
	u.lock.RLock()
	defer u.lock.RUnlock()
	if u.conn != nil {
		return u.conn.SetWriteDeadline(t)
	}
	return nil
}

// noopAddr 是初始化前的占位地址，Network/String 均为空以表明「尚不可知」。
type noopAddr struct{}

func (n noopAddr) Network() string { return "" }
func (n noopAddr) String() string  { return "" }
