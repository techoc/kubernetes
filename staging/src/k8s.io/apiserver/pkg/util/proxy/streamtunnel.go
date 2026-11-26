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

// TunnelingHandler 通过 WebSocket 隧道传输 SPDY。
type TunnelingHandler struct {
	// 用于上游 SPDY 与下游隧道之间的通信。
	upgradeHandler http.Handler
}

// NewTunnelingHandler 在上游 SPDY 连接与下游隧道连接之间建立隧道，
// 复用 UpgradeAwareProxy 的升级处理逻辑。
func NewTunnelingHandler(upgradeHandler http.Handler) *TunnelingHandler {
	return &TunnelingHandler{upgradeHandler: upgradeHandler}
}

// ServeHTTP 使用 upgradeHandler 在下游隧道连接与上游 SPDY 连接之间做隧道转发。
// 隧道连接是包装后的 WebSockets 连接，传输 SPDY 帧数据；若上游升级失败，
// 通信委派给传入的 "w" ResponseWriter。
// 客户端必须在 WebSocket 子协议中声明 SPDY 隧道协议，否则无法确定
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

	// 字段 w 与 conn 互斥：升级成功时 conn 被劫持并交给后续 upgradeHandler，
	//
	writer := &tunnelingResponseWriter{
		logger: logger,
		//
		w: w,
		//
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

// createSPDYRequest 修改传入的请求：移除 WebSockets 头部，添加 SPDY 升级信息，
// 包括客户端可接受的 spdy 协议。必须克隆请求——原请求仍被下游 WebSocket 升级
// 使用，直接修改会破坏它。请求体被丢弃：SPDY 升级请求本身无 body，保留它会让上游误读。
func createSPDYRequest(req *http.Request, spdyProtocols ...string) *http.Request {
	clone := utilnet.CloneRequest(req)
	// 从 HTTP 请求中清除 WebSocket 头。
	clone.Header.Del(wsstream.WebSocketProtocolHeader)
	clone.Header.Del("Sec-Websocket-Key")
	clone.Header.Del("Sec-Websocket-Version")
	clone.Header.Del(httpstream.HeaderUpgrade)
	// 为上游 SPDY 升级更新 HTTP 请求。
	clone.Method = "POST"
	clone.Body = nil // 移除未使用的请求体。
	clone.Header.Set(httpstream.HeaderUpgrade, spdy.HeaderSpdy31)
	clone.Header.Del(httpstream.HeaderProtocolVersion)
	for i := range spdyProtocols {
		clone.Header.Add(httpstream.HeaderProtocolVersion, spdyProtocols[i])
	}
	return clone
}

// spdyProtocolsFromWebsocketProtocols 先筛选出以 "SPDY/3.1+" 为前缀的 Kubernetes
// websocket 子协议，再去掉前缀得到 spdy 协议列表。只接受同时带 Kubernetes 前缀与
// 后缀的子协议，避免把无关的自定义子协议误当成 SPDY 版本传给上游。
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

// tunnelingResponseWriter 实现 http.ResponseWriter 与 http.Hijacker 接口，
// 只有非升级响应可以用 WriteHeader() 和 Write() 写入。一旦调用过 Write 或
// WriteHeader，Hijack 返回错误；一旦调用过 Hijack，Write、WriteHeader 与
// Hijack 均返回错误。状态机是单向的（written / hijacked 至多一个为真），
// 互斥锁保证并发安全。这种严格性让「升级成功」与「返回普通错误」两条路径
// 不会交叉，避免写出半截响应。
type tunnelingResponseWriter struct {
	logger klog.Logger
	// w 用于委托 Header()、WriteHeader() 与 Write() 调用
	w http.ResponseWriter
	// conn 由 Hijack() 返回
	conn net.Conn
	// mu 保护写操作
	mu sync.Mutex
	// wrote 记录是否已调用 WriteHeader 或 Write
	written bool
	// hijacked 记录是否已调用 Hijack
	hijacked bool
}

// Hijack 返回委托的 net.Conn。
// 若之前已调用 Write()、WriteHeader() 或 Hijack()，返回错误。
// 返回的 bufio.ReadWriter 始终为 nil。
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

// Header 委托给保存的 http.ResponseWriter。
func (w *tunnelingResponseWriter) Header() http.Header {
	return w.w.Header()
}

// Write 委托给保存的 http.ResponseWriter。
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

// WriteHeader 委派给保存的 "http.ResponseWriter"。101 必须经由 Hijack 后的
// 连接发出，直接 WriteHeader(101) 会绕过 WebSocket 握手逻辑；这里转换为 500，
// 让配置错误显式失败而非静默产生坏连接。
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
		// 101 升级响应必须经被劫持的连接发送，不能走 WriteHeader
		w.logger.Error(nil, "WriteHeader called with 101 upgrade")
		http.Error(w.w, "unexpected upgrade", http.StatusInternalServerError)
		return
	}

	// 无需转换的非升级响应直接透传
	w.w.WriteHeader(statusCode)
}

// headerInterceptingConn 包装隧道 "net.Conn"，先从上游 SPDY 连接读取并排空
// HTTP 响应的状态行与头部，再据此决定如何初始化用于写数据的委派连接。
// SPDY 客户端期望先读到完整的 HTTP 响应头，再开始读写帧；因此这里先缓冲并
// 解析头部，解析成功后才初始化真正的下游连接，并把剩余字节（可能已含 body）转发出去。
type headerInterceptingConn struct {
	logger klog.Logger
	// 所有 net.Conn 方法委托给 initializableConn。
	// 只有读取响应头并用其结果调用 initializableConn.InitializeWrite() 后，
	//
	initializableConn

	lock          sync.Mutex
	headerBuffer  []byte
	initialized   bool
	initializeErr error
}

// initializableConn 在首次 Write 前必须先初始化。
type initializableConn interface {
	net.Conn
	// 读到后端响应头时调用 InitializeWrite。
	// backendResponse 保存解析后的头。
	// backendResponseBytes 是解析头所用的原始字节。
	InitializeWrite(backendResponse *http.Response, backendResponseBytes []byte) error
}

const maxHeaderBytes = 1 << 20

// 正常头/体分隔符（\r\n\r\n，但 Go 容忍开头的 \r 缺失）
var lfCRLF = []byte("\n\r\n")

// 不含 \r 的头/体分隔符（Go 容忍）
var lfLF = []byte("\n\n")

// Write 先拦截并吞掉 HTTP 响应，待响应被完整读取并处理后，再委派给隧道的
// "net.Conn"。头部可能分多次 Write 到达，因此在未见分隔符前持续累积并返回成功；
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

	// 防止无界缓冲
	if len(h.headerBuffer)+len(b) > maxHeaderBytes {
		return 0, fmt.Errorf("header size limit exceeded")
	}

	// 累积到 headerBuffer
	h.headerBuffer = append(h.headerBuffer, b...)

	// 尝试解析 HTTP 响应头
	var headerBytes, bodyBytes []byte
	if i := bytes.Index(h.headerBuffer, lfCRLF); i != -1 {
		// 以 \n\r\n 结尾的头
		headerBytes = h.headerBuffer[0 : i+len(lfCRLF)]
		bodyBytes = h.headerBuffer[i+len(lfCRLF):]
	} else if i := bytes.Index(h.headerBuffer, lfLF); i != -1 {
		// 以 \n\n 结尾的头（Go 容忍）
		headerBytes = h.headerBuffer[0 : i+len(lfLF)]
		bodyBytes = h.headerBuffer[i+len(lfLF):]
	} else {
		// 尚未收到完整的头
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
	// req 是用于升级的 WebSocket 请求
	req *http.Request
	// w 是 WebSocket writer，用于升级与写错误响应
	w http.ResponseWriter

	// lock 保护 conn 与 err
	lock sync.RWMutex
	// conn 非 nil 表示 InitializeWrite 成功
	conn net.Conn
	// err 非 nil 表示 InitializeWrite 失败，或在它之前调用了 Close
	err error
}

// InitializeWrite 根据后端响应决定下游连接的形态：
//   - 101：校验 Upgrade/Connection 头后，升级为 WebSocket 并包装成隧道连接；
//   - 其他状态：把原始响应字节回放到 hijack 得到的连接上，透传给客户端。
//
// 错误路径会关闭可能已打开的连接，避免泄漏；升级后不再向 ResponseWriter
// 写错误（此时 HTTP 响应已提交，再写会产生无效数据）。
func (u *tunnelingWebsocketUpgraderConn) InitializeWrite(backendResponse *http.Response, backendResponseBytes []byte) (err error) {
	// 出错时确保关闭自己打开的连接
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

		// 握手时把 server 选定的 SPDY 协议转换为隧道 WebSocket 协议
		var serverWebsocketProtocols []string
		if backendSPDYProtocol := strings.TrimSpace(backendResponse.Header.Get(httpstream.HeaderProtocolVersion)); backendSPDYProtocol != "" {
			serverWebsocketProtocols = []string{constants.WebsocketsSPDYTunnelingPrefix + backendSPDYProtocol}
		} else {
			serverWebsocketProtocols = []string{}
		}

		// 尝试升级 WebSocket 连接。
		// 此后不再需要向响应写错误。
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

	// 非升级请求一律透传后端响应
	logger.Error(nil, "SPDY upgrade failed", "status", backendResponse.Status)
	metrics.IncStreamTunnelRequest(context.Background(), strconv.Itoa(backendResponse.StatusCode))

	// 尝试劫持连接
	conn, _, err = u.w.(http.Hijacker).Hijack()
	if err != nil {
		logger.Error(err, "Unable to hijack response")
		u.err = err
		return u.err
	}
	// 把后端响应的原始字节回放到 hijack 得到的连接上。设置写超时，避免客户端
	// 不读取时永久阻塞；回放的是原始字节，保证状态行、头部与 body 与后端完全一致。
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
	// 初始化完成前返回空读而不阻塞
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
	// 记录已关闭，避免再次写入或初始化
	u.err = fmt.Errorf("connection closed")
	// 写响应
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
