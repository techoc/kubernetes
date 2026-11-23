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
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/streaming/pkg/runtime"
)

// 客户端在 Upgrade 头里必须声明这个版本；服务端只认 SPDY/3.1，
// 不做版本协商，声明别的版本一律按 upgrade 失败处理。
const HeaderSpdy31 = "SPDY/3.1"

// responseUpgrader knows how to upgrade HTTP responses. It
// implements the httpstream.ResponseUpgrader interface.
// 结构体只携带 pingPeriod：是否发送 Ping 是建连时的一次性决策，之后不再改。
type responseUpgrader struct {
	pingPeriod time.Duration
}

// connWrapper is used to wrap a hijacked connection and its bufio.Reader. All
// calls will be handled directly by the underlying net.Conn with the exception
// of Read and Close calls, which will consider data in the bufio.Reader. This
// ensures that data already inside the used bufio.Reader instance is also
// read.
// 必须包这一层：Hijack 返回的 bufio.Reader 里可能已经预读了客户端发来的 SPDY 帧，
// 直接把裸 conn 交给 spdystream 会丢掉这些字节，握手就卡住了。
type connWrapper struct {
	net.Conn
	closed    int32
	bufReader *bufio.Reader
}

// 底层 conn 关闭后 bufio.Reader 里可能仍有预读数据；先看 closed 标志直接返回 EOF，
// 免得 spdystream 在已经关闭的连接上继续等下一帧。
func (w *connWrapper) Read(b []byte) (n int, err error) {
	if atomic.LoadInt32(&w.closed) == 1 {
		return 0, io.EOF
	}
	return w.bufReader.Read(b)
}

// 用 atomic 而不是 mutex：关闭标志只会被并发的 Read 观察到，
// 读写本身已经由底层 conn 和 bufio.Reader 各自同步，不必再加锁。
func (w *connWrapper) Close() error {
	err := w.Conn.Close()
	atomic.StoreInt32(&w.closed, 1)
	return err
}

// NewResponseUpgrader returns a new httpstream.ResponseUpgrader that is
// capable of upgrading HTTP responses using SPDY/3.1 via the
// spdystream package.
// 默认不发 Ping；只有明确需要绕过 LB 空闲超时时，调用方才用 WithPings 版本。
func NewResponseUpgrader() httpstream.ResponseUpgrader {
	return NewResponseUpgraderWithPings(0)
}

// NewResponseUpgraderWithPings returns a new httpstream.ResponseUpgrader that
// is capable of upgrading HTTP responses using SPDY/3.1 via the spdystream
// package.
//
// If pingPeriod is non-zero, for each incoming connection a background
// goroutine will send periodic Ping frames to the server. Use this to keep
// idle connections through certain load balancers alive longer.
// 注意注释里写的是“向 server 发 Ping”，但服务端升连后实际上由底层
// spdystream 自行回 Pong；这里的 Ping 周期只在本端作为发起方时生效。
func NewResponseUpgraderWithPings(pingPeriod time.Duration) httpstream.ResponseUpgrader {
	return responseUpgrader{pingPeriod: pingPeriod}
}

// UpgradeResponse upgrades an HTTP response to one that supports multiplexed
// streams. newStreamHandler will be called synchronously whenever the
// other end of the upgraded connection creates a new stream.
// 返回 nil 表示升级失败且已经向客户端写了错误响应；调用方不应再拿它去建连。
func (u responseUpgrader) UpgradeResponse(w http.ResponseWriter, req *http.Request, newStreamHandler httpstream.NewStreamHandler) httpstream.Connection {
	connectionHeader := strings.ToLower(req.Header.Get(httpstream.HeaderConnection))
	upgradeHeader := strings.ToLower(req.Header.Get(httpstream.HeaderUpgrade))
	// 用 Contains 而非等号：Connection 头允许逗号分隔多个 token，Upgrade 头也可能带多个协议。
	// 两边都转小写再比，避免大小写差异导致误判。
	if !strings.Contains(connectionHeader, strings.ToLower(httpstream.HeaderUpgrade)) || !strings.Contains(upgradeHeader, strings.ToLower(HeaderSpdy31)) {
		errorMsg := fmt.Sprintf("unable to upgrade: missing upgrade headers in request: %#v", req.Header)
		http.Error(w, errorMsg, http.StatusBadRequest)
		return nil
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		// 包装过或未实现 Hijacker 的 ResponseWriter 无法交出底层 TCP 连接，
		// 没有裸连接就谈不上跑 SPDY，这里直接报错而不是降级成普通 HTTP。
		errorMsg := "unable to upgrade: unable to hijack response"
		http.Error(w, errorMsg, http.StatusInternalServerError)
		return nil
	}

	w.Header().Add(httpstream.HeaderConnection, httpstream.HeaderUpgrade)
	w.Header().Add(httpstream.HeaderUpgrade, HeaderSpdy31)
	w.WriteHeader(http.StatusSwitchingProtocols)

	// 先写 101 再 Hijack：Hijack 之后 net/http 不再负责写响应，
	// 所以握手头必须在交出连接前就位。
	conn, bufrw, err := hijacker.Hijack()
	if err != nil {
		runtime.HandleErrorWithContext(req.Context(), err, "Unable to upgrade: error hijacking response")
		return nil
	}

	connWithBuf := &connWrapper{Conn: conn, bufReader: bufrw.Reader}
	// 把带缓冲的连接交给 SPDY，别丢掉 Hijack 之前预读的字节。
	spdyConn, err := NewServerConnectionWithPings(connWithBuf, newStreamHandler, u.pingPeriod)
	if err != nil {
		runtime.HandleErrorWithContext(req.Context(), err, "Unable to upgrade: error creating SPDY server connection")
		return nil
	}

	return spdyConn
}
