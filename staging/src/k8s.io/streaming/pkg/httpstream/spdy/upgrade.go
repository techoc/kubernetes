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

// responseUpgrader 通过 spdystream 将 HTTP 响应升级为支持多路复用的连接。
// 结构体只携带 pingPeriod：是否发送 Ping 是建连时的一次性决策，之后不再改。
type responseUpgrader struct {
	pingPeriod time.Duration
}

// connWrapper 包装 Hijack 返回的连接及其 bufio.Reader。除 Read 和 Close 外，
// 其他调用直接透传给底层 net.Conn；读写和关闭时还必须考虑 Reader 中已预读的数据，
// 否则这些字节会丢失。
type connWrapper struct {
	net.Conn
	closed    int32
	bufReader *bufio.Reader
}

// Read 在连接关闭后直接返回 EOF，避免 spdystream 继续等待下一帧。
func (w *connWrapper) Read(b []byte) (n int, err error) {
	if atomic.LoadInt32(&w.closed) == 1 {
		return 0, io.EOF
	}
	return w.bufReader.Read(b)
}

// Close 用 atomic 发布关闭状态，供并发 Read 观察，避免额外加锁。
func (w *connWrapper) Close() error {
	err := w.Conn.Close()
	atomic.StoreInt32(&w.closed, 1)
	return err
}

// NewResponseUpgrader 创建通过 spdystream 使用 SPDY/3.1 升级 HTTP 响应的 ResponseUpgrader。
// 默认不发 Ping；只有明确需要避免负载均衡器回收空闲连接时，才使用 WithPings 版本。
func NewResponseUpgrader() httpstream.ResponseUpgrader {
	return NewResponseUpgraderWithPings(0)
}

// NewResponseUpgraderWithPings 创建通过 spdystream 使用 SPDY/3.1 升级 HTTP 响应的 ResponseUpgrader。
// pingPeriod 非零时，每条传入连接都会启动后台 goroutine 定期发送 Ping，
// 用于避免某些负载均衡器过早回收空闲连接。
func NewResponseUpgraderWithPings(pingPeriod time.Duration) httpstream.ResponseUpgrader {
	return responseUpgrader{pingPeriod: pingPeriod}
}

// UpgradeResponse 将 HTTP 响应升级为支持多路复用的连接。对端创建新流时，
// newStreamHandler 会被同步调用。升级失败时返回 nil，调用方不能继续用它建流。
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
