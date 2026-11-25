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
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/moby/spdystream"
	"k8s.io/klog/v2"
	"k8s.io/streaming/pkg/httpstream"
)

// connection 维护 spdystream.Connection 及其流的状态。
// 额外维护一张 stream 表，是为了 Close 时能对所有在途 stream 统一 Reset；
// 只关底层 conn 会让对端悬挂在半开的 stream 上。
type connection struct {
	conn             *spdystream.Connection
	streams          map[uint32]httpstream.Stream
	streamLock       sync.Mutex
	newStreamHandler httpstream.NewStreamHandler
	ping             func() (time.Duration, error)
}

// NewClientConnection 创建 SPDY 客户端连接，不发送保活 Ping。
func NewClientConnection(conn net.Conn) (httpstream.Connection, error) {
	return NewClientConnectionWithPings(conn, 0)
}

// NewClientConnectionWithPings 创建 SPDY 客户端连接。
// pingPeriod 非零时，后台 goroutine 会定期向服务端发送 Ping，避免空闲连接被过早回收。
// 客户端使用 NoOpNewStreamHandler，主动建流通过 CreateStream 完成。
func NewClientConnectionWithPings(conn net.Conn, pingPeriod time.Duration) (httpstream.Connection, error) {
	spdyConn, err := spdystream.NewConnection(conn, false)
	if err != nil {
		// 握手失败时连接归本函数所有，调用方拿不到返回值，这里必须自己关掉。
		defer conn.Close()
		return nil, err
	}

	return newConnection(spdyConn, httpstream.NoOpNewStreamHandler, pingPeriod, spdyConn.Ping), nil
}

// NewServerConnection 创建 SPDY 服务端连接。收到客户端新建的流时调用 newStreamHandler，
// 由它决定接受还是拒绝该流。
func NewServerConnection(conn net.Conn, newStreamHandler httpstream.NewStreamHandler) (httpstream.Connection, error) {
	return NewServerConnectionWithPings(conn, newStreamHandler, 0)
}

// NewServerConnectionWithPings 创建 SPDY 服务端连接。收到客户端新建的流时调用 newStreamHandler。
// pingPeriod 非零时，后台 goroutine 会定期发送 Ping，避免空闲连接被过早回收。
// spdystream.NewConnection 的第二个参数为 true，表示本端扮演服务端，决定握手中的帧语义。
func NewServerConnectionWithPings(conn net.Conn, newStreamHandler httpstream.NewStreamHandler, pingPeriod time.Duration) (httpstream.Connection, error) {
	spdyConn, err := spdystream.NewConnection(conn, true)
	if err != nil {
		defer conn.Close()
		return nil, err
	}

	return newConnection(spdyConn, newStreamHandler, pingPeriod, spdyConn.Ping), nil
}

// newConnection 包装底层连接，并启动收帧循环；配置了 Ping 时同时启动保活循环。
// 服务端收到客户端新建的流时调用 newStreamHandler。
func newConnection(conn *spdystream.Connection, newStreamHandler httpstream.NewStreamHandler, pingPeriod time.Duration, pingFn func() (time.Duration, error)) httpstream.Connection {
	c := &connection{
		conn:             conn,
		newStreamHandler: newStreamHandler,
		ping:             pingFn,
		streams:          make(map[uint32]httpstream.Stream),
	}
	go conn.Serve(c.newSpdyStream)
	if pingPeriod > 0 && pingFn != nil {
		go c.sendPings(pingPeriod)
	}
	return c
}

// createStreamResponseTimeout 是等待对端确认新流的超时时间，只覆盖建流握手阶段。
const createStreamResponseTimeout = 30 * time.Second

// Close 先 Reset 所有流，再关闭底层连接，避免对端的流一直处于半开状态。
func (c *connection) Close() error {
	c.streamLock.Lock()
	for _, s := range c.streams {
		// Reset 会立刻通知对端 RST_STREAM；Close 可能等待对端读完数据，不适合这里。
		s.Reset()
	}
	// 清空表防止重复 Reset，也方便 GC 回收已经不再引用的 stream。
	c.streams = make(map[uint32]httpstream.Stream, 0)
	c.streamLock.Unlock()

	// 所有流都已拆除，底层连接可以立即关闭，无需等待优雅终止流程。
	return c.conn.Close()
}

// RemoveStreams 从连接维护的表中移除指定流，但不会关闭这些流。
func (c *connection) RemoveStreams(streams ...httpstream.Stream) {
	c.streamLock.Lock()
	for _, stream := range streams {
		// 建流超时时调用方可能传入 nil。
		if stream != nil {
			delete(c.streams, stream.Identifier())
		}
	}
	c.streamLock.Unlock()
}

// CreateStream 使用指定请求头创建流，并在收到对端确认后将其登记到连接。
// 未确认的流不能登记，否则 Close 可能向对端不存在的流发送 Reset。
func (c *connection) CreateStream(headers http.Header) (httpstream.Stream, error) {
	stream, err := c.conn.CreateStream(headers, nil, false)
	if err != nil {
		return nil, err
	}
	if err = stream.WaitTimeout(createStreamResponseTimeout); err != nil {
		return nil, err
	}

	c.registerStream(stream)
	return stream, nil
}

// registerStream 将流登记到连接维护的表中，内部持锁保护并发访问。
func (c *connection) registerStream(s httpstream.Stream) {
	c.streamLock.Lock()
	c.streams[s.Identifier()] = s
	c.streamLock.Unlock()
}

// CloseChan 返回底层 spdystream.Connection 的关闭通知通道。
func (c *connection) CloseChan() <-chan bool {
	return c.conn.CloseChan()
}

// newSpdyStream 是 spdystream.Connection.Serve 使用的新流处理器。
// 它调用业务回调决定接受或拒绝流，并在接受后将流登记到连接。
// replySent 表示回复已发送；必须先登记再关闭通道，避免依赖该通道的清理逻辑观察到空窗。
func (c *connection) newSpdyStream(stream *spdystream.Stream) {
	replySent := make(chan struct{})
	err := c.newStreamHandler(stream, replySent)
	rejectStream := (err != nil)
	if rejectStream {
		//nolint:logcheck // 正常情况下不会拒绝流，保留日志便于排查意外错误。
		klog.Warningf("Stream rejected: %v", err)
		stream.Reset()
		return
	}

	c.registerStream(stream)
	stream.SendReply(http.Header{}, rejectStream)
	close(replySent)
}

// SetIdleTimeout 设置连接自动关闭前的空闲时长；Ping 等帧活动也会重置空闲计时器。
func (c *connection) SetIdleTimeout(timeout time.Duration) {
	c.conn.SetIdleTimeout(timeout)
}

// sendPings 定期发送保活 Ping，并在连接关闭时退出。
// 单次 Ping 失败只记日志，因为可能是瞬时网络问题；真正关闭时由 CloseChan 通知。
func (c *connection) sendPings(period time.Duration) {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-c.conn.CloseChan():
			return
		case <-t.C:
		}
		if _, err := c.ping(); err != nil {
			//nolint:logcheck // 正常情况下不会失败，保留日志便于排查。
			klog.V(3).Infof("SPDY Ping failed: %v", err)
			// 瞬时失败后继续重试，连接真正关闭时由 CloseChan 通知。
		}
	}
}
