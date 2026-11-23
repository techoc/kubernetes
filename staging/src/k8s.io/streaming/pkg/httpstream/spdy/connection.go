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

// connection maintains state about a spdystream.Connection and its associated
// streams.
// 额外维护一张 stream 表，是为了 Close 时能对所有在途 stream 统一 Reset；
// 只关底层 conn 会让对端悬挂在半开的 stream 上。
type connection struct {
	conn             *spdystream.Connection
	streams          map[uint32]httpstream.Stream
	streamLock       sync.Mutex
	newStreamHandler httpstream.NewStreamHandler
	ping             func() (time.Duration, error)
}

// NewClientConnection creates a new SPDY client connection.
// 不发 Ping 的快捷入口；需要保活时走 WithPings 版本。
func NewClientConnection(conn net.Conn) (httpstream.Connection, error) {
	return NewClientConnectionWithPings(conn, 0)
}

// NewClientConnectionWithPings creates a new SPDY client connection.
//
// If pingPeriod is non-zero, a background goroutine will send periodic Ping
// frames to the server. Use this to keep idle connections through certain load
// balancers alive longer.
// 客户端侧没有业务上的新 stream 回调，所以传 NoOpNewStreamHandler；
// 主动建流走 CreateStream，对端推送的流在这个角色里没有意义。
func NewClientConnectionWithPings(conn net.Conn, pingPeriod time.Duration) (httpstream.Connection, error) {
	spdyConn, err := spdystream.NewConnection(conn, false)
	if err != nil {
		// 握手失败时连接归本函数所有，调用方拿不到返回值，这里必须自己关掉。
		defer conn.Close()
		return nil, err
	}

	return newConnection(spdyConn, httpstream.NoOpNewStreamHandler, pingPeriod, spdyConn.Ping), nil
}

// NewServerConnection creates a new SPDY server connection. newStreamHandler
// will be invoked when the server receives a newly created stream from the
// client.
// newStreamHandler 由服务端提供，决定接受还是拒绝客户端推来的流。
func NewServerConnection(conn net.Conn, newStreamHandler httpstream.NewStreamHandler) (httpstream.Connection, error) {
	return NewServerConnectionWithPings(conn, newStreamHandler, 0)
}

// NewServerConnectionWithPings creates a new SPDY server connection.
// newStreamHandler will be invoked when the server receives a newly created
// stream from the client.
//
// If pingPeriod is non-zero, a background goroutine will send periodic Ping
// frames to the server. Use this to keep idle connections through certain load
// balancers alive longer.
// spdystream.NewConnection 的第二个参数为 true，表示本端扮演 server：
// 这会决定 SPDY 握手中的帧语义（server push / 流 ID 奇偶等），客户端侧传的是 false。
func NewServerConnectionWithPings(conn net.Conn, newStreamHandler httpstream.NewStreamHandler, pingPeriod time.Duration) (httpstream.Connection, error) {
	spdyConn, err := spdystream.NewConnection(conn, true)
	if err != nil {
		defer conn.Close()
		return nil, err
	}

	return newConnection(spdyConn, newStreamHandler, pingPeriod, spdyConn.Ping), nil
}

// newConnection returns a new connection wrapping conn. newStreamHandler
// will be invoked when the server receives a newly created stream from the
// client.
// Serve 和 sendPings 都在这里拉起；调用方拿到连接时，收帧循环已经在跑了。
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

// createStreamResponseTimeout indicates how long to wait for the other side to
// acknowledge the new stream before timing out.
// 只覆盖握手阶段（等 SYN_REPLY），不限制 stream 建立后的读写时长。
const createStreamResponseTimeout = 30 * time.Second

// Close first sends a reset for all of the connection's streams, and then
// closes the underlying spdystream.Connection.
func (c *connection) Close() error {
	c.streamLock.Lock()
	for _, s := range c.streams {
		// calling Reset instead of Close ensures that all streams are fully torn down
		// Reset 会立刻通知对端 RST_STREAM，Close 则可能走优雅关闭、一直等对端把数据读完。
		s.Reset()
	}
	// 清空表防止重复 Reset，也方便 GC 回收已经不再引用的 stream。
	c.streams = make(map[uint32]httpstream.Stream, 0)
	c.streamLock.Unlock()

	// now that all streams are fully torn down, it's safe to call close on the underlying connection,
	// which should be able to terminate immediately at this point, instead of waiting for any
	// remaining graceful stream termination.
	return c.conn.Close()
}

// RemoveStreams can be used to removes a set of streams from the Connection.
// 业务侧读完 stream 后调用，避免长连接上 stream 表无限增长；它不会关闭这些 stream。
func (c *connection) RemoveStreams(streams ...httpstream.Stream) {
	c.streamLock.Lock()
	for _, stream := range streams {
		// It may be possible that the provided stream is nil if timed out.
		if stream != nil {
			delete(c.streams, stream.Identifier())
		}
	}
	c.streamLock.Unlock()
}

// CreateStream creates a new stream with the specified headers and registers
// it with the connection.
// 先等对端确认再登记：未被确认的 stream 不该进入 c.streams，
// 否则 Close 时会向一个可能根本不存在于对端的流发 Reset。
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

// registerStream adds the stream s to the connection's list of streams that
// it owns.
// 由 CreateStream 和 newSpdyStream 两条路径调用，都必须持锁写表。
func (c *connection) registerStream(s httpstream.Stream) {
	c.streamLock.Lock()
	c.streams[s.Identifier()] = s
	c.streamLock.Unlock()
}

// CloseChan returns a channel that, when closed, indicates that the underlying
// spdystream.Connection has been closed.
// 直接透传底层通道：关闭事件由 spdystream 产生，这里没有额外状态要同步。
func (c *connection) CloseChan() <-chan bool {
	return c.conn.CloseChan()
}

// newSpdyStream is the internal new stream handler used by spdystream.Connection.Serve.
// It calls connection's newStreamHandler, giving it the opportunity to accept or reject
// the stream. If newStreamHandler returns an error, the stream is rejected. If not, the
// stream is accepted and registered with the connection.
// replySent 用来告诉上层处理器何时可以认为回复已经发出；必须先注册再关通道，
// 否则处理器一旦依赖该通道做清理，可能撞上 stream 还没登记进表的空窗。
func (c *connection) newSpdyStream(stream *spdystream.Stream) {
	replySent := make(chan struct{})
	err := c.newStreamHandler(stream, replySent)
	rejectStream := (err != nil)
	if rejectStream {
		//nolint:logcheck // Hopefully this never gets triggered.
		klog.Warningf("Stream rejected: %v", err)
		stream.Reset()
		return
	}

	c.registerStream(stream)
	stream.SendReply(http.Header{}, rejectStream)
	close(replySent)
}

// SetIdleTimeout sets the amount of time the connection may remain idle before
// it is automatically closed.
// 透传给 spdystream；这里的“idle”按帧活动计算，Ping 帧也会把计时器重置。
func (c *connection) SetIdleTimeout(timeout time.Duration) {
	c.conn.SetIdleTimeout(timeout)
}

// 用 CloseChan 而不是 defer t.Stop 之外的手段退出：连接一关，goroutine 立刻收工，
// 不必再空转一个 ticker。Ping 失败只记日志不退出，因为单次失败可能只是网络抖动。
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
			//nolint:logcheck // Hopefully this never gets triggered.
			klog.V(3).Infof("SPDY Ping failed: %v", err)
			// Continue, in case this is a transient failure.
			// c.conn.CloseChan above will tell us when the connection is
			// actually closed.
		}
	}
}
