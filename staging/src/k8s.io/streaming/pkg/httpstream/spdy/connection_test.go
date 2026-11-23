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

package spdy

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/spdystream"
	"k8s.io/streaming/pkg/httpstream"
)

// 用一对 io.Copy 搭一个纯转发代理，模拟真实链路上的中间节点。
// 端口通过 channel 回传，避免测试里硬编码监听地址。
func runProxy(t *testing.T, backendUrl string, proxyUrl chan<- string, proxyDone chan<- struct{}, errCh chan<- error) {
	listener, err := net.Listen("tcp4", "localhost:0")
	if err != nil {
		errCh <- err
		return
	}
	defer listener.Close()

	proxyUrl <- listener.Addr().String()

	clientConn, err := listener.Accept()
	if err != nil {
		t.Errorf("proxy: error accepting client connection: %v", err)
		return
	}

	backendConn, err := net.Dial("tcp4", backendUrl)
	if err != nil {
		t.Errorf("proxy: error dialing backend: %v", err)
		return
	}
	defer backendConn.Close()

	// 双向转发各占一个 goroutine，等待两者都退出才算代理任务完成。
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(backendConn, clientConn)
	}()

	go func() {
		defer wg.Done()
		io.Copy(clientConn, backendConn)
	}()

	wg.Wait()

	proxyDone <- struct{}{}
}

// 服务端在收到流并看到应答帧发出后主动读一次，确认对端关闭能及时传播过来。
func runServer(t *testing.T, backendUrl chan<- string, serverDone chan<- struct{}, errCh chan<- error) {
	listener, err := net.Listen("tcp4", "localhost:0")
	if err != nil {
		errCh <- err
		return
	}
	defer listener.Close()

	backendUrl <- listener.Addr().String()

	conn, err := listener.Accept()
	if err != nil {
		t.Errorf("server: error accepting connection: %v", err)
		return
	}

	streamChan := make(chan httpstream.Stream)
	replySentChan := make(chan (<-chan struct{}))
	spdyConn, err := NewServerConnection(conn, func(stream httpstream.Stream, replySent <-chan struct{}) error {
		streamChan <- stream
		replySentChan <- replySent
		return nil
	})
	if err != nil {
		t.Errorf("server: error creating spdy connection: %v", err)
		return
	}

	stream := <-streamChan
	replySent := <-replySentChan
	// 等应答帧真正发出再读，保证读取结果反映的是对端关闭而不是时序巧合。
	<-replySent

	buf := make([]byte, 1)
	_, err = stream.Read(buf)
	if err != io.EOF {
		t.Errorf("server: unexpected read error: %v", err)
		return
	}

	// 再等 spdystream 层确认底层连接已撤掉，避免只看到应用层关闭。
	<-spdyConn.CloseChan()
	raw := spdyConn.(*connection).conn
	if err := raw.Wait(15 * time.Second); err != nil {
		t.Errorf("server: timed out waiting for connection closure: %v", err)
	}

	serverDone <- struct{}{}
}

// 回归测试：客户端关闭连接后，经代理转发的链路两端都应立刻感知，而不是等超时。
func TestConnectionCloseIsImmediateThroughAProxy(t *testing.T) {
	errCh := make(chan error)

	// 缓冲为 1，便于两个 goroutine 各自通知完成而不互相阻塞。
	serverDone := make(chan struct{}, 1)
	backendUrlChan := make(chan string)
	go runServer(t, backendUrlChan, serverDone, errCh)

	var backendUrl string
	select {
	case err := <-errCh:
		t.Fatalf("server: error listening: %v", err)
	case backendUrl = <-backendUrlChan:
	}

	proxyDone := make(chan struct{}, 1)
	proxyUrlChan := make(chan string)
	go runProxy(t, backendUrl, proxyUrlChan, proxyDone, errCh)

	var proxyUrl string
	select {
	case err := <-errCh:
		t.Fatalf("error listening: %v", err)
	case proxyUrl = <-proxyUrlChan:
	}

	conn, err := net.Dial("tcp4", proxyUrl)
	if err != nil {
		t.Fatalf("client: error connecting to proxy: %v", err)
	}

	spdyConn, err := NewClientConnection(conn)
	if err != nil {
		t.Fatalf("client: error creating spdy connection: %v", err)
	}

	// 先建一条流，让关闭行为与实际使用路径一致。
	if _, err := spdyConn.CreateStream(http.Header{}); err != nil {
		t.Fatalf("client: error creating stream: %v", err)
	}

	spdyConn.Close()
	raw := spdyConn.(*connection).conn
	if err := raw.Wait(15 * time.Second); err != nil {
		t.Fatalf("client: timed out waiting for connection closure: %v", err)
	}

	// 用总超时兜底，避免关闭传播失效时测试永久挂住。
	expired := time.NewTimer(15 * time.Second)
	defer expired.Stop()
	i := 0
	for {
		select {
		case <-expired.C:
			t.Fatalf("timed out waiting for proxy and/or server closure")
		case <-serverDone:
			i++
		case <-proxyDone:
			i++
		}
		if i == 2 {
			break
		}
	}
}

// 验证保活 Ping 双向发送，且不会干扰正常流上的数据读写。
func TestConnectionPings(t *testing.T) {
	// 周期设短些，让测试在数秒内就能观察到足够多的 Ping。
	const pingPeriod = 10 * time.Millisecond
	timeout := time.After(10 * time.Second)

	// Set up server connection.
	listener, err := net.Listen("tcp4", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	srvErr := make(chan error, 1)
	go func() {
		defer close(srvErr)

		srvConn, err := listener.Accept()
		if err != nil {
			srvErr <- fmt.Errorf("server: error accepting connection: %v", err)
			return
		}
		defer srvConn.Close()

		spdyConn, err := spdystream.NewConnection(srvConn, true)
		if err != nil {
			srvErr <- fmt.Errorf("server: error creating spdy connection: %v", err)
			return
		}

		// 原子计数，因为 Ping 回调与断言分别在不同 goroutine 中执行。
		var pingsSent int64
		srvSPDYConn := newConnection(
			spdyConn,
			func(stream httpstream.Stream, replySent <-chan struct{}) error {
				// Echo all the incoming data.
				go io.Copy(stream, stream)
				return nil
			},
			pingPeriod,
			func() (time.Duration, error) {
				// 返回零延迟由 Ping 循环自行按周期调度，这里只关心次数。
				atomic.AddInt64(&pingsSent, 1)
				return 0, nil
			})
		defer srvSPDYConn.Close()

		// Wait for the connection to close, to prevent defers from running
		// early.
		select {
		case <-timeout:
			srvErr <- fmt.Errorf("server: timeout waiting for connection to close")
			return
		case <-srvSPDYConn.CloseChan():
		}

		// Count pings sent by the server.
		gotPings := atomic.LoadInt64(&pingsSent)
		if gotPings < 1 {
			t.Errorf("server: failed to send any pings (check logs)")
		}
	}()

	// Set up client connection.
	clConn, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatalf("client: error connecting to proxy: %v", err)
	}
	defer clConn.Close()
	clSPDYConn, err := NewClientConnection(clConn)
	if err != nil {
		t.Fatalf("client: error creating spdy connection: %v", err)
	}
	defer clSPDYConn.Close()
	start := time.Now()
	clSPDYStream, err := clSPDYConn.CreateStream(http.Header{})
	if err != nil {
		t.Fatalf("client: error creating stream: %v", err)
	}
	defer clSPDYStream.Close()

	// Send some data both ways, to make sure pings don't interfere with
	// regular messages.
	in := "foo"
	if _, err := fmt.Fprintln(clSPDYStream, in); err != nil {
		t.Fatalf("client: error writing data to stream: %v", err)
	}
	var out string
	if _, err := fmt.Fscanln(clSPDYStream, &out); err != nil {
		t.Fatalf("client: error reading data from stream: %v", err)
	}
	if in != out {
		t.Errorf("client: received data doesn't match sent data: got %q, want %q", out, in)
	}

	// Wait for at least 2 pings to get sent each way before closing the
	// connection.
	elapsed := time.Since(start)
	if elapsed < 3*pingPeriod {
		time.Sleep(3*pingPeriod - elapsed)
	}
	clSPDYConn.Close()

	select {
	case err, ok := <-srvErr:
		if ok && err != nil {
			t.Error(err)
		}
	case <-timeout:
		t.Errorf("timed out waiting for server to exit")
	}
}

// 只实现 Stream 接口所需的桩，用于验证流的登记/移除逻辑而不牵涉真实网络。
type fakeStream struct{ id uint32 }

func (*fakeStream) Read(p []byte) (int, error)  { return 0, nil }
func (*fakeStream) Write(p []byte) (int, error) { return 0, nil }
func (*fakeStream) Close() error                { return nil }
func (*fakeStream) Reset() error                { return nil }
func (*fakeStream) Headers() http.Header        { return nil }
func (f *fakeStream) Identifier() uint32        { return f.id }

// 覆盖 RemoveStreams 的边界：不存在的流、批量移除、以及 nil 入参都不能引发 panic。
func TestConnectionRemoveStreams(t *testing.T) {
	c := &connection{streams: make(map[uint32]httpstream.Stream)}
	stream0 := &fakeStream{id: 0}
	stream1 := &fakeStream{id: 1}
	stream2 := &fakeStream{id: 2}

	c.registerStream(stream0)
	c.registerStream(stream1)

	if len(c.streams) != 2 {
		t.Fatalf("should have two streams, has %d", len(c.streams))
	}

	// not exists
	c.RemoveStreams(stream2)

	if len(c.streams) != 2 {
		t.Fatalf("should have two streams, has %d", len(c.streams))
	}

	// remove all existing
	c.RemoveStreams(stream0, stream1)

	// remove nil stream should not crash
	c.RemoveStreams(nil)

	if len(c.streams) != 0 {
		t.Fatalf("should not have any streams, has %d", len(c.streams))
	}

}
