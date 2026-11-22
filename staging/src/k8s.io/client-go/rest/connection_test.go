/*
Copyright 2019 The Kubernetes Authors.

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

package rest

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
)

// tcpLB 是一个极简的 TCP 负载均衡/代理，用来在测试中插入可切断的中间连接，
// 从而模拟"TCP 连接已损坏但未被正确关闭"的场景。dials 用于统计已接受的连接数。
type tcpLB struct {
	t         *testing.T
	ln        net.Listener
	serverURL string
	dials     int32
}

// handleConnection 在客户端与真实服务端之间双向转发数据，直到 stopCh 关闭后断开，
// 从而制造连接被"半关闭"的效果。
func (lb *tcpLB) handleConnection(in net.Conn, stopCh chan struct{}) {
	out, err := net.Dial("tcp", lb.serverURL)
	if err != nil {
		lb.t.Log(err)
		return
	}
	go io.Copy(out, in)
	go io.Copy(in, out)
	<-stopCh
	if err := out.Close(); err != nil {
		lb.t.Fatalf("failed to close connection: %v", err)
	}
}

// serve 接受一个连接并增加 dials 计数；每次只处理一个连接，便于精确控制连接代数。
func (lb *tcpLB) serve(stopCh chan struct{}) {
	conn, err := lb.ln.Accept()
	if err != nil {
		lb.t.Fatalf("failed to accept: %v", err)
	}
	atomic.AddInt32(&lb.dials, 1)
	go lb.handleConnection(conn, stopCh)
}

// newLB 在本地随机端口上监听，serverURL 为真实后端地址。
func newLB(t *testing.T, serverURL string) *tcpLB {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind: %v", err)
	}
	lb := tcpLB{
		serverURL: serverURL,
		ln:        ln,
		t:         t,
	}
	return &lb
}

const (
	readIdleTimeout int = 1
	pingTimeout     int = 1
)

// TestReconnectBrokenTCP 验证 HTTP/2 健康检查能发现并剔除已损坏的连接，
// 使得后续请求不会复用坏连接，而是新建连接（dials 从 1 变为 2）。
func TestReconnectBrokenTCP(t *testing.T) {
	// 缩短读空闲与 ping 超时，避免测试等待过久；同时确保 HTTP/2 处于启用状态。
	t.Setenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS", strconv.Itoa(readIdleTimeout))
	t.Setenv("HTTP2_PING_TIMEOUT_SECONDS", strconv.Itoa(pingTimeout))
	t.Setenv("DISABLE_HTTP2", "")
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, %s", r.Proto)
	}))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("failed to parse URL from %q: %v", ts.URL, err)
	}
	lb := newLB(t, u.Host)
	defer lb.ln.Close()
	stopCh := make(chan struct{})
	go lb.serve(stopCh)
	transport, ok := ts.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("failed to assert *http.Transport")
	}
	config := &Config{
		Host:      "https://" + lb.ln.Addr().String(),
		Transport: utilnet.SetTransportDefaults(transport),
		Timeout:   1 * time.Second,
		// These fields are required to create a REST client.
		ContentConfig: ContentConfig{
			GroupVersion:         &schema.GroupVersion{},
			NegotiatedSerializer: &serializer.CodecFactory{},
		},
	}
	client, err := RESTClientFor(config)
	if err != nil {
		t.Fatalf("failed to create REST client: %v", err)
	}
	data, err := client.Get().AbsPath("/").DoRaw(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %s: %v", data, err)
	}
	if string(data) != "Hello, HTTP/2.0" {
		t.Fatalf("unexpected response: %s", data)
	}

	// Deliberately let the LB stop proxying traffic for the current
	// connection. This mimics a broken TCP connection that's not properly
	// closed.
	close(stopCh)

	stopCh = make(chan struct{})
	go lb.serve(stopCh)
	// Sleep enough time for the HTTP/2 health check to detect and close
	// the broken TCP connection.
	time.Sleep(time.Duration(1+readIdleTimeout+pingTimeout) * time.Second)
	// If the HTTP/2 health check were disabled, the broken connection
	// would still be in the connection pool, the following request would
	// then reuse the broken connection instead of creating a new one, and
	// thus would fail.
	data, err = client.Get().AbsPath("/").DoRaw(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if string(data) != "Hello, HTTP/2.0" {
		t.Fatalf("unexpected response: %s", data)
	}
	dials := atomic.LoadInt32(&lb.dials)
	if dials != 2 {
		t.Fatalf("expected %d dials, got %d", 2, dials)
	}
}

// TestReconnectBrokenTCP_HTTP1 验证 HTTP/1.1 场景：连接损坏后关闭空闲连接，客户端应重新拨号
// 而不是复用已损坏的连接。
//
// 1. connect to https server with http1.1 using a TCP proxy
// 2. the connection has keepalive enabled so it will be reused
// 3. break the TCP connection stopping the proxy
// 4. close the idle connection to force creating a new connection
// 5. count that there are 2 connections to the server (we didn't reuse the original connection)
func TestReconnectBrokenTCP_HTTP1(t *testing.T) {
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, %s", r.Proto)
	}))
	ts.EnableHTTP2 = false
	ts.StartTLS()
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("failed to parse URL from %q: %v", ts.URL, err)
	}
	lb := newLB(t, u.Host)
	defer lb.ln.Close()
	stopCh := make(chan struct{})
	go lb.serve(stopCh)
	transport, ok := ts.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatal("failed to assert *http.Transport")
	}
	config := &Config{
		Host:      "https://" + lb.ln.Addr().String(),
		Transport: utilnet.SetTransportDefaults(transport),
		// large timeout, otherwise the broken connection will be cleaned by it
		Timeout: wait.ForeverTestTimeout,
		// These fields are required to create a REST client.
		ContentConfig: ContentConfig{
			GroupVersion:         &schema.GroupVersion{},
			NegotiatedSerializer: &serializer.CodecFactory{},
		},
	}
	config.TLSClientConfig.NextProtos = []string{"http/1.1"}
	client, err := RESTClientFor(config)
	if err != nil {
		t.Fatalf("failed to create REST client: %v", err)
	}

	data, err := client.Get().AbsPath("/").DoRaw(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %s: %v", data, err)
	}
	if string(data) != "Hello, HTTP/1.1" {
		t.Fatalf("unexpected response: %s", data)
	}

	// Deliberately let the LB stop proxying traffic for the current
	// connection. This mimics a broken TCP connection that's not properly
	// closed.
	close(stopCh)

	stopCh = make(chan struct{})
	go lb.serve(stopCh)
	// Close the idle connections
	utilnet.CloseIdleConnectionsFor(client.Client.Transport)

	// If the client didn't close the idle connections, the broken connection
	// would still be in the connection pool, the following request would
	// then reuse the broken connection instead of creating a new one, and
	// thus would fail.
	data, err = client.Get().AbsPath("/").DoRaw(context.TODO())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if string(data) != "Hello, HTTP/1.1" {
		t.Fatalf("unexpected response: %s", data)
	}
	dials := atomic.LoadInt32(&lb.dials)
	if dials != 2 {
		t.Fatalf("expected %d dials, got %d", 2, dials)
	}
}

// TestReconnectBrokenTCPInFlight_HTTP1 验证存在在途请求且连接损坏时，新请求会另建连接，
// 而原来的在途请求最终只能超时失败。
//
// 1. connect to https server with http1.1 using a TCP proxy making the connection to timeout
// 2. the connection has keepalive enabled so it will be reused
// 3. close the in-flight connection to force creating a new connection
// 4. count that there are 2 connections on the LB but only one succeeds
func TestReconnectBrokenTCPInFlight_HTTP1(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	received := make(chan struct{})

	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hang" {
			conn, _, _ := w.(http.Hijacker).Hijack()
			close(received)
			<-done
			conn.Close()
		}
		fmt.Fprintf(w, "Hello, %s", r.Proto)
	}))
	ts.EnableHTTP2 = false
	ts.StartTLS()
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("failed to parse URL from %q: %v", ts.URL, err)
	}

	lb := newLB(t, u.Host)
	defer lb.ln.Close()
	stopCh := make(chan struct{})
	go lb.serve(stopCh)

	transport, ok := ts.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatal("failed to assert *http.Transport")
	}
	config := &Config{
		Host:      "https://" + lb.ln.Addr().String(),
		Transport: utilnet.SetTransportDefaults(transport),
		// Use something extraordinary large to not hit the timeout
		Timeout: wait.ForeverTestTimeout,
		// These fields are required to create a REST client.
		ContentConfig: ContentConfig{
			GroupVersion:         &schema.GroupVersion{},
			NegotiatedSerializer: &serializer.CodecFactory{},
		},
	}
	config.TLSClientConfig.NextProtos = []string{"http/1.1"}

	client, err := RESTClientFor(config)
	if err != nil {
		t.Fatalf("failed to create REST client: %v", err)
	}

	// The request will connect, hang and eventually time out
	// but we can use a context to close once the test is done
	// we are only interested in have an inflight connection
	ctx, cancel := context.WithCancel(context.Background())
	reqErrCh := make(chan error, 1)
	defer close(reqErrCh)
	go func() {
		_, err = client.Get().AbsPath("/hang").DoRaw(ctx)
		reqErrCh <- err
	}()

	// wait until it connect to the server
	select {
	case <-received:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("Test timed out waiting for first request to fail")
	}

	// Deliberately let the LB stop proxying traffic for the current
	// connection. This mimics a broken TCP connection that's not properly
	// closed.
	close(stopCh)

	stopCh = make(chan struct{})
	go lb.serve(stopCh)

	// New request will fail if tries to reuse the connection
	data, err := client.Get().AbsPath("/").DoRaw(context.Background())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if string(data) != "Hello, HTTP/1.1" {
		t.Fatalf("unexpected response: %s", data)
	}
	dials := atomic.LoadInt32(&lb.dials)
	if dials != 2 {
		t.Fatalf("expected %d dials, got %d", 2, dials)
	}

	// cancel the in-flight connection
	cancel()
	select {
	case <-reqErrCh:
		if err == nil {
			t.Fatal("Connection succeeded but was expected to timeout")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out waiting for the request to fail")
	}

}

// TestRestClientTimeout 验证 rest.Config.Timeout 会作用于整个请求，
// 服务端响应慢于超时时间时客户端应返回 deadline exceeded 错误。
func TestRestClientTimeout(t *testing.T) {
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		fmt.Fprintf(w, "Hello, %s", r.Proto)
	}))
	ts.Start()
	defer ts.Close()

	config := &Config{
		Host:    ts.URL,
		Timeout: 1 * time.Second,
		// These fields are required to create a REST client.
		ContentConfig: ContentConfig{
			GroupVersion:         &schema.GroupVersion{},
			NegotiatedSerializer: &serializer.CodecFactory{},
		},
	}
	client, err := RESTClientFor(config)
	if err != nil {
		t.Fatalf("failed to create REST client: %v", err)
	}
	_, err = client.Get().AbsPath("/").DoRaw(context.TODO())
	if err == nil {
		t.Fatalf("timeout error expected")
	}
	if !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("timeout error expected, received %v", err)
	}
}
