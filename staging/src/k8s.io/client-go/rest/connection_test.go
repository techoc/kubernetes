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
		// 这些字段是创建 REST 客户端的必填项。
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

	// 故意让 LB 停止代理当前连接的流量，模拟 TCP 连接损坏但未正确关闭的场景。
	close(stopCh)

	stopCh = make(chan struct{})
	go lb.serve(stopCh)
	// 等待足够长时间，让 HTTP/2 健康检查发现并关闭损坏的连接。
	time.Sleep(time.Duration(1+readIdleTimeout+pingTimeout) * time.Second)
	// 若禁用 HTTP/2 健康检查，损坏连接仍留在连接池中，后续请求会复用它而非新建，
	// 从而失败。
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
// 1) 通过 TCP 代理以 HTTP/1.1 连接 HTTPS 服务端；2) 连接启用 keepalive 因而会被复用；
// 3) 停止代理以损坏 TCP 连接；4) 关闭空闲连接强制新建；5) 断言服务端收到 2 次连接（未复用旧连接）。
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
		// 超时设得足够大，否则损坏连接会先被超时清理。
		Timeout: wait.ForeverTestTimeout,
		// 这些字段是创建 REST 客户端的必填项。
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

	// 故意让 LB 停止代理当前连接的流量，模拟 TCP 连接损坏但未正确关闭的场景。
	close(stopCh)

	stopCh = make(chan struct{})
	go lb.serve(stopCh)
	// 关闭空闲连接。
	utilnet.CloseIdleConnectionsFor(client.Client.Transport)

	// 若客户端不关闭空闲连接，损坏连接仍留在池中，后续请求会复用它而非新建，从而失败。
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
// 1) 通过 TCP 代理以 HTTP/1.1 连接 HTTPS 服务端并让连接超时；2) keepalive 使连接被复用；
// 3) 关闭在途连接强制新建；4) 断言 LB 上有 2 次连接但只有一次成功。
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
		// 超时设得极大，避免触发超时。
		Timeout: wait.ForeverTestTimeout,
		// 这些字段是创建 REST 客户端的必填项。
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

	// 该请求会连上后挂起直至超时；测试结束时用 context 关闭即可。这里只关心存在一个在途连接。
	ctx, cancel := context.WithCancel(context.Background())
	reqErrCh := make(chan error, 1)
	defer close(reqErrCh)
	go func() {
		_, err = client.Get().AbsPath("/hang").DoRaw(ctx)
		reqErrCh <- err
	}()

	// 等待其连接到服务端。
	select {
	case <-received:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("Test timed out waiting for first request to fail")
	}

	// 故意让 LB 停止代理当前连接的流量，模拟 TCP 连接损坏但未正确关闭的场景。
	close(stopCh)

	stopCh = make(chan struct{})
	go lb.serve(stopCh)

	// 新请求若复用该连接就会失败。
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

	// 取消在途请求。
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
		// 这些字段是创建 REST 客户端的必填项。
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
