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

package net

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"

	"golang.org/x/net/http2"

	netutils "k8s.io/utils/net"
)

// getIPNet 解析 CIDR 并返回 *net.IPNet；解析失败时返回 nil。
func getIPNet(cidr string) *net.IPNet {
	_, ipnet, _ := netutils.ParseCIDRSloppy(cidr)
	return ipnet
}

// TestIPNetEqual 覆盖 IPNetEqual：掩码与网段都相同才算相等，
// 主机位不同（10.0.0.0/24 vs 10.0.0.1/24）仍视为同一网段。
func TestIPNetEqual(t *testing.T) {
	testCases := []struct {
		ipnet1 *net.IPNet
		ipnet2 *net.IPNet
		expect bool
	}{
		// nil 输入场景（解析 "" 得到 nil）。
		{
			getIPNet("10.0.0.1/24"),
			getIPNet(""),
			false,
		},
		{
			getIPNet("10.0.0.0/24"),
			getIPNet("10.0.0.0/24"),
			true,
		},
		{
			getIPNet("10.0.0.0/24"),
			getIPNet("10.0.0.1/24"),
			true,
		},
		{
			getIPNet("10.0.0.0/25"),
			getIPNet("10.0.0.0/24"),
			false,
		},
		{
			getIPNet("10.0.1.0/24"),
			getIPNet("10.0.0.0/24"),
			false,
		},
	}

	for _, tc := range testCases {
		if tc.expect != IPNetEqual(tc.ipnet1, tc.ipnet2) {
			t.Errorf("Expect equality of %s and %s be to %v", tc.ipnet1.String(), tc.ipnet2.String(), tc.expect)
		}
	}
}

// TestIsConnectionRefused 验证只有 ECONNREFUSED 被判为连接被拒绝，
// ECONNRESET 不算；嵌套在 os.SyscallError 中也能识别。
func TestIsConnectionRefused(t *testing.T) {
	testCases := []struct {
		err    error
		expect bool
	}{
		{
			&url.Error{Err: &net.OpError{Err: syscall.ECONNRESET}},
			false,
		},
		{
			&url.Error{Err: &net.OpError{Err: syscall.ECONNREFUSED}},
			true,
		},
		{&url.Error{Err: &net.OpError{Err: &os.SyscallError{Err: syscall.ECONNREFUSED}}},
			true,
		},
	}

	for _, tc := range testCases {
		if result := IsConnectionRefused(tc.err); result != tc.expect {
			t.Errorf("Expect to be %v, but actual is %v", tc.expect, result)
		}
	}
}

// tcpLB 是一个极简 TCP 代理，用于模拟“连接存在但对端停止转发”的故障。
type tcpLB struct {
	t         *testing.T
	ln        net.Listener
	serverURL string
}

// handleConnection 建立到后端的连接并双向复制数据，直到 stopCh 关闭。
// stopCh 关闭后只关闭后端连接，故意不关闭入站连接，以模拟半开/损坏的 TCP 连接。
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

// serve 接受一个连接并交给 handleConnection 处理。
func (lb *tcpLB) serve(stopCh chan struct{}) {
	conn, err := lb.ln.Accept()
	if err != nil {
		lb.t.Fatalf("failed to accept: %v", err)
	}
	lb.handleConnection(conn, stopCh)
}

// newLB 在 127.0.0.1 的随机端口上监听，返回指向 serverURL 的代理。
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

// TestIsConnectionReset 通过 HTTP/2 与 TCP 代理复现连接丢失：
// 先正常完成一次请求，然后让代理停止转发，确认第二次请求返回的错误
// 能被 IsHTTP2ConnectionLost 识别。
func TestIsConnectionReset(t *testing.T) {
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
	stopCh, stoppedCh := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stoppedCh)
		lb.serve(stopCh)
	}()

	c := ts.Client()
	transport, ok := ts.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("failed to assert *http.Transport")
	}
	t2, err := http2.ConfigureTransports(transport)
	if err != nil {
		t.Fatalf("failed to configure *http.Transport: %+v", err)
	}
	// 缩短空闲与 ping 超时，让连接丢失能快速暴露。
	t2.ReadIdleTimeout = time.Second
	t2.PingTimeout = time.Second
	resp, err := c.Get("https://" + lb.ln.Addr().String())
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
	if string(data) != "Hello, HTTP/2.0" {
		t.Fatalf("unexpected response: %s", data)
	}

	// 故意让代理停止为当前连接转发流量，模拟未被正确关闭的损坏 TCP 连接。
	close(stopCh)
	<-stoppedCh
	_, err = c.Get("https://" + lb.ln.Addr().String())
	if !IsHTTP2ConnectionLost(err) {
		t.Fatalf("expected HTTP2ConnectionLost error, got %v", err)
	}
}
