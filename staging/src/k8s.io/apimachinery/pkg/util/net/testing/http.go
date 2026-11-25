/*
Copyright 2023 The Kubernetes Authors.

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

// nettesting 包提供网络功能测试工具。
// 不要在生产代码中使用这些工具，因为它们尚未经过安全审查。
package nettesting

import (
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"sync"
	"testing"
)

// NewHTTPProxyHandler 创建 HTTPProxyHandler，并接受可选 hook；
// hook 在处理器早期调用以导出请求状态，返回 false 时处理器立即以服务器错误返回。
// 确保仅在测试中使用，此代码尚未经过安全审查。
//
// 中文补充：hook 在反向代理转发之前调用，可用于断言请求（如 HTTPS_PROXY 是否生效）；
// 返回 false 会直接中断请求，便于模拟代理失败。
func NewHTTPProxyHandler(t testing.TB, hook func(req *http.Request) bool) *HTTPProxyHandler {
	h := &HTTPProxyHandler{
		hook: hook,
		httpProxy: httputil.ReverseProxy{
			Director: func(req *http.Request) {
				// 普通 HTTP 代理请求的 URL 已是绝对地址，这里把目标改写为请求的 Host。
				req.URL.Scheme = "http"
				req.URL.Host = req.Host
			},
		},
		t: t,
	}
	return h
}

// HTTPProxyHandler 实现处理 http_proxy 和 https_proxy 请求的简单处理器，供测试使用。
type HTTPProxyHandler struct {
	// handlerDone 跟踪进行中的请求，Wait 用于避免测试结束时仍有 goroutine 运行。
	handlerDone sync.WaitGroup
	hook        func(r *http.Request) bool
	//httpProxy 是处理标准 HTTP 代理请求的反向代理。
	httpProxy httputil.ReverseProxy
	t         testing.TB
}

// ServeHTTP 处理 HTTP 代理请求。
// 中文补充：普通 HTTP 请求走 ReverseProxy；HTTPS 通过 CONNECT 建立隧道后手动双向复制。
func (h *HTTPProxyHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	h.handlerDone.Add(1)
	defer h.handlerDone.Done()

	if h.hook != nil {
		if ok := h.hook(req); !ok {
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	// 打印请求内容仅用于测试诊断，失败不影响代理。
	b, err := httputil.DumpRequest(req, false)
	if err != nil {
		h.t.Logf("Failed to dump request, host=%s: %v", req.Host, err)
	} else {
		h.t.Logf("Proxy Request: %s", string(b))
	}

	if req.Method != http.MethodConnect {
		h.httpProxy.ServeHTTP(rw, req)
		return
	}

	//CONNECT 代理
	// CONNECT 隧道：先连后端，再劫持客户端连接，然后双向拷贝字节流。

	sconn, err := net.Dial("tcp", req.Host)
	if err != nil {
		h.t.Logf("Failed to dial proxy backend, host=%s: %v", req.Host, err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer sconn.Close()

	hj, ok := rw.(http.Hijacker)
	if !ok {
		h.t.Logf("Can't switch protocols using non-Hijacker ResponseWriter: type=%T, host=%s", rw, req.Host)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 必须在 Hijack 之前写出 200，客户端收到后才开始隧道传输。
	rw.WriteHeader(http.StatusOK)

	conn, brw, err := hj.Hijack()
	if err != nil {
		h.t.Logf("Failed to hijack client connection, host=%s: %v", req.Host, err)
		return
	}
	defer conn.Close()

	// 把 Hijack 前缓冲的写数据刷给客户端。
	if err := brw.Flush(); err != nil {
		h.t.Logf("Failed to flush pending writes to client, host=%s: %v", req.Host, err)
		return
	}
	// 把 Hijack 前缓冲的读数据转给后端，避免丢失客户端抢先发送的字节。
	if _, err := io.Copy(sconn, io.LimitReader(brw, int64(brw.Reader.Buffered()))); err != nil {
		h.t.Logf("Failed to flush buffered reads to server, host=%s: %v", req.Host, err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// 两个方向各自阻塞到对端关闭；任一方向结束不影响另一方向，故需等两者都退出。
	go func() {
		defer wg.Done()
		defer h.t.Logf("Server read close, host=%s", req.Host)
		io.Copy(conn, sconn)
	}()
	go func() {
		defer wg.Done()
		defer h.t.Logf("Server write close, host=%s", req.Host)
		io.Copy(sconn, conn)
	}()

	wg.Wait()
	h.t.Logf("Done handling CONNECT request, host=%s", req.Host)
}

// Wait 阻塞到所有已进入 ServeHTTP 的请求处理完毕。
func (h *HTTPProxyHandler) Wait() {
	h.handlerDone.Wait()
}
