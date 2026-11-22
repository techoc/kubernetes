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

package filters

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/endpoints/responsewriter"
	"k8s.io/apiserver/pkg/server/httplog"
)

// WithTimeoutForNonLongRunningRequests times out non-long-running requests after the time given by timeout.
//
// 只对非长时请求生效：watch、exec、proxy 等长连接若被 60s 超时切断，功能会直接不可用。
// 超时由请求 context 的 deadline 驱动（上游的 WithRequestDeadline 设置），此处不再自设时长。
func WithTimeoutForNonLongRunningRequests(handler http.Handler, longRunning apirequest.LongRunningRequestCheck) http.Handler {
	if longRunning == nil {
		return handler
	}
	timeoutFunc := func(req *http.Request) (*http.Request, bool, func(), *apierrors.StatusError) {
		// TODO unify this with apiserver.MaxInFlightLimit
		ctx := req.Context()

		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if !ok {
			// if this happens, the handler chain isn't setup correctly because there is no request info
			return req, false, func() {}, apierrors.NewInternalError(fmt.Errorf("no request info found for request during timeout"))
		}

		if longRunning(req, requestInfo) {
			return req, true, nil, nil
		}

		// 超时返回 504，但请求终止指标仍要记录，否则耗时统计会缺一条。
		postTimeoutFn := func() {
			metrics.RecordRequestTermination(req, requestInfo, metrics.APIServerComponent, http.StatusGatewayTimeout)
		}
		return req, false, postTimeoutFn, apierrors.NewTimeoutError("request did not complete within the allotted timeout", 0)
	}
	return WithTimeout(handler, timeoutFunc)
}

// timeoutFunc 根据请求决定是否跳过超时控制（长时请求），并给出超时时的错误与收尾回调。
type timeoutFunc = func(*http.Request) (req *http.Request, longRunning bool, postTimeoutFunc func(), err *apierrors.StatusError)

// WithTimeout returns an http.Handler that runs h with a timeout
// determined by timeoutFunc. The new http.Handler calls h.ServeHTTP to handle
// each request, but if a call runs for longer than its time limit, the
// handler responds with a 504 Gateway Timeout error and the message
// provided. (If msg is empty, a suitable default message will be sent.) After
// the handler times out, writes by h to its http.ResponseWriter will return
// http.ErrHandlerTimeout. If timeoutFunc returns a nil timeout channel, no
// timeout will be enforced. recordFn is a function that will be invoked whenever
// a timeout happens.
func WithTimeout(h http.Handler, timeoutFunc timeoutFunc) http.Handler {
	return &timeoutHandler{h, timeoutFunc}
}

type timeoutHandler struct {
	handler http.Handler
	timeout timeoutFunc
}

// ServeHTTP 把实际处理放到独立 goroutine，主 goroutine 只等待“处理完成”或“context 到期”。
//
// 用 goroutine 而不是在同一个栈上等待，是因为 http.Handler 无法被中断；超时后
// 只能放弃等待并让 response writer 拒绝后续写入。
func (t *timeoutHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r, longRunning, postTimeoutFn, err := t.timeout(r)
	if longRunning {
		t.handler.ServeHTTP(w, r)
		return
	}

	timeoutCh := r.Context().Done()

	// resultCh is used as both errCh and stopCh
	resultCh := make(chan interface{})
	var tw timeoutWriter
	tw, w = newTimeoutWriter(w)

	// Make a copy of request and work on it in new goroutine
	// to avoid race condition when accessing/modifying request (e.g. headers)
	rCopy := r.Clone(r.Context())
	go func() {
		defer func() {
			err := recover()
			// do not wrap the sentinel ErrAbortHandler panic value
			if err != nil && err != http.ErrAbortHandler {
				// Same as stdlib http server code. Manually allocate stack
				// trace buffer size to prevent excessively large logs
				const size = 64 << 10
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				err = fmt.Sprintf("%v\n%s", err, buf)
			}
			// 无论正常返回还是 panic 都必须向 resultCh 发送，否则超时分支的接收方会永久阻塞。
			resultCh <- err
		}()
		t.handler.ServeHTTP(w, rCopy)
	}()
	select {
	case err := <-resultCh:
		// panic if error occurs; stop otherwise
		//
		// 把 panic 重新抛到处理请求的 goroutine 上，交给 http.Server 统一记录与断连，
		// 保持与未包装时一致的可观测行为。
		if err != nil {
			panic(err)
		}
		return
	case <-timeoutCh:
		defer func() {
			// resultCh needs to have a reader, since the function doing
			// the work needs to send to it. This is defer'd to ensure it runs
			// ever if the post timeout work itself panics.
			//
			// 后台 goroutine 稍后收尾：记录超时后处理器又运行了多久，用于发现“卡死”的 handler。
			go func() {
				timedOutAt := time.Now()
				res := <-resultCh

				status := metrics.PostTimeoutHandlerOK
				if res != nil {
					// a non nil res indicates that there was a panic.
					status = metrics.PostTimeoutHandlerPanic
				}

				metrics.RecordRequestPostTimeout(metrics.PostTimeoutSourceTimeoutHandler, status)
				utilruntime.HandleErrorWithContext(r.Context(), nil, "Post-timeout activity", "timeElapsed", time.Since(timedOutAt), "method", r.Method, "path", r.URL.Path, "result", res)
			}()
		}()
		// 超时后的 handler 栈已经不再有参考价值，关闭堆栈打印以免日志刷屏。
		httplog.SetStacktracePredicate(r.Context(), func(status int) bool {
			return false
		})
		defer postTimeoutFn()
		tw.timeout(err)
	}
}

// timeoutWriter 在保留 http.ResponseWriter 语义的同时增加超时控制能力。
type timeoutWriter interface {
	http.ResponseWriter
	timeout(*apierrors.StatusError)
}

// newTimeoutWriter 返回内部 writer（用于触发超时）与外层包装（交给 handler 写入）。
func newTimeoutWriter(w http.ResponseWriter) (timeoutWriter, http.ResponseWriter) {
	base := &baseTimeoutWriter{w: w, handlerHeaders: w.Header().Clone()}
	wrapped := responsewriter.WrapForHTTP1Or2(base)

	return base, wrapped
}

var _ http.ResponseWriter = &baseTimeoutWriter{}
var _ responsewriter.UserProvidedDecorator = &baseTimeoutWriter{}

// baseTimeoutWriter 缓冲 handler 写的 header，直到第一次真正写入才提交给底层 writer。
//
// 这层缓冲是超时能返回规范 504 JSON 的前提：如果 handler 一开始就把 header 刷出去，
// 超时后就无法再改状态码，只能断开连接。
type baseTimeoutWriter struct {
	w http.ResponseWriter

	// headers written by the normal handler
	handlerHeaders http.Header

	mu sync.Mutex
	// if the timeout handler has timeout
	timedOut bool
	// if this timeout writer has wrote header
	wroteHeader bool
	// if this timeout writer has been hijacked
	hijacked bool
}

func (tw *baseTimeoutWriter) Unwrap() http.ResponseWriter {
	return tw.w
}

// Header 在超时后返回一个丢弃用的空 header，避免 handler 继续改写已提交的响应。
func (tw *baseTimeoutWriter) Header() http.Header {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut {
		return http.Header{}
	}

	return tw.handlerHeaders
}

// Write 在超时后返回 http.ErrHandlerTimeout，让 handler 能感知到自己已被放弃。
func (tw *baseTimeoutWriter) Write(p []byte) (int, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut {
		return 0, http.ErrHandlerTimeout
	}
	if tw.hijacked {
		return 0, http.ErrHijacked
	}

	if !tw.wroteHeader {
		copyHeaders(tw.w.Header(), tw.handlerHeaders)
		tw.wroteHeader = true
	}
	return tw.w.Write(p)
}

// Flush 在超时后变为空操作，防止把已中止的响应推给客户端。
func (tw *baseTimeoutWriter) Flush() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut {
		return
	}

	// the outer ResponseWriter object returned by WrapForHTTP1Or2 implements
	// http.Flusher if the inner object (tw.w) implements http.Flusher.
	tw.w.(http.Flusher).Flush()
}

// WriteHeader 只在第一次调用时生效，与 net/http 的语义一致。
func (tw *baseTimeoutWriter) WriteHeader(code int) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut || tw.wroteHeader || tw.hijacked {
		return
	}

	copyHeaders(tw.w.Header(), tw.handlerHeaders)
	tw.wroteHeader = true
	tw.w.WriteHeader(code)
}

func copyHeaders(dst, src http.Header) {
	for k, v := range src {
		dst[k] = v
	}
}

// timeout 由超时分支调用，决定是返回 504 还是强制断开连接。
func (tw *baseTimeoutWriter) timeout(err *apierrors.StatusError) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	tw.timedOut = true

	// The timeout writer has not been used by the inner handler.
	// We can safely timeout the HTTP request by sending by a timeout
	// handler
	if !tw.wroteHeader && !tw.hijacked {
		tw.w.WriteHeader(http.StatusGatewayTimeout)
		enc := json.NewEncoder(tw.w)
		enc.Encode(&err.ErrStatus)
	} else {
		// The timeout writer has been used by the inner handler. There is
		// no way to timeout the HTTP request at the point. We have to shutdown
		// the connection for HTTP1 or reset stream for HTTP2.
		//
		// Note from the golang's docs:
		// If ServeHTTP panics, the server (the caller of ServeHTTP) assumes
		// that the effect of the panic was isolated to the active request.
		// It recovers the panic, logs a stack trace to the server error log,
		// and either closes the network connection or sends an HTTP/2
		// RST_STREAM, depending on the HTTP protocol. To abort a handler so
		// the client sees an interrupted response but the server doesn't log
		// an error, panic with the value ErrAbortHandler.
		//
		// We are throwing http.ErrAbortHandler deliberately so that a client is notified and to suppress a not helpful stacktrace in the logs
		//
		// 已经写出 header 时无法再改状态码，唯一能让客户端知道“响应不完整”的办法就是断开连接。
		panic(http.ErrAbortHandler)
	}
}

// CloseNotify 超时后立即返回已关闭的 channel，通知 handler 连接已不可用。
func (tw *baseTimeoutWriter) CloseNotify() <-chan bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut {
		done := make(chan bool)
		close(done)
		return done
	}

	// the outer ResponseWriter object returned by WrapForHTTP1Or2 implements
	// http.CloseNotifier if the inner object (tw.w) implements http.CloseNotifier.
	return tw.w.(http.CloseNotifier).CloseNotify()
}

// Hijack 用于协议升级（如 exec 的 SPDY）。被 hijack 后超时控制失效，
// 因为底层连接已脱离 http.Server 管理。
func (tw *baseTimeoutWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut {
		return nil, nil, http.ErrHandlerTimeout
	}

	// the outer ResponseWriter object returned by WrapForHTTP1Or2 implements
	// http.Hijacker if the inner object (tw.w) implements http.Hijacker.
	conn, rw, err := tw.w.(http.Hijacker).Hijack()
	if err == nil {
		tw.hijacked = true
	}
	return conn, rw, err
}
