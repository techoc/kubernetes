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

// WithTimeoutForNonLongRunningRequests 在给定时间后让非长时运行请求超时。
//
// 只对非长时请求生效：watch、exec、proxy 等长连接若被 60s 超时切断，功能会直接不可用。
// 超时由请求 context 的 deadline 驱动（上游的 WithRequestDeadline 设置），此处不再自设时长。
func WithTimeoutForNonLongRunningRequests(handler http.Handler, longRunning apirequest.LongRunningRequestCheck) http.Handler {
	if longRunning == nil {
		return handler
	}
	timeoutFunc := func(req *http.Request) (*http.Request, bool, func(), *apierrors.StatusError) {
		// TODO 与 apiserver.MaxInFlightLimit 统一
		ctx := req.Context()

		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if !ok {
			// 若发生这种情况，说明 handler 链装配有误——没有 request info
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

// WithTimeout 返回一个 http.Handler，它用 timeoutFunc 决定的超时时间运行 h。
// 新的 http.Handler 调用 h.ServeHTTP 处理每个请求，但若执行时间
// 超过限制，handler 会以 504 Gateway Timeout 错误及给定的消息
// 响应。（msg 为空时发送合适的默认消息。）超时之后，
// h 对其 http.ResponseWriter 的写入将返回
// http.ErrHandlerTimeout。若 timeoutFunc 返回 nil 超时 channel，则不
// 强制超时。recordFn 在每次超时时被调用。
//
// 强制超时。recordFn 在每次超时时被调用。
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

	// resultCh 同时用作 errCh 与 stopCh
	resultCh := make(chan interface{})
	var tw timeoutWriter
	tw, w = newTimeoutWriter(w)

	// 拷贝 request 并在新 goroutine 中处理，
	// 避免访问/修改 request（如 header）时发生竞态
	rCopy := r.Clone(r.Context())
	go func() {
		defer func() {
			err := recover()
			// 不包装哨兵值 ErrAbortHandler
			if err != nil && err != http.ErrAbortHandler {
				// 与标准库 http server 代码一致。手动指定栈
				// trace 缓冲区大小，避免日志过大
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
		// 发生错误则 panic，否则停止
		//
		// 把 panic 重新抛到处理请求的 goroutine 上，交给 http.Server 统一记录与断连，
		// 保持与未包装时一致的可观测行为。
		if err != nil {
			panic(err)
		}
		return
	case <-timeoutCh:
		defer func() {
			// resultCh 必须有读者，因为执行工作的函数
			// 需要向它发送结果。用 defer 确保即使超时后的处理本身 panic，
			// 也仍会执行。
			//
			// 后台 goroutine 稍后收尾：记录超时后处理器又运行了多久，用于发现“卡死”的 handler。
			go func() {
				timedOutAt := time.Now()
				res := <-resultCh

				status := metrics.PostTimeoutHandlerOK
				if res != nil {
					// res 非 nil 表示发生了 panic。
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

	// 正常 handler 写入的 header
	handlerHeaders http.Header

	mu sync.Mutex
	// 超时 handler 是否已超时
	timedOut bool
	// 该 timeout writer 是否已写入 header
	wroteHeader bool
	// 该 timeout writer 是否已被劫持
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

	// 当内部对象（tw.w）实现 http.Flusher 时，
	// WrapForHTTP1Or2 返回的外层 ResponseWriter 也实现 http.Flusher。
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

	// 内部 handler 尚未使用 timeout writer。
	// 此时可安全地用超时 handler 让
	// HTTP 请求超时。
	if !tw.wroteHeader && !tw.hijacked {
		tw.w.WriteHeader(http.StatusGatewayTimeout)
		enc := json.NewEncoder(tw.w)
		enc.Encode(&err.ErrStatus)
	} else {
		// 内部 handler 已使用 timeout writer。此时
		// 无法再让 HTTP 请求超时，只能关闭 HTTP1 连接
		// 或重置 HTTP2 流。
		//
		// Go 文档说明：
		// 若 ServeHTTP panic，服务器（ServeHTTP 的调用方）会认为
		// panic 的影响仅限当前活动请求。
		// 它会恢复 panic，向服务器错误日志记录栈跟踪，
		// 并根据 HTTP 协议关闭网络连接或发送 HTTP/2
		// RST_STREAM。若要中止 handler，使客户端看到响应中断但服务器不记录
		// 错误，应使用 ErrAbortHandler 作为 panic 值。
		//
		// 特意抛出 http.ErrAbortHandler，既让客户端得到通知，又避免日志中出现无用的栈跟踪
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

	// 当内部对象（tw.w）实现 http.CloseNotifier 时，
	// WrapForHTTP1Or2 返回的外层 ResponseWriter 也实现 http.CloseNotifier。
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

	// 当内部对象（tw.w）实现 http.Hijacker 时，
	// WrapForHTTP1Or2 返回的外层 ResponseWriter 也实现 http.Hijacker。
	conn, rw, err := tw.w.(http.Hijacker).Hijack()
	if err == nil {
		tw.hijacked = true
	}
	return conn, rw, err
}
