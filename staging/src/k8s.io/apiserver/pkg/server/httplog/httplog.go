/*
Copyright 2014 The Kubernetes Authors.

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

package httplog

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/endpoints/responsewriter"
	"k8s.io/apiserver/pkg/server/routine"
	"k8s.io/klog/v2"
)

// StacktracePred returns true if a stacktrace should be logged for this status.
type StacktracePred func(httpStatus int) (logStacktrace bool)

// ShouldLogRequestPred returns true if logging should be enabled for this request
type ShouldLogRequestPred func() bool

type logger interface {
	Addf(format string, data ...interface{})
}

type respLoggerContextKeyType int

// respLoggerContextKey 用于在请求 context 中存储 respLogger 指针。
// 使用私有类型作为 key，避免与其它包在 context 中存入的字符串键冲突。
const respLoggerContextKey respLoggerContextKeyType = iota

// 在 ResponseWriter 之上加一层封装，以便跟踪耗时与错误信息来源。
// 包装 ResponseWriter 的核心目的是记录状态码与耗时——标准库不暴露已写出的状态码。
//
// TODO：现在已在使用 go-restful，理论上不再需要包装 http.ResponseWriter——
// 我们可以从 go-restful 中恢复 panic，这层包装的日志价值存疑。
//
// 并发注意：请求超时时，其他 goroutine 仍可能向 addedInfo 等字段追加内容，
// 因此这些字段的读写都由 mutex 保护；而 status/statusRecorded 只在请求处理路径写。
type respLogger struct {
	hijacked       bool
	statusRecorded bool
	status         int
	statusStack    string
	// mutex is used when accessing addedInfo, addedKeyValuePairs and logStacktracePred.
	// They can be modified by other goroutine when logging happens (in case of request timeout)
	mutex              sync.Mutex
	addedInfo          strings.Builder
	addedKeyValuePairs []interface{}
	startTime          time.Time

	captureErrorOutput bool

	req         *http.Request
	userAgent   string
	contentType string
	w           http.ResponseWriter

	logStacktracePred StacktracePred
}

var _ http.ResponseWriter = &respLogger{}
var _ responsewriter.UserProvidedDecorator = &respLogger{}

// Unwrap 暴露底层 ResponseWriter，使 WrapForHTTP1Or2 能识别 Flusher/Hijacker 等能力。
func (rl *respLogger) Unwrap() http.ResponseWriter {
	return rl.w
}

// Simple logger that logs immediately when Addf is called
type passthroughLogger struct{}

// Addf logs info immediately.
func (passthroughLogger) Addf(format string, data ...interface{}) {
	klog.V(2).Info(fmt.Sprintf(format, data...))
}

// DefaultStacktracePred 是 StacktracePred 的默认实现：
// 只对 1xx/5xx 记录栈，排除 101（协议切换）——WebSocket 升级成功不是错误，
// 记录栈只会制造噪音。
func DefaultStacktracePred(status int) bool {
	return (status < http.StatusOK || status >= http.StatusInternalServerError) && status != http.StatusSwitchingProtocols
}

const withLoggingLevel = 3

// WithLogging 用日志功能包装 handler。
// 默认仅在 -v=3 及以上启用，避免每个请求都产生日志。
// 通过闭包而非常量判断，使运行期日志级别变化能立即生效。
func WithLogging(handler http.Handler, pred StacktracePred) http.Handler {
	return withLogging(handler, pred, func() bool {
		return klog.V(withLoggingLevel).Enabled()
	})
}

// withLogging 是 WithLogging 的可测试版本：日志开关由调用方注入。
// 若上层用 WithRoutine 把处理放到独立 goroutine，ServeHTTP 返回时
// 请求可能尚未真正完成；此时把 rl.Log 追加为该 goroutine 的收尾任务，
// 保证日志在响应完成后才输出，且不会重复输出（logFunc 置 nil）。
func withLogging(handler http.Handler, stackTracePred StacktracePred, shouldLogRequest ShouldLogRequestPred) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !shouldLogRequest() {
			handler.ServeHTTP(w, req)
			return
		}

		ctx := req.Context()
		// 重复包装会导致日志重复且耗时统计失真，直接 panic 暴露装配错误。
		if old := respLoggerFromRequest(req); old != nil {
			panic("multiple WithLogging calls!")
		}

		// 优先使用接收时间戳（由更外层过滤器记录），使 latency 覆盖完整处理链路，
		// 而不是只统计本过滤器内部的耗时。
		startTime := time.Now()
		if receivedTimestamp, ok := request.ReceivedTimestampFrom(ctx); ok {
			startTime = receivedTimestamp
		}

		rl := newLoggedWithStartTime(req, w, startTime)
		rl.StacktraceWhen(stackTracePred)
		req = req.WithContext(context.WithValue(ctx, respLoggerContextKey, rl))

		var logFunc func()
		logFunc = rl.Log
		defer func() {
			if logFunc != nil {
				logFunc()
			}
		}()

		w = responsewriter.WrapForHTTP1Or2(rl)
		handler.ServeHTTP(w, req)

		// We need to ensure that the request is logged after it is processed.
		// In case the request is executed in a separate goroutine created via
		// WithRoutine handler in the handler chain (i.e. above handler.ServeHTTP()
		// would return request is completely responsed), we want the logging to
		// happen in that goroutine too, so we append it to the task.
		if routine.AppendTask(ctx, &routine.Task{Func: rl.Log}) {
			logFunc = nil
		}
	})
}

// respLoggerFromContext returns the respLogger or nil.
func respLoggerFromContext(ctx context.Context) *respLogger {
	val := ctx.Value(respLoggerContextKey)
	if rl, ok := val.(*respLogger); ok {
		return rl
	}
	return nil
}

func respLoggerFromRequest(req *http.Request) *respLogger {
	return respLoggerFromContext(req.Context())
}

// newLoggedWithStartTime 构造 logger 并快照 User-Agent/Content-Type。
// 头部在此时复制，是因为请求超时后其他 goroutine 可能修改请求头，
// 延迟到 Log 时再读取会与写者产生 map 并发读写竞态。
func newLoggedWithStartTime(req *http.Request, w http.ResponseWriter, startTime time.Time) *respLogger {
	logger := &respLogger{
		startTime:         startTime,
		req:               req,
		userAgent:         req.UserAgent(),
		contentType:       req.Header.Get("Content-Type"),
		w:                 w,
		logStacktracePred: DefaultStacktracePred,
	}
	return logger
}

// newLogged turns a normal response writer into a logged response writer.
func newLogged(req *http.Request, w http.ResponseWriter) *respLogger {
	return newLoggedWithStartTime(req, w, time.Now())
}

// LogOf 返回隐藏在 w 中的 logger；若不存在已有 logger，则创建 passthroughLogger，
// 后者在 Addf 被调用时立即输出到 stdout。
// 返回 passthroughLogger 而非 nil，让调用方无需判空即可安全调用 Addf。
func LogOf(req *http.Request, w http.ResponseWriter) logger {
	if rl := respLoggerFromRequest(req); rl != nil {
		return rl
	}
	return &passthroughLogger{}
}

// Unlogged 返回原始 ResponseWriter；若 w 不是我们插入的 logger 则原样返回 w。
// 某些需要直接控制连接的处理器（如流式代理）必须拿到未被包装的 writer，
// 否则 Hijack/Flush 语义会被中间层改变。
func Unlogged(req *http.Request, w http.ResponseWriter) http.ResponseWriter {
	if rl := respLoggerFromRequest(req); rl != nil {
		return rl.w
	}
	return w
}

// StacktraceWhen sets the stacktrace logging predicate, which decides when to log a stacktrace.
// There's a default, so you don't need to call this unless you don't like the default.
func (rl *respLogger) StacktraceWhen(pred StacktracePred) *respLogger {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	rl.logStacktracePred = pred
	return rl
}

// StatusIsNot returns a StacktracePred which will cause stacktraces to be logged
// for any status *not* in the given list.
func StatusIsNot(statuses ...int) StacktracePred {
	statusesNoTrace := map[int]bool{}
	for _, s := range statuses {
		statusesNoTrace[s] = true
	}
	return func(status int) bool {
		_, ok := statusesNoTrace[status]
		return !ok
	}
}

// Addf adds additional data to be logged with this request.
func (rl *respLogger) Addf(format string, data ...interface{}) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	rl.addedInfo.WriteString(fmt.Sprintf(format, data...))
}

// AddInfof 向请求关联的 logger 追加自由文本；无 logger 时静默忽略。
func AddInfof(ctx context.Context, format string, data ...interface{}) {
	if rl := respLoggerFromContext(ctx); rl != nil {
		rl.Addf(format, data...)
	}
}

// AddKeyValue 追加结构化键值对；写入顺序即输出顺序。
func (rl *respLogger) AddKeyValue(key string, value interface{}) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	rl.addedKeyValuePairs = append(rl.addedKeyValuePairs, key, value)
}

// AddKeyValue adds a (key, value) pair to the httplog associated
// with the request.
// Use this function if you want your data to show up in httplog
// in a more structured and readable way.
func AddKeyValue(ctx context.Context, key string, value interface{}) {
	if rl := respLoggerFromContext(ctx); rl != nil {
		rl.AddKeyValue(key, value)
	}
}

// SetStacktracePredicate sets a custom stacktrace predicate for the
// logger associated with the given request context.
func SetStacktracePredicate(ctx context.Context, pred StacktracePred) {
	if rl := respLoggerFromContext(ctx); rl != nil {
		rl.StacktraceWhen(pred)
	}
}

// Log 设计为在请求处理器结束时通过 defer 调用一次。
// 在持有 mutex 的情况下读取并输出，保证 addedKeyValuePairs/addedInfo
// 不会与其他 goroutine 的写入交错；延迟在此时才计算，覆盖完整处理时间。
func (rl *respLogger) Log() {
	latency := time.Since(rl.startTime)
	auditID := audit.GetAuditIDTruncated(rl.req.Context())
	verb := metrics.NormalizedVerb(rl.req)

	keysAndValues := []interface{}{
		"verb", verb,
		"URI", rl.req.RequestURI,
		"latency", latency,
		// We can't get UserAgent from rl.req.UserAgent() here as it accesses headers map,
		// which can be modified in another goroutine when apiserver request times out.
		// For example authentication filter modifies request's headers,
		// This can cause apiserver to crash with unrecoverable fatal error.
		// More info about concurrent read and write for maps: https://golang.org/doc/go1.6#runtime
		"userAgent", rl.userAgent,
		"contentType", rl.contentType,
		"audit-ID", auditID,
		"srcIP", rl.req.RemoteAddr,
	}
	// Lock for accessing addedKeyValuePairs and addedInfo
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	keysAndValues = append(keysAndValues, rl.addedKeyValuePairs...)

	if rl.hijacked {
		// 连接已被接管（如 WebSocket/SPDY），没有常规 HTTP 响应状态可言。
		keysAndValues = append(keysAndValues, "hijacked", true)
	} else {
		keysAndValues = append(keysAndValues, "resp", rl.status)
		if len(rl.statusStack) > 0 {
			keysAndValues = append(keysAndValues, "statusStack", rl.statusStack)
		}
		info := rl.addedInfo.String()
		if len(info) > 0 {
			keysAndValues = append(keysAndValues, "addedInfo", info)
		}
	}

	klog.V(withLoggingLevel).InfoSDepth(1, "HTTP", keysAndValues...)
}

// Header implements http.ResponseWriter.
func (rl *respLogger) Header() http.Header {
	return rl.w.Header()
}

// Write 实现 http.ResponseWriter 接口。
// 若 WriteHeader 未被显式调用，首次 Write 隐式记录 200，
// 与 net/http 的行为一致；否则日志中的状态码会是零值。
func (rl *respLogger) Write(b []byte) (int, error) {
	if !rl.statusRecorded {
		rl.recordStatus(http.StatusOK) // Default if WriteHeader hasn't been called
	}
	if rl.captureErrorOutput {
		rl.Addf("logging error output: %q\n", string(b))
	}
	return rl.w.Write(b)
}

// WriteHeader implements http.ResponseWriter.
func (rl *respLogger) WriteHeader(status int) {
	rl.recordStatus(status)
	rl.w.WriteHeader(status)
}

// Hijack 标记连接已被接管并透传到底层 writer。
// 只有底层实现 http.Hijacker 时才能调用，否则类型断言会 panic——
// 这是有意为之，说明包装链装配有误。
func (rl *respLogger) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	rl.hijacked = true

	// the outer ResponseWriter object returned by WrapForHTTP1Or2 implements
	// http.Hijacker if the inner object (rl.w) implements http.Hijacker.
	return rl.w.(http.Hijacker).Hijack()
}

// recordStatus 记录状态码，并按谓词决定是否捕获栈与错误输出。
// 栈与后续的响应体捕获绑定——只有在需要栈时才有必要把错误响应体
// 一并记入日志，否则会平白放大日志体积。
func (rl *respLogger) recordStatus(status int) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	rl.status = status
	rl.statusRecorded = true
	if rl.logStacktracePred(status) {
		// Only log stacks for errors
		stack := make([]byte, 50*1024)
		stack = stack[:runtime.Stack(stack, false)]
		rl.statusStack = "\n" + string(stack)
		rl.captureErrorOutput = true
	} else {
		rl.statusStack = ""
	}
}
