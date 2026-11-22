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

// Package runtime 提供进程级的 panic/错误处理原语（HandleCrash、HandleError），
// 用于在无法向上返回错误的 goroutine 中统一记录与兜底，并提供 Must、
// GetCaller 等小工具。
package runtime

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/klog/v2/textlogger"
)

var (
	// ReallyCrash controls the behavior of HandleCrash and defaults to
	// true. It's exposed so components can optionally set to false
	// to restore prior behavior. This flag is mostly used for tests to validate
	// crash conditions.
	//
	// 中文说明：仅用于测试；生产代码不应把它设为 false，否则真正的 panic
	// 会被吞掉。
	ReallyCrash = true
)

// PanicHandlers is a list of functions which will be invoked when a panic happens.
//
// The code invoking these handlers prepares a contextual logger so that
// klog.FromContext(ctx) already skips over the panic handler itself and
// several other intermediate functions, ideally such that the log output
// is attributed to the code which triggered the panic.
//
// 中文说明：这是全局可替换的钩子，测试或组件可以追加/替换处理器；默认
// 处理器 logPanic 会打印调用栈。
var PanicHandlers = []func(context.Context, interface{}){logPanic}

// HandleCrash simply catches a crash and logs an error. Meant to be called via
// defer.  Additional context-specific handlers can be provided, and will be
// called in case of panic.  HandleCrash actually crashes, after calling the
// handlers and logging the panic message.
//
// E.g., you can provide one or more additional handlers for something like shutting down go routines gracefully.
//
// Contextual logging: HandleCrashWithContext or HandleCrashWithLogger should be used instead of HandleCrash in code which supports contextual logging.
//
// 中文说明：在非上下文场景中调用；内部把无 ctx 的处理器适配成带 ctx 的
// 形式后复用统一实现，保证调用栈深度一致。
func HandleCrash(additionalHandlers ...func(interface{})) {
	if r := recover(); r != nil {
		additionalHandlersWithContext := make([]func(context.Context, interface{}), len(additionalHandlers))
		for i, handler := range additionalHandlers {
			additionalHandlersWithContext[i] = func(_ context.Context, r interface{}) {
				handler(r)
			}
		}

		handleCrash(context.Background(), r, additionalHandlersWithContext...)
	}
}

// HandleCrashWithContext simply catches a crash and logs an error. Meant to be called via
// defer.  Additional context-specific handlers can be provided, and will be
// called in case of panic.  HandleCrash actually crashes, after calling the
// handlers and logging the panic message.
//
// E.g., you can provide one or more additional handlers for something like shutting down go routines gracefully.
//
// The context is used to determine how to log.
// 中文说明：上下文中的 logger 决定日志输出位置，适合请求级代码。
func HandleCrashWithContext(ctx context.Context, additionalHandlers ...func(context.Context, interface{})) {
	if r := recover(); r != nil {
		handleCrash(ctx, r, additionalHandlers...)
	}
}

// HandleCrashWithLogger simply catches a crash and logs an error. Meant to be called via
// defer.  Additional context-specific handlers can be provided, and will be
// called in case of panic.  HandleCrash actually crashes, after calling the
// handlers and logging the panic message.
//
// E.g., you can provide one or more additional handlers for something like shutting down go routines gracefully.
//
// 中文说明：已有 logger 但无 ctx 时使用；内部把 logger 放进新的 ctx。
func HandleCrashWithLogger(logger klog.Logger, additionalHandlers ...func(context.Context, interface{})) {
	if r := recover(); r != nil {
		ctx := klog.NewContext(context.Background(), logger)
		handleCrash(ctx, r, additionalHandlers...)
	}
}

// handleCrash is the common implementation of the HandleCrash* variants.
// Having those call a common implementation ensures that the stack depth
// is the same regardless through which path the handlers get invoked.
//
// 中文说明：ReallyCrash 为 false 时仅记录不重新 panic，便于测试验证
// 崩溃路径；生产环境默认继续 panic 以保留 fast-fail 语义。
func handleCrash(ctx context.Context, r any, additionalHandlers ...func(context.Context, interface{})) {
	// We don't really know how many call frames to skip because the Go
	// panic handler is between us and the code where the panic occurred.
	// If it's one function (as in Go 1.21), then skipping four levels
	// gets us to the function which called the `defer HandleCrashWithontext(...)`.
	logger := klog.FromContext(ctx).WithCallDepth(4)
	ctx = klog.NewContext(ctx, logger)

	for _, fn := range PanicHandlers {
		fn(ctx, r)
	}
	for _, fn := range additionalHandlers {
		fn(ctx, r)
	}
	if ReallyCrash {
		// Actually proceed to panic.
		panic(r)
	}
}

// logPanic logs the caller tree when a panic occurs (except in the special case of http.ErrAbortHandler).
func logPanic(ctx context.Context, r interface{}) {
	if r == http.ErrAbortHandler {
		// honor the http.ErrAbortHandler sentinel panic value:
		//   ErrAbortHandler is a sentinel panic value to abort a handler.
		//   While any panic from ServeHTTP aborts the response to the client,
		//   panicking with ErrAbortHandler also suppresses logging of a stack trace to the server's error log.
		// 中文说明：这是 net/http 约定的“静默中止”信号，日志处理器必须放行。
		return
	}

	// Same as stdlib http server code. Manually allocate stack trace buffer size
	// to prevent excessively large logs
	const size = 64 << 10
	stacktrace := make([]byte, size)
	stacktrace = stacktrace[:runtime.Stack(stacktrace, false)]

	logger := klog.FromContext(ctx)

	// For backwards compatibility, conversion to string
	// is handled here instead of defering to the logging
	// backend.
	if _, ok := r.(string); ok {
		logger.Error(nil, "Observed a panic", "panic", r, "stacktrace", string(stacktrace))
	} else {
		logger.Error(nil, "Observed a panic", "panic", fmt.Sprintf("%v", r), "panicGoValue", fmt.Sprintf("%#v", r), "stacktrace", string(stacktrace))
	}
}

// ErrorHandlers is a list of functions which will be invoked when a nonreturnable
// error occurs.
// TODO(lavalamp): for testability, this and the below HandleError function
// should be packaged up into a testable and reusable object.
var ErrorHandlers = []ErrorHandler{
	logError,
	// 1ms was the number folks were able to stomach as a global rate limit.
	// If you need to log errors more than 1000 times a second, you
	// should probably consider fixing your code instead. :)
	backoffError(1 * time.Millisecond),
}

// ErrorHandler is called indirectly through [HandleError], [HandleErrorWithContext] or [HandleErrorWithLogger].
// It is passed the same parameters that a structured logging backend needs to log a problem.
// It follows the semantic described for [HandleErrorWithContext] and [logr.Logger.Error]:
// - err is optional and may be nil
// - msg is string that describes the problem
// - keysAndValues contains additional information that varies between different occurrences of the problem
//
// [ErrorToString] can be used to convert these parameters into a single string, using the klog text output.
type ErrorHandler func(ctx context.Context, err error, msg string, keysAndValues ...interface{})

// ErrorToString takes the parameters passed to [ErrorHandler] and
// formats them as a string using the klog text output.
//
// If any of the values is a multi-line string, then the resulting
// string also uses line breaks and indention for the sake of readability.
// Does not include a trailing newline.
//
// Use errors.New if an error instead of a string is needed.
//
// 中文说明：临时构造一个输出到 buffer 的 textlogger，从而让字符串格式化
// 与真实日志保持一致；不带头部（时间/级别），便于嵌入其他消息。
func ErrorToString(err error, msg string, keysAndValues ...interface{}) string {
	var buffer bytes.Buffer
	config := textlogger.NewConfig(
		textlogger.Output(&buffer),
		textlogger.WithHeader(false),
	)
	logger := textlogger.NewLogger(config)
	logger.Error(err, msg, keysAndValues...)
	result := buffer.String()
	result = strings.TrimSpace(result)
	return result
}

// HandlerError is a method to invoke when a non-user facing piece of code cannot
// return an error and needs to indicate it has been ignored. Invoking this method
// is preferable to logging the error - the default behavior is to log but the
// errors may be sent to a remote server for analysis.
//
// Contextual logging: HandleErrorWithContext should be used instead of HandleError in code which supports contextual logging.
func HandleError(err error) {
	// this is sometimes called with a nil error.  We probably shouldn't fail and should do nothing instead
	// 中文说明：nil 错误静默忽略，与 HandleErrorWithContext 明确不同。
	if err == nil {
		return
	}

	handleError(context.Background(), err, "Unhandled Error")
}

// HandlerErrorWithContext is a method to invoke when a non-user facing piece of code cannot
// return an error and needs to indicate it has been ignored. Invoking this method
// is preferable to logging the error - the default behavior is to log but the
// errors may be sent to a remote server for analysis. The context is used to
// determine how to log the error.
//
// If contextual logging is enabled, the default log output is equivalent to
//
//	logr.FromContext(ctx).WithName("UnhandledError").Error(err, msg, keysAndValues...)
//
// Without contextual logging, it is equivalent to:
//
//	klog.ErrorS(err, msg, keysAndValues...)
//
// In contrast to HandleError, passing nil for the error is still going to
// trigger a log entry. Don't construct a new error or wrap an error
// with fmt.Errorf. Instead, add additional information via the mssage
// and key/value pairs.
//
// This variant should be used instead of HandleError because it supports
// structured, contextual logging. Alternatively, [HandleErrorWithLogger] can
// be used if a logger is available instead of a context.
//
// 中文说明：即使 err 为 nil 也会记录一条日志（用于没有错误对象但需要上报
// 异常状态的情形），因此不要为了“有 err”而包装一个假错误。
func HandleErrorWithContext(ctx context.Context, err error, msg string, keysAndValues ...interface{}) {
	handleError(ctx, err, msg, keysAndValues...)
}

// HandleErrorWithLogger is an alternative to [HandlerErrorWithContext] which accepts
// a logger for contextual logging.
func HandleErrorWithLogger(logger klog.Logger, err error, msg string, keysAndValues ...interface{}) {
	handleError(klog.NewContext(context.Background(), logger), err, msg, keysAndValues...)
}

// handleError is the common implementation of the HandleError* variants.
// Using this common implementation ensures that the stack depth
// is the same regardless through which path the handlers get invoked.
//
// 中文说明：按注册顺序调用所有 ErrorHandlers——默认是“记录 + 限速”，
// 替换列表即可把错误转发到远程分析系统。
func handleError(ctx context.Context, err error, msg string, keysAndValues ...interface{}) {
	for _, fn := range ErrorHandlers {
		fn(ctx, err, msg, keysAndValues...)
	}
}

// logError prints an error with the call stack of the location it was reported.
// It expects to be called as <caller> -> HandleError[WithContext] -> handleError -> logError.
//
// 中文说明：WithCallDepth(3) 正好跳过 logError/handleError/HandleError* 三层。
func logError(ctx context.Context, err error, msg string, keysAndValues ...interface{}) {
	logger := klog.FromContext(ctx).WithCallDepth(3)
	logger = klog.LoggerWithName(logger, "UnhandledError")
	logger.Error(err, msg, keysAndValues...) //nolint:logcheck // logcheck complains about unknown key/value pairs.
}

// backoffError blocks if it is called more often than the minPeriod.
// 中文说明：包装成无状态外观，但内部共享同一 backoff 实例以便全局限速。
func backoffError(minPeriod time.Duration) ErrorHandler {
	r := &rudimentaryErrorBackoff{
		lastErrorTime: time.Now(),
		minPeriod:     minPeriod,
	}

	return func(ctx context.Context, err error, msg string, keysAndValues ...interface{}) {
		r.OnError()
	}
}

// rudimentaryErrorBackoff 用“上次报错时间”实现全局最小间隔；锁只保护时间
// 字段，不覆盖 sleep。
type rudimentaryErrorBackoff struct {
	minPeriod time.Duration // immutable
	// TODO(lavalamp): use the clock for testability. Need to move that
	// package for that to be accessible here.
	lastErrorTimeLock sync.Mutex
	lastErrorTime     time.Time
}

// OnError will block if it is called more often than the embedded period time.
// This will prevent overly tight hot error loops.
func (r *rudimentaryErrorBackoff) OnError() {
	now := time.Now() // start the timer before acquiring the lock
	r.lastErrorTimeLock.Lock()
	d := now.Sub(r.lastErrorTime)
	r.lastErrorTime = time.Now()
	r.lastErrorTimeLock.Unlock()

	// Do not sleep with the lock held because that causes all callers of HandleError to block.
	// We only want the current goroutine to block.
	// A negative or zero duration causes time.Sleep to return immediately.
	// If the time moves backwards for any reason, do nothing.
	time.Sleep(r.minPeriod - d)
}

// GetCaller returns the caller of the function that calls it.
// 中文说明：Callers 的 skip=3 跳过 runtime.Callers、GetCaller 与直接调用者，
// 得到实际业务代码位置；依赖固定调用层级，包装会增加跳过层数。
func GetCaller() string {
	var pc [1]uintptr
	runtime.Callers(3, pc[:])
	f := runtime.FuncForPC(pc[0])
	if f == nil {
		return "Unable to find caller"
	}
	return f.Name()
}

// RecoverFromPanic replaces the specified error with an error containing the
// original error, and  the call tree when a panic occurs. This enables error
// handlers to handle errors and panics the same way.
//
// 中文说明：典型用法是 `defer utilruntime.RecoverFromPanic(&err)`，让
// “函数返回错误”和“panic”在调用方看来是同一种失败。
func RecoverFromPanic(err *error) {
	if r := recover(); r != nil {
		// Same as stdlib http server code. Manually allocate stack trace buffer size
		// to prevent excessively large logs
		const size = 64 << 10
		stacktrace := make([]byte, size)
		stacktrace = stacktrace[:runtime.Stack(stacktrace, false)]

		*err = fmt.Errorf(
			"recovered from panic %q. (err=%v) Call stack:\n%s",
			r,
			*err,
			stacktrace)
	}
}

// Must panics on non-nil errors. Useful to handling programmer level errors.
// 中文说明：只用于启动期、不可恢复的编程错误；不要把用户输入错误交给它。
func Must(err error) {
	if err != nil {
		panic(err)
	}
}
