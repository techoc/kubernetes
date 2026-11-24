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
	// ReallyCrash 控制 HandleCrash 的最终行为，默认值为 true。
	// 暴露该变量是为了兼容需要恢复旧行为的组件；主要用于测试验证崩溃处理路径。
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

// HandleCrash 捕获当前 goroutine 中尚未处理的 panic，并记录错误。该函数应通过 defer 调用。
//
// 调用方可以传入额外处理器，在发生 panic 时执行清理操作，例如优雅地停止相关 goroutine。
// 所有处理器和日志记录完成后，如果 ReallyCrash 为 true，仍会重新触发 panic。
//
// 支持上下文日志的代码应优先使用 HandleCrashWithContext 或 HandleCrashWithLogger。
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

// HandleCrashWithContext 从当前 goroutine 的 defer 中捕获 panic，并使用 ctx 中的 logger 记录。
//
// 调用方可以提供额外处理器，用于 panic 后的资源清理或状态通知。所有处理器执行完毕后，
// 若 ReallyCrash 为 true，则重新触发 panic。ctx 同时决定日志上下文和归属信息。
// 中文说明：上下文中的 logger 决定日志输出位置，适合请求级代码。
func HandleCrashWithContext(ctx context.Context, additionalHandlers ...func(context.Context, interface{})) {
	if r := recover(); r != nil {
		handleCrash(ctx, r, additionalHandlers...)
	}
}

// HandleCrashWithLogger 从当前 goroutine 的 defer 中捕获 panic，并使用指定 logger 记录。
//
// 调用方可以提供额外处理器，用于 panic 后的资源清理或状态通知。所有处理器执行完毕后，
// 若 ReallyCrash 为 true，则重新触发 panic。该入口适用于已有 logger、但没有 ctx 的场景。
//
// 中文说明：已有 logger 但无 ctx 时使用；内部把 logger 放进新的 ctx。
func HandleCrashWithLogger(logger klog.Logger, additionalHandlers ...func(context.Context, interface{})) {
	if r := recover(); r != nil {
		ctx := klog.NewContext(context.Background(), logger)
		handleCrash(ctx, r, additionalHandlers...)
	}
}

// handleCrash 是所有 HandleCrash* 入口共用的实现。
//
// 统一经过该函数可以让不同入口调用处理器时具有一致的调用栈深度，
// 从而避免日志调用位置因入口不同而产生偏差。
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
		// 执行最终的 panic：处理器和日志记录完成后保留 fast-fail 行为。
		panic(r)
	}
}

// logPanic logs the caller tree when a panic occurs (except in the special case of http.ErrAbortHandler).
func logPanic(ctx context.Context, r interface{}) {
	if r == http.ErrAbortHandler {
		// 遵循 http.ErrAbortHandler 哨兵值的约定：它用于主动中止 HTTP handler。
		// ServeHTTP 中的任意 panic 都会中止客户端响应；使用 ErrAbortHandler 还会抑制
		// 服务端错误日志中的堆栈跟踪输出。
		// 中文说明：这是 net/http 约定的“静默中止”信号，日志处理器必须放行。
		return
	}

	// 与标准库 net/http 服务端的做法一致，手动限制堆栈缓冲区大小，避免日志体积过大。
	// to prevent excessively large logs
	const size = 64 << 10
	stacktrace := make([]byte, size)
	stacktrace = stacktrace[:runtime.Stack(stacktrace, false)]

	logger := klog.FromContext(ctx)

	// 为保持向后兼容，这里先将 panic 值转换为字符串，而不是交给底层日志后端处理。
	if _, ok := r.(string); ok {
		logger.Error(nil, "Observed a panic", "panic", r, "stacktrace", string(stacktrace))
	} else {
		logger.Error(nil, "Observed a panic", "panic", fmt.Sprintf("%v", r), "panicGoValue", fmt.Sprintf("%#v", r), "stacktrace", string(stacktrace))
	}
}

// ErrorHandlers 是遇到无法向上返回的错误时依次调用的处理器列表。
//
// TODO(lavalamp)：为了便于测试，应将该列表与下方 HandleError 封装为可复用、可独立测试的对象。
var ErrorHandlers = []ErrorHandler{
	logError,
	// 全局限速间隔设为 1ms。若错误日志频率超过每秒 1000 次，通常应优先排查并修复错误源。
	// If you need to log errors more than 1000 times a second, you
	// should probably consider fixing your code instead. :)
	backoffError(1 * time.Millisecond),
}

// ErrorHandler 是由 HandleError、HandleErrorWithContext 或 HandleErrorWithLogger 间接调用的处理器类型。
// 参数与结构化日志后端记录问题时所需的信息一致，语义遵循 HandleErrorWithContext 和 logr.Logger.Error：
// - err 可选，允许为 nil；
// - msg 描述问题；
// - keysAndValues 携带不同问题实例的附加上下文信息。
//
// 可使用 ErrorToString 按 klog 文本日志格式将这些参数转换为单个字符串。
type ErrorHandler func(ctx context.Context, err error, msg string, keysAndValues ...interface{})

// ErrorToString 使用 klog 文本日志格式，将传给 ErrorHandler 的参数格式化为字符串。
//
// 如果任一参数包含多行文本，结果也会保留换行并添加缩进，以提高可读性。
// 返回值不包含末尾换行符。
//
// 如果需要的是 error 对象而不是字符串，请使用 errors.New。
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

// HandleError 用于无法向上返回错误、但需要报告已忽略问题的非用户可见代码。
//
// 相比直接记录日志，更推荐调用该方法：默认行为是记录错误，但处理器也可以将错误发送到远程系统分析。
// 支持上下文日志的代码应使用 HandleErrorWithContext。
func HandleError(err error) {
	// 某些调用路径可能传入 nil；此时不应额外制造错误或日志，直接返回即可。
	// 中文说明：nil 错误静默忽略，与 HandleErrorWithContext 明确不同。
	if err == nil {
		return
	}

	handleError(context.Background(), err, "Unhandled Error")
}

// HandleErrorWithContext 用于无法向上返回错误、但需要报告已忽略问题的非用户可见代码。
//
// 相比直接记录日志，更推荐调用该方法：默认行为是记录错误，但处理器也可以将错误发送到远程系统分析。
// ctx 决定日志使用的上下文信息。
//
// 启用上下文日志时，默认输出等价于：
//
//	logr.FromContext(ctx).WithName("UnhandledError").Error(err, msg, keysAndValues...)
//
// 未启用上下文日志时，默认输出等价于：
//
//	klog.ErrorS(err, msg, keysAndValues...)
//
// 与 HandleError 不同，即使 err 为 nil 也会触发日志记录。不要为了填充 err 而新建或包装错误；
// 应通过 msg 和键值对补充上下文信息。
//
// 该入口支持结构化、上下文日志，因此优先于 HandleError。若已有 logger 而非 context，
// 可以使用 HandleErrorWithLogger。
//
// 中文说明：即使 err 为 nil 也会记录一条日志（用于没有错误对象但需要上报
// 异常状态的情形），因此不要为了“有 err”而包装一个假错误。
func HandleErrorWithContext(ctx context.Context, err error, msg string, keysAndValues ...interface{}) {
	handleError(ctx, err, msg, keysAndValues...)
}

// HandleErrorWithLogger 是 HandleErrorWithContext 的替代入口，直接接收 logger 以提供上下文日志能力。
func HandleErrorWithLogger(logger klog.Logger, err error, msg string, keysAndValues ...interface{}) {
	handleError(klog.NewContext(context.Background(), logger), err, msg, keysAndValues...)
}

// handleError 是所有 HandleError* 入口共用的实现。统一经过该函数可以保证
// 不同入口调用处理器时具有相同的调用栈深度。
//
// 中文说明：按注册顺序调用所有 ErrorHandlers——默认是“记录 + 限速”，
// 替换列表即可把错误转发到远程分析系统。
func handleError(ctx context.Context, err error, msg string, keysAndValues ...interface{}) {
	for _, fn := range ErrorHandlers {
		fn(ctx, err, msg, keysAndValues...)
	}
}

// logError 记录错误及其上报位置的调用栈。预期调用链为：
// <caller> -> HandleError[WithContext] -> handleError -> logError。
//
// 中文说明：WithCallDepth(3) 正好跳过 logError/handleError/HandleError* 三层。
func logError(ctx context.Context, err error, msg string, keysAndValues ...interface{}) {
	logger := klog.FromContext(ctx).WithCallDepth(3)
	logger = klog.LoggerWithName(logger, "UnhandledError")
	logger.Error(err, msg, keysAndValues...) //nolint:logcheck // logcheck complains about unknown key/value pairs.
}

// backoffError 返回一个限速处理器；调用频率超过 minPeriod 时会阻塞当前 goroutine。
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

// OnError 在调用频率超过配置的最小间隔时阻塞当前 goroutine，防止错误热循环过于密集。
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

// GetCaller 返回直接调用该函数的调用方函数名。
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

// RecoverFromPanic 捕获当前 goroutine 的 panic，并将指定错误替换为包含原错误和 panic 调用栈的新错误。
// 这样上层错误处理逻辑可以用统一方式处理普通错误与 panic。
//
// 中文说明：典型用法是 `defer utilruntime.RecoverFromPanic(&err)`，让
// “函数返回错误”和“panic”在调用方看来是同一种失败。
func RecoverFromPanic(err *error) {
	if r := recover(); r != nil {
		// 与标准库 net/http 服务端的做法一致，手动限制堆栈缓冲区大小，避免日志体积过大。
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

// Must 在 err 非 nil 时触发 panic，适用于程序员错误或初始化阶段无法继续的失败。
// 中文说明：只用于启动期、不可恢复的编程错误；不要把用户输入错误交给它。
func Must(err error) {
	if err != nil {
		panic(err)
	}
}
