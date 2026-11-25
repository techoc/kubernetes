/*
Copyright 2021 The Kubernetes Authors.

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

package responsewriter

import (
	"bufio"
	"net"
	"net/http"
)

// UserProvidedDecorator 表示使用方（即本包的调用方）提供的装饰器，它包装了内层的
// http.ResponseWriter 对象。该装饰器必须通过 Unwrap 函数返回被装饰的内层
// http.ResponseWriter 对象。
type UserProvidedDecorator interface {
	http.ResponseWriter

	// Unwrap 返回与用户提供的装饰器相关联的内层 http.ResponseWriter 对象。
	Unwrap() http.ResponseWriter
}

// WrapForHTTP1Or2 接受一个包装了“内层”http.ResponseWriter 的用户装饰器，并可能在外面
// 再包一层新的 http.ResponseWriter：它通过委托给用户装饰器（若其实现了对应方法）
// 或内层 http.ResponseWriter（否则）来实现 http.CloseNotifier、http.Flusher
// 和/或 http.Hijacker，从而保证返回对象实现的接口子集与内层对象完全一致。
// 本函数处理以下三种情况：
//   - 内层 ResponseWriter 同时实现 `http.CloseNotifier`、`http.Flusher` 和
//     `http.Hijacker`（HTTP/1.1 服务器提供这样的 ResponseWriter）。
//   - 内层 ResponseWriter 实现 `http.CloseNotifier` 和 `http.Flusher`，
//     但不实现 `http.Hijacker`（HTTP/2 服务器提供这样的 ResponseWriter）。
//   - 其余所有情况都归为第三类，直接返回传入的 ResponseWriter。
//
// 涉及三个术语：
//   - “outer”：WrapForHTTP1Or2 返回的 ResponseWriter 对象。
//   - “用户提供的装饰器”或“middle”：包装内层 ResponseWriter 的那个装饰器对象，
//     它实现 UserProvidedDecorator 接口，可能实现也可能不实现
//     http.CloseNotifier、http.Flusher 或 http.Hijacker。
//   - “inner”：被用户装饰器扩展的那个 ResponseWriter。
//
// 关键不变量：返回对象暴露的可选接口集合必须与 inner 完全一致，
// 否则调用方按接口断言时会误判连接能力（例如 HTTP/2 下谎报可 Hijack）。
func WrapForHTTP1Or2(decorator UserProvidedDecorator) http.ResponseWriter {
	// 以下结论来自 go net/http 文档：
	// HTTP/1.x 与 HTTP/2 的默认 ResponseWriter 实现都支持 Flusher，handler 应在运行时检测该能力。
	//
	// Hijacker 由允许 handler 接管连接的 ResponseWriter 实现：HTTP/1.x 默认支持，
	// HTTP/2 刻意不支持，包装器也可能不支持，handler 同样应在运行时检测。
	//
	// CloseNotifier 用于检测底层连接已断开。Deprecated: 该接口早于 Go 的 context 包，
	// 新代码应改用 Request.Context。
	inner := decorator.Unwrap()
	if innerNotifierFlusher, ok := inner.(CloseNotifierFlusher); ok {
		// HTTP/2 请求的默认 ResponseWriter 对象（http2responseWriter）实现 Flusher 和 CloseNotifier。
		outerHTTP2 := outerWithCloseNotifyAndFlush{
			UserProvidedDecorator:     decorator,
			InnerCloseNotifierFlusher: innerNotifierFlusher,
		}

		if innerHijacker, hijackable := inner.(http.Hijacker); hijackable {
			// HTTP/1.x 请求的默认 ResponseWriter 实现同时提供 CloseNotifier、Flusher 和 Hijacker。
			return &outerWithCloseNotifyFlushAndHijack{
				outerWithCloseNotifyAndFlush: outerHTTP2,
				InnerHijacker:                innerHijacker,
			}
		}

		return outerHTTP2
	}

	// 无论 http/1.x 还是 http2 请求，都不应该走到这里。
	return decorator
}

// CloseNotifierFlusher 是 http.CloseNotifier 与 http.Flusher 的组合，
// http/1.x 和 http2 请求都适用。
type CloseNotifierFlusher interface {
	http.CloseNotifier
	http.Flusher
}

// GetOriginal 沿着被包装的 http.ResponseWriter 链条一直向下查找，
// 返回过滤器链中第一个 handler 收到的原始 http.ResponseWriter 对象。
// 用于需要访问底层连接（如 hijack、flush）的过滤器绕过装饰层。
func GetOriginal(w http.ResponseWriter) http.ResponseWriter {
	decorator, ok := w.(UserProvidedDecorator)
	if !ok {
		return w
	}

	inner := decorator.Unwrap()
	if inner == w {
		// 这里出现了无限循环，正常流程不应该走到这一步。
		// 自引用会无限递归，宁可 panic 也不要栈溢出。
		panic("http.ResponseWriter decorator chain has a cycle")
	}

	return GetOriginal(inner)
}

//nolint:staticcheck // SA1019
var _ http.CloseNotifier = outerWithCloseNotifyAndFlush{}
var _ http.Flusher = outerWithCloseNotifyAndFlush{}
var _ http.ResponseWriter = outerWithCloseNotifyAndFlush{}
var _ UserProvidedDecorator = outerWithCloseNotifyAndFlush{}

// outerWithCloseNotifyAndFlush 是外层对象，仅为用户装饰器补充 http.CloseNotifier
// 和 http.Flusher。
type outerWithCloseNotifyAndFlush struct {
	// UserProvidedDecorator 是使用方提供的对象，它装饰了内层 ResponseWriter。
	UserProvidedDecorator

	// 内层对象的 http.CloseNotifier 和 http.Flusher 实现。
	InnerCloseNotifierFlusher CloseNotifierFlusher
}

func (wr outerWithCloseNotifyAndFlush) CloseNotify() <-chan bool {
	// 优先使用装饰器自己的实现，它可能带有额外语义；否则回退到 inner。
	if notifier, ok := wr.UserProvidedDecorator.(http.CloseNotifier); ok {
		return notifier.CloseNotify()
	}

	return wr.InnerCloseNotifierFlusher.CloseNotify()
}

// Flush 同样优先委托给装饰器，保证装饰器对 flush 的拦截不被绕过。
func (wr outerWithCloseNotifyAndFlush) Flush() {
	if flusher, ok := wr.UserProvidedDecorator.(http.Flusher); ok {
		flusher.Flush()
		return
	}

	wr.InnerCloseNotifierFlusher.Flush()
}

//lint:file-ignore SA1019 Keep supporting deprecated http.CloseNotifier
var _ http.CloseNotifier = outerWithCloseNotifyFlushAndHijack{}
var _ http.Flusher = outerWithCloseNotifyFlushAndHijack{}
var _ http.Hijacker = outerWithCloseNotifyFlushAndHijack{}
var _ http.ResponseWriter = outerWithCloseNotifyFlushAndHijack{}
var _ UserProvidedDecorator = outerWithCloseNotifyFlushAndHijack{}

// outerWithCloseNotifyFlushAndHijack 是外层对象，为用户装饰器补充 http.CloseNotifier、
// http.Flusher 和 http.Hijacker，仅适用于 http/1.x 请求。
type outerWithCloseNotifyFlushAndHijack struct {
	outerWithCloseNotifyAndFlush

	// 内层对象的 http.Hijacker 实现。
	InnerHijacker http.Hijacker
}

// Hijack 仅在 inner 本身可 hijack 时才会被调用到（外层类型只在那种情况下构造）。
func (wr outerWithCloseNotifyFlushAndHijack) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := wr.UserProvidedDecorator.(http.Hijacker); ok {
		return hijacker.Hijack()
	}

	return wr.InnerHijacker.Hijack()
}
