/*
Copyright 2020 The Kubernetes Authors.

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
	"net/http"
	"sync"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/util/net"
)

// WarningHandler 是处理 warning 响应头的接口。
// Kubernetes 服务端通过 HTTP `Warning` 响应头返回弃用提示等信息，
// 该接口在每次收到 warning header 时被调用。新代码应优先实现
// WarningHandlerWithContext 以支持日志上下文与取消。
type WarningHandler interface {
	// HandleWarningHeader is called with the warn code, agent, and text when a warning header is countered.
	HandleWarningHeader(code int, agent string, text string)
}

// WarningHandlerWithContext is an interface for handling warning headers with
// support for contextual logging.
type WarningHandlerWithContext interface {
	// HandleWarningHeaderWithContext is called with the warn code, agent, and text when a warning header is countered.
	HandleWarningHeaderWithContext(ctx context.Context, code int, agent string, text string)
}

var (
	// 默认处理器在包初始化时即存在，避免未配置时出现 nil 判断分支。
	defaultWarningHandler     WarningHandlerWithContext = WarningLogger{}
	defaultWarningHandlerLock sync.RWMutex
)

// SetDefaultWarningHandler 设置客户端遇到 warning 响应头时使用的默认处理器。
// 默认会记录 warning。内置了若干实现：
//   - NoWarnings 抑制 warning。
//   - WarningLogger 记录 warning。
//   - NewWarningWriter() 把 warning 输出到指定的 writer。
//
// logcheck:context // SetDefaultWarningHandlerWithContext should be used instead of SetDefaultWarningHandler in code which supports contextual logging.
// 该默认处理器是进程级全局状态，影响所有 rest 客户端；测试或多组件共用
// 进程时应注意相互干扰。传入 nil 会清空默认处理器（收到 warning 时静默丢弃）。
func SetDefaultWarningHandler(l WarningHandler) {
	if l == nil {
		SetDefaultWarningHandlerWithContext(nil)
		return
	}
	SetDefaultWarningHandlerWithContext(warningLoggerNopContext{l: l})
}

// SetDefaultWarningHandlerWithContext is a variant of [SetDefaultWarningHandler] which supports contextual logging.
//
// 中文：写锁保护，允许并发读取默认处理器。
func SetDefaultWarningHandlerWithContext(l WarningHandlerWithContext) {
	defaultWarningHandlerLock.Lock()
	defer defaultWarningHandlerLock.Unlock()
	defaultWarningHandler = l
}

// getDefaultWarningHandler 读取当前默认处理器。
func getDefaultWarningHandler() WarningHandlerWithContext {
	defaultWarningHandlerLock.RLock()
	defer defaultWarningHandlerLock.RUnlock()
	l := defaultWarningHandler
	return l
}

// warningLoggerNopContext 把不支持 context 的旧处理器适配为新接口。
type warningLoggerNopContext struct {
	l WarningHandler
}

// HandleWarningHeaderWithContext 丢弃 ctx，直接委托给旧接口。
func (w warningLoggerNopContext) HandleWarningHeaderWithContext(_ context.Context, code int, agent string, message string) {
	w.l.HandleWarningHeader(code, agent, message)
}

// NoWarnings is an implementation of [WarningHandler] and [WarningHandlerWithContext] that suppresses warnings.
type NoWarnings struct{}

// HandleWarningHeader 丢弃所有 warning。
func (NoWarnings) HandleWarningHeader(code int, agent string, message string) {}

// HandleWarningHeaderWithContext 丢弃所有 warning。
func (NoWarnings) HandleWarningHeaderWithContext(ctx context.Context, code int, agent string, message string) {
}

var _ WarningHandler = NoWarnings{}
var _ WarningHandlerWithContext = NoWarnings{}

// WarningLogger is an implementation of [WarningHandler] and [WarningHandlerWithContext] that logs code 299 warnings
type WarningLogger struct{}

// HandleWarningHeader 使用 Background context 调用带 context 的实现。
func (w WarningLogger) HandleWarningHeader(code int, agent string, message string) {
	w.HandleWarningHeaderWithContext(context.Background(), code, agent, message)
}

// HandleWarningHeaderWithContext 把 warning 写入 klog。
//
// 中文：只有 code 299（Kubernetes 约定的“警告”）且有正文时才输出；其它代码按
// RFC 语义可能只是提示，避免产生噪声。
func (WarningLogger) HandleWarningHeaderWithContext(ctx context.Context, code int, agent string, message string) {
	if code != 299 || len(message) == 0 {
		return
	}
	klog.FromContext(ctx).Info("Warning: " + message)
}

var _ WarningHandler = WarningLogger{}
var _ WarningHandlerWithContext = WarningLogger{}

// warningWriter 把 warning 输出到指定 writer，可选去重与着色。
type warningWriter struct {
	// out is the writer to output warnings to
	out io.Writer
	// opts contains options controlling warning output
	opts WarningWriterOptions
	// writtenLock 保护去重集合 written 与计数器 writtenCount，使多个并发请求的 warning 输出串行化。
	writtenLock  sync.Mutex
	writtenCount int
	written      map[string]struct{}
}

// WarningWriterOptions controls the behavior of a WarningHandler constructed using NewWarningWriter()
type WarningWriterOptions struct {
	// Deduplicate 表示同一条 warning 消息只输出一次；
	// 在处理大量 warning 的长生命周期进程中置为 true，会因去重集合只增不减而持续占用内存，需权衡。
	Deduplicate bool
	// Color indicates that warning output can include ANSI color codes
	Color bool
}

// NewWarningWriter 返回一个 WarningHandler 实现，把 code 299 的 warning 输出到指定 writer。
// 未启用 Deduplicate 时不分配 map，减少无谓内存；返回类型是未导出的
// 具体类型，调用方只能依赖 WarningHandler 接口，这也便于后续扩展。
func NewWarningWriter(out io.Writer, opts WarningWriterOptions) *warningWriter {
	h := &warningWriter{out: out, opts: opts}
	if opts.Deduplicate {
		h.written = map[string]struct{}{}
	}
	return h
}

const (
	yellowColor = "\u001b[33;1m"
	resetColor  = "\u001b[0m"
)

// HandleWarningHeader prints warnings with code=299 to the configured writer.
//
// 中文：仅实现旧接口；写入操作在锁内完成，保证多行输出不会交错。
func (w *warningWriter) HandleWarningHeader(code int, agent string, message string) {
	if code != 299 || len(message) == 0 {
		return
	}

	w.writtenLock.Lock()
	defer w.writtenLock.Unlock()

	if w.opts.Deduplicate {
		if _, alreadyWritten := w.written[message]; alreadyWritten {
			return
		}
		w.written[message] = struct{}{}
	}
	w.writtenCount++

	if w.opts.Color {
		fmt.Fprintf(w.out, "%sWarning:%s %s\n", yellowColor, resetColor, message)
	} else {
		fmt.Fprintf(w.out, "Warning: %s\n", message)
	}
}

// WarningCount 返回已写出的 warning 数量（去重后）。
func (w *warningWriter) WarningCount() int {
	w.writtenLock.Lock()
	defer w.writtenLock.Unlock()
	return w.writtenCount
}

// handleWarnings 解析响应头并对每条 warning 调用处理器。
// handler 为 nil 时回退到全局默认处理器；解析失败的头会被忽略（错误
// 直接丢弃），因为 warning 是尽力而为的附加信息，不应影响正常请求结果。
// 返回解析出的 warning 列表，便于调用方记录或测试断言。
func handleWarnings(ctx context.Context, headers http.Header, handler WarningHandlerWithContext) []net.WarningHeader {
	if handler == nil {
		handler = getDefaultWarningHandler()
	}

	warnings, _ := net.ParseWarningHeaders(headers["Warning"])
	for _, warning := range warnings {
		handler.HandleWarningHeaderWithContext(ctx, warning.Code, warning.Agent, warning.Text)
	}
	return warnings
}
