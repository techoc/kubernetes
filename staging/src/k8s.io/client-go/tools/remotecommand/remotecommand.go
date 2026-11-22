/*
Copyright 2015 The Kubernetes Authors.

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

package remotecommand

import (
	"context"
	"io"
	"net/http"

	"k8s.io/klog/v2"
	"k8s.io/streaming/pkg/httpstream"
)

// StreamOptions holds information pertaining to the current streaming session:
// input/output streams, if the client is requesting a TTY, and a terminal size queue to
// support terminal resizing.
//
// StreamOptions 描述一次流式会话的输入输出与终端参数。任一 io 字段为 nil 时，
// 表示不传输该方向的流（例如仅需输出时可不设 Stdin）。
type StreamOptions struct {
	// Stdin 客户端侧输入（发送到远端进程）。
	Stdin io.Reader
	// Stdout 远端标准输出写入的本地目标。
	Stdout io.Writer
	// Stderr 远端标准错误写入的本地目标；Tty 为 true 时被忽略。
	Stderr io.Writer
	// Tty 是否为远端请求分配伪终端。
	Tty bool
	// TerminalSizeQueue 在 TTY 模式下提供终端尺寸变更事件，实现窗口自适应。
	TerminalSizeQueue TerminalSizeQueue
}

// Executor is an interface for transporting shell-style streams.
//
// Executor 抽象了向远端传输 shell 风格流的机制（如 exec、attach、port-forward）。
type Executor interface {
	// Deprecated: use StreamWithContext instead to avoid possible resource leaks.
	// See https://github.com/kubernetes/kubernetes/pull/103177 for details.
	//
	// 已弃用：请改用 StreamWithContext，以避免可能的资源泄漏。
	Stream(options StreamOptions) error

	// StreamWithContext initiates the transport of the standard shell streams. It will
	// transport any non-nil stream to a remote system, and return an error if a problem
	// occurs. If tty is set, the stderr stream is not used (raw TTY manages stdout and
	// stderr over the stdout stream).
	// The context controls the entire lifetime of stream execution.
	//
	// StreamWithContext 启动标准 shell 流的传输；仅传输非 nil 的流，出错时返回 error。
	// Tty 为 true 时不使用 stderr（原始 TTY 会在 stdout 流上复用 stdout 与 stderr）。
	// ctx 控制整个流式过程的生存期，取消 ctx 会终止会话并释放相关资源。
	StreamWithContext(ctx context.Context, options StreamOptions) error
}

// streamCreator 由底层连接实现，负责按给定请求头创建一条复用流。
type streamCreator interface {
	CreateStream(headers http.Header) (httpstream.Stream, error)
}

// streamProtocolHandler 负责在连接上执行某种协议握手（如 SPDY/WebSocket），
// ready 用于向调用方通知“流已就绪”。
type streamProtocolHandler interface {
	stream(logger klog.Logger, conn streamCreator, ready chan<- struct{}) error
}
