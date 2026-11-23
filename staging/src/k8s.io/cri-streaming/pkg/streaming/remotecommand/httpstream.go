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

package remotecommand

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/streaming/pkg/httpstream/spdy"
	"k8s.io/streaming/pkg/httpstream/wsstream"
	"k8s.io/streaming/pkg/runtime"

	"k8s.io/klog/v2"
)

// Options contains details about which streams are required for
// remote command execution.
// Options 描述一次远端命令执行需要哪些流：Stdin/Stdout/Stderr 分别对应三条标准流，
// TTY 表示是否分配伪终端；当 TTY 为真时服务端不再单独使用 stderr 流。
type Options struct {
	Stdin  bool
	Stdout bool
	Stderr bool
	TTY    bool
}

// NewOptions creates a new Options from the Request.
// NewOptions 从 HTTP 请求参数解析出 Options，并校验至少请求了一条标准流。
func NewOptions(req *http.Request) (*Options, error) {
	// 参数值为 "1" 表示开启对应能力。
	tty := req.FormValue(ExecTTYParam) == "1"
	stdin := req.FormValue(ExecStdinParam) == "1"
	stdout := req.FormValue(ExecStdoutParam) == "1"
	stderr := req.FormValue(ExecStderrParam) == "1"
	if tty && stderr {
		// TTY 模式下终端本身合并了 stdout 与 stderr，无法再单独提供 stderr 流，故忽略该请求。
		// TODO: make this an error before we reach this method
		klog.V(4).InfoS("Access to exec with tty and stderr is not supported, bypassing stderr")
		stderr = false
	}

	// 没有任何数据通道时客户端无法得到输出或发送输入，直接拒绝。
	if !stdin && !stdout && !stderr {
		return nil, fmt.Errorf("you must specify at least 1 of stdin, stdout, stderr")
	}

	return &Options{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		TTY:    tty,
	}, nil
}

// connectionContext contains the connection and streams used when
// forwarding an attach or execute session into a container.
// connectionContext 持有转发一次 attach 或 exec 会话所需的连接与流：
// conn 是底层连接，stdinStream/stdoutStream/stderrStream 是三条标准流，
// writeStatus 负责在 error 流写状态，resizeStream/resizeChan 传递终端尺寸，tty 表示是否使用伪终端。
type connectionContext struct {
	conn         io.Closer
	stdinStream  io.ReadCloser
	stdoutStream io.WriteCloser
	stderrStream io.WriteCloser
	writeStatus  func(status *streamStatusError) error
	resizeStream io.ReadCloser
	resizeChan   chan TerminalSize
	tty          bool
}

// streamAndReply holds both a Stream and a channel that is closed when the stream's reply frame is
// enqueued. Consumers can wait for replySent to be closed prior to proceeding, to ensure that the
// replyFrame is enqueued before the connection's goaway frame is sent (e.g. if a stream was
// received and right after, the connection gets closed).
// streamAndReply 把一条 Stream 与其 replySent 通道绑定：replySent 关闭表示该流的应答帧已入队。
// httpstream.Stream 是嵌入字段，replySent 用于等待应答发送完毕，避免连接在应答前就被关闭。
type streamAndReply struct {
	httpstream.Stream
	replySent <-chan struct{}
}

// waitStreamReply waits until either replySent or stop is closed. If replySent is closed, it sends
// an empty struct to the notify channel.
// waitStreamReply 等待 replySent 或 stop 关闭；收到应答时向 notify 发一个信号，
// stop 关闭时静默退出，从而在主流程结束或超时后回收该 goroutine。
func waitStreamReply(replySent <-chan struct{}, notify chan<- struct{}, stop <-chan struct{}) {
	select {
	case <-replySent:
		notify <- struct{}{}
	case <-stop:
	}
}

// createStreams 是统一的入口：先根据请求类型选择 WebSocket 或 HTTP/1.1 Upgrade（SPDY）建流，
// 再为所有实现方式统一启动 resize 事件的转发 goroutine。
func createStreams(req *http.Request, w http.ResponseWriter, opts *Options, supportedStreamProtocols []string, idleTimeout, streamCreationTimeout time.Duration) (*connectionContext, bool) {
	var ctx *connectionContext
	var ok bool
	// WebSocket 请求走独立的建流路径，否则使用多次 Upgrade 的 SPDY/httpstream 路径。
	if wsstream.IsWebSocketRequest(req) {
		ctx, ok = createWebSocketStreams(req, w, opts, idleTimeout)
	} else {
		ctx, ok = createHTTPStreamStreams(req, w, opts, supportedStreamProtocols, idleTimeout, streamCreationTimeout)
	}
	if !ok {
		return nil, false
	}

	// 仅当协商到了 resize 流时才创建通道并启动消费者，把 JSON 尺寸事件转成 TerminalSize。
	if ctx.resizeStream != nil {
		ctx.resizeChan = make(chan TerminalSize)
		go handleResizeEvents(req.Context(), ctx.resizeStream, ctx.resizeChan)
	}

	return ctx, true
}

// createHTTPStreamStreams 处理 HTTP/1.1 Upgrade 路径：协商子协议、升级连接，并按协议版本等待客户端建好所有流。
func createHTTPStreamStreams(req *http.Request, w http.ResponseWriter, opts *Options, supportedStreamProtocols []string, idleTimeout, streamCreationTimeout time.Duration) (*connectionContext, bool) {
	// Handshake 从 Sec-WebSocket-Protocol 之类的头部挑出双方都支持的子协议，失败时已写了错误响应。
	protocol, err := httpstream.Handshake(req, w, supportedStreamProtocols)
	if err != nil {
		// Handshake writes the error to the client
		// 握手失败：错误响应已由 Handshake 写出。
		return nil, false
	}

	// 客户端每建立一条流，回调就把它连同 replySent 一起投递到该通道。
	streamCh := make(chan streamAndReply)

	upgrader := spdy.NewResponseUpgrader()
	conn := upgrader.UpgradeResponse(w, req, func(stream httpstream.Stream, replySent <-chan struct{}) error {
		streamCh <- streamAndReply{Stream: stream, replySent: replySent}
		return nil
	})
	// from this point on, we can no longer call methods on response
	// 升级之后响应写入器已被接管，不能再对其调用任何方法。
	if conn == nil {
		// The upgrader is responsible for notifying the client of any errors that
		// occurred during upgrading. All we can do is return here at this point
		// if we weren't successful in upgrading.
		// 升级器已负责通知客户端错误，这里只能返回失败。
		return nil, false
	}

	// 设置连接空闲超时，防止会话结束后连接长期挂起。
	conn.SetIdleTimeout(idleTimeout)

	// 按协商出的协议选择对应的流处理器，各版本对 error 流格式和 resize 的支持不同。
	var handler protocolHandler
	switch protocol {
	case StreamProtocolV4Name:
		handler = &v4ProtocolHandler{}
	case StreamProtocolV3Name:
		handler = &v3ProtocolHandler{}
	case StreamProtocolV2Name:
		handler = &v2ProtocolHandler{}
	case "":
		// 客户端未协商版本时回退到 v1，以保持与旧版 kubelet 的兼容。
		klog.V(4).InfoS("Client did not request protocol negotiation. Falling back", "protocol", StreamProtocolV1Name)
		fallthrough
	case StreamProtocolV1Name:
		handler = &v1ProtocolHandler{}
	}

	// count the streams client asked for, starting with 1
	// error 流始终需要，因此从 1 开始，再按请求累加 stdin/stdout/stderr 与 resize。
	expectedStreams := 1
	if opts.Stdin {
		expectedStreams++
	}
	if opts.Stdout {
		expectedStreams++
	}
	if opts.Stderr {
		expectedStreams++
	}
	if opts.TTY && handler.supportsTerminalResizing() {
		// 只有 TTY 且协议支持时客户端才会额外创建 resize 流。
		expectedStreams++
	}

	// 用定时器约束客户端建流的总体耗时，避免恶意或异常客户端长期占用连接。
	expired := time.NewTimer(streamCreationTimeout)
	defer expired.Stop()

	// 等待收齐所有流；超时或收到意外流类型时返回错误。
	ctx, err := handler.waitForStreams(streamCh, expectedStreams, expired.C)
	if err != nil {
		runtime.HandleError(err)
		return nil, false
	}

	ctx.conn = conn
	ctx.tty = opts.TTY

	return ctx, true
}

// protocolHandler 抽象不同协议版本建流方式的差异：各版本对 error 流格式与 resize 支持不同。
type protocolHandler interface {
	// waitForStreams waits for the expected streams or a timeout, returning a
	// remoteCommandContext if all the streams were received, or an error if not.
	// waitForStreams 等待收齐期望数量的流或超时，成功返回连接上下文，失败返回错误。
	waitForStreams(streams <-chan streamAndReply, expectedStreams int, expired <-chan time.Time) (*connectionContext, error)
	// supportsTerminalResizing returns true if the protocol handler supports terminal resizing
	// supportsTerminalResizing 报告该协议是否支持终端尺寸调整，决定是否需要等待 resize 流。
	supportsTerminalResizing() bool
}

// v4ProtocolHandler implements the V4 protocol version for streaming command execution. It only differs
// in from v3 in the error stream format using a json-marshaled status object which carries
// the process' exit code.
// v4ProtocolHandler 实现 v4 协议；与 v3 的唯一区别是 error 流改用携带退出码的 JSON 状态对象。
type v4ProtocolHandler struct{}

// waitForStreams 按流头部的 streamType 把各条流装入上下文，并等待它们的应答帧全部入队。
func (*v4ProtocolHandler) waitForStreams(streams <-chan streamAndReply, expectedStreams int, expired <-chan time.Time) (*connectionContext, error) {
	ctx := &connectionContext{}
	receivedStreams := 0
	replyChan := make(chan struct{})
	// stop 在函数返回时关闭，用于让所有 waitStreamReply goroutine 退出，避免泄漏。
	stop := make(chan struct{})
	defer close(stop)
WaitForStreams:
	for {
		select {
		case stream := <-streams:
			// 流类型由客户端在该流的头部声明，据此分派到对应的字段。
			streamType := stream.Headers().Get(StreamType)
			switch streamType {
			case StreamTypeError:
				// v4 的 error 流写入 JSON 状态对象，其中携带进程退出码。
				ctx.writeStatus = v4WriteStatusFunc(stream) // write json errors
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeStdin:
				ctx.stdinStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeStdout:
				ctx.stdoutStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeStderr:
				ctx.stderrStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeResize:
				ctx.resizeStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			default:
				// 未知流类型只记录，不计入期望数量。
				runtime.HandleError(fmt.Errorf("unexpected stream type: %q", streamType))
			}
		case <-replyChan:
			// 每收到一个应答信号就计数，收齐后退出等待循环。
			receivedStreams++
			if receivedStreams == expectedStreams {
				break WaitForStreams
			}
		case <-expired:
			// 客户端未在限定时间内建好所有流，放弃本次会话。
			// TODO find a way to return the error to the user. Maybe use a separate
			// stream to report errors?
			return nil, errors.New("timed out waiting for client to create streams")
		}
	}

	return ctx, nil
}

// supportsTerminalResizing returns true because v4ProtocolHandler supports it
// supportsTerminalResizing 对 v4 返回 true，因为该协议支持终端尺寸调整。
func (*v4ProtocolHandler) supportsTerminalResizing() bool { return true }

// v3ProtocolHandler implements the V3 protocol version for streaming command execution.
// v3ProtocolHandler 实现 v3 协议，相比 v2 增加了对 resize 流的支持。
type v3ProtocolHandler struct{}

// waitForStreams 按流类型分派并等待收齐流；v3 的 error 流沿用旧的纯文本格式。
func (*v3ProtocolHandler) waitForStreams(streams <-chan streamAndReply, expectedStreams int, expired <-chan time.Time) (*connectionContext, error) {
	ctx := &connectionContext{}
	receivedStreams := 0
	replyChan := make(chan struct{})
	// 函数返回时关闭 stop，回收所有等待应答的 helper goroutine。
	stop := make(chan struct{})
	defer close(stop)
WaitForStreams:
	for {
		select {
		case stream := <-streams:
			// 客户端在流头部声明类型，据此填入上下文的对应字段。
			streamType := stream.Headers().Get(StreamType)
			switch streamType {
			case StreamTypeError:
				// v3 不携带退出码，只写纯文本错误信息。
				ctx.writeStatus = v1WriteStatusFunc(stream)
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeStdin:
				ctx.stdinStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeStdout:
				ctx.stdoutStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeStderr:
				ctx.stderrStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeResize:
				ctx.resizeStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			default:
				runtime.HandleError(fmt.Errorf("unexpected stream type: %q", streamType))
			}
		case <-replyChan:
			// 应答计数达到期望值即认为所有流已就绪。
			receivedStreams++
			if receivedStreams == expectedStreams {
				break WaitForStreams
			}
		case <-expired:
			// 建流超时则放弃，避免连接被无限占用。
			// TODO find a way to return the error to the user. Maybe use a separate
			// stream to report errors?
			return nil, errors.New("timed out waiting for client to create streams")
		}
	}

	return ctx, nil
}

// supportsTerminalResizing returns true because v3ProtocolHandler supports it
// supportsTerminalResizing 对 v3 返回 true，因为该协议支持终端尺寸调整。
func (*v3ProtocolHandler) supportsTerminalResizing() bool { return true }

// v2ProtocolHandler implements the V2 protocol version for streaming command execution.
// v2ProtocolHandler 实现 v2 协议，主要修复 v1 的缺陷，但仍不支持 resize。
type v2ProtocolHandler struct{}

// waitForStreams 收齐 stdin/stdout/stderr/error 四条流；v2 没有 resize 流分支。
func (*v2ProtocolHandler) waitForStreams(streams <-chan streamAndReply, expectedStreams int, expired <-chan time.Time) (*connectionContext, error) {
	ctx := &connectionContext{}
	receivedStreams := 0
	replyChan := make(chan struct{})
	// 结束后关闭 stop，确保等待应答的 goroutine 一并退出。
	stop := make(chan struct{})
	defer close(stop)
WaitForStreams:
	for {
		select {
		case stream := <-streams:
			// 按头部声明的流类型分派到上下文字段。
			streamType := stream.Headers().Get(StreamType)
			switch streamType {
			case StreamTypeError:
				ctx.writeStatus = v1WriteStatusFunc(stream)
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeStdin:
				ctx.stdinStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeStdout:
				ctx.stdoutStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeStderr:
				ctx.stderrStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			default:
				// v2 只认识上述四种类型，其余一律忽略并记录。
				runtime.HandleError(fmt.Errorf("unexpected stream type: %q", streamType))
			}
		case <-replyChan:
			// 统计已入队的应答，收齐后结束等待。
			receivedStreams++
			if receivedStreams == expectedStreams {
				break WaitForStreams
			}
		case <-expired:
			// 建流超时，返回错误终止会话。
			// TODO find a way to return the error to the user. Maybe use a separate
			// stream to report errors?
			return nil, errors.New("timed out waiting for client to create streams")
		}
	}

	return ctx, nil
}

// supportsTerminalResizing returns false because v2ProtocolHandler doesn't support it.
// supportsTerminalResizing 对 v2 返回 false，因为该协议不支持终端尺寸调整。
func (*v2ProtocolHandler) supportsTerminalResizing() bool { return false }

// v1ProtocolHandler implements the V1 protocol version for streaming command execution.
// v1ProtocolHandler 实现最初的 v1 协议；保留了一些历史兼容行为，见下方 waitForStreams 中的说明。
type v1ProtocolHandler struct{}

// waitForStreams 收齐 v1 的四条流；由于历史兼容原因，这里会额外 Reset error 流并关闭 stdin 流。
func (*v1ProtocolHandler) waitForStreams(streams <-chan streamAndReply, expectedStreams int, expired <-chan time.Time) (*connectionContext, error) {
	ctx := &connectionContext{}
	receivedStreams := 0
	replyChan := make(chan struct{})
	// 返回时关闭 stop，清理所有等待应答的 goroutine。
	stop := make(chan struct{})
	defer close(stop)
WaitForStreams:
	for {
		select {
		case stream := <-streams:
			// 依据流头部的类型把流填入上下文。
			streamType := stream.Headers().Get(StreamType)
			switch streamType {
			case StreamTypeError:
				ctx.writeStatus = v1WriteStatusFunc(stream)

				// This defer statement shouldn't be here, but due to previous refactoring, it ended up in
				// here. This is what 1.0.x kubelets do, so we're retaining that behavior. This is fixed in
				// the v2ProtocolHandler.
				// 该 defer 属于历史遗留：1.0.x kubelet 的客户端依赖此行为，故保留以兼容旧版本。
				defer stream.Reset()

				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeStdin:
				ctx.stdinStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeStdout:
				ctx.stdoutStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			case StreamTypeStderr:
				ctx.stderrStream = stream
				go waitStreamReply(stream.replySent, replyChan, stop)
			default:
				runtime.HandleError(fmt.Errorf("unexpected stream type: %q", streamType))
			}
		case <-replyChan:
			// 收齐应答后跳出等待循环。
			receivedStreams++
			if receivedStreams == expectedStreams {
				break WaitForStreams
			}
		case <-expired:
			// 建流超时则放弃会话。
			// TODO find a way to return the error to the user. Maybe use a separate
			// stream to report errors?
			return nil, errors.New("timed out waiting for client to create streams")
		}
	}

	// 兼容旧客户端：建流完成后立即关闭 stdin，避免对端一直等待输入结束信号。
	if ctx.stdinStream != nil {
		ctx.stdinStream.Close()
	}

	return ctx, nil
}

// supportsTerminalResizing returns false because v1ProtocolHandler doesn't support it.
// supportsTerminalResizing 对 v1 返回 false，因为该协议不支持终端尺寸调整。
func (*v1ProtocolHandler) supportsTerminalResizing() bool { return false }

// handleResizeEvents 从 resize 流持续解码 JSON 格式的 TerminalSize，并转发到 channel。
func handleResizeEvents(reqctx context.Context, stream io.Reader, channel chan<- TerminalSize) {
	// 该 goroutine 由 createStreams 启动，崩溃时记录而不是让整个进程挂掉。
	defer runtime.HandleCrash()
	// 循环结束（流关闭或解码失败）时关闭 channel，通知消费者不会再有尺寸事件。
	defer close(channel)

	decoder := json.NewDecoder(stream)
	for {
		size := TerminalSize{}
		// 一次解码一条 JSON 消息；读到流末尾或格式错误即结束循环。
		if err := decoder.Decode(&size); err != nil {
			break
		}
		select {
		case channel <- size:
		case <-reqctx.Done():
			// 请求已结束而消费者不再接收，及时退出以避免 goroutine 泄漏。
			// To prevent go routine leak.
			return
		}
	}
}

// v1WriteStatusFunc 构造旧协议使用的状态写入函数：只在失败时向 error 流写纯文本消息。
func v1WriteStatusFunc(stream io.Writer) func(status *streamStatusError) error {
	return func(status *streamStatusError) error {
		// 成功状态不占用 error 流，直接跳过。
		if status.status().Status == statusSuccess {
			return nil // send error messages
		}
		_, err := stream.Write([]byte(status.Error()))
		return err
	}
}

// v4WriteStatusFunc returns a WriteStatusFunc that marshals a status object
// as json in the error channel.
func v4WriteStatusFunc(stream io.Writer) func(status *streamStatusError) error {
	return func(status *streamStatusError) error {
		bs, err := json.Marshal(status.status())
		if err != nil {
			return err
		}
		_, err = stream.Write(bs)
		return err
	}
}
