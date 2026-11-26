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

package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	constants "k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream/wsstream"
)

const (
	// idleTimeout 是为 websocket 服务端连接设置的读写截止时间，超过后读写会返回
	// i/o 超时。目前与 kubelet websocket 服务端使用相同的值，保持一致以避免
	// 两端超时策略不同造成“半死”连接。
	defaultIdleConnectionTimeout = 4 * time.Hour

	// 向 websocket 连接写错误的截止时间，超过后触发 io/timeout。
	// 错误写不出去时不能无限等待，否则会拖住 handler。
	writeErrorDeadline = 10 * time.Second
)

// Options 包含远程命令执行需要哪些流的细节。
// 只有被请求的流才会真正建立，未请求的通道在协议里标记为 Ignore。
type Options struct {
	Stdin  bool
	Stdout bool
	Stderr bool
	Tty    bool
}

// conns 包含把 attach 或 exec 会话转发进容器时使用的连接与各流。
// writeStatus 把 apiserver 的错误以 Status JSON 写入专用错误通道，
// 客户端据此区分“会话结束”与“传输中断”。
type conns struct {
	conn         io.Closer
	stdinStream  io.ReadCloser
	stdoutStream io.WriteCloser
	stderrStream io.WriteCloser
	writeStatus  func(status *apierrors.StatusError) error
	resizeStream io.ReadCloser
	resizeChan   chan remotecommand.TerminalSize
	tty          bool
}

// 创建 WebSocket 服务端流以响应 WebSocket 客户端，按传入的流选项创建各流。
// resize 事件在独立 goroutine 中读取，并捕获 panic——resize 流关闭时的
// panic 不应传播到 HTTP handler。
func webSocketServerStreams(req *http.Request, w http.ResponseWriter, opts Options) (*conns, error) {
	ctx, err := createWebSocketStreams(req, w, opts)
	if err != nil {
		return nil, err
	}

	if ctx.resizeStream != nil {
		ctx.resizeChan = make(chan remotecommand.TerminalSize)
		go func() {
			// panic 时 resize channel 会关闭，且 panic 不会拖垮调用方。
			defer func() {
				if p := recover(); p != nil {
					// 标准的 panic 日志。
					for _, fn := range runtime.PanicHandlers {
						fn(req.Context(), p)
					}
				}
			}()
			handleResizeEvents(req.Context(), ctx.resizeStream, ctx.resizeChan)
		}()
	}

	return ctx, nil
}

// 从传入的流中读取终端 resize 事件并排队到传入的通道。
// 解码失败即视为流结束并关闭 channel；ctx.Done 分支避免请求结束时
// goroutine 泄漏。
func handleResizeEvents(ctx context.Context, stream io.Reader, channel chan<- remotecommand.TerminalSize) {
	defer close(channel)

	decoder := json.NewDecoder(stream)
	for {
		size := remotecommand.TerminalSize{}
		if err := decoder.Decode(&size); err != nil {
			break
		}

		select {
		case channel <- size:
		case <-ctx.Done():
			// 为避免该 goroutine 泄漏，HTTP 请求结束时就退出。若启动过程失败、
			// 没有任何角色消费这些 resize 事件，通常会走到这里。
			//
			return
		}
	}
}

// createChannels 返回 shell 连接的标准通道类型（STDIN 0、STDOUT 1、STDERR 2）
// 及近似的 duplex 值，同时创建错误（3）与 resize（4）通道。
// 错误通道始终写、resize 始终读——客户端必须能收到错误，也能随时调整窗口。
func createChannels(opts Options) []wsstream.ChannelType {
	// 打开请求的各 channel，并总是打开错误 channel
	channels := make([]wsstream.ChannelType, 5)
	channels[constants.StreamStdIn] = readChannel(opts.Stdin)
	channels[constants.StreamStdOut] = writeChannel(opts.Stdout)
	channels[constants.StreamStdErr] = writeChannel(opts.Stderr)
	channels[constants.StreamErr] = wsstream.WriteChannel
	channels[constants.StreamResize] = wsstream.ReadChannel
	return channels
}

// real 为 true 时返回 wsstream.ReadChannel，否则返回 wsstream.IgnoreChannel。
func readChannel(real bool) wsstream.ChannelType {
	if real {
		return wsstream.ReadChannel
	}
	return wsstream.IgnoreChannel
}

// real 为 true 时返回 wsstream.WriteChannel，否则返回 wsstream.IgnoreChannel。
func writeChannel(real bool) wsstream.ChannelType {
	if real {
		return wsstream.WriteChannel
	}
	return wsstream.IgnoreChannel
}

// createWebSocketStreams 返回一个 "conns" 结构，包含执行 exec 或 attach 所需的
// websocket 连接与各流。仅声明 v5 子协议是有意为之——apiserver 作为服务端只需支持
// kubelet 当前的协议版本；不兼容的旧客户端会被协商拒绝。
func createWebSocketStreams(req *http.Request, w http.ResponseWriter, opts Options) (*conns, error) {
	channels := createChannels(opts)
	conn := wsstream.NewConn(map[string]wsstream.ChannelProtocolConfig{
		// WebSocket 服务端只支持 remote command 版本 5。
		constants.StreamProtocolV5Name: {
			Binary:   true,
			Channels: channels,
		},
	})
	conn.SetIdleTimeout(defaultIdleConnectionTimeout)
	// 打开连接即响应 WebSocket 客户端，协商
	// WebSocket 升级连接与子协议。
	_, streams, err := conn.Open(w, req)
	if err != nil {
		return nil, err
	}

	// 向最低的可写 channel 发送空消息，通知客户端连接已建立
	switch {
	case opts.Stdout:
		_, err = streams[constants.StreamStdOut].Write([]byte{})
	case opts.Stderr:
		_, err = streams[constants.StreamStdErr].Write([]byte{})
	default:
		_, err = streams[constants.StreamErr].Write([]byte{})
	}
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("write error during websocket server creation: %v", err)
	}

	ctx := &conns{
		conn:         conn,
		stdinStream:  streams[constants.StreamStdIn],
		stdoutStream: streams[constants.StreamStdOut],
		stderrStream: streams[constants.StreamStdErr],
		tty:          opts.Tty,
		resizeStream: streams[constants.StreamResize],
	}

	// writeStatus 返回一个 WriteStatusFunc，把给定的 api Status
	// 以 json 形式写入错误 channel。
	ctx.writeStatus = func(status *apierrors.StatusError) error {
		bs, err := json.Marshal(status.Status())
		if err != nil {
			return err
		}
		// 带 deadline 把状态错误写入错误流。
		conn.SetWriteDeadline(writeErrorDeadline)
		_, err = streams[constants.StreamErr].Write(bs)
		return err
	}

	return ctx, nil
}
