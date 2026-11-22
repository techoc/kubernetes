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
	// idleTimeout is the read/write deadline set for websocket server connection. Reading
	// or writing the connection will return an i/o timeout if this deadline is exceeded.
	// Currently, we use the same value as the kubelet websocket server.
	// 中文说明：与 kubelet 保持一致，避免两端超时策略不同造成“半死”连接。
	defaultIdleConnectionTimeout = 4 * time.Hour

	// Deadline for writing errors to the websocket connection before io/timeout.
	// 中文说明：错误写不出去时不能无限等待，否则会拖住 handler。
	writeErrorDeadline = 10 * time.Second
)

// Options contains details about which streams are required for
// remote command execution.
// 中文说明：只有被请求的流才会真正建立，未请求的通道在协议里标记为 Ignore。
type Options struct {
	Stdin  bool
	Stdout bool
	Stderr bool
	Tty    bool
}

// conns contains the connection and streams used when
// forwarding an attach or execute session into a container.
// 中文说明：writeStatus 把 apiserver 的错误以 Status JSON 写入专用错误通道，
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

// Create WebSocket server streams to respond to a WebSocket client. Creates the streams passed
// in the stream options.
//
// 中文说明：resize 事件在独立 goroutine 中读取，并捕获 panic——resize 流关闭时的
// panic 不应传播到 HTTP handler。
func webSocketServerStreams(req *http.Request, w http.ResponseWriter, opts Options) (*conns, error) {
	ctx, err := createWebSocketStreams(req, w, opts)
	if err != nil {
		return nil, err
	}

	if ctx.resizeStream != nil {
		ctx.resizeChan = make(chan remotecommand.TerminalSize)
		go func() {
			// Resize channel closes in panic case, and panic does not take down caller.
			defer func() {
				if p := recover(); p != nil {
					// Standard panic logging.
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

// Read terminal resize events off of passed stream and queue into passed channel.
// 中文说明：解码失败即视为流结束并关闭 channel；ctx.Done 分支避免请求结束时
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
			// To avoid leaking this routine, exit if the http request finishes. This path
			// would generally be hit if starting the process fails and nothing is started to
			// ingest these resize events.
			return
		}
	}
}

// createChannels returns the standard channel types for a shell connection (STDIN 0, STDOUT 1, STDERR 2)
// along with the approximate duplex value. It also creates the error (3) and resize (4) channels.
//
// 中文说明：错误通道始终写、resize 始终读——客户端必须能收到错误，也能随时调整窗口。
func createChannels(opts Options) []wsstream.ChannelType {
	// open the requested channels, and always open the error channel
	channels := make([]wsstream.ChannelType, 5)
	channels[constants.StreamStdIn] = readChannel(opts.Stdin)
	channels[constants.StreamStdOut] = writeChannel(opts.Stdout)
	channels[constants.StreamStdErr] = writeChannel(opts.Stderr)
	channels[constants.StreamErr] = wsstream.WriteChannel
	channels[constants.StreamResize] = wsstream.ReadChannel
	return channels
}

// readChannel returns wsstream.ReadChannel if real is true, or wsstream.IgnoreChannel.
func readChannel(real bool) wsstream.ChannelType {
	if real {
		return wsstream.ReadChannel
	}
	return wsstream.IgnoreChannel
}

// writeChannel returns wsstream.WriteChannel if real is true, or wsstream.IgnoreChannel.
func writeChannel(real bool) wsstream.ChannelType {
	if real {
		return wsstream.WriteChannel
	}
	return wsstream.IgnoreChannel
}

// createWebSocketStreams returns a "conns" struct containing the websocket connection and
// streams needed to perform an exec or an attach.
//
// 中文说明：仅声明 v5 子协议是有意为之——apiserver 作为服务端只需支持 kubelet 当前的
// 协议版本；不兼容的旧客户端会被协商拒绝。
func createWebSocketStreams(req *http.Request, w http.ResponseWriter, opts Options) (*conns, error) {
	channels := createChannels(opts)
	conn := wsstream.NewConn(map[string]wsstream.ChannelProtocolConfig{
		// WebSocket server only supports remote command version 5.
		constants.StreamProtocolV5Name: {
			Binary:   true,
			Channels: channels,
		},
	})
	conn.SetIdleTimeout(defaultIdleConnectionTimeout)
	// Opening the connection responds to WebSocket client, negotiating
	// the WebSocket upgrade connection and the subprotocol.
	_, streams, err := conn.Open(w, req)
	if err != nil {
		return nil, err
	}

	// Send an empty message to the lowest writable channel to notify the client the connection is established
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

	// writeStatus returns a WriteStatusFunc that marshals a given api Status
	// as json in the error channel.
	ctx.writeStatus = func(status *apierrors.StatusError) error {
		bs, err := json.Marshal(status.Status())
		if err != nil {
			return err
		}
		// Write status error to error stream with deadline.
		conn.SetWriteDeadline(writeErrorDeadline)
		_, err = streams[constants.StreamErr].Write(bs)
		return err
	}

	return ctx, nil
}
