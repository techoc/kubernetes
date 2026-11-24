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
	"fmt"
	"net/http"
	"time"

	"k8s.io/cri-streaming/pkg/streaming/internal/httpresponse"
	"k8s.io/streaming/pkg/httpstream/wsstream"
	"k8s.io/streaming/pkg/runtime"
)

const (
	// 五个 WebSocket 通道的编号：stdin(0)、stdout(1)、stderr(2)、error(3)、resize(4)。
	stdinChannel = iota
	stdoutChannel
	stderrChannel
	errorChannel
	resizeChannel

	// 服务端支持的 WebSocket 子协议：前两个是 v4 之前的旧协议（二进制/base64 两种帧格式），
	// 后两个是 v4 协议，两者差别在于 error 流上的状态对象是否以 JSON 编码。
	preV4BinaryWebsocketProtocol = wsstream.ChannelWebSocketProtocol
	preV4Base64WebsocketProtocol = wsstream.Base64ChannelWebSocketProtocol
	v4BinaryWebsocketProtocol    = "v4." + wsstream.ChannelWebSocketProtocol
	v4Base64WebsocketProtocol    = "v4." + wsstream.Base64ChannelWebSocketProtocol
)

// createChannels 返回 shell 连接所需的五个通道类型及大致的双工模式：
// 按请求打开 stdin/stdout/stderr，error 通道始终可写，resize 通道始终可读（是否真正使用由上层决定）。
func createChannels(opts *Options) []wsstream.ChannelType {
	// 只打开请求的通道，但 error 通道无条件打开，用于回传状态。
	channels := make([]wsstream.ChannelType, 5)
	channels[stdinChannel] = readChannel(opts.Stdin)
	channels[stdoutChannel] = writeChannel(opts.Stdout)
	channels[stderrChannel] = writeChannel(opts.Stderr)
	channels[errorChannel] = wsstream.WriteChannel
	channels[resizeChannel] = wsstream.ReadChannel
	return channels
}

// readChannel 在需要该通道时返回 ReadChannel，否则返回 IgnoreChannel 让底层忽略它。
func readChannel(real bool) wsstream.ChannelType {
	if real {
		return wsstream.ReadChannel
	}
	return wsstream.IgnoreChannel
}

// writeChannel 在需要该通道时返回 WriteChannel，否则返回 IgnoreChannel 让底层忽略它。
func writeChannel(real bool) wsstream.ChannelType {
	if real {
		return wsstream.WriteChannel
	}
	return wsstream.IgnoreChannel
}

// createWebSocketStreams 完成 WebSocket 升级并构建 connectionContext，其中包含 exec 或 attach 所需的各条流。
func createWebSocketStreams(req *http.Request, w http.ResponseWriter, opts *Options, idleTimeout time.Duration) (*connectionContext, bool) {
	// 根据请求参数决定打开哪些通道。
	channels := createChannels(opts)
	// 注册所有可接受的协议配置；空字符串表示客户端未协商版本时的默认二进制协议。
	conn := wsstream.NewConn(map[string]wsstream.ChannelProtocolConfig{
		"": {
			Binary:   true,
			Channels: channels,
		},
		preV4BinaryWebsocketProtocol: {
			Binary:   true,
			Channels: channels,
		},
		preV4Base64WebsocketProtocol: {
			Binary:   false,
			Channels: channels,
		},
		v4BinaryWebsocketProtocol: {
			Binary:   true,
			Channels: channels,
		},
		v4Base64WebsocketProtocol: {
			Binary:   false,
			Channels: channels,
		},
	})
	conn.SetIdleTimeout(idleTimeout)
	// Open 内部完成协议协商与 HTTP Upgrade；使用原始 ResponseWriter 以绕过中间包装。
	negotiatedProtocol, streams, err := conn.Open(httpresponse.GetOriginal(w), req)
	if err != nil {
		// 升级失败：记录原因并向上层报告失败。
		runtime.HandleError(fmt.Errorf("unable to upgrade websocket connection: %v", err))
		return nil, false
	}

	// TODO：将该逻辑泛化到 SPDY 与 WebSocket，并移到本方法之外。
	// 向编号最小的可写通道发一条空消息，通知客户端连接已建立；优先 stdout，其次 stderr，最后 error。
	switch {
	case opts.Stdout:
		streams[stdoutChannel].Write([]byte{})
	case opts.Stderr:
		streams[stderrChannel].Write([]byte{})
	default:
		streams[errorChannel].Write([]byte{})
	}

	// 把升级得到的流按通道编号装入上下文，resize 流的消费者由 createStreams 统一启动。
	ctx := &connectionContext{
		conn:         conn,
		stdinStream:  streams[stdinChannel],
		stdoutStream: streams[stdoutChannel],
		stderrStream: streams[stderrChannel],
		tty:          opts.TTY,
		resizeStream: streams[resizeChannel],
	}

	// v4 协议在 error 流上写 JSON 状态对象（含退出码），旧协议只写纯文本错误消息。
	switch negotiatedProtocol {
	case v4BinaryWebsocketProtocol, v4Base64WebsocketProtocol:
		ctx.writeStatus = v4WriteStatusFunc(streams[errorChannel])
	default:
		ctx.writeStatus = v1WriteStatusFunc(streams[errorChannel])
	}

	return ctx, true
}
