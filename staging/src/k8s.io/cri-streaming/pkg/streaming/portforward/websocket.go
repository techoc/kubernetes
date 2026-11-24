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

package portforward

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/cri-streaming/pkg/streaming/internal/httpresponse"
	"k8s.io/streaming/pkg/httpstream/wsstream"
	"k8s.io/streaming/pkg/runtime"
)

const (
	// dataChannel 表示数据通道在通道序列中的下标。
	dataChannel = iota
	// errorChannel 表示错误通道在通道序列中的下标。
	errorChannel

	// v4BinaryWebsocketProtocol 是 v4 版本的二进制通道 WebSocket 子协议名。
	v4BinaryWebsocketProtocol = "v4." + wsstream.ChannelWebSocketProtocol
	// v4Base64WebsocketProtocol 是 v4 版本的 Base64 编码通道 WebSocket 子协议名。
	v4Base64WebsocketProtocol = "v4." + wsstream.Base64ChannelWebSocketProtocol
)

// V4Options 保存端口转发所需的端口信息；所有字段都必须在 CRI 的 PortForwardRequest 中显式声明。
type V4Options struct {
	// Ports 是需要转发的目标端口列表，每个端口对应一对数据/错误流。
	Ports []int32
}

// NewV4Options 从 HTTP 请求中解析端口列表；非 WebSocket 请求返回空选项。
func NewV4Options(req *http.Request) (*V4Options, error) {
	if !wsstream.IsWebSocketRequest(req) {
		// HTTP Upgrade（SPDY）路径的端口随每条流传递，不需要从 URL 解析。
		return &V4Options{}, nil
	}

	// WebSocket 路径通过 port 查询参数指定端口，参数可重复出现或用逗号分隔。
	portStrings := req.URL.Query()[PortHeader]
	if len(portStrings) == 0 {
		return nil, fmt.Errorf("query parameter %q is required", PortHeader)
	}

	ports := make([]int32, 0, len(portStrings))
	for _, portString := range portStrings {
		if len(portString) == 0 {
			return nil, fmt.Errorf("query parameter %q cannot be empty", PortHeader)
		}
		// 逗号分隔的写法允许一次传入多个端口。
		for _, p := range strings.Split(portString, ",") {
			// 端口必须是 1 到 65535 之间的十进制整数。
			port, err := strconv.ParseUint(p, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("unable to parse %q as a port: %v", portString, err)
			}
			if port < 1 {
				return nil, fmt.Errorf("port %q must be > 0", portString)
			}
			ports = append(ports, int32(port))
		}
	}

	return &V4Options{
		Ports: ports,
	}, nil
}

// BuildV4Options 根据给定的端口列表直接构造 V4Options。
func BuildV4Options(ports []int32) (*V4Options, error) {
	return &V4Options{Ports: ports}, nil
}

// handleWebSocketStreams 通过 PortForwarder 处理 WebSocket 端口转发请求。
// 每个端口分配一对通道（数据通道 n、错误通道 n+1），并在每个流的开头以小端无符号 16 位整数写入对应端口。
func handleWebSocketStreams(req *http.Request, w http.ResponseWriter, portForwarder PortForwarder, podName string, uid string, opts *V4Options, supportedPortForwardProtocols []string, idleTimeout, streamCreationTimeout time.Duration) error {
	// 每个端口申请两个通道：一个可读可写的数据通道和一个只写的错误通道。
	channels := make([]wsstream.ChannelType, 0, len(opts.Ports)*2)
	for i := 0; i < len(opts.Ports); i++ {
		channels = append(channels, wsstream.ReadWriteChannel, wsstream.WriteChannel)
	}
	// 同时接受空子协议、二进制子协议与 Base64 子协议；通道布局一致，只是编码方式不同。
	conn := wsstream.NewConn(map[string]wsstream.ChannelProtocolConfig{
		"": {
			Binary:   true,
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
	// 空闲超时防止连接长期无数据却不释放。
	conn.SetIdleTimeout(idleTimeout)
	// 打开连接并完成协议升级；返回流的顺序与上面声明的通道顺序一致。
	_, streams, err := conn.Open(httpresponse.GetOriginal(w), req)
	if err != nil {
		err = fmt.Errorf("unable to upgrade websocket connection: %v", err)
		return err
	}
	defer conn.Close()
	streamPairs := make([]*websocketStreamPair, len(opts.Ports))
	for i := range streamPairs {
		// 按端口顺序把相邻的两个通道组装成流对。
		streamPair := websocketStreamPair{
			port:        opts.Ports[i],
			dataStream:  streams[i*2+dataChannel],
			errorStream: streams[i*2+errorChannel],
		}
		streamPairs[i] = &streamPair

		portBytes := make([]byte, 2)
		// 每个流的前两个字节携带小端编码的端口号，供客户端识别流对应的端口。
		// 端口恒为正数，可安全转换为 16 位无符号整数。
		binary.LittleEndian.PutUint16(portBytes, uint16(streamPair.port))
		streamPair.dataStream.Write(portBytes)
		streamPair.errorStream.Write(portBytes)
	}
	// 构造处理器，启动所有端口的转发并等待其结束。
	h := &websocketStreamHandler{
		conn:        conn,
		streamPairs: streamPairs,
		pod:         podName,
		uid:         uid,
		forwarder:   portForwarder,
	}
	h.run()

	return nil
}

// websocketStreamPair 表示一个端口对应的数据/错误通道对。
// 字段含义：port 是目标端口；dataStream 是可读写的数据通道；errorStream 是只写的错误通道。
type websocketStreamPair struct {
	port        int32
	dataStream  io.ReadWriteCloser
	errorStream io.WriteCloser
}

// websocketStreamHandler 在一条 WebSocket 连接上处理一次端口转发请求中的所有流对。
// 字段含义：conn 是 WebSocket 连接；streamPairs 是各端口的流对；pod 与 uid 标识目标 Pod；
// forwarder 是真正执行端口转发的组件。
type websocketStreamHandler struct {
	conn        *wsstream.Conn
	streamPairs []*websocketStreamPair
	pod         string
	uid         string
	forwarder   PortForwarder
}

// run 为每个流对启动一个转发协程，并等待它们全部结束。
func (h *websocketStreamHandler) run() {
	// 用 WaitGroup 等待所有端口的转发协程完成。
	wg := sync.WaitGroup{}
	wg.Add(len(h.streamPairs))

	for _, pair := range h.streamPairs {
		// 各端口互不依赖，可以并发转发；复制 pair 避免闭包捕获循环变量。
		p := pair
		go func() {
			defer wg.Done()
			h.portForward(p)
		}()
	}

	// 阻塞直到所有转发结束，通常意味着客户端断开或连接超时。
	wg.Wait()
}

// portForward 为给定流对调用 forwarder.PortForward，并在出错时把错误写入错误通道。
func (h *websocketStreamHandler) portForward(p *websocketStreamPair) {
	ctx := context.Background()
	// 转发结束后关闭两条通道，通知对端不会再有数据。
	defer p.dataStream.Close()
	defer p.errorStream.Close()

	klog.V(5).InfoS("Connection invoking forwarder.PortForward for port", "connection", h.conn, "port", p.port)
	// 阻塞执行端口转发，数据直接从 WebSocket 数据通道读写。
	err := h.forwarder.PortForward(ctx, h.pod, h.uid, p.port, p.dataStream)
	klog.V(5).InfoS("Connection done invoking forwarder.PortForward for port", "connection", h.conn, "port", p.port)

	if err != nil {
		msg := fmt.Errorf("error forwarding port %d to pod %s, uid %v: %v", p.port, h.pod, h.uid, err)
		runtime.HandleError(msg)
		// 把错误文本写入错误通道，让客户端感知转发失败。
		fmt.Fprint(p.errorStream, msg.Error())
	}
}
