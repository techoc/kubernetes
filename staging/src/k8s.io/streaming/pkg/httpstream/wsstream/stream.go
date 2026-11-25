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

package wsstream

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/websocket"

	"k8s.io/klog/v2"
	"k8s.io/streaming/pkg/runtime"
)

// WebSocket 子协议 "binary.k8s.io" 只向客户端发送消息，忽略客户端发来的消息。
// 客户端收到的是写入流的原始字节，也支持零字节消息。
//
// 单向协议：客户端发来的内容由 IgnoreReceivesWithLogger 读取并丢弃；
// 该协议没有通道号，无法区分回传目标。
const binaryWebSocketProtocol = "binary.k8s.io"

// WebSocket 子协议 "base64.binary.k8s.io" 只向客户端发送消息，忽略客户端发来的消息。
// 客户端收到的是写入流内容的 base64 文本，也支持零字节消息。
//
// 与二进制变体行为一致，只是将载荷 base64 文本化，供只接受文本帧的客户端使用；
// 解码由客户端负责。
const base64BinaryWebSocketProtocol = "base64.binary.k8s.io"

// ReaderProtocolConfig 描述一个承载单条流的 WebSocket 子协议。
//
// 单流协议不需要通道表，仅由 Binary 决定使用原始字节还是 base64 文本。
type ReaderProtocolConfig struct {
	// Binary 为 true 时发送原始字节，为 false 时对每帧做 base64 编码。
	Binary bool
}

// NewDefaultReaderProtocols 返回包含空协议名、"binary.k8s.io" 和
// "base64.binary.k8s.io" 的单流协议表。
//
// 空协议名与二进制协议使用原始字节，base64 变体使用文本编码；
// 客户端不指定子协议时仍能按原始字节方式接收流。
func NewDefaultReaderProtocols() map[string]ReaderProtocolConfig {
	return map[string]ReaderProtocolConfig{
		"":                            {Binary: true},
		binaryWebSocketProtocol:       {Binary: true},
		base64BinaryWebSocketProtocol: {Binary: false},
	}
}

// Reader 通过 WebSocket 向客户端返回任意字节流。
//
// 与 Conn 不同，Reader 只向客户端发送数据，不解析客户端数据；无缓冲的 err 通道
// 是 Copy 与流 goroutine 之间唯一的完成信号，同时传递结果并建立同步。
// logger 供接收 goroutine 关联请求日志；r 是数据源；ping 在数据前发送空帧以尽早打通连接。
// timeout 在每次读写活动后顺延；protocols 按子协议名给出编码方式，selectedProtocol
// 记录握手选定的协议；handleCrash 可在测试中替换，以控制 panic 处理行为。
type Reader struct {
	logger           klog.Logger
	err              chan error
	r                io.Reader
	ping             bool
	timeout          time.Duration
	protocols        map[string]ReaderProtocolConfig
	selectedProtocol string

	handleCrash func(ctx context.Context, additionalHandlers ...func(context.Context, interface{})) // 测试中可替换
}

// NewReader 创建 WebSocket 管道，将 r 的内容复制到客户端连接。
// ping 为 true 时，在开始读取流之前发送一条零长度消息。
// protocols 将子协议名映射到 ReaderProtocolConfig；客户端未指定子协议时使用空字符串。
//
// 保留旧签名只是为了兼容调用方，内部固定使用 klog.Background()；
// 使用上下文日志的代码应调用 NewReaderWithLogger。
//
//logcheck:context // NewReaderWithLogger should be used instead of NewReader in code which supports contextual logging.
func NewReader(r io.Reader, ping bool, protocols map[string]ReaderProtocolConfig) *Reader {
	return NewReaderWithLogger(klog.Background(), r, ping, protocols)
}

// NewReaderWithLogger 使用指定日志记录器创建 WebSocket 管道，将 r 的内容复制到客户端。
// ping 为 true 时，在开始读取流之前发送一条零长度消息。
// protocols 将子协议名映射到 ReaderProtocolConfig；客户端未指定子协议时使用空字符串。
//
// err 通道不带缓冲：Copy 是唯一接收者，无缓冲确保流的 goroutine 交付结果后才能退出。
func NewReaderWithLogger(logger klog.Logger, r io.Reader, ping bool, protocols map[string]ReaderProtocolConfig) *Reader {
	return &Reader{
		logger:      logger,
		r:           r,
		err:         make(chan error),
		ping:        ping,
		protocols:   protocols,
		handleCrash: runtime.HandleCrashWithContext,
	}
}

// SetIdleTimeout 设置 Reader 在读写空闲多久后超时；未设置时不启用空闲超时。
//
// 必须在 Copy 之前设置：流一旦开始，超时值会被读循环和接收 goroutine 同时读取，
// 中途修改既无同步保证，也不会影响已经阻塞的操作。
func (r *Reader) SetIdleTimeout(duration time.Duration) {
	r.timeout = duration
}

// 把协议表展开成候选列表交给通用握手函数；map 遍历顺序无意义，
// 最终协议由客户端提议顺序和允许集合决定，这里只负责校验合法性。
func (r *Reader) handshake(config *websocket.Config, req *http.Request) error {
	supportedProtocols := make([]string, 0, len(r.protocols))
	for p := range r.protocols {
		supportedProtocols = append(supportedProtocols, p)
	}
	return handshake(config, req, supportedProtocols)
}

// Copy 将 Reader 的内容写入 HTTP 响应，并在返回时关闭创建的 WebSocket。
//
// ServeHTTP 会阻塞到连接关闭，所以放进 goroutine；当前 goroutine 等待 err 通道，
// 直到流结束或失败才返回。panic 由 goroutine 内的 handleCrash 兜住，
// 避免流式 handler 的异常越过 HTTP 框架边界。
func (r *Reader) Copy(w http.ResponseWriter, req *http.Request) error {
	go func() {
		// 这里包一层 handleCrash 是为了把 goroutine 内的 panic 限制在本请求内。
		defer r.handleCrash(req.Context())
		websocket.Server{Handshake: r.handshake, Handler: r.handle}.ServeHTTP(w, req)
	}()
	return <-r.err
}

// handle 实现 WebSocket 服务端处理器。
//
// 客户端主动断开和服务端流发送完毕都可能触发关闭；用 sync.Once 确保只关一次，
// 避免后到的路径对已关闭连接重复调用 Close。
func (r *Reader) handle(ws *websocket.Conn) {
	// 客户端断开或服务端完成发送时，以先发生者为准关闭连接。
	closeConnOnce := &sync.Once{}
	closeConn := func() {
		closeConnOnce.Do(func() {
			ws.Close()
		})
	}

	// 握手已把协议列表收敛为单项，第一项即最终选择。
	negotiated := ws.Config().Protocol
	r.selectedProtocol = negotiated[0]
	// 先注册 close(r.err) 再注册 closeConn：defer 后进先出，
	// 保证连接先关闭，等待 Copy 的一方醒来时连接已经不可写。
	defer close(r.err)
	defer closeConn()

	// 单流协议不期待客户端发数据，但仍需有人读取，否则底层连接会卡住；
	// 对端断开时顺手关连接，让仍在写的 messageCopy 尽早收到错误。
	go func() {
		// 常驻 goroutine 内的 panic 必须就地兜住，不能靠 Copy 的 defer。
		defer runtime.HandleCrashWithLogger(r.logger)
		// 单流协议不要求客户端发送数据，但仍需读取以维持 WebSocket 连接；
		// 客户端关闭后，服务端也关闭连接，让阻塞的发送尽早失败。
		IgnoreReceivesWithLogger(r.logger, ws, r.timeout)
		closeConn()
	}()

	// 把编码方式由协议配置翻译成 messageCopy 的布尔开关，避免调用方自行推导。
	r.err <- messageCopy(ws, r.r, !r.protocols[r.selectedProtocol].Binary, r.ping, r.timeout)
}

// 每次读写前顺延截止时间，超时表达的是“空闲多久”而非连接总寿命；
// 未配置时不设 deadline，避免零值时间立即过期。
func resetTimeout(ws *websocket.Conn, timeout time.Duration) {
	if timeout > 0 {
		ws.SetDeadline(time.Now().Add(timeout))
	}
}

// 按底层 Reader 的每次 Read 结果发一帧，不攒批：这样长流能持续向客户端推进，
// 也不会因为等待填满缓冲区而引入额外延迟。EOF 视为正常结束，其他错误原样上报。
func messageCopy(ws *websocket.Conn, r io.Reader, base64Encode, ping bool, timeout time.Duration) error {
	// 2KB 兼顾短消息的及时性和大块数据的系统调用次数；它是复用缓冲，
	// 每帧都按 n 截断后再发送，不会把上一次读取的残留字节带出去。
	buf := make([]byte, 2048)
	if ping {
		// ping 帧必须按协商的编码方式发送空消息，否则客户端按文本模式解析会报错。
		resetTimeout(ws, timeout)
		if base64Encode {
			if err := websocket.Message.Send(ws, ""); err != nil {
				return err
			}
		} else {
			if err := websocket.Message.Send(ws, []byte{}); err != nil {
				return err
			}
		}
	}
	for {
		resetTimeout(ws, timeout)
		n, err := r.Read(buf)
		if err != nil {
			// EOF 表示流正常结束；其余错误必须让 Copy 的调用方看到，
			// 不能悄悄吞掉，否则上层以为数据完整。
			if err == io.EOF {
				return nil
			}
			return err
		}
		// n==0 且 err==nil 时什么也不发，避免制造无意义的空帧干扰帧序。
		if n > 0 {
			if base64Encode {
				if err := websocket.Message.Send(ws, base64.StdEncoding.EncodeToString(buf[:n])); err != nil {
					return err
				}
			} else {
				if err := websocket.Message.Send(ws, buf[:n]); err != nil {
					return err
				}
			}
		}
	}
}
