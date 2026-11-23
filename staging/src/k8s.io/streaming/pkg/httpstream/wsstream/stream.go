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

// The WebSocket subprotocol "binary.k8s.io" will only send messages to the
// client and ignore messages sent to the server. The received messages are
// the exact bytes written to the stream. Zero byte messages are possible.
//
// 单向协议：客户端发来的内容一律被 IgnoreReceivesWithLogger 吞掉，
// 因为这里没有通道号可用来区分回传目标，回显只会造成语义混乱。
const binaryWebSocketProtocol = "binary.k8s.io"

// The WebSocket subprotocol "base64.binary.k8s.io" will only send messages to the
// client and ignore messages sent to the server. The received messages are
// a base64 version of the bytes written to the stream. Zero byte messages are
// possible.
//
// 与二进制变体行为一致，只是把载荷做 base64 文本化，供只接受文本帧的客户端使用；
// 解码由客户端负责，服务端不额外保存编码状态。
const base64BinaryWebSocketProtocol = "base64.binary.k8s.io"

// ReaderProtocolConfig describes a websocket subprotocol with one stream.
//
// 单流协议不需要通道表，唯一的编码选择就是配置里的全部语义；
// 布尔值 false 即代表整条流都走 base64。
type ReaderProtocolConfig struct {
	// true 表示直接发送原始字节，false 表示对每帧做 base64 编码。
	Binary bool
}

// NewDefaultReaderProtocols returns a stream protocol map with the
// subprotocols "", "channel.k8s.io", "base64.channel.k8s.io".
//
// 空协议名与二进制协议映射到同一编码，base64 变体单独列出；
// 这样客户端不指定子协议时仍能按原始字节方式接收流。
func NewDefaultReaderProtocols() map[string]ReaderProtocolConfig {
	return map[string]ReaderProtocolConfig{
		"":                            {Binary: true},
		binaryWebSocketProtocol:       {Binary: true},
		base64BinaryWebSocketProtocol: {Binary: false},
	}
}

// Reader supports returning an arbitrary byte stream over a websocket channel.
//
// 与 Conn 相反，这里只往客户端写、不解析客户端数据；err 通道是 Copy 与
// 流的 goroutine 之间唯一的完成信号，同时承担结果和同步两件事。
// logger 传给接收 goroutine 以便日志关联到具体请求；err 是 Copy 与流 goroutine 之间的完成信号。
// r 是数据源，ping 在数据前插入空帧以尽早打通连接；timeout 在每次读写成功后顺延。
// protocols 按子协议名给出编码方式，selectedProtocol 是握手选定的那一个。
// handleCrash 测试时可替换，避免真实 HandleCrash 把测试进程带崩。
type Reader struct {
	logger           klog.Logger
	err              chan error
	r                io.Reader
	ping             bool
	timeout          time.Duration
	protocols        map[string]ReaderProtocolConfig
	selectedProtocol string

	handleCrash func(ctx context.Context, additionalHandlers ...func(context.Context, interface{})) // overridable for testing
}

// NewReader creates a WebSocket pipe that will copy the contents of r to a provided
// WebSocket connection. If ping is true, a zero length message will be sent to the client
// before the stream begins reading.
//
// The protocols parameter maps subprotocol names to StreamProtocols. The empty string
// subprotocol name is used if websocket.Config.Protocol is empty.
//
// 保留旧签名只是兼容尚无上下文日志的调用方，内部固定用 Background logger；
// 新代码应传入带请求信息的 logger，否则异常日志缺少上下文。
//
//logcheck:context // NewReaderWithLogger should be used instead of NewReader in code which supports contextual logging.
func NewReader(r io.Reader, ping bool, protocols map[string]ReaderProtocolConfig) *Reader {
	return NewReaderWithLogger(klog.Background(), r, ping, protocols)
}

// NewReaderWithLogger creates a WebSocket pipe that will copy the contents of r to a provided
// WebSocket connection. If ping is true, a zero length message will be sent to the client
// before the stream begins reading.
//
// The protocols parameter maps subprotocol names to StreamProtocols. The empty string
// subprotocol name is used if websocket.Config.Protocol is empty.
//
// err 通道不带缓冲：Copy 是唯一的接收者，无缓冲能让它与流的 goroutine
// 严格同步，结果交付后不会再有第二个写入者。
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

// SetIdleTimeout sets the interval for both reads and writes before timeout. If not specified,
// there is no timeout on the reader.
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

// Copy the reader to the response. The created WebSocket is closed after this
// method completes.
//
// ServeHTTP 会阻塞到连接关闭，所以放进 goroutine；当前 goroutine 等在 err 通道上，
// 直到流结束或失败才返回。panic 通过 handleCrash 在 goroutine 内部兜住，
// 不让流式 handler 的异常越过 HTTP 框架边界。
func (r *Reader) Copy(w http.ResponseWriter, req *http.Request) error {
	go func() {
		// 这里包一层 handleCrash 是为了把 goroutine 内的 panic 限制在本请求内。
		defer r.handleCrash(req.Context())
		websocket.Server{Handshake: r.handshake, Handler: r.handle}.ServeHTTP(w, req)
	}()
	return <-r.err
}

// handle implements a WebSocket handler.
//
// 两条关闭路径并存：客户端主动断开，或服务端流发送完毕。用 sync.Once 确保
// 只关一次，否则后到的路径会对已关闭的连接再次 Close 并报错。
func (r *Reader) handle(ws *websocket.Conn) {
	// Close the connection when the client requests it, or when we finish streaming, whichever happens first
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
		// This blocks until the connection is closed.
		// Client should not send anything.
		IgnoreReceivesWithLogger(r.logger, ws, r.timeout)
		// Once the client closes, we should also close
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
