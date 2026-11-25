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

package remotecommand

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	gwebsocket "github.com/gorilla/websocket"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/remotecommand"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/transport/websocket"
	"k8s.io/klog/v2"
	"k8s.io/streaming/pkg/httpstream"
)

// writeDeadline 定义客户端向 WebSocket 连接写入必须在多长时间内完成，
// 超时即产生 i/o 超时错误。
// 写超时防止对端停止读取时写协程永久阻塞，进而让整个命令无法收尾。
const writeDeadline = 60 * time.Second

var (
	_ Executor          = &wsStreamExecutor{}
	_ streamCreator     = &wsStreamCreator{}
	_ httpstream.Stream = &stream{}

	// streamType2streamID 把 API 的流类型映射为协议中的单字节流 ID；
	// 这些 ID 在 v5.channel.k8s.io 协议中固定，不能随意调整。
	streamType2streamID = map[string]byte{
		v1.StreamTypeStdin:  remotecommand.StreamStdIn,
		v1.StreamTypeStdout: remotecommand.StreamStdOut,
		v1.StreamTypeStderr: remotecommand.StreamStdErr,
		v1.StreamTypeError:  remotecommand.StreamErr,
		v1.StreamTypeResize: remotecommand.StreamResize,
	}
)

const (
	// pingPeriod 定义发送心跳 "ping" 消息的频率。
	pingPeriod = 5 * time.Second
	// pingReadDeadline 定义在 WebSocket 读取超时报错前，等待响应心跳 "pong"
	// 消息的时间。该时长必须始终大于 "pingPeriod"。以 ping 周期来定义这个
	// 截止时间，相当于允许在触发超时前丢弃 "X"（例如 12）个 ping。
	pingReadDeadline = (pingPeriod * 12) + (1 * time.Second)
)

// wsStreamExecutor 负责通过 httpstream 连接传输标准 shell 流。
//
// 与 SPDY 不同，WebSocket 没有原生的多流复用，所有流共用一条连接，
// 因此写入必须串行化，读端依靠每帧首字节的流 ID 解复用。
type wsStreamExecutor struct {
	transport http.RoundTripper
	upgrader  websocket.ConnectionHolder
	method    string
	url       string
	// 按优先级排列的请求协议（例如 v5.channel.k8s.io 优先于 v4.channel.k8s.io）。
	protocols []string
	// 握手过程中选定的协议；握手失败时可能为空字符串。
	negotiated string
	// 向对端发送 "ping" 心跳消息的频率。
	heartbeatPeriod time.Duration
	// 接收 "pong" 响应允许的最长时间。
	heartbeatDeadline time.Duration
}

// NewWebSocketExecutor 创建一个仅协商 V5 协议的 WebSocket 执行器。
// 之所以只支持 V5 协议，是为了保证版本偏斜（version skew）场景下的正确行为。
// 旧版 API Server 会把 upgrade 请求代理到容器运行时中支持 V1-V4 的旧式
// WebSocket 服务器，而这些旧式 WebSocket 服务器不会处理新的 CLOSE 信号。
func NewWebSocketExecutor(config *restclient.Config, method, url string) (Executor, error) {
	return NewWebSocketExecutorForProtocols(config, method, url, remotecommand.StreamProtocolV5Name)
}

// NewWebSocketExecutorForProtocols 允许通过 WebSocket 连接执行命令。
//
// protocols 按优先级排列，服务端选择第一个双方都支持的子协议；调用方通常
// 只需使用 NewWebSocketExecutor。
func NewWebSocketExecutorForProtocols(config *restclient.Config, method, url string, protocols ...string) (Executor, error) {
	transport, upgrader, err := websocket.RoundTripperFor(config)
	if err != nil {
		return nil, fmt.Errorf("error creating websocket transports: %v", err)
	}
	return &wsStreamExecutor{
		transport:         transport,
		upgrader:          upgrader,
		method:            method,
		url:               url,
		protocols:         protocols,
		heartbeatPeriod:   pingPeriod,
		heartbeatDeadline: pingReadDeadline,
	}, nil
}

// Deprecated: 请改用 StreamWithContext，以避免可能的资源泄漏。
// 详见 https://github.com/kubernetes/kubernetes/pull/103177。
// 旧方法没有 context，调用方无法取消，连接可能一直存活。
func (e *wsStreamExecutor) Stream(options StreamOptions) error {
	return e.StreamWithContext(context.Background(), options)
}

// StreamWithContext 将 HTTPRequest 升级为 WebSocket 连接，并启动若干 goroutine
// 在连接上实现所需的流。"options" 参数决定请求哪些流，出错时返回 error。
// 该方法不能与同一个 executor 并发运行（因为 upgrader 中保存了状态）。
//
// 握手完成后先等待 readyChan：只有所有流创建完毕，读解复用循环才能安全启动，
// 否则可能收到指向尚未创建流的数据帧。
func (e *wsStreamExecutor) StreamWithContext(ctx context.Context, options StreamOptions) error {
	req, err := http.NewRequestWithContext(ctx, e.method, e.url, nil)
	if err != nil {
		return err
	}
	conn, err := websocket.Negotiate(e.transport, e.upgrader, req, e.protocols...)
	if err != nil {
		return err
	}
	if conn == nil {
		panic(fmt.Errorf("websocket connection is nil"))
	}
	defer conn.Close()
	e.negotiated = conn.Subprotocol()
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Subprotocol negotiated", "protocol", e.negotiated)

	var streamer streamProtocolHandler
	switch e.negotiated {
	case remotecommand.StreamProtocolV5Name:
		streamer = newStreamProtocolV5(options)
	case remotecommand.StreamProtocolV4Name:
		streamer = newStreamProtocolV4(options)
	case remotecommand.StreamProtocolV3Name:
		streamer = newStreamProtocolV3(options)
	case remotecommand.StreamProtocolV2Name:
		streamer = newStreamProtocolV2(options)
	case "":
		logger.V(4).Info("The server did not negotiate a streaming protocol version, falling back", "protocol", remotecommand.StreamProtocolV1Name)
		fallthrough
	case remotecommand.StreamProtocolV1Name:
		streamer = newStreamProtocolV1(options)
	}

	panicChan := make(chan any, 1)
	errorChan := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()

		readyChan := make(chan struct{})
		creator := newWSStreamCreator(logger, conn)
		go func() {
			select {
			// 等到所有流都创建完毕后再启动 readDemuxLoop。
			// 这是为了避免 readDemuxLoop 收到指向尚未创建的流的消息的竞态。
			case <-readyChan:
			case <-ctx.Done():
				creator.closeAllStreamReaders(ctx.Err())
				return
			}

			creator.readDemuxLoop(
				e.upgrader.DataBufferSize(),
				e.heartbeatPeriod,
				e.heartbeatDeadline,
			)
		}()
		errorChan <- streamer.stream(logger, creator, readyChan)
	}()

	select {
	case p := <-panicChan:
		panic(p)
	case err := <-errorChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// wsStreamCreator 管理一条 WebSocket 连接上的所有逻辑流。
//
// 读是单协程解复用、写由各流共享一把锁；关闭时统一以错误终止所有流的
// 管道，使阻塞中的 Read 立即返回。
type wsStreamCreator struct {
	logger klog.Logger
	conn   *gwebsocket.Conn
	// 保护对 WebSocket 连接的写入；读取无锁
	connWriteLock sync.Mutex
	// 流 ID 到流的映射；多条流共同读写这一条连接
	streams   map[byte]*stream
	streamsMu sync.Mutex
	// setStreamErr 保存要返回给 setStreams 调用方的错误。
	// 它在 closeAllStreamReaders 中被填充
	setStreamErr error
}

// newWSStreamCreator 创建流管理器；streams 在首次使用前为空映射。
func newWSStreamCreator(logger klog.Logger, conn *gwebsocket.Conn) *wsStreamCreator {
	return &wsStreamCreator{
		logger:  logger,
		conn:    conn,
		streams: map[byte]*stream{},
	}
}

// getStream 按流 ID 查找流；不存在时返回 nil。
func (c *wsStreamCreator) getStream(id byte) *stream {
	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()
	return c.streams[id]
}

// setStream 注册流；若连接已关闭则返回关闭原因，阻止新建流。
func (c *wsStreamCreator) setStream(id byte, s *stream) error {
	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()
	if c.setStreamErr != nil {
		return c.setStreamErr
	}
	c.streams[id] = s
	return nil
}

// CreateStream 使用请求头中的 id 在 "c.conn" 连接上创建一条流。
// 返回 Stream 结构（或 nil）以及可能发生的错误。
//
// 每个流用一对 io.Pipe 连接“解复用读”和调用方的 Read；写则直接写入共享
// WebSocket 连接。同一类型只允许一条流，避免协议歧义。
func (c *wsStreamCreator) CreateStream(headers http.Header) (httpstream.Stream, error) {
	streamType := headers.Get(v1.StreamType)
	id, ok := streamType2streamID[streamType]
	if !ok {
		return nil, fmt.Errorf("unknown stream type: %s", streamType)
	}
	if s := c.getStream(id); s != nil {
		return nil, fmt.Errorf("duplicate stream for type %s", streamType)
	}
	reader, writer := io.Pipe()
	s := &stream{
		logger:        klog.LoggerWithValues(c.logger, "id", id),
		headers:       headers,
		readPipe:      reader,
		writePipe:     writer,
		conn:          c.conn,
		connWriteLock: &c.connWriteLock,
		id:            id,
	}
	if err := c.setStream(id, s); err != nil {
		_ = s.writePipe.Close()
		_ = s.readPipe.Close()
		return nil, err
	}
	return s, nil
}

// readDemuxLoop 是本端对 WebSocket 连接的无锁读取处理器。
// 该循环读取连接数据，并根据流 ID 将数据解复用到各条流的管道中。
// 该循环*不能*并发运行，因为同一时刻只能有一个 WebSocket 连接读取者
// （加读锁也没有好处）。
//
// 每帧格式为 [1 字节流 ID][负载]；未知流 ID 的帧被丢弃而不是报错，以便
// 客户端与未来版本的服务端保持兼容。
func (c *wsStreamCreator) readDemuxLoop(bufferSize int, period time.Duration, deadline time.Duration) {
	// 初始化并启动 ping/pong 心跳。
	h := newHeartbeat(c.logger, c.conn, period, deadline)
	// 设置 WebSocket 读取的初始超时。
	c.logger.V(5).Info("Websocket read starts", "deadline", deadline)
	if err := c.conn.SetReadDeadline(time.Now().Add(deadline)); err != nil {
		c.logger.Error(err, "Websocket initial setting read deadline failed")
		return
	}
	go h.start()
	// 缓冲区大小必须与创建 WebSocket 客户端时读取缓冲区的分配大小一致。
	// 不一致会导致连接读取不完整。
	readBuffer := make([]byte, bufferSize)
	for {
		// NextReader() 只返回数据消息（BinaryMessage 或 Text Message）。
		// 虽然这个调用永远不会返回 ping、pong、close 等控制帧，但必须调用它
		// 才能处理这些消息类型。同一时刻只能有一个读取者，所以这个读取循环
		// *不能*并发运行；读取不需要加锁。在当前读取者未被处理完之前调用
		// "NextReader()" 会关闭当前读取者。若心跳读取截止时间超时，这个
		// "NextReader()" 会返回 i/o 错误，由错误处理逻辑完成清理。
		messageType, r, err := c.conn.NextReader()
		if err != nil {
			websocketErr, ok := err.(*gwebsocket.CloseError)
			if ok && websocketErr.Code == gwebsocket.CloseNormalClosure {
				err = nil // 正常关闭，读取方会收到 io.EOF
			} else {
				err = fmt.Errorf("next reader: %w", err)
			}
			c.closeAllStreamReaders(err)
			return
		}
		// 所有远程命令协议只收发二进制数据消息。
		if messageType != gwebsocket.BinaryMessage {
			c.closeAllStreamReaders(fmt.Errorf("unexpected message type: %d", messageType))
			return
		}
		// 只读一个字节也没关系，因为底层库本来就把实际连接包装成了带缓冲的读取器。
		_, err = io.ReadFull(r, readBuffer[:1])
		if err != nil {
			c.closeAllStreamReaders(fmt.Errorf("read stream id: %w", err))
			return
		}
		streamID := readBuffer[0]
		s := c.getStream(streamID)
		if s == nil {
			c.logger.Error(nil, "Unknown stream, discarding message", "id", streamID)
			continue
		}
		for {
			nr, errRead := r.Read(readBuffer)
			if nr > 0 {
				// 将数据写入流的管道。这里可能阻塞。
				_, errWrite := s.writePipe.Write(readBuffer[:nr])
				if errWrite != nil {
					// 管道必定已被流的使用者关闭。
					// 无需处理，丢弃该消息。
					break
				}
			}
			if errRead != nil {
				if errRead == io.EOF {
					break
				}
				c.closeAllStreamReaders(fmt.Errorf("read message: %w", errRead))
				return
			}
		}
	}
}

// closeAllStreamReaders 关闭所有流中的读取端。
// 这会让所有 stream.Read() 调用解除阻塞，并阻止将来再创建新的流。
func (c *wsStreamCreator) closeAllStreamReaders(err error) {
	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()
	for _, s := range c.streams {
		// 关闭 writePipe 会让所有 readPipe.Read() 调用方解除阻塞，并阻止后续写入。
		_ = s.writePipe.CloseWithError(err)
	}
	// 确保此后 setStreams 的调用方都能收到错误
	if err != nil {
		c.setStreamErr = err
	} else {
		c.setStreamErr = fmt.Errorf("closed all streams")
	}
}

// stream 是 WebSocket 协议上的一条逻辑流。
//
// 读来自 io.Pipe（由解复用循环写入），写直接落到共享连接；因此并发写必须
// 经过 connWriteLock。
type stream struct {
	logger    klog.Logger
	headers   http.Header
	readPipe  *io.PipeReader
	writePipe *io.PipeWriter
	// conn 用于直接向连接写入。
	// Close() / Reset() 之后置为 nil，以防止后续写入。
	conn *gwebsocket.Conn
	// connWriteLock 保护 conn 免受并发写入。同一时刻必须只有一个写者、一个读者。
	// 由于底层连接是共享的，这把锁在所有流之间共享。
	connWriteLock *sync.Mutex
	id            byte
}

// Read 从解复用管道读取数据；连接关闭时返回带有原因的错误或 io.EOF。
func (s *stream) Read(p []byte) (n int, err error) {
	return s.readPipe.Read(p)
}

// Write 直接向底层 WebSocket 连接写入。
func (s *stream) Write(p []byte) (n int, err error) {
	s.logger.V(8).Info("Write() on stream")
	defer s.logger.V(8).Info("Write() done on stream")
	s.connWriteLock.Lock()
	defer s.connWriteLock.Unlock()
	if s.conn == nil {
		return 0, fmt.Errorf("write on closed stream %d", s.id)
	}
	err = s.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
	if err != nil {
		s.logger.V(4).Info("Websocket setting write deadline failed", "err", err)
		return 0, err
	}
	// 消息写入器本身会缓冲消息数据，无需我们自己再做缓冲。
	// 只需把 id 和数据分两次写入，避免分配中间缓冲区。
	w, err := s.conn.NextWriter(gwebsocket.BinaryMessage)
	if err != nil {
		return 0, err
	}
	defer func() {
		if w != nil {
			w.Close()
		}
	}()
	_, err = w.Write([]byte{s.id})
	if err != nil {
		return 0, err
	}
	n, err = w.Write(p)
	if err != nil {
		return n, err
	}
	err = w.Close()
	w = nil
	return n, err
}

// Close 半关闭该流，表示本端已完成对该流的使用。
func (s *stream) Close() error {
	s.logger.V(6).Info("Close() on stream")
	defer s.logger.V(6).Info("Close() done on stream")
	s.connWriteLock.Lock()
	defer s.connWriteLock.Unlock()
	if s.conn == nil {
		return fmt.Errorf("Close() on already closed stream %d", s.id)
	}
	// 向 WebSocket 对端传达 CLOSE 流信号。
	err := s.conn.WriteMessage(gwebsocket.BinaryMessage, []byte{remotecommand.StreamClose, s.id})
	s.conn = nil
	return err
}

// Reset 半关闭对端并关闭本端写管道，用于在错误路径上尽快释放资源。
func (s *stream) Reset() error {
	s.logger.V(4).Info("Reset() on stream")
	defer s.logger.V(4).Info("Reset() done on stream")
	s.Close()
	return s.writePipe.Close()
}

// Headers 返回创建流时的请求头，包含流类型与端口等信息。
func (s *stream) Headers() http.Header {
	return s.headers
}

// Identifier 返回协议中的流 ID。
func (s *stream) Identifier() uint32 {
	return uint32(s.id)
}

// heartbeat 封装 WebSocket ping/pong 心跳所需的数据。
// 心跳的工作方式是：先在 WebSocket 连接上设置读取截止时间，之后每次心跳
// 成功就把这个截止时间向未来推。若心跳 "pong" 在截止时间前没有响应，
// "readDemuxLoop" 中的 "NextReader()" 调用会返回 i/o 错误，从而触发
// 连接关闭与清理。
type heartbeat struct {
	logger klog.Logger
	conn   *gwebsocket.Conn
	// period 定义向对端发送 "ping" 心跳消息的频率
	period time.Duration
	// 关闭 "closer" 通道会清理心跳定时器
	closer chan struct{}
	// 可选的、随 "ping" 消息一起发送的数据
	message []byte
	// 可选收到的随 "pong" 消息返回的数据，与 ping 发出的一致
	pongMessage []byte
}

// newHeartbeat 创建封装了运行 WebSocket 连接 ping/pong 机制所需字段的
// heartbeat 结构，并在 WebSocket 连接上设置好处理函数。
//
// CloseHandler 被包装而不是替换，保留底层库默认的关闭帧处理。
func newHeartbeat(logger klog.Logger, conn *gwebsocket.Conn, period time.Duration, deadline time.Duration) *heartbeat {
	h := &heartbeat{
		logger: logger,
		conn:   conn,
		period: period,
		closer: make(chan struct{}),
	}
	// 设置处理函数：收到对端返回的 "pong" 消息时，把读取截止时间向未来推。
	// 收到的 "msg" 可能为空。
	h.conn.SetPongHandler(func(msg string) error {
		// 把读取截止时间向未来推。
		logger.V(6).Info("Pong message received -- resetting read deadline", "message", msg)
		err := h.conn.SetReadDeadline(time.Now().Add(deadline))
		if err != nil {
			logger.Error(err, "Websocket setting read deadline failed")
			return err
		}
		if len(msg) > 0 {
			h.pongMessage = []byte(msg)
		}
		return nil
	})
	// 设置处理函数：本端收到 "Close" 消息时清理定时器。
	closeHandler := h.conn.CloseHandler()
	h.conn.SetCloseHandler(func(code int, text string) error {
		close(h.closer)
		return closeHandler(code, text)
	})
	return h
}

// setMessage 是随 "ping" 心跳发送的可选数据。按照 WebSocket RFC，
// 随 "ping" 消息发送的数据应当在 "pong" 消息中原样返回。
//
// setMessage 必须在 start 之前调用；heartbeat 启动后不再加锁，事后写入会有
// 数据竞争。
func (h *heartbeat) setMessage(msg string) {
	h.message = []byte(msg)
}

// start 启动心跳：设置所需的处理函数，并循环每隔 "period" 发送一次 "ping"
// 消息，直到 "closer" 通道被关闭。
func (h *heartbeat) start() {
	// 循环：每隔 "period" 持续通过 WebSocket 连接发送 "ping" 消息。
	// 只有明确的致命错误才退出循环；超时与 ErrCloseSent 可能是暂时性的，
	// 连接是否关闭由读取循环判定。
	t := time.NewTicker(h.period)
	defer t.Stop()
	for {
		select {
		case <-h.closer:
			h.logger.V(5).Info("Closed channel -- returning")
			return
		case <-t.C:
			// "WriteControl" 不需要用互斥锁保护。根据 gorilla/websockets
			// 库的文档："The Close and WriteControl methods can
			// be called concurrently with all other methods."
			if err := h.conn.WriteControl(gwebsocket.PingMessage, h.message, time.Now().Add(pingReadDeadline)); err == nil {
				h.logger.V(6).Info("Websocket Ping succeeeded")
			} else {
				h.logger.Error(err, "Websocket Ping failed")
				if errors.Is(err, gwebsocket.ErrCloseSent) {
					// 继续循环，因为 c.conn.CloseChan 已经在管理连接的关闭
					continue
				} else if e, ok := err.(net.Error); ok && e.Timeout() {
					// 继续，因为这可能只是暂时性失败。
					// 上面的 c.conn.CloseChan 会告诉我们连接是否真正关闭。
					// 若 Temporary 函数没有被弃用，我们本会使用它。
					// 不过大多数暂时性错误本来就是超时错误。
					continue
				}
				return
			}
		}
	}
}
