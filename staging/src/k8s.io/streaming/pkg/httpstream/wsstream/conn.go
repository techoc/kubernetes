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
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/websocket"

	"k8s.io/klog/v2"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/streaming/pkg/runtime"
)

// 客户端把这个头里的子协议列表交给服务端挑选；大小写不敏感由 net/http 保证，
// 但头的值（协议名）本身是大小写敏感的，协议判断不能图省事做大小写折叠。
const WebSocketProtocolHeader = "Sec-Websocket-Protocol"

// The Websocket subprotocol "channel.k8s.io" prepends each binary message with a byte indicating
// the channel number (zero indexed) the message was sent on. Messages in both directions should
// prefix their messages with this channel byte. When used for remote execution, the channel numbers
// are by convention defined to match the POSIX file-descriptors assigned to STDIN, STDOUT, and STDERR
// (0, 1, and 2). No other conversion is performed on the raw subprotocol - writes are sent as they
// are received by the server.
//
// Example client session:
//
//	CONNECT http://server.com with subprotocol "channel.k8s.io"
//	WRITE []byte{0, 102, 111, 111, 10} # send "foo\n" on channel 0 (STDIN)
//	READ  []byte{1, 10}                # receive "\n" on channel 1 (STDOUT)
//	CLOSE
//
// 用固定长度的前导字节而不是分隔符来标记通道，是因为载荷是任意二进制数据，
// 任何分隔符都可能出现在内容里；代价是每帧多出一个字节。
const ChannelWebSocketProtocol = "channel.k8s.io"

// The Websocket subprotocol "base64.channel.k8s.io" base64 encodes each message with a character
// indicating the channel number (zero indexed) the message was sent on. Messages in both directions
// should prefix their messages with this channel char. When used for remote execution, the channel
// numbers are by convention defined to match the POSIX file-descriptors assigned to STDIN, STDOUT,
// and STDERR ('0', '1', and '2'). The data received on the server is base64 decoded (and must be
// be valid) and data written by the server to the client is base64 encoded.
//
// Example client session:
//
//	CONNECT http://server.com with subprotocol "base64.channel.k8s.io"
//	WRITE []byte{48, 90, 109, 57, 118, 67, 103, 111, 61} # send "foo\n" (base64: "Zm9vCgo=") on channel '0' (STDIN)
//	READ  []byte{49, 67, 103, 61, 61} # receive "\n" (base64: "Cg==") on channel '1' (STDOUT)
//	CLOSE
//
// 这个变体存在的理由是某些中间代理不支持二进制 WebSocket 帧；
// 通道号也改用字符表示，以便整帧都是可打印文本。
const Base64ChannelWebSocketProtocol = "base64.channel.k8s.io"

// 255 不能作为通道号使用，所以拿它当半关闭信号：这样单字节就能区分
// “数据帧”和“关闭某个通道”，无需新的帧类型或带外控制通道。
const streamCloseSignal = 255

// 区分原始帧和 base64 帧；解码开销只在协商到文本子协议时才付出。
type codecType int

const (
	rawCodec codecType = iota
	base64Codec
)

// 描述单个通道在本端的方向。用枚举而不是两个 bool，是为了让“完全忽略”
// 这个第四种状态有明确名字，调用方不必记住 read=false && write=false 的含义。
type ChannelType int

const (
	// 两端都不转发，占住通道号只为和协议版本里的编号保持一致。
	IgnoreChannel ChannelType = iota
	// 只接收对端发来的数据；本端写入直接丢弃，避免误把输入当输出。
	ReadChannel
	// 只向对端发送数据；本端读取立即得到 EOF。
	WriteChannel
	// 双向转发，用于 stdin/stdout 这类需要回显的通道。
	ReadWriteChannel
)

// IsWebSocketRequest returns true if the incoming request contains connection upgrade headers
// for WebSockets.
//
// Upgrade 头单独判定，是因为通用升级请求也可能是 SPDY；只有两者同时满足才算 WebSocket。
// 头名由 net/http 规范化，值用 EqualFold 比较以容忍客户端的大小写写法。
func IsWebSocketRequest(req *http.Request) bool {
	if !strings.EqualFold(req.Header.Get("Upgrade"), "websocket") {
		return false
	}
	return httpstream.IsUpgradeRequest(req)
}

// IsWebSocketRequestWithStreamCloseProtocol returns true if the request contains headers
// identifying that it is requesting a websocket upgrade with a remotecommand protocol
// version that supports the "CLOSE" signal; false otherwise.
//
// 客户端可以一次列出多个候选子协议，只要其中有支持半关闭的就算命中；
// 最终用哪个协议仍由握手阶段决定，这里只用于提前分流处理逻辑。
func IsWebSocketRequestWithStreamCloseProtocol(req *http.Request) bool {
	if !IsWebSocketRequest(req) {
		return false
	}
	requestedProtocols := strings.TrimSpace(req.Header.Get(WebSocketProtocolHeader))
	for _, requestedProtocol := range strings.Split(requestedProtocols, ",") {
		if protocolSupportsStreamClose(strings.TrimSpace(requestedProtocol)) {
			return true
		}
	}

	return false
}

// IsWebSocketRequestWithTunnelingProtocol returns true if the request contains headers
// identifying that it is requesting a websocket upgrade with a tunneling protocol;
// false otherwise.
//
// 同半关闭判断一样，只探测“列表里是否存在隧道协议”；具体的 SPDY 版本协商
// 留给隧道层完成，避免在这里固守某个版本号而随协议演进而失效。
func IsWebSocketRequestWithTunnelingProtocol(req *http.Request) bool {
	if !IsWebSocketRequest(req) {
		return false
	}
	requestedProtocols := strings.TrimSpace(req.Header.Get(WebSocketProtocolHeader))
	for _, requestedProtocol := range strings.Split(requestedProtocols, ",") {
		if protocolSupportsWebsocketTunneling(strings.TrimSpace(requestedProtocol)) {
			return true
		}
	}

	return false
}

// IgnoreReceives reads from a WebSocket until it is closed, then returns. If timeout is set, the
// read and write deadlines are pushed every time a new message is received.
//
// Contextual logging: IgnoreReceivesWithLogger should be used instead of IgnoreReceives in code which uses contextual logging.
//
// 保留旧签名是为了兼容尚无上下文日志的调用方；它固定用 Background logger，
// 因此日志里不会带上请求信息，新代码应直接传 logger。
func IgnoreReceives(ws *websocket.Conn, timeout time.Duration) {
	IgnoreReceivesWithLogger(klog.Background(), ws, timeout)
}

// IgnoreReceivesWithLogger reads from a WebSocket until it is closed, then returns. If timeout is set, the
// read and write deadlines are pushed every time a new message is received.
//
// 只读不解析：这个 goroutine 的目的是让对端发来的数据有地方可去，否则
// 底层连接会因为无人读取而卡住。收到任何消息就刷新超时，空闲连接最终自然过期。
func IgnoreReceivesWithLogger(logger klog.Logger, ws *websocket.Conn, timeout time.Duration) {
	// 作为常驻 goroutine，panic 不能越过这里，否则会直接带崩进程。
	defer runtime.HandleCrashWithLogger(logger)
	var data []byte
	for {
		resetTimeout(ws, timeout)
		if err := websocket.Message.Receive(ws, &data); err != nil {
			return
		}
	}
}

// handshake ensures the provided user protocol matches one of the allowed protocols. It returns
// no error if no protocol is specified.
//
// 未指定子协议时补上空字符串参与匹配，这样默认协议也能出现在允许列表里；
// 命中后把列表收敛为单项，避免底层库挑中客户端实际不认识的协议。
func handshake(config *websocket.Config, req *http.Request, allowed []string) error {
	protocols := config.Protocol
	if len(protocols) == 0 {
		protocols = []string{""}
	}

	for _, protocol := range protocols {
		for _, allow := range allowed {
			if allow == protocol {
				config.Protocol = []string{protocol}
				return nil
			}
		}
	}

	return fmt.Errorf("requested protocol(s) are not supported: %v; supports %v", config.Protocol, allowed)
}

// ChannelProtocolConfig describes a websocket subprotocol with channels.
//
// Binary 决定收发用原始字节还是 base64；Channels 按序号给出每个通道的方向，
// 它同时定义了帧首字节的合法范围——越界的通道号一律当作协议错误处理。
// 只有文本化的子协议才置 false，此时每帧承载的是 base64 文本而非原始字节。
// 切片下标即通道号，元素描述该通道在本端是否可读、可写。
type ChannelProtocolConfig struct {
	Binary   bool
	Channels []ChannelType
}

// NewDefaultChannelProtocols returns a channel protocol map with the
// subprotocols "", "channel.k8s.io", "base64.channel.k8s.io" and the given
// channels.
//
// 默认让空协议名和 "channel.k8s.io" 走二进制帧，只有 base64 变体走文本帧；
// 这样客户端完全不提子协议时也能按裸通道协议工作，无需额外协商步骤。
func NewDefaultChannelProtocols(channels []ChannelType) map[string]ChannelProtocolConfig {
	return map[string]ChannelProtocolConfig{
		"":                             {Binary: true, Channels: channels},
		ChannelWebSocketProtocol:       {Binary: true, Channels: channels},
		Base64ChannelWebSocketProtocol: {Binary: false, Channels: channels},
	}
}

// Conn supports sending multiple binary channels over a websocket connection.
//
// 服务端的 websocket.Server 在独立 goroutine 里跑握手和处理循环，
// ready 用来把“握手已完成、通道已建好”这一事实同步给调用 Open 的 goroutine；
// Close 必须等这个信号，否则可能在 ws 仍为 nil 时就去关连接。
// 提供给握手的候选协议表，键是子协议名；selectedProtocol 是握手后选定的协议。
// channels 按通道号索引，codec 由选定协议推导，ready 同步握手完成，ws 仅在 initialize 后可用。
// timeout 是每次收发成功后顺延的空闲超时。
type Conn struct {
	protocols        map[string]ChannelProtocolConfig
	selectedProtocol string
	channels         []*websocketChannel
	codec            codecType
	ready            chan struct{}
	ws               *websocket.Conn
	timeout          time.Duration
}

// NewConn creates a WebSocket connection that supports a set of channels. Channels begin each
// web socket message with a single byte indicating the channel number (0-N). 255 is reserved for
// future use. The channel types for each channel are passed as an array, supporting the different
// duplex modes. Read and Write refer to whether the channel can be used as a Reader or Writer.
//
// The protocols parameter maps subprotocol names to ChannelProtocols. The empty string subprotocol
// name is used if websocket.Config.Protocol is empty.
//
// 构造阶段只初始化同步原语和协议表：真正的连接、通道和编码器都要等握手
// 成功后才能确定，提前创建会让失败路径留下需要回收的资源。
func NewConn(protocols map[string]ChannelProtocolConfig) *Conn {
	return &Conn{
		ready:     make(chan struct{}),
		protocols: protocols,
	}
}

// SetIdleTimeout sets the interval for both reads and writes before timeout. If not specified,
// there is no timeout on the connection.
//
// 必须在 Open 之前设置：超时值会被接收循环和每次发送读取，运行中途改动
// 没有同步保护，也不保证对已经阻塞的读写生效。
func (conn *Conn) SetIdleTimeout(duration time.Duration) {
	conn.timeout = duration
}

// SetWriteDeadline sets a timeout on writing to the websocket connection. The
// passed "duration" identifies how far into the future the write must complete
// by before the timeout fires.
//
// 这是单次写入的硬截止时间，和 SetIdleTimeout 的“每次活动后顺延”语义不同；
// 用于大帧发送时限制最长等待时间，而不是控制整体连接寿命。
func (conn *Conn) SetWriteDeadline(duration time.Duration) {
	conn.ws.SetWriteDeadline(time.Now().Add(duration)) //nolint:errcheck
}

// Open the connection and create channels for reading and writing. It returns
// the selected subprotocol, a slice of channels and an error.
//
// websocket.Server.ServeHTTP 会一直阻塞到连接关闭，因此必须放到 goroutine 里跑，
// 同时监听 ready 和“服务提前结束”两个信号：后者说明握手失败，不能让调用方
// 永远等下去。panic 单独走一个通道传回，避免跨 goroutine 丢失调用栈。
func (conn *Conn) Open(w http.ResponseWriter, req *http.Request) (string, []io.ReadWriteCloser, error) {
	// serveHTTPComplete is channel that is closed/selected when "websocket#ServeHTTP" finishes.
	serveHTTPComplete := make(chan struct{})
	// Ensure panic in spawned goroutine is propagated into the parent goroutine.
	panicChan := make(chan any, 1)
	go func() {
		// If websocket server returns, propagate panic if necessary. Otherwise,
		// signal HTTPServe finished by closing "serveHTTPComplete".
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			} else {
				close(serveHTTPComplete)
			}
		}()
		websocket.Server{Handshake: conn.handshake, Handler: conn.handle}.ServeHTTP(w, req)
	}()

	// In normal circumstances, "websocket.Server#ServeHTTP" calls "initialize" which closes
	// "conn.ready" and then blocks until serving is complete.
	select {
	case <-conn.ready:
		klog.FromContext(req.Context()).V(8).Info("websocket server initialized--serving")
	case <-serveHTTPComplete:
		// websocket server returned before completing initialization; cleanup and return error.
		conn.closeNonThreadSafe() //nolint:errcheck
		return "", nil, fmt.Errorf("websocket server finished before becoming ready")
	case p := <-panicChan:
		panic(p)
	}

	rwc := make([]io.ReadWriteCloser, len(conn.channels))
	for i := range conn.channels {
		rwc[i] = conn.channels[i]
	}
	return conn.selectedProtocol, rwc, nil
}

// 只有握手成功才会走到这里：先根据协商出的子协议确定编解码方式和通道方向，
// 再建通道；最后关闭 ready，让等待中的 Open 拿到一致的 Conn 状态。
func (conn *Conn) initialize(ws *websocket.Conn) {
	// 握手已把 config.Protocol 收敛为单项，所以取第一个就是最终协议。
	negotiated := ws.Config().Protocol
	conn.selectedProtocol = negotiated[0]
	p := conn.protocols[conn.selectedProtocol]
	if p.Binary {
		conn.codec = rawCodec
	} else {
		conn.codec = base64Codec
	}
	conn.ws = ws
	conn.channels = make([]*websocketChannel, len(p.Channels))
	// IgnoreChannel 也建对象，目的是让通道号在切片里保持连续占位；
	// 这样收到的帧可以直接按序号索引，不必为“跳号”单独维护映射表。
	for i, t := range p.Channels {
		switch t {
		case ReadChannel:
			conn.channels[i] = newWebsocketChannel(conn, byte(i), true, false)
		case WriteChannel:
			conn.channels[i] = newWebsocketChannel(conn, byte(i), false, true)
		case ReadWriteChannel:
			conn.channels[i] = newWebsocketChannel(conn, byte(i), true, true)
		case IgnoreChannel:
			conn.channels[i] = newWebsocketChannel(conn, byte(i), false, false)
		}
	}

	close(conn.ready)
}

// 把 map 的键展开成列表交给通用握手函数：候选顺序无语义，
// 因为最终协议由客户端提议顺序和服务端允许集合共同决定，这里只负责校验。
func (conn *Conn) handshake(config *websocket.Config, req *http.Request) error {
	supportedProtocols := make([]string, 0, len(conn.protocols))
	for p := range conn.protocols {
		supportedProtocols = append(supportedProtocols, p)
	}
	return handshake(config, req, supportedProtocols)
}

// 每次收到或发送一帧都顺延截止时间，使超时表达的是“空闲多久”而不是
// “连接总寿命”；未配置超时时干脆不碰底层 deadline，免得引入零值语义。
func (conn *Conn) resetTimeout() {
	if conn.timeout > 0 {
		conn.ws.SetDeadline(time.Now().Add(conn.timeout))
	}
}

// closeNonThreadSafe cleans up by closing streams and the websocket
// connection *without* waiting for the "ready" channel.
//
// 专供握手失败的清理路径使用：那时 ready 永远不会被关闭，若走 Close 会死锁。
// 先关通道再关连接，让阻塞在 Pipe 读写上的调用方先拿到错误。
func (conn *Conn) closeNonThreadSafe() error {
	for _, s := range conn.channels {
		s.Close()
	}
	var err error
	if conn.ws != nil {
		err = conn.ws.Close()
	}
	return err
}

// Close is only valid after Open has been called
//
// 先等 ready 是必要的：Open 可能仍在另一个 goroutine 里做握手，
// 此时 conn.ws 和 channels 都还没填好，贸然清理会漏关资源。
func (conn *Conn) Close() error {
	<-conn.ready
	return conn.closeNonThreadSafe()
}

// protocolSupportsStreamClose returns true if the passed protocol
// supports the stream close signal (currently only V5 remotecommand);
// false otherwise.
//
// 用精确比较而不是前缀匹配：协议名大小写敏感，V4 之前的版本没有半关闭信号，
// 误判会让控制字节被当成通道数据投递给用户。
func protocolSupportsStreamClose(protocol string) bool {
	return protocol == "v5.channel.k8s.io"
}

// protocolSupportsWebsocketTunneling returns true if the passed protocol
// is a tunneled Kubernetes spdy protocol; false otherwise.
//
// 隧道子协议把自定义后缀挂在 SPDY/3.1+ 后面，所以只认固定前缀和 .k8s.io 结尾；
// 中间部分留给具体的隧道实现，避免这里跟着每个新版本改判断逻辑。
func protocolSupportsWebsocketTunneling(protocol string) bool {
	return strings.HasPrefix(protocol, "SPDY/3.1+") && strings.HasSuffix(protocol, ".k8s.io")
}

// handle implements a websocket handler.
//
// 接收循环运行在服务端 goroutine 里：每帧首字节是通道号（base64 协议下是字符），
// 其余载荷写进对应通道的 Pipe。帧格式不合法时只记日志并丢弃该帧，
// 不让单个坏包拖垮整条连接。
func (conn *Conn) handle(ws *websocket.Conn) {
	conn.initialize(ws)
	// 接收循环退出时一并关掉所有通道和连接，避免对端读端永远阻塞。
	defer conn.Close()
	supportsStreamClose := protocolSupportsStreamClose(conn.selectedProtocol)
	// conn.handle is typically used on the server-side and thus we have a request,
	// but don't assume that and use klog.Background as fallback.
	logger := klog.Background()
	if req := ws.Request(); req != nil {
		logger = klog.FromContext(req.Context())
	}

	for {
		conn.resetTimeout()
		var data []byte
		if err := websocket.Message.Receive(ws, &data); err != nil {
			// 正常关闭走 EOF；其他错误才需要记日志，免得关闭连接刷出噪音。
			if err != io.EOF {
				logger.Error(err, "Error on socket receive")
			}
			break
		}
		// 空帧没有通道字节，忽略即可，不能拿 data[0] 去索引通道。
		if len(data) == 0 {
			continue
		}
		// 半关闭信号必须严格是“信号字节 + 通道号”两字节；长度不对说明协议出错，
		// 直接退出循环，因为继续解析下去只会把控制字节当数据投递。
		if supportsStreamClose && data[0] == streamCloseSignal {
			if len(data) != 2 {
				logger.Error(nil, "Single channel byte should follow stream close signal", "receivedLength", len(data)-1)
				break
			} else {
				channel := data[1]
				if int(channel) >= len(conn.channels) {
					logger.Error(nil, "Close is targeted for a channel that is not valid, possible protocol error", "channel", channel)
					break
				}
				logger.V(4).Info("Received half-close signal from client, close stream", "channel", channel)
				// 对端声明不再往这个通道写数据；关闭 Pipe 写端让本端读侧收到 EOF，
				// 其他通道仍可继续传输，这也正是半关闭相对于直接断开连接的价值。
				conn.channels[channel].Close() // After first Close, other closes are noop.
			}
			continue
		}
		channel := data[0]
		if conn.codec == base64Codec {
			// 文本协议里通道号是 '0'-'9' 的字符，减去 '0' 还原成序号；
			// 畸形字符会算出越界值，被下面的检查挡掉。
			channel = channel - '0'
		}
		data = data[1:]
		// 只挡上界：通道号是无符号字节，不会出现负数索引。
		if int(channel) >= len(conn.channels) {
			logger.V(6).Info("Frame is targeted for a reader that is not valid, possible protocol error", "channel", channel)
			continue
		}
		if _, err := conn.channels[channel].DataFromSocket(data); err != nil {
			logger.Error(err, "Unable to write frame", "sendLength", len(data), "channel", channel, "err", err)
			continue
		}
	}
}

// write multiplexes the specified channel onto the websocket
//
// 发送路径按协商结果二选一：二进制帧直接前置通道字节，文本帧前置通道字符
// 并把载荷做 base64 编码。返回长度是原始载荷长度，符合 io.Writer 的语义，
// 调用方看到的字节数不受编码方式影响。
func (conn *Conn) write(num byte, data []byte) (int, error) {
	conn.resetTimeout()
	switch conn.codec {
	case rawCodec:
		// 每帧单独分配，避免在多通道并发写入时复用缓冲区造成串帧。
		frame := make([]byte, len(data)+1)
		frame[0] = num
		copy(frame[1:], data)
		if err := websocket.Message.Send(conn.ws, frame); err != nil {
			return 0, err
		}
	case base64Codec:
		frame := string('0'+num) + base64.StdEncoding.EncodeToString(data)
		if err := websocket.Message.Send(conn.ws, frame); err != nil {
			return 0, err
		}
	}
	return len(data), nil
}

// websocketChannel represents a channel in a connection
//
// 每个通道由一对 io.Pipe 构成：接收循环往写端塞数据，调用方从读端取；
// 发送方向则直接经 Conn.write 落到 websocket，不需要第二个管道。
// 每个通道由一对 io.Pipe 构成：接收循环往 w 写，调用方从 r 读；发送方向直接走 conn。
// num 是帧首部的通道号；read/write 为方向开关，误用时按“无害化”处理避免阻塞调用方。
type websocketChannel struct {
	conn *Conn
	num  byte
	r    io.Reader
	w    io.WriteCloser

	read, write bool
}

// newWebsocketChannel creates a pipe for writing to a websocket. Do not write to this pipe
// prior to the connection being opened. It may be no, half, or full duplex depending on
// read and write.
//
// 用同步的 io.Pipe 而非缓冲队列，是为了让慢读者自然地反向施压；
// 但这也意味着没有读者时接收循环会阻塞，所以每个通道即便不开放读权限
// 也要有一个不会被消费的管道端点，写操作由 DataFromSocket 提前短路掉。
func newWebsocketChannel(conn *Conn, num byte, read, write bool) *websocketChannel {
	r, w := io.Pipe()
	return &websocketChannel{conn, num, r, w, read, write}
}

// 假装成功写入只读通道，而不是报错：调用方常把同一个 writer 传给通用拷贝逻辑，
// 在这类场景下“没有输出目标”应当静默丢弃，不应升级成连接级错误。
func (p *websocketChannel) Write(data []byte) (int, error) {
	if !p.write {
		return len(data), nil
	}
	return p.conn.write(p.num, data)
}

// DataFromSocket is invoked by the connection receiver to move data from the connection
// into a specific channel.
//
// 和 Write 对称：本端不读的通道直接丢弃载荷并报告“已消费”，
// 否则接收循环会卡在这个无人读取的管道上，连带阻断其他通道。
func (p *websocketChannel) DataFromSocket(data []byte) (int, error) {
	if !p.read {
		return len(data), nil
	}

	// 二进制数据原样写进管道；base64 载荷先解码，解码失败时整帧丢弃，
	// 避免把损坏内容喂给上层读者。
	switch p.conn.codec {
	case rawCodec:
		return p.w.Write(data)
	case base64Codec:
		dst := make([]byte, len(data))
		n, err := base64.StdEncoding.Decode(dst, data)
		if err != nil {
			return 0, err
		}
		return p.w.Write(dst[:n])
	}
	return 0, nil
}

// 只写通道立即返回 EOF，让按 io.Reader 接口写出的上层代码能自然收尾，
// 而不会因为等待一个永远不会有数据的管道而挂住。
func (p *websocketChannel) Read(data []byte) (int, error) {
	if !p.read {
		return 0, io.EOF
	}
	return p.r.Read(data)
}

// 关闭的是管道写端：接收循环和对端半关闭信号都通过它向本端读者传递 EOF。
// 多次关闭是安全的，io.Pipe 的 Close 可重入。
func (p *websocketChannel) Close() error {
	return p.w.Close()
}
