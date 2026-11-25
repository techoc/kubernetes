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

// WebSocket 子协议 "channel.k8s.io" 在每个二进制消息前添加一个字节，
// 表示消息发送到的通道号（从 0 开始）。双向消息都必须带上该通道字节。
// 用于远程执行时，通道号按约定对应 STDIN、STDOUT 和 STDERR 的 POSIX 文件描述符
// （0、1、2）。原始子协议不进行其他转换，服务端按收到的内容原样写出。
//
// 客户端会话示例：
//
//	CONNECT http://server.com with subprotocol "channel.k8s.io"
//	WRITE []byte{0, 102, 111, 111, 10} # send "foo\n" on channel 0 (STDIN)
//	READ  []byte{1, 10}                # receive "\n" on channel 1 (STDOUT)
//	CLOSE
//
// 用固定长度的前导字节而不是分隔符来标记通道，是因为载荷是任意二进制数据，
// 任何分隔符都可能出现在内容里；代价是每帧多出一个字节。
const ChannelWebSocketProtocol = "channel.k8s.io"

// WebSocket 子协议 "base64.channel.k8s.io" 用 base64 编码每条消息，并以一个字符
// 表示消息发送到的通道号（从 0 开始）。双向消息都必须带上该通道字符。
// 用于远程执行时，通道号按约定对应 STDIN、STDOUT 和 STDERR 的 POSIX 文件描述符
// （'0'、'1'、'2'）。服务端收到的数据必须是合法的 base64 并会被解码；
// 服务端写给客户端的数据会先进行 base64 编码。
//
// 客户端会话示例：
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

// IsWebSocketRequest 判断传入请求是否包含升级为 WebSocket 所需的连接升级头。
//
// Upgrade 头单独判定，是因为通用升级请求也可能是 SPDY；只有两者同时满足才算 WebSocket。
// 头名由 net/http 规范化，值用 EqualFold 比较以容忍客户端的大小写写法。
func IsWebSocketRequest(req *http.Request) bool {
	if !strings.EqualFold(req.Header.Get("Upgrade"), "websocket") {
		return false
	}
	return httpstream.IsUpgradeRequest(req)
}

// IsWebSocketRequestWithStreamCloseProtocol 判断请求是否要求升级到支持 "CLOSE"
// 信号的 WebSocket RemoteCommand 协议；不满足时返回 false。
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

// IsWebSocketRequestWithTunnelingProtocol 判断请求是否要求升级到支持隧道协议的
// WebSocket；不满足时返回 false。
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

// IgnoreReceives 持续读取 WebSocket 消息直到连接关闭。
// 设置 timeout 后，每收到一条消息都会顺延读写截止时间。
//
// 保留旧签名是为了兼容尚无上下文日志的调用方；它固定使用 klog.Background()，
// 日志不含请求信息。使用上下文日志的代码应改用 IgnoreReceivesWithLogger。
func IgnoreReceives(ws *websocket.Conn, timeout time.Duration) {
	IgnoreReceivesWithLogger(klog.Background(), ws, timeout)
}

// IgnoreReceivesWithLogger 使用指定日志记录器持续读取 WebSocket 消息，直到连接关闭。
// 设置 timeout 后，每收到一条消息都会顺延读写截止时间。
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

// handshake 确保客户端提议的子协议至少有一个出现在服务端允许列表中。
// 未指定子协议时补上空字符串参与匹配，使默认协议也能出现在允许列表里；
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

// ChannelProtocolConfig 描述一个带通道的 WebSocket 子协议。
//
// Binary 决定收发使用原始字节还是 base64 文本；Channels 按序号给出每个通道的方向，
// 切片下标即通道号，同时界定帧首通道字节的合法范围。
// 只有文本化的子协议才将 Binary 置为 false。
type ChannelProtocolConfig struct {
	// Binary 为 true 时使用二进制帧，为 false 时使用 base64 文本帧。
	Binary bool
	// Channels 按通道号顺序描述各通道在本端的读写方向。
	Channels []ChannelType
}

// NewDefaultChannelProtocols 为给定通道返回默认协议表，包含空协议名、
// "channel.k8s.io" 和 "base64.channel.k8s.io"。
//
// 空协议名和 "channel.k8s.io" 使用二进制帧，base64 变体使用文本帧；
// 客户端不指定子协议时也能按裸通道协议工作。
func NewDefaultChannelProtocols(channels []ChannelType) map[string]ChannelProtocolConfig {
	return map[string]ChannelProtocolConfig{
		"":                             {Binary: true, Channels: channels},
		ChannelWebSocketProtocol:       {Binary: true, Channels: channels},
		Base64ChannelWebSocketProtocol: {Binary: false, Channels: channels},
	}
}

// Conn 在单个 WebSocket 连接上复用多个二进制或 base64 文本通道。
//
// Open 在独立 goroutine 中运行握手和处理循环，并通过 ready 通知调用方握手已完成、
// 通道已建好；Close 必须等待该信号，避免在连接尚未初始化时访问字段。
// protocols 是可供握手的子协议配置表，selectedProtocol 记录最终选定的协议；
// channels 按通道号索引，codec 由选定协议推导，ws 仅在 initialize 后可用。
// timeout 是每次收发活动后顺延的空闲超时。
type Conn struct {
	protocols        map[string]ChannelProtocolConfig
	selectedProtocol string
	channels         []*websocketChannel
	codec            codecType
	ready            chan struct{}
	ws               *websocket.Conn
	timeout          time.Duration
}

// NewConn 创建支持一组通道的 WebSocket 连接。
// 每条消息以单个通道字节或字符开头，通道号从 0 开始；255 保留给半关闭信号。
// 各通道的方向支持忽略、只读、只写和双向；读写权限决定返回的对象能否作为
// io.Reader 或 io.Writer 使用，禁用方向会被静默丢弃或返回 EOF。
//
// protocols 将子协议名映射到 ChannelProtocolConfig；客户端未指定子协议时使用空字符串。
// 构造阶段只初始化同步原语和协议表，连接、通道和编码器须等握手成功后确定。
func NewConn(protocols map[string]ChannelProtocolConfig) *Conn {
	return &Conn{
		ready:     make(chan struct{}),
		protocols: protocols,
	}
}

// SetIdleTimeout 设置连接在读写空闲多久后超时；未设置时不启用空闲超时。
//
// 必须在 Open 之前设置：超时值会被接收循环和每次发送读取，运行中途改动
// 没有同步保护，也不保证对已经阻塞的读写生效。
func (conn *Conn) SetIdleTimeout(duration time.Duration) {
	conn.timeout = duration
}

// SetWriteDeadline 设置 WebSocket 写入必须在 duration 时间内完成的截止时间。
//
// 必须在 Open 成功后调用。这是单次写入的硬截止时间，与 SetIdleTimeout 的
// “每次活动后顺延”语义不同，用于限制大帧发送的最长等待时间。
func (conn *Conn) SetWriteDeadline(duration time.Duration) {
	conn.ws.SetWriteDeadline(time.Now().Add(duration)) //nolint:errcheck
}

// Open 建立连接并创建读写通道，返回选定的子协议、通道列表和错误。
//
// websocket.Server.ServeHTTP 会一直阻塞到连接关闭，因此放到 goroutine 中运行；
// Open 同时等待握手完成和服务提前结束信号，后者用于避免握手失败时永久阻塞。
// panic 通过单独通道传回调用方 goroutine，以便保留原始调用栈。
func (conn *Conn) Open(w http.ResponseWriter, req *http.Request) (string, []io.ReadWriteCloser, error) {
	// websocket.Server.ServeHTTP 结束时关闭该通道，使 Open 不必继续等待握手。
	serveHTTPComplete := make(chan struct{})
	// 将子 goroutine 中的 panic 传回调用方，避免异常在 goroutine 内丢失。
	panicChan := make(chan any, 1)
	go func() {
		// 优先传回 panic；正常退出则关闭 serveHTTPComplete。
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			} else {
				close(serveHTTPComplete)
			}
		}()
		websocket.Server{Handshake: conn.handshake, Handler: conn.handle}.ServeHTTP(w, req)
	}()

	// 正常情况下，ServeHTTP 先调用 initialize 关闭 conn.ready，再阻塞到服务结束。
	select {
	case <-conn.ready:
		klog.FromContext(req.Context()).V(8).Info("websocket server initialized--serving")
	case <-serveHTTPComplete:
		// 服务在初始化完成前退出，清理尚未建立完整的资源并返回错误。
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

// closeNonThreadSafe 关闭所有流和 WebSocket 连接，不等待 ready 通道。
//
// 专供握手失败的清理路径使用：那时 ready 永远不会被关闭，若走 Close 会死锁。
// 此方法不做并发同步，调用方必须确保没有其他 goroutine 正在访问连接。
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

// Close 关闭所有通道和底层连接，仅在 Open 成功返回后调用。
//
// 先等待 ready 是必要的：Open 可能仍在另一个 goroutine 中握手，
// 此时 conn.ws 和 channels 尚未填好。握手失败后的清理必须使用 closeNonThreadSafe。
func (conn *Conn) Close() error {
	<-conn.ready
	return conn.closeNonThreadSafe()
}

// protocolSupportsStreamClose 判断协议是否支持流半关闭信号；目前仅 V5 RemoteCommand 支持。
//
// 用精确比较而不是前缀匹配：协议名大小写敏感，V4 之前的版本没有半关闭信号，
// 误判会让控制字节被当成通道数据投递给用户。
func protocolSupportsStreamClose(protocol string) bool {
	return protocol == "v5.channel.k8s.io"
}

// protocolSupportsWebsocketTunneling 判断协议是否为承载 Kubernetes SPDY 隧道的
// WebSocket 子协议。
//
// 隧道子协议把自定义后缀挂在 SPDY/3.1+ 后面，所以只认固定前缀和 .k8s.io 结尾；
// 中间部分留给具体的隧道实现，避免这里跟着每个新版本改判断逻辑。
func protocolSupportsWebsocketTunneling(protocol string) bool {
	return strings.HasPrefix(protocol, "SPDY/3.1+") && strings.HasSuffix(protocol, ".k8s.io")
}

// handle 实现 WebSocket 服务端处理器。
//
// 接收循环按帧首字节或字符中的通道号，将剩余载荷写入对应通道的 Pipe。
// 无效通道的数据帧会被丢弃；半关闭信号格式错误或目标通道无效时则终止连接，
// 避免继续按错误协议状态投递数据。
func (conn *Conn) handle(ws *websocket.Conn) {
	conn.initialize(ws)
	// 接收循环退出时一并关掉所有通道和连接，避免对端读端永远阻塞。
	defer conn.Close()
	supportsStreamClose := protocolSupportsStreamClose(conn.selectedProtocol)
	// handle 通常在服务端运行，可优先使用请求上下文中的日志记录器；
	// 但调用方不一定提供请求，因此以 klog.Background() 兜底。
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
				conn.channels[channel].Close() // 首次关闭后，重复调用为无操作。
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

// write 将指定通道的数据复用发送到 WebSocket。
//
// 发送路径按协商结果二选一：二进制帧直接前置通道字节，文本帧前置通道字符
// 并对载荷做 base64 编码。返回长度是原始载荷长度，符合 io.Writer 的语义，
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

// websocketChannel 表示连接中的一个通道。
//
// 每个通道由一对 io.Pipe 构成：接收循环往 w 写，调用方从 r 读；
// 发送方向直接经 Conn.write 写到 WebSocket。num 是帧首部的通道号；
// read/write 是方向开关，禁用方向会被静默丢弃或返回 EOF，避免阻塞调用方。
type websocketChannel struct {
	conn *Conn
	num  byte
	r    io.Reader
	w    io.WriteCloser

	read, write bool
}

// newWebsocketChannel 创建通道的接收管道，read 和 write 决定其单向或双向能力。
// 连接打开前不应写入该管道。
//
// 使用同步的 io.Pipe 而非缓冲队列，可让慢读者自然形成反向压力；
// 没有读者时接收循环会阻塞，因此禁用读方向的通道由 DataFromSocket 直接丢弃载荷。
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

// DataFromSocket 由连接接收循环调用，将 WebSocket 载荷送入指定通道。
//
// 和 Write 对称：本端不读的通道直接丢弃载荷并报告“已消费”，
// 避免接收循环卡在无人读取的管道上，连带阻断其他通道。
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
