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

package httpstream

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// 升级协商用到的 HTTP 头名称集中于此，服务端与客户端共用同一套常量，避免拼写漂移。
// HeaderProtocolVersion 由客户端声明支持的子协议列表，服务端用它回写最终选定的协议。
// HeaderAcceptedProtocolVersions 在协商失败时把可接受列表回给客户端。
const (
	HeaderConnection               = "Connection"
	HeaderUpgrade                  = "Upgrade"
	HeaderProtocolVersion          = "X-Stream-Protocol-Version"
	HeaderAcceptedProtocolVersions = "X-Accepted-Stream-Protocol-Versions"
)

// NewStreamHandler 定义新 Stream 到达时调用的处理函数。
// 返回 nil 表示接受该流，返回错误表示拒绝；replySent 在应答发送后关闭，
// 调用方可据此确保对端先看到应答，再执行依赖该顺序的操作。
type NewStreamHandler func(stream Stream, replySent <-chan struct{}) error

// NoOpNewStreamHandler 接受新流但不执行其他处理，适用于只关心连接建立的调用方。
func NoOpNewStreamHandler(stream Stream, replySent <-chan struct{}) error { return nil }

// Dialer 定义与服务端建立流式连接的能力。
// 把“按协议优先级协商建链”抽象出来，调用方无需关心底层用的是 SPDY 还是其他实现。
type Dialer interface {

	// Dial 按 protocols 提供的优先级顺序选择协议，并与服务端建立流式连接。
	Dial(protocols ...string) (Connection, string, error)
}

// UpgradeRoundTripper 扩展 http.RoundTripper，使 HTTP 请求能够升级为支持双向多路复用流的连接。
// RoundTrip 成功完成升级后，客户端可调用 NewConnection 获取升级后的连接。
// 把“发请求”与“取连接”拆成两步，让调用方先检查 HTTP 响应再决定是否接管底层连接。
type UpgradeRoundTripper interface {
	http.RoundTripper
	// NewConnection 校验升级响应，并创建新的流式 Connection。
	NewConnection(resp *http.Response) (Connection, error)
}

// ResponseUpgrader 定义服务端升级 HTTP 请求/响应、启用流式传输的能力。
// 服务端视角的升级入口，与 UpgradeRoundTripper（客户端视角）配对使用。
type ResponseUpgrader interface {
	// UpgradeResponse 将 HTTP 响应升级为支持多路复用流的响应。
	// 对端每创建一个新流，newStreamHandler 都会被异步调用。
	UpgradeResponse(w http.ResponseWriter, req *http.Request, newStreamHandler NewStreamHandler) Connection
}

// Connection 表示升级后的 HTTP 流式连接。
// 用细粒度方法暴露生命周期与流管理，具体复用方式由底层协议自行决定。
type Connection interface {
	// CreateStream 使用指定请求头创建一个新 Stream。
	CreateStream(headers http.Header) (Stream, error)
	// Close 重置该连接上的所有 Stream，并关闭底层连接。
	Close() error
	// CloseChan 返回一个通道；底层连接关闭时，该通道也会关闭。
	CloseChan() <-chan bool
	// SetIdleTimeout 设置连接允许保持空闲的最长时间；超过该时间后连接会自动关闭。
	SetIdleTimeout(timeout time.Duration)
	// RemoveStreams 从 Connection 管理的集合中移除指定的 Stream。
	RemoveStreams(streams ...Stream)
}

// Stream 表示升级连接中的一个双向通信通道。
// 除了普通的读写关闭，还暴露 Reset 与头部/标识，便于在不关闭整条连接的情况下终止单条流。
type Stream interface {
	io.ReadWriteCloser
	// Reset 同时关闭流的两个方向，表示客户端和服务端都不能再使用该流。
	Reset() error
	// Headers 返回创建该 Stream 时使用的请求头。
	Headers() http.Header
	// Identifier 返回该 Stream 的唯一标识。
	Identifier() uint32
}

// UpgradeFailureError 封装流式连接升级失败的具体原因，并实现 error 接口。
// 包装一层具体原因，使上层能通过 errors.As 稳定识别“升级失败”这一类错误。
type UpgradeFailureError struct {
	// Cause 是导致升级失败的错误。
	Cause error
}

func (u *UpgradeFailureError) Error() string {
	return fmt.Sprintf("unable to upgrade streaming request: %s", u.Cause)
}

// IsUpgradeFailure 判断给定错误本身或其包装链中是否包含 UpgradeFailureError。
// 用 errors.As 而不是类型断言，兼容调用链上追加的上下文信息。
func IsUpgradeFailure(err error) bool {
	if err == nil {
		return false
	}
	var upgradeErr *UpgradeFailureError
	return errors.As(err, &upgradeErr)
}

// IsHTTPSProxyError 判断错误是否为 Gorilla/WebSocket 在拨号 HTTPS 代理时产生的特定错误；
// 相关背景见 https://github.com/kubernetes/kubernetes/issues/126134。
// 该错误没有公开类型可断言，只能按已知文本匹配；识别出来后上层才能给出针对性提示。
func IsHTTPSProxyError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "proxy: unknown scheme: https")
}

// IsUpgradeRequest 判断给定 HTTP 请求是否包含连接升级标头。
// 逐项检查 Connection 头并忽略大小写，兼容不同客户端的写法。
func IsUpgradeRequest(req *http.Request) bool {
	for _, h := range req.Header[http.CanonicalHeaderKey(HeaderConnection)] {
		if strings.Contains(strings.ToLower(h), strings.ToLower(HeaderUpgrade)) {
			return true
		}
	}
	return false
}

// 按客户端偏好的顺序挑选第一个服务端也支持的协议；全不匹配时返回空串由调用方决定如何拒绝。
func negotiateProtocol(clientProtocols, serverProtocols []string) string {
	for i := range clientProtocols {
		for j := range serverProtocols {
			if clientProtocols[i] == serverProtocols[j] {
				return clientProtocols[i]
			}
		}
	}
	return ""
}

// 把可能以逗号分隔、带空格的协议头拆平并去空，便于逐项做精确匹配。
func commaSeparatedHeaderValues(header []string) []string {
	var parsedClientProtocols []string
	for i := range header {
		for _, clientProtocol := range strings.Split(header[i], ",") {
			if proto := strings.Trim(clientProtocol, " "); len(proto) > 0 {
				parsedClientProtocols = append(parsedClientProtocols, proto)
			}
		}
	}
	return parsedClientProtocols
}

// Handshake 执行子协议协商：从客户端提出的协议中选择服务端支持的首个匹配项。
// 缺少有效子协议时返回错误并写入 HTTP 400；双方没有共同协议时返回 HTTP 403，
// 并通过响应头告知客户端服务端支持的协议列表。协商成功时在响应头中写入选定协议。
func Handshake(req *http.Request, w http.ResponseWriter, serverProtocols []string) (string, error) {
	// 没有可提供的协议属于服务端配置错误，直接 panic 让问题尽早暴露。
	if len(serverProtocols) == 0 {
		panic(fmt.Errorf("unable to upgrade: serverProtocols is required"))
	}
	values, ok := req.Header[http.CanonicalHeaderKey(HeaderProtocolVersion)]
	if !ok {
		err := fmt.Errorf("unable to upgrade: header %s does not exist in request with %d headers", HeaderProtocolVersion, len(req.Header))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return "", err
	}
	if len(values) == 0 {
		err := fmt.Errorf("unable to upgrade: header %s is empty", HeaderProtocolVersion)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return "", err
	}
	clientProtocols := commaSeparatedHeaderValues(values)
	// 头存在但只有逗号和空格，等同于客户端没给出有效协议。
	if len(clientProtocols) == 0 {
		err := fmt.Errorf("unable to upgrade: header %s contains %s, but no valid protocols", HeaderProtocolVersion, values)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return "", err
	}

	negotiatedProtocol := negotiateProtocol(clientProtocols, serverProtocols)
	if len(negotiatedProtocol) == 0 {
		// 告知客户端服务端能接受什么，便于其按新列表重试而不是盲目重试。
		for i := range serverProtocols {
			w.Header().Add(HeaderAcceptedProtocolVersions, serverProtocols[i])
		}
		err := fmt.Errorf("unable to upgrade: unable to negotiate protocol: client supports %v, server accepts %v", clientProtocols, serverProtocols)
		http.Error(w, err.Error(), http.StatusForbidden)
		return "", err
	}

	w.Header().Add(HeaderProtocolVersion, negotiatedProtocol)
	return negotiatedProtocol, nil
}
