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
// HeaderProtocolVersion 由客户端声明支持的子协议，服务端用它回写最终选定的那一个。
// HeaderAcceptedProtocolVersions 在协商失败时把可接受列表回给客户端。
const (
	HeaderConnection               = "Connection"
	HeaderUpgrade                  = "Upgrade"
	HeaderProtocolVersion          = "X-Stream-Protocol-Version"
	HeaderAcceptedProtocolVersions = "X-Accepted-Stream-Protocol-Versions"
)

// NewStreamHandler defines a function that is called when a new Stream is
// received. If no error is returned, the Stream is accepted; otherwise,
// the stream is rejected. After the reply frame has been sent, replySent is closed.
// 返回错误即拒绝新流；replySent 让调用方能在对方看到应答之后再决定下一步。
type NewStreamHandler func(stream Stream, replySent <-chan struct{}) error

// NoOpNewStreamHandler is a stream handler that accepts a new stream and
// performs no other logic.
// 占位用的接受策略：调用方只关心连接建立、不处理流内容时传入它。
func NoOpNewStreamHandler(stream Stream, replySent <-chan struct{}) error { return nil }

// Dialer knows how to open a streaming connection to a server.
// 把“按协议优先级协商建链”抽象出来，调用方无需关心底层用的是 SPDY 还是其他实现。
type Dialer interface {

	// Dial opens a streaming connection to a server using one of the protocols
	// specified (in order of most preferred to least preferred).
	Dial(protocols ...string) (Connection, string, error)
}

// UpgradeRoundTripper is a type of http.RoundTripper that is able to upgrade
// HTTP requests to support multiplexed bidirectional streams. After RoundTrip()
// is invoked, if the upgrade is successful, clients may retrieve the upgraded
// connection by calling UpgradeRoundTripper.Connection().
// 把“发请求”与“取连接”拆成两步，让调用方先检查 HTTP 响应再决定是否接管底层连接。
type UpgradeRoundTripper interface {
	http.RoundTripper
	// NewConnection validates the response and creates a new Connection.
	NewConnection(resp *http.Response) (Connection, error)
}

// ResponseUpgrader knows how to upgrade HTTP requests and responses to
// add streaming support to them.
// 服务端视角的升级入口，与 UpgradeRoundTripper（客户端视角）配对使用。
type ResponseUpgrader interface {
	// UpgradeResponse upgrades an HTTP response to one that supports multiplexed
	// streams. newStreamHandler will be called asynchronously whenever the
	// other end of the upgraded connection creates a new stream.
	UpgradeResponse(w http.ResponseWriter, req *http.Request, newStreamHandler NewStreamHandler) Connection
}

// Connection represents an upgraded HTTP connection.
// 用细粒度方法暴露生命周期与流管理，具体复用方式由底层协议自行决定。
type Connection interface {
	// CreateStream creates a new Stream with the supplied headers.
	CreateStream(headers http.Header) (Stream, error)
	// Close resets all streams and closes the connection.
	Close() error
	// CloseChan returns a channel that is closed when the underlying connection is closed.
	CloseChan() <-chan bool
	// SetIdleTimeout sets the amount of time the connection may remain idle before
	// it is automatically closed.
	SetIdleTimeout(timeout time.Duration)
	// RemoveStreams can be used to remove a set of streams from the Connection.
	RemoveStreams(streams ...Stream)
}

// Stream represents a bidirectional communications channel that is part of an
// upgraded connection.
// 除了普通的读写关闭，还暴露 Reset 与头部/标识，便于在不关闭整条连接的情况下终止单条流。
type Stream interface {
	io.ReadWriteCloser
	// Reset closes both directions of the stream, indicating that neither client
	// or server can use it any more.
	Reset() error
	// Headers returns the headers used to create the stream.
	Headers() http.Header
	// Identifier returns the stream's ID.
	Identifier() uint32
}

// UpgradeFailureError encapsulates the cause for why the streaming
// upgrade request failed. Implements error interface.
// 包装一层具体原因，使上层能通过 errors.As 稳定识别“升级失败”这一类错误。
type UpgradeFailureError struct {
	Cause error
}

func (u *UpgradeFailureError) Error() string {
	return fmt.Sprintf("unable to upgrade streaming request: %s", u.Cause)
}

// IsUpgradeFailure returns true if the passed error is (or wrapped error contains)
// the UpgradeFailureError.
// 用 errors.As 而不是类型断言，兼容调用链上追加的上下文信息。
func IsUpgradeFailure(err error) bool {
	if err == nil {
		return false
	}
	var upgradeErr *UpgradeFailureError
	return errors.As(err, &upgradeErr)
}

// isHTTPSProxyError returns true if error is Gorilla/Websockets HTTPS Proxy dial error;
// false otherwise (see https://github.com/kubernetes/kubernetes/issues/126134).
// 该错误没有公开类型可断言，只能按已知文本匹配；识别出来后上层才能给出针对性提示。
func IsHTTPSProxyError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "proxy: unknown scheme: https")
}

// IsUpgradeRequest returns true if the given request is a connection upgrade request
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

// Handshake performs a subprotocol negotiation. If the client did request a
// subprotocol, Handshake will select the first common value found in
// serverProtocols, otherwise it will return an error and write an HTTP BadRequest to the response.
// If a match is found, Handshake adds a response header indicating the chosen subprotocol.
// If no match is found, HTTP forbidden is returned, along with a response header containing
// the list of protocols the server can accept.
// 用状态码区分失败原因：缺头/空头是 400，双方协议不兼容是 403 并回带可接受列表。
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
