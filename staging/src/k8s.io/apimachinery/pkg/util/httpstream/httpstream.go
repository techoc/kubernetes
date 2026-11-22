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

// HTTP 头常量：Connection/Upgrade 用于连接升级，X-Stream-Protocol-Version 系列用于子协议协商。
const (
	HeaderConnection               = "Connection"
	HeaderUpgrade                  = "Upgrade"
	HeaderProtocolVersion          = "X-Stream-Protocol-Version"
	HeaderAcceptedProtocolVersions = "X-Accepted-Stream-Protocol-Versions"
)

// NewStreamHandler defines a function that is called when a new Stream is
// received. If no error is returned, the Stream is accepted; otherwise,
// the stream is rejected. After the reply frame has been sent, replySent is closed.
// 中文补充：replySent 关闭即表示服务端已把应答帧写出，handler 可据此做后续清理。
type NewStreamHandler func(stream Stream, replySent <-chan struct{}) error

// NoOpNewStreamHandler is a stream handler that accepts a new stream and
// performs no other logic.
func NoOpNewStreamHandler(stream Stream, replySent <-chan struct{}) error { return nil }

// Dialer knows how to open a streaming connection to a server.
type Dialer interface {

	// Dial opens a streaming connection to a server using one of the protocols
	// specified (in order of most preferred to least preferred).
	// 中文补充：协议按偏好降序传入；返回实际选中的协议名供调用方记录。
	Dial(protocols ...string) (Connection, string, error)
}

// UpgradeRoundTripper is a type of http.RoundTripper that is able to upgrade
// HTTP requests to support multiplexed bidirectional streams. After RoundTrip()
// is invoked, if the upgrade is successful, clients may retrieve the upgraded
// connection by calling UpgradeRoundTripper.Connection().
type UpgradeRoundTripper interface {
	http.RoundTripper
	// NewConnection validates the response and creates a new Connection.
	// 中文补充：升级失败时由该调用返回错误，而不是在 RoundTrip 中暴露。
	NewConnection(resp *http.Response) (Connection, error)
}

// ResponseUpgrader knows how to upgrade HTTP requests and responses to
// add streaming support to them.
type ResponseUpgrader interface {
	// UpgradeResponse upgrades an HTTP response to one that supports multiplexed
	// streams. newStreamHandler will be called asynchronously whenever the
	// other end of the upgraded connection creates a new stream.
	// 中文补充：handler 在独立 goroutine 中调用，实现需自行保证并发安全。
	UpgradeResponse(w http.ResponseWriter, req *http.Request, newStreamHandler NewStreamHandler) Connection
}

// Connection represents an upgraded HTTP connection.
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
type Stream interface {
	io.ReadWriteCloser
	// Reset closes both directions of the stream, indicating that neither client
	// or server can use it any more.
	// 中文补充：Reset 用于异常终止，与正常 Close 的区别是不保证数据完整传输。
	Reset() error
	// Headers returns the headers used to create the stream.
	Headers() http.Header
	// Identifier returns the stream's ID.
	Identifier() uint32
}

// UpgradeFailureError encapsulates the cause for why the streaming
// upgrade request failed. Implements error interface.
type UpgradeFailureError struct {
	Cause error
}

// Error 返回包含底层原因的升级失败描述。
func (u *UpgradeFailureError) Error() string {
	return fmt.Sprintf("unable to upgrade streaming request: %s", u.Cause)
}

// IsUpgradeFailure returns true if the passed error is (or wrapped error contains)
// the UpgradeFailureError.
// 中文补充：用 errors.As 检查包装链，调用方无需自行 Unwrap。
func IsUpgradeFailure(err error) bool {
	if err == nil {
		return false
	}
	var upgradeErr *UpgradeFailureError
	return errors.As(err, &upgradeErr)
}

// isHTTPSProxyError returns true if error is Gorilla/Websockets HTTPS Proxy dial error;
// false otherwise (see https://github.com/kubernetes/kubernetes/issues/126134).
// 中文补充：Gorilla 的代理错误没有类型可断言，只能匹配其错误文本，因此该函数是启发式的。
func IsHTTPSProxyError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "proxy: unknown scheme: https")
}

// IsUpgradeRequest returns true if the given request is a connection upgrade request
// 中文补充：按 RFC 7230 逐个检查 Connection 头中的 token（大小写不敏感），
// 而不是简单比较整串，以兼容 "keep-alive, Upgrade" 这类组合值。
func IsUpgradeRequest(req *http.Request) bool {
	for _, h := range req.Header[http.CanonicalHeaderKey(HeaderConnection)] {
		if strings.Contains(strings.ToLower(h), strings.ToLower(HeaderUpgrade)) {
			return true
		}
	}
	return false
}

// negotiateProtocol 按客户端偏好顺序寻找第一个服务端也支持的协议；
// 嵌套循环的顺序决定了“客户端优先级”优先于“服务端优先级”。
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

// commaSeparatedHeaderValues 把可能包含逗号分隔多个值的头部拆成扁平列表，
// 并丢弃空 token（如尾部多余的逗号）。
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
//
// 中文补充：各失败分支直接写 HTTP 响应，因此调用方不应再向 w 写入内容。
func Handshake(req *http.Request, w http.ResponseWriter, serverProtocols []string) (string, error) {
	// 服务端没有可协商协议属于编程错误，用 panic 尽早暴露。
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
	if len(clientProtocols) == 0 {
		err := fmt.Errorf("unable to upgrade: header %s contains %s, but no valid protocols", HeaderProtocolVersion, values)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return "", err
	}

	negotiatedProtocol := negotiateProtocol(clientProtocols, serverProtocols)
	if len(negotiatedProtocol) == 0 {
		// 协商失败时把服务端支持的协议列表回传，便于客户端诊断或降级。
		for i := range serverProtocols {
			w.Header().Add(HeaderAcceptedProtocolVersions, serverProtocols[i])
		}
		err := fmt.Errorf("unable to upgrade: unable to negotiate protocol: client supports %v, server accepts %v", clientProtocols, serverProtocols)
		http.Error(w, err.Error(), http.StatusForbidden)
		return "", err
	}

	// 成功时回写选定的子协议，客户端据此确认后续帧使用的协议。
	w.Header().Add(HeaderProtocolVersion, negotiatedProtocol)
	return negotiatedProtocol, nil
}
