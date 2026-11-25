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

// NewStreamHandler 定义收到新 Stream 时调用的函数。
// 不返回错误时接受该 Stream，否则拒绝；
// 发送回复帧后关闭 replySent。
// 中文补充：replySent 关闭即表示服务端已把应答帧写出，handler 可据此做后续清理。
type NewStreamHandler func(stream Stream, replySent <-chan struct{}) error

// NoOpNewStreamHandler 是接受新流且不执行其他逻辑的处理器。
func NoOpNewStreamHandler(stream Stream, replySent <-chan struct{}) error { return nil }

// Dialer 知道如何与服务器建立流式连接。
type Dialer interface {

	//Dial 按偏好从高到低选择一种协议与服务器建立流式连接。
	// 中文补充：协议按偏好降序传入；返回实际选中的协议名供调用方记录。
	Dial(protocols ...string) (Connection, string, error)
}

// UpgradeRoundTripper 是一种可将 HTTP 请求升级为多路复用双向流的 http.RoundTripper。
// 调用 RoundTrip() 后，如果升级成功，
// 客户端可通过 UpgradeRoundTripper.Connection() 获取升级后的连接。
type UpgradeRoundTripper interface {
	http.RoundTripper
	//NewConnection 校验响应并创建新 Connection。
	// 中文补充：升级失败时由该调用返回错误，而不是在 RoundTrip 中暴露。
	NewConnection(resp *http.Response) (Connection, error)
}

// ResponseUpgrader 知道如何升级 HTTP 请求和响应以支持流式传输。
type ResponseUpgrader interface {
	//UpgradeResponse 将 HTTP 响应升级为支持多路复用流的响应。
	//升级连接的对端创建新流时，异步调用 newStreamHandler。
	// 中文补充：handler 在独立 goroutine 中调用，实现需自行保证并发安全。
	UpgradeResponse(w http.ResponseWriter, req *http.Request, newStreamHandler NewStreamHandler) Connection
}

// Connection 表示升级后的 HTTP 连接。
type Connection interface {
	//CreateStream 使用指定的请求头创建新 Stream。
	CreateStream(headers http.Header) (Stream, error)
	//Close 重置所有流并关闭连接。
	Close() error
	//CloseChan 返回一个在底层连接关闭时关闭的 channel。
	CloseChan() <-chan bool
	//SetIdleTimeout 设置连接在自动关闭前允许空闲的时长。
	SetIdleTimeout(timeout time.Duration)
	//RemoveStreams 可从 Connection 中移除一组流。
	RemoveStreams(streams ...Stream)
}

// Stream 表示升级连接中的双向通信通道。
type Stream interface {
	io.ReadWriteCloser
	//Reset 关闭流的两个方向，表示客户端和服务器都不再使用它。
	// 中文补充：Reset 用于异常终止，与正常 Close 的区别是不保证数据完整传输。
	Reset() error
	//Headers 返回创建流时使用的请求头。
	Headers() http.Header
	//Identifier 返回流的 ID。
	Identifier() uint32
}

// UpgradeFailureError 封装流升级请求失败的原因，并实现 error 接口。
type UpgradeFailureError struct {
	Cause error
}

// Error 返回包含底层原因的升级失败描述。
func (u *UpgradeFailureError) Error() string {
	return fmt.Sprintf("unable to upgrade streaming request: %s", u.Cause)
}

// 如果传入的错误是 UpgradeFailureError（或其包装链中包含该错误），IsUpgradeFailure 返回 true。
// 中文补充：用 errors.As 检查包装链，调用方无需自行 Unwrap。
func IsUpgradeFailure(err error) bool {
	if err == nil {
		return false
	}
	var upgradeErr *UpgradeFailureError
	return errors.As(err, &upgradeErr)
}

// 如果错误是 Gorilla/Websockets 的 HTTPS 代理拨号错误，isHTTPSProxyError 返回 true；
// 否则返回 false（参见 https://github.com/kubernetes/kubernetes/issues/126134）。
// 中文补充：Gorilla 的代理错误没有类型可断言，只能匹配其错误文本，因此该函数是启发式的。
func IsHTTPSProxyError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "proxy: unknown scheme: https")
}

// 如果给定请求是连接升级请求，IsUpgradeRequest 返回 true。
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

// Handshake 协商子协议。如果客户端请求了子协议，
// Handshake 会选择 serverProtocols 中的第一个共同值；
// 否则返回错误并向响应写入 HTTP BadRequest。
// 找到匹配项后，Handshake 添加响应头指明所选子协议。
// 没有匹配项时返回 HTTP forbidden，并
// 在响应头中列出服务器可接受的协议。
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
