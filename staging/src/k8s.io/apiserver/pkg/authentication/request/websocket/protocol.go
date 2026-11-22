/*
Copyright 2017 The Kubernetes Authors.

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

// Package websocket 实现从 WebSocket 子协议中提取 bearer token 的
// authenticator.Request。浏览器无法自定义 Authorization 头，因此把 token
// 编码进 Sec-WebSocket-Protocol 是唯一可行的鉴权方式。
package websocket

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/textproto"
	"strings"
	"unicode/utf8"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/streaming/pkg/httpstream/wsstream"
)

// bearerProtocolPrefix 是承载 token 的子协议前缀；后面紧跟无填充的
// base64url 编码 token。
const bearerProtocolPrefix = "base64url.bearer.authorization.k8s.io."

// protocolHeader 预先规范化头部名，避免每次请求都做大小写转换。
var protocolHeader = textproto.CanonicalMIMEHeaderKey("Sec-WebSocket-Protocol")

var errInvalidToken = errors.New("invalid bearer token")

// ProtocolAuthenticator allows a websocket connection to provide a bearer token as a subprotocol
// in the format "base64url.bearer.authorization.<base64url-without-padding(bearer-token)>"
//
// 之所以使用子协议而不是查询参数：token 不应出现在 URL/访问日志中，
// 而子协议同样只存在于请求头里。
type ProtocolAuthenticator struct {
	// auth is the token authenticator to use to validate the token
	auth authenticator.Token
}

// NewProtocolAuthenticator 包装一个 token authenticator。
func NewProtocolAuthenticator(auth authenticator.Token) *ProtocolAuthenticator {
	return &ProtocolAuthenticator{auth}
}

// AuthenticateRequest 从 Sec-WebSocket-Protocol 中解析 token 并委托给
// 底层 authenticator。非 WebSocket 请求返回 (nil, false, nil)，表示不处理。
func (a *ProtocolAuthenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	// Only accept websocket connections
	if !wsstream.IsWebSocketRequest(req) {
		return nil, false, nil
	}

	token := ""
	sawTokenProtocol := false
	// filteredProtocols 保存除 token 子协议外的其它子协议：认证成功后需要
	// 把它们回写，客户端依赖服务端 echo 的子协议才能完成握手。
	filteredProtocols := []string{}
	for _, protocolHeader := range req.Header[protocolHeader] {
		for _, protocol := range strings.Split(protocolHeader, ",") {
			protocol = strings.TrimSpace(protocol)

			if !strings.HasPrefix(protocol, bearerProtocolPrefix) {
				filteredProtocols = append(filteredProtocols, protocol)
				continue
			}

			// 多个 token 子协议意味着请求被拼接或伪造，直接拒绝而不是
			// 任选其一。
			if sawTokenProtocol {
				return nil, false, errors.New("multiple base64.bearer.authorization tokens specified")
			}
			sawTokenProtocol = true

			encodedToken := strings.TrimPrefix(protocol, bearerProtocolPrefix)
			decodedToken, err := base64.RawURLEncoding.DecodeString(encodedToken)
			if err != nil {
				return nil, false, errors.New("invalid base64.bearer.authorization token encoding")
			}
			// token 必须是合法 UTF-8，否则无法作为字符串交给 authenticator。
			if !utf8.Valid(decodedToken) {
				return nil, false, errors.New("invalid base64.bearer.authorization token")
			}
			token = string(decodedToken)
		}
	}

	// Must pass at least one other subprotocol so that we can remove the one containing the bearer token,
	// and there is at least one to echo back to the client
	//
	// 若只剩 token 子协议，认证成功后会被整体移除，客户端将收到空的
	// Sec-WebSocket-Protocol，握手失败；因此提前报错更清晰。
	if len(token) > 0 && len(filteredProtocols) == 0 {
		return nil, false, errors.New("missing additional subprotocol")
	}

	if len(token) == 0 {
		return nil, false, nil
	}

	resp, ok, err := a.auth.AuthenticateToken(req.Context(), token)

	// on success, remove the protocol with the token
	if ok {
		// https://tools.ietf.org/html/rfc6455#section-11.3.4 indicates the Sec-WebSocket-Protocol header may appear multiple times
		// in a request, and is logically the same as a single Sec-WebSocket-Protocol header field that contains all values
		//
		// 回写时必须移除 token，防止它被下游处理器或日志记录。
		req.Header.Set(protocolHeader, strings.Join(filteredProtocols, ","))
	}

	// If the token authenticator didn't error, provide a default error
	if !ok && err == nil {
		err = errInvalidToken
	}

	return resp, ok, err
}
