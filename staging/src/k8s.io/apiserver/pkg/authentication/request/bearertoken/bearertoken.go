/*
Copyright 2014 The Kubernetes Authors.

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

package bearertoken

import (
	"errors"
	"net/http"
	"strings"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/warning"
)

const (
	// 当 Authorization 头形如 "Bearer  token"（token 前多一个空格）时，
	// 按规范解析会得到空 token。这里除拒绝外还回传一条 warning，
	// 帮助客户端定位拼写问题，而不是只看到含糊的 401。
	invalidTokenWithSpaceWarning = "the provided Authorization header contains extra space before the bearer token, and is ignored"
)

// Authenticator 从 HTTP Authorization 头中提取 Bearer token 并交给
// authenticator.Token 验证。它只负责解析与转发，不持有任何凭据状态，
// 因此可被多个请求并发复用。
type Authenticator struct {
	auth authenticator.Token
}

// New 使用给定的 token 认证器构造 Authenticator。
func New(auth authenticator.Token) *Authenticator {
	return &Authenticator{auth}
}

// invalidToken 用于 token 认证器既未返回错误也未通过时兜底，
// 保证调用方永远不会收到 (nil, false, nil) 这种语义含糊的结果。
var invalidToken = errors.New("invalid bearer token")

// AuthenticateRequest 实现 authenticator.Request。
// 返回值语义：ok 表示认证成功；err 非空表示认证失败且应终止请求。
func (a *Authenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	// 去掉首尾空白后再判断，避免因代理追加空格而误判为非 Bearer。
	auth := strings.TrimSpace(req.Header.Get("Authorization"))
	if auth == "" {
		return nil, false, nil
	}
	// SplitN 限制为 3 段，以便识别 "Bearer  token"（中间多空格）这种错误格式。
	parts := strings.SplitN(auth, " ", 3)
	// 非 Bearer 方案交给后续认证器处理，而不是直接拒绝。
	if len(parts) < 2 || strings.ToLower(parts[0]) != "bearer" {
		return nil, false, nil
	}

	token := parts[1]

	// Empty bearer tokens aren't valid
	if len(token) == 0 {
		// The space before the token case
		// 三段说明 token 前存在额外空格，给出可操作的 warning。
		if len(parts) == 3 {
			warning.AddWarning(req.Context(), "", invalidTokenWithSpaceWarning)
		}
		return nil, false, nil
	}

	resp, ok, err := a.auth.AuthenticateToken(req.Context(), token)
	// if we authenticated successfully, go ahead and remove the bearer token so that no one
	// is ever tempted to use it inside of the API server
	// 认证成功后立即删除头，防止下游组件（审计、代理、控制器）意外记录或转发凭据。
	if ok {
		req.Header.Del("Authorization")
	}

	// If the token authenticator didn't error, provide a default error
	if !ok && err == nil {
		err = invalidToken
	}

	return resp, ok, err
}
