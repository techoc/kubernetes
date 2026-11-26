/*
Copyright 2018 The Kubernetes Authors.

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

package authenticator

import (
	"context"
	"fmt"
	"net/http"
)

// authenticate 把“不感知 audience”的认证器包装成感知 audience 的行为：
// 先求隐式 audience 与请求目标 audience 的交集，为空则直接判定不匹配，
// 否则调用委托认证器并填充交集结果。
func authenticate(ctx context.Context, implicitAuds Audiences, authenticate func() (*Response, bool, error)) (*Response, bool, error) {
	targetAuds, ok := AudiencesFrom(ctx)
	// 当 api audiences 永不为空时可以移除此包装；那大概要等 TokenRequest GA 之后再过 N 个版本。
	// 旧客户端不传 audience 时退化为无条件认证，保持向后兼容。
	// 老客户端不传 audience 时退化为无条件认证，保持向后兼容。
	if !ok {
		return authenticate()
	}
	auds := implicitAuds.Intersect(targetAuds)
	if len(auds) == 0 {
		// 无交集意味着该认证器本就不服务于这些 audience，应视为未处理而非失败。
		return nil, false, nil
	}
	resp, ok, err := authenticate()
	if err != nil || !ok {
		return nil, false, err
	}
	if len(resp.Audiences) > 0 {
		// 也许该认证器本就感知 audience。
		// 包装了感知 audience 的认证器属于误用，显式报错以免 audience 被错误覆盖。
		return nil, false, fmt.Errorf("audience agnostic authenticator wrapped an authenticator that returned audiences: %q", resp.Audiences)
	}
	resp.Audiences = auds
	return resp, true, nil
}

// audAgnosticRequestAuthenticator 为 HTTP 请求认证器提供 audience 适配层。
type audAgnosticRequestAuthenticator struct {
	implicit Audiences
	delegate Request
}

// 编译期断言：确保实现了 Request 接口。
var _ = Request(&audAgnosticRequestAuthenticator{})

// AuthenticateRequest 委托给底层认证器，并补充 audience 校验。
func (a *audAgnosticRequestAuthenticator) AuthenticateRequest(req *http.Request) (*Response, bool, error) {
	return authenticate(req.Context(), a.implicit, func() (*Response, bool, error) {
		return a.delegate.AuthenticateRequest(req)
	})
}

// WrapAudienceAgnosticRequest 包装不感知 audience 的请求认证器，
// 将其接受的 audience 限制为一组隐式 audience。
func WrapAudienceAgnosticRequest(implicit Audiences, delegate Request) Request {
	return &audAgnosticRequestAuthenticator{
		implicit: implicit,
		delegate: delegate,
	}
}

// audAgnosticTokenAuthenticator 为 bearer token 认证器提供 audience 适配层。
type audAgnosticTokenAuthenticator struct {
	implicit Audiences
	delegate Token
}

// 编译期断言：确保实现了 Token 接口。
var _ = Token(&audAgnosticTokenAuthenticator{})

// AuthenticateToken 委托给底层 token 认证器，并补充 audience 校验。
func (a *audAgnosticTokenAuthenticator) AuthenticateToken(ctx context.Context, tok string) (*Response, bool, error) {
	return authenticate(ctx, a.implicit, func() (*Response, bool, error) {
		return a.delegate.AuthenticateToken(ctx, tok)
	})
}

// WrapAudienceAgnosticToken 包装不感知 audience 的 token 认证器，
// 将其接受的 audience 限制为一组隐式 audience。
func WrapAudienceAgnosticToken(implicit Audiences, delegate Token) Token {
	return &audAgnosticTokenAuthenticator{
		implicit: implicit,
		delegate: delegate,
	}
}
