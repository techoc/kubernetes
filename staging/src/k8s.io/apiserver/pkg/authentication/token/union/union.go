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

// Package union 把多个 token 认证器串成链：按顺序尝试，直到某个认证器
// 成功或（可选地）返回错误。它只处理 token，不涉及请求头解析。
package union

import (
	"context"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/authentication/authenticator"
)

// unionAuthTokenHandler authenticates tokens using a chain of authenticator.Token objects
// unionAuthTokenHandler 是不可变链的持有者；所有字段在构造时确定，因此并发
// 调用 AuthenticateToken 安全。
type unionAuthTokenHandler struct {
	// Handlers is a chain of request authenticators to delegate to
	Handlers []authenticator.Token
	// FailOnError determines whether an error returns short-circuits the chain
	// FailOnError 用于“不可忽略错误”的场景（如匿名认证前的强制认证链）。
	FailOnError bool
}

// New returns a token authenticator that validates credentials using a chain of authenticator.Token objects.
// The entire chain is tried until one succeeds. If all fail, an aggregate error is returned.
// 单个认证器时直接返回它，省去一层间接调用。
func New(authTokenHandlers ...authenticator.Token) authenticator.Token {
	if len(authTokenHandlers) == 1 {
		return authTokenHandlers[0]
	}
	return &unionAuthTokenHandler{Handlers: authTokenHandlers, FailOnError: false}
}

// NewFailOnError returns a token authenticator that validates credentials using a chain of authenticator.Token objects.
// The first error short-circuits the chain.
// 错误短路可避免在某个认证器不可用时继续尝试后续认证器，从而防止降级绕过。
func NewFailOnError(authTokenHandlers ...authenticator.Token) authenticator.Token {
	if len(authTokenHandlers) == 1 {
		return authTokenHandlers[0]
	}
	return &unionAuthTokenHandler{Handlers: authTokenHandlers, FailOnError: true}
}

// AuthenticateToken authenticates the token using a chain of authenticator.Token objects.
// 成功即返回；默认模式下收集错误继续尝试，全部失败后聚合返回，调用方可用
// 错误判断是“凭据无效”还是“后端不可用”。
func (authHandler *unionAuthTokenHandler) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	var errlist []error
	for _, currAuthRequestHandler := range authHandler.Handlers {
		info, ok, err := currAuthRequestHandler.AuthenticateToken(ctx, token)
		if err != nil {
			if authHandler.FailOnError {
				return info, ok, err
			}
			errlist = append(errlist, err)
			continue
		}

		if ok {
			return info, ok, err
		}
	}

	return nil, false, utilerrors.NewAggregate(errlist)
}
