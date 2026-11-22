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

// Package authenticator 定义认证器的核心接口：从 token 或 HTTP 请求中识别用户，
// 并通过 Response 报告认证结果与受众信息。
package authenticator

import (
	"context"
	"net/http"

	"k8s.io/apiserver/pkg/authentication/user"
)

// Token checks a string value against a backing authentication store and
// returns a Response or an error if the token could not be checked.
//
// 返回值语义：(response, true, nil) 表示认证成功；(nil, false, nil) 表示该认证器
// 不认识此 token，应继续尝试下一个；(nil, false, err) 表示认证失败或后端出错。
type Token interface {
	AuthenticateToken(ctx context.Context, token string) (*Response, bool, error)
}

// Request attempts to extract authentication information from a request and
// returns a Response or an error if the request could not be checked.
type Request interface {
	AuthenticateRequest(req *http.Request) (*Response, bool, error)
}

// TokenFunc is a function that implements the Token interface.
type TokenFunc func(ctx context.Context, token string) (*Response, bool, error)

// AuthenticateToken implements authenticator.Token.
func (f TokenFunc) AuthenticateToken(ctx context.Context, token string) (*Response, bool, error) {
	return f(ctx, token)
}

// RequestFunc is a function that implements the Request interface.
type RequestFunc func(req *http.Request) (*Response, bool, error)

// AuthenticateRequest implements authenticator.Request.
func (f RequestFunc) AuthenticateRequest(req *http.Request) (*Response, bool, error) {
	return f(req)
}

// Response is the struct returned by authenticator interfaces upon successful
// authentication. It contains information about whether the authenticator
// authenticated the request, information about the context of the
// authentication, and information about the authenticated user.
type Response struct {
	// Audiences is the set of audiences the authenticator was able to validate
	// the token against. If the authenticator is not audience aware, this field
	// will be empty.
	Audiences Audiences
	// User is the UserInfo associated with the authentication context.
	User user.Info
}
