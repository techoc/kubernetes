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

// Token 依据后端认证存储校验字符串 token，
// 成功返回 Response，无法校验时返回错误。
//
// 返回值语义：(response, true, nil) 表示认证成功；(nil, false, nil) 表示该认证器
// 不认识此 token，应继续尝试下一个；(nil, false, err) 表示认证失败或后端出错。
type Token interface {
	AuthenticateToken(ctx context.Context, token string) (*Response, bool, error)
}

// Request 尝试从请求中提取认证信息，
// 无法校验请求时返回错误。
type Request interface {
	AuthenticateRequest(req *http.Request) (*Response, bool, error)
}

// TokenFunc 是实现 Token 接口的函数类型。
type TokenFunc func(ctx context.Context, token string) (*Response, bool, error)

// AuthenticateToken 实现 authenticator.Token。
func (f TokenFunc) AuthenticateToken(ctx context.Context, token string) (*Response, bool, error) {
	return f(ctx, token)
}

// RequestFunc 是实现 Request 接口的函数类型。
type RequestFunc func(req *http.Request) (*Response, bool, error)

// AuthenticateRequest 实现 authenticator.Request。
func (f RequestFunc) AuthenticateRequest(req *http.Request) (*Response, bool, error) {
	return f(req)
}

// Response 是认证器在认证成功时返回的结构体。
// 它携带认证是否成功、认证上下文（受众）以及认证用户的信息。
type Response struct {
	// Audiences 是认证器校验 token 时认可的受众集合。
	// 若认证器不感知受众，此字段为空。
	//
	Audiences Audiences
	// User 是与认证上下文关联的 UserInfo。
	User user.Info
}
