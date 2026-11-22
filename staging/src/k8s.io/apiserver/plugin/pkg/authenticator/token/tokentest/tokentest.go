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

// Package tokentest 提供基于内存 map 的 TokenAuthenticator 测试实现。
package tokentest

import (
	"context"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

// TokenAuthenticator 按 token 字符串查表完成认证，仅用于测试。
// 注意：Tokens 不是并发安全的，测试中如需并发访问需自行加锁。
type TokenAuthenticator struct {
	Tokens map[string]*user.DefaultInfo
}

// New 返回带空 token 表的 TokenAuthenticator。
func New() *TokenAuthenticator {
	return &TokenAuthenticator{
		Tokens: make(map[string]*user.DefaultInfo),
	}
}

// AuthenticateToken 查表返回对应用户；未命中时返回 (nil, false, nil)，
// 表示“本认证器不认这个 token”，而不是认证失败。
func (a *TokenAuthenticator) AuthenticateToken(ctx context.Context, value string) (*authenticator.Response, bool, error) {
	user, ok := a.Tokens[value]
	if !ok {
		return nil, false, nil
	}
	return &authenticator.Response{User: user}, true, nil
}
