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

package group

import (
	"context"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

// TokenGroupAdder 为已认证用户追加额外组。
// 典型用途：把来自令牌的固定身份（例如某个 service account）并入
// 预定义的组，从而复用 RBAC 中按组授权的规则，而无需逐用户绑定。
type TokenGroupAdder struct {
	// Authenticator 负责实际的身份认证决策
	Authenticator authenticator.Token
	// Groups 是认证成功后追加到 user.Info 的额外组
	Groups []string
}

// NewTokenGroupAdder 包装一个令牌认证器，认证成功时把指定组追加到返回的用户上。
// 返回接口类型而非具体类型，便于在调用链中与其它 authenticator.Token 装饰器组合。
func NewTokenGroupAdder(auth authenticator.Token, groups []string) authenticator.Token {
	return &TokenGroupAdder{auth, groups}
}

// AuthenticateToken 先委托底层认证器，成功后再合并组。
// 认证失败（ok=false 或 err!=nil）时原样返回，不修改结果，
// 避免在认证未通过时构造出任何用户身份。
func (g *TokenGroupAdder) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	r, ok, err := g.Authenticator.AuthenticateToken(ctx, token)
	if err != nil || !ok {
		return nil, ok, err
	}

	// 预分配容量，避免两次 append 触发多次扩容。
	newGroups := make([]string, 0, len(r.User.GetGroups())+len(g.Groups))
	newGroups = append(newGroups, r.User.GetGroups()...)
	newGroups = append(newGroups, g.Groups...)

	// 浅拷贝 Response，再替换 User：不能原地修改底层认证器返回的对象，
	// 因为该对象可能被缓存或被其他调用方共享。
	ret := *r // shallow copy
	ret.User = &user.DefaultInfo{
		Name:   r.User.GetName(),
		UID:    r.User.GetUID(),
		Groups: newGroups,
		Extra:  r.User.GetExtra(),
	}
	return &ret, true, nil
}
