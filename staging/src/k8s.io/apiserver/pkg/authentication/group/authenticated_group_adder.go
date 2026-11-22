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

// Package group 提供对认证结果中的用户组进行补充的包装器，
// 例如为已认证用户自动加上 system:authenticated。
//
// 中文说明：这些包装器位于认证器与鉴权器之间，只调整 user.Info，不改变认证结论。
package group

import (
	"net/http"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

// AuthenticatedGroupAdder adds system:authenticated group when appropriate
//
// 中文说明：所有通过认证的非匿名用户都应属于 system:authenticated，但认证器本身
// 不保证这一点；该包装器补上该组，使 RBAC 的 group 绑定对任意认证方式一致生效。
type AuthenticatedGroupAdder struct {
	// Authenticator is delegated to make the authentication decision
	Authenticator authenticator.Request
}

// NewAuthenticatedGroupAdder wraps a request authenticator, and adds the system:authenticated group when appropriate.
// Authentication must succeed, the user must not be system:anonymous, the groups system:authenticated or system:unauthenticated must
// not be present
//
// 中文说明：认证失败时原样返回，不修改错误；包装器只在成功且确有必要时追加组。
func NewAuthenticatedGroupAdder(auth authenticator.Request) authenticator.Request {
	return &AuthenticatedGroupAdder{auth}
}

// AuthenticateRequest 委托底层认证器，并视情况补充 system:authenticated 组。
func (g *AuthenticatedGroupAdder) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	r, ok, err := g.Authenticator.AuthenticateRequest(req)
	if err != nil || !ok {
		return nil, ok, err
	}

	// 匿名用户不属于 authenticated；其组由认证器自行决定。
	if r.User.GetName() == user.Anonymous {
		return r, true, nil
	}
	// 已显式带有 system:authenticated 或 system:unauthenticated 时不再追加，
	// 后者尤其重要：不能把标记为未认证的用户“升级”为已认证。
	for _, group := range r.User.GetGroups() {
		if group == user.AllAuthenticated || group == user.AllUnauthenticated {
			return r, true, nil
		}
	}

	// 不修改原对象：底层响应可能被缓存或被其它过滤器共享，必须写时复制。
	newGroups := make([]string, 0, len(r.User.GetGroups())+1)
	newGroups = append(newGroups, r.User.GetGroups()...)
	newGroups = append(newGroups, user.AllAuthenticated)

	ret := *r // shallow copy
	ret.User = &user.DefaultInfo{
		Name:   r.User.GetName(),
		UID:    r.User.GetUID(),
		Groups: newGroups,
		Extra:  r.User.GetExtra(),
	}
	return &ret, true, nil
}
