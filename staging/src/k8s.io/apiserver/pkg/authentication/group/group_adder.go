/*
Copyright 2016 The Kubernetes Authors.

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
	"net/http"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

// GroupAdder 把组附加到已认证的 user.Info 上。
// 典型用途是把 system:masters 等特权组附加到回环客户端身份上。
type GroupAdder struct {
	// Authenticator 被委派来作出认证决定
	Authenticator authenticator.Request
	// Groups 是在认证成功后追加到 user.Info 的额外组
	Groups []string
}

// NewGroupAdder 包装一个请求认证器，并在认证成功时把指定的组附加到返回的 user 上。
// 只包裹认证请求阶段；认证失败时原样返回，不改变错误与 ok 语义。
func NewGroupAdder(auth authenticator.Request, groups []string) authenticator.Request {
	return &GroupAdder{auth, groups}
}

// AuthenticateRequest 先委托给底层认证器，成功后再构造新的 user.Info。
// 这里复制响应结构而非原地修改，避免污染底层认证器可能复用的缓存对象
// （例如 tokenfile authenticator 会返回共享的 *user.DefaultInfo）。
func (g *GroupAdder) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	r, ok, err := g.Authenticator.AuthenticateRequest(req)
	if err != nil || !ok {
		return nil, ok, err
	}

	// 预分配容量并先放原有组，保证附加组追加在末尾，便于断言与日志阅读。
	newGroups := make([]string, 0, len(r.User.GetGroups())+len(g.Groups))
	newGroups = append(newGroups, r.User.GetGroups()...)
	newGroups = append(newGroups, g.Groups...)

	ret := *r // shallow copy
	ret.User = &user.DefaultInfo{
		Name:   r.User.GetName(),
		UID:    r.User.GetUID(),
		Groups: newGroups,
		Extra:  r.User.GetExtra(),
	}
	return &ret, true, nil
}
