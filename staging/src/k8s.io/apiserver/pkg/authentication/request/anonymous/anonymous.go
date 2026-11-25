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

// Package anonymous 提供认证链末端的匿名认证器：未被前序认证器识别的请求
// 会被赋予 system:anonymous 身份与 system:unauthenticated 组。
package anonymous

import (
	"net/http"

	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
)

const (
	anonymousUser        = user.Anonymous
	unauthenticatedGroup = user.AllUnauthenticated
)

// Authenticator 把未被其他认证器识别的请求标记为匿名用户。
// 它是认证链的兜底环节，本身不做任何凭据校验。
type Authenticator struct {
	allowedPaths map[string]bool
}

// AuthenticateRequest 总是返回成功，因此会“吞掉”后续认证器；
// 仅当配置了允许路径且当前路径不在其中时，才放行给链上后续环节。
func (a *Authenticator) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	if len(a.allowedPaths) > 0 && !a.allowedPaths[req.URL.Path] {
		return nil, false, nil
	}

	// 透传请求上下文中的 audience，使匿名身份也能通过 audience 校验。
	auds, _ := authenticator.AudiencesFrom(req.Context())
	return &authenticator.Response{
		User: &user.DefaultInfo{
			Name:   anonymousUser,
			Groups: []string{unauthenticatedGroup},
		},
		Audiences: auds,
	}, true, nil
}

// NewAuthenticator 返回一个新的匿名认证器。conditions 为空时所有请求都被认证为匿名；
// conditions 非空时，只有至少匹配一个条件的请求才被认证为匿名。
// 条件目前只支持路径精确匹配；用 map 去重，重复配置不会改变行为。
func NewAuthenticator(conditions []apiserver.AnonymousAuthCondition) authenticator.Request {
	allowedPaths := make(map[string]bool)
	for _, c := range conditions {
		allowedPaths[c.Path] = true
	}

	return &Authenticator{allowedPaths: allowedPaths}
}
