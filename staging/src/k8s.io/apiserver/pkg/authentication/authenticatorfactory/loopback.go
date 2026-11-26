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

package authenticatorfactory

import (
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/request/bearertoken"
	"k8s.io/apiserver/pkg/authentication/token/tokenfile"
	"k8s.io/apiserver/pkg/authentication/user"
)

// NewFromTokens 返回一个 authenticator.Request 或错误。
//
// NewFromTokens 用静态 token 表构造 Bearer token 认证器，主要服务于
// --token-auth-file 这类回环/测试场景。
// WrapAudienceAgnosticToken 表示 token 文件中的身份不区分 audience：
// 它们被视作对任意请求的 audience 都有效，因此集群管理员不应在生产环境依赖它做多租户隔离。
func NewFromTokens(tokens map[string]*user.DefaultInfo, audiences authenticator.Audiences) authenticator.Request {
	return bearertoken.New(authenticator.WrapAudienceAgnosticToken(audiences, tokenfile.New(tokens)))
}
