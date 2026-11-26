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

// Package union 把多个 authenticator.Request 组合成一条认证链，按顺序
// 尝试，直到某个认证器成功。
package union

import (
	"net/http"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/authentication/authenticator"
)

// unionAuthRequestHandler 使用一串 authenticator.Request 认证请求。
//
// unionAuthRequestHandler 按顺序调用认证器。是否在遇到第一个错误时短路
// 由 FailOnError 决定：默认收集所有错误后聚合返回，便于日志展示全部
// 失败原因；FailOnError 模式则用于错误必须立即上报的场景。
type unionAuthRequestHandler struct {
	//Handlers 是依次委托的请求认证器链
	Handlers []authenticator.Request
	//FailOnError 决定出错是否短路整条链
	FailOnError bool
}

// New 返回一个认证器，用一串 authenticator.Request 校验凭据。
// 依次尝试整条链直到成功；全部失败则返回聚合错误。
//
// New 构造"尽力尝试"的认证链：某个认证器报错不会阻止后续认证器继续尝试，
// 最终把所有错误聚合返回。注意若只有一个认证器则直接返回它，省去一层包装
// （此时不会发生聚合）。
func New(authRequestHandlers ...authenticator.Request) authenticator.Request {
	if len(authRequestHandlers) == 1 {
		return authRequestHandlers[0]
	}
	return &unionAuthRequestHandler{Handlers: authRequestHandlers, FailOnError: false}
}

// NewFailOnError 返回一个认证器，用一串 authenticator.Request 校验凭据。
// 第一个错误即短路整条链。
//
// NewFailOnError 构造"遇错即停"的认证链。适用于错误意味着配置或后端
// 不可用、继续尝试没有意义的认证器组合；第一个错误会立即返回。
func NewFailOnError(authRequestHandlers ...authenticator.Request) authenticator.Request {
	if len(authRequestHandlers) == 1 {
		return authRequestHandlers[0]
	}
	return &unionAuthRequestHandler{Handlers: authRequestHandlers, FailOnError: true}
}

// AuthenticateRequest 用一串 authenticator.Request 认证请求。
//
// AuthenticateRequest 依次尝试各认证器：任一返回 ok=true 即成功并立即
// 返回，后续认证器不再执行。全部失败时返回聚合错误；若无错误则返回
// (nil, false, nil)，表示"无法认证"而非"认证出错"，二者对 HTTP 层
// 的响应（401 与 500）有本质区别。
func (authHandler *unionAuthRequestHandler) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	var errlist []error
	for _, currAuthRequestHandler := range authHandler.Handlers {
		resp, ok, err := currAuthRequestHandler.AuthenticateRequest(req)
		if err != nil {
			// FailOnError 模式下立即短路，避免把互不相关的错误混在一起。
			if authHandler.FailOnError {
				return resp, ok, err
			}
			errlist = append(errlist, err)
			continue
		}

		if ok {
			return resp, ok, err
		}
	}

	return nil, false, utilerrors.NewAggregate(errlist)
}
