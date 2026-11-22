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

package filters

import (
	"fmt"
	"net/http"

	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/request"
)

// WithRequestInfo attaches a RequestInfo to the context.
//
// 该过滤器必须尽早安装：后续的鉴权、准入、审计都依赖上下文中解析出的
// 动词/资源/命名空间信息。解析失败说明路由与解析器配置不一致，属于服务端
// 配置错误，因此返回 500 而不是 4xx，避免把内部问题伪装成客户端错误。
func WithRequestInfo(handler http.Handler, resolver request.RequestInfoResolver) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		info, err := resolver.NewRequestInfo(req)
		if err != nil {
			responsewriters.InternalError(w, req, fmt.Errorf("failed to create RequestInfo: %v", err))
			return
		}

		// WithContext 生成新请求而非就地修改，保证并发安全；下游看到的上下文已携带 RequestInfo。
		req = req.WithContext(request.WithRequestInfo(ctx, info))

		handler.ServeHTTP(w, req)
	})
}
