/*
Copyright 2021 The Kubernetes Authors.

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

package aggregator

import (
	"encoding/json"
	"fmt"
	"net/http"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/util/responsewriter"
	"k8s.io/kube-openapi/pkg/handler3"
)

// 独立的哨兵错误类型让上层能区分“后端没有这个文档”和“下载失败”：
// 前者是正常状态，应该被当作空结果处理，后者才需要重试并报警。
type NotFoundError struct {
}

func (e *NotFoundError) Error() string {
	return ""
}

// Downloader 是 OpenAPI 下载器，会尝试从 /openapi/v3 与 /openap/v3/<group>/<version> 端点拉取规范。
// 下载器不持有连接或缓存，只把“向后端 handler 发请求并解析响应”这一步封装起来；
// 聚合器可以在 APIService 变化时换掉 handler，而不必重建下载器。
type Downloader struct {
}

// NewDownloader 创建一个新的 OpenAPI 下载器。
func NewDownloader() Downloader {
	return Downloader{}
}

// 后端 handler 通常有认证中间件，直接调用会因缺少用户上下文而被拒绝；
// 这里塞入聚合器专用身份，表示请求来自 kube-aggregator 而不是真实终端用户。
func (s *Downloader) handlerWithUser(handler http.Handler, info user.Info) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		req = req.WithContext(request.WithUser(req.Context(), info))
		handler.ServeHTTP(w, req)
	})
}

// OpenAPIV3Root 从 APIService 下载 OpenAPI v3 根文档。
// 根文档只列 group/version 到文档的链接，不含具体 schema；先取它能避免
// 为一个根本不提供新发现文档的旧后端逐个请求子路径。
func (s *Downloader) OpenAPIV3Root(handler http.Handler) (*handler3.OpenAPIV3Discovery, int, error) {
	handler = s.handlerWithUser(handler, &user.DefaultInfo{Name: aggregatorUser})
	// 直接调用 handler 绕过了真实的网络栈，超时必须由超时包装器兜底，
	// 否则一个卡住的后端会挂住整个聚合循环。
	handler = http.TimeoutHandler(handler, specDownloadTimeout, "request timed out")

	req, err := http.NewRequest("GET", "/openapi/v3", nil)
	if err != nil {
		return nil, 0, err
	}
	// 内存 writer 把响应直接留在进程内：这里本就不是对外提供 HTTP 服务，
	// 而是复用同一套 handler 链路拿到状态码和字节，便于统一处理 404/200。
	writer := responsewriter.NewInMemoryResponseWriter()
	handler.ServeHTTP(writer, req)

	// 404 是受支持的旧后端行为，按“没有新文档”处理并返回 nil 让调用方回退；
	// 其他非 200 状态才是真正的异常，需要错误信息驱动重试。
	switch writer.RespCode() {
	case http.StatusNotFound:
		return nil, writer.RespCode(), nil
	case http.StatusOK:
		groups := handler3.OpenAPIV3Discovery{}
		if err := json.Unmarshal(writer.Data(), &groups); err != nil {
			return nil, writer.RespCode(), err
		}
		return &groups, writer.RespCode(), nil
	}
	return nil, writer.RespCode(), fmt.Errorf("Error, could not get list of group versions for APIService")
}
