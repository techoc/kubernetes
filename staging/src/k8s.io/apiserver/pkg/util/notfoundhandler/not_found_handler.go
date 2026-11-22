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

// Package notfoundhandler 提供委托链末端的兜底 handler，用于区分
// “路径尚未安装”与“路径确实不存在”两种情况。
package notfoundhandler

import (
	"context"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// New returns an HTTP handler that is meant to be executed at the end of the delegation chain.
// It checks if the request have been made before the server has installed all known HTTP paths.
// In that case it returns a 503 response otherwise it returns a 404.
//
// Note that we don't want to add additional checks to the readyz path as it might prevent fixing bricked clusters.
// This specific handler is meant to "protect" requests that arrive before the paths and handlers are fully initialized.
//
// 中文说明：启动早期返回 503+Retry-After 而非 404，避免客户端把“服务尚未就绪”
// 误判为资源不存在并触发删除等破坏性操作；故意不检查 readyz 路径，以免阻塞自救。
func New(serializer runtime.NegotiatedSerializer, isMuxAndDiscoveryCompleteFn func(ctx context.Context) bool) *Handler {
	return &Handler{serializer: serializer, isMuxAndDiscoveryCompleteFn: isMuxAndDiscoveryCompleteFn}
}

// Handler 是委托链末端的兜底 handler。
type Handler struct {
	serializer                  runtime.NegotiatedSerializer
	isMuxAndDiscoveryCompleteFn func(ctx context.Context) bool
}

func (h *Handler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if !h.isMuxAndDiscoveryCompleteFn(req.Context()) {
		errMsg := "the request has been made before all known HTTP paths have been installed, please try again"
		err := apierrors.NewServiceUnavailable(errMsg)
		if err.ErrStatus.Details == nil {
			err.ErrStatus.Details = &metav1.StatusDetails{}
		}
		// 建议客户端 5 秒后重试，给路径安装留出时间。
		err.ErrStatus.Details.RetryAfterSeconds = int32(5)

		// 尽量使用请求本身的组版本编码错误，保持与其他错误响应一致的格式。
		gv := schema.GroupVersion{Group: "unknown", Version: "unknown"}
		requestInfo, ok := apirequest.RequestInfoFrom(req.Context())
		if ok {
			gv.Group = requestInfo.APIGroup
			gv.Version = requestInfo.APIVersion
		}
		responsewriters.ErrorNegotiated(err, h.serializer, gv, rw, req)
		return
	}
	http.NotFound(rw, req)
}
