/*
Copyright 2023 The Kubernetes Authors.

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
	"errors"
	"net/http"

	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"
)

// WithWatchTerminationDuringShutdown 在优雅关闭期间终止 watch 请求。
//
// 为什么需要：watch 是长连接，若不主动打断，关闭流程会一直等待它们结束。
// 该过滤器把 shutdown 信号注入请求 context，并跟踪在途 watch，
// 使 apiserver 能及时响应 SIGTERM 而不丢请求。
func WithWatchTerminationDuringShutdown(handler http.Handler, termination apirequest.ServerShutdownSignal, wg RequestWaitGroup) http.Handler {
	// 任一依赖缺失时退化为不包装，并记录告警，避免静默地失去关闭能力。
	if termination == nil || wg == nil {
		klog.Warningf("watch termination during shutdown not attached to the handler chain")
		return handler
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if !ok {
			// 出现这种情况说明 handler 链装配有误，缺少 request info。
			responsewriters.InternalError(w, req, errors.New("no RequestInfo found in the context"))
			return
		}
		// 只对 watch 动词做特殊处理，普通请求直接放行。
		if !watchVerbs.Has(requestInfo.Verb) {
			handler.ServeHTTP(w, req)
			return
		}

		if err := wg.Add(1); err != nil {
			// apiserver 关闭时通知客户端重试。
			// 客户端很可能访问到了另一台服务器，因此快速重试有利于响应性。
			//
			// 返回 Retry-After 让客户端快速重连到其他实例，而不是等到 TCP 超时。
			waitGroupWriteRetryAfterToResponse(w)
			return
		}

		// 把 ServerShutdownSignal 附加到 watch 请求上，
		// 让 watch handler 循环在服务端发出关闭信号时立即返回。
		//
		ctx = apirequest.WithServerShutdownSignal(req.Context(), termination)
		req = req.WithContext(ctx)

		// 确保无论 handler 如何返回都释放等待计数。
		defer wg.Done()
		handler.ServeHTTP(w, req)
	})
}
