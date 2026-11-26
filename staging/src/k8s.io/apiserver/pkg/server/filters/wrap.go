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
	"net/http"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/httplog"
	"k8s.io/klog/v2"
)

// WithPanicRecovery 包装 http Handler，recover 并记录 panic（http.ErrAbortHandler 特例不记录）。
//
// 这是 apiserver 的最后一道崩溃隔离：单个请求的 panic 不应拖垮整个进程。
// 对客户端返回 500 而不泄露堆栈；堆栈只进服务端日志，避免信息泄露给不可信调用方。
func WithPanicRecovery(handler http.Handler, resolver request.RequestInfoResolver) http.Handler {
	return withPanicRecovery(handler, func(w http.ResponseWriter, req *http.Request, err interface{}) {
		if err == http.ErrAbortHandler {
			// 遵循 http.ErrAbortHandler 哨兵 panic 值的语义
			//
			// ServeHTTP panic 时，server（ServeHTTP 的调用方）认为
			// panic 的影响仅局限于当前请求。
			// 它会 recover panic、把堆栈写入 server 错误日志，
			// 并根据 HTTP 协议关闭连接或发送 HTTP/2
			// RST_STREAM。若想中止 handler，让客户端看到中断响应但
			// server 不记录错误，就用 ErrAbortHandler 值 panic。
			//
			// 注意 HandleCrash 调用完 handlers 后会真的让进程崩溃
			//
			// ErrAbortHandler 是"故意中断"的哨兵值（客户端断开、超时取消等），
			// 不是缺陷，因此不打印堆栈，只记指标与一条限速日志。
			if info, err := resolver.NewRequestInfo(req); err != nil {
				metrics.RecordRequestAbort(req, nil)
			} else {
				metrics.RecordRequestAbort(req, info)
			}
			// 此处可配置不同 handler，默认链会限速。应在指标更新之后调用，
			// 以免限速造成延迟。若因超时请求的速率限制而卡住，说明 server 问题严重；
			// 但一般而言，为超时保留日志信号是有用的。
			runtime.HandleErrorWithContext(req.Context(), nil, "Timeout or abort while handling", "method", req.Method, "URI", req.RequestURI, "auditID", audit.GetAuditIDTruncated(req.Context()))
			return
		}
		// 真正的 panic：先给客户端一个固定文案的 500，再记录错误。
		// 文案保持稳定，便于用户检索；细节只出现在服务端日志里。
		http.Error(w, "This request caused apiserver to panic. Look in the logs for details.", http.StatusInternalServerError)
		klog.ErrorS(nil, "apiserver panic'd", "method", req.Method, "URI", req.RequestURI, "auditID", audit.GetAuditIDTruncated(req.Context()))
	})
}

// WithHTTPLogging 记录收到的请求。
func WithHTTPLogging(handler http.Handler) http.Handler {
	return httplog.WithLogging(handler, httplog.DefaultStacktracePred)
}

// withPanicRecovery 把恢复逻辑放在 defer 中，确保无论 handler 从何处 panic 都能被捕获。
// crashHandler 作为参数注入，便于测试或不同调用方定制上报方式。
func withPanicRecovery(handler http.Handler, crashHandler func(http.ResponseWriter, *http.Request, interface{})) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer runtime.HandleCrash(func(err interface{}) {
			crashHandler(w, req, err)
		})

		// 分发给内部 handler
		handler.ServeHTTP(w, req)
	})
}
