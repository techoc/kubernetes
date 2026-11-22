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

// WithPanicRecovery wraps an http Handler to recover and log panics (except in the special case of http.ErrAbortHandler panics, which suppress logging).
//
// 这是 apiserver 的最后一道崩溃隔离：单个请求的 panic 不应拖垮整个进程。
// 对客户端返回 500 而不泄露堆栈；堆栈只进服务端日志，避免信息泄露给不可信调用方。
func WithPanicRecovery(handler http.Handler, resolver request.RequestInfoResolver) http.Handler {
	return withPanicRecovery(handler, func(w http.ResponseWriter, req *http.Request, err interface{}) {
		if err == http.ErrAbortHandler {
			// Honor the http.ErrAbortHandler sentinel panic value
			//
			// If ServeHTTP panics, the server (the caller of ServeHTTP) assumes
			// that the effect of the panic was isolated to the active request.
			// It recovers the panic, logs a stack trace to the server error log,
			// and either closes the network connection or sends an HTTP/2
			// RST_STREAM, depending on the HTTP protocol. To abort a handler so
			// the client sees an interrupted response but the server doesn't log
			// an error, panic with the value ErrAbortHandler.
			//
			// Note that HandleCrash function is actually crashing, after calling the handlers
			//
			// ErrAbortHandler 是"故意中断"的哨兵值（客户端断开、超时取消等），
			// 不是缺陷，因此不打印堆栈，只记指标与一条限速日志。
			if info, err := resolver.NewRequestInfo(req); err != nil {
				metrics.RecordRequestAbort(req, nil)
			} else {
				metrics.RecordRequestAbort(req, info)
			}
			// This call can have different handlers, but the default chain rate limits. Call it after the metrics are updated
			// in case the rate limit delays it.  If you outrun the rate for this one timed out requests, something has gone
			// seriously wrong with your server, but generally having a logging signal for timeouts is useful.
			runtime.HandleErrorWithContext(req.Context(), nil, "Timeout or abort while handling", "method", req.Method, "URI", req.RequestURI, "auditID", audit.GetAuditIDTruncated(req.Context()))
			return
		}
		// 真正的 panic：先给客户端一个固定文案的 500，再记录错误。
		// 文案保持稳定，便于用户检索；细节只出现在服务端日志里。
		http.Error(w, "This request caused apiserver to panic. Look in the logs for details.", http.StatusInternalServerError)
		klog.ErrorS(nil, "apiserver panic'd", "method", req.Method, "URI", req.RequestURI, "auditID", audit.GetAuditIDTruncated(req.Context()))
	})
}

// WithHTTPLogging enables logging of incoming requests.
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

		// Dispatch to the internal handler
		handler.ServeHTTP(w, req)
	})
}
