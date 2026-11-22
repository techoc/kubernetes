/*
Copyright 2017 The Kubernetes Authors.

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
	"fmt"
	"net/http"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/kubernetes/scheme"
)

// RequestWaitGroup helps with the accounting of request(s) that are in
// flight: the caller is expected to invoke Add(1) before executing the
// request handler and then invoke Done() when the handler finishes.
// NOTE: implementations must ensure that it is thread-safe
// when invoked from multiple goroutines.
//
// 中文说明：与 sync.WaitGroup 不同，Add 在 Wait 之后返回错误而不是 panic，
// 这样优雅停机时新请求可以被明确拒绝。
type RequestWaitGroup interface {
	// Add adds delta, which may be negative, similar to sync.WaitGroup.
	// If Add with a positive delta happens after Wait, it will return error,
	// which prevent unsafe Add.
	Add(delta int) error

	// Done decrements the WaitGroup counter.
	Done()
}

// WithWaitGroup adds all non long-running requests to wait group, which is used for graceful shutdown.
func WithWaitGroup(handler http.Handler, longRunning apirequest.LongRunningRequestCheck, wg RequestWaitGroup) http.Handler {
	// NOTE: both WithWaitGroup and WithRetryAfter must use the same exact isRequestExemptFunc 'isRequestExemptFromRetryAfter,
	// otherwise SafeWaitGroup might wait indefinitely and will prevent the server from shutting down gracefully.
	return withWaitGroup(handler, longRunning, wg, isRequestExemptFromRetryAfter)
}

// withWaitGroup 是实际实现：长请求（watch、exec 等）不计入等待组，否则停机
// 会被永远阻塞；普通请求在进入 handler 前 Add，退出时 Done。
func withWaitGroup(handler http.Handler, longRunning apirequest.LongRunningRequestCheck, wg RequestWaitGroup, isRequestExemptFn isRequestExemptFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if !ok {
			// if this happens, the handler chain isn't setup correctly because there is no request info
			responsewriters.InternalError(w, req, errors.New("no RequestInfo found in the context"))
			return
		}

		if longRunning(req, requestInfo) {
			handler.ServeHTTP(w, req)
			return
		}

		if err := wg.Add(1); err != nil {
			// shutdown delay duration has elapsed and SafeWaitGroup.Wait has been invoked,
			// this means 'WithRetryAfter' has started sending Retry-After response.
			// we are going to exempt the same set of requests that WithRetryAfter are
			// exempting from being rejected with a Retry-After response.
			//
			// 中文说明：豁免集合必须与 WithRetryAfter 保持一致，否则健康检查等
			// 请求会在停机窗口内被误拒，负载均衡器会把整个实例摘除。
			if isRequestExemptFn(req) {
				handler.ServeHTTP(w, req)
				return
			}

			// When apiserver is shutting down, signal clients to retry
			// There is a good chance the client hit a different server, so a tight retry is good for client responsiveness.
			waitGroupWriteRetryAfterToResponse(w)
			return
		}

		defer wg.Done()
		handler.ServeHTTP(w, req)
	})
}

// waitGroupWriteRetryAfterToResponse 返回 503 + Retry-After: 1，提示客户端
// 立刻重试（大概率会命中另一个尚未停机的实例）。
func waitGroupWriteRetryAfterToResponse(w http.ResponseWriter) {
	w.Header().Add("Retry-After", "1")
	w.Header().Set("Content-Type", runtime.ContentTypeJSON)
	// 防止内容嗅探，错误体必须按声明的 JSON 类型处理。
	w.Header().Set("X-Content-Type-Options", "nosniff")
	statusErr := apierrors.NewServiceUnavailable("apiserver is shutting down").Status()
	w.WriteHeader(int(statusErr.Code))
	fmt.Fprintln(w, runtime.EncodeOrDie(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &statusErr))
}
