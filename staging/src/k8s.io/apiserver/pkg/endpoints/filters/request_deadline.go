/*
Copyright 2020 The Kubernetes Authors.

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
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	// The 'timeout' query parameter in the request URL has an invalid duration specifier
	invalidTimeoutInURL = "invalid timeout specified in the request URL"
)

// WithRequestDeadline determines the timeout duration applicable to the given request and sets a new context
// with the appropriate deadline.
// auditWrapper provides an http.Handler that audits a failed request.
// longRunning returns true if he given request is a long running request.
// requestTimeoutMaximum specifies the default request timeout value.
//
// 长时请求（watch、exec 等）不受 timeout 约束：它们本就设计为长时间挂起，
// 强加截止时间会破坏客户端语义。时间基准优先使用 ReceivedTimestamp，
// 这样排队时间也被计入超时预算，而不是从进入本过滤器才开始计时。
func WithRequestDeadline(handler http.Handler, sink audit.Sink, policy audit.PolicyRuleEvaluator, longRunning request.LongRunningRequestCheck,
	negotiatedSerializer runtime.NegotiatedSerializer, requestTimeoutMaximum time.Duration) http.Handler {
	return withRequestDeadline(handler, sink, policy, longRunning, negotiatedSerializer, requestTimeoutMaximum, clock.RealClock{})
}

// withRequestDeadline 是 WithRequestDeadline 的可注入时钟版本，便于测试。
func withRequestDeadline(handler http.Handler, sink audit.Sink, policy audit.PolicyRuleEvaluator, longRunning request.LongRunningRequestCheck,
	negotiatedSerializer runtime.NegotiatedSerializer, requestTimeoutMaximum time.Duration, clock clock.PassiveClock) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()

		requestInfo, ok := request.RequestInfoFrom(ctx)
		if !ok {
			handleError(w, req, http.StatusInternalServerError, nil, "no RequestInfo found in context, handler chain must be wrong")
			return
		}
		if longRunning(req, requestInfo) {
			handler.ServeHTTP(w, req)
			return
		}

		// 先解析 timeout 参数；非法值必须在进入业务 handler 前以 400 拒绝，
		// 并走审计路径，保证被拒绝的请求同样留下审计记录。
		userSpecifiedTimeout, ok, err := parseTimeout(req)
		if err != nil {
			statusErr := apierrors.NewBadRequest(err.Error())

			klog.Errorf("Error - %s: %#v", err.Error(), req.RequestURI)

			failed := failedErrorHandler(negotiatedSerializer, statusErr)
			failWithAudit := withFailedRequestAudit(failed, statusErr, sink, policy)
			failWithAudit.ServeHTTP(w, req)
			return
		}

		timeout := requestTimeoutMaximum
		if ok {
			// we use the default timeout enforced by the apiserver:
			// - if the user has specified a timeout of 0s, this implies no timeout on the user's part.
			// - if the user has specified a timeout that exceeds the maximum deadline allowed by the apiserver.
			if userSpecifiedTimeout > 0 && userSpecifiedTimeout < requestTimeoutMaximum {
				timeout = userSpecifiedTimeout
			}
		}

		// 以请求到达 apiserver 的时间为起点，而不是本过滤器执行的时间，
		// 使前面过滤器消耗的排队时间计入同一预算。
		started := clock.Now()
		if requestStartedTimestamp, ok := request.ReceivedTimestampFrom(ctx); ok {
			started = requestStartedTimestamp
		}

		ctx, cancel := context.WithDeadline(ctx, started.Add(timeout))
		defer cancel()

		req = req.WithContext(ctx)
		handler.ServeHTTP(w, req)
	})
}

// withFailedRequestAudit decorates a failed http.Handler and is used to audit a failed request.
// statusErr is used to populate the Message property of ResponseStatus.
//
// 没有审计 sink 或策略时直接透传，避免在未启用审计的集群上引入额外开销。
func withFailedRequestAudit(failedHandler http.Handler, statusErr *apierrors.StatusError, sink audit.Sink, policy audit.PolicyRuleEvaluator) http.Handler {
	if sink == nil || policy == nil {
		return failedHandler
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ac, err := evaluatePolicyAndCreateAuditEvent(req, policy, sink)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failed to create audit event: %v", err))
			responsewriters.InternalError(w, req, errors.New("failed to create audit event"))
			return
		}

		if !ac.Enabled() {
			failedHandler.ServeHTTP(w, req)
			return
		}

		respStatus := &metav1.Status{}
		if statusErr != nil {
			respStatus.Message = statusErr.Error()
		}
		ac.SetEventResponseStatus(respStatus)
		ac.SetEventStage(auditinternal.StageResponseStarted)

		// 包一层 response writer，让审计事件能记录真实的响应状态码与体积。
		rw := decorateResponseWriter(req.Context(), w, true)
		failedHandler.ServeHTTP(rw, req)
	})
}

// failedErrorHandler returns an http.Handler that uses the specified StatusError object
// to render an error response to the request.
//
// 按请求的 GroupVersion 协商编码，保证错误响应与客户端期望的 API 版本一致。
func failedErrorHandler(s runtime.NegotiatedSerializer, statusError *apierrors.StatusError) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		requestInfo, found := request.RequestInfoFrom(ctx)
		if !found {
			responsewriters.InternalError(w, req, errors.New("no RequestInfo found in the context"))
			return
		}

		gv := schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}
		responsewriters.ErrorNegotiated(statusError, s, gv, w, req)
	})
}

// parseTimeout parses the given HTTP request URL and extracts the timeout query parameter
// value if specified by the user.
// If a timeout is not specified the function returns false and err is set to nil
// If the value specified is malformed then the function returns false and err is set
//
// 返回值 ok 表示用户显式指定了 timeout；duration 为 0 或负数时由调用方
// 视为“不限时”，仍以 apiserver 的最大值为准。
func parseTimeout(req *http.Request) (time.Duration, bool, error) {
	value := req.URL.Query().Get("timeout")
	if value == "" {
		return 0, false, nil
	}

	timeout, err := time.ParseDuration(value)
	if err != nil {
		return 0, false, fmt.Errorf("%s - %s", invalidTimeoutInURL, err.Error())
	}

	return timeout, true, nil
}

// handleError does the following:
// a) it writes the specified error code, and msg to the ResponseWriter
// object, it does not print the given innerErr into the ResponseWriter object.
// b) additionally, it prints the given msg, and innerErr to the log with other
// request scoped data that helps identify the given request.
//
// 内外有别的错误信息是有意为之：客户端只看到 msg，内部细节（innerErr）
// 只进日志，避免泄露服务端实现细节。
func handleError(w http.ResponseWriter, r *http.Request, code int, innerErr error, msg string) {
	http.Error(w, msg, code)
	klog.ErrorSDepth(1, innerErr, msg, "method", r.Method, "URI", r.RequestURI, "auditID", audit.GetAuditIDTruncated(r.Context()))
}
