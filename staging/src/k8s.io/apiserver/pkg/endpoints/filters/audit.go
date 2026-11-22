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
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/endpoints/responsewriter"
)

// WithAudit decorates a http.Handler with audit logging information for all the
// requests coming to the server. Audit level is decided according to requests'
// attributes and audit policy. Logs are emitted to the audit sink to
// process events. If sink or audit policy is nil, no decoration takes place.
//
// 审计必须发生在认证与授权之后：只有拿到完整的用户与请求属性，策略才能正确匹配。
// 关闭审计（sink/policy 为 nil）时直接返回原 handler，零开销。
func WithAudit(handler http.Handler, sink audit.Sink, policy audit.PolicyRuleEvaluator, longRunningCheck request.LongRunningRequestCheck) http.Handler {
	if sink == nil || policy == nil {
		return handler
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ac, err := evaluatePolicyAndCreateAuditEvent(req, policy, sink)
		if err != nil {
			// 审计配置失败不能降级为"静默不审计"，否则会造成审计缺口；直接拒绝请求。
			utilruntime.HandleError(fmt.Errorf("failed to create audit event: %v", err))
			responsewriters.InternalError(w, req, errors.New("failed to create audit event"))
			return
		}

		if !ac.Enabled() {
			handler.ServeHTTP(w, req)
			return
		}

		ctx := req.Context()

		// 第一阶段事件：请求已到达。写入失败即丢弃计数并返回错误，避免无记录的请求继续执行。
		if processed := ac.ProcessEventStage(ctx, auditinternal.StageRequestReceived); !processed {
			audit.ApiserverAuditDroppedCounter.WithContext(ctx).Inc()
			responsewriters.InternalError(w, req, errors.New("failed to store audit event"))
			return
		}

		// intercept the status code
		// 长时请求（watch、exec）需要在响应开始时先落一条事件，否则可能长时间没有任何审计记录。
		isLongRunning := false
		if longRunningCheck != nil {
			ri, _ := request.RequestInfoFrom(ctx)
			if longRunningCheck(req, ri) {
				isLongRunning = true
			}
		}
		respWriter := decorateResponseWriter(ctx, w, isLongRunning)

		// send audit event when we leave this func, either via a panic or cleanly. In the case of long
		// running requests, this will be the second audit event.
		defer func() {
			if r := recover(); r != nil {
				// 先记录 panic 事件再重新抛出，保证 panic 仍由外层恢复逻辑处理，
				// 审计只做旁路观察，不改变崩溃语义。
				defer panic(r)
				ac.SetEventResponseStatus(&metav1.Status{
					Code:    http.StatusInternalServerError,
					Status:  metav1.StatusFailure,
					Reason:  metav1.StatusReasonInternalError,
					Message: fmt.Sprintf("APIServer panic'd: %v", r),
				})
				ac.ProcessEventStage(ctx, auditinternal.StagePanic)
				return
			}

			// if no StageResponseStarted event was sent b/c neither a status code nor a body was sent, fake it here
			// But Audit-Id http header will only be sent when http.ResponseWriter.WriteHeader is called.
			//
			// 连接被提前关闭时可能从未写过状态码，这里补一个成功状态，避免事件缺少响应字段。
			fakedSuccessStatus := &metav1.Status{
				Code:    http.StatusOK,
				Status:  metav1.StatusSuccess,
				Message: "Connection closed early",
			}
			if ac.GetEventResponseStatus() == nil {
				ac.SetEventResponseStatus(fakedSuccessStatus)
				if isLongRunning {
					// A nil ResponseStatus means the writer never processed the ResponseStarted stage, so do that now.
					ac.ProcessEventStage(ctx, auditinternal.StageResponseStarted)
				}
			}
			writeLatencyToAnnotation(ctx)
			ac.ProcessEventStage(ctx, auditinternal.StageResponseComplete)
		}()
		handler.ServeHTTP(respWriter, req)
	})
}

// evaluatePolicyAndCreateAuditEvent is responsible for evaluating the audit
// policy configuration applicable to the request and initializing the audit
// context with the audit config for the request, the sink to write to, and the request metadata.
// - error if anything bad happened
func evaluatePolicyAndCreateAuditEvent(req *http.Request, policy audit.PolicyRuleEvaluator, sink audit.Sink) (*audit.AuditContext, error) {
	ctx := req.Context()
	ac := audit.AuditContextFrom(ctx)
	if ac == nil {
		// Auditing not configured.
		// 没有审计上下文说明本请求路径未启用审计（例如探活端点），直接放行。
		return nil, nil
	}

	// 策略匹配依赖授权属性，属性缺失会导致规则匹配到错误的级别，因此视为硬错误。
	attribs, err := GetAuthorizerAttributes(ctx)
	if err != nil {
		return ac, fmt.Errorf("failed to GetAuthorizerAttributes: %v", err)
	}

	rac := policy.EvaluatePolicyRule(attribs)
	audit.ObservePolicyLevel(ctx, rac.Level)
	err = ac.Init(rac, sink)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize audit context: %w", err)
	}
	if rac.Level == auditinternal.LevelNone {
		// Don't audit.
		return ac, nil
	}

	// 接收时间戳在更早的过滤器里已写入上下文；缺失时回退到当前时间，保证事件仍可用。
	requestReceivedTimestamp, ok := request.ReceivedTimestampFrom(ctx)
	if !ok {
		requestReceivedTimestamp = time.Now()
	}
	audit.LogRequestMetadata(ctx, req, requestReceivedTimestamp, attribs)

	return ac, nil
}

// writeLatencyToAnnotation writes the latency incurred in different
// layers of the apiserver to the annotations of the audit object.
//
// 仅在总耗时超过阈值时记录分层延迟，避免每个事件都携带大量注解、放大审计日志体积。
func writeLatencyToAnnotation(ctx context.Context) {
	ac := audit.AuditContextFrom(ctx)
	// we will track latency in annotation only when the total latency
	// of the given request exceeds 500ms, this is in keeping with the
	// traces in rest/handlers for create, delete, update,
	// get, list, and deletecollection.
	const threshold = 500 * time.Millisecond
	latency := time.Since(ac.GetEventRequestReceivedTimestamp().Time)
	if latency <= threshold {
		return
	}

	// if we are tracking latency incurred inside different layers within
	// the apiserver, add these as annotation to the audit event object.
	layerLatencies := request.AuditAnnotationsFromLatencyTrackers(ctx)
	if len(layerLatencies) == 0 {
		// latency tracking is not enabled for this request
		return
	}

	// record the total latency for this request, for convenience.
	layerLatencies["apiserver.latency.k8s.io/total"] = latency.String()
	audit.AddAuditAnnotationsMap(ctx, layerLatencies)
}

// decorateResponseWriter 包装响应写入器以捕获状态码；processResponseStartedStage
// 仅在长时请求时为 true，使 ResponseStarted 事件尽早落盘。
func decorateResponseWriter(ctx context.Context, responseWriter http.ResponseWriter, processResponseStartedStage bool) http.ResponseWriter {
	delegate := &auditResponseWriter{
		ctx:            ctx,
		ResponseWriter: responseWriter,

		processResponseStartedStage: processResponseStartedStage,
	}

	return responsewriter.WrapForHTTP1Or2(delegate)
}

var _ http.ResponseWriter = &auditResponseWriter{}
var _ responsewriter.UserProvidedDecorator = &auditResponseWriter{}

// auditResponseWriter intercepts WriteHeader, sets it in the event. If the sink is set, it will
// create immediately an event (for long running requests).
//
// once 保证状态码与 ResponseStarted 事件只处理一次：Go 的 ResponseWriter 可能被多次
// 调用 WriteHeader，但审计事件里只应有一个响应状态。
type auditResponseWriter struct {
	http.ResponseWriter
	ctx  context.Context
	once sync.Once

	processResponseStartedStage bool
}

// Unwrap 暴露被包装的写入器，供 responsewriter 的装饰器链识别与替换。
func (a *auditResponseWriter) Unwrap() http.ResponseWriter {
	return a.ResponseWriter
}

// processCode 首次调用时记录状态码并按需推进 ResponseStarted 阶段。
func (a *auditResponseWriter) processCode(code int) {
	a.once.Do(func() {
		ac := audit.AuditContextFrom(a.ctx)
		ac.SetEventResponseStatusCode(int32(code))
		if a.processResponseStartedStage {
			ac.ProcessEventStage(a.ctx, auditinternal.StageResponseStarted)
		}
	})
}

// Write 在首次写入时补记 200：Go 会在内部隐式调用 WriteHeader，若不主动记录，
// 审计事件将缺失响应状态。
func (a *auditResponseWriter) Write(bs []byte) (int, error) {
	// the Go library calls WriteHeader internally if no code was written yet. But this will go unnoticed for us
	a.processCode(http.StatusOK)
	return a.ResponseWriter.Write(bs)
}

// WriteHeader 记录真实状态码后转发。
func (a *auditResponseWriter) WriteHeader(code int) {
	a.processCode(code)
	a.ResponseWriter.WriteHeader(code)
}

// Hijack 在协议切换（如 SPDY/WebSocket）前伪造 101 状态，
// 否则审计事件会缺少这次升级的响应码。
func (a *auditResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	// fake a response status before protocol switch happens
	a.processCode(http.StatusSwitchingProtocols)

	// the outer ResponseWriter object returned by WrapForHTTP1Or2 implements
	// http.Hijacker if the inner object (a.ResponseWriter) implements http.Hijacker.
	return a.ResponseWriter.(http.Hijacker).Hijack()
}
