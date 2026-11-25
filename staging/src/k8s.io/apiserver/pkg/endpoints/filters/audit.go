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

// WithAudit 给 http.Handler 加上审计日志装饰，覆盖进入服务端的所有请求。
// 审计级别由请求属性和审计策略共同决定；事件通过 audit sink 写出。
// sink 或审计策略为 nil 时不做任何装饰。
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

		// 装饰响应写入器，用于拦截状态码
		// 长时请求（watch、exec）需要在响应开始时先落一条事件，否则可能长时间没有任何审计记录。
		isLongRunning := false
		if longRunningCheck != nil {
			ri, _ := request.RequestInfoFrom(ctx)
			if longRunningCheck(req, ri) {
				isLongRunning = true
			}
		}
		respWriter := decorateResponseWriter(ctx, w, isLongRunning)

		// 离开本函数时（无论 panic 还是正常返回）发送审计事件。
		// 对长时请求来说，这将是第二个审计事件。
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

			// 如果既没写状态码也没写响应体，就不会发出 StageResponseStarted 事件，这里补一个。
			// 但 Audit-Id 这个 HTTP 头只在 http.ResponseWriter.WriteHeader 被调用时才会发送。
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
					// ResponseStatus 为 nil 说明写入器从未处理过 ResponseStarted 阶段，现在补上。
					ac.ProcessEventStage(ctx, auditinternal.StageResponseStarted)
				}
			}
			writeLatencyToAnnotation(ctx)
			ac.ProcessEventStage(ctx, auditinternal.StageResponseComplete)
		}()
		handler.ServeHTTP(respWriter, req)
	})
}

// evaluatePolicyAndCreateAuditEvent 负责求值该请求适用的审计策略配置，并用请求的
// 审计配置、要写入的 sink 以及请求元数据初始化审计上下文。
// - 发生任何严重错误时返回 error
func evaluatePolicyAndCreateAuditEvent(req *http.Request, policy audit.PolicyRuleEvaluator, sink audit.Sink) (*audit.AuditContext, error) {
	ctx := req.Context()
	ac := audit.AuditContextFrom(ctx)
	if ac == nil {
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
		// 不做审计。
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

// writeLatencyToAnnotation 把 apiserver 各层产生的延迟写入审计对象的注解。
//
// 仅在总耗时超过阈值时记录分层延迟，避免每个事件都携带大量注解、放大审计日志体积。
func writeLatencyToAnnotation(ctx context.Context) {
	ac := audit.AuditContextFrom(ctx)
	// 只有当请求总耗时超过 500ms 时才把分层延迟写入注解，
	// 这与 rest/handlers 中 create、delete、update、
	// get、list、deletecollection 的 trace 阈值保持一致。
	const threshold = 500 * time.Millisecond
	latency := time.Since(ac.GetEventRequestReceivedTimestamp().Time)
	if latency <= threshold {
		return
	}

	// 如果开启了 apiserver 内部分层延迟跟踪，
	// 把这些分层耗时作为注解加到审计事件对象上。
	layerLatencies := request.AuditAnnotationsFromLatencyTrackers(ctx)
	if len(layerLatencies) == 0 {
		// 本请求未启用延迟跟踪
		return
	}

	// 顺便记录该请求的总延迟，方便排查。
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

// auditResponseWriter 拦截 WriteHeader 并写入事件。若设置了 sink，
// 会立即生成一个事件（针对长时请求）。
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
	// 若尚未写过状态码，Go 标准库会在 Write 内部隐式调用 WriteHeader，但对我们不可见
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
	// 在协议切换发生前先伪造一个响应状态
	a.processCode(http.StatusSwitchingProtocols)

	// WrapForHTTP1Or2 返回的外层 ResponseWriter 是否实现 http.Hijacker，
	// 取决于内层对象（a.ResponseWriter）是否实现 http.Hijacker。
	return a.ResponseWriter.(http.Hijacker).Hijack()
}
