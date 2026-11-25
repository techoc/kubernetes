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

package filters

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/request"

	"k8s.io/apiserver/pkg/authentication/user"
	tracing "k8s.io/component-base/tracing"
)

// WithTracing 在传入请求被采样时为请求添加追踪。
// 追踪开关由传入的 TracerProvider 决定：传入 Noop provider 时仍保留
// W3C/B3 上下文传播（用于把上游 trace 带给下游），但不产生 span，
// 这样无需在过滤器里额外分支。
func WithTracing(handler http.Handler, tp trace.TracerProvider) http.Handler {
	opts := []otelhttp.Option{
		// 使用与组件共享的传播器集合，保证 API server 与 kubelet/controller 互认。
		otelhttp.WithPropagators(tracing.Propagators()),
		// 特权组（system:masters、monitoring）视为内部端点，不注入外部请求的父 span，
		// 避免把内部调用与外部用户请求混在同一条 trace 上。
		otelhttp.WithPublicEndpointFn(notSystemPrivilegedGroup),
		otelhttp.WithTracerProvider(tp),
		// span 名使用稳定的路由模板而非原始 URL，避免高基数标签。
		otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
			ctx := r.Context()
			info, exist := request.RequestInfoFrom(ctx)
			if !exist || !info.IsResourceRequest {
				return r.Method
			}
			return getSpanNameFromRequestInfo(info, r)
		}),
	}
	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 把 otelhttp 的追踪起始时间调整为与 Prometheus 指标使用的时间一致，
		// 统一起点后，trace 时长与 request latency 指标可以相互对照。
		if startTime, ok := request.ReceivedTimestampFrom(r.Context()); ok {
			r = r.WithContext(otelhttp.ContextWithStartTime(r.Context(), startTime))
		}
		// 给 otelhttp 的 span 添加 http.target 属性，
		// 用于绕过 https://github.com/open-telemetry/opentelemetry-go-contrib/issues/3743
		span := trace.SpanFromContext(r.Context())
		if r.URL != nil {
			span.SetAttributes(attribute.Key("http.target").String(r.URL.RequestURI()))
		}
		// 如果 auditID 可用就添加该属性，便于把 trace 与审计日志条目关联起来。
		// audit-id 已截断，避免在 span 中泄露完整标识或撑爆属性长度。
		span.SetAttributes(attribute.String("audit-id", audit.GetAuditIDTruncated(r.Context())))
		handler.ServeHTTP(w, r)
	})
	// 即使使用 Noop TracerProvider，otelhttp 仍会负责上下文传播。
	// 参见 https://github.com/open-telemetry/opentelemetry-go/tree/main/example/passthrough
	return otelhttp.NewHandler(wrappedHandler, "KubernetesAPI", opts...)
}

// getSpanNameFromRequestInfo 用 RequestInfo 构造低基数 span 名：
// namespace 与 name 被替换为占位符，防止每个对象产生一个 span 名。
func getSpanNameFromRequestInfo(info *request.RequestInfo, r *http.Request) string {
	spanName := "/" + info.APIPrefix
	if info.APIGroup != "" {
		spanName += "/" + info.APIGroup
	}
	spanName += "/" + info.APIVersion
	if info.Namespace != "" {
		spanName += "/namespaces/{:namespace}"
	}
	spanName += "/" + info.Resource
	if info.Name != "" {
		spanName += "/" + "{:name}"
	}
	if info.Subresource != "" {
		spanName += "/" + info.Subresource
	}
	return r.Method + " " + spanName
}

// notSystemPrivilegedGroup 判断请求是否来自普通用户。
// 特权组走内部路径，采样与父 span 行为不同。
func notSystemPrivilegedGroup(req *http.Request) bool {
	if u, ok := request.UserFrom(req.Context()); ok {
		for _, group := range u.GetGroups() {
			if group == user.SystemPrivilegedGroup || group == user.MonitoringGroup {
				return false
			}
		}
	}
	return true
}
