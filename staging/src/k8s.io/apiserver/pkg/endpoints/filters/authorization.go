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
	"context"
	"errors"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/request"
)

const (
	// 高级审计中设置的注解键名
	decisionAnnotationKey = "authorization.k8s.io/decision"
	reasonAnnotationKey   = "authorization.k8s.io/reason"

	// 高级审计中设置的注解值
	decisionAllow  = "allow"
	decisionForbid = "forbid"
	reasonError    = "internal error"
)

type recordAuthorizationMetricsFunc func(ctx context.Context, authorized authorizer.Decision, err error, authStart time.Time, authFinish time.Time)

// WithAuthorization 把所有授权通过的请求转交给 handler，否则返回 forbidden 错误。
//
// 这是公开入口，负责注册指标后委托内部实现；测试通过 withAuthorization
// 注入假的指标记录函数，避免依赖全局指标注册表。
func WithAuthorization(hhandler http.Handler, auth authorizer.UnconditionalAuthorizer, s runtime.NegotiatedSerializer) http.Handler {
	RegisterMetrics()
	return withAuthorization(hhandler, auth, s, recordAuthorizationMetrics)
}

// withAuthorization 是鉴权过滤器的实际实现。
// a 为 nil 时直接放行并告警——这是显式的“关闭鉴权”配置，不能静默处理。
func withAuthorization(handler http.Handler, a authorizer.UnconditionalAuthorizer, s runtime.NegotiatedSerializer, metrics recordAuthorizationMetricsFunc) http.Handler {
	if a == nil {
		klog.Warning("Authorization is disabled")
		return handler
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		authorizationStart := time.Now()

		// 属性缺失说明 requestinfo 过滤器没有正确安装，属于服务端装配错误。
		attributes, err := GetAuthorizerAttributes(ctx)
		if err != nil {
			responsewriters.InternalError(w, req, err)
			return
		}
		authorized, reason, err := a.Authorize(ctx, attributes)

		authorizationFinish := time.Now()
		request.TrackAuthorizationLatency(ctx, authorizationFinish.Sub(authorizationStart))
		// 用 defer 记录指标，确保所有返回路径（含 panic 之外的提前返回）都被计入。
		defer func() {
			metrics(ctx, authorized, err, authorizationStart, authorizationFinish)
		}()

		// 先看决策再看错误，是有意为之——允许决策优先，避免把“带错误但已允许”的
		// 结果误判为内部错误而拒绝请求。
		if authorized == authorizer.DecisionAllow {
			audit.AddAuditAnnotations(ctx,
				decisionAnnotationKey, decisionAllow,
				reasonAnnotationKey, reason)
			handler.ServeHTTP(w, req)
			return
		}
		if err != nil {
			// 错误原因可能包含内部细节，审计里只记录固定字符串，不回显错误文本。
			audit.AddAuditAnnotation(ctx, reasonAnnotationKey, reasonError)
			responsewriters.InternalError(w, req, err)
			return
		}

		// 其余情况（Deny 或 NoOpinion）统一按 Forbidden 处理。
		klog.V(4).InfoS("Forbidden", "URI", req.RequestURI, "reason", reason)
		audit.AddAuditAnnotations(ctx,
			decisionAnnotationKey, decisionForbid,
			reasonAnnotationKey, reason)
		responsewriters.Forbidden(attributes, w, req, reason, s)
	})
}

// GetAuthorizerAttributes 从请求 context 构造鉴权属性。
// RequestInfo 由前面的过滤器写入；缺失时返回错误而不是构造空属性，
// 否则可能因“空资源”被策略意外放行。
func GetAuthorizerAttributes(ctx context.Context) (authorizer.Attributes, error) {
	attribs := authorizer.AttributesRecord{}

	user, ok := request.UserFrom(ctx)
	if ok {
		attribs.User = user
	}

	requestInfo, found := request.RequestInfoFrom(ctx)
	if !found {
		return nil, errors.New("no RequestInfo found in the context")
	}

	// 先设置资源请求和非资源请求都通用的属性
	attribs.ResourceRequest = requestInfo.IsResourceRequest
	attribs.Path = requestInfo.Path
	attribs.Verb = requestInfo.Verb

	attribs.APIGroup = requestInfo.APIGroup
	attribs.APIVersion = requestInfo.APIVersion
	attribs.Resource = requestInfo.Resource
	attribs.Subresource = requestInfo.Subresource
	attribs.Namespace = requestInfo.Namespace
	attribs.Name = requestInfo.Name

	// 解析错误不立即返回，而是随属性一起传递，让鉴权器自行决定如何对待
	// （例如拒绝或忽略选择器），保持属性构造不失败。
	if len(requestInfo.FieldSelector) > 0 {
		fieldSelector, err := fields.ParseSelector(requestInfo.FieldSelector)
		if err != nil {
			attribs.FieldSelectorRequirements, attribs.FieldSelectorParsingErr = nil, err
		} else {
			if requirements := fieldSelector.Requirements(); len(requirements) > 0 {
				attribs.FieldSelectorRequirements, attribs.FieldSelectorParsingErr = fieldSelector.Requirements(), nil
			}
		}
	}

	if len(requestInfo.LabelSelector) > 0 {
		labelSelector, err := labels.Parse(requestInfo.LabelSelector)
		if err != nil {
			attribs.LabelSelectorRequirements, attribs.LabelSelectorParsingErr = nil, err
		} else {
			if requirements, _ /*selectable*/ := labelSelector.Requirements(); len(requirements) > 0 {
				attribs.LabelSelectorRequirements, attribs.LabelSelectorParsingErr = requirements, nil
			}
		}
	}

	return &attribs, nil
}
