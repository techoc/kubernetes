/*
Copyright 2025 The Kubernetes Authors.

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

package impersonation

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/filters"
	"k8s.io/apiserver/pkg/endpoints/filters/impersonation/metrics"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/httplog"
	"k8s.io/klog/v2"
)

// WithConstrainedImpersonation 实现 https://kep.k8s.io/5284 描述的 constrained 模拟，
// 同时完整重写了 legacy 模拟以兼容既有行为。
// 大体上，constrained 模拟通过多次授权检查，实现对模拟权限的细粒度表达。
// 例如某个 service account 可能被授权模拟与其关联的 node，但仅限于 list pods 时。
// 更多细节见链接中的 KEP。
//
// 安全边界：所有 impersonation 头都在本包内被消费并从请求头中删除，下游 handler 无法得知
// 发生过 impersonation，从而避免其他组件基于该事实产生不同行为。
func WithConstrainedImpersonation(handler http.Handler, a authorizer.UnconditionalAuthorizer, s runtime.NegotiatedSerializer) http.Handler {
	metrics.RegisterMetrics()

	// 包装授权器以记录每次授权调用的耗时与结果，供 SLO 观测。
	ma := &metricsAuthorizer{
		delegate:                a,
		recordAuthorizationCall: metrics.RecordImpersonationAuthorizationCall,
	}

	recordAttempt := func(ctx context.Context, mode, decision string, duration time.Duration) {
		metrics.RecordImpersonationAttempt(mode, decision, duration)
		request.TrackImpersonationLatency(ctx, duration)
	}

	return &constrainedImpersonationHandler{
		handler: handler,
		tracker: newImpersonationModesTracker(ma),
		s:       s,

		recordAttempt:     recordAttempt,
		metricsAuthorizer: ma,
	}
}

// constrainedImpersonationHandler 在请求进入业务 handler 前完成身份替换。
type constrainedImpersonationHandler struct {
	handler http.Handler
	tracker *impersonationModesTracker
	s       runtime.NegotiatedSerializer

	// 以便单元测试可以覆盖指标记录逻辑
	recordAttempt     func(ctx context.Context, mode, decision string, duration time.Duration)
	metricsAuthorizer *metricsAuthorizer
}

// ServeHTTP 解析 impersonation 头，尝试各授权模式，成功后把模拟用户写入请求上下文。
func (c *constrainedImpersonationHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	wantedUser, err := processImpersonationHeaders(req.Header)
	if err != nil {
		responsewriters.RespondWithError(w, req, err, c.s)
		return
	}
	if wantedUser == nil { // 未尝试模拟，直接交给下一个 handler
		c.handler.ServeHTTP(w, req)
		return
	}
	// 授权属性由上游 filter 预先计算好，这里直接复用，保证与后续真正授权检查看到的一致。
	attributes, err := filters.GetAuthorizerAttributes(ctx)
	if err != nil {
		responsewriters.InternalError(w, req, err)
		return
	}
	requestor := attributes.GetUser()
	if requestor == nil {
		responsewriters.InternalError(w, req, errors.New("no User found in the context"))
		return
	}

	start := time.Now()
	impersonatedUser, err := c.tracker.getImpersonatedUser(ctx, wantedUser, attributes)
	duration := time.Since(start)
	if err != nil {
		c.recordAttempt(ctx, "", "denied", duration)
		klog.V(4).InfoS("Forbidden", "URI", req.RequestURI, "err", err)
		responsewriters.RespondWithError(w, req, err, c.s)
		return
	}
	c.recordAttempt(ctx, modeFromConstraint(impersonatedUser.constraint), "allowed", duration)

	// 只有到这里才替换上下文中的用户；失败的 impersonation 不会污染请求身份。
	req = req.WithContext(request.WithUser(ctx, impersonatedUser.user))
	httplog.LogOf(req, w).Addf("%v is impersonating %v", userString(requestor), userString(impersonatedUser.user))
	audit.LogImpersonatedUser(ctx, impersonatedUser.user, impersonatedUser.constraint)

	c.handler.ServeHTTP(w, req)
}

// processImpersonationHeaders 把给定请求头中的模拟头转换成等价的 user.DefaultInfo。
// 得到的用户是输入头的原始表示，也就是说没有做任何默认值填充或其他修改。
func processImpersonationHeaders(headers http.Header) (*user.DefaultInfo, error) {
	wantedUser := &user.DefaultInfo{}

	wantedUser.Name = headers.Get(authenticationv1.ImpersonateUserHeader)
	hasUser := len(wantedUser.Name) > 0

	wantedUser.UID = headers.Get(authenticationv1.ImpersonateUIDHeader)
	hasUID := len(wantedUser.UID) > 0

	hasGroups := false
	for _, group := range headers[authenticationv1.ImpersonateGroupHeader] {
		hasGroups = true
		wantedUser.Groups = append(wantedUser.Groups, group)
	}

	hasUserExtra := false
	for headerName, values := range headers {
		if !strings.HasPrefix(headerName, authenticationv1.ImpersonateUserExtraHeaderPrefix) {
			continue
		}

		hasUserExtra = true

		if len(values) == 0 {
			// 看起来有点奇怪，但与 legacy 模拟中 buildImpersonationRequests 的行为一致：
			// http1 使用 textproto.Reader#ReadMIMEHeader，values 切片确实可能为空；
			// http2 使用 http.Header#Add，values 切片始终非空
			continue
		}

		// 头名大小写不敏感，统一小写后反转义，与旧实现的键格式保持一致。
		extraKey := unescapeExtraKey(strings.ToLower(headerName[len(authenticationv1.ImpersonateUserExtraHeaderPrefix):]))

		if wantedUser.Extra == nil {
			wantedUser.Extra = map[string][]string{}
		}
		wantedUser.Extra[extraKey] = append(wantedUser.Extra[extraKey], values...)
	}

	// 只给了 UID/组/extra 而没给用户名是无效请求，必须显式报错而不是忽略多余头。
	if !hasUser && (hasUID || hasGroups || hasUserExtra) {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("requested %#v without impersonating a user name", wantedUser))
	}

	if !hasUser {
		return nil, nil
	}

	// 清除请求中所有模拟相关的头，避免下游层得知曾经发生过模拟；
	// 我们不希望本包之外的任何逻辑根据“是否使用过模拟”改变行为
	headers.Del(authenticationv1.ImpersonateUserHeader)
	headers.Del(authenticationv1.ImpersonateUIDHeader)
	headers.Del(authenticationv1.ImpersonateGroupHeader)
	for headerName := range headers {
		if strings.HasPrefix(headerName, authenticationv1.ImpersonateUserExtraHeaderPrefix) {
			headers.Del(headerName)
		}
	}

	return wantedUser, nil
}

// impersonationModesTracker 记录某个请求者用户上次成功的模拟模式。
// 这样我们可以先检查更安全的 constrained 模拟模式，同时保持整体
// 开销不变，因为 legacy 模拟会被永久支持。
type impersonationModesTracker struct {
	modes    []impersonationMode
	idxCache *modeIndexCache
}

// newImpersonationModesTracker 构建按优先级排序的模式列表，并包一层仅在 V(5) 时启用的详细日志授权器。
func newImpersonationModesTracker(a authorizer.UnconditionalAuthorizer) *impersonationModesTracker {
	loggingAuthorizer := authorizer.AuthorizerFunc(func(ctx context.Context, attributes authorizer.Attributes) (authorizer.Decision, string, error) {
		decision, reason, err := a.Authorize(ctx, attributes)
		// 构造一条详细的授权日志
		// 整块逻辑加条件判断，避免做一大堆用不上的字符串拼接
		if klogV := klog.V(5); klogV.Enabled() { // 与 RBAC authorizer 详细日志使用同一日志级别
			u := attributes.GetUser()
			fieldSelector, _ := attributes.GetFieldSelector()
			labelSelector, _ := attributes.GetLabelSelector()
			klogV.InfoSDepth(3, "Impersonation authorization check",
				// 不能直接把 attributes 传给 logger，那样无法记录这些方法调用的真实返回值；
				// 模拟逻辑大量通过这些方法的包装来附加额外行为
				"username", u.GetName(),
				"uid", u.GetUID(),
				"groups", u.GetGroups(),
				"extra", u.GetExtra(),

				"isResourceRequest", attributes.IsResourceRequest(),

				"namespace", attributes.GetNamespace(),
				"verb", attributes.GetVerb(),
				"group", attributes.GetAPIGroup(),
				"version", attributes.GetAPIVersion(),
				"resource", attributes.GetResource(),
				"subresource", attributes.GetSubresource(),
				"name", attributes.GetName(),
				"fieldSelector", fieldSelector,
				"labelSelector", labelSelector,

				"path", attributes.GetPath(),

				"decision", decision,
				"reason", reason,
				"err", err,
			)
		}
		return decision, reason, err
	})
	return &impersonationModesTracker{
		modes:    allImpersonationModes(loggingAuthorizer),
		idxCache: newModeIndexCache(),
	}
}

// getImpersonatedUser 依次尝试各 impersonation 模式，返回第一个成功的模拟用户。
// 失败时返回首个错误（而非最后一个），因为最早的错误通常来自更严格、更有信息量的检查。
func (t *impersonationModesTracker) getImpersonatedUser(ctx context.Context, wantedUser *user.DefaultInfo, attributes authorizer.Attributes) (*impersonatedUserInfo, error) {
	// 所有模式共用同一个缓存键，保证只惰性构造一次
	key := &impersonationCacheKey{wantedUser: wantedUser, attributes: attributes}
	var firstErr error

	// 先尝试上次成功的模式，以降低模拟的均摊成本；
	// 除非某次模拟成功触发短路，否则所有模式都会被尝试
	modeIdx, modeIdxOk := t.idxCache.get(attributes)
	if modeIdxOk {
		impersonatedUser, err := t.modes[modeIdx].check(ctx, key, wantedUser, attributes)
		if err == nil && impersonatedUser != nil {
			return impersonatedUser, nil
		}
		firstErr = err
	}

	for i, mode := range t.modes {
		if modeIdxOk && i == modeIdx {
			continue // 跳过已经尝试过的模式
		}

		impersonatedUser, err := mode.check(ctx, key, wantedUser, attributes)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if impersonatedUser == nil {
			continue
		}
		t.idxCache.set(attributes, i)
		return impersonatedUser, nil
	}

	if firstErr != nil {
		return nil, firstErr
	}

	// 理论上不该发生，但要确保所有模拟模式都失败时按 fail-closed 处理
	return nil, errors.New("all impersonation modes failed")
}

// 目前 constrained 模拟只支持 unconditional 授权器
var _ = authorizer.UnconditionalAuthorizer(&metricsAuthorizer{})

// metricsAuthorizer 给底层授权器加上按模式/结果分类的耗时指标。
type metricsAuthorizer struct {
	delegate                authorizer.UnconditionalAuthorizer
	recordAuthorizationCall func(mode, decision string, duration time.Duration)
}

// Authorize 透传授权结果并记录一次调用指标。
func (m *metricsAuthorizer) Authorize(ctx context.Context, attributes authorizer.Attributes) (authorizer.Decision, string, error) {
	start := time.Now()
	decision, reason, err := m.delegate.Authorize(ctx, attributes)
	duration := time.Since(start)

	m.recordAuthorizationCall(modeFromVerb(attributes.GetVerb()), decisionToLabel(decision), duration)

	return decision, reason, err
}

// modeFromConstraint 把模拟用户携带的约束名映射为指标标签；无约束即旧式 impersonation。
func modeFromConstraint(constraint string) string {
	if len(constraint) == 0 {
		return "legacy"
	}
	return modeFromVerb(constraint)
}

// modeFromVerb 把授权动词映射为稳定的模式名，未知动词归入 "unknown"，
// 避免新增动词时指标基数失控。
func modeFromVerb(verb string) string {
	switch {
	case verb == "impersonate":
		return "legacy"
	case verb == "impersonate:associated-node" || strings.HasPrefix(verb, "impersonate-on:associated-node:"):
		return "associated-node"
	case verb == "impersonate:arbitrary-node" || strings.HasPrefix(verb, "impersonate-on:arbitrary-node:"):
		return "arbitrary-node"
	case verb == "impersonate:serviceaccount" || strings.HasPrefix(verb, "impersonate-on:serviceaccount:"):
		return "serviceaccount"
	case verb == "impersonate:user-info" || strings.HasPrefix(verb, "impersonate-on:user-info:"):
		return "user-info"
	default:
		return "unknown"
	}
}

// decisionToLabel 把三态决策折叠成二值标签，仅用于指标。
func decisionToLabel(decision authorizer.Decision) string {
	if decision == authorizer.DecisionAllow {
		return "allowed"
	}
	return "denied"
}
