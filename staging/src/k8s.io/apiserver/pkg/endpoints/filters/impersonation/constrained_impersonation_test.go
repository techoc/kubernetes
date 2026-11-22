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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit/policy"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/filters"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/transport"
)

// constrainedImpersonationTest 同时承担测试夹具与假审计后端/假授权器的职责：
// 它记录授权检查、审计事件、指标与缓存状态，供断言使用。
// 由于部分用例并行执行，可变的集中状态由 lock 保护。
type constrainedImpersonationTest struct {
	t testing.TB

	constrainedImpersonationHandler *constrainedImpersonationHandler
	authzChecks                     []authzCheck
	echoCalled                      bool
	auditEvents                     []*auditinternal.Event

	attemptMode          string
	attemptDecision      string
	authorizationMetrics map[string]int // "mode/decision" -> count

	// only used for parallelHandler
	lock sync.Mutex
}

// authzCheck 记录一次授权调用及其期望/实际决策。
type authzCheck struct {
	attributes authorizer.AttributesRecord
	decision   authorizer.Decision
}

// ProcessEvents 作为审计后端接收事件；必须深拷贝，因为事件对象会被原地复用。
func (c *constrainedImpersonationTest) ProcessEvents(evs ...*auditinternal.Event) bool {
	for _, e := range evs {
		c.auditEvents = append(c.auditEvents, e.DeepCopy()) // event is mutated in place so capture its current state
	}
	return true
}

// Authorize 是一个白名单式假授权器：按 (用户, verb, resource, subresource, name)
// 精确匹配允许，其余一律 NoOpinion（等价于拒绝）。
// 延迟记录到 authzChecks，确保记录的是最终决策（包括 panic 前的路径）。
func (c *constrainedImpersonationTest) Authorize(ctx context.Context, a authorizer.Attributes) (decision authorizer.Decision, reason string, err error) {
	u := a.GetUser()

	defer func() {
		c.lock.Lock()
		c.authzChecks = append(c.authzChecks, authzCheck{
			attributes: comparableAttributes(a),
			decision:   decision,
		})
		c.lock.Unlock()
	}()

	if u.GetName() == "sa-impersonater" && a.GetVerb() == "impersonate:serviceaccount" && a.GetResource() == "serviceaccounts" {
		return authorizer.DecisionAllow, "", nil
	}

	if u.GetName() == "system:serviceaccount:default:node" && a.GetVerb() == "impersonate:arbitrary-node" && a.GetResource() == "nodes" {
		return authorizer.DecisionAllow, "", nil
	}

	if u.GetName() == "node-impersonater" && a.GetVerb() == "impersonate:arbitrary-node" && a.GetResource() == "nodes" {
		return authorizer.DecisionAllow, "", nil
	}

	if len(u.GetGroups()) > 0 && u.GetGroups()[0] == "associate-node-impersonater" && a.GetVerb() == "impersonate:associated-node" && a.GetResource() == "nodes" {
		return authorizer.DecisionAllow, "", nil
	}

	if u.GetName() == "user-impersonater" && a.GetVerb() == "impersonate:user-info" && a.GetResource() == "users" {
		return authorizer.DecisionAllow, "", nil
	}

	if u.GetName() == "user-impersonater" && a.GetVerb() == "impersonate:user-info" && a.GetResource() == "uids" {
		return authorizer.DecisionAllow, "", nil
	}

	if len(u.GetGroups()) > 0 && u.GetGroups()[0] == "group-impersonater" && a.GetVerb() == "impersonate:user-info" && a.GetResource() == "groups" {
		return authorizer.DecisionAllow, "", nil
	}

	if len(u.GetGroups()) > 0 && u.GetGroups()[0] == "extra-setter-scopes" && a.GetVerb() == "impersonate:user-info" && a.GetResource() == "userextras" && a.GetSubresource() == "pandas.io/scopes" {
		return authorizer.DecisionAllow, "", nil
	}

	if u.GetName() == "legacy-impersonater" && a.GetVerb() == "impersonate" {
		return authorizer.DecisionAllow, "", nil
	}

	// 受约束的 “代理执行”（impersonate-on）只放行读操作，防止借道变更资源。
	if u.GetName() != "legacy-impersonator" &&
		strings.HasPrefix(a.GetVerb(), "impersonate-on:") &&
		(strings.HasSuffix(a.GetVerb(), "list") || strings.HasSuffix(a.GetVerb(), "get")) {
		return authorizer.DecisionAllow, "", nil
	}

	// many-groups-impersonater: can impersonate users and any groups via wildcard
	if u.GetName() == "many-groups-impersonater" && a.GetVerb() == "impersonate:user-info" && a.GetResource() == "users" {
		return authorizer.DecisionAllow, "", nil
	}
	if u.GetName() == "many-groups-impersonater" && a.GetVerb() == "impersonate:user-info" && a.GetResource() == "groups" && a.GetName() == "*" {
		return authorizer.DecisionAllow, "", nil
	}

	// many-extras-impersonater: can impersonate users and any extras via wildcard
	if u.GetName() == "many-extras-impersonater" && a.GetVerb() == "impersonate:user-info" && a.GetResource() == "users" {
		return authorizer.DecisionAllow, "", nil
	}
	if u.GetName() == "many-extras-impersonater" && a.GetVerb() == "impersonate:user-info" && a.GetResource() == "userextras" && a.GetSubresource() == "*" && a.GetName() == "*" {
		return authorizer.DecisionAllow, "", nil
	}

	// mixed-impersonater: can impersonate users, specific groups, and specific extras (but NOT via wildcards)
	if u.GetName() == "mixed-impersonater" && a.GetVerb() == "impersonate:user-info" && a.GetResource() == "users" {
		return authorizer.DecisionAllow, "", nil
	}
	// Allow specific group names, but NOT wildcard
	if u.GetName() == "mixed-impersonater" && a.GetVerb() == "impersonate:user-info" && a.GetResource() == "groups" && a.GetName() != "*" {
		return authorizer.DecisionAllow, "", nil
	}
	// Allow specific extras for tags.io/env, but NOT wildcard
	if u.GetName() == "mixed-impersonater" && a.GetVerb() == "impersonate:user-info" && a.GetResource() == "userextras" &&
		a.GetSubresource() == "tags.io/env" && a.GetName() != "*" {
		return authorizer.DecisionAllow, "", nil
	}

	return authorizer.DecisionNoOpinion, "deny by default", nil
}

// echoUserInfoHandler 返回请求上下文中的用户（JSON 编码），并在代理成功后
// 断言所有 impersonate-* 请求头都已被剥离——泄漏这些头可能被后端误用。
func (c *constrainedImpersonationTest) echoUserInfoHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		c.lock.Lock()
		c.echoCalled = true
		c.lock.Unlock()

		u, ok := request.UserFrom(req.Context())
		if !ok {
			c.t.Fatal("user not found in request")
		}

		_ = json.NewEncoder(w).Encode(comparableUser(u))

		if _, ok := req.Header[authenticationv1.ImpersonateUserHeader]; ok {
			c.t.Fatal("user header still present")
		}
		if _, ok := req.Header[authenticationv1.ImpersonateUIDHeader]; ok {
			c.t.Fatal("uid header still present")
		}
		if _, ok := req.Header[authenticationv1.ImpersonateGroupHeader]; ok {
			c.t.Fatal("group header still present")
		}
		for key := range req.Header {
			if strings.HasPrefix(key, authenticationv1.ImpersonateUserExtraHeaderPrefix) {
				c.t.Fatalf("extra header still present: %v", key)
			}
		}
	}
}

// authenticationHandler 通过测试专用请求头注入认证用户，替代真实认证器。
func (c *constrainedImpersonationTest) authenticationHandler(handler http.Handler) http.Handler {
	return filters.WithAuthentication(handler, authenticator.RequestFunc(func(req *http.Request) (*authenticator.Response, bool, error) {
		userData := req.Header.Get(testUserHeader)
		if len(userData) == 0 {
			c.t.Fatal("missing user header")
		}
		var u user.DefaultInfo
		if err := json.Unmarshal([]byte(userData), &u); err != nil {
			c.t.Fatal(err)
		}
		return &authenticator.Response{User: &u}, true, nil
	}), nil, nil, nil)
}

// requestInfoHandler 通过测试专用请求头注入 RequestInfo，让测试能精确控制
// 资源、动词与命名空间，而无需构造真实 URL。
func (c *constrainedImpersonationTest) requestInfoHandler(handler http.Handler) http.Handler {
	return filters.WithRequestInfo(handler, requestInfoFunc(func(req *http.Request) (*request.RequestInfo, error) {
		requestInfoData := req.Header.Get(testRequestInfoHeader)
		if len(requestInfoData) == 0 {
			c.t.Fatal("missing request info header")
		}
		var r request.RequestInfo
		if err := json.Unmarshal([]byte(requestInfoData), &r); err != nil {
			c.t.Fatal(err)
		}
		return &r, nil
	}))
}

// requestInfoFunc 把函数适配为 request.RequestInfoFactory。
type requestInfoFunc func(*http.Request) (*request.RequestInfo, error)

// NewRequestInfo 调用被包装的函数。
func (f requestInfoFunc) NewRequestInfo(req *http.Request) (*request.RequestInfo, error) {
	return f(req)
}

const (
	testUserHeader        = "insecure-test-user-json"
	testRequestInfoHeader = "insecure-test-request-info-json"
)

// handler 组装与 DefaultBuildHandlerChain 相同顺序的处理链
// （Audit → Authentication → LatencyTrackers → RequestInfo），
// 并把指标回调替换为测试可观测的计数器。
func (c *constrainedImpersonationTest) handler() http.Handler {
	s := runtime.NewScheme()
	metav1.AddToGroupVersion(s, metav1.SchemeGroupVersion)
	addImpersonation := WithConstrainedImpersonation(c.echoUserInfoHandler(), c, serializer.NewCodecFactory(s))
	c.constrainedImpersonationHandler = addImpersonation.(*constrainedImpersonationHandler)

	recordAttempt := c.constrainedImpersonationHandler.recordAttempt
	c.constrainedImpersonationHandler.recordAttempt = func(ctx context.Context, mode, decision string, duration time.Duration) {
		c.attemptMode = mode
		c.attemptDecision = decision
		recordAttempt(ctx, mode, decision, duration)
	}
	c.constrainedImpersonationHandler.metricsAuthorizer.recordAuthorizationCall = func(mode, decision string, _ time.Duration) {
		if c.authorizationMetrics == nil {
			c.authorizationMetrics = map[string]int{}
		}
		c.authorizationMetrics[mode+"/"+decision]++
	}

	fakeRuleEvaluator := policy.NewFakePolicyRuleEvaluator(auditinternal.LevelRequestResponse, nil)

	// follow the same handler chain order as DefaultBuildHandlerChain
	addAudit := filters.WithAudit(addImpersonation, c, fakeRuleEvaluator, nil)
	addAuthentication := c.authenticationHandler(addAudit)
	addLatencyTrackers := filters.WithLatencyTrackers(addAuthentication)
	addRequestInfo := c.requestInfoHandler(addLatencyTrackers)
	// set received timestamp >500ms in the past so audit annotations are emitted
	// 延迟注解只在耗时超过阈值时才写入；回拨接收时间以稳定触发该路径。
	addReceivedTimestamp := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		req = req.WithContext(request.WithReceivedTimestamp(req.Context(), time.Now().Add(-time.Hour)))
		addRequestInfo.ServeHTTP(w, req)
	})
	addAuditInit := filters.WithAuditInit(addReceivedTimestamp)
	return addAuditInit
}

// parallelHandler 是 handler 的精简版：去掉审计与延迟跟踪，
// 只保留认证/请求信息/代理，便于并发测试减少噪声。
func (c *constrainedImpersonationTest) parallelHandler() http.Handler {
	s := runtime.NewScheme()
	metav1.AddToGroupVersion(s, metav1.SchemeGroupVersion)
	addImpersonation := WithConstrainedImpersonation(c.echoUserInfoHandler(), c, serializer.NewCodecFactory(s))
	c.constrainedImpersonationHandler = addImpersonation.(*constrainedImpersonationHandler)

	c.constrainedImpersonationHandler.recordAttempt = func(ctx context.Context, mode, decision string, _ time.Duration) {}
	c.constrainedImpersonationHandler.metricsAuthorizer.recordAuthorizationCall = func(mode, decision string, _ time.Duration) {}

	addAuthentication := c.authenticationHandler(addImpersonation)
	addRequestInfo := c.requestInfoHandler(addAuthentication)
	return addRequestInfo
}

// testRoundTripper 把测试请求的用户与 RequestInfo 编码进请求头，
// 再交给底层 transport 发送，模拟上游已将身份解析完成。
type testRoundTripper struct {
	user        *user.DefaultInfo
	requestInfo *request.RequestInfo
	delegate    http.RoundTripper
}

// RoundTrip 注入测试头后委托发送。
func (t *testRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	userData, err := json.Marshal(t.user)
	if err != nil {
		return nil, err
	}
	requestInfoData, err := json.Marshal(t.requestInfo)
	if err != nil {
		return nil, err
	}
	r.Header.Set(testUserHeader, string(userData))
	r.Header.Set(testRequestInfoHeader, string(requestInfoData))
	return t.delegate.RoundTrip(r)
}

// assertAttributes 对比期望与实际授权检查序列（含顺序与决策）。
func (c *constrainedImpersonationTest) assertAttributes(r testRequest) {
	authzChecks := c.authzChecks
	c.authzChecks = nil

	require.Len(c.t, authzChecks, len(r.expectedAuthzChecks))

	// normally all authorization checks are done against the requestor
	// but in some cases such as associated-node, we check against a slightly different user
	expectedAttributesUser := r.expectedAttributesUser
	if expectedAttributesUser == nil {
		expectedAttributesUser = r.requestor
	}

	for i := range len(r.expectedAuthzChecks) {
		expectedAttributes := withUser(r.expectedAuthzChecks[i].attributes, expectedAttributesUser)
		require.Equal(c.t, expectedAttributes, authzChecks[i].attributes, "authz check %d: attributes mismatch", i)
		require.Equal(c.t, r.expectedAuthzChecks[i].decision, authzChecks[i].decision, "authz check %d: decision mismatch", i)
	}
}

// comparableAttributes 把任意 Attributes 规整为可直接 DeepEqual 的 AttributesRecord。
func comparableAttributes(attributes authorizer.Attributes) authorizer.AttributesRecord {
	fs, errFS := attributes.GetFieldSelector()
	ls, errLS := attributes.GetLabelSelector()
	return authorizer.AttributesRecord{
		User:                      comparableUser(attributes.GetUser()),
		Verb:                      attributes.GetVerb(),
		Namespace:                 attributes.GetNamespace(),
		APIGroup:                  attributes.GetAPIGroup(),
		APIVersion:                attributes.GetAPIVersion(),
		Resource:                  attributes.GetResource(),
		Subresource:               attributes.GetSubresource(),
		Name:                      attributes.GetName(),
		ResourceRequest:           attributes.IsResourceRequest(),
		Path:                      attributes.GetPath(),
		FieldSelectorRequirements: fs,
		FieldSelectorParsingErr:   errFS,
		LabelSelectorRequirements: ls,
		LabelSelectorParsingErr:   errLS,
	}
}

// comparableAuditUser 把审计 UserInfo 转换为 DefaultInfo，便于统一比较。
func comparableAuditUser(u *authenticationv1.UserInfo) *user.DefaultInfo {
	if u == nil {
		return nil
	}
	var extra map[string][]string
	if len(u.Extra) > 0 {
		extra = make(map[string][]string, len(u.Extra))
		for k, v := range u.Extra {
			extra[k] = v
		}
	}
	return &user.DefaultInfo{
		Name:   u.Username,
		UID:    u.UID,
		Groups: u.Groups,
		Extra:  extra,
	}
}

// comparableUser 提取 user.Info 的关键字段。
func comparableUser(u user.Info) *user.DefaultInfo {
	return &user.DefaultInfo{
		Name:   u.GetName(),
		UID:    u.GetUID(),
		Groups: u.GetGroups(),
		Extra:  u.GetExtra(),
	}
}

// assertEchoCalled 断言业务 handler 是否被执行，并重置标记。
func (c *constrainedImpersonationTest) assertEchoCalled(expectedCalled bool) {
	called := c.echoCalled
	c.echoCalled = false
	require.Equal(c.t, expectedCalled, called)
}

// assertMetrics 断言本次请求产生的 attempt 与授权指标，并重置计数。
func (c *constrainedImpersonationTest) assertMetrics(r testRequest) {
	c.t.Helper()

	attemptMode := c.attemptMode
	attemptDecision := c.attemptDecision
	authorizationMetrics := c.authorizationMetrics
	c.attemptMode = ""
	c.attemptDecision = ""
	c.authorizationMetrics = nil

	require.Equal(c.t, r.expectedAttemptMode, attemptMode, "unexpected attempt mode")
	require.Equal(c.t, r.expectedAttemptDecision, attemptDecision, "unexpected attempt decision")
	require.Equal(c.t, r.expectedAuthorizationMetrics, authorizationMetrics, "unexpected authorization metrics")
}

// assertAuditEvents 断言审计事件包含请求接收与响应完成两条，
// 延迟注解只在响应完成事件的耗时超阈值时出现，
// 且成功代理时 AuthenticationMetadata 记录所用约束模式。
func (c *constrainedImpersonationTest) assertAuditEvents(r testRequest) {
	c.t.Helper()

	events := c.auditEvents
	c.auditEvents = nil

	require.Len(c.t, events, 2)

	requestReceived := events[0]
	responseComplete := events[1]

	require.Empty(c.t, requestReceived.Annotations["apiserver.latency.k8s.io/impersonation"])
	require.Regexp(c.t, "^[0-9.]+[µnm]s$", responseComplete.Annotations["apiserver.latency.k8s.io/impersonation"])

	require.Nil(c.t, requestReceived.ImpersonatedUser)
	require.Equal(c.t, r.expectedImpersonatedUser, comparableAuditUser(responseComplete.ImpersonatedUser))

	require.Nil(c.t, requestReceived.ResponseStatus)
	require.Equal(c.t, r.expectedMessage, responseComplete.ResponseStatus.Message)

	require.Nil(c.t, requestReceived.AuthenticationMetadata)
	var expectedAuthenticationMetadata *auditinternal.AuthenticationMetadata
	// 只有受约束代理成功时才报告约束模式；legacy 走原有审计语义。
	if r.expectedCode == http.StatusOK && r.expectedAttemptMode != "legacy" {
		expectedAuthenticationMetadata = &auditinternal.AuthenticationMetadata{
			ImpersonationConstraint: "impersonate:" + r.expectedAttemptMode,
		}
	}
	require.Equal(c.t, expectedAuthenticationMetadata, responseComplete.AuthenticationMetadata)
}

// assertCache 断言两级缓存（请求者→模式索引、模式→outer/inner）的内容与预期一致；
// expectedCache 为 nil 表示所有缓存必须为空。
func (c *constrainedImpersonationTest) assertCache(r testRequest) {
	rr := require.New(c.t)

	tracker := c.constrainedImpersonationHandler.tracker
	idxCacheInternals := tracker.idxCache.cache

	if r.expectedCache == nil {
		rr.Zero(idxCacheInternals.Len())
		for _, mode := range tracker.modes {
			outer, inner := mode.cachesForTests()
			rr.Zero(outer.cache.Len())
			rr.Zero(inner.cache.Len())
		}
		return
	}

	rr.Equal(len(r.expectedCache.modeIdx), idxCacheInternals.Len())
	for username, expectedVerb := range r.expectedCache.modeIdx {
		modeIdx, ok := idxCacheInternals.Get(usernameHash(username))
		rr.True(ok)
		actualVerb := tracker.modes[modeIdx.(int)].verbForTests()
		rr.Equal(expectedVerb, actualVerb)
	}

	for _, mode := range tracker.modes {
		verb := mode.verbForTests()
		outer, inner := mode.cachesForTests()
		expectedMode, ok := r.expectedCache.modes[verb]
		if !ok {
			rr.Zero(outer.cache.Len())
			rr.Zero(inner.cache.Len())
			continue
		}
		rr.Equal(len(expectedMode.outer), outer.cache.Len())
		for expectedKey, expectedUser := range expectedMode.outer {
			c.checkCacheEntry(outer, expectedKey.wantedUser, expectedKey.attributes, expectedKey.rawKey, expectedUser, verb)
		}

		rr.Equal(len(expectedMode.inner), inner.cache.Len())
		for expectedKey, expectedUser := range expectedMode.inner {
			c.checkCacheEntry(inner, expectedKey.wantedUser, authorizer.AttributesRecord{User: expectedKey.requestor}, expectedKey.rawKey, expectedUser, verb)
		}
	}
}

// usernameHash 返回 FNV-128a 十六进制摘要，与生产代码的模式索引键保持一致。
func usernameHash(username string) string {
	hash := fnvSum128a([]byte(username))
	return fmt.Sprintf("%x", hash)
}

// fnvSum128a 计算 FNV-128a 摘要。
func fnvSum128a(data []byte) []byte {
	h := fnv.New128a()
	h.Write(data)
	var sum [16]byte
	return h.Sum(sum[:0])
}

// checkCacheEntry 用 buildKey 重新构造 key 并验证缓存中的用户与约束模式，
// 同时校验 key 的“rawKey 哈希/请求者名”后缀格式。
func (c *constrainedImpersonationTest) checkCacheEntry(cache *impersonationCache, wantedUser *user.DefaultInfo, attributes authorizer.Attributes, rawKey string, expectedUser *user.DefaultInfo, expectedVerb string) {
	rr := require.New(c.t)
	keyString, err := buildKey(wantedUser, attributes)
	rr.NoError(err)
	val, ok := cache.cache.Get(keyString)
	rr.True(ok)
	impersonatedUser := val.(*impersonatedUserInfo)
	rr.Equal(expectedVerb, impersonatedUser.constraint)
	rr.Equal(expectedUser, impersonatedUser.user)
	rr.Equal(keyHash(rawKey)+"/"+attributes.GetUser().GetName(), keyString, "hash of %q does not match", rawKey)
}

// keyHash 返回 SHA-256 十六进制摘要；缓存键只保存原始键的哈希以限制内存占用。
func keyHash(rawKey string) string {
	hash := sha256.Sum256([]byte(rawKey))
	return fmt.Sprintf("%x", hash[:])
}

// withConstrainedImpersonationAttributes 构造受约束代理的授权属性。
func withConstrainedImpersonationAttributes(a authorizer.AttributesRecord, mode string) authorizer.AttributesRecord {
	a.Verb = "impersonate:" + mode
	a.APIGroup = "authentication.k8s.io"
	a.APIVersion = "v1"
	a.ResourceRequest = true
	return a
}

// withImpersonateOnAttributes 构造 “impersonate-on:<mode>:<verb>” 形式的代理执行属性。
func withImpersonateOnAttributes(requestInfo *request.RequestInfo, mode string) authorizer.AttributesRecord {
	a := requestInfoToAttributes(requestInfo)
	a.Verb = "impersonate-on:" + mode + ":" + a.Verb
	return a
}

// requestInfoToAttributes 借用 filters 的实际转换逻辑得到属性，保证与生产一致。
func requestInfoToAttributes(requestInfo *request.RequestInfo) authorizer.AttributesRecord {
	requestCtx := request.WithRequestInfo(context.Background(), requestInfo)
	attrs, err := filters.GetAuthorizerAttributes(requestCtx)
	if err != nil {
		panic(err)
	}
	return *(attrs.(*authorizer.AttributesRecord))
}

// withLegacyImpersonateAttributes 构造旧版 “impersonate” 授权属性。
func withLegacyImpersonateAttributes(a authorizer.AttributesRecord) authorizer.AttributesRecord {
	a.Verb = "impersonate"
	a.APIVersion = "v1"
	a.ResourceRequest = true
	return a
}

// withUser 覆盖属性中的用户。
func withUser(a authorizer.AttributesRecord, u *user.DefaultInfo) authorizer.AttributesRecord {
	a.User = u
	return a
}

// expectAuthzCheck 构造期望的授权检查。
func expectAuthzCheck(attributes authorizer.AttributesRecord, decision authorizer.Decision) authzCheck {
	return authzCheck{
		attributes: attributes,
		decision:   decision,
	}
}

// expectAllow 构造期望放行的检查。
func expectAllow(attributes authorizer.AttributesRecord) authzCheck {
	return expectAuthzCheck(attributes, authorizer.DecisionAllow)
}

// expectDeny 构造期望拒绝的检查；假授权器拒绝时返回 NoOpinion。
func expectDeny(attributes authorizer.AttributesRecord) authzCheck {
	return expectAuthzCheck(attributes, authorizer.DecisionNoOpinion)
}

// outerCacheKey 构造外层缓存的期望键。
func outerCacheKey(wantedUser, requestor *user.DefaultInfo, req *request.RequestInfo, rawKey string) outerKey {
	attributes := withUser(requestInfoToAttributes(req), requestor)
	return outerKey{
		wantedUser: wantedUser,
		attributes: &attributes, // use a *authorizer.AttributesRecord so that it can be used in a map key
		rawKey:     rawKey,
	}
}

// expectedCache 描述两级缓存的期望内容。
type expectedCache struct {
	modeIdx map[string]string            // username -> mode verb
	modes   map[string]expectedModeCache // mode verb -> cache
}

// expectedModeCache 描述某个代理模式下的 outer/inner 缓存内容。
type expectedModeCache struct {
	outer map[outerKey]*user.DefaultInfo
	inner map[innerKey]*user.DefaultInfo
}

// outerKey 是外层缓存的期望键：被代理用户 + 完整属性 + 原始键。
type outerKey struct {
	wantedUser *user.DefaultInfo
	attributes *authorizer.AttributesRecord
	rawKey     string
}

// innerKey 是内层缓存的期望键：被代理用户 + 请求者 + 原始键。
type innerKey struct {
	wantedUser, requestor *user.DefaultInfo
	rawKey                string
}

// testRequest 描述一次请求及其全部期望结果（状态码、用户、授权检查、缓存、指标、审计）。
type testRequest struct {
	request          *request.RequestInfo
	requestor        *user.DefaultInfo
	impersonatedUser *user.DefaultInfo

	expectedImpersonatedUser *user.DefaultInfo
	expectedMessage          string

	expectedAttributesUser *user.DefaultInfo // nil means use requestor
	expectedAuthzChecks    []authzCheck
	expectedCache          *expectedCache
	expectedCode           int

	expectedAttemptMode          string
	expectedAttemptDecision      string
	expectedAuthorizationMetrics map[string]int
}

// associatedNodeTestCase 针对 “associated-node” 模式：
// 代理目标必须是请求者所在节点对应的 system:node:<node> 用户。
// 这些用例共享同一个 handler，因此后续请求会命中缓存，
// 期望的授权次数与缓存内容也随之变化。
func associatedNodeTestCase() []testRequest {
	getSecretRequest := &request.RequestInfo{
		IsResourceRequest: true,
		Verb:              "get",
		APIVersion:        "v1",
		Resource:          "secrets",
		Name:              "foo",
		Namespace:         "bar",
	}
	getPodRequest := &request.RequestInfo{
		IsResourceRequest: true,
		Verb:              "get",
		APIVersion:        "v1",
		Resource:          "pods",
		Name:              "foo",
		Namespace:         "bar",
	}
	saDefaultOnNode1 := &user.DefaultInfo{
		Name:   "system:serviceaccount:default:default",
		Groups: []string{"associate-node-impersonater"},
		Extra: map[string][]string{
			serviceaccount.NodeNameKey: {"node1"},
		},
	}
	saDefaultOnNode2 := &user.DefaultInfo{
		Name:   "system:serviceaccount:default:default",
		Groups: []string{"associate-node-impersonater"},
		Extra: map[string][]string{
			serviceaccount.NodeNameKey: {"node2"},
		},
	}
	saDefaultOnAnyNode := &user.DefaultInfo{
		Name:   "system:serviceaccount:default:default",
		Groups: []string{"associate-node-impersonater"},
		Extra: map[string][]string{
			"authentication.kubernetes.io/associated-node-keys": {"authentication.kubernetes.io/node-name"},
		},
	}
	node1FullUserInfo := &user.DefaultInfo{
		Name:   "system:node:node1",
		Groups: []string{user.NodesGroup, "system:authenticated"},
	}
	node2FullUserInfo := &user.DefaultInfo{
		Name:   "system:node:node2",
		Groups: []string{user.NodesGroup, "system:authenticated"},
	}
	cacheWithOnlyNode1Data := &expectedCache{
		modeIdx: map[string]string{
			"system:serviceaccount:default:default": "impersonate:associated-node",
		},
		modes: map[string]expectedModeCache{
			"impersonate:associated-node": {
				outer: map[outerKey]*user.DefaultInfo{
					outerCacheKey(&user.DefaultInfo{Name: "system:node:*"}, saDefaultOnAnyNode, getSecretRequest,
						"\x00\x00\x00\x1d\x00\x00\x00\rsystem:node:*\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb7\x00\x00\x00%system:serviceaccount:default:default\x00\x00\x00\x00\x00\x00\x00\x1f\x00\x00\x00\x1bassociate-node-impersonater\x00\x00\x00c\x00\x00\x001authentication.kubernetes.io/associated-node-keys\x00\x00\x00*\x00\x00\x00&authentication.kubernetes.io/node-name\x00\x00\x004\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\asecrets\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
					): node1FullUserInfo,
				},
				inner: map[innerKey]*user.DefaultInfo{
					{
						wantedUser: &user.DefaultInfo{Name: "system:node:*"},
						requestor:  saDefaultOnAnyNode,
						rawKey:     "\x00\x00\x00\x1d\x00\x00\x00\rsystem:node:*\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb7\x00\x00\x00%system:serviceaccount:default:default\x00\x00\x00\x00\x00\x00\x00\x1f\x00\x00\x00\x1bassociate-node-impersonater\x00\x00\x00c\x00\x00\x001authentication.kubernetes.io/associated-node-keys\x00\x00\x00*\x00\x00\x00&authentication.kubernetes.io/node-name\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
					}: node1FullUserInfo,
				},
			},
		},
	}
	cacheWithMultipleRequests := &expectedCache{
		modeIdx: cacheWithOnlyNode1Data.modeIdx,
		modes: map[string]expectedModeCache{
			"impersonate:associated-node": {
				outer: map[outerKey]*user.DefaultInfo{
					outerCacheKey(&user.DefaultInfo{Name: "system:node:*"}, saDefaultOnAnyNode, getSecretRequest,
						"\x00\x00\x00\x1d\x00\x00\x00\rsystem:node:*\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb7\x00\x00\x00%system:serviceaccount:default:default\x00\x00\x00\x00\x00\x00\x00\x1f\x00\x00\x00\x1bassociate-node-impersonater\x00\x00\x00c\x00\x00\x001authentication.kubernetes.io/associated-node-keys\x00\x00\x00*\x00\x00\x00&authentication.kubernetes.io/node-name\x00\x00\x004\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\asecrets\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
					): node1FullUserInfo,
					// even though this request was made on node2, since the inner cache matched it returned a value for node1
					outerCacheKey(&user.DefaultInfo{Name: "system:node:*"}, saDefaultOnAnyNode, getPodRequest,
						"\x00\x00\x00\x1d\x00\x00\x00\rsystem:node:*\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xb7\x00\x00\x00%system:serviceaccount:default:default\x00\x00\x00\x00\x00\x00\x00\x1f\x00\x00\x00\x1bassociate-node-impersonater\x00\x00\x00c\x00\x00\x001authentication.kubernetes.io/associated-node-keys\x00\x00\x00*\x00\x00\x00&authentication.kubernetes.io/node-name\x00\x00\x001\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
					): node1FullUserInfo,
				},
				inner: cacheWithOnlyNode1Data.modes["impersonate:associated-node"].inner,
			},
		},
	}
	return []testRequest{
		{
			request:                  getSecretRequest,
			requestor:                saDefaultOnNode1,
			impersonatedUser:         &user.DefaultInfo{Name: "system:node:node1"}, // node matches
			expectedImpersonatedUser: node1FullUserInfo,
			expectedAttributesUser:   saDefaultOnAnyNode,
			expectedAuthzChecks: []authzCheck{
				expectAllow(withImpersonateOnAttributes(getSecretRequest, "associated-node")),
				expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "nodes", Name: "*"}, "associated-node")),
			},
			expectedCache:           cacheWithOnlyNode1Data,
			expectedCode:            http.StatusOK,
			expectedAttemptMode:     "associated-node",
			expectedAttemptDecision: "allowed",
			expectedAuthorizationMetrics: map[string]int{
				"associated-node/allowed": 2, // impersonate-on + impersonate:associated-node
			},
		},
		{
			request:                      getSecretRequest,
			requestor:                    saDefaultOnNode2,
			impersonatedUser:             &user.DefaultInfo{Name: "system:node:node2"}, // node matches
			expectedImpersonatedUser:     node2FullUserInfo,
			expectedAttributesUser:       saDefaultOnAnyNode,
			expectedAuthzChecks:          nil, // no authz checks for the second request
			expectedCache:                cacheWithOnlyNode1Data,
			expectedCode:                 http.StatusOK,
			expectedAttemptMode:          "associated-node",
			expectedAttemptDecision:      "allowed",
			expectedAuthorizationMetrics: nil, // full cache hit
		},
		{
			request:                getSecretRequest,
			requestor:              saDefaultOnNode2,
			impersonatedUser:       &user.DefaultInfo{Name: "system:node:node1"}, // node does not match
			expectedMessage:        `nodes.authentication.k8s.io "node1" is forbidden: User "system:serviceaccount:default:default" cannot impersonate:arbitrary-node resource "nodes" in API group "authentication.k8s.io" at the cluster scope: deny by default`,
			expectedAttributesUser: nil,
			expectedAuthzChecks: []authzCheck{
				expectAllow(withImpersonateOnAttributes(getSecretRequest, "arbitrary-node")),
				expectDeny(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "nodes", Name: "node1"}, "arbitrary-node")),
				expectDeny(withLegacyImpersonateAttributes(authorizer.AttributesRecord{Resource: "users", Name: "system:node:node1"})),
			},
			expectedCache:           cacheWithOnlyNode1Data,
			expectedCode:            http.StatusForbidden,
			expectedAttemptMode:     "",
			expectedAttemptDecision: "denied",
			expectedAuthorizationMetrics: map[string]int{
				"arbitrary-node/allowed": 1, // impersonate-on:arbitrary-node:get
				"arbitrary-node/denied":  1, // impersonate:arbitrary-node nodes
				"legacy/denied":          1, // impersonate users
			},
		},
		{
			request:                  getPodRequest,
			requestor:                saDefaultOnNode2,
			impersonatedUser:         &user.DefaultInfo{Name: "system:node:node2"}, // node matches
			expectedImpersonatedUser: node2FullUserInfo,
			expectedAttributesUser:   saDefaultOnAnyNode,
			expectedAuthzChecks: []authzCheck{
				expectAllow(withImpersonateOnAttributes(getPodRequest, "associated-node")), // one authz check because different request info
			},
			expectedCache:           cacheWithMultipleRequests,
			expectedCode:            http.StatusOK,
			expectedAttemptMode:     "associated-node",
			expectedAttemptDecision: "allowed",
			expectedAuthorizationMetrics: map[string]int{
				"associated-node/allowed": 1, // impersonate-on only (inner cache hit)
			},
		},
		{
			request:                      getPodRequest,
			requestor:                    saDefaultOnNode1,
			impersonatedUser:             &user.DefaultInfo{Name: "system:node:node1"}, // node matches
			expectedImpersonatedUser:     node1FullUserInfo,
			expectedAttributesUser:       nil,
			expectedAuthzChecks:          nil, // no authz checks for pod request via node1
			expectedCache:                cacheWithMultipleRequests,
			expectedCode:                 http.StatusOK,
			expectedAttemptMode:          "associated-node",
			expectedAttemptDecision:      "allowed",
			expectedAuthorizationMetrics: nil, // full cache hit
		},
	}
}

// impersonationTestCase 是一组共享同一个 handler 的请求序列。
// 共享 handler 很重要：缓存状态会跨请求累积，后面的请求期望命中前面建立的缓存。
type impersonationTestCase struct {
	name     string
	requests []testRequest
}

// constrainedImpersonationTestCases 汇总所有受约束代理场景，
// 每个用例聚焦一种模式（serviceaccount、node、user-info、legacy 等）
// 及其授权、缓存、指标与审计的组合行为。
func constrainedImpersonationTestCases() []impersonationTestCase {
	getPodRequest := &request.RequestInfo{
		IsResourceRequest: true,
		Verb:              "get",
		APIVersion:        "v1",
		Resource:          "pods",
		Name:              "foo",
		Namespace:         "bar",
	}

	getAnotherPodRequest := &request.RequestInfo{
		IsResourceRequest: true,
		Verb:              "get",
		APIVersion:        "v1",
		Resource:          "pods",
		Name:              "foo1",
		Namespace:         "bar1",
	}

	createPodRequest := &request.RequestInfo{
		IsResourceRequest: true,
		Verb:              "create",
		APIVersion:        "v1",
		Resource:          "pods",
		Name:              "foo",
		Namespace:         "bar",
	}

	getDeploymentRequest := &request.RequestInfo{
		IsResourceRequest: true,
		Verb:              "get",
		APIVersion:        "v1",
		APIGroup:          "apps",
		Resource:          "deployments",
		Name:              "foo",
		Namespace:         "bar",
	}

	anyone := &user.DefaultInfo{Name: "anyone"}
	anyoneAuthenticated := &user.DefaultInfo{Name: "anyone", Groups: []string{"system:authenticated"}}
	saUser := &user.DefaultInfo{Name: "system:serviceaccount:default:default"}
	saUserAuthenticated := &user.DefaultInfo{
		Name:   "system:serviceaccount:default:default",
		Groups: []string{"system:serviceaccounts", "system:serviceaccounts:default", "system:authenticated"}}
	nodeUser := &user.DefaultInfo{Name: "system:node:node1"}
	nodeUserAuthenticated := &user.DefaultInfo{
		Name:   "system:node:node1",
		Groups: []string{user.NodesGroup, "system:authenticated"},
	}

	userImpersonator := &user.DefaultInfo{Name: "user-impersonater"}
	saImpersonator := &user.DefaultInfo{Name: "sa-impersonater"}
	nodeImpersonator := &user.DefaultInfo{Name: "node-impersonater"}

	testCases := []impersonationTestCase{
		{
			name: "impersonating-error",
			requests: []testRequest{
				{
					request:          getPodRequest,
					requestor:        &user.DefaultInfo{Name: "tester"},
					impersonatedUser: anyone,
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "user-info")),
						expectDeny(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "anyone"}, "user-info")),
						expectDeny(withLegacyImpersonateAttributes(authorizer.AttributesRecord{Resource: "users", Name: "anyone"})),
					},
					expectedCache:           nil,
					expectedCode:            http.StatusForbidden,
					expectedMessage:         `users.authentication.k8s.io "anyone" is forbidden: User "tester" cannot impersonate:user-info resource "users" in API group "authentication.k8s.io" at the cluster scope: deny by default`,
					expectedAttemptMode:     "",
					expectedAttemptDecision: "denied",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 1, // impersonate-on:user-info:get
						"user-info/denied":  1, // impersonate:user-info users
						"legacy/denied":     1, // impersonate users
					},
				},
			},
		},
		{
			name: "impersonating-user-get-allowed-create-disallowed",
			requests: []testRequest{
				{
					request:                  getPodRequest,
					requestor:                userImpersonator,
					impersonatedUser:         anyone,
					expectedImpersonatedUser: anyoneAuthenticated,
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "anyone"}, "user-info")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							userImpersonator.Name: "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(anyone, userImpersonator, getPodRequest,
										"\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x001\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): anyoneAuthenticated,
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: anyone,
										requestor:  userImpersonator,
										rawKey:     "\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: anyoneAuthenticated,
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "user-info",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 2, // impersonate-on + impersonate:user-info
					},
				},
				{
					request:                  getAnotherPodRequest,
					requestor:                userImpersonator,
					impersonatedUser:         anyone,
					expectedImpersonatedUser: anyoneAuthenticated,
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getAnotherPodRequest, "user-info")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							userImpersonator.Name: "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(anyone, userImpersonator, getPodRequest,
										"\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x001\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): anyoneAuthenticated,
									outerCacheKey(anyone, userImpersonator, getAnotherPodRequest,
										"\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x003\x00\x00\x00\x03get\x01\x00\x00\x00\x04bar1\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x04foo1\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): anyoneAuthenticated,
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: anyone,
										requestor:  userImpersonator,
										rawKey:     "\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: anyoneAuthenticated,
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "user-info",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 1, // impersonate-on (inner cache hit skips impersonate:user-info)
					},
				},
				{
					request:          createPodRequest,
					requestor:        userImpersonator,
					impersonatedUser: anyone,
					expectedAuthzChecks: []authzCheck{
						expectDeny(withImpersonateOnAttributes(createPodRequest, "user-info")),
						expectDeny(withLegacyImpersonateAttributes(authorizer.AttributesRecord{Resource: "users", Name: "anyone"})),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							userImpersonator.Name: "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(anyone, userImpersonator, getPodRequest,
										"\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x001\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): anyoneAuthenticated,
									outerCacheKey(anyone, userImpersonator, getAnotherPodRequest,
										"\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x003\x00\x00\x00\x03get\x01\x00\x00\x00\x04bar1\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x04foo1\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): anyoneAuthenticated,
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: anyone,
										requestor:  userImpersonator,
										rawKey:     "\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: anyoneAuthenticated,
								},
							},
						},
					},
					expectedCode:            http.StatusForbidden,
					expectedMessage:         `pods "foo" is forbidden: User "user-impersonater" cannot impersonate-on:user-info:create resource "pods" in API group "" in the namespace "bar": deny by default`,
					expectedAttemptMode:     "",
					expectedAttemptDecision: "denied",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/denied": 1, // impersonate-on:user-info:create
						"legacy/denied":    1, // impersonate users
					},
				},
			},
		},
		{
			name: "impersonating-sa-allowed",
			requests: []testRequest{
				{
					request:                  getPodRequest,
					requestor:                saImpersonator,
					impersonatedUser:         saUser,
					expectedImpersonatedUser: saUserAuthenticated,
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "serviceaccount")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "serviceaccounts", Namespace: "default", Name: "default"}, "serviceaccount")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							saImpersonator.Name: "impersonate:serviceaccount",
						},
						modes: map[string]expectedModeCache{
							"impersonate:serviceaccount": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(saUser, saImpersonator, getPodRequest,
										"\x00\x00\x005\x00\x00\x00%system:serviceaccount:default:default\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1f\x00\x00\x00\x0fsa-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x001\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): saUserAuthenticated,
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: saUser,
										requestor:  saImpersonator,
										rawKey:     "\x00\x00\x005\x00\x00\x00%system:serviceaccount:default:default\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x1f\x00\x00\x00\x0fsa-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: saUserAuthenticated,
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "serviceaccount",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"serviceaccount/allowed": 2, // impersonate-on + impersonate:serviceaccount
					},
				},
			},
		},
		{
			name: "impersonating-node-not-allowed",
			requests: []testRequest{
				{
					request:          getPodRequest,
					requestor:        saImpersonator,
					impersonatedUser: nodeUser,
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "arbitrary-node")),
						expectDeny(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "nodes", Name: "node1"}, "arbitrary-node")),
						expectDeny(withLegacyImpersonateAttributes(authorizer.AttributesRecord{Resource: "users", Name: "system:node:node1"})),
					},
					expectedCache:           nil,
					expectedCode:            http.StatusForbidden,
					expectedMessage:         `nodes.authentication.k8s.io "node1" is forbidden: User "sa-impersonater" cannot impersonate:arbitrary-node resource "nodes" in API group "authentication.k8s.io" at the cluster scope: deny by default`,
					expectedAttemptMode:     "",
					expectedAttemptDecision: "denied",
					expectedAuthorizationMetrics: map[string]int{
						"arbitrary-node/allowed": 1, // impersonate-on:arbitrary-node:get
						"arbitrary-node/denied":  1, // impersonate:arbitrary-node nodes
						"legacy/denied":          1, // impersonate users
					},
				},
			},
		},
		{
			name: "impersonating-node-not-allowed-action",
			requests: []testRequest{
				{
					request:          createPodRequest,
					requestor:        nodeImpersonator,
					impersonatedUser: nodeUser,
					expectedAuthzChecks: []authzCheck{
						expectDeny(withImpersonateOnAttributes(createPodRequest, "arbitrary-node")),
						expectDeny(withLegacyImpersonateAttributes(authorizer.AttributesRecord{Resource: "users", Name: "system:node:node1"})),
					},
					expectedCache:           nil,
					expectedCode:            http.StatusForbidden,
					expectedMessage:         `pods "foo" is forbidden: User "node-impersonater" cannot impersonate-on:arbitrary-node:create resource "pods" in API group "" in the namespace "bar": deny by default`,
					expectedAttemptMode:     "",
					expectedAttemptDecision: "denied",
					expectedAuthorizationMetrics: map[string]int{
						"arbitrary-node/denied": 1, // impersonate-on:arbitrary-node:create
						"legacy/denied":         1, // impersonate users
					},
				},
			},
		},
		{
			name: "impersonating-node-allowed",
			requests: []testRequest{
				{
					request:                  getPodRequest,
					requestor:                nodeImpersonator,
					impersonatedUser:         nodeUser,
					expectedImpersonatedUser: nodeUserAuthenticated,
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "arbitrary-node")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "nodes", Name: "node1"}, "arbitrary-node")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							nodeImpersonator.Name: "impersonate:arbitrary-node",
						},
						modes: map[string]expectedModeCache{
							"impersonate:arbitrary-node": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(nodeUser, nodeImpersonator, getPodRequest,
										"\x00\x00\x00!\x00\x00\x00\x11system:node:node1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11node-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x001\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): nodeUserAuthenticated,
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: nodeUser,
										requestor:  nodeImpersonator,
										rawKey:     "\x00\x00\x00!\x00\x00\x00\x11system:node:node1\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11node-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: nodeUserAuthenticated,
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "arbitrary-node",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"arbitrary-node/allowed": 2, // impersonate-on + impersonate:arbitrary-node
					},
				},
			},
		},
		{
			name: "disallowed-userextra-3",
			requests: []testRequest{
				{
					request: getPodRequest,
					requestor: &user.DefaultInfo{
						Name:   "user-impersonater",
						Groups: []string{"group-impersonater"},
					},
					impersonatedUser: &user.DefaultInfo{
						Name:   "system:admin",
						Groups: []string{"extra-setter-scopes"},
						Extra:  map[string][]string{"pandas.io/scopes": {"scope-a", "scope-b"}},
					},
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "system:admin"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "groups", Name: "extra-setter-scopes"}, "user-info")),
						expectDeny(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "pandas.io/scopes", Name: "scope-a"}, "user-info")),
						expectDeny(withLegacyImpersonateAttributes(authorizer.AttributesRecord{Resource: "users", Name: "system:admin"})),
					},
					expectedCache:           nil,
					expectedCode:            http.StatusForbidden,
					expectedMessage:         `userextras.authentication.k8s.io "scope-a" is forbidden: User "user-impersonater" cannot impersonate:user-info resource "userextras/pandas.io/scopes" in API group "authentication.k8s.io" at the cluster scope: deny by default`,
					expectedAttemptMode:     "",
					expectedAttemptDecision: "denied",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 3, // impersonate-on:get + users + groups
						"user-info/denied":  1, // userextras scope-a
						"legacy/denied":     1, // impersonate users
					},
				},
			},
		},
		{
			name: "allowed-userextras",
			requests: []testRequest{
				{
					request: getPodRequest,
					requestor: &user.DefaultInfo{
						Name:   "user-impersonater",
						Groups: []string{"extra-setter-scopes"},
					},
					impersonatedUser: &user.DefaultInfo{
						Name:  "system:admin",
						Extra: map[string][]string{"pandas.io/scopes": {"scope-a", "scope-b", "5", "4", "3", "2", "1"}},
					},
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "system:admin",
						Groups: []string{"system:authenticated"},
						Extra:  map[string][]string{"pandas.io/scopes": {"scope-a", "scope-b", "5", "4", "3", "2", "1"}},
					},
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "system:admin"}, "user-info")),
						expectDeny(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "*", Name: "*"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "pandas.io/scopes", Name: "scope-a"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "pandas.io/scopes", Name: "scope-b"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "pandas.io/scopes", Name: "5"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "pandas.io/scopes", Name: "4"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "pandas.io/scopes", Name: "3"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "pandas.io/scopes", Name: "2"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "pandas.io/scopes", Name: "1"}, "user-info")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							userImpersonator.Name: "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(
										&user.DefaultInfo{
											Name:  "system:admin",
											Extra: map[string][]string{"pandas.io/scopes": {"scope-a", "scope-b", "5", "4", "3", "2", "1"}},
										},
										&user.DefaultInfo{
											Name:   "user-impersonater",
											Groups: []string{"extra-setter-scopes"},
										},
										getPodRequest, "\x00\x00\x00c\x00\x00\x00\fsystem:admin\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00G\x00\x00\x00\x10pandas.io/scopes\x00\x00\x00/\x00\x00\x00\ascope-a\x00\x00\x00\ascope-b\x00\x00\x00\x015\x00\x00\x00\x014\x00\x00\x00\x013\x00\x00\x00\x012\x00\x00\x00\x011\x00\x00\x008\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x17\x00\x00\x00\x13extra-setter-scopes\x00\x00\x00\x00\x00\x00\x001\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"): {
										Name:   "system:admin",
										Groups: []string{"system:authenticated"},
										Extra:  map[string][]string{"pandas.io/scopes": {"scope-a", "scope-b", "5", "4", "3", "2", "1"}},
									},
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: &user.DefaultInfo{
											Name:  "system:admin",
											Extra: map[string][]string{"pandas.io/scopes": {"scope-a", "scope-b", "5", "4", "3", "2", "1"}},
										},
										requestor: &user.DefaultInfo{
											Name:   "user-impersonater",
											Groups: []string{"extra-setter-scopes"},
										},
										rawKey: "\x00\x00\x00c\x00\x00\x00\fsystem:admin\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00G\x00\x00\x00\x10pandas.io/scopes\x00\x00\x00/\x00\x00\x00\ascope-a\x00\x00\x00\ascope-b\x00\x00\x00\x015\x00\x00\x00\x014\x00\x00\x00\x013\x00\x00\x00\x012\x00\x00\x00\x011\x00\x00\x008\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x17\x00\x00\x00\x13extra-setter-scopes\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: {
										Name:   "system:admin",
										Groups: []string{"system:authenticated"},
										Extra:  map[string][]string{"pandas.io/scopes": {"scope-a", "scope-b", "5", "4", "3", "2", "1"}},
									},
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "user-info",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 9, // impersonate-on:get + users + 7 individual extra values
						"user-info/denied":  1, // wildcard userextras check
					},
				},
			},
		},
		{
			name:     "allowed-associate-node",
			requests: associatedNodeTestCase(),
		},
		{
			name: "disallowed-associate-node-without-sa",
			requests: []testRequest{
				{
					request: getPodRequest,
					requestor: &user.DefaultInfo{
						Name:   "user-impersonater",
						Groups: []string{"associate-node-impersonater"},
						Extra: map[string][]string{
							serviceaccount.NodeNameKey: {"node1"},
						},
					},
					impersonatedUser: &user.DefaultInfo{Name: "system:node:node1"},
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "arbitrary-node")),
						expectDeny(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "nodes", Name: "node1"}, "arbitrary-node")),
						expectDeny(withLegacyImpersonateAttributes(authorizer.AttributesRecord{Resource: "users", Name: "system:node:node1"})),
					},
					expectedCache:           nil,
					expectedCode:            http.StatusForbidden,
					expectedMessage:         `nodes.authentication.k8s.io "node1" is forbidden: User "user-impersonater" cannot impersonate:arbitrary-node resource "nodes" in API group "authentication.k8s.io" at the cluster scope: deny by default`,
					expectedAttemptMode:     "",
					expectedAttemptDecision: "denied",
					expectedAuthorizationMetrics: map[string]int{
						"arbitrary-node/allowed": 1, // impersonate-on:arbitrary-node:get
						"arbitrary-node/denied":  1, // impersonate:arbitrary-node nodes
						"legacy/denied":          1, // impersonate users
					},
				},
			},
		},
		{
			name: "allowed-legacy-impersonator",
			requests: []testRequest{
				{
					request:          getPodRequest,
					requestor:        &user.DefaultInfo{Name: "legacy-impersonater"},
					impersonatedUser: &user.DefaultInfo{Name: "system:admin"},
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "system:admin",
						Groups: []string{"system:authenticated"},
					},
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "user-info")),
						expectDeny(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "system:admin"}, "user-info")),
						expectAllow(withLegacyImpersonateAttributes(authorizer.AttributesRecord{Resource: "users", Name: "system:admin"})),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							"legacy-impersonater": "impersonate",
						},
						modes: nil, // legacy impersonation does not cache
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "legacy",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 1, // impersonate-on:user-info:get
						"user-info/denied":  1, // impersonate:user-info users
						"legacy/allowed":    1, // impersonate users
					},
				},
			},
		},
		{
			name: "system:masters-group-not-allowed",
			requests: []testRequest{
				{
					request:   getPodRequest,
					requestor: userImpersonator,
					impersonatedUser: &user.DefaultInfo{
						Name:   "admin-user",
						Groups: []string{user.SystemPrivilegedGroup},
					},
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "admin-user"}, "user-info")),
						expectDeny(withLegacyImpersonateAttributes(authorizer.AttributesRecord{Resource: "users", Name: "admin-user"})),
					},
					expectedCache:           nil,
					expectedCode:            http.StatusForbidden,
					expectedMessage:         `groups.authentication.k8s.io "system:masters" is forbidden: User "user-impersonater" cannot impersonate:user-info resource "groups" in API group "authentication.k8s.io" at the cluster scope: impersonating the system:masters group is not allowed`,
					expectedAttemptMode:     "",
					expectedAttemptDecision: "denied",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 2, // impersonate-on:user-info:get, impersonate:user-info users
						"legacy/denied":     1, // impersonate users
					},
				},
			},
		},
		{
			name: "many-groups-wildcard-allowed",
			requests: []testRequest{
				{
					request: getPodRequest,
					requestor: &user.DefaultInfo{
						Name: "many-groups-impersonater",
					},
					impersonatedUser: &user.DefaultInfo{
						Name:   "bob",
						Groups: []string{"group1", "group2", "group3", "group4"},
					},
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "bob",
						Groups: []string{"group1", "group2", "group3", "group4", "system:authenticated"},
					},
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "bob"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "groups", Name: "*"}, "user-info")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							"many-groups-impersonater": "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(&user.DefaultInfo{
										Name:   "bob",
										Groups: []string{"group1", "group2", "group3", "group4"},
									}, &user.DefaultInfo{Name: "many-groups-impersonater"}, getPodRequest,
										"\x00\x00\x00;\x00\x00\x00\x03bob\x00\x00\x00\x00\x00\x00\x00(\x00\x00\x00\x06group1\x00\x00\x00\x06group2\x00\x00\x00\x06group3\x00\x00\x00\x06group4\x00\x00\x00\x00\x00\x00\x00(\x00\x00\x00\x18many-groups-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x001\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): {
										Name:   "bob",
										Groups: []string{"group1", "group2", "group3", "group4", "system:authenticated"},
									},
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: &user.DefaultInfo{
											Name:   "bob",
											Groups: []string{"group1", "group2", "group3", "group4"},
										},
										requestor: &user.DefaultInfo{Name: "many-groups-impersonater"},
										rawKey:    "\x00\x00\x00;\x00\x00\x00\x03bob\x00\x00\x00\x00\x00\x00\x00(\x00\x00\x00\x06group1\x00\x00\x00\x06group2\x00\x00\x00\x06group3\x00\x00\x00\x06group4\x00\x00\x00\x00\x00\x00\x00(\x00\x00\x00\x18many-groups-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: {
										Name:   "bob",
										Groups: []string{"group1", "group2", "group3", "group4", "system:authenticated"},
									},
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "user-info",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 3, // impersonate-on:user-info:get, impersonate:user-info users/groups
					},
				},
			},
		},
		{
			name: "many-extras-wildcard-allowed",
			requests: []testRequest{
				{
					request: getPodRequest,
					requestor: &user.DefaultInfo{
						Name: "many-extras-impersonater",
					},
					impersonatedUser: &user.DefaultInfo{
						Name: "alice",
						Extra: map[string][]string{
							"scopes.example.com/key1": {"val1"},
							"scopes.example.com/key2": {"val2"},
							"scopes.example.com/key3": {"val3"},
							"scopes.example.com/key4": {"val4"},
						},
					},
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "alice",
						Groups: []string{"system:authenticated"},
						Extra: map[string][]string{
							"scopes.example.com/key1": {"val1"},
							"scopes.example.com/key2": {"val2"},
							"scopes.example.com/key3": {"val3"},
							"scopes.example.com/key4": {"val4"},
						},
					},
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "alice"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "*", Name: "*"}, "user-info")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							"many-extras-impersonater": "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(&user.DefaultInfo{
										Name: "alice",
										Extra: map[string][]string{
											"scopes.example.com/key1": {"val1"},
											"scopes.example.com/key2": {"val2"},
											"scopes.example.com/key3": {"val3"},
											"scopes.example.com/key4": {"val4"},
										},
									}, &user.DefaultInfo{Name: "many-extras-impersonater"}, getPodRequest,
										"\x00\x00\x00\xb1\x00\x00\x00\x05alice\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x9c\x00\x00\x00\x17scopes.example.com/key1\x00\x00\x00\b\x00\x00\x00\x04val1\x00\x00\x00\x17scopes.example.com/key2\x00\x00\x00\b\x00\x00\x00\x04val2\x00\x00\x00\x17scopes.example.com/key3\x00\x00\x00\b\x00\x00\x00\x04val3\x00\x00\x00\x17scopes.example.com/key4\x00\x00\x00\b\x00\x00\x00\x04val4\x00\x00\x00(\x00\x00\x00\x18many-extras-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x001\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): {
										Name:   "alice",
										Groups: []string{"system:authenticated"},
										Extra: map[string][]string{
											"scopes.example.com/key1": {"val1"},
											"scopes.example.com/key2": {"val2"},
											"scopes.example.com/key3": {"val3"},
											"scopes.example.com/key4": {"val4"},
										},
									},
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: &user.DefaultInfo{
											Name: "alice",
											Extra: map[string][]string{
												"scopes.example.com/key1": {"val1"},
												"scopes.example.com/key2": {"val2"},
												"scopes.example.com/key3": {"val3"},
												"scopes.example.com/key4": {"val4"},
											},
										},
										requestor: &user.DefaultInfo{Name: "many-extras-impersonater"},
										rawKey:    "\x00\x00\x00\xb1\x00\x00\x00\x05alice\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x9c\x00\x00\x00\x17scopes.example.com/key1\x00\x00\x00\b\x00\x00\x00\x04val1\x00\x00\x00\x17scopes.example.com/key2\x00\x00\x00\b\x00\x00\x00\x04val2\x00\x00\x00\x17scopes.example.com/key3\x00\x00\x00\b\x00\x00\x00\x04val3\x00\x00\x00\x17scopes.example.com/key4\x00\x00\x00\b\x00\x00\x00\x04val4\x00\x00\x00(\x00\x00\x00\x18many-extras-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: {
										Name:   "alice",
										Groups: []string{"system:authenticated"},
										Extra: map[string][]string{
											"scopes.example.com/key1": {"val1"},
											"scopes.example.com/key2": {"val2"},
											"scopes.example.com/key3": {"val3"},
											"scopes.example.com/key4": {"val4"},
										},
									},
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "user-info",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 3, // impersonate-on:user-info:get, impersonate:user-info users/extras
					},
				},
			},
		},
		{
			name: "mixed-groups-extras-wildcard-denied-individuals-allowed",
			requests: []testRequest{
				{
					request: getPodRequest,
					requestor: &user.DefaultInfo{
						Name: "mixed-impersonater",
					},
					impersonatedUser: &user.DefaultInfo{
						Name:   "frank",
						Groups: []string{"dev", "ops", "security", "audit"},
						Extra: map[string][]string{
							"tags.io/env": {"prod", "staging", "dev"},
						},
					},
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "frank",
						Groups: []string{"dev", "ops", "security", "audit", "system:authenticated"},
						Extra: map[string][]string{
							"tags.io/env": {"prod", "staging", "dev"},
						},
					},
					expectedAuthzChecks: []authzCheck{
						// impersonate-on check passes
						expectAllow(withImpersonateOnAttributes(getPodRequest, "user-info")),
						// user check passes
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "frank"}, "user-info")),
						// wildcard groups check FAILS (mixed-impersonater doesn't allow wildcard groups)
						expectDeny(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "groups", Name: "*"}, "user-info")),
						// individual group checks PASS - demonstrating fallback after wildcard failure
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "groups", Name: "dev"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "groups", Name: "ops"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "groups", Name: "security"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "groups", Name: "audit"}, "user-info")),
						// individual extra checks PASS (wildcard not attempted for extras with single subresource)
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "tags.io/env", Name: "prod"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "tags.io/env", Name: "staging"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "userextras", Subresource: "tags.io/env", Name: "dev"}, "user-info")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							"mixed-impersonater": "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(&user.DefaultInfo{
										Name:   "frank",
										Groups: []string{"dev", "ops", "security", "audit"},
										Extra: map[string][]string{
											"tags.io/env": {"prod", "staging", "dev"},
										},
									}, &user.DefaultInfo{Name: "mixed-impersonater"}, getPodRequest,
										"\x00\x00\x00e\x00\x00\x00\x05frank\x00\x00\x00\x00\x00\x00\x00#\x00\x00\x00\x03dev\x00\x00\x00\x03ops\x00\x00\x00\bsecurity\x00\x00\x00\x05audit\x00\x00\x00-\x00\x00\x00\vtags.io/env\x00\x00\x00\x1a\x00\x00\x00\x04prod\x00\x00\x00\astaging\x00\x00\x00\x03dev\x00\x00\x00\"\x00\x00\x00\x12mixed-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x001\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): {
										Name:   "frank",
										Groups: []string{"dev", "ops", "security", "audit", "system:authenticated"},
										Extra: map[string][]string{
											"tags.io/env": {"prod", "staging", "dev"},
										},
									},
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: &user.DefaultInfo{
											Name:   "frank",
											Groups: []string{"dev", "ops", "security", "audit"},
											Extra: map[string][]string{
												"tags.io/env": {"prod", "staging", "dev"},
											},
										},
										requestor: &user.DefaultInfo{Name: "mixed-impersonater"},
										rawKey:    "\x00\x00\x00e\x00\x00\x00\x05frank\x00\x00\x00\x00\x00\x00\x00#\x00\x00\x00\x03dev\x00\x00\x00\x03ops\x00\x00\x00\bsecurity\x00\x00\x00\x05audit\x00\x00\x00-\x00\x00\x00\vtags.io/env\x00\x00\x00\x1a\x00\x00\x00\x04prod\x00\x00\x00\astaging\x00\x00\x00\x03dev\x00\x00\x00\"\x00\x00\x00\x12mixed-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: {
										Name:   "frank",
										Groups: []string{"dev", "ops", "security", "audit", "system:authenticated"},
										Extra: map[string][]string{
											"tags.io/env": {"prod", "staging", "dev"},
										},
									},
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "user-info",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 9, // impersonate-on:user-info:get; impersonate:user-info users; 4*impersonate:user-info groups; 3*impersonate:user-info extras
						"user-info/denied":  1, // impersonate:user-info groups "*"
					},
				},
			},
		},
		{
			name: "continuous-same-allowed-user-requests",
			requests: []testRequest{
				{
					request:          getDeploymentRequest,
					requestor:        userImpersonator,
					impersonatedUser: &user.DefaultInfo{Name: "bob"},
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "bob",
						Groups: []string{"system:authenticated"},
					},
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getDeploymentRequest, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "bob"}, "user-info")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							userImpersonator.Name: "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(&user.DefaultInfo{Name: "bob"}, userImpersonator, getDeploymentRequest,
										"\x00\x00\x00\x13\x00\x00\x00\x03bob\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00<\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\vdeployments\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x04apps\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): {
										Name:   "bob",
										Groups: []string{"system:authenticated"},
									},
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: &user.DefaultInfo{Name: "bob"},
										requestor:  userImpersonator,
										rawKey:     "\x00\x00\x00\x13\x00\x00\x00\x03bob\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: {
										Name:   "bob",
										Groups: []string{"system:authenticated"},
									},
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "user-info",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 2, // impersonate-on + impersonate:user-info
					},
				},
				{
					request:          getDeploymentRequest,
					requestor:        userImpersonator,
					impersonatedUser: &user.DefaultInfo{Name: "bob"},
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "bob",
						Groups: []string{"system:authenticated"},
					},
					expectedAuthzChecks: nil, // no authz checks for cached request
					expectedCode:        http.StatusOK,
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							userImpersonator.Name: "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(&user.DefaultInfo{Name: "bob"}, userImpersonator, getDeploymentRequest,
										"\x00\x00\x00\x13\x00\x00\x00\x03bob\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00<\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\vdeployments\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x04apps\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): {
										Name:   "bob",
										Groups: []string{"system:authenticated"},
									},
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: &user.DefaultInfo{Name: "bob"},
										requestor:  userImpersonator,
										rawKey:     "\x00\x00\x00\x13\x00\x00\x00\x03bob\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: {
										Name:   "bob",
										Groups: []string{"system:authenticated"},
									},
								},
							},
						},
					},
					expectedAttemptMode:          "user-info",
					expectedAttemptDecision:      "allowed",
					expectedAuthorizationMetrics: nil, // full cache hit
				},
			},
		},
		{
			name: "impersonating-user-with-uid",
			requests: []testRequest{
				{
					request:   getPodRequest,
					requestor: &user.DefaultInfo{Name: "user-impersonater", UID: "requestor-uid-123"},
					impersonatedUser: &user.DefaultInfo{
						Name: "anyone",
						UID:  "wanted-uid-456",
					},
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "anyone",
						UID:    "wanted-uid-456",
						Groups: []string{"system:authenticated"},
					},
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "anyone"}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "uids", Name: "wanted-uid-456"}, "user-info")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							"user-impersonater": "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(
										&user.DefaultInfo{Name: "anyone", UID: "wanted-uid-456"},
										&user.DefaultInfo{Name: "user-impersonater", UID: "requestor-uid-123"},
										getPodRequest,
										"\x00\x00\x00$\x00\x00\x00\x06anyone\x00\x00\x00\x0ewanted-uid-456\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x002\x00\x00\x00\x11user-impersonater\x00\x00\x00\x11requestor-uid-123\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x001\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): {
										Name:   "anyone",
										UID:    "wanted-uid-456",
										Groups: []string{"system:authenticated"},
									},
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: &user.DefaultInfo{Name: "anyone", UID: "wanted-uid-456"},
										requestor:  &user.DefaultInfo{Name: "user-impersonater", UID: "requestor-uid-123"},
										rawKey:     "\x00\x00\x00$\x00\x00\x00\x06anyone\x00\x00\x00\x0ewanted-uid-456\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x002\x00\x00\x00\x11user-impersonater\x00\x00\x00\x11requestor-uid-123\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: {
										Name:   "anyone",
										UID:    "wanted-uid-456",
										Groups: []string{"system:authenticated"},
									},
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "user-info",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 3, // impersonate-on + impersonate:user-info users + uids
					},
				},
			},
		},
		{
			name: "impersonating-user-subresource-request",
			requests: []testRequest{
				{
					request: &request.RequestInfo{
						IsResourceRequest: true,
						Verb:              "get",
						APIVersion:        "v1",
						Resource:          "pods",
						Subresource:       "status",
						Name:              "foo",
						Namespace:         "bar",
					},
					requestor:                userImpersonator,
					impersonatedUser:         anyone,
					expectedImpersonatedUser: anyoneAuthenticated,
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(&request.RequestInfo{
							IsResourceRequest: true,
							Verb:              "get",
							APIVersion:        "v1",
							Resource:          "pods",
							Subresource:       "status",
							Name:              "foo",
							Namespace:         "bar",
						}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "anyone"}, "user-info")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							userImpersonator.Name: "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(anyone, userImpersonator, &request.RequestInfo{
										IsResourceRequest: true,
										Verb:              "get",
										APIVersion:        "v1",
										Resource:          "pods",
										Subresource:       "status",
										Name:              "foo",
										Namespace:         "bar",
									},
										"\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x007\x00\x00\x00\x03get\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x06status\x00\x00\x00\x03foo\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									): anyoneAuthenticated,
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: anyone,
										requestor:  userImpersonator,
										rawKey:     "\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: anyoneAuthenticated,
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "user-info",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 2, // impersonate-on + impersonate:user-info
					},
				},
			},
		},
		{
			name: "impersonating-user-non-resource-request",
			requests: []testRequest{
				{
					request: &request.RequestInfo{
						IsResourceRequest: false,
						Verb:              "get",
						Path:              "/healthz",
					},
					requestor:                userImpersonator,
					impersonatedUser:         anyone,
					expectedImpersonatedUser: anyoneAuthenticated,
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(&request.RequestInfo{
							IsResourceRequest: false,
							Verb:              "get",
							Path:              "/healthz",
						}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "anyone"}, "user-info")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							userImpersonator.Name: "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(anyone, userImpersonator, &request.RequestInfo{
										IsResourceRequest: false,
										Verb:              "get",
										Path:              "/healthz",
									},
										"\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00-\x00\x00\x00\x03get\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\b/healthz\x00\x00\x00\x00\x00\x00\x00\x00",
									): anyoneAuthenticated,
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: anyone,
										requestor:  userImpersonator,
										rawKey:     "\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: anyoneAuthenticated,
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "user-info",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 2, // impersonate-on + impersonate:user-info
					},
				},
			},
		},
		{
			name: "impersonating-user-with-field-and-label-selectors",
			requests: []testRequest{
				{
					request: &request.RequestInfo{
						IsResourceRequest: true,
						Verb:              "list",
						APIVersion:        "v1",
						Resource:          "pods",
						Namespace:         "bar",
						FieldSelector:     "spec.nodeName=node1",
						LabelSelector:     "app=web",
					},
					requestor:                userImpersonator,
					impersonatedUser:         anyone,
					expectedImpersonatedUser: anyoneAuthenticated,
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(&request.RequestInfo{
							IsResourceRequest: true,
							Verb:              "list",
							APIVersion:        "v1",
							Resource:          "pods",
							Namespace:         "bar",
							FieldSelector:     "spec.nodeName=node1",
							LabelSelector:     "app=web",
						}, "user-info")),
						expectAllow(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "anyone"}, "user-info")),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							userImpersonator.Name: "impersonate:user-info",
						},
						modes: map[string]expectedModeCache{
							"impersonate:user-info": {
								outer: map[outerKey]*user.DefaultInfo{
									outerCacheKey(anyone, userImpersonator, &request.RequestInfo{
										IsResourceRequest: true,
										Verb:              "list",
										APIVersion:        "v1",
										Resource:          "pods",
										Namespace:         "bar",
										FieldSelector:     "spec.nodeName=node1",
										LabelSelector:     "app=web",
									}, "\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00/\x00\x00\x00\x04list\x01\x00\x00\x00\x03bar\x00\x00\x00\x04pods\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02v1\x01\x00\x00\x00\x00\x00\x00\x00#\x00\x00\x00\x1f\x00\x00\x00\rspec.nodeName\x00\x00\x00\x01=\x00\x00\x00\x05node1\x00\x00\x00\v\x00\x00\x00\aapp=web"): anyoneAuthenticated,
								},
								inner: map[innerKey]*user.DefaultInfo{
									{
										wantedUser: anyone,
										requestor:  userImpersonator,
										rawKey:     "\x00\x00\x00\x16\x00\x00\x00\x06anyone\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00!\x00\x00\x00\x11user-impersonater\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
									}: anyoneAuthenticated,
								},
							},
						},
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "user-info",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 2, // impersonate-on + impersonate:user-info
					},
				},
			},
		},
		{
			name: "legacy-fallback",
			requests: []testRequest{
				{
					request:          getPodRequest,
					requestor:        &user.DefaultInfo{Name: "legacy-impersonater"},
					impersonatedUser: anyone,
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "anyone",
						Groups: []string{"system:authenticated"},
					},
					expectedAuthzChecks: []authzCheck{
						expectAllow(withImpersonateOnAttributes(getPodRequest, "user-info")),
						expectDeny(withConstrainedImpersonationAttributes(authorizer.AttributesRecord{Resource: "users", Name: "anyone"}, "user-info")),
						expectAllow(withLegacyImpersonateAttributes(authorizer.AttributesRecord{Resource: "users", Name: "anyone"})),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							"legacy-impersonater": "impersonate",
						},
						modes: nil, // legacy impersonation does not cache
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "legacy",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"user-info/allowed": 1, // impersonate-on:user-info:get
						"user-info/denied":  1, // impersonate:user-info users
						"legacy/allowed":    1, // impersonate users
					},
				},
				{
					request:          getDeploymentRequest,
					requestor:        &user.DefaultInfo{Name: "legacy-impersonater"},
					impersonatedUser: anyone,
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "anyone",
						Groups: []string{"system:authenticated"},
					},
					expectedAuthzChecks: []authzCheck{
						expectAllow(withLegacyImpersonateAttributes(authorizer.AttributesRecord{Resource: "users", Name: "anyone"})),
					},
					expectedCache: &expectedCache{
						modeIdx: map[string]string{
							"legacy-impersonater": "impersonate",
						},
						modes: nil, // legacy impersonation does not cache
					},
					expectedCode:            http.StatusOK,
					expectedAttemptMode:     "legacy",
					expectedAttemptDecision: "allowed",
					expectedAuthorizationMetrics: map[string]int{
						"legacy/allowed": 1, // impersonate users (mode index cache hit skips constrained checks)
					},
				},
			},
		},
	}
	return testCases
}

// TestConstrainedImpersonationFilter 端到端验证受约束代理过滤器：
// 为每个用例分配独立 URL 与 handler，并行执行，逐请求比对
// 响应码/响应体、授权检查序列、缓存状态、指标与审计事件。
func TestConstrainedImpersonationFilter(t *testing.T) {
	testCases := constrainedImpersonationTestCases()

	// 每个用例通过独立路径复用同一个 httptest server，避免为每个子测试启动服务器。
	var mux http.ServeMux
	tests := make([]*constrainedImpersonationTest, len(testCases))
	handlers := make([]http.Handler, len(testCases))
	for i := range len(testCases) {
		mux.Handle("/"+strconv.Itoa(i), http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			handlers[i].ServeHTTP(w, req)
		}))
	}

	server := httptest.NewServer(&mux)
	t.Cleanup(server.Close)

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			test := &constrainedImpersonationTest{t: t}
			tests[i] = test
			handlers[i] = test.handler()

			for _, r := range tc.requests {
				resp := doImpersonationRequest(t, http.DefaultTransport, server.URL+"/"+strconv.Itoa(i), r, r.expectedCode)

				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				_ = resp.Body.Close()

				// 成功路径响应体是被代理用户的 JSON；失败路径响应体是 Status。
				if r.expectedCode == http.StatusOK {
					var actualUser user.DefaultInfo
					if err := json.Unmarshal(body, &actualUser); err != nil {
						t.Errorf("unexpected error: %v, body=\n%s", err, string(body))
					}

					require.Equal(t, r.expectedImpersonatedUser, &actualUser)
					test.assertEchoCalled(true)
					require.NotNil(t, r.expectedImpersonatedUser) // sanity check test data
					require.Empty(t, r.expectedMessage)           // sanity check test data
				} else {
					var status metav1.Status
					if err := json.Unmarshal(body, &status); err != nil {
						t.Errorf("unexpected error: %v, body=\n%s", err, string(body))
					}
					require.Equal(t, r.expectedMessage, status.Message)
					test.assertEchoCalled(false)
					require.NotEmpty(t, r.expectedMessage)     // sanity check test data
					require.Nil(t, r.expectedImpersonatedUser) // sanity check test data
				}

				test.assertAttributes(r)
				test.assertCache(r)
				test.assertMetrics(r)
				test.assertAuditEvents(r)
			}
		})
	}
}

// TestConstrainedImpersonationParallel 在 -race 下验证并发安全性：
// 多个用户/目标/操作组合同时打向同一 handler，检查结果互不串扰、
// 缓存与模式索引在并发访问下保持一致。
func TestConstrainedImpersonationParallel(t *testing.T) {
	// This test verifies goroutine safety by sending concurrent requests.
	// Each test case defines multiple requests that are sent in parallel,
	// ensuring no data races or request interference occurs.

	// Common request infos
	getPods := &request.RequestInfo{IsResourceRequest: true, Verb: "get", APIVersion: "v1", Resource: "pods"}
	listServices := &request.RequestInfo{IsResourceRequest: true, Verb: "list", APIVersion: "v1", Resource: "services"}
	createConfigMaps := &request.RequestInfo{IsResourceRequest: true, Verb: "create", APIVersion: "v1", Resource: "configmaps"}
	createSecrets := &request.RequestInfo{IsResourceRequest: true, Verb: "create", APIVersion: "v1", Resource: "secrets"}

	type parallelRequest struct {
		requestor                *user.DefaultInfo
		impersonatedUser         string
		requestInfo              *request.RequestInfo
		expectedStatusCode       int
		expectedImpersonatedUser *user.DefaultInfo
	}

	testCases := []struct {
		name     string
		requests []parallelRequest
	}{
		{
			name: "different-users-same-target",
			requests: []parallelRequest{
				{
					requestor:          &user.DefaultInfo{Name: "sa-impersonater"},
					impersonatedUser:   "system:serviceaccount:default:my-sa",
					requestInfo:        getPods,
					expectedStatusCode: http.StatusOK,
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "system:serviceaccount:default:my-sa",
						Groups: []string{"system:serviceaccounts", "system:serviceaccounts:default", "system:authenticated"},
					},
				},
				{
					requestor:          &user.DefaultInfo{Name: "node-impersonater"},
					impersonatedUser:   "system:serviceaccount:default:my-sa",
					requestInfo:        getPods,
					expectedStatusCode: http.StatusForbidden,
				},
				{
					requestor:          &user.DefaultInfo{Name: "user-impersonater"},
					impersonatedUser:   "system:serviceaccount:default:my-sa",
					requestInfo:        getPods,
					expectedStatusCode: http.StatusForbidden,
				},
			},
		},
		{
			name: "same-user-same-targets",
			requests: []parallelRequest{
				{
					requestor:          &user.DefaultInfo{Name: "sa-impersonater"},
					impersonatedUser:   "system:serviceaccount:default:my-sa",
					requestInfo:        getPods,
					expectedStatusCode: http.StatusOK,
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "system:serviceaccount:default:my-sa",
						Groups: []string{"system:serviceaccounts", "system:serviceaccounts:default", "system:authenticated"},
					},
				},
				{
					requestor:          &user.DefaultInfo{Name: "sa-impersonater"},
					impersonatedUser:   "system:serviceaccount:default:my-sa",
					requestInfo:        getPods,
					expectedStatusCode: http.StatusOK,
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "system:serviceaccount:default:my-sa",
						Groups: []string{"system:serviceaccounts", "system:serviceaccounts:default", "system:authenticated"},
					},
				},
			},
		},
		{
			name: "same-user-different-targets",
			requests: []parallelRequest{
				{
					requestor:          &user.DefaultInfo{Name: "sa-impersonater"},
					impersonatedUser:   "system:serviceaccount:default:sa1",
					requestInfo:        getPods,
					expectedStatusCode: http.StatusOK,
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "system:serviceaccount:default:sa1",
						Groups: []string{"system:serviceaccounts", "system:serviceaccounts:default", "system:authenticated"},
					},
				},
				{
					requestor:          &user.DefaultInfo{Name: "sa-impersonater"},
					impersonatedUser:   "system:serviceaccount:kube-system:sa2",
					requestInfo:        getPods,
					expectedStatusCode: http.StatusOK,
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "system:serviceaccount:kube-system:sa2",
						Groups: []string{"system:serviceaccounts", "system:serviceaccounts:kube-system", "system:authenticated"},
						Extra:  map[string][]string{},
					},
				},
				{
					requestor:          &user.DefaultInfo{Name: "sa-impersonater"},
					impersonatedUser:   "system:node:node-1",
					requestInfo:        getPods,
					expectedStatusCode: http.StatusForbidden,
				},
			},
		},
		{
			name: "same-user-same-target-different-operations",
			requests: []parallelRequest{
				{
					requestor:          &user.DefaultInfo{Name: "sa-impersonater"},
					impersonatedUser:   "system:serviceaccount:default:my-sa",
					requestInfo:        getPods,
					expectedStatusCode: http.StatusOK,
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "system:serviceaccount:default:my-sa",
						Groups: []string{"system:serviceaccounts", "system:serviceaccounts:default", "system:authenticated"},
					},
				},
				{
					requestor:          &user.DefaultInfo{Name: "sa-impersonater"},
					impersonatedUser:   "system:serviceaccount:default:my-sa",
					requestInfo:        listServices,
					expectedStatusCode: http.StatusOK,
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "system:serviceaccount:default:my-sa",
						Groups: []string{"system:serviceaccounts", "system:serviceaccounts:default", "system:authenticated"},
					},
				},
				{
					requestor:          &user.DefaultInfo{Name: "sa-impersonater"},
					impersonatedUser:   "system:serviceaccount:default:my-sa",
					requestInfo:        createConfigMaps,
					expectedStatusCode: http.StatusForbidden,
				},
				{
					requestor:          &user.DefaultInfo{Name: "sa-impersonater"},
					impersonatedUser:   "system:serviceaccount:default:my-sa",
					requestInfo:        createSecrets,
					expectedStatusCode: http.StatusForbidden,
				},
			},
		},
		{
			name: "mixed-scenarios",
			requests: []parallelRequest{
				{
					requestor:          &user.DefaultInfo{Name: "sa-impersonater"},
					impersonatedUser:   "system:serviceaccount:default:sa1",
					requestInfo:        getPods,
					expectedStatusCode: http.StatusOK,
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "system:serviceaccount:default:sa1",
						Groups: []string{"system:serviceaccounts", "system:serviceaccounts:default", "system:authenticated"},
					},
				},
				{
					requestor:          &user.DefaultInfo{Name: "node-impersonater"},
					impersonatedUser:   "system:node:node-1",
					requestInfo:        listServices,
					expectedStatusCode: http.StatusOK,
					expectedImpersonatedUser: &user.DefaultInfo{
						Name:   "system:node:node-1",
						Groups: []string{"system:nodes", "system:authenticated"},
						Extra:  map[string][]string{},
					},
				},
				{
					requestor:          &user.DefaultInfo{Name: "user-impersonater"},
					impersonatedUser:   "alice",
					requestInfo:        createConfigMaps,
					expectedStatusCode: http.StatusForbidden,
				},
				{
					requestor:          &user.DefaultInfo{Name: "unauthorized-user"},
					impersonatedUser:   "bob",
					requestInfo:        getPods,
					expectedStatusCode: http.StatusForbidden,
				},
			},
		},
	}

	type resultType struct {
		requestIdx int
		statusCode int
		body       []byte
		err        error
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			test := &constrainedImpersonationTest{t: t}
			server := httptest.NewServer(test.parallelHandler())
			defer server.Close()

			var wg sync.WaitGroup
			results := make(chan resultType, len(tc.requests))

			// Send all requests concurrently
			// 并发发送全部请求，制造缓存读写竞争；
			// 每个请求自带 requestor 与目标用户，互不共享可变状态。
			for idx, req := range tc.requests {
				wg.Go(func() {
					rt := &testRoundTripper{
						user:        req.requestor,
						requestInfo: req.requestInfo,
						delegate:    http.DefaultTransport,
					}
					client := &http.Client{Transport: rt}

					httpReq, err := http.NewRequest(http.MethodGet, server.URL, nil)
					if err != nil {
						results <- resultType{requestIdx: idx, err: err}
						return
					}

					if len(req.impersonatedUser) > 0 {
						httpReq.Header.Add(authenticationv1.ImpersonateUserHeader, req.impersonatedUser)
					}

					resp, err := client.Do(httpReq)
					if err != nil {
						results <- resultType{requestIdx: idx, err: err}
						return
					}
					defer func() {
						err := resp.Body.Close()
						if err != nil {
							results <- resultType{requestIdx: idx, err: err}
						}
					}()

					body, err := io.ReadAll(resp.Body)
					if err != nil {
						results <- resultType{requestIdx: idx, err: err}
						return
					}

					results <- resultType{
						requestIdx: idx,
						statusCode: resp.StatusCode,
						body:       body,
					}
				})
			}

			// Wait for all requests to complete
			wg.Wait()
			close(results)

			// Collect results
			// 用索引映射收结果，避免并发完成顺序影响断言语义。
			resultMap := make(map[int]resultType)
			for res := range results {
				require.NoError(t, res.err, "request %d: unexpected error", res.requestIdx)
				_, ok := resultMap[res.requestIdx]
				require.False(t, ok, "request %d: should not duplicate in the result", res.requestIdx)
				resultMap[res.requestIdx] = res
			}

			// Verify each request's result
			for idx, req := range tc.requests {
				res, ok := resultMap[idx]
				require.True(t, ok, "request %d: missing result", idx)

				require.Equal(t, req.expectedStatusCode, res.statusCode,
					"request %d: status code mismatch (requestor: %s, target: %s, verb: %s, resource: %s)",
					idx, req.requestor.Name, req.impersonatedUser, req.requestInfo.Verb, req.requestInfo.Resource)

				if req.expectedImpersonatedUser != nil {
					// Success case - verify impersonated user
					var actualUser user.DefaultInfo
					err := json.Unmarshal(res.body, &actualUser)
					require.NoError(t, err, "request %d: failed to unmarshal user", idx)
					require.Equal(t, req.expectedImpersonatedUser.Name, actualUser.Name,
						"request %d: user name mismatch", idx)
					require.ElementsMatch(t, req.expectedImpersonatedUser.Groups, actualUser.Groups,
						"request %d: groups mismatch", idx)
				}
			}
		})
	}
}

// benchmarkHandler 构造只含认证/请求信息/代理的最小处理链，
// 并把指标回调置为空操作，使基准测试只测代理本身的成本。
func (c *constrainedImpersonationTest) benchmarkHandler(withImpersonation func(http.Handler, authorizer.UnconditionalAuthorizer, runtime.NegotiatedSerializer) http.Handler) http.Handler {
	s := runtime.NewScheme()
	metav1.AddToGroupVersion(s, metav1.SchemeGroupVersion)
	addImpersonation := withImpersonation(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {}), c, serializer.NewCodecFactory(s))
	h := addImpersonation.(*constrainedImpersonationHandler)
	h.recordAttempt = func(ctx context.Context, mode, decision string, _ time.Duration) {}
	h.metricsAuthorizer.recordAuthorizationCall = func(mode, decision string, _ time.Duration) {}
	addAuthentication := c.authenticationHandler(addImpersonation)
	addRequestInfo := c.requestInfoHandler(addAuthentication)
	return addRequestInfo
}

// BenchmarkImpersonation 对比受约束代理与旧版代理在全部用例上的性能，
// 用于评估新增的授权/缓存逻辑带来的额外开销。
func BenchmarkImpersonation(b *testing.B) {
	testCases := constrainedImpersonationTestCases()

	type impersonationHandler struct {
		name     string
		fn       func(http.Handler, authorizer.UnconditionalAuthorizer, runtime.NegotiatedSerializer) http.Handler
		isLegacy bool
	}

	impersonators := []impersonationHandler{
		{
			name: "constrained",
			fn:   WithConstrainedImpersonation,
		},
		{
			name:     "legacy",
			fn:       WithImpersonation,
			isLegacy: true,
		},
	}

	for _, imp := range impersonators {
		b.Run(imp.name, func(b *testing.B) {
			for _, tc := range testCases {
				b.Run(tc.name, func(b *testing.B) {
					test := &constrainedImpersonationTest{t: b}
					handler := test.benchmarkHandler(imp.fn)

					server := httptest.NewServer(handler)
					defer server.Close()

					baseTransport := &http.Transport{
						DisableKeepAlives: true,
					}

					b.ResetTimer()
					for b.Loop() {
						for _, r := range tc.requests {
							resp := doImpersonationRequest(b, baseTransport, server.URL, r, benchmarkExpectedCode(r, imp.isLegacy))
							_ = resp.Body.Close()
						}
					}
					b.StopTimer()
				})
			}
		})
	}
}

// benchmarkExpectedCode 修正旧版代理的期望状态码：
// WithImpersonation 不认识受约束的 impersonate:* 动词，因此这些请求会被拒绝。
func benchmarkExpectedCode(r testRequest, isLegacy bool) int {
	if isLegacy && r.expectedCode == http.StatusOK && r.requestor.Name != "legacy-impersonater" {
		// legacy WithImpersonation denies any requestor that only has constrained impersonation verbs
		return http.StatusForbidden
	}
	return r.expectedCode
}

// doImpersonationRequest 使用 client-go 的 impersonating transport 发送请求，
// 并断言响应状态码；返回的响应体由调用方读取并关闭。
func doImpersonationRequest(tb testing.TB, baseTransport http.RoundTripper, url string, r testRequest, expectedCode int) *http.Response {
	tb.Helper()

	client := &http.Client{
		Transport: &testRoundTripper{
			user:        r.requestor,
			requestInfo: r.request,
			delegate: transport.NewImpersonatingRoundTripper(
				transport.ImpersonationConfig{
					UserName: r.impersonatedUser.Name,
					UID:      r.impersonatedUser.UID,
					Groups:   r.impersonatedUser.Groups,
					Extra:    r.impersonatedUser.Extra,
				},
				baseTransport,
			),
		},
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(tb, err)

	resp, err := client.Do(req)
	require.NoError(tb, err)
	require.Equal(tb, expectedCode, resp.StatusCode)

	return resp
}
