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
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
)

// WithFailedAuthenticationAudit 装饰 WithAuthentication 中使用的失败 http.Handler，
// 只记录认证失败的请求。
//
// 未配置审计后端或策略时直接返回原 handler，避免为每个失败请求付出额外开销。
func WithFailedAuthenticationAudit(failedHandler http.Handler, sink audit.Sink, policy audit.PolicyRuleEvaluator) http.Handler {
	if sink == nil || policy == nil {
		return failedHandler
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ac, err := evaluatePolicyAndCreateAuditEvent(req, policy, sink)
		if err != nil {
			// 审计事件创建失败时拒绝继续处理：不能在没有审计记录的情况下响应认证请求。
			utilruntime.HandleError(fmt.Errorf("failed to create audit event: %v", err))
			responsewriters.InternalError(w, req, errors.New("failed to create audit event"))
			return
		}

		if !ac.Enabled() {
			// 当前审计级别不记录该请求，无需装饰 ResponseWriter。
			failedHandler.ServeHTTP(w, req)
			return
		}

		// 把尝试过的认证方式写进状态消息，便于事后排查"为何认证失败"。
		ac.SetEventResponseStatus(&metav1.Status{
			Message: getAuthMethods(req),
		})
		// 用装饰后的 writer 捕获失败响应体/状态码，写入审计事件。
		rw := decorateResponseWriter(req.Context(), w, true)
		failedHandler.ServeHTTP(rw, req)
	})
}

// getAuthMethods 收集请求中出现过的凭据类型，仅用于审计消息，不涉及凭据内容本身。
func getAuthMethods(req *http.Request) string {
	authMethods := []string{}

	if _, _, ok := req.BasicAuth(); ok {
		authMethods = append(authMethods, "basic")
	}

	auth := strings.TrimSpace(req.Header.Get("Authorization"))
	parts := strings.Split(auth, " ")
	if len(parts) > 1 && strings.ToLower(parts[0]) == "bearer" {
		authMethods = append(authMethods, "bearer")
	}

	token := strings.TrimSpace(req.URL.Query().Get("access_token"))
	if len(token) > 0 {
		authMethods = append(authMethods, "access_token")
	}

	if req.TLS != nil && len(req.TLS.PeerCertificates) > 0 {
		authMethods = append(authMethods, "x509")
	}

	if len(authMethods) > 0 {
		return fmt.Sprintf("Authentication failed, attempted: %s", strings.Join(authMethods, ", "))
	}
	return "Authentication failed, no credentials provided"
}
