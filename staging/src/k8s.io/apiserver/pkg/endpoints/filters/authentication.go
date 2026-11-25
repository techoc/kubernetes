/*
Copyright 2014 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/authenticatorfactory"
	"k8s.io/apiserver/pkg/authentication/request/headerrequest"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
)

type authenticationRecordMetricsFunc func(context.Context, *authenticator.Response, bool, error, authenticator.Audiences, time.Time, time.Time)

// WithAuthentication 创建一个 http handler，尝试把给定请求认证为用户，并把认证到的
// 用户存入该请求的上下文。认证失败或出错时使用 failed handler。成功时，请求上的
// "Authorization" 头会被移除，然后调用 handler 处理请求。
func WithAuthentication(handler http.Handler, auth authenticator.Request, failed http.Handler, apiAuds authenticator.Audiences, requestHeaderConfig *authenticatorfactory.RequestHeaderConfig) http.Handler {
	RegisterMetrics()
	return withAuthentication(handler, auth, failed, apiAuds, requestHeaderConfig, recordAuthenticationMetrics)
}

// withAuthentication 是 WithAuthentication 的可测试实现，metrics 作为参数注入。
func withAuthentication(handler http.Handler, auth authenticator.Request, failed http.Handler, apiAuds authenticator.Audiences, requestHeaderConfig *authenticatorfactory.RequestHeaderConfig, metrics authenticationRecordMetricsFunc) http.Handler {
	if auth == nil {
		klog.Warning("Authentication is disabled")
		return handler
	}
	// 前端代理（front-proxy）的标准头部。即使本 apiserver 未启用请求头认证，
	// 也必须在认证成功后清除它们，防止恶意客户端伪造身份头被下游信任。
	standardRequestHeaderConfig := &authenticatorfactory.RequestHeaderConfig{
		UsernameHeaders:     headerrequest.StaticStringSlice{"X-Remote-User"},
		UIDHeaders:          headerrequest.StaticStringSlice{"X-Remote-Uid"},
		GroupHeaders:        headerrequest.StaticStringSlice{"X-Remote-Group"},
		ExtraHeaderPrefixes: headerrequest.StaticStringSlice{"X-Remote-Extra-"},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		authenticationStart := time.Now()

		// 把 API 的受众集合放进上下文，供各认证器（尤其是 JWT）校验 token 的 aud。
		if len(apiAuds) > 0 {
			req = req.WithContext(authenticator.WithAudiences(req.Context(), apiAuds))
		}
		resp, ok, err := auth.AuthenticateRequest(req)
		authenticationFinish := time.Now()
		genericapirequest.TrackAuthenticationLatency(req.Context(), authenticationFinish.Sub(authenticationStart))
		defer func() {
			metrics(req.Context(), resp, ok, err, apiAuds, authenticationStart, authenticationFinish)
		}()
		if err != nil || !ok {
			if err != nil {
				klog.ErrorS(err, "Unable to authenticate the request")
			}
			failed.ServeHTTP(w, req)
			return
		}

		// 认证器可能忽略受众限制，这里统一再校验一次，避免 token 被跨受众重放。
		if !audiencesAreAcceptable(apiAuds, resp.Audiences) {
			err = fmt.Errorf("unable to match the audience: %v , accepted: %v", resp.Audiences, apiAuds)
			klog.Error(err)
			failed.ServeHTTP(w, req)
			return
		}

		// 认证成功之后就不再需要 authorization 头。
		req.Header.Del("Authorization")

		// 删除标准前置代理头
		headerrequest.ClearAuthenticationHeaders(
			req.Header,
			standardRequestHeaderConfig.UsernameHeaders,
			standardRequestHeaderConfig.UIDHeaders,
			standardRequestHeaderConfig.GroupHeaders,
			standardRequestHeaderConfig.ExtraHeaderPrefixes,
		)

		// 同时删除自定义的前置代理头
		if requestHeaderConfig != nil {
			headerrequest.ClearAuthenticationHeaders(
				req.Header,
				requestHeaderConfig.UsernameHeaders,
				requestHeaderConfig.UIDHeaders,
				requestHeaderConfig.GroupHeaders,
				requestHeaderConfig.ExtraHeaderPrefixes,
			)
		}

		// http2 是开销较大且容易被滥用的协议，
		// 可参考 CVE-2023-44487 和 CVE-2023-39325。
		// 不要让未认证的客户端长期保持这类连接
		// （那样相当于把它们降级为禁用 keep-alive 的 http1
		// 性能水平）。
		if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.UnauthenticatedHTTP2DOSMitigation) && req.ProtoMajor == 2 && isAnonymousUser(resp.User) {
			// 把该连接限制为只服务当前这一个请求，
			// 随后发送 GOAWAY 并关闭 TCP 连接
			// https://github.com/golang/net/commit/97aa3a539ec716117a9d15a4659a911f50d13c3c
			w.Header().Set("Connection", "close")
		}
		// 认证成功：把 user.Info 写入请求上下文，后续授权与审计从这里读取。
		req = req.WithContext(genericapirequest.WithUser(req.Context(), resp.User))
		handler.ServeHTTP(w, req)
	})
}

// Unauthorized 返回认证失败时使用的处理器，统一输出 401。
func Unauthorized(s runtime.NegotiatedSerializer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// http2 是开销较大且容易被滥用的协议，
		// 可参考 CVE-2023-44487 和 CVE-2023-39325。
		// 不要让未认证的客户端长期保持这类连接
		// （那样相当于把它们降级为禁用 keep-alive 的 http1
		// 性能水平）。
		if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.UnauthenticatedHTTP2DOSMitigation) && req.ProtoMajor == 2 {
			// 把该连接限制为只服务当前这一个请求，
			// 随后发送 GOAWAY 并关闭 TCP 连接
			// https://github.com/golang/net/commit/97aa3a539ec716117a9d15a4659a911f50d13c3c
			w.Header().Set("Connection", "close")
		}
		ctx := req.Context()
		// 错误的 GV 需要从 RequestInfo 中取得，才能按请求的组版本序列化 Status。
		requestInfo, found := genericapirequest.RequestInfoFrom(ctx)
		if !found {
			responsewriters.InternalError(w, req, errors.New("no RequestInfo found in the context"))
			return
		}

		gv := schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}
		responsewriters.ErrorNegotiated(apierrors.NewUnauthorized("Unauthorized"), s, gv, w, req)
	})
}

// audiencesAreAcceptable 判断响应受众与 API 受众是否有交集。
//
// 任一方为空都视为可接受：空 API 受众表示未配置限制，空响应受众表示认证器
// 未声明受众（例如客户端证书），此时不应因为"无法比较"而拒绝。
func audiencesAreAcceptable(apiAuds, responseAudiences authenticator.Audiences) bool {
	if len(apiAuds) == 0 || len(responseAudiences) == 0 {
		return true
	}

	return len(apiAuds.Intersect(responseAudiences)) > 0
}

// isAnonymousUser 同时检查用户名与 unauthenticated 组，兼容只设置其中一项的认证器。
func isAnonymousUser(u user.Info) bool {
	if u.GetName() == user.Anonymous {
		return true
	}
	for _, group := range u.GetGroups() {
		if group == user.AllUnauthenticated {
			return true
		}
	}
	return false
}
