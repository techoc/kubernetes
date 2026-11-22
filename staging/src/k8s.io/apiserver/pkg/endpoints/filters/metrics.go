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
	"strings"
	"sync"
	"time"

	"k8s.io/apiserver/pkg/authorization/authorizer"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

/*
 * By default, all the following metrics are defined as falling under
 * ALPHA stability level https://github.com/kubernetes/enhancements/blob/master/keps/sig-instrumentation/1209-metrics-stability/kubernetes-control-plane-metrics-stability.md#stability-classes)
 *
 * Promoting the stability level of the metric is a responsibility of the component owner, since it
 * involves explicitly acknowledging support for the metric across multiple releases, in accordance with
 * the metric stability policy.
 */
const (
	// 认证结果标签：成功、凭据不足（failed）与处理出错（error），
	// 区分后两者才能判断是用户问题还是服务端问题。
	successLabel = "success"
	failureLabel = "failure"
	errorLabel   = "error"

	// 鉴权决策标签，与 authorizer.Decision 的四种结果一一对应。
	allowedLabel   = "allowed"
	deniedLabel    = "denied"
	noOpinionLabel = "no-opinion"
)

var (
	// authenticatedUserCounter 按用户名统计成功认证的请求。用户名为压缩
	// 后的类别（见 compressUsername），以控制标签基数。
	authenticatedUserCounter = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Name:           "authenticated_user_requests",
			Help:           "Counter of authenticated requests broken out by username.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"username"},
	)

	// authenticatedAttemptsCounter 统计认证尝试次数及结果分布。
	authenticatedAttemptsCounter = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Name:           "authentication_attempts",
			Help:           "Counter of authenticated attempts.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	// authenticationLatency 记录认证耗时；指数桶覆盖毫秒到数十秒，
	// 适配本地认证与远端 token webhook 两种量级。
	authenticationLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Name:           "authentication_duration_seconds",
			Help:           "Authentication duration in seconds broken out by result.",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	// authorizationAttemptsCounter 统计鉴权尝试次数及决策结果。
	authorizationAttemptsCounter = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Name:           "authorization_attempts_total",
			Help:           "Counter of authorization attempts broken down by result. It can be either 'allowed', 'denied', 'no-opinion' or 'error'.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)

	// authorizationLatency 记录鉴权耗时；RBAC 本地计算通常微秒级，
	// 外部 webhook 则可能到秒级。
	authorizationLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Name:           "authorization_duration_seconds",
			Help:           "Authorization duration in seconds broken out by result.",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"result"},
	)
)

var registerMetricsOnce sync.Once

// RegisterMetrics registers the authentication and authorization filter metrics
// with the legacy registry. It is invoked when the authentication/authorization
// filters are installed into the handler chain (rather than from an init()
// function), so that metric feature gates such as NativeHistograms have been
// applied before these histogram metrics are created.
//
// 之所以不在 init() 中注册：histogram 的创建方式依赖 NativeHistograms
// 等特性门控，而门控在 init 阶段可能尚未确定，延迟到过滤器安装时注册
// 才能保证配置生效。sync.Once 保证重复安装链时不会重复注册。
func RegisterMetrics() {
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(authenticatedUserCounter)
		legacyregistry.MustRegister(authenticatedAttemptsCounter)
		legacyregistry.MustRegister(authenticationLatency)
		legacyregistry.MustRegister(authorizationAttemptsCounter)
		legacyregistry.MustRegister(authorizationLatency)
	})
}

// recordAuthorizationMetrics 根据鉴权决策与错误记录次数和耗时。错误优先于
// no-opinion 归为 error，因为出错时决策值本身不可信。
func recordAuthorizationMetrics(ctx context.Context, authorized authorizer.Decision, err error, authStart time.Time, authFinish time.Time) {
	var resultLabel string

	switch {
	case authorized == authorizer.DecisionAllow:
		resultLabel = allowedLabel
	case err != nil:
		resultLabel = errorLabel
	case authorized == authorizer.DecisionDeny:
		resultLabel = deniedLabel
	case authorized == authorizer.DecisionNoOpinion:
		resultLabel = noOpinionLabel
	}

	authorizationAttemptsCounter.WithContext(ctx).WithLabelValues(resultLabel).Inc()
	authorizationLatency.WithContext(ctx).WithLabelValues(resultLabel).Observe(authFinish.Sub(authStart).Seconds())
}

// recordAuthenticationMetrics 记录认证结果与耗时。注意：认证成功但返回的
// audience 不被 API server 接受时也记为 error，而不是 success，因为对
// 调用方而言这次认证实际不可用（例如 token 签发给别的集群）。
func recordAuthenticationMetrics(ctx context.Context, resp *authenticator.Response, ok bool, err error, apiAudiences authenticator.Audiences, authStart time.Time, authFinish time.Time) {
	var resultLabel string

	switch {
	case err != nil || (resp != nil && !audiencesAreAcceptable(apiAudiences, resp.Audiences)):
		resultLabel = errorLabel
	case !ok:
		resultLabel = failureLabel
	default:
		resultLabel = successLabel
		authenticatedUserCounter.WithContext(ctx).WithLabelValues(compressUsername(resp.User.GetName())).Inc()
	}

	authenticatedAttemptsCounter.WithContext(ctx).WithLabelValues(resultLabel).Inc()
	authenticationLatency.WithContext(ctx).WithLabelValues(resultLabel).Observe(authFinish.Sub(authStart).Seconds())
}

// compressUsername maps all possible usernames onto a small set of categories
// of usernames. This is done both to limit the cardinality of the
// authorized_user_requests metric, and to avoid pushing actual usernames in the
// metric.
//
// compressUsername 把用户名压缩为少量类别：既限制指标标签基数，又避免
// 把真实用户名（可能是邮箱）写入指标。内部身份保留原名，便于区分系统
// 组件流量。
func compressUsername(username string) string {
	switch {
	// Known internal identities.
	// 这些身份由 kube-apiserver 内部产生，名称固定且基数有限。
	case username == "admin" ||
		username == "client" ||
		username == "kube_proxy" ||
		username == "kubelet" ||
		username == "system:serviceaccount:kube-system:default":
		return username
	// Probably an email address.
	// 含 @ 的多为用户邮箱，统一归为 email_id 以免泄露。
	case strings.Contains(username, "@"):
		return "email_id"
	// Anything else (custom service accounts, custom external identities, etc.)
	default:
		return "other"
	}
}
