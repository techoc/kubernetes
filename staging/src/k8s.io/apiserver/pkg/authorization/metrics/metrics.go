/*
Copyright 2024 The Kubernetes Authors.

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

// Package metrics 为授权器提供指标埋点包装：统计每个授权器类型/名称的
// 终态决策（allowed/denied/conditional），NoOpinion 不计数，因为它不
// 代表最终结果。
package metrics

import (
	"context"
	"sync"

	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	namespace = "apiserver"
	subsystem = "authorization"
)

var (
	authorizationDecisionsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "decisions_total",
			Help:           "Total number of terminal decisions made by an authorizer split by authorizer type, name, and decision.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"type", "name", "decision"},
	)
)

var registerMetrics sync.Once

// RegisterMetrics 幂等注册指标；多个授权器共享同一个计数器。
func RegisterMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(authorizationDecisionsTotal)
	})
}

// ResetMetricsForTest 清空计数器，仅供测试使用。
func ResetMetricsForTest() {
	authorizationDecisionsTotal.Reset()
}

// RecordAuthorizationDecision 记录一次终态决策。
func RecordAuthorizationDecision(authorizerType, authorizerName, decision string) {
	authorizationDecisionsTotal.WithLabelValues(authorizerType, authorizerName, decision).Inc()
}

// InstrumentedAuthorizer 包装授权器并自动注册指标，返回的包装器与原
// 授权器行为完全一致，只是多了一次计数。
func InstrumentedAuthorizer(authorizerType string, authorizerName string, delegate authorizer.Authorizer) authorizer.Authorizer {
	RegisterMetrics()
	return &instrumentedAuthorizer{
		authorizerType: string(authorizerType),
		authorizerName: authorizerName,
		delegate:       delegate,
	}
}

var _ = authorizer.Authorizer(&instrumentedAuthorizer{})

// instrumentedAuthorizer 委托给真实授权器，并在返回后记录决策。
type instrumentedAuthorizer struct {
	authorizerType string
	authorizerName string
	delegate       authorizer.Authorizer
}

// Authorize 委托并记录终态决策；NoOpinion 表示“未表态”，不计入指标，
// 以免把多授权器串联时的中间结果误当作最终结果。
func (a *instrumentedAuthorizer) Authorize(ctx context.Context, attributes authorizer.Attributes) (authorizer.Decision, string, error) {
	decision, reason, err := a.delegate.Authorize(ctx, attributes)
	switch decision {
	case authorizer.DecisionNoOpinion:
		// 非终结状态，不上报
	case authorizer.DecisionAllow:
		// 与 SubjectAccessReview 的 status.allowed 字段名一致
		RecordAuthorizationDecision(a.authorizerType, a.authorizerName, "allowed")
	case authorizer.DecisionDeny:
		// 与 SubjectAccessReview 的 status.denied 字段名一致
		RecordAuthorizationDecision(a.authorizerType, a.authorizerName, "denied")
	default:
		RecordAuthorizationDecision(a.authorizerType, a.authorizerName, "unknown")
	}
	return decision, reason, err
}

// ConditionsAwareAuthorize 委派给被包装的授权器。
func (a *instrumentedAuthorizer) ConditionsAwareAuthorize(ctx context.Context, attributes authorizer.Attributes) authorizer.ConditionsAwareDecision {
	decision := a.delegate.ConditionsAwareAuthorize(ctx, attributes)
	switch {
	case decision.IsNoOpinion():
		// 非终结状态，不上报
	case decision.IsAllow():
		// 与 SubjectAccessReview 的 status.allowed 字段名一致
		RecordAuthorizationDecision(a.authorizerType, a.authorizerName, "allowed")
	case decision.IsDeny():
		// 与 SubjectAccessReview 的 status.denied 字段名一致
		RecordAuthorizationDecision(a.authorizerType, a.authorizerName, "denied")
	default:
		// ConditionsAwareDecision 保证不存在 Allow/Deny/NoOpinion/ConditionsMap/Union
		// 之外的状态，后两者属于条件决策。
		//
		RecordAuthorizationDecision(a.authorizerType, a.authorizerName, "conditional")
	}
	return decision
}

// EvaluateConditions 委派给被包装的授权器，并像 Authorize 一样记录指标。
func (a *instrumentedAuthorizer) EvaluateConditions(ctx context.Context, unevaluatedDecision authorizer.ConditionsAwareDecision, data authorizer.ConditionsData) (authorizer.Decision, string, error) {
	decision, reason, err := a.delegate.EvaluateConditions(ctx, unevaluatedDecision, data)
	switch decision {
	case authorizer.DecisionNoOpinion:
		// 非终结状态，不上报
	case authorizer.DecisionAllow:
		// 与 SubjectAccessReview 的 status.allowed 字段名一致
		RecordAuthorizationDecision(a.authorizerType, a.authorizerName, "allowed")
	case authorizer.DecisionDeny:
		// 与 SubjectAccessReview 的 status.denied 字段名一致
		RecordAuthorizationDecision(a.authorizerType, a.authorizerName, "denied")
	default:
		RecordAuthorizationDecision(a.authorizerType, a.authorizerName, "unknown")
	}
	return decision, reason, err
}
