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

package authorizerfactory

import (
	"context"
	"errors"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

var _ = authorizer.Authorizer(alwaysAllowAuthorizer{})
var _ = authorizer.RuleResolver(alwaysAllowAuthorizer{})

// alwaysAllowAuthorizer 是 authorizer.Attributes 的一种实现，
// 对任何授权请求都返回允许。
// 适用于测试或以开放方式使用 kubernetes 的场景。
type alwaysAllowAuthorizer struct{}

// Authorize 无条件允许；调用方应只把它用于测试或显式开放的场景。
func (alwaysAllowAuthorizer) Authorize(ctx context.Context, a authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
	return authorizer.DecisionAllow, "", nil
}

// ConditionsAwareAuthorize 不感知 conditions，只转换 Authorize 决策。
func (a alwaysAllowAuthorizer) ConditionsAwareAuthorize(ctx context.Context, attrs authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return authorizer.ConditionsAwareDecisionFromParts(a.Authorize(ctx, attrs))
}

// 此授权器不支持 EvaluateConditions。
func (alwaysAllowAuthorizer) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
}

// RulesFor 报告"可执行一切"，与 Authorize 的始终允许语义保持一致。
func (alwaysAllowAuthorizer) RulesFor(ctx context.Context, user user.Info, namespace string) ([]authorizer.ResourceRuleInfo, []authorizer.NonResourceRuleInfo, bool, error) {
	rrInfo := []authorizer.ResourceRuleInfo{
		&authorizer.DefaultResourceRuleInfo{
			Verbs:     []string{"*"},
			APIGroups: []string{"*"},
			Resources: []string{"*"},
		},
	}
	nrrInfo := []authorizer.NonResourceRuleInfo{
		&authorizer.DefaultNonResourceRuleInfo{
			Verbs:           []string{"*"},
			NonResourceURLs: []string{"*"},
		},
	}
	return rrInfo, nrrInfo, false, nil
}

// NewAlwaysAllowAuthorizer 返回始终允许的授权器。
func NewAlwaysAllowAuthorizer() *alwaysAllowAuthorizer {
	return new(alwaysAllowAuthorizer)
}

var _ = authorizer.Authorizer(alwaysDenyAuthorizer{})
var _ = authorizer.RuleResolver(alwaysDenyAuthorizer{})

// alwaysDenyAuthorizer 是 authorizer.Attributes 的一种实现，
// 对任何授权请求都返回拒绝。
// 适用于单元测试中强制操作被禁止的场景。
type alwaysDenyAuthorizer struct{}

// Authorize 返回 NoOpinion 而非 Deny：在授权器链中这意味着"我不表态"，
// 若链上再无其他授权器，最终结果会被当作拒绝。
func (alwaysDenyAuthorizer) Authorize(ctx context.Context, a authorizer.Attributes) (decision authorizer.Decision, reason string, err error) {
	return authorizer.DecisionNoOpinion, "Everything is forbidden.", nil
}

// ConditionsAwareAuthorize 不感知 conditions，只转换 Authorize 决策。
func (d alwaysDenyAuthorizer) ConditionsAwareAuthorize(ctx context.Context, attrs authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return authorizer.ConditionsAwareDecisionFromParts(d.Authorize(ctx, attrs))
}

// 此授权器不支持 EvaluateConditions。
func (alwaysDenyAuthorizer) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
}

// RulesFor 返回空规则集，表示该用户没有任何权限。
func (alwaysDenyAuthorizer) RulesFor(ctx context.Context, user user.Info, namespace string) ([]authorizer.ResourceRuleInfo, []authorizer.NonResourceRuleInfo, bool, error) {
	return []authorizer.ResourceRuleInfo{}, []authorizer.NonResourceRuleInfo{}, false, nil
}

// NewAlwaysDenyAuthorizer 返回始终拒绝的授权器。
func NewAlwaysDenyAuthorizer() *alwaysDenyAuthorizer {
	return new(alwaysDenyAuthorizer)
}

var _ = authorizer.Authorizer(&privilegedGroupAuthorizer{})

// privilegedGroupAuthorizer 对属于指定用户组之一的请求放行，用于 loopback 等特权场景。
type privilegedGroupAuthorizer struct {
	groups []string
}

// ConditionsAwareAuthorize 不感知 conditions，只转换 Authorize 决策。
func (r *privilegedGroupAuthorizer) ConditionsAwareAuthorize(ctx context.Context, attr authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return authorizer.ConditionsAwareDecisionFromParts(r.Authorize(ctx, attr))
}

// 此授权器不支持 EvaluateConditions。
func (r *privilegedGroupAuthorizer) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
}

// Authorize 按用户组成员关系判定；没有用户信息时返回错误而非静默拒绝，
// 以便上层区分"未认证"和"已认证但无权限"。
func (r *privilegedGroupAuthorizer) Authorize(ctx context.Context, attr authorizer.Attributes) (authorizer.Decision, string, error) {
	if attr.GetUser() == nil {
		return authorizer.DecisionNoOpinion, "Error", errors.New("no user on request.")
	}
	for _, attr_group := range attr.GetUser().GetGroups() {
		for _, priv_group := range r.groups {
			if priv_group == attr_group {
				return authorizer.DecisionAllow, "", nil
			}
		}
	}
	return authorizer.DecisionNoOpinion, "", nil
}

// NewPrivilegedGroups 用于 loopback 场景
func NewPrivilegedGroups(groups ...string) *privilegedGroupAuthorizer {
	return &privilegedGroupAuthorizer{
		groups: groups,
	}
}
