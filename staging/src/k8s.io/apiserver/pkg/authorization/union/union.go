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

// Package union 实现把多个子授权器组合起来的授权器。
// union 授权器依次调用各子授权器，返回第一个 Allow 或 Deny 决策。
// 若子授权器返回 NoOpinion，则继续下一个；若已是最后一个，则以 NoOpinion 作为聚合结果。
// 也就是说，union 授权器在聚合决策的同时，支持子授权器的短路允许与拒绝。
//
// 作为聚合结果返回。也就是说，union 授权器在聚合决策的同时，
// 支持子授权器的短路允许与拒绝。
// subauthorizers.
package union

import (
	"context"
	"errors"
	"fmt"
	"strings"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

var _ = authorizer.Authorizer(unionAuthzHandler{})

// unionAuthzHandler 针对一串 authorizer.Authorizer 做授权。
type unionAuthzHandler struct {
	chain            []NamedAuthorizer
	nameToAuthorizer map[string]authorizer.Authorizer
}

// NamedAuthorizer 是 (string, Authorizer) 二元组，为每个 Authorizer 关联一个名字。
// 名字在同一个 union 授权器内必须唯一，且 Authorizer 不能为 nil。
// 名字在同一个 union 授权器内必须唯一，且 Authorizer 不能为 nil。
type NamedAuthorizer struct {
	AuthorizerName string
	Authorizer     authorizer.Authorizer
}

// New 返回一个针对一串 authorizer.Authorizer 做授权的授权器。
//
// 名称必须唯一，因为 ConditionsAwareAuthorize 之后需要按名称找回对应的
// authorizer 来求值条件；重名会导致条件被错误地交给另一个授权器处理。
func New(authorizationHandlers ...NamedAuthorizer) (authorizer.Authorizer, error) {
	nameToAuthorizer := make(map[string]authorizer.Authorizer, len(authorizationHandlers))
	for _, handler := range authorizationHandlers {
		if len(handler.AuthorizerName) == 0 {
			return nil, fmt.Errorf("empty AuthorizerName in union.New is invalid")
		}

		if _, duplicateName := nameToAuthorizer[handler.AuthorizerName]; duplicateName {
			return nil, fmt.Errorf("duplicate AuthorizerName %q in union.New", handler.AuthorizerName)
		}

		nameToAuthorizer[handler.AuthorizerName] = handler.Authorizer
	}
	return unionAuthzHandler{
		chain:            authorizationHandlers,
		nameToAuthorizer: nameToAuthorizer,
	}, nil
}

// Authorize 针对一串 authorizer.Authorizer 做授权；成功返回 nil，失败返回错误。
//
// 短路语义：第一个返回 Allow 或 Deny 的授权器决定结果；NoOpinion 继续向后。
// 全部为 NoOpinion 时聚合各授权器的 reason 与 error 一并返回，便于诊断。
func (authzHandler unionAuthzHandler) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	var (
		errlist    []error
		reasonlist []string
	)

	for _, currAuthzHandler := range authzHandler.chain {
		decision, reason, err := currAuthzHandler.Authorizer.Authorize(ctx, a)

		if err != nil {
			errlist = append(errlist, err)
		}
		if len(reason) != 0 {
			// 带授权器名前缀，避免多个 reason 合并后无法区分来源。
			reasonlist = append(reasonlist, fmt.Sprintf("%s: %s", currAuthzHandler.AuthorizerName, reason))
		}
		switch decision {
		case authorizer.DecisionAllow, authorizer.DecisionDeny:
			return decision, reason, err
		case authorizer.DecisionNoOpinion:
			// 继续处理下一个授权器
		}
	}

	return authorizer.DecisionNoOpinion, strings.Join(reasonlist, "\n"), utilerrors.NewAggregate(errlist)
}

// ConditionsAwareAuthorize 使用子授权器链对给定 attributes 做条件感知授权；
// 若子授权器返回条件式决策，可能返回 Union 类型的 ConditionsAwareDecision。
func (authzHandler unionAuthzHandler) ConditionsAwareAuthorize(ctx context.Context, a authorizer.Attributes) authorizer.ConditionsAwareDecision {
	var decisions authorizer.ConditionsAwareDecisionUnion

	for _, currAuthzHandler := range authzHandler.chain {
		// 前置条件：此前见过的叶子决策均为 NoOpinion 或 ConditionsMap 类型。

		// 调用子授权器的条件感知方法，并把决策加入切片，无论其类型如何。
		// 因为后续 EvaluateConditions 依赖切片中的决策索引，
		// 来关联决策与应当求值它的授权器（若需要）。
		//
		decision := currAuthzHandler.Authorizer.ConditionsAwareAuthorize(ctx, a)
		decisions.Add(currAuthzHandler.AuthorizerName, decision)

		// 出现任何 Allow/Deny 决策叶子时无需继续遍历链条；
		// 无条件的 Allow/Deny 与 Authorize 的短路行为保持一致。
		if decision.ContainsUnconditionalAllowOrDeny() {
			return decisions.ToDecision()
		}
		// => 所有叶子均为 NoOpinion 或 ConditionsMap，继续处理下一个授权器
	}

	// 走到这里说明所有叶子决策均为 NoOpinion 或 ConditionsMap 类型。
	// 若全部为 NoOpinion，构造函数会折叠为单个 NoOpinion 决策。
	return decisions.ToDecision()
}

// EvaluateConditions 使用 union 授权器的子授权器链和给定数据，
// 求值 ConditionsAwareAuthorize 返回的 Union 类型未求值决策。
func (authzHandler unionAuthzHandler) EvaluateConditions(ctx context.Context, unevaluatedDecision authorizer.ConditionsAwareDecision, data authorizer.ConditionsData) (authorizer.Decision, string, error) {
	// 不应发生此情况：授权器只会对自身返回的 union 未求值决策回调。
	// 调用方也不应对 Allow/Deny/NoOpinion 决策调用 EvaluateConditions。
	if !unevaluatedDecision.IsUnion() {
		return unevaluatedDecision.FailureDecision(), "failed closed", errors.New("the union authorizer can only evaluate union ConditionsAwareDecisions")
	}

	// 求值流程返回无条件结果后，这里与 Authorize() 的逻辑一一对应。
	var (
		errlist    []error
		reasonlist []string
	)

	for currentAuthorizerName, unevaluatedSubDecision := range unevaluatedDecision.UnionedDecisions() {
		// 前置条件：此前见过的叶子决策均已求值为 NoOpinion，或属于无法识别的模式。

		var decision authorizer.Decision
		var reason string
		var err error
		switch {
		// 若在 union 链中遇到 Allow 或 Deny，即为最终答案。
		case unevaluatedSubDecision.IsDeny():
			return authorizer.DecisionDeny, unevaluatedSubDecision.Reason(), unevaluatedSubDecision.Error()
		case unevaluatedSubDecision.IsAllow():
			return authorizer.DecisionAllow, unevaluatedSubDecision.Reason(), unevaluatedSubDecision.Error()
		// NoOpinion 无法求值，但仍需保存其 reason 和 error。
		case unevaluatedSubDecision.IsNoOpinion():
			decision, reason, err = authorizer.DecisionNoOpinion, unevaluatedSubDecision.Reason(), unevaluatedSubDecision.Error()
		// ConditionsMap 或 Union 类型由其对应的授权器求值。
		case unevaluatedSubDecision.IsConditionsMap(), unevaluatedSubDecision.IsUnion():
			decision, reason, err = authzHandler.evaluateConditions(ctx, currentAuthorizerName, unevaluatedSubDecision, data)
			// 防御不可能的求值结果：求值结果必须是该子决策声明过的可能结果之一，否则 fail closed。
			if !unevaluatedSubDecision.PossibleDecisions().Has(decision) {
				decision, reason, err = unevaluatedSubDecision.FailureDecision(), "failed closed", fmt.Errorf("evaluated to decision %s, but only %v were possible", decision, sets.List(unevaluatedSubDecision.PossibleDecisions()))
			}
		default:
			return unevaluatedDecision.FailureDecision(), "failed closed", fmt.Errorf("saw unrecognized subDecision in union authorizer EvaluateConditions: %s", unevaluatedSubDecision)
		}

		switch decision {
		case authorizer.DecisionAllow, authorizer.DecisionDeny:
			return decision, reason, err
		case authorizer.DecisionNoOpinion:
			// 继续处理下一个授权器，保存错误与 reason 供后续使用。

			if err != nil {
				errlist = append(errlist, err)
			}
			if len(reason) != 0 {
				reasonlist = append(reasonlist, fmt.Sprintf("%s: %s", currentAuthorizerName, reason))
			}
		}
	}

	return authorizer.DecisionNoOpinion, strings.Join(reasonlist, "\n"), utilerrors.NewAggregate(errlist)
}

// evaluateConditions 按名称找回子授权器并委托其求值条件；找不到时 fail closed。
func (authzHandler unionAuthzHandler) evaluateConditions(ctx context.Context, authorizerName string, unevaluatedSubDecision authorizer.ConditionsAwareDecision, data authorizer.ConditionsData) (authorizer.Decision, string, error) {
	authorizer, ok := authzHandler.nameToAuthorizer[authorizerName]
	if !ok {
		return unevaluatedSubDecision.FailureDecision(), "failed closed", fmt.Errorf("couldn't find authorizer with name: %q", authorizerName)
	}
	return authorizer.EvaluateConditions(ctx, unevaluatedSubDecision, data)
}

// unionAuthzRulesHandler 针对一串 authorizer.RuleResolver 做授权。
type unionAuthzRulesHandler []authorizer.RuleResolver

// NewRuleResolvers 返回一个针对一串 authorizer.Authorizer 做授权的授权器。
func NewRuleResolvers(authorizationHandlers ...authorizer.RuleResolver) authorizer.RuleResolver {
	return unionAuthzRulesHandler(authorizationHandlers)
}

// RulesFor 针对一串 authorizer.RuleResolver 查询规则；成功返回 nil，失败返回错误。
//
// 与 Authorize 不同，规则查询需要汇总所有子授权器的结果，无法短路；
// 任一子授权器报告不完整（incomplete）时整体即为不完整。
func (authzHandler unionAuthzRulesHandler) RulesFor(ctx context.Context, user user.Info, namespace string) ([]authorizer.ResourceRuleInfo, []authorizer.NonResourceRuleInfo, bool, error) {
	var (
		errList              []error
		resourceRulesList    []authorizer.ResourceRuleInfo
		nonResourceRulesList []authorizer.NonResourceRuleInfo
	)
	incompleteStatus := false

	for _, currAuthzHandler := range authzHandler {
		resourceRules, nonResourceRules, incomplete, err := currAuthzHandler.RulesFor(ctx, user, namespace)

		if incomplete {
			incompleteStatus = true
		}
		if err != nil {
			errList = append(errList, err)
		}
		if len(resourceRules) > 0 {
			resourceRulesList = append(resourceRulesList, resourceRules...)
		}
		if len(nonResourceRules) > 0 {
			nonResourceRulesList = append(nonResourceRulesList, nonResourceRules...)
		}
	}

	return resourceRulesList, nonResourceRulesList, incompleteStatus, utilerrors.NewAggregate(errList)
}
