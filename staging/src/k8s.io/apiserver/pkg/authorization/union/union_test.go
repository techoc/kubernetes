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

package union

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// mockAuthzHandler 返回固定的决策与错误，用于验证 union 的短路与合并逻辑。
type mockAuthzHandler struct {
	decision authorizer.Decision
	err      error
}

// Authorize 返回预设的决策与错误。
func (mock *mockAuthzHandler) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	return mock.decision, "", mock.err
}

// ConditionsAwareAuthorize 不是条件感知实现，把 Authorize 的决策转换过去。
func (mock *mockAuthzHandler) ConditionsAwareAuthorize(ctx context.Context, a authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return authorizer.ConditionsAwareDecisionFromParts(mock.Authorize(ctx, a))
}

// EvaluateConditions 不被该授权器支持。
func (*mockAuthzHandler) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
}

// mustNewIndexed 为每个授权器自动生成名称并构造 union 授权器。
// 适用于不关心具体名称的测试。
func mustNewIndexed(t *testing.T, handlers ...authorizer.Authorizer) authorizer.Authorizer {
	t.Helper()
	named := make([]NamedAuthorizer, len(handlers))
	for i, h := range handlers {
		named[i] = NamedAuthorizer{AuthorizerName: fmt.Sprintf("authz%d", i), Authorizer: h}
	}
	u, err := New(named...)
	if err != nil {
		t.Fatalf("union.New returned an unexpected error: %v", err)
	}
	return u
}

// TestAuthorizationSecondPasses 验证第一个授权器 NoOpinion 时继续询问第二个。
func TestAuthorizationSecondPasses(t *testing.T) {
	handler1 := &mockAuthzHandler{decision: authorizer.DecisionNoOpinion}
	handler2 := &mockAuthzHandler{decision: authorizer.DecisionAllow}
	authzHandler := mustNewIndexed(t, handler1, handler2)

	authorized, _, _ := authzHandler.Authorize(context.Background(), nil)
	if authorized != authorizer.DecisionAllow {
		t.Errorf("Unexpected authorization failure")
	}
}

// TestAuthorizationFirstPasses 验证第一个授权器 Allow 即返回，不再询问后续授权器。
func TestAuthorizationFirstPasses(t *testing.T) {
	handler1 := &mockAuthzHandler{decision: authorizer.DecisionAllow}
	handler2 := &mockAuthzHandler{decision: authorizer.DecisionNoOpinion}
	authzHandler := mustNewIndexed(t, handler1, handler2)

	authorized, _, _ := authzHandler.Authorize(context.Background(), nil)
	if authorized != authorizer.DecisionAllow {
		t.Errorf("Unexpected authorization failure")
	}
}

// TestAuthorizationNonePasses 验证所有授权器都 NoOpinion 时不会误判为 Allow。
func TestAuthorizationNonePasses(t *testing.T) {
	handler1 := &mockAuthzHandler{decision: authorizer.DecisionNoOpinion}
	handler2 := &mockAuthzHandler{decision: authorizer.DecisionNoOpinion}
	authzHandler := mustNewIndexed(t, handler1, handler2)

	authorized, _, _ := authzHandler.Authorize(context.Background(), nil)
	if authorized == authorizer.DecisionAllow {
		t.Errorf("Expected failed authorization")
	}
}

// TestAuthorizationError 验证授权器返回错误时 union 会把错误向上传递。
func TestAuthorizationError(t *testing.T) {
	handler1 := &mockAuthzHandler{err: fmt.Errorf("foo")}
	handler2 := &mockAuthzHandler{err: fmt.Errorf("foo")}
	authzHandler := mustNewIndexed(t, handler1, handler2)

	_, _, err := authzHandler.Authorize(context.Background(), nil)
	if err == nil {
		t.Errorf("Expected error: %v", err)
	}
}

// mockAuthzRuleHandler 提供预设的规则列表，用于测试 RuleResolvers 的合并。
type mockAuthzRuleHandler struct {
	resourceRules    []authorizer.ResourceRuleInfo
	nonResourceRules []authorizer.NonResourceRuleInfo
	err              error
}

// RulesFor 返回预设规则；设置 err 时返回错误。
func (mock *mockAuthzRuleHandler) RulesFor(ctx context.Context, user user.Info, namespace string) ([]authorizer.ResourceRuleInfo, []authorizer.NonResourceRuleInfo, bool, error) {
	if mock.err != nil {
		return []authorizer.ResourceRuleInfo{}, []authorizer.NonResourceRuleInfo{}, false, mock.err
	}
	return mock.resourceRules, mock.nonResourceRules, false, nil
}

// TestAuthorizationResourceRules 验证 RuleResolvers 按顺序拼接各授权器的资源规则。
func TestAuthorizationResourceRules(t *testing.T) {
	handler1 := &mockAuthzRuleHandler{
		resourceRules: []authorizer.ResourceRuleInfo{
			&authorizer.DefaultResourceRuleInfo{
				Verbs:     []string{"*"},
				APIGroups: []string{"*"},
				Resources: []string{"bindings"},
			},
			&authorizer.DefaultResourceRuleInfo{
				Verbs:     []string{"get", "list", "watch"},
				APIGroups: []string{"*"},
				Resources: []string{"*"},
			},
		},
	}
	handler2 := &mockAuthzRuleHandler{
		resourceRules: []authorizer.ResourceRuleInfo{
			&authorizer.DefaultResourceRuleInfo{
				Verbs:     []string{"*"},
				APIGroups: []string{"*"},
				Resources: []string{"events"},
			},
			&authorizer.DefaultResourceRuleInfo{
				Verbs:         []string{"get"},
				APIGroups:     []string{"*"},
				Resources:     []string{"*"},
				ResourceNames: []string{"foo"},
			},
		},
	}

	expected := []authorizer.DefaultResourceRuleInfo{
		{
			Verbs:     []string{"*"},
			APIGroups: []string{"*"},
			Resources: []string{"bindings"},
		},
		{
			Verbs:     []string{"get", "list", "watch"},
			APIGroups: []string{"*"},
			Resources: []string{"*"},
		},
		{
			Verbs:     []string{"*"},
			APIGroups: []string{"*"},
			Resources: []string{"events"},
		},
		{
			Verbs:         []string{"get"},
			APIGroups:     []string{"*"},
			Resources:     []string{"*"},
			ResourceNames: []string{"foo"},
		},
	}

	authzRulesHandler := NewRuleResolvers(handler1, handler2)

	rules, _, _, _ := authzRulesHandler.RulesFor(genericapirequest.NewContext(), nil, "")
	actual := getResourceRules(rules)
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected: \n%#v\n but actual: \n%#v\n", expected, actual)
	}
}

// TestAuthorizationNonResourceRules 验证非资源规则同样按顺序合并。
func TestAuthorizationNonResourceRules(t *testing.T) {
	handler1 := &mockAuthzRuleHandler{
		nonResourceRules: []authorizer.NonResourceRuleInfo{
			&authorizer.DefaultNonResourceRuleInfo{
				Verbs:           []string{"get"},
				NonResourceURLs: []string{"/api"},
			},
		},
	}

	handler2 := &mockAuthzRuleHandler{
		nonResourceRules: []authorizer.NonResourceRuleInfo{
			&authorizer.DefaultNonResourceRuleInfo{
				Verbs:           []string{"get"},
				NonResourceURLs: []string{"/api/*"},
			},
		},
	}

	expected := []authorizer.DefaultNonResourceRuleInfo{
		{
			Verbs:           []string{"get"},
			NonResourceURLs: []string{"/api"},
		},
		{
			Verbs:           []string{"get"},
			NonResourceURLs: []string{"/api/*"},
		},
	}

	authzRulesHandler := NewRuleResolvers(handler1, handler2)

	_, rules, _, _ := authzRulesHandler.RulesFor(genericapirequest.NewContext(), nil, "")
	actual := getNonResourceRules(rules)
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected: \n%#v\n but actual: \n%#v\n", expected, actual)
	}
}

// getResourceRules 把接口切片转换为可直接比较的具体类型，便于 DeepEqual 断言。
func getResourceRules(infos []authorizer.ResourceRuleInfo) []authorizer.DefaultResourceRuleInfo {
	rules := make([]authorizer.DefaultResourceRuleInfo, len(infos))
	for i, info := range infos {
		rules[i] = authorizer.DefaultResourceRuleInfo{
			Verbs:         info.GetVerbs(),
			APIGroups:     info.GetAPIGroups(),
			Resources:     info.GetResources(),
			ResourceNames: info.GetResourceNames(),
		}
	}
	return rules
}

// getNonResourceRules 同 getResourceRules，用于非资源规则。
func getNonResourceRules(infos []authorizer.NonResourceRuleInfo) []authorizer.DefaultNonResourceRuleInfo {
	rules := make([]authorizer.DefaultNonResourceRuleInfo, len(infos))
	for i, info := range infos {
		rules[i] = authorizer.DefaultNonResourceRuleInfo{
			Verbs:           info.GetVerbs(),
			NonResourceURLs: info.GetNonResourceURLs(),
		}
	}
	return rules
}

// TestAuthorizationUnequivocalDeny 验证 union 的决策合并顺序：
// 空链为 NoOpinion；按顺序遇到 Allow/Deny 立即返回；
// 即使 Deny 的授权器同时返回错误，Deny 仍是最终结果。
func TestAuthorizationUnequivocalDeny(t *testing.T) {
	cs := []struct {
		authorizers []authorizer.Authorizer
		decision    authorizer.Decision
	}{
		{
			// 没有授权器时没有任何一方给出意见
			authorizers: []authorizer.Authorizer{},
			decision:    authorizer.DecisionNoOpinion,
		},
		{
			// Allow 先于 Deny 出现，应短路返回 Allow
			authorizers: []authorizer.Authorizer{
				&mockAuthzHandler{decision: authorizer.DecisionNoOpinion},
				&mockAuthzHandler{decision: authorizer.DecisionAllow},
				&mockAuthzHandler{decision: authorizer.DecisionDeny},
			},
			decision: authorizer.DecisionAllow,
		},
		{
			// Deny 先于 Allow 出现，应短路返回 Deny
			authorizers: []authorizer.Authorizer{
				&mockAuthzHandler{decision: authorizer.DecisionNoOpinion},
				&mockAuthzHandler{decision: authorizer.DecisionDeny},
				&mockAuthzHandler{decision: authorizer.DecisionAllow},
			},
			decision: authorizer.DecisionDeny,
		},
		{
			// 带错误的 Deny 仍视为终止决策
			authorizers: []authorizer.Authorizer{
				&mockAuthzHandler{decision: authorizer.DecisionNoOpinion},
				&mockAuthzHandler{decision: authorizer.DecisionDeny, err: errors.New("webhook failed closed")},
				&mockAuthzHandler{decision: authorizer.DecisionAllow},
			},
			decision: authorizer.DecisionDeny,
		},
	}
	for i, c := range cs {
		t.Run(fmt.Sprintf("case %v", i), func(t *testing.T) {
			authzHandler := mustNewIndexed(t, c.authorizers...)

			decision, _, _ := authzHandler.Authorize(context.Background(), nil)
			if decision != c.decision {
				t.Errorf("Unexpected authorization failure: %v, expected: %v", decision, c.decision)
			}
		})
	}
}

// evalTestEffect 表示条件决策中单个条件的效果。
type evalTestEffect string

const (
	// effectNone 表示不使用条件决策，直接返回普通决策。
	effectNone evalTestEffect = ""
	// effectAllow 表示条件决策中带一个 allow 条件。
	effectAllow evalTestEffect = "Allow"
	// effectDeny 表示条件决策中带一个 deny 条件。
	effectDeny evalTestEffect = "Deny"
	// effectNoOpinion 表示条件决策中带一个 no-opinion 条件。
	effectNoOpinion evalTestEffect = "NoOpinion"
)

// evalTestAuthz 是用于测试 union 求值流程的可配置授权器。
type evalTestAuthz struct {
	// conditionEffect 非空时，ConditionsAwareAuthorize 返回只含该效果条件的 ConditionsMap；
	// 为空时返回 decision。
	conditionEffect evalTestEffect
	// decision 是 conditionEffect 为空时 ConditionsAwareAuthorize 的返回值。
	decision authorizer.Decision
	// authorizeErr 是 ConditionsAwareAuthorize 返回的错误。
	authorizeErr error

	// evalDecision 是 EvaluateConditions 的返回值。
	evalDecision authorizer.Decision
	// evalErr 是 EvaluateConditions 返回的错误。
	evalErr error
}

// Authorize 是非条件感知入口：条件决策无法在此表达，因此 fail closed 并报错。
func (a *evalTestAuthz) Authorize(ctx context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	if a.conditionEffect != effectNone {
		return a.ConditionsAwareAuthorize(ctx, attrs).FailureDecision(), "failed closed", fmt.Errorf("wanted to return a condition, but the client doesn't support it")
	}
	return a.decision, "", a.authorizeErr
}

// ConditionsAwareAuthorize 返回条件决策或普通决策。
func (a *evalTestAuthz) ConditionsAwareAuthorize(ctx context.Context, attrs authorizer.Attributes) authorizer.ConditionsAwareDecision {
	if a.conditionEffect != effectNone {
		cond := []authorizer.Condition{authorizer.GenericCondition{ID: "test-cond", Condition: "test"}}
		var deny, noOpinion, allow []authorizer.Condition
		switch a.conditionEffect {
		case effectAllow:
			allow = cond
		case effectDeny:
			deny = cond
		case effectNoOpinion:
			noOpinion = cond
		}
		return authorizer.ConditionsAwareDecisionConditionsMap(deny, noOpinion, allow)
	}
	return authorizer.ConditionsAwareDecisionFromParts(a.decision, "", a.authorizeErr)
}

// EvaluateConditions 只对 ConditionsMap 生效；对普通决策调用属于调用方错误。
func (a *evalTestAuthz) EvaluateConditions(ctx context.Context, decision authorizer.ConditionsAwareDecision, data authorizer.ConditionsData) (authorizer.Decision, string, error) {
	if !decision.IsConditionsMap() {
		return decision.FailureDecision(), "failed closed", errors.New("evalTestAuthz.EvaluateConditions must be called on a ConditionsMap, which was returned previously")
	}
	return a.evalDecision, "", a.evalErr
}

// TestUnionEvaluateConditions 测试嵌套 union 授权器 DAG 上完整的
// Authorize + EvaluateConditions 流程：
//
//		union0 = [union1, union3, authz5]
//		union1 = [union2, authz3]
//		union2 = [authz1, authz2]
//	    union3 = [authz4]
//
// 表驱动用例覆盖：无条件决策短路、条件决策合并、以及条件求值得到
// “该子决策不可能产生的结果”时如何 fail closed。
func TestUnionEvaluateConditions(t *testing.T) {

	noOpinion := func() *evalTestAuthz {
		return &evalTestAuthz{decision: authorizer.DecisionNoOpinion}
	}

	tests := []struct {
		name                                   string
		authz1, authz2, authz3, authz4, authz5 *evalTestAuthz
		wantAuthorizeDecision                  string
		wantFinalDecision                      string
		wantAuthorizeErr                       bool
		wantFinalErr                           bool
	}{
		// === 具体决策（不含条件） ===

		{
			// 全部 NoOpinion：外层 union 也只能给出 NoOpinion
			name:                  "all noopinion",
			authz1:                noOpinion(),
			authz2:                noOpinion(),
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `NoOpinion`,
			wantFinalDecision:     `NoOpinion`,
		},
		{
			name:                  "authz1 allow short-circuits everything",
			authz1:                &evalTestAuthz{decision: authorizer.DecisionAllow},
			authz2:                noOpinion(),
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Allow`,
			wantFinalDecision:     `Allow`,
		},
		{
			name:                  "authz1 deny short-circuits everything",
			authz1:                &evalTestAuthz{decision: authorizer.DecisionDeny},
			authz2:                noOpinion(),
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Deny`,
			wantFinalDecision:     `Deny`,
		},
		{
			name:                  "authz1 noopinion authz2 allow",
			authz1:                noOpinion(),
			authz2:                &evalTestAuthz{decision: authorizer.DecisionAllow},
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Allow`,
			wantFinalDecision:     `Allow`,
		},
		{
			name:                  "authz1 authz2 noopinion authz3 allow",
			authz1:                noOpinion(),
			authz2:                noOpinion(),
			authz3:                &evalTestAuthz{decision: authorizer.DecisionAllow},
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Allow`,
			wantFinalDecision:     `Allow`,
		},
		{
			name:                  "all inner noopinion authz5 allow",
			authz1:                noOpinion(),
			authz2:                noOpinion(),
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                &evalTestAuthz{decision: authorizer.DecisionAllow},
			wantAuthorizeDecision: `Allow`,
			wantFinalDecision:     `Allow`,
		},
		{
			name:                  "authz1 noopinion authz2 deny",
			authz1:                noOpinion(),
			authz2:                &evalTestAuthz{decision: authorizer.DecisionDeny},
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Deny`,
			wantFinalDecision:     `Deny`,
		},

		// === 条件决策 ===

		{
			name: "authz2 conditional allow evals to allow",
			authz1: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionAllow,
			},
			authz2:                noOpinion(),
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: ConditionsMap(allows=1), authz2: NoOpinion], authz3: NoOpinion], union3: NoOpinion, authz5: NoOpinion]`,
			wantFinalDecision:     `Allow`,
		},
		{
			name:   "authz2 conditional allow evals to noopinion",
			authz1: noOpinion(),
			authz2: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: NoOpinion, authz2: ConditionsMap(allows=1)], authz3: NoOpinion], union3: NoOpinion, authz5: NoOpinion]`,
			wantFinalDecision:     `NoOpinion`,
		},
		{
			name:   "authz3 conditional deny evals to deny",
			authz1: noOpinion(),
			authz2: noOpinion(),
			authz3: &evalTestAuthz{
				conditionEffect: effectDeny,
				evalDecision:    authorizer.DecisionDeny,
			},
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Union[union1: Union[union2: NoOpinion, authz3: ConditionsMap(denies=1)], union3: NoOpinion, authz5: NoOpinion]`,
			wantFinalDecision:     `Deny`,
		},
		{
			name:   "authz4 conditional deny evals to noopinion",
			authz1: noOpinion(),
			authz2: noOpinion(),
			authz3: noOpinion(),
			authz4: &evalTestAuthz{
				conditionEffect: effectDeny,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Union[union1: NoOpinion, union3: Union[authz4: ConditionsMap(denies=1)], authz5: NoOpinion]`,
			wantFinalDecision:     `NoOpinion`,
		},
		{
			name:   "authz5 conditional noopinion evals to noopinion",
			authz1: noOpinion(),
			authz2: noOpinion(),
			authz3: noOpinion(),
			authz4: noOpinion(),
			authz5: &evalTestAuthz{
				conditionEffect: effectNoOpinion,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			wantAuthorizeDecision: `NoOpinion(reason="authz5: {only NoOpinion conditions always evaluate to NoOpinion}")`,
			wantFinalDecision:     `NoOpinion(reason="authz5: {only NoOpinion conditions always evaluate to NoOpinion}")`,
		},

		// === 条件决策与具体决策混合 ===

		// conditional => noopinion, noopinion 	=== NoOpinion ok
		// conditional => noopinion, allow		=== Allow ok
		// conditional => noopinion, deny		=== Deny ok
		// conditional => allow, noopinion		=== Allow ok
		// conditional => allow, allow			=== Allow ok
		// conditional => allow, deny			=== Allow ok
		// conditional => deny, noopinion		=== Deny ok
		// conditional => deny, allow			=== Deny ok
		// conditional => deny, deny			=== Deny ok
		//
		// conditional => noopinion, conditional => noopinion	=== NoOpinion ok
		// conditional => noopinion, conditional => allow		=== Allow ok
		// conditional => noopinion, conditional => deny		=== Deny ok
		// ==> summarized as conditional => noopinion, X		=== X
		// conditional => allow, conditional => noopinion		=== Allow
		// conditional => allow, conditional => allow			=== Allow
		// conditional => allow, conditional => deny			=== Allow
		// ==> summarized as conditional => allow, <anything> 	=== Allow
		// conditional => deny, conditional => noopinion		=== Deny
		// conditional => deny, conditional => allow			=== Deny
		// conditional => deny, conditional => deny				=== Deny
		// ==> summarized as conditional => deny, <anything> 	=== Deny

		// 定理：NoOpinion 决策可插入任意位置
		//		    === 最终决策不随排列改变
		// 定理：最终 Decision 始终相同，无论授权器列表是扁平的还是切成 DAG
		//
		// 定理：具体 Allow 或 Deny 之后的后续项无关紧要：
		//
		// allow, <anything>		=== Allow
		// deny, <anything>			=== Deny

		// TODO(luxas): Implement differential testing
		// a) For a chain of length N, create all permutations of N^6 as test cases
		//	  ==> or instead work through all the possible combinations, and then insert NoOpinions?
		//	  ==> e.g. start with "I want an Allow response, what are all the ways I can get that?"
		// b) Using the formal model, compute the final decision
		// c) For each test case, create each possible DAG combination of that chain
		// d) For each DAG combo, run Authorize + Evaluate, and assert the final decision

		{
			// PossibleDecisions of [CM(allow), Allow] is {Allow} (the CM either matches → Allow,
			// or evaluates to NoOpinion → next leaf Allow), so each nested union eagerly
			// simplifies to Allow.
			name: "authz1 conditional => noopinion, authz2 allow (eagerly simplifies)",
			authz1: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz2:                &evalTestAuthz{decision: authorizer.DecisionAllow},
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Allow`,
			wantFinalDecision:     `Allow`,
		},
		{
			name:   "authz3 conditional => noopinion, authz5 deny",
			authz1: noOpinion(),
			authz2: noOpinion(),
			authz3: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz4:                noOpinion(),
			authz5:                &evalTestAuthz{decision: authorizer.DecisionDeny},
			wantAuthorizeDecision: `Union[union1: Union[union2: NoOpinion, authz3: ConditionsMap(allows=1)], union3: NoOpinion, authz5: Deny]`,
			wantFinalDecision:     `Deny`,
		},
		{
			name: "authz1 conditional => allow, authz2 allow (eagerly simplifies)",
			authz1: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionAllow,
			},
			authz2:                &evalTestAuthz{decision: authorizer.DecisionAllow},
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Allow`,
			wantFinalDecision:     `Allow`,
		},
		{
			name:   "authz3 conditional => allow, authz5 deny",
			authz1: noOpinion(),
			authz2: noOpinion(),
			authz3: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionAllow,
			},
			authz4:                noOpinion(),
			authz5:                &evalTestAuthz{decision: authorizer.DecisionDeny},
			wantAuthorizeDecision: `Union[union1: Union[union2: NoOpinion, authz3: ConditionsMap(allows=1)], union3: NoOpinion, authz5: Deny]`,
			wantFinalDecision:     `Allow`,
		},
		{
			name:   "authz2 conditional => deny, authz3 allow",
			authz1: noOpinion(),
			authz2: &evalTestAuthz{
				conditionEffect: effectDeny,
				evalDecision:    authorizer.DecisionDeny,
			},
			authz3:                &evalTestAuthz{decision: authorizer.DecisionAllow},
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: NoOpinion, authz2: ConditionsMap(denies=1)], authz3: Allow]]`,
			wantFinalDecision:     `Deny`,
		},
		{
			// inner=[union1=NoOpinion, union3=Union[CM(deny)], authz5=Deny].
			// PossibleDecisions: NoOpinion → {NoOpinion}; Union[CM(deny)] → {NoOpinion, Deny};
			// Deny → {Deny} (short-circuits, NoOpinion deleted) ⇒ {Deny}. Eagerly simplifies.
			name:   "authz4 conditional => deny, authz5 deny (eagerly simplifies to Deny)",
			authz1: noOpinion(),
			authz2: noOpinion(),
			authz3: noOpinion(),
			authz4: &evalTestAuthz{
				conditionEffect: effectDeny,
				evalDecision:    authorizer.DecisionDeny,
			},
			authz5:                &evalTestAuthz{decision: authorizer.DecisionDeny},
			wantAuthorizeDecision: `Deny`,
			wantFinalDecision:     `Deny`,
		},

		// === 多个条件决策 ===

		{
			name: "authz1 conditional => noopinion, authz3 conditional => allow",
			authz1: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz2: noOpinion(),
			authz3: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionAllow,
			},
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: ConditionsMap(allows=1), authz2: NoOpinion], authz3: ConditionsMap(allows=1)], union3: NoOpinion, authz5: NoOpinion]`,
			wantFinalDecision:     `Allow`,
		},
		{
			name:   "authz1 conditional => noopinion, authz3 conditional => deny",
			authz1: noOpinion(),
			authz2: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz3: &evalTestAuthz{
				conditionEffect: effectDeny,
				evalDecision:    authorizer.DecisionDeny,
			},
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: NoOpinion, authz2: ConditionsMap(allows=1)], authz3: ConditionsMap(denies=1)], union3: NoOpinion, authz5: NoOpinion]`,
			wantFinalDecision:     `Deny`,
		},
		{
			name: "authz1 conditional => allow, authz5 conditional => allow",
			authz1: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionAllow,
			},
			authz2: noOpinion(),
			authz3: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz4: noOpinion(),
			authz5: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionAllow,
			},
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: ConditionsMap(allows=1), authz2: NoOpinion], authz3: ConditionsMap(allows=1)], union3: NoOpinion, authz5: ConditionsMap(allows=1)]`,
			wantFinalDecision:     `Allow`,
		},
		{
			name: "authz1 conditional => allow, authz5 conditional => deny",
			authz1: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionAllow,
			},
			authz2: noOpinion(),
			authz3: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz4: noOpinion(),
			authz5: &evalTestAuthz{
				conditionEffect: effectDeny,
				evalDecision:    authorizer.DecisionDeny,
			},
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: ConditionsMap(allows=1), authz2: NoOpinion], authz3: ConditionsMap(allows=1)], union3: NoOpinion, authz5: ConditionsMap(denies=1)]`,
			wantFinalDecision:     `Allow`,
		},
		{
			name: "all conditionals eval noopinion",
			authz1: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz2: &evalTestAuthz{
				conditionEffect: effectDeny,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz3: &evalTestAuthz{
				conditionEffect: effectNoOpinion,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz4: noOpinion(),
			authz5: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: ConditionsMap(allows=1), authz2: ConditionsMap(denies=1)], authz3: NoOpinion(reason="only NoOpinion conditions always evaluate to NoOpinion")], union3: NoOpinion, authz5: ConditionsMap(allows=1)]`,
			wantFinalDecision:     `NoOpinion(reason="union1: authz3: only NoOpinion conditions always evaluate to NoOpinion")`,
		},

		// === 链中出现条件 deny ===

		{
			name: "authz1 conditional => deny, authz5 conditional => allow",
			authz1: &evalTestAuthz{
				conditionEffect: effectDeny,
				evalDecision:    authorizer.DecisionDeny,
			},
			authz2: noOpinion(),
			authz3: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz4: noOpinion(),
			authz5: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionAllow,
			},
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: ConditionsMap(denies=1), authz2: NoOpinion], authz3: ConditionsMap(allows=1)], union3: NoOpinion, authz5: ConditionsMap(allows=1)]`,
			wantFinalDecision:     `Deny`,
		},
		{
			name: "authz1 conditional => deny, authz5 conditional => deny",
			authz1: &evalTestAuthz{
				conditionEffect: effectDeny,
				evalDecision:    authorizer.DecisionDeny,
			},
			authz2: noOpinion(),
			authz3: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz4: noOpinion(),
			authz5: &evalTestAuthz{
				conditionEffect: effectDeny,
				evalDecision:    authorizer.DecisionDeny,
			},
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: ConditionsMap(denies=1), authz2: NoOpinion], authz3: ConditionsMap(allows=1)], union3: NoOpinion, authz5: ConditionsMap(denies=1)]`,
			wantFinalDecision:     `Deny`,
		},

		// === 错误处理 ===

		{
			// authz1's error rides through three layers of union ToDecision (union2, union1, union0).
			// Each layer prepends the source authorizer name when collecting deciding errors,
			// so the final error string is "union1: union2: authz1: authz error".
			name: "authorize error propagated",
			authz1: &evalTestAuthz{
				decision:     authorizer.DecisionAllow,
				authorizeErr: errors.New("authz error"),
			},
			authz2:                noOpinion(),
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Allow(err="union1: union2: authz1: authz error")`,
			wantAuthorizeErr:      true,
			wantFinalDecision:     `Allow(err="union1: union2: authz1: authz error")`,
			wantFinalErr:          true,
		},
		{
			name: "evaluate error propagated",
			authz1: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionAllow,
				evalErr:         errors.New("eval error"),
			},
			authz2:                noOpinion(),
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: ConditionsMap(allows=1), authz2: NoOpinion], authz3: NoOpinion], union3: NoOpinion, authz5: NoOpinion]`,
			wantFinalDecision:     `Allow(err="eval error")`,
			wantFinalErr:          true,
		},
		// === 不可能的求值结果 ===
		{
			name: "evaluate error propagated",
			authz1: &evalTestAuthz{
				conditionEffect: effectDeny,
				evalDecision:    authorizer.DecisionAllow,
			},
			authz2:                noOpinion(),
			authz3:                noOpinion(),
			authz4:                noOpinion(),
			authz5:                noOpinion(),
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: ConditionsMap(denies=1), authz2: NoOpinion], authz3: NoOpinion], union3: NoOpinion, authz5: NoOpinion]`,
			wantFinalDecision:     `Deny(reason="failed closed", err="evaluated to decision Allow, but only [Deny NoOpinion] were possible")`,
			wantFinalErr:          true,
		},
		{
			// 回归防护：不可能的 sub-decision 必须 fail closed 到该 SUB 决策自身的
			// FailureDecision，而不是外层 union 的 FailureDecision。这里 authz5 的
			// Allow-only ConditionsMap 的 FailureDecision=NoOpinion，而外层 union0
			// 的 FailureDecision=Deny（authz1 向其可能集合贡献了 Deny）。
			// 若守卫逻辑退化为使用外层 FailureDecision，下面的结果会变成 Deny 而非 NoOpinion。
			name: "impossible sub-decision fails closed to sub's FailureDecision, not outer union's",
			authz1: &evalTestAuthz{
				conditionEffect: effectDeny,
				evalDecision:    authorizer.DecisionNoOpinion,
			},
			authz2: noOpinion(),
			authz3: noOpinion(),
			authz4: noOpinion(),
			authz5: &evalTestAuthz{
				conditionEffect: effectAllow,
				evalDecision:    authorizer.DecisionDeny,
			},
			wantAuthorizeDecision: `Union[union1: Union[union2: Union[authz1: ConditionsMap(denies=1), authz2: NoOpinion], authz3: NoOpinion], union3: NoOpinion, authz5: ConditionsMap(allows=1)]`,
			wantFinalDecision:     `NoOpinion(reason="authz5: failed closed", err="evaluated to decision Deny, but only [Allow NoOpinion] were possible")`,
			wantFinalErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 每次子测试都重建整棵 union 树，避免状态在用例之间泄漏。
			union3, err := New(NamedAuthorizer{AuthorizerName: "authz4", Authorizer: tt.authz4})
			if err != nil {
				t.Fatalf("union3 New: %v", err)
			}
			union2, err := New(
				NamedAuthorizer{AuthorizerName: "authz1", Authorizer: tt.authz1},
				NamedAuthorizer{AuthorizerName: "authz2", Authorizer: tt.authz2},
			)
			if err != nil {
				t.Fatalf("union2 New: %v", err)
			}
			union1, err := New(
				NamedAuthorizer{AuthorizerName: "union2", Authorizer: union2},
				NamedAuthorizer{AuthorizerName: "authz3", Authorizer: tt.authz3},
			)
			if err != nil {
				t.Fatalf("union1 New: %v", err)
			}
			union0, err := New(
				NamedAuthorizer{AuthorizerName: "union1", Authorizer: union1},
				NamedAuthorizer{AuthorizerName: "union3", Authorizer: union3},
				NamedAuthorizer{AuthorizerName: "authz5", Authorizer: tt.authz5},
			)
			if err != nil {
				t.Fatalf("union0 New: %v", err)
			}

			attrs := authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "testuser"},
				Verb: "get",
			}

			ctx := context.Background()
			authzDecision := union0.ConditionsAwareAuthorize(ctx, attrs)

			authzErr := authzDecision.Error()
			if (authzErr != nil) != tt.wantAuthorizeErr {
				t.Fatalf("ConditionsAwareAuthorize() error = %v, wantErr %v", authzErr, tt.wantAuthorizeErr)
			}
			if authzDecision.String() != tt.wantAuthorizeDecision {
				t.Errorf("ConditionsAwareAuthorize() = %s, want %s", authzDecision.String(), tt.wantAuthorizeDecision)
			}

			// 用 ConditionsAwareDecision 包装只为统一字符串格式，便于断言。
			finalDecision := authzDecision
			if !authzDecision.IsUnconditional() {
				// 只有包含未求值条件的决策才需要继续调用 EvaluateConditions。
				finalDecision = authorizer.ConditionsAwareDecisionFromParts(union0.EvaluateConditions(ctx, authzDecision, nil))
			}
			finalErr := finalDecision.Error()
			if (finalErr != nil) != tt.wantFinalErr {
				t.Fatalf("EvaluateConditions() error = %v, wantErr %v", finalErr, tt.wantFinalErr)
			}
			if finalDecision.String() != tt.wantFinalDecision {
				t.Errorf("EvaluateConditions() = %s, want %s", finalDecision.String(), tt.wantFinalDecision)
			}
		})
	}
}

// countingAuthz 包装另一个 Authorizer 并记录各方法被调用的次数，
// 用于验证短路是否真的避免了调用后续授权器。
type countingAuthz struct {
	inner             authorizer.Authorizer
	conditionsAware   int
	evaluateCondCalls int
}

// Authorize 直接透传。
func (c *countingAuthz) Authorize(ctx context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	return c.inner.Authorize(ctx, attrs)
}

// ConditionsAwareAuthorize 计数后透传。
func (c *countingAuthz) ConditionsAwareAuthorize(ctx context.Context, attrs authorizer.Attributes) authorizer.ConditionsAwareDecision {
	c.conditionsAware++
	return c.inner.ConditionsAwareAuthorize(ctx, attrs)
}

// EvaluateConditions 计数后透传。
func (c *countingAuthz) EvaluateConditions(ctx context.Context, d authorizer.ConditionsAwareDecision, data authorizer.ConditionsData) (authorizer.Decision, string, error) {
	c.evaluateCondCalls++
	return c.inner.EvaluateConditions(ctx, d, data)
}

// TestUnionConditionsAwareAuthorizeShortCircuit 验证一旦某个授权器返回无条件的
// Allow 或 Deny，union 就不会再调用链中其后的授权器。
func TestUnionConditionsAwareAuthorizeShortCircuit(t *testing.T) {
	cases := []struct {
		name       string
		decisions  []authorizer.Decision // 前 N 个授权器返回的决策
		wantCalled []int                 // 每个授权器应被调用的次数
	}{
		{
			// 第二个授权器 Allow，后两个不应被调用
			name:       "Allow short-circuits later authorizers",
			decisions:  []authorizer.Decision{authorizer.DecisionNoOpinion, authorizer.DecisionAllow, authorizer.DecisionDeny, authorizer.DecisionAllow},
			wantCalled: []int{1, 1, 0, 0},
		},
		{
			// 第二个授权器 Deny，后两个不应被调用
			name:       "Deny short-circuits later authorizers",
			decisions:  []authorizer.Decision{authorizer.DecisionNoOpinion, authorizer.DecisionDeny, authorizer.DecisionAllow, authorizer.DecisionAllow},
			wantCalled: []int{1, 1, 0, 0},
		},
		{
			// 全部 NoOpinion，必须走完整条链
			name:       "all NoOpinions exhausts the chain",
			decisions:  []authorizer.Decision{authorizer.DecisionNoOpinion, authorizer.DecisionNoOpinion, authorizer.DecisionNoOpinion},
			wantCalled: []int{1, 1, 1},
		},
		{
			// 第一个就 Allow，后续一个都不调用
			name:       "first authorizer Allow short-circuits everything",
			decisions:  []authorizer.Decision{authorizer.DecisionAllow, authorizer.DecisionAllow, authorizer.DecisionAllow},
			wantCalled: []int{1, 0, 0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handlers := make([]authorizer.Authorizer, len(tc.decisions))
			counters := make([]*countingAuthz, len(tc.decisions))
			for i, d := range tc.decisions {
				counters[i] = &countingAuthz{inner: &mockAuthzHandler{decision: d}}
				handlers[i] = counters[i]
			}

			authz := mustNewIndexed(t, handlers...)
			_ = authz.ConditionsAwareAuthorize(context.Background(), nil)

			for i, c := range counters {
				if c.conditionsAware != tc.wantCalled[i] {
					t.Errorf("authorizer[%d] ConditionsAwareAuthorize called %d times, want %d", i, c.conditionsAware, tc.wantCalled[i])
				}
			}
		})
	}
}

// TestUnionConditionsAwareAuthorizeEmptyChain 验证没有子授权器时 union 返回 NoOpinion。
func TestUnionConditionsAwareAuthorizeEmptyChain(t *testing.T) {
	authz, err := New()
	if err != nil {
		t.Fatalf("union.New returned an unexpected error: %v", err)
	}
	d := authz.ConditionsAwareAuthorize(context.Background(), nil)
	if !d.IsNoOpinion() {
		t.Errorf("expected NoOpinion from empty chain, got %s", d.String())
	}
}

// TestUnionEvaluateConditionsRoutesByAuthorizerName 验证 EvaluateConditions 在派发
// ConditionsMap 求值时，依据 authorizerName 中解析出的索引把子决策路由回其来源授权器。
func TestUnionEvaluateConditionsRoutesByAuthorizerName(t *testing.T) {
	// 第一个授权器的 ConditionsMap 求值为 NoOpinion，第二个求值为 Allow。
	// 这里要验证的是：每个未求值的子决策都会按 authorizerName 回到正确的授权器。
	first := &evalTestAuthz{conditionEffect: effectAllow, evalDecision: authorizer.DecisionNoOpinion}
	second := &evalTestAuthz{conditionEffect: effectAllow, evalDecision: authorizer.DecisionAllow}
	authz, err := New(
		NamedAuthorizer{AuthorizerName: "first", Authorizer: first},
		NamedAuthorizer{AuthorizerName: "second", Authorizer: second},
	)
	if err != nil {
		t.Fatalf("union.New returned an unexpected error: %v", err)
	}

	d := authz.ConditionsAwareAuthorize(context.Background(), nil)
	if !d.IsUnion() {
		t.Fatalf("expected Union, got %s", d.String())
	}

	gotDecision, _, err := authz.EvaluateConditions(context.Background(), d, nil)
	if err != nil {
		t.Fatalf("EvaluateConditions returned error: %v", err)
	}
	if gotDecision != authorizer.DecisionAllow {
		t.Errorf("EvaluateConditions decision = %v, want Allow (routed via second authorizer)", gotDecision)
	}
}

// TestUnionEvaluateConditionsUnconditionalShortCircuit 验证 EvaluateConditions
// 对无条件决策直接返回包装后的结果，不会下探到链中。
func TestUnionEvaluateConditionsUnconditionalShortCircuit(t *testing.T) {
	a1 := &countingAuthz{inner: &evalTestAuthz{decision: authorizer.DecisionAllow}}
	a2 := &countingAuthz{inner: &evalTestAuthz{decision: authorizer.DecisionNoOpinion}}
	authz := mustNewIndexed(t, a1, a2)

	d := authz.ConditionsAwareAuthorize(context.Background(), nil)
	// 短路后应为无条件 Allow。
	if !d.IsAllow() {
		t.Fatalf("expected Allow, got %s", d.String())
	}
	if a1.conditionsAware != 1 || a2.conditionsAware != 0 {
		t.Errorf("ConditionsAwareAuthorize should not have called sub-authorizer ConditionsAwareAuthorize (a1=%d, a2=%d)", a1.conditionsAware, a2.conditionsAware)
	}
}
