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

package metrics

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// TestRecordAuthorizationDecisionsTotal 验证 InstrumentedAuthorizer 对
// apiserver_authorization_decisions_total 的计数语义：
// allow/deny 按 decision、type、name 维度计数；NoOpinion 不产生指标；
// 无法识别的 decision 记为 unknown；条件授权器与非条件授权器行为一致。
func TestRecordAuthorizationDecisionsTotal(t *testing.T) {
	// prefix 是期望的 HELP/TYPE 行，所有子用例共用。
	prefix := `
    # HELP apiserver_authorization_decisions_total [ALPHA] Total number of terminal decisions made by an authorizer split by authorizer type, name, and decision.
    # TYPE apiserver_authorization_decisions_total counter`
	metrics := []string{
		namespace + "_" + subsystem + "_decisions_total",
	}

	// 重置全局计数器，避免与其他测试的注册/累积互相影响。
	authorizationDecisionsTotal.Reset()
	RegisterMetrics()

	dummyAuthorizer := &dummyAuthorizer{}
	dummyConditionalAuthorizer := &dummyConditionalAuthorizer{}
	a := InstrumentedAuthorizer("mytype", "myname", dummyAuthorizer)
	ac := InstrumentedAuthorizer("myconditionaltype", "myconditionalname", dummyConditionalAuthorizer)

	// allow
	{
		dummyAuthorizer.decision = authorizer.DecisionAllow
		_, _, _ = a.Authorize(context.Background(), nil)
		expectedValue := prefix + `
			apiserver_authorization_decisions_total{decision="allowed",name="myname",type="mytype"} 1
		`
		if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
			t.Fatal(err)
		}
		authorizationDecisionsTotal.Reset()
	}

	// allow（条件授权器）：条件决策的 allow 同样计入 allowed
	{
		dummyConditionalAuthorizer.authorizeDecision = authorizer.ConditionsAwareDecisionAllow("", nil)
		_, _, _ = ac.Authorize(context.Background(), nil)
		expectedValue := prefix + `
			apiserver_authorization_decisions_total{decision="allowed",name="myconditionalname",type="myconditionaltype"} 1
		`
		if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
			t.Fatal(err)
		}
		authorizationDecisionsTotal.Reset()
	}

	// deny
	{
		dummyAuthorizer.decision = authorizer.DecisionDeny
		_, _, _ = a.Authorize(context.Background(), nil)
		_, _, _ = a.Authorize(context.Background(), nil)
		expectedValue := prefix + `
			apiserver_authorization_decisions_total{decision="denied",name="myname",type="mytype"} 2
		`
		if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
			t.Fatal(err)
		}
		authorizationDecisionsTotal.Reset()
	}

	// deny（条件授权器）
	{
		dummyConditionalAuthorizer.authorizeDecision = authorizer.ConditionsAwareDecisionDeny("", nil)
		_, _, _ = ac.Authorize(context.Background(), nil)
		_, _, _ = ac.Authorize(context.Background(), nil)
		expectedValue := prefix + `
			apiserver_authorization_decisions_total{decision="denied",name="myconditionalname",type="myconditionaltype"} 2
		`
		if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
			t.Fatal(err)
		}
		authorizationDecisionsTotal.Reset()
	}

	// no-opinion 不产生指标：它不是终止决策
	{
		dummyAuthorizer.decision = authorizer.DecisionNoOpinion
		_, _, _ = a.Authorize(context.Background(), nil)
		_, _, _ = a.Authorize(context.Background(), nil)
		expectedValue := prefix + `
		`
		if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
			t.Fatal(err)
		}
		authorizationDecisionsTotal.Reset()
	}

	// no-opinion 不产生指标（条件授权器）
	{
		dummyConditionalAuthorizer.authorizeDecision = authorizer.ConditionsAwareDecisionNoOpinion("", nil)
		_, _, _ = ac.Authorize(context.Background(), nil)
		_, _, _ = ac.Authorize(context.Background(), nil)
		expectedValue := prefix + `
		`
		if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
			t.Fatal(err)
		}
		authorizationDecisionsTotal.Reset()
	}

	// 未知 decision 记为 unknown：用于发现新决策值未更新的指标映射
	{
		dummyAuthorizer.decision = authorizer.DecisionDeny + 10
		_, _, _ = a.Authorize(context.Background(), nil)
		expectedValue := prefix + `
			apiserver_authorization_decisions_total{decision="unknown",name="myname",type="mytype"} 1
		`
		if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expectedValue), metrics...); err != nil {
			t.Fatal(err)
		}
		authorizationDecisionsTotal.Reset()
	}
	// TODO(luxas): Add a test for getting a conditional decision from ConditionsAwareAuthorize, and evaluating a condition, once introduced
}

// dummyAuthorizer 是只返回固定决策的授权器，用于隔离指标逻辑。
type dummyAuthorizer struct {
	decision authorizer.Decision
	err      error
}

// Authorize 返回预设的决策与错误。
func (d *dummyAuthorizer) Authorize(ctx context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	return d.decision, "", d.err
}

// ConditionsAwareAuthorize 不是条件感知实现，把 Authorize 的决策转换过去。
func (d *dummyAuthorizer) ConditionsAwareAuthorize(ctx context.Context, attrs authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return authorizer.ConditionsAwareDecisionFromParts(d.Authorize(ctx, attrs))
}

// EvaluateConditions 不被该授权器支持，返回“条件求值不支持”错误。
func (*dummyAuthorizer) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
}

// dummyConditionalAuthorizer 是返回固定条件决策的授权器。
type dummyConditionalAuthorizer struct {
	authorizeDecision authorizer.ConditionsAwareDecision
	evalDecision      authorizer.Decision
	evalErr           error
}

// Authorize 把条件决策折叠为普通决策；遇到无法折叠的条件决策时 fail closed。
func (d *dummyConditionalAuthorizer) Authorize(ctx context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	switch {
	case d.authorizeDecision.IsAllow():
		return authorizer.DecisionAllow, d.authorizeDecision.Reason(), d.authorizeDecision.Error()
	case d.authorizeDecision.IsNoOpinion():
		return authorizer.DecisionNoOpinion, d.authorizeDecision.Reason(), d.authorizeDecision.Error()
	case d.authorizeDecision.IsDeny():
		return authorizer.DecisionDeny, d.authorizeDecision.Reason(), d.authorizeDecision.Error()
	default: // 条件决策：调用方不该走这个非条件感知的方法
		return authorizer.DecisionDeny, "failed closed: wanted to return a conditional decision, but called on the conditions-unaware method", nil
	}
}

// ConditionsAwareAuthorize 直接返回预设的条件决策。
func (d *dummyConditionalAuthorizer) ConditionsAwareAuthorize(ctx context.Context, attrs authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return d.authorizeDecision
}

// EvaluateConditions 返回预设的求值结果。
func (d *dummyConditionalAuthorizer) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return d.evalDecision, "", d.evalErr
}
