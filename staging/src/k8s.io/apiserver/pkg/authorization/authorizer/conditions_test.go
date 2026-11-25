/*
Copyright The Kubernetes Authors.

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

package authorizer_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/google/cel-go/cel"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// unionDecision 用给定的 decisions 构造 ConditionsAwareDecisionUnion，为每个
// decision 分配合成的 authorizerName（"0.example.com"、"1.example.com"……），并返回等价的 ConditionsAwareDecision。
// 它只是公共 Add + ToDecision API 之上的薄封装，目的是让现有测试用例保持可读。
func unionDecision(decisions ...authorizer.ConditionsAwareDecision) authorizer.ConditionsAwareDecision {
	var u authorizer.ConditionsAwareDecisionUnion
	for i, d := range decisions {
		u.Add(strconv.Itoa(i)+".example.com", d)
	}
	return u.ToDecision()
}

// TestConditionsAwareDecision 是 ConditionsAwareDecision 的构造与并集化简的回归测试。
//
// 核心不变量（fail closed）：任何非法输入——条件过多、重复 ID、非法标签、
// 空 ConditionsMap——都必须收敛为 NoOpinion 或 Deny 并带错误，绝不能放行；
// 同时无条件的 Allow/Deny 会短路并集，ConditionsMap 则保留多结果。
func TestConditionsAwareDecision(t *testing.T) {
	unexpectedErr := fmt.Errorf("unexpected things happened")
	otherErr := fmt.Errorf("other error")

	genericCond := func(id string) authorizer.Condition {
		return authorizer.GenericCondition{ID: id, Condition: "x", Type: "test"}
	}

	ctx := t.Context()
	sampleAttrs := authorizer.AttributesRecord{}

	makeConditionsSlice := func(conditionCount int) []authorizer.Condition {
		allowConditionList := make([]authorizer.Condition, conditionCount)
		for i := range conditionCount {
			allowConditionList[i] = authorizer.GenericCondition{ID: fmt.Sprintf("cond-%d", i)}
		}
		return allowConditionList
	}

	condMapAllow := authorizer.ConditionsAwareDecisionConditionsMap(
		nil, nil,
		[]authorizer.Condition{authorizer.GenericCondition{ID: "allow-cond"}},
	)
	condMapDeny := authorizer.ConditionsAwareDecisionConditionsMap(
		[]authorizer.Condition{authorizer.GenericCondition{ID: "deny-cond"}},
		nil, nil,
	)
	condMapDenyAndAllow := authorizer.ConditionsAwareDecisionConditionsMap(
		[]authorizer.Condition{authorizer.GenericCondition{ID: "deny-1"}},
		nil,
		[]authorizer.Condition{authorizer.GenericCondition{ID: "allow-1"}},
	)

	allow := authorizer.ConditionsAwareDecisionAllow("", nil)
	deny := authorizer.ConditionsAwareDecisionDeny("", nil)
	noOp := authorizer.ConditionsAwareDecisionNoOpinion("", nil)

	tests := []struct {
		name                                 string
		testDecisions                        []authorizer.ConditionsAwareDecision
		wantIsAllow                          bool
		wantIsNoOpinion                      bool
		wantIsDeny                           bool
		wantIsConditionsMap                  bool
		wantIsUnion                          bool
		wantContainsUnconditionalAllowOrDeny bool
		wantPossibleDecisions                sets.Set[authorizer.Decision]
		wantReason                           string
		wantAnyError                         bool
		wantErrorIs                          error
		wantString                           string
	}{
		{
			name: "zero value",
			testDecisions: []authorizer.ConditionsAwareDecision{
				{},
				authorizer.ConditionsAwareDecisionFromParts(0, "", nil),
				authorizer.AuthorizerFunc(func(_ context.Context, _ authorizer.Attributes) (named1 authorizer.Decision, named2 string, named3 error) {
					return
				}).ConditionsAwareAuthorize(ctx, sampleAttrs),
			},
			wantIsDeny:  true,
			wantReason:  "",
			wantErrorIs: nil,
			wantString:  `Deny`,
		},
		{
			name: "deny constructor",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionDeny("foo", unexpectedErr),
				authorizer.ConditionsAwareDecisionFromParts(authorizer.DecisionDeny, "foo", unexpectedErr),
				authorizer.AuthorizerFunc(func(_ context.Context, _ authorizer.Attributes) (authorizer.Decision, string, error) {
					return authorizer.DecisionDeny, "foo", unexpectedErr
				}).ConditionsAwareAuthorize(ctx, sampleAttrs),
			},
			wantIsDeny:  true,
			wantReason:  "foo",
			wantErrorIs: unexpectedErr,
			wantString:  `Deny(reason="foo", err="unexpected things happened")`,
		},
		{
			name: "allow constructor",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionAllow("ok", nil),
				authorizer.ConditionsAwareDecisionFromParts(authorizer.DecisionAllow, "ok", nil),
				authorizer.AuthorizerFunc(func(_ context.Context, _ authorizer.Attributes) (authorizer.Decision, string, error) {
					return authorizer.DecisionAllow, "ok", nil
				}).ConditionsAwareAuthorize(ctx, sampleAttrs),
			},
			wantIsAllow: true,
			wantReason:  "ok",
			wantErrorIs: nil,
			wantString:  `Allow(reason="ok")`,
		},
		{
			name: "noopinion constructor",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionNoOpinion("", nil),
				authorizer.ConditionsAwareDecisionFromParts(authorizer.DecisionNoOpinion, "", nil),
				authorizer.AuthorizerFunc(func(_ context.Context, _ authorizer.Attributes) (authorizer.Decision, string, error) {
					return authorizer.DecisionNoOpinion, "", nil
				}).ConditionsAwareAuthorize(ctx, sampleAttrs),
			},
			wantIsNoOpinion: true,
			wantReason:      "",
			wantErrorIs:     nil,
			wantString:      `NoOpinion`,
		},
		{
			name: "from parts: unsupported mode",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionFromParts(42, "", nil),
				authorizer.AuthorizerFunc(func(_ context.Context, _ authorizer.Attributes) (authorizer.Decision, string, error) {
					return 42, "", nil
				}).ConditionsAwareAuthorize(ctx, sampleAttrs),
			},
			wantIsDeny:   true,
			wantReason:   "",
			wantAnyError: true,
			wantString:   `Deny(err="unknown unconditional decision type: 42")`,
		},
		{
			name: "from parts: unsupported mode with other error",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionFromParts(42, "foo", otherErr),
				authorizer.AuthorizerFunc(func(_ context.Context, _ authorizer.Attributes) (authorizer.Decision, string, error) {
					return 42, "foo", otherErr
				}).ConditionsAwareAuthorize(ctx, sampleAttrs),
			},
			wantIsDeny:  true,
			wantReason:  "foo",
			wantErrorIs: otherErr,
			wantString:  `Deny(reason="foo", err="[other error, unknown unconditional decision type: 42]")`,
		},
		{
			name: "construct valid allow/noopinion conditionsmap",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionConditionsMap(nil, nil, makeConditionsSlice(authorizer.MaxConditionsPerMap)),
			},
			wantIsConditionsMap:   true,
			wantString:            `ConditionsMap(allows=128)`,
			wantPossibleDecisions: sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionAllow),
		},
		{
			name: "construct valid allow/noopinion/deny conditionsmap",
			testDecisions: []authorizer.ConditionsAwareDecision{
				condMapDenyAndAllow,
			},
			wantIsConditionsMap:   true,
			wantString:            `ConditionsMap(denies=1, allows=1)`,
			wantPossibleDecisions: sets.New(authorizer.DecisionDeny, authorizer.DecisionNoOpinion, authorizer.DecisionAllow),
		},
		{
			name: "too many Allow conditions",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionConditionsMap(nil, nil, makeConditionsSlice(authorizer.MaxConditionsPerMap+1)),
			},
			wantIsNoOpinion: true,
			wantReason:      "failed closed",
			wantAnyError:    true,
			wantString:      `NoOpinion(reason="failed closed", err="too many conditions: 129 exceeds maximum of 128")`,
		},
		{
			name: "too many conditions, with one Deny",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionConditionsMap([]authorizer.Condition{authorizer.GenericCondition{ID: "deny-cond"}}, nil, makeConditionsSlice(authorizer.MaxConditionsPerMap)),
			},
			wantIsDeny:   true,
			wantReason:   "failed closed",
			wantAnyError: true,
			wantString:   `Deny(reason="failed closed", err="too many conditions: 129 exceeds maximum of 128")`,
		},
		{
			name: "duplicate IDs",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionConditionsMap(
					[]authorizer.Condition{authorizer.GenericCondition{ID: "foo"}},
					nil,
					[]authorizer.Condition{authorizer.GenericCondition{ID: "foo"}},
				),
			},
			wantIsDeny:   true,
			wantReason:   "failed closed",
			wantAnyError: true,
			wantString:   `Deny(reason="failed closed", err="duplicate condition ID \"foo\"")`,
		},
		{
			name: "condition ID must be a Kubernetes label, one condition error enough to fail closed (in Deny)",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionConditionsMap(
					[]authorizer.Condition{authorizer.GenericCondition{ID: "not a kubernetes label"}},
					nil,
					[]authorizer.Condition{authorizer.GenericCondition{ID: "foo"}},
				),
			},
			wantIsDeny:   true,
			wantReason:   "failed closed",
			wantAnyError: true,
			wantString:   `Deny(reason="failed closed", err="invalid condition ID \"not a kubernetes label\": name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')")`,
		},
		{
			name: "condition ID must be a Kubernetes label, one condition error enough to fail closed (in NoOpinion)",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionConditionsMap(
					nil,
					[]authorizer.Condition{authorizer.GenericCondition{ID: "not a kubernetes label"}},
					[]authorizer.Condition{authorizer.GenericCondition{ID: "foo"}},
				),
				authorizer.ConditionsAwareDecisionConditionsMap(
					nil,
					nil,
					[]authorizer.Condition{authorizer.GenericCondition{ID: "not a kubernetes label"}},
				),
			},
			wantIsNoOpinion: true,
			wantReason:      "failed closed",
			wantAnyError:    true,
			wantString:      `NoOpinion(reason="failed closed", err="invalid condition ID \"not a kubernetes label\": name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')")`,
		},
		{
			name: "condition type must be a Kubernetes label",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionConditionsMap(
					nil,
					[]authorizer.Condition{authorizer.GenericCondition{ID: "bar", Type: "not a kubernetes label"}},
					[]authorizer.Condition{authorizer.GenericCondition{ID: "foo"}},
				),
			},
			wantIsNoOpinion: true,
			wantReason:      "failed closed",
			wantAnyError:    true,
			wantString:      `NoOpinion(reason="failed closed", err="invalid condition type \"not a kubernetes label\": name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')")`,
		},
		{
			name: "empty ConditionsMap",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionConditionsMap(nil, nil, nil),
			},
			wantIsNoOpinion: true,
			wantReason:      "no conditions",
			wantAnyError:    true,
			wantString:      `NoOpinion(reason="no conditions", err="at least one condition must be passed to ConditionsAwareDecisionConditionsMap(), got none")`,
		},
		{
			// 短路：仅有 NoOpinion 条件 => 构造函数直接把结果化简为 NoOpinion，
			// 根本不会返回 ConditionsMap（即便返回了，其求值结果也是 NoOpinion）。
			name: "noopinion-only conditions short-circuit to NoOpinion",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionConditionsMap(
					nil,
					[]authorizer.Condition{authorizer.GenericCondition{ID: "nop-1"}},
					nil,
				),
			},
			wantIsNoOpinion: true,
			wantReason:      "only NoOpinion conditions always evaluate to NoOpinion",
			wantString:      `NoOpinion(reason="only NoOpinion conditions always evaluate to NoOpinion")`,
		},
		// 并集化简：单个无条件决策直接塌缩；单个 ConditionsMap 保留为 Union；
		// 全部 NoOpinion 时合并原因与错误。以下用例逐项锁定这些规则。
		{
			name: "union: empty yields NoOpinion",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(),
			},
			wantIsNoOpinion:                      true,
			wantContainsUnconditionalAllowOrDeny: false,
			wantReason:                           "",
			wantString:                           `NoOpinion`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionNoOpinion),
		},
		{
			// 单个无条件决策会被化简为该决策本身；按 ToDecision 的行为，reason 会被
			// "%s: %s" 格式的 authorizer 名前缀修饰（前缀即子决策的 authorizerName）。
			name: "union: single Allow simplifies to Allow",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(authorizer.ConditionsAwareDecisionAllow("ok", nil)),
			},
			wantIsAllow:                          true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `0.example.com: {ok}`,
			wantString:                           `Allow(reason="0.example.com: {ok}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow),
		},
		{
			name: "union: single Deny simplifies to Deny",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(authorizer.ConditionsAwareDecisionDeny("denied", nil)),
			},
			wantIsDeny:                           true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `0.example.com: {denied}`,
			wantString:                           `Deny(reason="0.example.com: {denied}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionDeny),
		},
		{
			name: "union: single NoOpinion simplifies to NoOpinion",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(authorizer.ConditionsAwareDecisionNoOpinion("noop", nil)),
			},
			wantIsNoOpinion:                      true,
			wantContainsUnconditionalAllowOrDeny: false,
			wantReason:                           `0.example.com: {noop}`,
			wantString:                           `NoOpinion(reason="0.example.com: {noop}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionNoOpinion),
		},
		{
			name: "union: single ConditionsMap wrapped",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(condMapAllow),
			},
			wantIsUnion:                          true,
			wantContainsUnconditionalAllowOrDeny: false,
			wantReason:                           "",
			wantString:                           `Union[0.example.com: ConditionsMap(allows=1)]`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionAllow),
		},
		{
			name: "union: single Union wrapped",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(unionDecision(condMapDeny, authorizer.ConditionsAwareDecisionAllow("", nil))),
			},
			wantIsUnion:                          true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           "",
			wantString:                           `Union[0.example.com: Union[0.example.com: ConditionsMap(denies=1), 1.example.com: Allow]]`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow, authorizer.DecisionDeny),
		},
		{
			name: "union: all NoOpinion yields merged NoOpinion",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					authorizer.ConditionsAwareDecisionNoOpinion("a", nil),
					authorizer.ConditionsAwareDecisionNoOpinion("", unexpectedErr),
					authorizer.ConditionsAwareDecisionNoOpinion("c", otherErr),
				),
			},
			wantIsNoOpinion:                      true,
			wantContainsUnconditionalAllowOrDeny: false,
			wantReason:                           `0.example.com: {a}, 2.example.com: {c}`,
			wantErrorIs:                          unexpectedErr,
			wantString:                           `NoOpinion(reason="0.example.com: {a}, 2.example.com: {c}", err="[1.example.com: unexpected things happened, 2.example.com: other error]")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionNoOpinion),
		},
		{
			// 内层并集带已聚合的 reason 化简为 NoOpinion。外层并集化简时，
			// collectReasonsAndErrors 把内层匹配为无条件 NoOpinion 叶子，只在其前面
			// 加上外层子名；内层聚合后的字符串原样出现在冒号之后。
			name: "union: nested NoOpinion fold embeds inner aggregate under outer prefix",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					unionDecision(
						authorizer.ConditionsAwareDecisionNoOpinion("a", nil),
						authorizer.ConditionsAwareDecisionNoOpinion("b", nil),
					),
				),
			},
			wantIsNoOpinion:                      true,
			wantContainsUnconditionalAllowOrDeny: false,
			wantReason:                           `0.example.com: {0.example.com: {a}, 1.example.com: {b}}`,
			wantString:                           `NoOpinion(reason="0.example.com: {0.example.com: {a}, 1.example.com: {b}}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionNoOpinion),
		},
		{
			// 两个内层并集各自带着自己聚合的 reason 化简为 NoOpinion。随后外层并集
			// 化简，产生扁平化、带前缀嵌入的字符串，按顺序包含两个内层聚合结果。
			name: "union: nested NoOpinion fold with siblings at each level",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					unionDecision(
						authorizer.ConditionsAwareDecisionNoOpinion("a1", nil),
						authorizer.ConditionsAwareDecisionNoOpinion("a2", nil),
					),
					unionDecision(
						authorizer.ConditionsAwareDecisionNoOpinion("b1", nil),
						authorizer.ConditionsAwareDecisionNoOpinion("b2", nil),
					),
				),
			},
			wantIsNoOpinion:                      true,
			wantContainsUnconditionalAllowOrDeny: false,
			wantReason:                           `0.example.com: {0.example.com: {a1}, 1.example.com: {a2}}, 1.example.com: {0.example.com: {b1}, 1.example.com: {b2}}`,
			wantString:                           `NoOpinion(reason="0.example.com: {0.example.com: {a1}, 1.example.com: {a2}}, 1.example.com: {0.example.com: {b1}, 1.example.com: {b2}}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionNoOpinion),
		},
		{
			// 多层级深度的错误：内层并集化简时用内层子名包装其子节点的错误；外层化简
			// 再用外层子名包装（已被包装过的）内层聚合结果，并在外层额外聚合一个
			// 兄弟错误。结果：两个带前缀包装的错误位于长度为 2 的聚合错误中。
			name: "union: nested NoOpinion fold with errors at multiple depths",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					authorizer.ConditionsAwareDecisionNoOpinion("outer-a", otherErr),
					unionDecision(
						authorizer.ConditionsAwareDecisionNoOpinion("inner-b", unexpectedErr),
						authorizer.ConditionsAwareDecisionNoOpinion("inner-c", nil),
					),
				),
			},
			wantIsNoOpinion:                      true,
			wantContainsUnconditionalAllowOrDeny: false,
			wantReason:                           `0.example.com: {outer-a}, 1.example.com: {0.example.com: {inner-b}, 1.example.com: {inner-c}}`,
			wantErrorIs:                          unexpectedErr,
			wantString:                           `NoOpinion(reason="0.example.com: {outer-a}, 1.example.com: {0.example.com: {inner-b}, 1.example.com: {inner-c}}", err="[0.example.com: other error, 1.example.com: 0.example.com: unexpected things happened]")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionNoOpinion),
		},
		{
			// Add 在遇到第一个 Allow/Deny 叶子后即短路，因此末尾的 Deny("second") 被丢弃。
			// 剩余的内层切片是 [NoOpinion, NoOpinion, Allow]，所以化简后的 reason
			// 指向 authorizer 名为 "2.example.com" 的 Allow。
			name: "union: Allow before Deny returns Allow, NoOpinions are ignored",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					authorizer.ConditionsAwareDecisionNoOpinion("skip", nil),
					authorizer.ConditionsAwareDecisionNoOpinion("skip", nil),
					authorizer.ConditionsAwareDecisionAllow("first", nil),
					authorizer.ConditionsAwareDecisionDeny("second", nil),
				),
			},
			wantIsAllow:                          true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `2.example.com: {first}`,
			wantString:                           `Allow(reason="2.example.com: {first}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow),
		},
		{
			name: "union: Deny before Allow returns Deny, NoOpinions are ignored",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					authorizer.ConditionsAwareDecisionNoOpinion("skip", nil),
					authorizer.ConditionsAwareDecisionNoOpinion("skip", nil),
					authorizer.ConditionsAwareDecisionDeny("first", nil),
					authorizer.ConditionsAwareDecisionAllow("second", nil),
				),
			},
			wantIsDeny:                           true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `2.example.com: {first}`,
			wantString:                           `Deny(reason="2.example.com: {first}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionDeny),
		},
		// 真正的并集决策（未被化简）
		{
			name: "union: noopinion + conditionsmap(allow) + noopinion",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					authorizer.ConditionsAwareDecisionNoOpinion("no-op1", nil),
					condMapAllow,
					authorizer.ConditionsAwareDecisionNoOpinion("no-op2", nil)),
			},
			wantIsUnion:                          true,
			wantContainsUnconditionalAllowOrDeny: false,
			wantReason:                           "",
			wantString:                           `Union[0.example.com: NoOpinion(reason="no-op1"), 1.example.com: ConditionsMap(allows=1), 2.example.com: NoOpinion(reason="no-op2")]`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionAllow),
		},
		{
			// ConditionsMap(仅 allow) 后接 Allow 时 PossibleDecisions={Allow}：若
			// ConditionsMap 求值为 Allow，结果是 Allow；若求值为 NoOpinion，
			// 则由末尾的 Allow 接管。无论哪种情况，并集都会立即化简为 Allow。
			name: "union: conditionsmap(allow) + allow simplifies to Allow",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(condMapAllow, authorizer.ConditionsAwareDecisionAllow("allowed", nil)),
			},
			wantIsAllow:                          true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `1.example.com: {allowed}`,
			wantString:                           `Allow(reason="1.example.com: {allowed}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow),
		},
		{
			name: "union: conditionsmap(allow) + deny",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(condMapAllow, authorizer.ConditionsAwareDecisionDeny("no", nil)),
			},
			wantIsUnion:                          true,
			wantContainsUnconditionalAllowOrDeny: true, // 内部存在一个 Deny
			wantReason:                           "",
			wantString:                           `Union[0.example.com: ConditionsMap(allows=1), 1.example.com: Deny(reason="no")]`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow, authorizer.DecisionDeny),
		},
		{
			name: "union: conditionsmap(deny) + noopinion",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(condMapDeny, authorizer.ConditionsAwareDecisionNoOpinion("noop", nil)),
			},
			wantIsUnion:                          true,
			wantContainsUnconditionalAllowOrDeny: false,
			wantReason:                           "",
			wantString:                           `Union[0.example.com: ConditionsMap(denies=1), 1.example.com: NoOpinion(reason="noop")]`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionDeny),
		},
		{
			name: "union: conditionsmap(deny) + allow with error",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(condMapDeny, authorizer.ConditionsAwareDecisionAllow("allowed", unexpectedErr)),
			},
			wantIsUnion:                          true,
			wantContainsUnconditionalAllowOrDeny: true, // 内部存在一个 Allow
			wantReason:                           "",
			wantString:                           `Union[0.example.com: ConditionsMap(denies=1), 1.example.com: Allow(reason="allowed", err="unexpected things happened")]`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow, authorizer.DecisionDeny),
		},
		{
			name: "union: conditionsmap(allow) + conditionsmap(deny)",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(condMapAllow, condMapDeny),
			},
			wantIsUnion:                          true,
			wantContainsUnconditionalAllowOrDeny: false,
			wantReason:                           "",
			wantString:                           `Union[0.example.com: ConditionsMap(allows=1), 1.example.com: ConditionsMap(denies=1)]`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionAllow, authorizer.DecisionDeny),
		},
		{
			// 内层并集 [condMapAllow, Allow("ok")] 化简为 Allow(reason="1.example.com: ok")。
			// 末尾的 NoOpinion 被外层 Add 的短路逻辑丢弃（已存在 Allow）。
			// 剩余的外层内层是 [condMapAllow, Allow("1.example.com: ok")]，
			// 它会再次化简为 Allow，并带上嵌套的 authorizer 名前缀。
			name: "union: nested with allow simplifies through both levels",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					condMapAllow,
					unionDecision(condMapAllow, authorizer.ConditionsAwareDecisionAllow("ok", nil)),
					authorizer.ConditionsAwareDecisionNoOpinion("don't care", nil),
				),
			},
			wantIsAllow:                          true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `1.example.com: {1.example.com: {ok}}`,
			wantString:                           `Allow(reason="1.example.com: {1.example.com: {ok}}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow),
		},
		{
			// 两层嵌套且每一层都发生化简。内层化简产生
			// Allow(reason="0.example.com: deep")；外层化简把它匹配为无条件 Allow 叶子，
			// 并用外层自己的子名加前缀。
			name: "union: two-level nested allow (inner union folds to Allow)",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					unionDecision(authorizer.ConditionsAwareDecisionAllow("deep", nil)),
				),
			},
			wantIsAllow:                          true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `0.example.com: {0.example.com: {deep}}`,
			wantString:                           `Allow(reason="0.example.com: {0.example.com: {deep}}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow),
		},
		{
			// 三层嵌套，错误在每次化简中一路传递。每次化简都用子名前缀包装子决策的
			// Error()（通过 fmt.Errorf %w），再放回单元素聚合错误中，因此最终的 err
			// 打印出来（不带方括号）是三个拼接的前缀。
			name: "union: three-level nested allow with error propagates through every fold",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					unionDecision(
						unionDecision(authorizer.ConditionsAwareDecisionAllow("deep", unexpectedErr)),
					),
				),
			},
			wantIsAllow:                          true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `0.example.com: {0.example.com: {0.example.com: {deep}}}`,
			wantErrorIs:                          unexpectedErr,
			wantString:                           `Allow(reason="0.example.com: {0.example.com: {0.example.com: {deep}}}", err="0.example.com: 0.example.com: 0.example.com: unexpected things happened")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow),
		},
		{
			// 外层并集有一个 NoOpinion 兄弟节点。NoOpinion 不匹配 Allow 过滤条件，
			// 对聚合结果没有贡献；只有已化简的内层 Allow 的 reason 会浮现，
			// 并带上外层子名前缀。
			name: "union: nested allow with sibling NoOpinion in outer",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					authorizer.ConditionsAwareDecisionNoOpinion("outer-noop", nil),
					unionDecision(condMapAllow, authorizer.ConditionsAwareDecisionAllow("inner-allow", nil)),
				),
			},
			wantIsAllow:                          true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `1.example.com: {1.example.com: {inner-allow}}`,
			wantString:                           `Allow(reason="1.example.com: {1.example.com: {inner-allow}}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow),
		},
		{
			// 与 "two-level nested allow" 用例对照：这里嵌套的并集并不会化简
			// （其 PossibleDecisions 是 {NoOpinion, Allow} —— 两个结果）。外层化简时，
			// collectReasonsAndErrors 通过 IsUnion 分支递归进入仍是 Union 的内层。
			// 内层没有匹配的 Allow 叶子（它的 NoOpinion 被过滤掉，ConditionsMap
			// 被两个分支都跳过），因此只有外层直接的 Allow 叶子有贡献。
			name: "union: nested allow where inner union stays a Union (recursed into, not folded)",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					unionDecision(condMapAllow, authorizer.ConditionsAwareDecisionNoOpinion("inner-noop", nil)),
					authorizer.ConditionsAwareDecisionAllow("outer-allow", nil),
				),
			},
			wantIsAllow:                          true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `1.example.com: {outer-allow}`,
			wantString:                           `Allow(reason="1.example.com: {outer-allow}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow),
		},
		{
			name: "union: deep nesting without anything unconditional",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					condMapAllow,
					unionDecision(
						condMapAllow,
						authorizer.ConditionsAwareDecisionNoOpinion("inner", nil),
						unionDecision(
							condMapDeny,
							authorizer.ConditionsAwareDecisionNoOpinion("inner2", nil)),
					),
				),
			},
			wantIsUnion:                          true,
			wantContainsUnconditionalAllowOrDeny: false,
			wantReason:                           "",
			wantString:                           `Union[0.example.com: ConditionsMap(allows=1), 1.example.com: Union[0.example.com: ConditionsMap(allows=1), 1.example.com: NoOpinion(reason="inner"), 2.example.com: Union[0.example.com: ConditionsMap(denies=1), 1.example.com: NoOpinion(reason="inner2")]]]`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionAllow, authorizer.DecisionDeny),
		},

		// --- 携带旁路（side-channel）错误的无条件叶子 ---
		{
			name: "Allow with error",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionAllow("ok", errors.New("warning")),
			},
			wantIsAllow:                          true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           "ok",
			wantAnyError:                         true,
			wantString:                           `Allow(reason="ok", err="warning")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow),
		},
		{
			name: "Deny with error",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionDeny("no", errors.New("warning")),
			},
			wantIsDeny:                           true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           "no",
			wantAnyError:                         true,
			wantString:                           `Deny(reason="no", err="warning")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionDeny),
		},
		{
			name: "NoOpinion with error",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionNoOpinion("meh", errors.New("warning")),
			},
			wantIsNoOpinion:       true,
			wantReason:            "meh",
			wantAnyError:          true,
			wantString:            `NoOpinion(reason="meh", err="warning")`,
			wantPossibleDecisions: sets.New(authorizer.DecisionNoOpinion),
		},

		// --- 纯 ConditionsMap 效果组（effect group）组合 ---
		{
			name: "conditionsmap: allow-only -> {NoOpinion, Allow}",
			testDecisions: []authorizer.ConditionsAwareDecision{
				condMapAllow,
			},
			wantIsConditionsMap:   true,
			wantString:            `ConditionsMap(allows=1)`,
			wantPossibleDecisions: sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionAllow),
		},
		{
			name: "conditionsmap: deny-only -> {NoOpinion, Deny}",
			testDecisions: []authorizer.ConditionsAwareDecision{
				condMapDeny,
			},
			wantIsConditionsMap:   true,
			wantString:            `ConditionsMap(denies=1)`,
			wantPossibleDecisions: sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionDeny),
		},
		{
			name: "conditionsmap: noOpinion + allow -> {NoOpinion, Allow}",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionConditionsMap(
					nil,
					[]authorizer.Condition{genericCond("nop-1")},
					[]authorizer.Condition{genericCond("allow-1")},
				),
			},
			wantIsConditionsMap:   true,
			wantString:            `ConditionsMap(noopinions=1, allows=1)`,
			wantPossibleDecisions: sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionAllow),
		},
		{
			name: "conditionsmap: deny + noOpinion -> {NoOpinion, Deny}",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionConditionsMap(
					[]authorizer.Condition{genericCond("deny-1")},
					[]authorizer.Condition{genericCond("nop-1")},
					nil,
				),
			},
			wantIsConditionsMap:   true,
			wantString:            `ConditionsMap(denies=1, noopinions=1)`,
			wantPossibleDecisions: sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionDeny),
		},
		{
			name: "conditionsmap: deny + allow -> {NoOpinion, Allow, Deny}",
			testDecisions: []authorizer.ConditionsAwareDecision{
				condMapDenyAndAllow,
			},
			wantIsConditionsMap:   true,
			wantString:            `ConditionsMap(denies=1, allows=1)`,
			wantPossibleDecisions: sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionAllow, authorizer.DecisionDeny),
		},
		{
			name: "conditionsmap: deny + noOpinion + allow -> {NoOpinion, Allow, Deny}",
			testDecisions: []authorizer.ConditionsAwareDecision{
				authorizer.ConditionsAwareDecisionConditionsMap(
					[]authorizer.Condition{genericCond("deny-1")},
					[]authorizer.Condition{genericCond("nop-1")},
					[]authorizer.Condition{genericCond("allow-1")},
				),
			},
			wantIsConditionsMap:   true,
			wantString:            `ConditionsMap(denies=1, noopinions=1, allows=1)`,
			wantPossibleDecisions: sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionAllow, authorizer.DecisionDeny),
		},

		// --- 上文未覆盖的并集序列 ---
		{
			name: "union: single ConditionsMap(deny) -> {NoOpinion, Deny}",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(condMapDeny),
			},
			wantIsUnion:           true,
			wantReason:            "",
			wantString:            `Union[0.example.com: ConditionsMap(denies=1)]`,
			wantPossibleDecisions: sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionDeny),
		},
		{
			// CM(deny) → Deny 会在那里短路；CM(deny) → NoOpinion 则落到后面的 Allow。
			// 两种结果都可达，所以该并集保持为 Union。
			name: "union: conditionsmap(deny) + allow stays Union",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(condMapDeny, allow),
			},
			wantIsUnion:                          true,
			wantContainsUnconditionalAllowOrDeny: true, // 末尾的 Allow 叶子
			wantReason:                           "",
			wantString:                           `Union[0.example.com: ConditionsMap(denies=1), 1.example.com: Allow]`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow, authorizer.DecisionDeny),
		},
		{
			// CM(deny) 要么给出 Deny，要么给出 NoOpinion；末尾的 Deny 兜住 NoOpinion
			// 分支——所以该并集会立即化简为 Deny。
			name: "union: conditionsmap(deny) + deny simplifies to Deny",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(condMapDeny, deny),
			},
			wantIsDeny:                           true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           "",
			wantString:                           `Deny`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionDeny),
		},
		{
			// 两层嵌套 Allow 用例的 Deny 对应版本。
			name: "union: two-level nested deny (inner union folds to Deny)",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					unionDecision(authorizer.ConditionsAwareDecisionDeny("deep", nil)),
				),
			},
			wantIsDeny:                           true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `0.example.com: {0.example.com: {deep}}`,
			wantString:                           `Deny(reason="0.example.com: {0.example.com: {deep}}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionDeny),
		},
		{
			// 三层 Deny 化简，错误逐层传递。机制与三层 Allow 变体相同。
			name: "union: three-level nested deny with error propagates through every fold",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					unionDecision(
						unionDecision(authorizer.ConditionsAwareDecisionDeny("deep", otherErr)),
					),
				),
			},
			wantIsDeny:                           true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `0.example.com: {0.example.com: {0.example.com: {deep}}}`,
			wantErrorIs:                          otherErr,
			wantString:                           `Deny(reason="0.example.com: {0.example.com: {0.example.com: {deep}}}", err="0.example.com: 0.example.com: 0.example.com: other error")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionDeny),
		},
		{
			// "嵌套 Allow 且内层并集保持 Union" 的 Deny 对应版本：内层并集的
			// PossibleDecisions={NoOpinion, Deny}（两个结果，不会化简）。外层化简为 Deny；
			// collectReasonsAndErrors 递归进入仍是 Union 的内层，但其 NoOpinion 被过滤掉、
			// CM(deny) 被跳过，因此只有外层直接的 Deny 叶子有贡献。
			name: "union: nested deny where inner union stays a Union (recursed into, not folded)",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(
					unionDecision(condMapDeny, authorizer.ConditionsAwareDecisionNoOpinion("inner-noop", nil)),
					authorizer.ConditionsAwareDecisionDeny("outer-deny", nil),
				),
			},
			wantIsDeny:                           true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           `1.example.com: {outer-deny}`,
			wantString:                           `Deny(reason="1.example.com: {outer-deny}")`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionDeny),
		},
		{
			// 后面没有可短路的 Allow/Deny，因此 NoOpinion 仍是可能结果。
			name: "union: conditionsmap(allow) + noopinion (no leaf) stays Union",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(condMapAllow, noOp),
			},
			wantIsUnion:           true,
			wantReason:            "",
			wantString:            `Union[0.example.com: ConditionsMap(allows=1), 1.example.com: NoOpinion]`,
			wantPossibleDecisions: sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionAllow),
		},
		{
			name: "union: single ConditionsMap(deny+allow) wrapped",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(condMapDenyAndAllow),
			},
			wantIsUnion:           true,
			wantReason:            "",
			wantString:            `Union[0.example.com: ConditionsMap(denies=1, allows=1)]`,
			wantPossibleDecisions: sets.New(authorizer.DecisionNoOpinion, authorizer.DecisionAllow, authorizer.DecisionDeny),
		},
		{
			name: "union: noopinion + conditionsmap(allow) + deny",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(noOp, condMapAllow, deny),
			},
			wantIsUnion:                          true,
			wantContainsUnconditionalAllowOrDeny: true, // 末尾的 Deny 叶子
			wantReason:                           "",
			wantString:                           `Union[0.example.com: NoOpinion, 1.example.com: ConditionsMap(allows=1), 2.example.com: Deny]`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow, authorizer.DecisionDeny),
		},
		{
			name: "union: conditionsmap(allow) + conditionsmap(deny) + allow",
			testDecisions: []authorizer.ConditionsAwareDecision{
				unionDecision(condMapAllow, condMapDeny, allow),
			},
			wantIsUnion:                          true,
			wantContainsUnconditionalAllowOrDeny: true,
			wantReason:                           "",
			wantString:                           `Union[0.example.com: ConditionsMap(allows=1), 1.example.com: ConditionsMap(denies=1), 2.example.com: Allow]`,
			wantPossibleDecisions:                sets.New(authorizer.DecisionAllow, authorizer.DecisionDeny),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, d := range tt.testDecisions {
				t.Run(fmt.Sprint(i), func(t *testing.T) {
					isAllowed := d.IsAllow()
					if isAllowed != tt.wantIsAllow {
						t.Errorf("IsAllowed() = %v, want %v", isAllowed, tt.wantIsAllow)
					}
					isNoOpinion := d.IsNoOpinion()
					if isNoOpinion != tt.wantIsNoOpinion {
						t.Errorf("IsNoOpinion() = %v, want %v", isNoOpinion, tt.wantIsNoOpinion)
					}
					isDenied := d.IsDeny()
					if isDenied != tt.wantIsDeny {
						t.Errorf("IsDenied() = %v, want %v", isDenied, tt.wantIsDeny)
					}
					isConditionsMap := d.IsConditionsMap()
					if isConditionsMap != tt.wantIsConditionsMap {
						t.Errorf("IsConditionsMap() = %v, want %v", isConditionsMap, tt.wantIsConditionsMap)
					}
					isUnion := d.IsUnion()
					if isUnion != tt.wantIsUnion {
						t.Errorf("IsUnion() = %v, want %v", isUnion, tt.wantIsUnion)
					}
					isUnconditional := d.IsUnconditional()
					wantIsUnconditional := tt.wantIsAllow || tt.wantIsDeny || tt.wantIsNoOpinion
					if isUnconditional != wantIsUnconditional {
						t.Errorf("IsUnconditional() = %v, want %v", isUnconditional, wantIsUnconditional)
					}
					containsUnconditionalAllowOrDeny := d.ContainsUnconditionalAllowOrDeny()
					// 普通 Allow/Deny 用例的默认断言值，避免字段冗余（stutter）
					if tt.wantIsDeny || tt.wantIsAllow {
						tt.wantContainsUnconditionalAllowOrDeny = true
					}
					if containsUnconditionalAllowOrDeny != tt.wantContainsUnconditionalAllowOrDeny {
						t.Errorf("ContainsUnconditionalAllowOrDeny() = %v, want %v", containsUnconditionalAllowOrDeny, tt.wantContainsUnconditionalAllowOrDeny)
					}
					gotPossibleDecisions := d.PossibleDecisions()
					// 普通 Allow/Deny/NoOpinion 用例的默认断言值，避免字段冗余（stutter）
					if tt.wantPossibleDecisions == nil {
						tt.wantPossibleDecisions = make(sets.Set[authorizer.Decision])
					}
					if tt.wantIsAllow {
						tt.wantPossibleDecisions.Insert(authorizer.DecisionAllow)
					}
					if tt.wantIsDeny {
						tt.wantPossibleDecisions.Insert(authorizer.DecisionDeny)
					}
					if tt.wantIsNoOpinion {
						tt.wantPossibleDecisions.Insert(authorizer.DecisionNoOpinion)
					}
					if !gotPossibleDecisions.Equal(tt.wantPossibleDecisions) {
						t.Errorf("PossibleDecisions() = %v, want %v",
							slices.Sorted(maps.Keys(gotPossibleDecisions)),
							slices.Sorted(maps.Keys(tt.wantPossibleDecisions)))
					}
					// 动态的基于属性的断言，排在向 tt.wantPossibleDecisions 填充动态默认值之后
					wantFailureDecision := authorizer.DecisionNoOpinion
					if tt.wantPossibleDecisions.Has(authorizer.DecisionDeny) {
						wantFailureDecision = authorizer.DecisionDeny
					}
					failureDecision := d.FailureDecision()
					if failureDecision != wantFailureDecision {
						t.Errorf("FailureDecision() = %v, want %v", failureDecision, wantFailureDecision)
					}
					if tt.wantIsConditionsMap {
						// 同时确认 ConditionsMap 的 FailureDecision 与之对齐
						failureDecision := d.ConditionsMap().FailureDecision()
						if failureDecision != wantFailureDecision {
							t.Errorf("ConditionsMap.FailureDecision() = %v, want %v", failureDecision, wantFailureDecision)
						}
					}
					gotReason := d.Reason()
					if gotReason != tt.wantReason {
						t.Errorf("Reason() = %v, want %v", gotReason, tt.wantReason)
					}
					gotError := d.Error()
					if tt.wantAnyError {
						if gotError == nil {
							t.Errorf("Error() = %v, want some error", nil)
						}
					} else {
						if !errors.Is(gotError, tt.wantErrorIs) {
							t.Errorf("Error() = %v, want %v", gotError, tt.wantErrorIs)
						}
					}

					gotString := d.String()
					if gotString != tt.wantString {
						t.Errorf("String() = %v, want %v", gotString, tt.wantString)
					}
				})
			}
		})
	}
}

var _ authorizer.Authorizer = sampleAuthorizer{}

type sampleAuthorizer struct{}

func (a sampleAuthorizer) Authorize(ctx context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	return unconditionalParts(a.ConditionsAwareAuthorize(ctx, attrs))
}

func (a sampleAuthorizer) ConditionsAwareAuthorize(ctx context.Context, attrs authorizer.Attributes) authorizer.ConditionsAwareDecision {
	switch attrs.GetUser().GetName() {
	case "alice":
		return authorizer.ConditionsAwareDecisionAllow("", nil)
	case "bob":
		return authorizer.ConditionsAwareDecisionDeny("", nil)
	case "carol":
		// 允许 carol 读取任何资源，但要求写入时设置 owner=carol 标签
		switch attrs.GetVerb() {
		case "list":
			return authorizer.ConditionsAwareDecisionAllow("", nil)
		case "update":
			return authorizer.ConditionsAwareDecisionConditionsMap(
				nil, nil,
				[]authorizer.Condition{authorizer.GenericCondition{
					ID: "owner-label-is-set",
					Condition: `
						(oldObject != null ? (has(oldObject.metadata) && has(oldObject.metadata.labels) && has(oldObject.metadata.labels.owner) && oldObject.metadata.labels.owner == "carol") : true) &&
						(object != null ? (has(object.metadata) && has(object.metadata.labels) && has(object.metadata.labels.owner) && object.metadata.labels.owner == "carol") : true)
					`,
					Type: "test-cel-conditions-type",
				}},
			)
		default:
			return authorizer.ConditionsAwareDecisionNoOpinion("", nil)
		}
	case "dave":
		// 允许 dave 读取任何资源，但写入时不得设置 classified 标签
		switch attrs.GetVerb() {
		case "list":
			return authorizer.ConditionsAwareDecisionAllow("", nil)
		case "create", "update", "delete":
			return authorizer.ConditionsAwareDecisionConditionsMap(
				[]authorizer.Condition{
					authorizer.GenericCondition{
						ID:        "deny-supersecret-label-on-oldObject",
						Condition: "oldObject != null && has(oldObject.metadata) && has(oldObject.metadata.labels) && has(oldObject.metadata.labels.supersecret)",
						Type:      "test-cel-conditions-type",
					},
					authorizer.GenericCondition{
						ID:        "deny-supersecret-label-on-object",
						Condition: "object != null && has(object.metadata) && has(object.metadata.labels) && has(object.metadata.labels.supersecret)",
						Type:      "test-cel-conditions-type",
					},
				},
				nil, nil,
			)
		default:
			return authorizer.ConditionsAwareDecisionNoOpinion("", nil)
		}
	default:
		return authorizer.ConditionsAwareDecisionNoOpinion("", nil)
	}
}

func (a sampleAuthorizer) EvaluateConditions(ctx context.Context, unevaluated authorizer.ConditionsAwareDecision, data authorizer.ConditionsData) (authorizer.Decision, string, error) {
	if !unevaluated.IsConditionsMap() {
		return unevaluated.FailureDecision(), "failed closed", errors.New("can only evaluate ConditionsMap decisions")
	}

	return celEvaluateConditions(ctx, unevaluated.ConditionsMap(), data)
}

// objWithLabels 构造带指定标签的非结构化对象；labels 为空时也保持合法（无 metadata）。
func objWithLabels(lbls map[string]string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{}}
	if len(lbls) > 0 {
		obj.SetLabels(lbls)
	}
	return obj
}

// unconditionalParts 把决策拆成 (Decision, reason, error)；若决策是 ConditionsMap 等有条件结果，
// 则按 fail-closed 返回 FailureDecision 且不返回 error（返回 500 会掩盖真正的授权结论）。
func unconditionalParts(d authorizer.ConditionsAwareDecision) (authorizer.Decision, string, error) {
	switch {
	case d.IsAllow():
		return authorizer.DecisionAllow, d.Reason(), d.Error()
	case d.IsDeny():
		return authorizer.DecisionDeny, d.Reason(), d.Error()
	case d.IsNoOpinion():
		return authorizer.DecisionNoOpinion, d.Reason(), d.Error()
	default:
		// 这里不返回错误，因为那可能导致 HTTP 响应码为 500 而不是 403。
		// 对于上文描述的在 Authorize 中调用本函数的场景，不返回错误很重要：
		// 始终 fail closed 是合法行为，因为一旦出现这种情况，说明请求者并未获得
		// 任何无条件权限。
		return d.FailureDecision(), "failed closed: tried to return conditional decision to conditions-unaware authorizer", nil
	}
}

// TestConditionsAwareDecisionConditionsMap_ClonesInputSlices 验证
// ConditionsAwareDecisionConditionsMap 会对输入切片做防御性拷贝，使得调用方
// 在构造完成后修改切片也无法改变所得到的决策。
//
// 防御性拷贝避免调用方在构造后修改切片，悄悄改变授权判定（属于安全边界）。
func TestConditionsAwareDecisionConditionsMap_ClonesInputSlices(t *testing.T) {
	denyConditions := []authorizer.Condition{authorizer.GenericCondition{ID: "deny-orig"}}
	noOpinionConditions := []authorizer.Condition{authorizer.GenericCondition{ID: "nop-orig"}}
	allowConditions := []authorizer.Condition{authorizer.GenericCondition{ID: "allow-orig"}}

	d := authorizer.ConditionsAwareDecisionConditionsMap(denyConditions, noOpinionConditions, allowConditions)
	if !d.IsConditionsMap() {
		t.Fatalf("expected ConditionsMap decision, got %s", d.String())
	}

	// 通过调用方持有的底层数组，修改每个输入切片的每个元素。
	denyConditions[0] = authorizer.GenericCondition{ID: "deny-mutated"}
	noOpinionConditions[0] = authorizer.GenericCondition{ID: "nop-mutated"}
	allowConditions[0] = authorizer.GenericCondition{ID: "allow-mutated"}

	collect := func(seq iter.Seq[authorizer.Condition]) []string {
		var ids []string
		for c := range seq {
			ids = append(ids, c.GetID())
		}
		return ids
	}

	cm := d.ConditionsMap()
	if got, want := collect(cm.DenyConditions()), []string{"deny-orig"}; !slices.Equal(got, want) {
		t.Errorf("DenyConditions IDs = %v, want %v (caller mutation must not leak)", got, want)
	}
	if got, want := collect(cm.NoOpinionConditions()), []string{"nop-orig"}; !slices.Equal(got, want) {
		t.Errorf("NoOpinionConditions IDs = %v, want %v (caller mutation must not leak)", got, want)
	}
	if got, want := collect(cm.AllowConditions()), []string{"allow-orig"}; !slices.Equal(got, want) {
		t.Errorf("AllowConditions IDs = %v, want %v (caller mutation must not leak)", got, want)
	}
}

// TestSampleAuthorizer 构造一个会返回 CEL 条件的示例授权器，验证：
// 条件未评估时对"不认识条件"的调用方 fail closed，评估后得到最终 Allow/Deny。
func TestSampleAuthorizer(t *testing.T) {
	type evalCase struct {
		name      string
		object    *unstructured.Unstructured
		oldObject *unstructured.Unstructured
		// 第一个元素对应不感知条件（conditions-unaware）的结果，第二个对应感知条件的结果。
		authorizeDecision [2]string
		finalDecision     [2]string
	}

	tests := []struct {
		name  string
		attrs authorizer.AttributesRecord
		cases []evalCase
	}{
		// alice：对所有动词无条件 Allow
		{
			name: "alice list",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "alice"},
				Verb: "list",
			},
			cases: []evalCase{
				{name: "allow", authorizeDecision: [2]string{`Allow`, `Allow`}},
			},
		},
		{
			name: "alice create",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "alice"},
				Verb: "create",
			},
			cases: []evalCase{
				{name: "allow", authorizeDecision: [2]string{`Allow`, `Allow`}},
			},
		},
		// bob：对所有动词无条件 Deny
		{
			name: "bob list",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "bob"},
				Verb: "list",
			},
			cases: []evalCase{
				{name: "deny", authorizeDecision: [2]string{`Deny`, `Deny`}},
			},
		},
		{
			name: "bob create",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "bob"},
				Verb: "create",
			},
			cases: []evalCase{
				{name: "deny", authorizeDecision: [2]string{`Deny`, `Deny`}},
			},
		},
		// carol：允许读取；写入有条件（设置 owner=carol 时 Allow）
		{
			name: "carol list",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "carol"},
				Verb: "list",
			},
			cases: []evalCase{
				{name: "allow", authorizeDecision: [2]string{`Allow`, `Allow`}},
			},
		},
		{
			name: "carol update",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "carol"},
				Verb: "update",
			},
			cases: []evalCase{
				{
					name:      "both objects with owner=carol",
					object:    objWithLabels(map[string]string{"owner": "carol"}),
					oldObject: objWithLabels(map[string]string{"owner": "carol"}),
					authorizeDecision: [2]string{
						`NoOpinion(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`,
						`ConditionsMap(allows=1)`,
					},
					finalDecision: [2]string{
						`NoOpinion(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`,
						`Allow(reason="condition \"owner-label-is-set\" allowed the request")`,
					},
				},
				{
					name:      "old with owner=carol, new without",
					object:    objWithLabels(map[string]string{"owner": "carol"}),
					oldObject: objWithLabels(nil),
					authorizeDecision: [2]string{
						`NoOpinion(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`,
						`ConditionsMap(allows=1)`,
					},
					finalDecision: [2]string{
						`NoOpinion(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`,
						`NoOpinion(reason="no conditions matched")`,
					},
				},
				{
					name:      "new with owner=carol, old with owner=alice",
					object:    objWithLabels(map[string]string{"owner": "alice"}),
					oldObject: objWithLabels(map[string]string{"owner": "carol"}),
					authorizeDecision: [2]string{
						`NoOpinion(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`,
						`ConditionsMap(allows=1)`,
					},
					finalDecision: [2]string{
						`NoOpinion(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`,
						`NoOpinion(reason="no conditions matched")`,
					},
				},
			},
		},
		{
			name: "carol unsupported verb",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "carol"},
				Verb: "patch",
			},
			cases: []evalCase{
				{name: "no opinion", authorizeDecision: [2]string{`NoOpinion`, `NoOpinion`}},
			},
		},
		// dave：允许读取；写入有条件（存在 supersecret 标签时 Deny）
		{
			name: "dave list",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "dave"},
				Verb: "list",
			},
			cases: []evalCase{
				{name: "allow", authorizeDecision: [2]string{`Allow`, `Allow`}},
			},
		},

		{
			name: "dave update",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "dave"},
				Verb: "update",
			},
			cases: []evalCase{
				{
					name:              "both objects with supersecret",
					object:            objWithLabels(map[string]string{"supersecret": "yes"}),
					oldObject:         objWithLabels(map[string]string{"supersecret": "yes"}),
					authorizeDecision: [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `ConditionsMap(denies=2)`},
					finalDecision:     [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `Deny(reason="condition \"deny-supersecret-label-on-oldObject\" denied the request")`},
				},
				{
					name:              "new with supersecret old without",
					object:            objWithLabels(map[string]string{"supersecret": "yes"}),
					oldObject:         objWithLabels(nil),
					authorizeDecision: [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `ConditionsMap(denies=2)`},
					finalDecision:     [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `Deny(reason="condition \"deny-supersecret-label-on-object\" denied the request")`},
				},
				{
					name:              "new without old with supersecret",
					object:            objWithLabels(nil),
					oldObject:         objWithLabels(map[string]string{"supersecret": "yes"}),
					authorizeDecision: [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `ConditionsMap(denies=2)`},
					finalDecision:     [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `Deny(reason="condition \"deny-supersecret-label-on-oldObject\" denied the request")`},
				},
				{
					name:              "both without supersecret",
					object:            objWithLabels(map[string]string{"safe": "true"}),
					oldObject:         objWithLabels(map[string]string{"safe": "true"}),
					authorizeDecision: [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `ConditionsMap(denies=2)`},
					finalDecision:     [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `NoOpinion(reason="no conditions matched")`},
				},
			},
		},
		{
			name: "dave create",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "dave"},
				Verb: "create",
			},
			cases: []evalCase{
				{
					name:              "create with supersecret",
					object:            objWithLabels(map[string]string{"supersecret": "yes"}),
					authorizeDecision: [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `ConditionsMap(denies=2)`},
					finalDecision:     [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `Deny(reason="condition \"deny-supersecret-label-on-object\" denied the request")`},
				},
				{
					name:              "create without supersecret",
					object:            objWithLabels(map[string]string{"safe": "true"}),
					authorizeDecision: [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `ConditionsMap(denies=2)`},
					finalDecision:     [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `NoOpinion(reason="no conditions matched")`},
				},
			},
		},
		{
			name: "dave delete",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "dave"},
				Verb: "delete",
			},
			cases: []evalCase{
				{
					name:              "delete with supersecret on old object",
					oldObject:         objWithLabels(map[string]string{"supersecret": "yes"}),
					authorizeDecision: [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `ConditionsMap(denies=2)`},
					finalDecision:     [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `Deny(reason="condition \"deny-supersecret-label-on-oldObject\" denied the request")`},
				},
				{
					name:              "delete without supersecret on old object",
					oldObject:         objWithLabels(map[string]string{"safe": "true"}),
					authorizeDecision: [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `ConditionsMap(denies=2)`},
					finalDecision:     [2]string{`Deny(reason="failed closed: tried to return conditional decision to conditions-unaware authorizer")`, `NoOpinion(reason="no conditions matched")`},
				},
			},
		},
		{
			name: "dave unsupported verb",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "dave"},
				Verb: "patch",
			},
			cases: []evalCase{
				{name: "no opinion", authorizeDecision: [2]string{`NoOpinion`, `NoOpinion`}},
			},
		},
		// unknown user：无意见（NoOpinion）
		{
			name: "unknown user get",
			attrs: authorizer.AttributesRecord{
				User: &user.DefaultInfo{Name: "unknown"},
				Verb: "list",
			},
			cases: []evalCase{
				{name: "no opinion", authorizeDecision: [2]string{`NoOpinion`, `NoOpinion`}},
			},
		},
	}

	authz := sampleAuthorizer{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, tc := range tt.cases {
				// 若只指定了授权决策，最终决策与其相同
				if len(tc.finalDecision[0]) == 0 && len(tc.finalDecision[1]) == 0 {
					tc.finalDecision[0] = tc.authorizeDecision[0]
					tc.finalDecision[1] = tc.authorizeDecision[1]
				}
				for i, supportsConditions := range [2]bool{false, true} {
					t.Run(fmt.Sprintf("%s/%t", tc.name, supportsConditions), func(t *testing.T) {
						var decision authorizer.ConditionsAwareDecision
						if supportsConditions {
							decision = authz.ConditionsAwareAuthorize(t.Context(), tt.attrs)
						} else {
							decision = authorizer.ConditionsAwareDecisionFromParts(authz.Authorize(t.Context(), tt.attrs))
						}

						if decision.String() != tc.authorizeDecision[i] {
							t.Errorf("got Authorize() decision %s, want %s", decision.String(), tc.authorizeDecision[i])
						}

						// celEvaluateConditions 只用到 object 和 oldObject，其余值一律置零，因为它们反正不会被使用。
						data := admission.NewAttributesRecord(tc.object, tc.oldObject, schema.GroupVersionKind{}, "", "", schema.GroupVersionResource{}, "", "", nil, false, nil)

						// 包装成 ConditionsAwareDecision 形式，只是为了获得统一的字符串比较机制。
						final := decision
						if !decision.IsUnconditional() {
							final = authorizer.ConditionsAwareDecisionFromParts(authz.EvaluateConditions(t.Context(), decision, data))
						}
						if final.String() != tc.finalDecision[i] {
							t.Errorf("got Evaluate() decision %s, want %s", final.String(), tc.finalDecision[i])
						}
					})
				}
			}
		})
	}
}

// celEvaluateConditions 用 object/oldObject 作为变量求值 ConditionsMap。
// 任何构建或转换失败都走 FailureDecision + "failed closed"，不允许降级为放行。
func celEvaluateConditions(ctx context.Context, conditionsMap authorizer.ConditionsMap, data authorizer.ConditionsData) (authorizer.Decision, string, error) {
	env, err := cel.NewEnv(
		cel.Variable("object", cel.DynType),
		cel.Variable("oldObject", cel.DynType),
	)
	if err != nil {
		return conditionsMap.FailureDecision(), "failed closed", fmt.Errorf("failed to create CEL env: %w", err)
	}

	if data == nil {
		return conditionsMap.FailureDecision(), "failed closed", errors.New("evaluating a CEL condition requires non-nil data.AdmissionControl")
	}

	obj, err := objectToResolveVal(data.GetObject())
	if err != nil {
		return conditionsMap.FailureDecision(), "failed closed", fmt.Errorf("failed to convert object to CEL ref.Val: %w", err)
	}

	oldObj, err := objectToResolveVal(data.GetOldObject())
	if err != nil {
		return conditionsMap.FailureDecision(), "failed closed", fmt.Errorf("failed to convert object to CEL ref.Val: %w", err)
	}

	vars := map[string]any{
		"object":    obj,
		"oldObject": oldObj,
	}

	return conditionsMap.Evaluate(ctx, data, func(ctx context.Context, c authorizer.Condition, _ authorizer.ConditionsData) (bool, error) {
		return evalCEL(env, c.GetCondition(), vars)
	})
}

// evalCEL 编译并求值单个 CEL 表达式，返回 true/false。
// 结果不是 bool 也视为错误，避免把非布尔值误当作允许。
func evalCEL(env *cel.Env, expr string, vars map[string]any) (bool, error) {
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return false, fmt.Errorf("CEL compile error for %q: %w", expr, issues.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		return false, fmt.Errorf("CEL program error for %q: %w", expr, err)
	}
	out, _, err := prg.Eval(vars)
	if err != nil {
		return false, fmt.Errorf("CEL eval error for %q: %w", expr, err)
	}
	result, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("CEL expression %q did not return bool, got %T", expr, out.Value())
	}
	return result, nil
}

// objectToResolveVal 把运行时对象转换为 CEL 可用的 map；nil 或 typed-nil 返回 nil，表示条件中引用的对象不存在。
func objectToResolveVal(r runtime.Object) (interface{}, error) {
	if r == nil || reflect.ValueOf(r).IsNil() {
		return nil, nil
	}
	ret, err := runtime.DefaultUnstructuredConverter.ToUnstructured(r)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// TestConditionsAwareDecisionUnionedDecisions 验证 UnionedDecisions 迭代器：
// 非并集决策没有子项，并集按加入顺序产出子决策（含嵌套并集的扁平化语义）。
func TestConditionsAwareDecisionUnionedDecisions(t *testing.T) {
	condMap := authorizer.ConditionsAwareDecisionConditionsMap(
		nil, nil,
		[]authorizer.Condition{authorizer.GenericCondition{ID: "test", Condition: "true", Type: "test-type"}},
	)
	noOp := authorizer.ConditionsAwareDecisionNoOpinion("noop", nil)

	t.Run("non-union has empty iterator", func(t *testing.T) {
		noUnionTestcases := []authorizer.ConditionsAwareDecision{
			condMap,
			noOp,
			authorizer.ConditionsAwareDecisionAllow("ok", nil),
			authorizer.ConditionsAwareDecisionDeny("not ok", nil),
		}
		for i, tc := range noUnionTestcases {
			t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
				count := 0
				for range tc.UnionedDecisions() {
					count++
				}
				if count != 0 {
					t.Errorf("expected 0 unioned decisions for %s, got %d", tc, count)
				}
			})
		}
	})

	t.Run("union iterates sub-decisions in order", func(t *testing.T) {
		union := unionDecision(condMap, noOp)
		var got []string
		for _, sub := range union.UnionedDecisions() {
			got = append(got, sub.Reason())
		}
		want := []string{"", "noop"}
		if len(got) != len(want) {
			t.Fatalf("expected %d sub-decisions, got %d", len(want), len(got))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("sub-decision[%d].Reason() = %q, want %q", i, got[i], want[i])
			}
		}
	})

	t.Run("early break in iterator", func(t *testing.T) {
		union := unionDecision(condMap, noOp)
		count := 0
		for range union.UnionedDecisions() {
			count++
			break
		}
		if count != 1 {
			t.Errorf("expected early break after 1 iteration, got %d", count)
		}
	})
}
