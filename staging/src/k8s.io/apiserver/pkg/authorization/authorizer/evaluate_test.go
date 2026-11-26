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
	"testing"

	"github.com/google/go-cmp/cmp"

	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// TestPartiallyEvaluateConditionsAwareDecision 验证部分求值器对 ConditionsAwareDecision
// 树的各种简化行为：内置条件可完全求值时折叠为 Allow/Deny/NoOpinion，遇到
// 不可求值的条件时保留为精化后的 ConditionsMap，Union 则按顺序逐项简化。
func TestPartiallyEvaluateConditionsAwareDecision(t *testing.T) {
	// mkCM 按 (effect, condition) 对构造 ConditionsMap 判定。
	mkCM := func(items ...effectCondition) authorizer.ConditionsAwareDecision {
		var deny, nop, allow []authorizer.Condition
		for _, it := range items {
			switch it.effect {
			case effectDeny:
				deny = append(deny, it.cond)
			case effectNoOpinion:
				nop = append(nop, it.cond)
			case effectAllow:
				allow = append(allow, it.cond)
			}
		}
		return authorizer.ConditionsAwareDecisionConditionsMap(deny, nop, allow)
	}

	// cnd 是带标签的 GenericCondition 简写构造器，description 可省略。
	cnd := func(effect conditionEffect, id, condition, typ, description string) effectCondition {
		return effectCondition{
			effect: effect,
			cond: authorizer.GenericCondition{
				ID: id, Condition: condition, Type: typ, Description: description,
			},
		}
	}

	tests := []struct {
		name string

		// 传给 PartiallyEvaluateConditionsAwareDecision 的输入判定。
		decision authorizer.ConditionsAwareDecision

		// 提供给部分求值器的 MaybeEvaluateConditionFunc；返回 Unevaluatable 时，
		// 该条件会保留在精化后的 ConditionsMap 中。
		builtinConditionsEvaluator authorizer.MaybeEvaluateConditionFunc

		// 返回判定树对应的预期快照。
		want snapDecision
	}{
		{
			name: "full builtin evaluation of one ConditionsMap => Deny",
			decision: mkCM(
				cnd(effectAllow, "c", "c", "transparent", "all ok"),
				cnd(effectDeny, "d", "d", "transparent", "very bad"),
			),
			builtinConditionsEvaluator: func(_ context.Context, condition authorizer.Condition, _ authorizer.ConditionsData) authorizer.ConditionEvaluationResult {
				return authorizer.ConditionEvaluationResultBoolean(condition.GetCondition() == "d")
			},
			want: snapDecision{Kind: "Deny", Reason: `condition "d" denied the request with description "very bad"`},
		},
		{
			name: "full builtin evaluation of one ConditionsMap => NoOpinion",
			decision: mkCM(
				cnd(effectAllow, "c", "c", "transparent", "all ok"),
				cnd(effectDeny, "d", "d", "transparent", "very bad"),
			),
			builtinConditionsEvaluator: func(_ context.Context, _ authorizer.Condition, _ authorizer.ConditionsData) authorizer.ConditionEvaluationResult {
				return authorizer.ConditionEvaluationResultBoolean(false)
			},
			want: snapDecision{Kind: "NoOpinion", Reason: "no conditions matched"},
		},
		{
			name: "full builtin evaluation of one ConditionsMap => Allow",
			decision: mkCM(
				cnd(effectAllow, "c", "c", "transparent", "all ok"),
				cnd(effectDeny, "d", "d", "transparent", "very bad"),
			),
			builtinConditionsEvaluator: func(_ context.Context, condition authorizer.Condition, _ authorizer.ConditionsData) authorizer.ConditionEvaluationResult {
				return authorizer.ConditionEvaluationResultBoolean(condition.GetCondition() == "c")
			},
			want: snapDecision{Kind: "Allow", Reason: `condition "c" allowed the request with description "all ok"`},
		},
		{
			// opaque 的 allow 条件无法在本进程求值，因此部分求值结果仍是
			// 只含该条件的精化 ConditionsMap。
			name: "partial builtin evaluation of one ConditionsMap => refined ConditionsMap",
			decision: mkCM(
				cnd(effectAllow, "c", "c", "opaque", "all ok"),
				cnd(effectDeny, "d", "d", "transparent", "very bad"),
			),
			builtinConditionsEvaluator: func(_ context.Context, condition authorizer.Condition, _ authorizer.ConditionsData) authorizer.ConditionEvaluationResult {
				if condition.GetType() == "transparent" {
					return authorizer.ConditionEvaluationResultBoolean(condition.GetCondition() == "c")
				}
				return authorizer.ConditionsEvaluationResultUnevaluatable()
			},
			want: snapDecision{
				Kind: "ConditionsMap",
				CM: &snapCM{
					Allow: []snapCondition{
						{ID: "c", Condition: "c", Type: "opaque", Description: "all ok"},
					},
				},
			},
		},
		{
			name: "builtin evaluation of union succeeds => Allow",
			decision: unionDecision(
				mkCM(
					cnd(effectAllow, "a", "a", "transparent", ""),
					cnd(effectDeny, "b", "b", "transparent", ""),
				),
				mkCM(
					cnd(effectAllow, "c", "c", "transparent", ""),
					cnd(effectDeny, "d", "d", "transparent", ""),
				),
			),
			builtinConditionsEvaluator: func(_ context.Context, condition authorizer.Condition, _ authorizer.ConditionsData) authorizer.ConditionEvaluationResult {
				return authorizer.ConditionEvaluationResultBoolean(condition.GetCondition() == "c")
			},
			want: snapDecision{Kind: "Allow", Reason: `1.example.com: {condition "c" allowed the request}`},
		},
		{
			name: "builtin evaluation of union succeeds => Deny",
			decision: unionDecision(
				mkCM(
					cnd(effectAllow, "a", "a", "transparent", ""),
					cnd(effectDeny, "b", "b", "transparent", ""),
				),
				mkCM(
					cnd(effectAllow, "c", "c", "transparent", ""),
					cnd(effectDeny, "d", "d", "transparent", ""),
				),
			),
			builtinConditionsEvaluator: func(_ context.Context, condition authorizer.Condition, _ authorizer.ConditionsData) authorizer.ConditionEvaluationResult {
				return authorizer.ConditionEvaluationResultBoolean(condition.GetCondition() == "d")
			},
			want: snapDecision{Kind: "Deny", Reason: `1.example.com: {condition "d" denied the request}`},
		},
		{
			// 第一个 ConditionsMap 含无法简化的 opaque allow 条件，Union 从此处短路，
			// 后续子判定原样收集；第二个 ConditionsMap 不会被执行求值，保持原样。
			name: "first conditionsmap cannot be simplified fully",
			decision: unionDecision(
				mkCM(
					cnd(effectAllow, "a", "a", "opaque", ""),
					cnd(effectDeny, "b", "b", "transparent", ""),
				),
				mkCM(
					cnd(effectAllow, "c", "c", "transparent", ""),
					cnd(effectDeny, "d", "d", "transparent", ""),
				),
			),
			builtinConditionsEvaluator: func(_ context.Context, condition authorizer.Condition, _ authorizer.ConditionsData) authorizer.ConditionEvaluationResult {
				if condition.GetType() == "transparent" {
					return authorizer.ConditionEvaluationResultBoolean(condition.GetCondition() == "c")
				}
				return authorizer.ConditionsEvaluationResultUnevaluatable()
			},
			want: snapDecision{
				Kind: "Union",
				Union: []snapDecision{
					{
						Kind: "ConditionsMap",
						CM: &snapCM{
							Allow: []snapCondition{{ID: "a", Condition: "a", Type: "opaque"}},
						},
					},
					{
						Kind: "ConditionsMap",
						CM: &snapCM{
							Deny:  []snapCondition{{ID: "d", Condition: "d", Type: "transparent"}},
							Allow: []snapCondition{{ID: "c", Condition: "c", Type: "transparent"}},
						},
					},
				},
			},
		},
		{
			// 第一个 ConditionsMap 完全简化为 NoOpinion（transparent 条件均不匹配）；
			// 第二个含 opaque deny 条件，无法简化，保留为精化后的 ConditionsMap；
			// 第三项是无条件 Deny，原样保留。
			name: "first conditionsmap can be simplified fully, but not second",
			decision: unionDecision(
				mkCM(
					cnd(effectAllow, "a", "a", "transparent", ""),
					cnd(effectDeny, "b", "b", "transparent", ""),
				),
				mkCM(
					cnd(effectAllow, "c", "c", "transparent", ""),
					cnd(effectDeny, "d", "d", "opaque", ""),
				),
				authorizer.ConditionsAwareDecisionDeny("something later denies", nil),
			),
			builtinConditionsEvaluator: func(_ context.Context, condition authorizer.Condition, _ authorizer.ConditionsData) authorizer.ConditionEvaluationResult {
				if condition.GetType() == "transparent" {
					return authorizer.ConditionEvaluationResultBoolean(condition.GetCondition() == "c")
				}
				return authorizer.ConditionsEvaluationResultUnevaluatable()
			},
			want: snapDecision{
				Kind: "Union",
				Union: []snapDecision{
					{Kind: "NoOpinion", Reason: "no conditions matched"},
					{
						Kind: "ConditionsMap",
						CM: &snapCM{
							Deny:  []snapCondition{{ID: "d", Condition: "d", Type: "opaque"}},
							Allow: []snapCondition{{ID: "c", Condition: "c", Type: "transparent"}},
						},
					},
					{Kind: "Deny", Reason: "something later denies"},
				},
			},
		},
		{
			name: "first allow conditionsmap => NoOpinion, noopinion, third conditional deny, finally Deny => Deny",
			decision: unionDecision(
				mkCM(
					cnd(effectAllow, "allow-false", "a", "transparent", ""),
				),
				authorizer.ConditionsAwareDecisionNoOpinion("", nil),
				mkCM(
					cnd(effectDeny, "b", "b", "opaque", ""),
				),
				authorizer.ConditionsAwareDecisionDeny("something later denies", nil),
			),
			builtinConditionsEvaluator: func(_ context.Context, condition authorizer.Condition, _ authorizer.ConditionsData) authorizer.ConditionEvaluationResult {
				if condition.GetType() == "transparent" {
					return authorizer.ConditionEvaluationResultBoolean(condition.GetCondition() == "allow-true")
				}
				return authorizer.ConditionsEvaluationResultUnevaluatable()
			},
			want: snapDecision{
				Kind:   "Deny",
				Reason: `3.example.com: {something later denies}`,
			},
		},
		{
			name: "first deny conditionsmap => NoOpinion, second conditional allow, noopinion, finally Allow => Allow",
			decision: unionDecision(
				mkCM(
					cnd(effectDeny, "deny-false", "a", "transparent", ""),
				),
				mkCM(
					cnd(effectAllow, "b", "b", "opaque", ""),
				),
				authorizer.ConditionsAwareDecisionNoOpinion("", nil),
				authorizer.ConditionsAwareDecisionAllow("something later allows", nil),
			),
			builtinConditionsEvaluator: func(_ context.Context, condition authorizer.Condition, _ authorizer.ConditionsData) authorizer.ConditionEvaluationResult {
				if condition.GetType() == "transparent" {
					return authorizer.ConditionEvaluationResultBoolean(condition.GetCondition() == "deny-true")
				}
				return authorizer.ConditionsEvaluationResultUnevaluatable()
			},
			want: snapDecision{
				Kind:   "Allow",
				Reason: `3.example.com: {something later allows}`,
			},
		},
		{
			name: "evaluateConditionFn can be nil, and evaluation to concrete can still succeed",
			decision: unionDecision(
				mkCM(
					// 该项应优先，最终结果为 Deny
					effectCondition{
						effect: effectDeny,
						cond: authorizer.GenericCondition{
							ID: "foo",
							EvaluateFunc: func(ctx context.Context, data authorizer.ConditionsData) authorizer.ConditionEvaluationResult {
								return authorizer.ConditionEvaluationResultBoolean(true)
							},
						},
					},
					cnd(effectAllow, "c", "c", "transparent", ""),
				),
				mkCM(
					cnd(effectAllow, "c", "c", "transparent", ""),
					cnd(effectDeny, "d", "d", "opaque", ""),
				),
				authorizer.ConditionsAwareDecisionDeny("something later denies", nil),
			),
			want: snapDecision{Kind: "Deny", Reason: `0.example.com: {condition "foo" denied the request}`},
		},
		{
			name: "evaluateConditionFn can be nil, and refinement can still happen",
			decision: unionDecision(
				mkCM(
					// 该条件应从精化后的判定中移除
					effectCondition{
						effect: effectDeny,
						cond: authorizer.GenericCondition{
							ID: "foo",
							EvaluateFunc: func(ctx context.Context, data authorizer.ConditionsData) authorizer.ConditionEvaluationResult {
								return authorizer.ConditionEvaluationResultBoolean(false)
							},
						},
					},
					cnd(effectAllow, "c", "c", "transparent", ""),
				),
				mkCM(
					cnd(effectAllow, "c", "c", "transparent", ""),
					cnd(effectDeny, "d", "d", "opaque", ""),
				),
				authorizer.ConditionsAwareDecisionDeny("something later denies", nil),
			),
			want: snapDecision{
				Kind: "Union",
				Union: []snapDecision{
					{
						Kind: "ConditionsMap",
						CM: &snapCM{
							Allow: []snapCondition{{ID: "c", Condition: "c", Type: "transparent"}},
						},
					},
					{
						Kind: "ConditionsMap",
						CM: &snapCM{
							Deny:  []snapCondition{{ID: "d", Condition: "d", Type: "opaque"}},
							Allow: []snapCondition{{ID: "c", Condition: "c", Type: "transparent"}},
						},
					},
					{Kind: "Deny", Reason: "something later denies"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := authorizer.PartiallyEvaluateConditionsAwareDecision(
				t.Context(),
				tt.decision,
				nil,
				tt.builtinConditionsEvaluator,
			)
			if diff := cmp.Diff(tt.want, snapshotDecision(got)); diff != "" {
				t.Errorf("decision mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// conditionEffect 把 Condition 归入 ConditionsMap 的 deny/noOpinion/allow 分片。
// 常量顺序与 ConditionsAwareDecisionConditionsMap 的参数顺序一致。
type conditionEffect int

const (
	effectDeny conditionEffect = iota
	effectNoOpinion
	effectAllow
)

// effectCondition 把 Condition 与其应归入的 effect 分片配对。
type effectCondition struct {
	effect conditionEffect
	cond   authorizer.Condition
}

// snapDecision 是可用于深度比较的判定树快照。
//
// 接口类型无法直接用 cmp.Diff 比较，因此把决策树转换成纯数据结构后再比对。
type snapDecision struct {
	Kind   string // "Allow" | "Deny" | "NoOpinion" | "ConditionsMap" | "Union"
	Reason string
	CM     *snapCM
	Union  []snapDecision
}

// snapCM 是 ConditionsMap 中三类条件的有序快照。
type snapCM struct {
	Deny      []snapCondition
	NoOpinion []snapCondition
	Allow     []snapCondition
}

// snapCondition 是单个条件用于比较的字段子集。
type snapCondition struct {
	ID          string
	Condition   string
	Type        string
	Description string
}

// snapshotDecision 递归地把决策树转换为可比较的快照。
func snapshotDecision(d authorizer.ConditionsAwareDecision) snapDecision {
	switch {
	case d.IsAllow():
		return snapDecision{Kind: "Allow", Reason: d.Reason()}
	case d.IsDeny():
		return snapDecision{Kind: "Deny", Reason: d.Reason()}
	case d.IsNoOpinion():
		return snapDecision{Kind: "NoOpinion", Reason: d.Reason()}
	case d.IsConditionsMap():
		return snapDecision{Kind: "ConditionsMap", CM: snapshotConditionsMap(d.ConditionsMap())}
	case d.IsUnion():
		var subs []snapDecision
		for _, sub := range d.UnionedDecisions() {
			subs = append(subs, snapshotDecision(sub))
		}
		return snapDecision{Kind: "Union", Union: subs}
	}
	return snapDecision{Kind: "Unknown"}
}

// snapshotConditionsMap 按 Deny/NoOpinion/Allow 三类分别快照条件集合。
func snapshotConditionsMap(cm authorizer.ConditionsMap) *snapCM {
	out := &snapCM{}
	for c := range cm.DenyConditions() {
		out.Deny = append(out.Deny, snapshotCondition(c))
	}
	for c := range cm.NoOpinionConditions() {
		out.NoOpinion = append(out.NoOpinion, snapshotCondition(c))
	}
	for c := range cm.AllowConditions() {
		out.Allow = append(out.Allow, snapshotCondition(c))
	}
	return out
}

// snapshotCondition 提取条件的可比较字段。
func snapshotCondition(c authorizer.Condition) snapCondition {
	return snapCondition{
		ID:          c.GetID(),
		Condition:   c.GetCondition(),
		Type:        c.GetType(),
		Description: c.GetDescription(),
	}
}
