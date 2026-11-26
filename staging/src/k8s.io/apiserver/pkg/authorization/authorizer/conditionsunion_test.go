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
	"reflect"
	"slices"
	"testing"

	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// TestConditionsAwareDecisionUnionAdd 考察 Add 方法的簿记行为：它必须拒绝重复的 authorizer
// 名称，并在首个 Allow/Deny 叶子之后停止追加。
// 重复的 authorizer 名称会造成条件重复计数，必须 fail closed；
// 无条件的 Allow/Deny 已经决定最终结果，后续加入的子决策不再影响并集。
func TestConditionsAwareDecisionUnionAdd(t *testing.T) {
	noOp := authorizer.ConditionsAwareDecisionNoOpinion("", nil)
	allow := authorizer.ConditionsAwareDecisionAllow("a", nil)
	deny := authorizer.ConditionsAwareDecisionDeny("d", nil)
	condMapAllow := authorizer.ConditionsAwareDecisionConditionsMap(nil, nil, []authorizer.Condition{authorizer.GenericCondition{ID: "allow-1"}})
	condMapDeny := authorizer.ConditionsAwareDecisionConditionsMap([]authorizer.Condition{authorizer.GenericCondition{ID: "deny-1"}}, nil, nil)

	t.Run("duplicate conditional authorizers fails closed", func(t *testing.T) {
		var u authorizer.ConditionsAwareDecisionUnion
		u.Add("nop", noOp)
		u.Add("dup", condMapAllow)
		u.Add("dup", condMapAllow)

		d := u.ToDecision()
		if !d.IsNoOpinion() {
			t.Errorf("expected NoOpinion (no Deny leaf), got %s", d.String())
		}
		if d.Error().Error() != `duplicate conditionalAuthorizerName "dup"` {
			t.Errorf("expected aggregated duplicate error, got %v", d.Error())
		}
	})

	t.Run("duplicate when outcome could be deny fails closed", func(t *testing.T) {
		var u authorizer.ConditionsAwareDecisionUnion
		u.Add("dup", condMapAllow)
		u.Add("dup", condMapDeny)
		u.Add("a", allow)

		d := u.ToDecision()
		if !d.IsDeny() {
			t.Errorf("expected Deny (deny leaf present), got %s", d.String())
		}
		if d.Error().Error() != `duplicate conditionalAuthorizerName "dup"` {
			t.Errorf("expected aggregated duplicate error, got %v", d.Error())
		}
	})

	t.Run("Allow leaf short-circuits subsequent Adds", func(t *testing.T) {
		var u authorizer.ConditionsAwareDecisionUnion
		u.Add("0.example.com", noOp)
		u.Add("1.example.com", allow)
		// 这些追加必须被 ContainsUnconditionalAllowOrDeny 短路逻辑静默丢弃。
		u.Add("2.example.com", deny)
		u.Add("3.example.com", condMapAllow)

		d := u.ToDecision()
		if !d.IsAllow() {
			t.Errorf("expected Allow after short-circuit, got %s", d.String())
		}

		// 遍历结果并集时，只应看到短路点之前（含该点）的条目。
		seen := map[string]bool{}
		for name := range d.UnionedDecisions() {
			seen[name] = true
		}
		// 被包装的决策是无条件 Allow，因此 UnionedDecisions 为空。
		if len(seen) != 0 {
			t.Errorf("expected unconditional Allow to expose no unioned decisions, got %v", seen)
		}
	})

	t.Run("Deny leaf short-circuits subsequent Adds", func(t *testing.T) {
		var u authorizer.ConditionsAwareDecisionUnion
		u.Add("", noOp)
		u.Add("", deny)
		u.Add("", allow)
		u.Add("3.example.com", condMapAllow)

		d := u.ToDecision()
		if !d.IsDeny() {
			t.Errorf("expected Deny after short-circuit, got %s", d.String())
		}
	})

	t.Run("ConditionsMap leaves do not short-circuit Add", func(t *testing.T) {
		var u authorizer.ConditionsAwareDecisionUnion
		u.Add("0.example.com", condMapAllow)
		u.Add("1.example.com", condMapAllow) // authorizerName 不同，仍会被追加。
		u.Add("2.example.com", noOp)

		d := u.ToDecision()
		if !d.IsUnion() {
			t.Fatalf("expected Union, got %s", d.String())
		}

		var names []string
		for name := range d.UnionedDecisions() {
			names = append(names, name)
		}
		want := []string{"0.example.com", "1.example.com", "2.example.com"}
		if !reflect.DeepEqual(names, want) {
			t.Errorf("UnionedDecisions names = %v, want %v", names, want)
		}
	})
}

// TestConditionsAwareDecisionUnion_AddAfterToDecisionDoesNotMutate 验证：
// 一旦 ConditionsAwareDecisionUnion.ToDecision 产出决策，构建器后续 Add 就不能再改动该决策——
// 构建器内部状态（包括缓存的 possible-decisions 集合）都是防御性复制的。
// （包括缓存的 possible-decisions 集合）都做了防御性复制。
//
// 该不变量保证决策对象可以安全地跨 goroutine 共享，而不受构建器后续修改影响。
func TestConditionsAwareDecisionUnion_AddAfterToDecisionDoesNotMutate(t *testing.T) {
	// 构造一个含两个 ConditionsMap 子决策的并集；两者都不是无条件 Allow/Deny 叶子，
	// 因此构建器不会短路后续的 Add 调用。
	cmAllow1 := authorizer.ConditionsAwareDecisionConditionsMap(
		nil, nil, []authorizer.Condition{authorizer.GenericCondition{ID: "allow-1"}},
	)
	cmAllow2 := authorizer.ConditionsAwareDecisionConditionsMap(
		nil, nil, []authorizer.Condition{authorizer.GenericCondition{ID: "allow-2"}},
	)

	var u authorizer.ConditionsAwareDecisionUnion
	u.Add("first.example.com", cmAllow1)
	u.Add("second.example.com", cmAllow2)
	d := u.ToDecision()
	if !d.IsUnion() {
		t.Fatalf("expected Union decision, got %s", d.String())
	}

	collectNames := func(d authorizer.ConditionsAwareDecision) []string {
		var names []string
		for name := range d.UnionedDecisions() {
			names = append(names, name)
		}
		return names
	}

	namesBefore := []string{"first.example.com", "second.example.com"}
	possibleBefore := d.PossibleDecisions()

	// 再向构建器追加一个子决策，引入此前不存在的新结果（Deny）。
	// 若 ToDecision 没有防御性复制构建器的缓存状态，这次 Add 就会泄漏到已经返回的决策中。
	//
	cmDeny := authorizer.ConditionsAwareDecisionConditionsMap(
		[]authorizer.Condition{authorizer.GenericCondition{ID: "deny-1"}}, nil, nil,
	)
	u.Add("third.example.com", cmDeny)

	if got := collectNames(d); !slices.Equal(got, namesBefore) {
		t.Errorf("UnionedDecisions names changed after builder Add: got %v, want %v (builder mutation must not leak)", got, namesBefore)
	}
	if got := d.PossibleDecisions(); !got.Equal(possibleBefore) {
		t.Errorf("PossibleDecisions changed after builder Add: got %v, want %v (builder mutation must not leak)",
			got.UnsortedList(), possibleBefore.UnsortedList())
	}
}
