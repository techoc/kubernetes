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

package authorizer

import (
	"fmt"
	"iter"
	"strings"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
)

// conditionsAwareDecisionType is a small enum-like type for keeping track of what type a ConditionsAwareDecision is.
// These values must never be exposed to users outside of this package, and should not be used for anything else than
// keeping track of what type of a ConditionsAwareDecision is.
//
// 中文说明：刻意与 Decision 的取值错开（11-14），避免类型转换或日志打印时
// 与无条件决策混淆。
type conditionsAwareDecisionType int

const (
	// conditionsAwareDecisionTypeDeny represents the unconditional Deny decision.
	// It is zero such that ConditionsAwareDecision{}.IsDeny() == true
	conditionsAwareDecisionTypeDeny conditionsAwareDecisionType = 0
	// conditionsAwareDecisionTypeAllow represents the unconditional Allow decision.
	// It has a different value from DecisionAllow to never be conflated with that.
	conditionsAwareDecisionTypeAllow conditionsAwareDecisionType = 11
	// conditionsAwareDecisionTypeNoOpinion represents the unconditional NoOpinion decision.
	// It has a different value from DecisionNoOpinion to never be conflated with that.
	conditionsAwareDecisionTypeNoOpinion conditionsAwareDecisionType = 12
	// conditionsAwareDecisionTypeConditionsMap represents the conditional ConditionsMap decision.
	conditionsAwareDecisionTypeConditionsMap conditionsAwareDecisionType = 13
	// conditionsAwareDecisionTypeUnion represents a conditional Union decision.
	conditionsAwareDecisionTypeUnion conditionsAwareDecisionType = 14
)

// ConditionsAwareDecision models an authorization decision that is conditions-aware.
// It is an enum type of the following five variants:
// - Allow: unconditional Allow.
// - Deny: unconditional Deny.
// - NoOpinion: unconditional NoOpinion.
// - Conditional: conditional on some previously-unseen data.
// - Union: an ordered list of sub-decisions, which forms a tree of decisions.
//
// The zero value (ConditionsAwareDecision{}) is equivalent to ConditionsAwareDecisionDeny().
// A ConditionsAwareDecision is passed by value.
// Important: A ConditionsAwareDecision is immutable after construction.
//
// 中文说明：不可变 + 按值传递，使决策可以安全地在并发评估中共享；条件树
// 由 Union 与 ConditionsMap 递归组成，评估到叶子才得到最终结论。
type ConditionsAwareDecision struct {
	decisionType conditionsAwareDecisionType

	conditionsMap ConditionsMap
	union         ConditionsAwareDecisionUnion

	reason string
	err    error
}

// ConditionsAwareDecisionDeny constructs a Deny decision with the given reason and error.
//
// 中文说明：Deny 取零值，使未初始化的决策天然 fail-closed。
func ConditionsAwareDecisionDeny(reason string, err error) ConditionsAwareDecision {
	return ConditionsAwareDecision{
		// conditionsAwareDecisionTypeDeny == 0 == zero value
		// => ConditionsAwareDecision{} == ConditionsAwareDecisionDeny()
		decisionType: conditionsAwareDecisionTypeDeny,
		reason:       reason,
		err:          err,
	}
}

// ConditionsAwareDecisionAllow constructs an Allow decision with the given reason and error.
func ConditionsAwareDecisionAllow(reason string, err error) ConditionsAwareDecision {
	return ConditionsAwareDecision{
		decisionType: conditionsAwareDecisionTypeAllow,
		reason:       reason,
		err:          err,
	}
}

// ConditionsAwareDecisionNoOpinion constructs a NoOpinion decision with the given reason and error.
func ConditionsAwareDecisionNoOpinion(reason string, err error) ConditionsAwareDecision {
	return ConditionsAwareDecision{
		decisionType: conditionsAwareDecisionTypeNoOpinion,
		reason:       reason,
		err:          err,
	}
}

// ConditionsAwareDecisionFromParts is meant to be used by conditions-unaware Authorizer implementations
// in order to implement ConditionsAwareAuthorize as:
// "return authorizer.ConditionsAwareDecisionFromParts(self.Authorize(ctx, a))"
//
// 中文说明：未知的 Decision 值一律按 Deny 处理并附加错误，保证新增枚举值时
// 不会因遗漏分支而意外放行。
func ConditionsAwareDecisionFromParts(unconditional Decision, reason string, err error) ConditionsAwareDecision {
	switch unconditional {
	case DecisionAllow:
		return ConditionsAwareDecisionAllow(reason, err)
	case DecisionNoOpinion:
		return ConditionsAwareDecisionNoOpinion(reason, err)
	case DecisionDeny:
		return ConditionsAwareDecisionDeny(reason, err)
	default:
		return ConditionsAwareDecisionDeny(reason, utilerrors.NewAggregate(
			[]error{
				err,
				fmt.Errorf("unknown unconditional decision type: %d", unconditional),
			},
		))
	}
}

// IsAllow returns true if the decision is an unconditional Allow.
func (d ConditionsAwareDecision) IsAllow() bool {
	return d.decisionType == conditionsAwareDecisionTypeAllow
}

// IsNoOpinion returns true if the decision is an unconditional NoOpinion.
func (d ConditionsAwareDecision) IsNoOpinion() bool {
	return d.decisionType == conditionsAwareDecisionTypeNoOpinion
}

// IsDeny returns true if the decision is an unconditional Deny.
func (d ConditionsAwareDecision) IsDeny() bool {
	return d.decisionType == conditionsAwareDecisionTypeDeny // == 0 == zero value
}

// IsConditionsMap returns true if the decision is a conditional response
// with a map of conditions to evaluate.
func (d ConditionsAwareDecision) IsConditionsMap() bool {
	return d.decisionType == conditionsAwareDecisionTypeConditionsMap
}

// ConditionsMap returns the ConditionsMap, which is non-empty
// if and only if IsConditionsMap is true.
func (d ConditionsAwareDecision) ConditionsMap() ConditionsMap {
	return d.conditionsMap
}

// IsUnion returns true if the decision consists of other sub-decisions
// unioned together in a tree-like structure.
func (d ConditionsAwareDecision) IsUnion() bool {
	return d.decisionType == conditionsAwareDecisionTypeUnion
}

// IsUnconditional is true if d is Allow, Deny or NoOpinion.
//
// 中文说明：调用方可用它判断是否必须进入条件求值路径。
func (d ConditionsAwareDecision) IsUnconditional() bool {
	return d.IsAllow() || d.IsDeny() || d.IsNoOpinion()
}

// unconditionalParts turns a ConditionsAwareDecision into the
// triple that Authorize expects. If the decision is
// conditional, the returned condition is Deny if there were at least
// some Deny condition, otherwise NoOpinion.
// This function is meant to be called when IsUnconditional() == true.
//
// If the authorizer is conditions-aware, it can choose to only implement
// real business logic in the ConditionsAwareAuthorize method, and implement
// Authorize() as "return self.ConditionsAwareAuthorize(ctx, attrs).unconditionalParts()"
//
// Private for now, to not encourage callers to perform conditions-aware logic where not needed.
//
// 中文说明：把条件决策降级为无条件决策时采取 fail-closed——返回 Deny 且不返回
// error，因为返回 error 会让 HTTP 层响应 500 而非 403，且暴露内部状态。
func (d ConditionsAwareDecision) unconditionalParts() (Decision, string, error) {
	switch {
	case d.IsAllow():
		return DecisionAllow, d.Reason(), d.Error()
	case d.IsDeny():
		return DecisionDeny, d.Reason(), d.Error()
	case d.IsNoOpinion():
		return DecisionNoOpinion, d.Reason(), d.Error()
	default:
		// An error is not returned here, as that could yield a HTTP response code of 500 instead of 403.
		// For the use-case described above with regards to calling this function in Authorize, not returning
		// an error is important, as it is valid to always fail closed, as if this happens, no unconditional
		// permissions were given the requestor.
		return d.FailureDecision(), "failed closed: tried to return conditional decision to conditions-unaware authorizer", nil
	}
}

// PossibleDecisions details what are the possible decision outcomes of this
// ConditionsAwareDecision. The return value is a subset of {Allow, Deny, NoOpinion},
// but never the empty set. If the set only contains a single value, it means the
// decision is unconditional.
//
// 中文说明：集合语义用于判断"是否可能拒绝"，是 FailureDecision 的决策依据。
func (d ConditionsAwareDecision) PossibleDecisions() sets.Set[Decision] {
	switch {
	case d.IsAllow():
		return sets.New(DecisionAllow)
	case d.IsNoOpinion():
		return sets.New(DecisionNoOpinion)
	case d.IsConditionsMap():
		return d.ConditionsMap().PossibleDecisions()
	case d.IsUnion():
		return d.union.PossibleDecisions()
	default: // default case Deny
		return sets.New(DecisionDeny)
	}
}

// FailureDecision returns either a Deny or NoOpinion decision that the caller can
// use if the caller encounters an unrecoverable error while processing a
// ConditionsAwareDecision. If this decision is or could evaluate to Deny
// this function returns DecisionDeny to the caller, in order to fail closed
// as a conservative approximation. Otherwise, NoOpinion is returned.
//
// 中文说明：只要条件树中存在任何可能 Deny 的分支，就保守地返回 Deny；
// 否则返回 NoOpinion，让后续授权器继续判断。
func (d ConditionsAwareDecision) FailureDecision() Decision {
	if d.PossibleDecisions().Has(DecisionDeny) {
		return DecisionDeny
	}
	return DecisionNoOpinion
}

// ContainsUnconditionalAllowOrDeny returns true whether there union contains at least one
// Allow or Deny decision within the tree of decisions.
//
// 中文说明：用于判断是否值得做条件求值——若树中不存在任何 Allow/Deny 叶子，
// 求值不可能改变最终结论。
func (d ConditionsAwareDecision) ContainsUnconditionalAllowOrDeny() bool {
	if d.IsAllow() || d.IsDeny() {
		return true
	}
	if d.IsNoOpinion() || d.IsConditionsMap() {
		return false
	}
	return d.union.ContainsUnconditionalAllowOrDeny()
}

// UnionedDecisions returns an iterator for unioned sub-decisions.
// This iterator is non-empty if and only if IsUnion() == true.
// Sub-decisions iteration order is preserved.
//
// 中文说明：保留顺序是因为 union 的语义依赖顺序（先到先得/短路），
// 迭代时不能重排。
func (d ConditionsAwareDecision) UnionedDecisions() iter.Seq2[string, ConditionsAwareDecision] {
	return func(yield func(string, ConditionsAwareDecision) bool) {
		for _, subDecision := range d.union.inner {
			if !yield(subDecision.conditionalAuthorizerName, subDecision.d) {
				return
			}
		}
	}
}

// Reason returns the reason supplied when constructing
// an unconditional decision.
// Reason is an empty string for conditional decisions.
func (d ConditionsAwareDecision) Reason() string {
	return d.reason
}

// Error returns the error supplied when constructing
// an unconditional decision.
// Error is nil for conditional decisions.
func (d ConditionsAwareDecision) Error() error {
	return d.err
}

// String returns a human-readable representation of the decision.
//
// 中文说明：输出刻意不包含敏感对象内容，只含 reason/error 与条件数量，
// 可安全写入日志与审计。
func (d ConditionsAwareDecision) String() string {
	if d.IsUnion() {
		// No need to take d.reason or d.err into account, as they are always zero for the union.
		b := strings.Builder{}
		b.WriteString("Union[")
		for i, sub := range d.union.inner {
			if i != 0 {
				b.WriteString(", ")
			}
			b.WriteString(sub.conditionalAuthorizerName)
			b.WriteString(": ")
			b.WriteString(sub.d.String())
		}
		b.WriteByte(']')
		return b.String()
	}
	params := []string{}
	if len(d.reason) != 0 {
		params = append(params, fmt.Sprintf("reason=%q", d.reason))
	}
	if d.err != nil {
		params = append(params, fmt.Sprintf("err=%q", d.err.Error()))
	}
	paramsStr := func() string {
		if len(params) == 0 {
			return ""
		}
		return fmt.Sprintf("(%s)", strings.Join(params, ", "))
	}
	if d.IsAllow() {
		return fmt.Sprintf("Allow%s", paramsStr())
	}
	if d.IsNoOpinion() {
		return fmt.Sprintf("NoOpinion%s", paramsStr())
	}
	if d.IsConditionsMap() {
		if len(d.conditionsMap.denyConditions) != 0 {
			params = append(params, fmt.Sprintf("denies=%d", len(d.conditionsMap.denyConditions)))
		}
		if len(d.conditionsMap.noOpinionConditions) != 0 {
			params = append(params, fmt.Sprintf("noopinions=%d", len(d.conditionsMap.noOpinionConditions)))
		}
		if len(d.conditionsMap.allowConditions) != 0 {
			params = append(params, fmt.Sprintf("allows=%d", len(d.conditionsMap.allowConditions)))
		}
		return fmt.Sprintf("ConditionsMap%s", paramsStr())
	}
	// Deny is written such that if none of the other modes apply,
	// IsDenied() is true.
	return fmt.Sprintf("Deny%s", paramsStr())
}
