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

// conditionsAwareDecisionType 是一个类似枚举的小类型，用于记录 ConditionsAwareDecision 属于哪种类型。
// 这些取值绝不能暴露给本包之外的用户，也不得用于记录决策类型之外的用途。
// 取值刻意与 Decision 错开（11-14），避免类型转换或日志打印时与无条件决策混淆。
type conditionsAwareDecisionType int

const (
	// conditionsAwareDecisionTypeDeny 表示无条件 Deny 决策。
	// 取零值，使 ConditionsAwareDecision{}.IsDeny() == true
	conditionsAwareDecisionTypeDeny conditionsAwareDecisionType = 0
	// conditionsAwareDecisionTypeAllow 表示无条件 Allow 决策。
	// 取值与 DecisionAllow 不同，避免两者混淆。
	conditionsAwareDecisionTypeAllow conditionsAwareDecisionType = 11
	// conditionsAwareDecisionTypeNoOpinion 表示无条件 NoOpinion 决策。
	// 取值与 DecisionNoOpinion 不同，避免两者混淆。
	conditionsAwareDecisionTypeNoOpinion conditionsAwareDecisionType = 12
	// conditionsAwareDecisionTypeConditionsMap 表示条件性的 ConditionsMap 决策。
	conditionsAwareDecisionTypeConditionsMap conditionsAwareDecisionType = 13
	// conditionsAwareDecisionTypeUnion 表示条件性的 Union 决策。
	conditionsAwareDecisionTypeUnion conditionsAwareDecisionType = 14
)

// ConditionsAwareDecision 表示一个「感知条件」的授权决策，是以下五种变体的枚举类型：
// - Allow：无条件放行。
// - Deny：无条件拒绝。
// - NoOpinion：无条件无意见。
// - Conditional：取决于此前尚未见到的数据。
// - Union：由子决策构成的有序列表，形成一棵决策树。
//
// 零值（ConditionsAwareDecision{}）等价于 ConditionsAwareDecisionDeny()。
// ConditionsAwareDecision 按值传递，且构造完成之后不可变，因此可以在并发评估中安全共享；
// 条件树由 Union 与 ConditionsMap 递归组成，评估到叶子才得到最终结论。
type ConditionsAwareDecision struct {
	decisionType conditionsAwareDecisionType

	conditionsMap ConditionsMap
	union         ConditionsAwareDecisionUnion

	reason string
	err    error
}

// ConditionsAwareDecisionDeny 用给定的 reason 和 error 构造一个 Deny 决策。
// Deny 对应结构体零值，使未初始化的决策天然 fail-closed。
func ConditionsAwareDecisionDeny(reason string, err error) ConditionsAwareDecision {
	return ConditionsAwareDecision{
		// conditionsAwareDecisionTypeDeny == 0 == 零值
		// => ConditionsAwareDecision{} == ConditionsAwareDecisionDeny()
		decisionType: conditionsAwareDecisionTypeDeny,
		reason:       reason,
		err:          err,
	}
}

// ConditionsAwareDecisionAllow 用给定的 reason 和 error 构造 Allow 决策。
func ConditionsAwareDecisionAllow(reason string, err error) ConditionsAwareDecision {
	return ConditionsAwareDecision{
		decisionType: conditionsAwareDecisionTypeAllow,
		reason:       reason,
		err:          err,
	}
}

// ConditionsAwareDecisionNoOpinion 用给定的 reason 和 error 构造 NoOpinion 决策。
func ConditionsAwareDecisionNoOpinion(reason string, err error) ConditionsAwareDecision {
	return ConditionsAwareDecision{
		decisionType: conditionsAwareDecisionTypeNoOpinion,
		reason:       reason,
		err:          err,
	}
}

// ConditionsAwareDecisionFromParts 供「不感知条件」的 Authorizer 实现使用，
// 以便把 ConditionsAwareAuthorize 实现为：
// "return authorizer.ConditionsAwareDecisionFromParts(self.Authorize(ctx, a))"
// 未知的 Decision 值一律按 Deny 处理并附加错误，保证新增枚举值时
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

// IsAllow 在决策为无条件 Allow 时返回 true。
func (d ConditionsAwareDecision) IsAllow() bool {
	return d.decisionType == conditionsAwareDecisionTypeAllow
}

// IsNoOpinion 在决策为无条件 NoOpinion 时返回 true。
func (d ConditionsAwareDecision) IsNoOpinion() bool {
	return d.decisionType == conditionsAwareDecisionTypeNoOpinion
}

// IsDeny 在决策为无条件 Deny 时返回 true。
func (d ConditionsAwareDecision) IsDeny() bool {
	return d.decisionType == conditionsAwareDecisionTypeDeny // == 0 == zero value
}

// IsConditionsMap 在决策为条件型（带一组待求值条件）时返回 true。
func (d ConditionsAwareDecision) IsConditionsMap() bool {
	return d.decisionType == conditionsAwareDecisionTypeConditionsMap
}

// ConditionsMap 返回 ConditionsMap，仅当 IsConditionsMap 为 true 时非空。
func (d ConditionsAwareDecision) ConditionsMap() ConditionsMap {
	return d.conditionsMap
}

// IsUnion 在决策由多个子决策以树状结构组合而成时返回 true。
func (d ConditionsAwareDecision) IsUnion() bool {
	return d.decisionType == conditionsAwareDecisionTypeUnion
}

// IsUnconditional 在 d 是 Allow、Deny 或 NoOpinion 时返回 true。
// 调用方可用它判断是否必须进入条件求值路径。
func (d ConditionsAwareDecision) IsUnconditional() bool {
	return d.IsAllow() || d.IsDeny() || d.IsNoOpinion()
}

// unconditionalParts 把一个 ConditionsAwareDecision 转换成 Authorize 所期望的三个返回值。
// 如果决策是条件型的，那么当条件中至少存在一个 Deny 条件时返回 Deny，否则返回 NoOpinion。
// 该函数应当在 IsUnconditional() == true 时被调用。
//
// 如果授权器本身是感知条件的，它可以只在 ConditionsAwareAuthorize 方法里实现真正的业务逻辑，
// 而把 Authorize() 实现为 "return self.ConditionsAwareAuthorize(ctx, attrs).unconditionalParts()"
//
// 目前保持私有，以免鼓励调用方在不需要的位置执行感知条件的逻辑。
// 把条件决策降级为无条件决策时采取 fail-closed——返回 Deny 且不返回
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
		// 此处不返回 error，否则 HTTP 层可能响应 500 而不是 403。
		// 对上文在 Authorize 中调用本函数的场景，不返回 error 很重要：
		// 始终 fail closed 是合理的——一旦走到这里，请求方
		// 并未获得任何无条件授权。
		return d.FailureDecision(), "failed closed: tried to return conditional decision to conditions-unaware authorizer", nil
	}
}

// PossibleDecisions 列出该 ConditionsAwareDecision 可能出现的所有决策结果。
// 返回值是 {Allow, Deny, NoOpinion} 的子集，但绝不会是空集；若集合中只有一个取值，
// 说明该决策是无条件的。集合语义用于判断"是否可能拒绝"，是 FailureDecision 的决策依据。
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

// FailureDecision 返回 Deny 或 NoOpinion，供调用方在处理 ConditionsAwareDecision
// 过程中遇到无法恢复的错误时使用。如果该决策本身或其求值结果可能是 Deny，
// 本函数返回 DecisionDeny，以便作为保守近似而 fail closed；否则返回 NoOpinion。
// 也就是说：只要条件树中存在任何可能 Deny 的分支，就保守地返回 Deny；
// 否则返回 NoOpinion，让后续授权器继续判断。
func (d ConditionsAwareDecision) FailureDecision() Decision {
	if d.PossibleDecisions().Has(DecisionDeny) {
		return DecisionDeny
	}
	return DecisionNoOpinion
}

// ContainsUnconditionalAllowOrDeny 在该 union 的决策树中至少包含一个 Allow 或 Deny
// 决策时返回 true。它用于判断是否值得做条件求值——若树中不存在任何 Allow/Deny 叶子，
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

// UnionedDecisions 返回被 union 的子决策的迭代器。
// 当且仅当 IsUnion() == true 时该迭代器非空，且子决策的迭代顺序保持不变。
// 保留顺序是因为 union 的语义依赖顺序（先到先得/短路），迭代时不能重排。
func (d ConditionsAwareDecision) UnionedDecisions() iter.Seq2[string, ConditionsAwareDecision] {
	return func(yield func(string, ConditionsAwareDecision) bool) {
		for _, subDecision := range d.union.inner {
			if !yield(subDecision.conditionalAuthorizerName, subDecision.d) {
				return
			}
		}
	}
}

// Reason 返回构造无条件决策时提供的 reason。
//
// 条件决策的 Reason 为空字符串。
func (d ConditionsAwareDecision) Reason() string {
	return d.reason
}

// Error 返回构造无条件决策时提供的 error。
//
// 条件决策的 Error 为 nil。
func (d ConditionsAwareDecision) Error() error {
	return d.err
}

// String 返回该决策的人类可读表示。输出刻意不包含敏感对象内容，
// 只含 reason/error 与条件数量，可安全写入日志与审计。
func (d ConditionsAwareDecision) String() string {
	if d.IsUnion() {
		// union 决策中 d.reason 与 d.err 恒为零值，无需考虑。
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
	// Deny 的写法保证：若其他模式都不适用，
	// IsDenied() 为 true。
	return fmt.Sprintf("Deny%s", paramsStr())
}
