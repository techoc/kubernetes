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
	"context"
	"fmt"
	"iter"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/sets"
)

// ConditionsMap 是条件集合，每个条件有 map 内唯一的
// ID，是从 ConditionsData 到 bool 的函数，效果为 Allow/Deny/NoOpinion。
// 必须通过 ConditionsAwareDecisionConditionsMap 构造。
// 构造时会校验所有条件。
//
// 条件按效果分组存放，求值时 Deny 优先于 NoOpinion，NoOpinion 优先于 Allow，
// 因此仅凭各组的非空性就能推导出可能的结果集合。
type ConditionsMap struct {
	//不变量：len(denyConditions) != 0 || len(allowConditions) != 0
	//
	//这里用切片而非真正的 map：ConditionsMap 不需要按键查找单个元素。
	//之所以叫 "map"，是因为强制了 ID（键）在所有条件（值）中唯一。
	//
	denyConditions      []Condition
	noOpinionConditions []Condition
	allowConditions     []Condition
}

// 当处理 ConditionsAwareDecision 遇到不可恢复错误时，调用方可以使用
// FailureDecision 返回的 Deny 或 NoOpinion 决策。若该决策可能求值为 Deny，
// 则返回 DecisionDeny，以保守近似实现 fail closed。
func (c ConditionsMap) FailureDecision() Decision {
	if c.PossibleDecisions().Has(DecisionDeny) {
		//注意：仅当 len(denyConditions) > 0 时才会如此。
		return DecisionDeny
	}
	return DecisionNoOpinion
}

// Length 返回 map 中元素个数。
func (c ConditionsMap) Length() int {
	return len(c.denyConditions) + len(c.noOpinionConditions) + len(c.allowConditions)
}

// DenyConditions 返回此 map 中的 Deny 条件。
// 顺序与传给 ConditionsAwareDecisionConditionsMap 时一致。
func (c ConditionsMap) DenyConditions() iter.Seq[Condition] {
	return func(yield func(Condition) bool) {
		for _, cond := range c.denyConditions {
			if !yield(cond) {
				return
			}
		}
	}
}

// NoOpinionConditions 返回此 map 中的 NoOpinion 条件。
// 顺序与传给 ConditionsAwareDecisionConditionsMap 时一致。
func (c ConditionsMap) NoOpinionConditions() iter.Seq[Condition] {
	return func(yield func(Condition) bool) {
		for _, cond := range c.noOpinionConditions {
			if !yield(cond) {
				return
			}
		}
	}
}

// AllowConditions 返回此 map 中的 Allow 条件。
// 顺序与传给 ConditionsAwareDecisionConditionsMap 时一致。
func (c ConditionsMap) AllowConditions() iter.Seq[Condition] {
	return func(yield func(Condition) bool) {
		for _, cond := range c.allowConditions {
			if !yield(cond) {
				return
			}
		}
	}
}

// PossibleDecisions 说明此 ConditionsAwareDecision 可能产生的决策结果。
// 返回值是 {Allow, Deny, NoOpinion} 的子集，但不会为空集。
// 若只含一个值，则表示决策是无条件的。
func (c ConditionsMap) PossibleDecisions() sets.Set[Decision] {
	possibleDecisions := sets.New(DecisionNoOpinion)
	if len(c.allowConditions) > 0 {
		possibleDecisions.Insert(DecisionAllow)
	}
	if len(c.denyConditions) > 0 {
		possibleDecisions.Insert(DecisionDeny)
	}
	return possibleDecisions
}

// MaxConditionsPerMap 是单个 ConditionsMap 允许的最大条件数。
//
// 上限用于约束求值成本：每次请求都要遍历全部条件，且 CEL 表达式可能很昂贵。
const MaxConditionsPerMap = 128

// ConditionsAwareDecisionConditionsMap 创建一个 ConditionsMap 决策。
// 条件按效果分组为 Deny、NoOpinion 与 Allow，规则如下：
// - Deny：任一 Deny 条件求值为 true，则 ConditionsMap 必然为 Deny，
// 且不再询问后续授权器。
// - NoOpinion：任一 NoOpinion 条件为 true，则该授权器的 ConditionsMap
// 不可能再得到 Allow，只能是 Deny 或 NoOpinion，取决于是否有 Deny 条件为真。
// 但链上后续授权器仍可 Allow 或 Deny。
// 它相当于较弱的拒绝：只覆盖该授权器自身的 allow 策略。
// - Allow：任一 Allow 条件为 true，ConditionsMap 求值为 Allow，
// 除非同时有 Deny/NoOpinion 条件为真（此时它们优先）。
//
// 所有条件必须为非 nil，否则此函数 panic。
//
// 构造期的所有错误都不会 panic，而是返回一个"失败关闭"的决策：存在 Deny 条件时返回 Deny，
// 否则返回 NoOpinion，确保配置错误不会意外放行请求。
func ConditionsAwareDecisionConditionsMap(denyConditions []Condition, noOpinionConditions []Condition, allowConditions []Condition) ConditionsAwareDecision {
	//存在 Deny 条件时 Deny 是可能的决策，因此此时应以 Deny fail closed。
	hasDenyEffect := len(denyConditions) > 0
	makeFailClosedError := func(err error) ConditionsAwareDecision {
		if hasDenyEffect {
			return ConditionsAwareDecisionDeny("failed closed", err)
		}
		return ConditionsAwareDecisionNoOpinion("failed closed", err)
	}

	//强制条件数下限为 1，并限制上限。
	conditionsAmount := len(denyConditions) + len(noOpinionConditions) + len(allowConditions)
	if conditionsAmount == 0 {
		//这里不用 makeFailClosedError 而直接返回 NoOpinion：此分支没有 Deny 条件，
		return ConditionsAwareDecisionNoOpinion("no conditions", fmt.Errorf("at least one condition must be passed to ConditionsAwareDecisionConditionsMap(), got none"))
	}
	if conditionsAmount > MaxConditionsPerMap {
		return makeFailClosedError(fmt.Errorf("too many conditions: %d exceeds maximum of %d", conditionsAmount, MaxConditionsPerMap))
	}
	//短路分支：若无 Allow 或 Deny 条件，则无论数据如何，此 ConditionsMap
	//只能求值为 NoOpinion，因此直接折叠。
	//这样维持了 len(denyConditions) != 0 || len(allowConditions) != 0 的不变量。
	if len(denyConditions) == 0 && len(allowConditions) == 0 {
		return ConditionsAwareDecisionNoOpinion("only NoOpinion conditions always evaluate to NoOpinion", nil)
	}

	// ID 在整张 map 内唯一，跨组也不能重复，否则无法稳定地引用某个条件。
	seenIDs := sets.New[string]()

	if err := validateConditions(seenIDs, denyConditions); err != nil {
		return makeFailClosedError(err)
	}
	if err := validateConditions(seenIDs, noOpinionConditions); err != nil {
		return makeFailClosedError(err)
	}
	if err := validateConditions(seenIDs, allowConditions); err != nil {
		return makeFailClosedError(err)
	}

	return ConditionsAwareDecision{
		decisionType: conditionsAwareDecisionTypeConditionsMap,
		conditionsMap: ConditionsMap{
			//不共享调用方持有的引用，确保构造后 ConditionsMap 不可变。
			denyConditions:      slices.Clone(denyConditions),
			noOpinionConditions: slices.Clone(noOpinionConditions),
			allowConditions:     slices.Clone(allowConditions),
		},
	}
}

// validateConditions 校验条件 ID 的唯一性与格式；ID 与类型都要求是合法 label key，
// 以便在指标和审计中作为标签使用。
func validateConditions(seenIDs sets.Set[string], conditions []Condition) error {
	for _, condition := range conditions {

		id := condition.GetID()
		if seenIDs.Has(id) {
			return fmt.Errorf("duplicate condition ID %q", id)
		}
		seenIDs.Insert(id)

		//按 label key 规则校验 ID。
		if errs := content.IsLabelKey(id); len(errs) > 0 {
			return fmt.Errorf("invalid condition ID %q: %s", id, strings.Join(errs, "; "))
		}

		//若设置了 type，也按 label key 规则校验。
		if conditionType := condition.GetType(); len(conditionType) != 0 {
			if errs := content.IsLabelKey(conditionType); len(errs) > 0 {
				return fmt.Errorf("invalid condition type %q: %s", conditionType, strings.Join(errs, "; "))
			}
		}
	}
	return nil
}

// GenericCondition 是 Condition 接口的通用实现，
// 将 EvaluateFunc 设为非 nil 时，可选择支持进程内快速求值。
type GenericCondition struct {
	// ID 在所属 ConditionsMap 内唯一。
	ID string
	// Condition 是条件的原始表达式文本，用于展示与审计。
	Condition string
	// Type 是可选的分类标签。
	Type string
	// Description 是面向用户的说明。
	Description string
	// EvaluateFunc 非 nil 时可在进程内直接求值，避免回源到 CEL 引擎。
	EvaluateFunc func(ctx context.Context, data ConditionsData) ConditionEvaluationResult
}

var _ Condition = GenericCondition{}

// GetID 返回条件在 map 内的唯一标识。
func (c GenericCondition) GetID() string {
	return c.ID
}

// GetCondition 返回条件的原始表达式文本。
func (c GenericCondition) GetCondition() string {
	return c.Condition
}

// GetType 返回条件类型标签，可能为空。
func (c GenericCondition) GetType() string {
	return c.Type
}

// GetDescription 返回面向用户的描述。
func (c GenericCondition) GetDescription() string {
	return c.Description
}

// Evaluate 执行条件求值；未提供 EvaluateFunc 时返回"无法求值"，
// 由调用方按失败关闭策略处理，而不是当作 true/false。
func (c GenericCondition) Evaluate(ctx context.Context, data ConditionsData) ConditionEvaluationResult {
	if c.EvaluateFunc == nil {
		return ConditionsEvaluationResultUnevaluatable()
	}
	return c.EvaluateFunc(ctx, data)
}
