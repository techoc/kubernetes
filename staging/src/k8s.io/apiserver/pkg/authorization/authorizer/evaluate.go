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
	"errors"
	"fmt"
	"iter"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

// partialConditionEvaluationResultType 是一个小型枚举，表示 ConditionEvaluationResult 的类型
type partialConditionEvaluationResultType int

const (
	partialConditionEvaluationResultTypeUnevaluatable partialConditionEvaluationResultType = iota
	partialConditionEvaluationResultTypeTrue
	partialConditionEvaluationResultTypeFalse
	partialConditionEvaluationResultTypeError
)

// ConditionEvaluationResult 是一个具有四种取值的枚举类型：
// - true 与 false：求值成功，并求值为该值
// - error：条件可以求值，但求值过程中出错。
// - unevaluatable：条件无法直接求值。这是结构体的零值。
//
// 使用紧凑枚举而非 (bool, error) 二元组，是为了明确区分“求值为 false”和
// “根本无法求值”两种情况——前者参与决策，后者必须留给后续评估器。
type ConditionEvaluationResult struct {
	resultType partialConditionEvaluationResultType
	err        error
}

// ConditionEvaluationResultBoolean 构造一个带布尔取值的求值结果。
func ConditionEvaluationResultBoolean(evalResult bool) ConditionEvaluationResult {
	if evalResult {
		return ConditionEvaluationResult{resultType: partialConditionEvaluationResultTypeTrue}
	}
	return ConditionEvaluationResult{resultType: partialConditionEvaluationResultTypeFalse}
}

// ConditionEvaluationResultError 表示条件可以求值，但求值失败。
// nil 错误会被规范化为非 nil 的哨兵错误，这样调用方始终可以相信
// IsError() 意味着 Error() != nil。
func ConditionEvaluationResultError(err error) ConditionEvaluationResult {
	if err == nil {
		return ConditionEvaluationResult{
			resultType: partialConditionEvaluationResultTypeError,
			err:        errors.New("unspecified evaluation error"),
		}
	}
	return ConditionEvaluationResult{
		resultType: partialConditionEvaluationResultTypeError,
		err:        err,
	}
}

// ConditionsEvaluationResultUnevaluatable 表示无法直接进行条件求值。
// 返回零值：这样任何零值 ConditionEvaluationResult 都被视为“不可求值”，
// 避免调用方忘记显式初始化时被误当作 false 而错误地放行。
func ConditionsEvaluationResultUnevaluatable() ConditionEvaluationResult {
	return ConditionEvaluationResult{
		resultType: partialConditionEvaluationResultTypeUnevaluatable, // == 0（与结构体的零值一致）
	}
}

// IsTrue 表示条件求值成功且结果为 true，这意味着它会影响 ConditionsMap 的决策。
// IsTrue 报告求值是否成功并得出 true。
func (r ConditionEvaluationResult) IsTrue() bool {
	return r.resultType == partialConditionEvaluationResultTypeTrue
}

// IsFalse 表示条件求值成功但结果为 false，因此不被计入考量。
// IsFalse 报告求值是否成功并得出 false。
func (r ConditionEvaluationResult) IsFalse() bool {
	return r.resultType == partialConditionEvaluationResultTypeFalse
}

// IsError 表示条件求值是否失败。
// IsError 报告是否尝试了求值但失败。
func (r ConditionEvaluationResult) IsError() bool {
	return r.resultType == partialConditionEvaluationResultTypeError
}

// Error 返回求值错误（如果有的话）。
func (r ConditionEvaluationResult) Error() error { return r.err }

// IsUnevaluatable 当不属于其他任何取值时为 true，也就是零值的情况。
// IsUnevaluatable 报告是否没有任何求值器能够处理该条件。
func (r ConditionEvaluationResult) IsUnevaluatable() bool {
	return r.resultType == partialConditionEvaluationResultTypeUnevaluatable
}

// Evaluate 主要使用各 Condition 自带的 Evaluate() 函数求值 ConditionsMap，
// 其次在设置了 evaluateConditionFn 时使用它。第一条匹配（true）的条件
// 会短路整个过程，以使求值尽可能高效。
//
// 调用方传入的 evaluateConditionFn 永远不会返回 unevaluatable，因此结果必然
// 收敛为 Allow/Deny/NoOpinion；unconditionalParts 负责剥掉这层包装。
func (c ConditionsMap) Evaluate(ctx context.Context, data ConditionsData, evaluateConditionFn EvaluateConditionFunc) (Decision, string, error) {
	// 这是在通用的私有函数与我们希望暴露给调用方的接口之间做一次转换。
	return partiallyEvaluateConditionsMapInternal(ctx, c, data, func(ctx context.Context, cond Condition, condData ConditionsData) ConditionEvaluationResult {
		// 由于我们从不返回 "unevaluatable"，返回的 ConditionsAwareDecision 必然是 Allow/Deny/NoOpinion 之一，因此可以用 unconditionalParts 将其拆分
		applied, err := evaluateConditionFn(ctx, cond, condData)
		if err != nil {
			return ConditionEvaluationResultError(err)
		}
		return ConditionEvaluationResultBoolean(applied)
	}).unconditionalParts()
}

// partiallyEvaluateConditionsMapInternal 主要使用各 Condition 自带的 Evaluate() 函数求值
// ConditionsMap，其次在设置了 evaluateFunc 时使用它。若 evaluateFunc 非 nil 且从不返回
// ConditionsEvaluationResultUnevaluatable，则返回的决策保证是 Allow/Deny/NoOpinion。
// 不过，本方法也可以用于求值条件子集（例如只支持某类条件的内置条件求值器），
// 对求值器无法识别的条件返回 ConditionsEvaluationResultUnevaluatable。
// 后一种情况下，可能返回一个部分求值的 ConditionsMap。
//
// 求值顺序固定为 Deny -> NoOpinion -> Allow，这是安全性的核心：任何一条 Deny
// 生效都立即否决，绝不会被后面对 Allow 的求值“覆盖”。同一效果内部，
// true > error > unevaluatable > false。
func partiallyEvaluateConditionsMapInternal(ctx context.Context, c ConditionsMap, data ConditionsData, evaluateConditionFn MaybeEvaluateConditionFunc) ConditionsAwareDecision {
	evalCond := func(cond Condition) ConditionEvaluationResult {
		// 首先尝试使用条件自带的求值函数。
		// 若结果为 unevaluatable 且设置了 evaluateConditionFn，则回退到它
		result := cond.Evaluate(ctx, data)
		if result.IsUnevaluatable() && evaluateConditionFn != nil {
			return evaluateConditionFn(ctx, cond, data)
		}
		return result
	}

	// 总体逻辑：Deny > NoOpinion > Allow。在一组效果相同的条件内部，true > error > unevaluatable > false。

	// 先求 Deny：一旦命中直接拒绝，后续条件无需再算。
	if len(c.denyConditions) != 0 {
		appliedReason, errored, unevaluated := conditionsToAppliedErroredUnevaluated(
			c.DenyConditions(), evalCond, "Deny", "denied the request")

		if len(appliedReason) != 0 {
			// 这里不返回任何错误，否则在 WithAuthorization HTTP 过滤器中会把响应变成 500 而不是 403
			// TODO(luxas): 我们可能想修改 WithAuthorization 的逻辑，使授权器在存在 deny 的情况下
			// 也能上报非关键错误。
			return ConditionsAwareDecisionDeny(appliedReason, nil)
		}
		if len(errored) != 0 {
			return ConditionsAwareDecisionDeny("one or more conditional evaluation errors occurred", utilerrors.NewAggregate(errored))
		}
		if len(unevaluated) != 0 {
			return ConditionsAwareDecisionConditionsMap(unevaluated, c.noOpinionConditions, c.allowConditions)
		}
	}

	// 如果走到这里，说明所有 Deny 条件都能求值，且求值结果均为 false、nil

	// Deny 全部为 false，再看 NoOpinion：命中即终止，但返回的是“无意见”。
	if len(c.noOpinionConditions) != 0 {
		appliedReason, errored, unevaluated := conditionsToAppliedErroredUnevaluated(
			c.NoOpinionConditions(), evalCond, "NoOpinion", "evaluated to NoOpinion")

		if len(appliedReason) != 0 {
			// 这里不返回任何错误，否则在 WithAuthorization HTTP 过滤器中会把响应变成 500 而不是 403
			return ConditionsAwareDecisionNoOpinion(appliedReason, nil)
		}
		if len(errored) != 0 {
			return ConditionsAwareDecisionNoOpinion("one or more conditional evaluation errors occurred", utilerrors.NewAggregate(errored))
		}
		if len(unevaluated) != 0 {
			// 注意：如果 len(c.allowConditions) == 0，ConditionsAwareDecisionConditionsMap 会把这折叠成一个无条件的 NoOpinion。
			return ConditionsAwareDecisionConditionsMap(nil, unevaluated, c.allowConditions)
		}
	}

	// 如果走到这里，说明所有 Deny 和 NoOpinion 条件都能求值，且求值结果均为 false、nil

	// 最后看 Allow：只有没有 Deny/NoOpinion 生效时才可能放行。
	if len(c.allowConditions) != 0 {
		appliedReason, errored, unevaluated := conditionsToAppliedErroredUnevaluated(
			c.AllowConditions(), evalCond, "Allow", "allowed the request")

		if len(appliedReason) != 0 {
			// 技术上可以在 Allow 的同时返回（并记录）错误，但对于短路命中 true 的情况
			// 不返回错误，因为此时错误列表可能不完整。
			return ConditionsAwareDecisionAllow(appliedReason, nil)
		}
		if len(errored) != 0 {
			return ConditionsAwareDecisionNoOpinion("one or more conditional evaluation errors occurred", utilerrors.NewAggregate(errored))
		}
		if len(unevaluated) != 0 {
			return ConditionsAwareDecisionConditionsMap(nil, nil, unevaluated)
		}
	}

	// 所有条件求值结果均为 false。这意味着一个简单的默认 NoOpinion。
	return ConditionsAwareDecisionNoOpinion("no conditions matched", nil)
}

// 注意：签名为 string、error、[]Condition，因为这是处理各返回值的顺序
// 第一条 true 会以 nil 错误短路求值，尽管在第一条 true 求值之前可能已经发生了一些错误，
// 因为那种情况下的错误列表未必是完整的。
//
// 三态返回：命中的原因字符串、累计的错误、以及无法求值的条件（留给后续评估器）。
func conditionsToAppliedErroredUnevaluated(conditions iter.Seq[Condition], evalCond func(cond Condition) ConditionEvaluationResult, effect, appliedDescription string) (string, []error, []Condition) {
	var errs []error
	var unevaluatedConditions []Condition
	for cond := range conditions {
		id := cond.GetID()
		evalResult := evalCond(cond)
		switch {
		case evalResult.IsTrue():
			reason := fmt.Sprintf("condition %q %s", id, appliedDescription)
			if desc := cond.GetDescription(); len(desc) != 0 {
				reason += fmt.Sprintf(" with description %q", desc)
			}
			// 注意：在短路命中的情况下，这里对错误返回 nil，因为列表并不完整。
			return reason, nil, nil
		case evalResult.IsFalse():
			continue
		case evalResult.IsUnevaluatable():
			unevaluatedConditions = append(unevaluatedConditions, cond)
			continue
		case evalResult.IsError():
			errs = append(errs, fmt.Errorf("condition %q with effect=%s evaluated to an error: %w", id, effect, evalResult.Error()))
			continue
		default:
			errs = append(errs, fmt.Errorf("condition %q with effect=%s evaluated to an error: unknown evaluation result %v", id, effect, evalResult))
			continue
		}
	}
	return "", errs, unevaluatedConditions
}

// PartiallyEvaluateConditionsAwareDecision 主要使用各条件自带的 Evaluate() 函数求值
// ConditionsAwareDecision，其次/可选地在设置了 evaluateConditionFn 时使用它。若
// evaluateConditionFn 非 nil 且从不返回 ConditionsEvaluationResultUnevaluatable，
// 则返回的决策保证是 Allow/Deny/NoOpinion。
// 不过，本方法也可以用于求值条件子集（例如只支持某类条件的内置条件求值器），
// 对求值器无法识别的条件返回 ConditionsEvaluationResultUnevaluatable。
// 后一种情况下会返回一个部分求值的 ConditionsAwareDecision。
// 求值 ConditionsMap 时，只有第一条匹配（true）的条件会短路整个过程，
// 以使求值尽可能高效。
func PartiallyEvaluateConditionsAwareDecision(ctx context.Context, unevaluatedDecision ConditionsAwareDecision, data ConditionsData, evaluateConditionFn MaybeEvaluateConditionFunc) ConditionsAwareDecision {
	switch {
	case unevaluatedDecision.IsConditionsMap():
		// 叶子节点：尝试用内置求值器求值或细化该叶子 ConditionsMap，折叠成具体决策。
		return partiallyEvaluateConditionsMapInternal(ctx, unevaluatedDecision.ConditionsMap(), data, evaluateConditionFn)
	case unevaluatedDecision.IsUnion():
		// 联合节点：深度优先重建决策链，保持与原链相同的短路语义。
		var newDecisionChain ConditionsAwareDecisionUnion
		// 以深度优先方式递归遍历决策 DAG。
		// 本函数中的逻辑应当与 union authorizer 的 ConditionsAwareAuthorize 保持一致。

		collectTailAfterConditionalDecision := false
		for authorizerName, unevaluatedSubDecision := range unevaluatedDecision.UnionedDecisions() {
			// 若 collectAndShortcircuitOnly == true，说明在上一次循环迭代中遇到了
			// 无法求值为 Allow/Deny/NoOpinion 的条件性决策。
			// 此时后续的所有决策都保持未求值状态。
			if collectTailAfterConditionalDecision {
				newDecisionChain.Add(authorizerName, unevaluatedSubDecision)
				continue
			}

			// 当 !collectTailAfterConditionalDecision 时：newDecisionChain 中迄今的所有决策都是 NoOpinion，
			// 因为如果之前存在 ConditionsMap 或 Union 类型的决策，collectTailAfterConditionalDecision 就为 true，
			// 而如果更早存在 Allow 或 Deny，函数就已经返回了。

			// 尝试求值或细化这棵决策树中的叶子 ConditionsMap。
			possiblyEvaluatedSubDecision := PartiallyEvaluateConditionsAwareDecision(ctx, unevaluatedSubDecision, data, evaluateConditionFn)
			newDecisionChain.Add(authorizerName, possiblyEvaluatedSubDecision)

			// 如果存在任何 Allow/Deny 决策叶子，就无需继续遍历该链。
			if possiblyEvaluatedSubDecision.ContainsUnconditionalAllowOrDeny() {
				return newDecisionChain.ToDecision()
			}

			// 遇到条件性决策后停止部分求值
			if !possiblyEvaluatedSubDecision.IsUnconditional() {
				collectTailAfterConditionalDecision = true
			}
		}
		// 如果走到这里，所有叶子决策都是 NoOpinion 或 ConditionsMap 类型。
		// 如果所有决策都是 NoOpinion，构造函数会将其折叠为单个 NoOpinion 决策。
		return newDecisionChain.ToDecision()
	default:
		// 已经是无条件决策，无需简化。
		return unevaluatedDecision
	}
}
