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
	"errors"
	"fmt"
	"slices"
	"strings"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
)

// namedConditionsAwareDecision 把条件授权器的名字与其判定绑定，
// 名字用于错误信息去重、以及追踪最终结论由哪个授权器贡献。
type namedConditionsAwareDecision struct {
	conditionalAuthorizerName string
	d                         ConditionsAwareDecision
}

// ConditionsAwareDecisionUnion 是 Union 型 ConditionsAwareDecision 的 builder，非线程安全。
//
// 该 builder 缓存了两个派生值（是否含无条件 Allow/Deny、可能判定并集），
// 以便在大量子判定时避免重复遍历。
type ConditionsAwareDecisionUnion struct {
	// 注意：新增字段时必须同步更新 ToDecision 中的深拷贝逻辑。
	inner []namedConditionsAwareDecision
	errs  []error
	// 缓存 inner 中是否存在无条件 Allow/Deny 叶子节点。
	containsUnconditionalAllowOrDeny bool
	// 缓存所有已加入判定的 PossibleDecisions 并集。它并不等于
	// ConditionsAwareDecisionUnion.PossibleDecisions：后者还要额外处理 NoOpinion——
	// 空并集时补上 NoOpinion，存在无条件 Allow/Deny 时再去掉。
	subDecisionsPossibleDecisions sets.Set[Decision]
}

// Add 向 Union builder 加入一个带名字的子判定。若已出现无条件 Allow/Deny，
// 后续判定不再影响最终结果，Add 直接空操作。名字为空或重复会记录错误；
// 返回值表示本次是否为无操作。Add 非线程安全。
func (unionMap *ConditionsAwareDecisionUnion) Add(conditionalAuthorizerName string, d ConditionsAwareDecision) {
	if unionMap.ContainsUnconditionalAllowOrDeny() {
		return // all items after the first concrete Allow or Deny aren't anyways used in evaluation, so they are not added to inner
	}

	// 名字为空或重复都记录错误而不立即返回：让 Add 收集完全部问题，
	// 最终由 ToDecision 以 fail-closed 方式一次性暴露。
	if len(conditionalAuthorizerName) == 0 {
		unionMap.errs = append(unionMap.errs, fmt.Errorf("conditionalAuthorizerName must be non-empty"))
	}
	if unionMap.hasConditionalAuthorizerName(conditionalAuthorizerName) {
		// 这里不短路：收集完所有问题才能一次性 fail-closed，尽可能多地暴露配置错误。
		unionMap.errs = append(unionMap.errs, fmt.Errorf("duplicate conditionalAuthorizerName %q", conditionalAuthorizerName))
	}
	// 一旦链中出现无条件 Allow/Deny，后续判定永不会生效，无需再接收。
	if d.ContainsUnconditionalAllowOrDeny() {
		unionMap.containsUnconditionalAllowOrDeny = true
	}
	unionMap.inner = append(unionMap.inner, namedConditionsAwareDecision{conditionalAuthorizerName: conditionalAuthorizerName, d: d})
	// 缓存可能结果集合是安全的：ConditionsAwareDecision 构造后不可变。
	if unionMap.subDecisionsPossibleDecisions == nil {
		unionMap.subDecisionsPossibleDecisions = sets.New[Decision]()
	}
	unionMap.subDecisionsPossibleDecisions.Insert(d.PossibleDecisions().UnsortedList()...)
}

// hasConditionalAuthorizerName 判断名字是否已添加过，用于检测重复。
func (unionMap *ConditionsAwareDecisionUnion) hasConditionalAuthorizerName(conditionalAuthorizerName string) bool {
	return slices.ContainsFunc(unionMap.inner, func(nd namedConditionsAwareDecision) bool {
		return nd.conditionalAuthorizerName == conditionalAuthorizerName
	})
}

// FailureDecision 在调用方处理 ConditionsAwareDecision 遇到不可恢复错误时，
// 返回一个可用的 Deny 或 NoOpinion 判定。只要可能判定为 Deny 就返回 Deny，
// 以保守方式 fail-closed；否则返回 NoOpinion。非线程安全。
func (unionMap *ConditionsAwareDecisionUnion) FailureDecision() Decision {
	// 只要存在 Deny 的可能就返回 Deny；安全边界上宁可误拒不可误放。
	if unionMap.PossibleDecisions().Has(DecisionDeny) {
		return DecisionDeny
	}
	return DecisionNoOpinion
}

// ContainsUnconditionalAllowOrDeny 判断并集内是否至少有一个无条件 Allow/Deny 判定。非线程安全。
func (unionMap *ConditionsAwareDecisionUnion) ContainsUnconditionalAllowOrDeny() bool {
	return unionMap.containsUnconditionalAllowOrDeny
}

// PossibleDecisions 返回本 Union 所有可能的判定结果，是 {Allow, Deny, NoOpinion}
// 的非空子集。非线程安全。
func (unionMap *ConditionsAwareDecisionUnion) PossibleDecisions() sets.Set[Decision] {
	// 默认结果是 NoOpinion。始终返回新集合，避免调用方改动内部状态。
	possibleDecisions := sets.New(DecisionNoOpinion)
	// 并入缓存的子判定结果集合。
	if unionMap.subDecisionsPossibleDecisions != nil {
		possibleDecisions.Insert(unionMap.subDecisionsPossibleDecisions.UnsortedList()...)
	}
	// 存在无条件 Allow/Deny 时，默认的 NoOpinion 不会实际出现，需剔除。
	if unionMap.ContainsUnconditionalAllowOrDeny() {
		possibleDecisions.Delete(DecisionNoOpinion)
	}
	return possibleDecisions
}

// ToDecision 把若干判定合成一棵树：Allow/Deny/NoOpinion/ConditionsMap 是叶子，
// Union 是内部节点。非线程安全。
func (unionMap *ConditionsAwareDecisionUnion) ToDecision() ConditionsAwareDecision {
	// 构建过程中遇到错误（如授权器名重复）时直接 fail-closed。
	if len(unionMap.errs) != 0 {
		err := utilerrors.NewAggregate(unionMap.errs)
		return ConditionsAwareDecisionFromParts(unionMap.FailureDecision(), "failed closed", err)
	}

	// 只有一种可能结果时无需运行期求值，直接折叠为无条件判定。
	// 单一可能结果意味着条件表达式无关紧要，可以省去运行期求值。
	if possibleDecisions := unionMap.PossibleDecisions(); possibleDecisions.Len() == 1 {
		onlyPossibleDecision := possibleDecisions.UnsortedList()[0]
		// 收集确定结果的 reason 与 error。唯一结果为 Allow/Deny 时理论上只需看最后一个
		// 判定，但仍与 NoOpinion 走同一套通用逻辑：聚合所有同类型判定的 reason/error。
		var aggregateReasons []string
		var aggregateErrors []error
		for _, namedDecision := range unionMap.inner {
			reasons, errs := collectReasonsAndErrors([]string{namedDecision.conditionalAuthorizerName}, namedDecision.d, onlyPossibleDecision)
			aggregateReasons = append(aggregateReasons, reasons...)
			aggregateErrors = append(aggregateErrors, errs...)
		}

		switch onlyPossibleDecision {
		case DecisionDeny:
			// 例如可能结果为 "[Deny, NoOpinion], [NoOpinion], [Deny], [Allow]" 的并集，
			// 无论开头的 ConditionsMap 如何求值，最终都只能是 Deny。
			return ConditionsAwareDecisionDeny(strings.Join(aggregateReasons, ", "), utilerrors.NewAggregate(aggregateErrors))
		case DecisionNoOpinion:
			// 空切片时唯一可能结果就是 NoOpinion；只有整条链全是 NoOpinion 才会走到这里，
			// 因此可以统一聚合。TODO: 形式化验证这一点。
			return ConditionsAwareDecisionNoOpinion(strings.Join(aggregateReasons, ", "), utilerrors.NewAggregate(aggregateErrors))
		case DecisionAllow:
			// 例如可能结果为 "[Allow, NoOpinion], [NoOpinion], [Allow], [Deny]" 的并集，
			// 无论开头的 ConditionsMap 如何求值，最终都只能是 Allow。
			return ConditionsAwareDecisionAllow(strings.Join(aggregateReasons, ", "), utilerrors.NewAggregate(aggregateErrors))
		default:
			return ConditionsAwareDecisionDeny("failed closed", errors.New("should be unreachable: ConditionsAwareDecision should only contain Allow/Deny/NoOpinion"))
		}
	}

	// 多个可能结果时保留 Union 结构，交由运行期对条件求值。
	return ConditionsAwareDecision{
		decisionType: conditionsAwareDecisionTypeUnion,
		union: ConditionsAwareDecisionUnion{
			// 克隆 builder 中的引用，保证结果不可变、不受后续修改影响。
			inner:                            slices.Clone(unionMap.inner),
			containsUnconditionalAllowOrDeny: unionMap.containsUnconditionalAllowOrDeny,
			subDecisionsPossibleDecisions:    unionMap.subDecisionsPossibleDecisions.Clone(),
		},
	}
}

// collectReasonsAndErrors 递归收集与目标判定同类型的叶子节点的 reason/error，
// 并在 reason 前加上授权器名字路径，便于定位是哪一层给出的结论。
func collectReasonsAndErrors(authorizerNamePrefix []string, condAwareDecision ConditionsAwareDecision, unconditionalDecisionToCollect Decision) (aggregateReasons []string, aggregateErrors []error) {
	if (unconditionalDecisionToCollect == DecisionAllow && condAwareDecision.IsAllow()) ||
		(unconditionalDecisionToCollect == DecisionNoOpinion && condAwareDecision.IsNoOpinion()) ||
		(unconditionalDecisionToCollect == DecisionDeny && condAwareDecision.IsDeny()) {
		if reason := condAwareDecision.Reason(); len(reason) != 0 {
			aggregateReasons = append(aggregateReasons, fmt.Sprintf("%s: {%s}", strings.Join(authorizerNamePrefix, ": "), reason))
		}
		if err := condAwareDecision.Error(); err != nil {
			aggregateErrors = append(aggregateErrors, fmt.Errorf("%s: %w", strings.Join(authorizerNamePrefix, ": "), err))
		}
		return
	}
	if condAwareDecision.IsUnion() {
		for authorizerName, subDecision := range condAwareDecision.UnionedDecisions() {
			reasons, errs := collectReasonsAndErrors(slices.Concat(authorizerNamePrefix, []string{authorizerName}), subDecision, unconditionalDecisionToCollect)
			aggregateReasons = append(aggregateReasons, reasons...)
			aggregateErrors = append(aggregateErrors, errs...)
		}
		return
	}
	return
}
