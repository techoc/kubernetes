/*
Copyright 2023 The Kubernetes Authors.

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

package matchconditions

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/cel-go/cel"
	celtypes "github.com/google/cel-go/common/types"

	v1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/admission"
	admissionmetrics "k8s.io/apiserver/pkg/admission/metrics"
	celplugin "k8s.io/apiserver/pkg/admission/plugin/cel"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/klog/v2"
)

// 编译期断言：MatchCondition 必须实现 CEL 表达式访问器接口。
var _ celplugin.ExpressionAccessor = &MatchCondition{}

// MatchCondition 包含编译、求值与匹配 CEL 表达式所需的输入。
//
// MatchCondition 是 v1.MatchCondition 的类型别名式包装（定义新类型而非 alias），
// 目的是为其附加 ExpressionAccessor 方法，同时不污染 API 类型。
type MatchCondition v1.MatchCondition

// GetExpression 返回用于求值的 CEL 源表达式。
func (v *MatchCondition) GetExpression() string {
	return v.Expression
}

// ReturnTypes 声明匹配条件必须返回 bool；编译期据此拒绝返回非布尔值的表达式。
func (v *MatchCondition) ReturnTypes() []*cel.Type {
	return []*cel.Type{cel.BoolType}
}

var _ Matcher = &matcher{}

// matcher 求值已编译的 CEL 表达式，判断给定请求是否匹配。
//
// failPolicy 决定表达式求值出错（而非返回 false）时的行为：Fail 拒绝请求，
// Ignore 视为不匹配并跳过 webhook 调用。
type matcher struct {
	filter      celplugin.ConditionEvaluator
	failPolicy  v1.FailurePolicyType
	matcherType string
	matcherKind string
	objectName  string
}

// NewMatcher 构造匹配器。failPolicy 为 nil 时默认 Fail，
// 这是 API 中 failurePolicy 的默认值，保证未显式配置时行为保守。
func NewMatcher(filter celplugin.ConditionEvaluator, failPolicy *v1.FailurePolicyType, matcherKind, matcherType, objectName string) Matcher {
	var f v1.FailurePolicyType
	if failPolicy == nil {
		f = v1.Fail
	} else {
		f = *failPolicy
	}
	return &matcher{
		filter:      filter,
		failPolicy:  f,
		matcherKind: matcherKind,
		matcherType: matcherType,
		objectName:  objectName,
	}
}

// Match 依次求值所有 matchConditions，只有全部为 true 才返回 Matches=true。
// 任一条件为 false 时立即短路返回，不再继续求值，也不调用 webhook。
func (m *matcher) Match(ctx context.Context, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, authz authorizer.UnconditionalAuthorizer) MatchResult {
	t := time.Now()
	// 一次性求值整组条件；预算使用 matchConditions 专用额度，防止单请求开销失控。
	evalResults, _, err := m.filter.ForInput(ctx, versionedAttr, celplugin.CreateAdmissionRequest(versionedAttr.Attributes, metav1.GroupVersionResource(versionedAttr.GetResource()), metav1.GroupVersionKind(versionedAttr.VersionedKind)), celplugin.OptionalVariableBindings{
		VersionedParams: versionedParams,
		Authorizer:      authz,
	}, nil, celconfig.RuntimeCELCostBudgetMatchConditions)

	if err != nil {
		admissionmetrics.Metrics.ObserveMatchConditionEvaluationTime(ctx, time.Since(t), m.objectName, m.matcherKind, m.matcherType, string(versionedAttr.GetOperation()))
		// filter 返回错误属于意外情况，也不是表达式求值错误，因此此处不递增指标
		// 框架级错误（非表达式求值错误）同样遵循 failurePolicy，保持与求值错误一致的外部行为。
		if m.failPolicy == v1.Fail {
			return MatchResult{
				Error: err,
			}
		} else if m.failPolicy == v1.Ignore {
			return MatchResult{
				Matches: false,
			}
		}
		//TODO: 增加 default 分支，避免将来引入其他失败类型时逻辑直接落空
	}

	errorList := []error{}
	for _, evalResult := range evalResults {
		matchCondition, ok := evalResult.ExpressionAccessor.(*MatchCondition)
		if !ok {
			// 理论上不应发生，若发生则按求值错误同等处理
			klog.Error("Invalid type conversion to MatchCondition")
			errorList = append(errorList, errors.New(fmt.Sprintf("internal error converting ExpressionAccessor to MatchCondition")))
			continue
		}
		if evalResult.Error != nil {
			// 记录错误但继续检查其余条件：false 结果优先于错误，可避免不必要的 webhook 调用。
			errorList = append(errorList, evalResult.Error)
			admissionmetrics.Metrics.ObserveMatchConditionEvalError(ctx, m.objectName, m.matcherKind, m.matcherType, string(versionedAttr.GetOperation()))
		}
		if evalResult.EvalResult == celtypes.False {
			admissionmetrics.Metrics.ObserveMatchConditionEvaluationTime(ctx, time.Since(t), m.objectName, m.matcherKind, m.matcherType, string(versionedAttr.GetOperation()))
			// 只要有任一条件为 false，就始终跳过调用 webhook
			// 报告首个不满足的条件名，便于用户定位问题。
			return MatchResult{
				Matches:             false,
				FailedConditionName: matchCondition.Name,
			}
		}
	}
	if len(errorList) > 0 {
		admissionmetrics.Metrics.ObserveMatchConditionEvaluationTime(ctx, time.Since(t), m.objectName, m.matcherKind, m.matcherType, string(versionedAttr.GetOperation()))
		// 若既有 true 又有求值错误，则依据 fail policy 处理
		if m.failPolicy == v1.Fail {
			// fail policy 为 fail 时，true 与错误的混合应拒绝请求且不调用 webhook
			err = utilerrors.NewAggregate(errorList)
			return MatchResult{
				Error: err,
			}
		} else if m.failPolicy == v1.Ignore {
			// fail policy 为 ignore 时，跳过对 webhook 的调用
			return MatchResult{
				Matches: false,
			}
		}
	}
	// 没有任何结果求值为 false 时，返回 Matches=true，并附上遇到的所有错误列表
	// 空条件列表也视为匹配，语义与“无 matchConditions”一致。
	return MatchResult{
		Matches: true,
	}
}
