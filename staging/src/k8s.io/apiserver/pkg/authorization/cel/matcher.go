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

package cel

import (
	"context"
	"fmt"
	"time"

	celgo "github.com/google/cel-go/cel"

	authorizationv1 "k8s.io/api/authorization/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

// CELMatcher 持有一组已编译的 matchCondition 表达式，用于对 SubjectAccessReview 求值。
//
// 一次 Eval 中的所有表达式都必须为 true 才匹配；任一为 false 立即短路。
// UsesLabelSelector/UsesFieldSelector 用于跳过无用的对象转换，降低每次授权检查的开销。
type CELMatcher struct {
	CompilationResults []CompilationResult

	// 记录是否有表达式用到 fieldSelector/labelSelector，未用到时可跳过对应对象转换。
	UsesLabelSelector bool
	UsesFieldSelector bool

	// 需要上报指标时才填写这些可选字段。
	Metrics        MatcherMetrics
	AuthorizerType string
	AuthorizerName string
}

// eval 用所有 matchCondition 表达式对给定的 SubjectAccessReview 求值。
//
// 语义：所有条件均为 true 才算匹配；出现任何求值错误则整体失败（返回聚合错误），
// 由调用方按各自 webhook 的 failurePolicy 决定放行还是拒绝。
func (c *CELMatcher) Eval(ctx context.Context, r *authorizationv1.SubjectAccessReview) (bool, error) {
	var evalErrors []error

	// 未配置指标时降级为 noop，避免调用点做 nil 判断。
	metrics := c.Metrics
	if metrics == nil {
		metrics = NoopMatcherMetrics{}
	}
	start := time.Now()
	// 用 defer 保证无论正常返回还是提前短路都记录耗时与失败计数。
	defer func() {
		metrics.RecordAuthorizationMatchConditionEvaluation(ctx, c.AuthorizerType, c.AuthorizerName, time.Since(start))
		if len(evalErrors) > 0 {
			metrics.RecordAuthorizationMatchConditionEvaluationFailure(ctx, c.AuthorizerType, c.AuthorizerName)
		}
	}()

	// 只把 Spec（即请求上下文）暴露给表达式，避免策略能窥探 review 的响应字段。
	va := map[string]interface{}{
		"request": convertObjectToUnstructured(&r.Spec, c.UsesFieldSelector, c.UsesLabelSelector),
	}
	for _, compilationResult := range c.CompilationResults {
		evalResult, _, err := compilationResult.Program.ContextEval(ctx, va)
		if err != nil {
			evalErrors = append(evalErrors, fmt.Errorf("cel evaluation error: expression '%v' resulted in error: %w", compilationResult.ExpressionAccessor.GetExpression(), err))
			continue
		}
		// 防御性类型检查：编译期已限定 bool，但动态类型仍可能因实现缺陷偏离。
		if evalResult.Type() != celgo.BoolType {
			evalErrors = append(evalErrors, fmt.Errorf("cel evaluation error: expression '%v' eval result type should be bool but got %W", compilationResult.ExpressionAccessor.GetExpression(), evalResult.Type()))
			continue
		}
		match, ok := evalResult.Value().(bool)
		if !ok {
			evalErrors = append(evalErrors, fmt.Errorf("cel evaluation error: expression '%v' eval result value should be bool but got %W", compilationResult.ExpressionAccessor.GetExpression(), evalResult.Value()))
			continue
		}
		// 任一 matchCondition 成功求值为 false 即可短路返回。
		if !match {
			metrics.RecordAuthorizationMatchConditionExclusion(ctx, c.AuthorizerType, c.AuthorizerName)
			return false, nil
		}
	}
	// 出现任何求值错误都整体返回失败。
	if len(evalErrors) > 0 {
		return false, utilerrors.NewAggregate(evalErrors)
	}
	// 只有所有条件都无错求值为 true 时才返回匹配。
	return true, nil
}
