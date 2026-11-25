/*
Copyright 2022 The Kubernetes Authors.

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

// 本文件针对 validator 的决策逻辑做单元测试：把 CEL 求值替换为预置结果，
// 从而精确覆盖 failurePolicy、messageExpression、审计注解与预算耗尽等分支。

package validating

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	celtypes "github.com/google/cel-go/common/types"
	"github.com/stretchr/testify/require"

	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/matchconditions"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/environment"
)

// fakeCelFilter 用预置的求值结果替代真实的 CEL 执行，
// 便于构造“表达式返回 true/false/报错”等确定场景。
var _ cel.ConditionEvaluator = &fakeCelFilter{}

// fakeCelFilter 实现 ConditionEvaluator：返回 evaluations 并按需消耗 1 点预算。
type fakeCelFilter struct {
	evaluations []cel.EvaluationResult
	throwError  bool
}

// ForInput 模拟单次求值：预算耗尽时返回与真实实现一致的 ErrOutOfBudget 错误，
// 保证 validator 对预算耗尽的处理分支能被覆盖。
func (f *fakeCelFilter) ForInput(ctx context.Context, versionedAttr *admission.VersionedAttributes, request *admissionv1.AdmissionRequest, optionalVars cel.OptionalVariableBindings, namespace *corev1.Namespace, costBudget int64) ([]cel.EvaluationResult, int64, error) {
	if costBudget <= 0 { // 该过滤器每次消耗 1 点预算，cost = 0 即失败。
		return nil, -1, &apiservercel.Error{
			Type:   apiservercel.ErrorTypeInvalid,
			Detail: "validation failed due to running out of cost budget, no further validation rules will be run",
			Cause:  apiservercel.ErrOutOfBudget,
		}
	}
	if f.throwError {
		return nil, -1, errors.New("test error")
	}
	return f.evaluations, costBudget - 1, nil
}

// CompilationErrors 恒为空：编译错误在别处测试。
func (f *fakeCelFilter) CompilationErrors() []error {
	return []error{}
}

// fakeCELMatcher 以固定结果替代 matchConditions 求值。
var _ matchconditions.Matcher = &fakeCELMatcher{}

// fakeCELMatcher 返回预先设定的匹配结果与错误。
type fakeCELMatcher struct {
	error   error
	matches bool
}

// Match 总是返回配置好的结果；FailedConditionName 固定为占位值。
func (f *fakeCELMatcher) Match(ctx context.Context, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, authz authorizer.UnconditionalAuthorizer) matchconditions.MatchResult {
	return matchconditions.MatchResult{Matches: f.matches, FailedConditionName: "placeholder", Error: f.error}
}

// TestValidate 表驱动覆盖 validator.Validate 的核心决策矩阵：
//   - 表达式结果（true/false/错误）与多条校验的组合；
//   - failurePolicy 为 Fail/Ignore/未设置（默认 Fail）时的行为；
//   - reason 缺省为 Invalid、显式设置时透传；
//   - messageExpression 覆盖静态 message、求值失败或为空时回退；
//   - 审计注解在 Fail/Ignore 下的取值；
//   - 表达式与 messageExpression 共享同一预算；
//   - matchConditions 不匹配时抑制失败，匹配出错时按“视为匹配”处理。
func TestValidate(t *testing.T) {
	ignore := v1.Ignore
	fail := v1.Fail

	forbiddenReason := metav1.StatusReasonForbidden
	unauthorizedReason := metav1.StatusReasonUnauthorized

	fakeAttr := admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{}, "default", "foo", schema.GroupVersionResource{}, "", admission.Create, nil, false, nil)
	fakeVersionedAttr, _ := admission.NewVersionedAttributes(fakeAttr, schema.GroupVersionKind{}, nil)

	cases := []struct {
		name               string
		failPolicy         *v1.FailurePolicyType
		matcher            matchconditions.Matcher
		evaluations        []cel.EvaluationResult
		messageEvaluations []cel.EvaluationResult
		auditEvaluations   []cel.EvaluationResult
		policyDecision     []PolicyDecision
		auditAnnotations   []PolicyAuditAnnotation
		throwError         bool
		costBudget         int64 // 留零表示使用默认预算
	}{
		{
			name: "test pass",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult:         celtypes.True,
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionAdmit,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			name: "test multiple pass",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult:         celtypes.True,
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
				{
					EvalResult:         celtypes.True,
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionAdmit,
					Elapsed: time.Millisecond,
				},
				{
					Action:  ActionAdmit,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			name: "test error with failurepolicy ignore",
			evaluations: []cel.EvaluationResult{
				{
					Error:              errors.New(""),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionAdmit,
					Elapsed: time.Millisecond,
				},
			},
			failPolicy: &ignore,
		},
		{
			// 未设置 failurePolicy 时按 Fail 处理（API 默认值由 webhook/策略定义补全，
			// 但 validator 自身也需保持这一默认，避免绕过 API 的调用路径静默放行）。
			name: "test error with failurepolicy nil",
			evaluations: []cel.EvaluationResult{
				{
					Error:              errors.New(""),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			name: "test fail with failurepolicy fail",
			evaluations: []cel.EvaluationResult{
				{
					Error:              errors.New(""),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Elapsed: time.Millisecond,
				},
			},
			failPolicy: &fail,
		},
		{
			// 每条校验独立产生一条决策，错误只影响其所属的那一条。
			name: "test fail with failurepolicy ignore with multiple validations",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult:         celtypes.True,
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
				{
					Error:              errors.New(""),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionAdmit,
					Elapsed: time.Millisecond,
				},
				{
					Action:  ActionAdmit,
					Elapsed: time.Millisecond,
				},
			},
			failPolicy: &ignore,
		},
		{
			name: "test fail with failurepolicy nil with multiple validations",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult:         celtypes.True,
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
				{
					Error:              errors.New(""),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionAdmit,
					Elapsed: time.Millisecond,
				},
				{
					Action:  ActionDeny,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			name: "test fail with failurepolicy fail with multiple validations",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult:         celtypes.True,
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
				{
					Error:              errors.New(""),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionAdmit,
					Elapsed: time.Millisecond,
				},
				{
					Action:  ActionDeny,
					Elapsed: time.Millisecond,
				},
			},
			failPolicy: &fail,
		},
		{
			name: "test fail with failurepolicy ignore with multiple failed validations",
			evaluations: []cel.EvaluationResult{
				{
					Error:              errors.New(""),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
				{
					Error:              errors.New(""),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionAdmit,
					Elapsed: time.Millisecond,
				},
				{
					Action:  ActionAdmit,
					Elapsed: time.Millisecond,
				},
			},
			failPolicy: &ignore,
		},
		{
			name: "test fail with failurepolicy nil with multiple failed validations",
			evaluations: []cel.EvaluationResult{
				{
					Error:              errors.New(""),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
				{
					Error:              errors.New(""),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Elapsed: time.Millisecond,
				},
				{
					Action:  ActionDeny,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			name: "test fail with failurepolicy fail with multiple failed validations",
			evaluations: []cel.EvaluationResult{
				{
					Error:              errors.New(""),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
				{
					Error:              errors.New(""),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Elapsed: time.Millisecond,
				},
				{
					Action:  ActionDeny,
					Elapsed: time.Millisecond,
				},
			},
			failPolicy: &fail,
		},
		{
			// 未设置 reason 时使用 Invalid，而不是空字符串——API 响应需要合法的 StatusReason。
			name: "test reason for fail no reason set",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Expression: "this.expression == unit.test",
					},
					Elapsed: time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Reason:  metav1.StatusReasonInvalid,
					Message: "failed expression: this.expression == unit.test",
					Elapsed: time.Millisecond,
				},
			},
			failPolicy: &fail,
		},
		{
			// 显式 reason 应原样透传，供客户端据此区分 Forbidden 与 Unauthorized。
			name: "test reason for fail reason set",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
					},
					Elapsed: time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Reason:  metav1.StatusReasonForbidden,
					Message: "failed expression: this.expression == unit.test",
					Elapsed: time.Millisecond,
				},
			},
			failPolicy: &fail,
		},
		{
			// 多条校验各自携带不同 reason，互不影响。
			name: "test reason for failed validations multiple validations",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
					},
					Elapsed: time.Millisecond,
				},
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &unauthorizedReason,
						Expression: "this.expression.2 == unit.test.2",
					},
					Elapsed: time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Reason:  metav1.StatusReasonForbidden,
					Message: "failed expression: this.expression == unit.test",
					Elapsed: time.Millisecond,
				},
				{
					Action:  ActionDeny,
					Reason:  metav1.StatusReasonUnauthorized,
					Message: "failed expression: this.expression.2 == unit.test.2",
					Elapsed: time.Millisecond,
				},
			},
			failPolicy: &fail,
		},
		{
			// 静态 message 在校验失败时直接作为拒绝原因。
			name: "test message for failed validations",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test",
					},
					Elapsed: time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Reason:  metav1.StatusReasonForbidden,
					Message: "test",
					Elapsed: time.Millisecond,
				},
			},
			failPolicy: &fail,
		},
		{
			name: "test message for failed validations multiple validations",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test1",
					},
					Elapsed: time.Millisecond,
				},
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test2",
					},
					Elapsed: time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Reason:  metav1.StatusReasonForbidden,
					Message: "test1",
					Elapsed: time.Millisecond,
				},
				{
					Action:  ActionDeny,
					Reason:  metav1.StatusReasonForbidden,
					Message: "test2",
					Elapsed: time.Millisecond,
				},
			},
			failPolicy: &fail,
		},
		{
			// 过滤层整体报错时只产生一条 Deny 决策，并直接携带底层错误文本；
			// 已求值的结果被丢弃，避免用部分结果做判断。
			name: "test filter error",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test1",
					},
					Elapsed: time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Message: "test error",
				},
			},
			failPolicy: &fail,
			throwError: true,
		},
		{
			// 即便有多条校验，过滤层报错也只合并为一条决策。
			name: "test filter error multiple evaluations",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test1",
					},
					Elapsed: time.Millisecond,
				},
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test2",
					},
					Elapsed: time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Message: "test error",
				},
			},
			failPolicy: &fail,
			throwError: true,
		},
		{
			// 没有校验规则时审计注解仍会被求值：审计注解独立于校验结果。
			name: "test empty validations with non-empty audit annotations",
			auditEvaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.String("string value"),
					ExpressionAccessor: &AuditAnnotationCondition{
						ValueExpression: "'string value'",
					},
					Elapsed: 2 * time.Millisecond,
				},
			},
			failPolicy: &fail,
			auditAnnotations: []PolicyAuditAnnotation{
				{
					Action:  AuditAnnotationActionPublish,
					Value:   "string value",
					Elapsed: 2 * time.Millisecond,
				},
			},
		},
		{
			// 校验通过时审计注解照常发布（用于记录“策略已检查”的证据）。
			name: "test non-empty validations with non-empty audit annotations",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.True,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test1",
					},
					Elapsed: time.Millisecond,
				},
			},
			auditEvaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.String("string value"),
					ExpressionAccessor: &AuditAnnotationCondition{
						ValueExpression: "'string value'",
					},
					Elapsed: 2 * time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionAdmit,
					Elapsed: time.Millisecond,
				},
			},
			auditAnnotations: []PolicyAuditAnnotation{
				{
					Action:  AuditAnnotationActionPublish,
					Value:   "string value",
					Elapsed: 2 * time.Millisecond,
				},
			},
			failPolicy: &fail,
		},
		{
			// 求值为 null 的注解被排除，非 null 的照常发布，二者互不影响。
			name: "test audit annotations with null return",
			auditEvaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.NullValue,
					ExpressionAccessor: &AuditAnnotationCondition{
						ValueExpression: "null",
					},
					Elapsed: 2 * time.Millisecond,
				},
				{
					EvalResult: celtypes.String("string value"),
					ExpressionAccessor: &AuditAnnotationCondition{
						ValueExpression: "'string value'",
					},
					Elapsed: 2 * time.Millisecond,
				},
			},
			auditAnnotations: []PolicyAuditAnnotation{
				{
					Action:  AuditAnnotationActionExclude,
					Elapsed: 2 * time.Millisecond,
				},
				{
					Action:  AuditAnnotationActionPublish,
					Value:   "string value",
					Elapsed: 2 * time.Millisecond,
				},
			},
			failPolicy: &fail,
		},
		{
			// 注解表达式求值出错：Fail 策略下记为 Error 动作，错误会向上传播。
			name: "test audit annotations with failPolicy=fail",
			auditEvaluations: []cel.EvaluationResult{
				{
					Error: fmt.Errorf("valueExpression ''this is not valid CEL' resulted in error: <nil>"),
					ExpressionAccessor: &AuditAnnotationCondition{
						ValueExpression: "'this is not valid CEL",
					},
					Elapsed: 2 * time.Millisecond,
				},
			},
			auditAnnotations: []PolicyAuditAnnotation{
				{
					Action:  AuditAnnotationActionError,
					Error:   "valueExpression ''this is not valid CEL' resulted in error: <nil>",
					Elapsed: 2 * time.Millisecond,
				},
			},
			failPolicy: &fail,
		},
		{
			// Ignore 策略下错误被吞掉，注解记为 Exclude（不发布），保证请求不受影响。
			name: "test audit annotations with failPolicy=ignore",
			auditEvaluations: []cel.EvaluationResult{
				{
					Error: fmt.Errorf("valueExpression ''this is not valid CEL' resulted in error: <nil>"),
					ExpressionAccessor: &AuditAnnotationCondition{
						ValueExpression: "'this is not valid CEL",
					},
					Elapsed: 2 * time.Millisecond,
				},
			},
			auditAnnotations: []PolicyAuditAnnotation{
				{
					Action:  AuditAnnotationActionExclude, // TODO: 这样对吗？
					Error:   "valueExpression ''this is not valid CEL' resulted in error: <nil>",
					Elapsed: 2 * time.Millisecond,
				},
			},
			failPolicy: &ignore,
		},
		{
			// messageExpression 求值成功时覆盖静态 message。
			name: "messageExpression successful, empty message",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
					},
					Elapsed: time.Millisecond,
				},
			},
			messageEvaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.String("evaluated message"),
					Elapsed:    time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Message: "evaluated message",
					Reason:  forbiddenReason,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			// 只有实际返回字符串结果的 messageExpression 才覆盖；
			// 第一条的求值结果为空（零值），因此保留其静态 message。
			name: "messageExpression for multiple validations",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "I am not overwritten",
					},
					Elapsed: time.Millisecond,
				},
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "I am overwritten",
					},
					Elapsed: time.Millisecond,
				},
			},
			messageEvaluations: []cel.EvaluationResult{
				{},
				{
					EvalResult: celtypes.String("evaluated message"),
					Elapsed:    time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Message: "I am not overwritten",
					Reason:  forbiddenReason,
					Elapsed: time.Millisecond,
				},
				{
					Action:  ActionDeny,
					Message: "evaluated message",
					Reason:  forbiddenReason,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			// 静态 message 被 messageExpression 的结果覆盖。
			name: "messageExpression successful, overwritten message",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "I am overwritten",
					},
					Elapsed: time.Millisecond,
				},
			},
			messageEvaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.String("evaluated message"),
					Elapsed:    time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Message: "evaluated message",
					Reason:  forbiddenReason,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			// messageExpression 求值失败时回退到原始 message，而不是丢弃拒绝原因。
			name: "messageExpression user failure",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test1",
					},
					Elapsed: time.Millisecond,
				},
			},
			messageEvaluations: []cel.EvaluationResult{
				{
					Error:   &apiservercel.Error{Type: apiservercel.ErrorTypeInvalid},
					Elapsed: time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Message: "test1", // 使用原始 message
					Reason:  forbiddenReason,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			// 求值为纯空白视为“空消息”，同样回退到静态 message。
			name: "messageExpression eval to empty",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test1",
					},
					Elapsed: time.Millisecond,
				},
			},
			messageEvaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.String(" "),
					Elapsed:    time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Message: "test1",
					Reason:  forbiddenReason,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			// 多行消息会被拒绝：messageExpression 的结果必须是单行，
			// 以免策略向 API 响应或审计日志注入换行。
			name: "messageExpression eval to multi-line",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test1",
					},
					Elapsed: time.Millisecond,
				},
			},
			messageEvaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.String("hello\nthere"),
					Elapsed:    time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Message: "test1",
					Reason:  forbiddenReason,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			// 超过 5KB 的消息被拒绝，防止策略放大响应体积。
			name: "messageExpression eval result too long",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test1",
					},
					Elapsed: time.Millisecond,
				},
			},
			messageEvaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.String(strings.Repeat("x", 5*1024+1)),
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Message: "test1",
					Reason:  forbiddenReason,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			// null 不是合法字符串结果，回退到静态 message。
			name: "messageExpression eval to null",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test1",
					},
					Elapsed: time.Millisecond,
				},
			},
			messageEvaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.NullValue,
					Elapsed:    time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Message: "test1",
					Reason:  forbiddenReason,
					Elapsed: time.Millisecond,
				},
			},
		},
		{
			// 预算在表达式与 messageExpression 之间共享：表达式先耗尽预算后，
			// messageExpression 无法求值，决策消息应反映预算耗尽而非原始 message。
			name: "messageExpression out of budget after successful eval of expression",
			evaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.False,
					ExpressionAccessor: &ValidationCondition{
						Reason:     &forbiddenReason,
						Expression: "this.expression == unit.test",
						Message:    "test1",
					},
					Elapsed: time.Millisecond,
				},
			},
			messageEvaluations: []cel.EvaluationResult{
				{
					EvalResult: celtypes.StringType, // 无关紧要
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action:  ActionDeny,
					Message: "running out of cost budget",
					Elapsed: time.Millisecond,
				},
			},
			costBudget: 1, // 由表达式与 messageExpression 共享，共需 1 + 1 = 2
		},
		{
			// matchConditions 不匹配时，即便校验表达式报错也不产生决策：
			// 策略未命中就不应影响请求。
			name:    "no match surpresses failure",
			matcher: &fakeCELMatcher{matches: false},
			evaluations: []cel.EvaluationResult{
				{
					Error:              errors.New("expected"),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{},
			failPolicy:     &fail,
		},
		{
			// matchConditions 求值出错时保守地视为“匹配”，再按 failurePolicy 决定去留，
			// 避免条件错误被当作不匹配而静默跳过策略。
			name:    "match error => presumed match",
			matcher: &fakeCELMatcher{matches: true, error: fmt.Errorf("test error")},
			evaluations: []cel.EvaluationResult{
				{
					Error:              errors.New("expected"),
					ExpressionAccessor: &ValidationCondition{},
					Elapsed:            time.Millisecond,
				},
			},
			policyDecision: []PolicyDecision{
				{
					Action: ActionDeny,
				},
			},
			failPolicy: &fail,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 未显式提供匹配器时默认匹配，使大多数用例只关注决策逻辑本身。
			var matcher matchconditions.Matcher
			if tc.matcher == nil {
				matcher = &fakeCELMatcher{matches: true}
			} else {
				matcher = tc.matcher
			}
			v := validator{
				failPolicy: tc.failPolicy,
				celMatcher: matcher,
				validationFilter: &fakeCelFilter{
					evaluations: tc.evaluations,
					throwError:  tc.throwError,
				},
				messageFilter: &fakeCelFilter{
					evaluations: tc.messageEvaluations,
					throwError:  tc.throwError,
				},
				auditAnnotationFilter: &fakeCelFilter{
					evaluations: tc.auditEvaluations,
					throwError:  tc.throwError,
				},
			}
			ctx := context.TODO()
			// costBudget 为 0 表示使用生产默认预算；非零值用于精确控制预算耗尽场景。
			var budget int64 = celconfig.RuntimeCELCostBudget
			if tc.costBudget != 0 {
				budget = tc.costBudget
			}
			validateResult := v.Validate(ctx, fakeVersionedAttr.GetResource(), fakeVersionedAttr, nil, nil, budget, nil)

			// 决策数量必须完全一致：多一条或少一条都说明合并/短路逻辑出错。
			require.Equal(t, len(validateResult.Decisions), len(tc.policyDecision))

			for i, policyDecision := range tc.policyDecision {
				// message 用子串匹配，因为实现可能附加表达式原文等上下文。
				if policyDecision.Action != validateResult.Decisions[i].Action {
					t.Errorf("Expected policy decision kind '%v' but got '%v'", policyDecision.Action, validateResult.Decisions[i].Action)
				}
				if !strings.Contains(validateResult.Decisions[i].Message, policyDecision.Message) {
					t.Errorf("Expected policy decision message contains '%v' but got '%v'", policyDecision.Message, validateResult.Decisions[i].Message)
				}
				if policyDecision.Reason != validateResult.Decisions[i].Reason {
					t.Errorf("Expected policy decision reason '%v' but got '%v'", policyDecision.Reason, validateResult.Decisions[i].Reason)
				}
				if policyDecision.Elapsed != validateResult.Decisions[i].Elapsed {
					t.Errorf("Expected policy decision elapsed time '%v', but got '%v'", policyDecision.Elapsed, validateResult.Decisions[i].Elapsed)
				}
			}
			// 审计注解的数量以审计求值结果为准，而不是期望列表，防止两边同时漏写而测试仍通过。
			require.Equal(t, len(tc.auditEvaluations), len(validateResult.AuditAnnotations))

			for i, auditAnnotation := range tc.auditAnnotations {
				actual := validateResult.AuditAnnotations[i]
				if auditAnnotation.Action != actual.Action {
					t.Errorf("Expected policy audit annotation action '%v' but got '%v'", auditAnnotation.Action, actual.Action)
				}
				if auditAnnotation.Error != actual.Error {
					t.Errorf("Expected audit annotation error '%v' but got '%v'", auditAnnotation.Error, actual.Error)
				}
				if auditAnnotation.Value != actual.Value {
					t.Errorf("Expected policy audit annotation value '%v' but got '%v'", auditAnnotation.Value, actual.Value)
				}
				if auditAnnotation.Elapsed != actual.Elapsed {
					t.Errorf("Expected policy audit annotation elapsed time '%v', but got '%v'", auditAnnotation.Elapsed, actual.Elapsed)
				}
			}
		})
	}
}

// TestContextCanceled 验证请求上下文被取消时校验立即中断并给出“operation interrupted”决策，
// 而不是继续执行可能很昂贵的 CEL 表达式——这保证客户端断开后不再浪费 apiserver CPU。
func TestContextCanceled(t *testing.T) {
	fail := v1.Fail

	fakeAttr := admission.NewAttributesRecord(nil, nil, schema.GroupVersionKind{}, "default", "foo", schema.GroupVersionResource{}, "", admission.Create, nil, false, nil)
	fakeVersionedAttr, _ := admission.NewVersionedAttributes(fakeAttr, schema.GroupVersionKind{}, nil)
	fc := cel.NewConditionCompiler(environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()))
	f := fc.CompileCondition([]cel.ExpressionAccessor{&ValidationCondition{Expression: "[1,2,3,4,5,6,7,8,9,10].map(x, [1,2,3,4,5,6,7,8,9,10].map(y, x*y)) == []"}}, cel.OptionalVariableDeclarations{HasParams: false, HasAuthorizer: false}, environment.StoredExpressions)
	v := validator{
		failPolicy:       &fail,
		celMatcher:       &fakeCELMatcher{matches: true},
		validationFilter: f,
		messageFilter:    f,
		auditAnnotationFilter: &fakeCelFilter{
			evaluations: nil,
			throwError:  false,
		},
	}
	ctx, cancel := context.WithCancel(context.TODO())
	// 立刻取消：表达式本身是嵌套 map 的昂贵计算，若未被中断会消耗大量预算。
	cancel()
	validationResult := v.Validate(ctx, fakeVersionedAttr.GetResource(), fakeVersionedAttr, nil, nil, celconfig.RuntimeCELCostBudget, nil)
	if len(validationResult.Decisions) != 1 || !strings.Contains(validationResult.Decisions[0].Message, "operation interrupted") {
		t.Errorf("Expected 'operation interrupted' but got %v", validationResult.Decisions)
	}
}
