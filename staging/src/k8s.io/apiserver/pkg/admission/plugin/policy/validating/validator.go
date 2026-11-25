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

package validating

import (
	"context"
	"errors"
	"fmt"
	"strings"

	celtypes "github.com/google/cel-go/common/types"

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
	"k8s.io/klog/v2"
)

// validator 实现 Validator 接口，持有一组已编译的 CEL 求值器，对单个请求依次执行匹配条件、
// validation 表达式、messageExpression 与审计注解表达式。
type validator struct {
	celMatcher            matchconditions.Matcher
	validationFilter      cel.ConditionEvaluator
	auditAnnotationFilter cel.ConditionEvaluator
	messageFilter         cel.ConditionEvaluator
	failPolicy            *v1.FailurePolicyType
	// 编译错误延迟到求值阶段处理：策略对象本身仍可存在，但每次准入都会按
	// failurePolicy 返回相应决策（Ignore 放行、Fail 拒绝）。
	compileError error
}

// NewValidator 组装 validator。各 filter 可同时为 nil（表示策略未配置该部分），
// 但一旦传入 err 非 nil，Validate 将不再执行表达式。
func NewValidator(validationFilter cel.ConditionEvaluator, celMatcher matchconditions.Matcher, auditAnnotationFilter, messageFilter cel.ConditionEvaluator, failPolicy *v1.FailurePolicyType, err error) Validator {
	return &validator{
		celMatcher:            celMatcher,
		validationFilter:      validationFilter,
		auditAnnotationFilter: auditAnnotationFilter,
		messageFilter:         messageFilter,
		failPolicy:            failPolicy,
		compileError:          err,
	}
}

// policyDecisionActionForError 把 CEL 求值/匹配错误映射为准入动作：
// Ignore 表示失败时放行，其余（含默认 Fail）一律拒绝，确保失败关闭。
func policyDecisionActionForError(f v1.FailurePolicyType) PolicyDecisionAction {
	if f == v1.Ignore {
		return ActionAdmit
	}
	return ActionDeny
}

// auditAnnotationEvaluationForError 错误期间不发布审计注解值：
// Ignore 时直接排除，否则记为错误供审计记录。
func auditAnnotationEvaluationForError(f v1.FailurePolicyType) PolicyAuditAnnotationAction {
	if f == v1.Ignore {
		return AuditAnnotationActionExclude
	}
	return AuditAnnotationActionError
}

// CompileError 返回编译期错误；nil 表示表达式可用。
func (v *validator) CompileError() error {
	return v.compileError
}

// Validate 把一组求值结果与 failure policy 转换为可执行的 PolicyDecision。
// runtimeCELCostBudget 仅供测试使用；调用方应始终传入 k8s.io/apiserver/pkg/apis/cel/config.go 中的常量 RuntimeCELCostBudget。
// Validate 把 CEL 求值结果翻译成可执行的准入决策与审计注解。整体顺序：
// 匹配条件 -> validation 表达式 -> message 表达式 -> 审计注解表达式。
// 任一阶段出错都按 failurePolicy 决定放行或拒绝，宁可失败关闭也不能
// 静默放行未经验证的请求。
func (v *validator) Validate(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *corev1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) ValidateResult {
	// failPolicy 未设置时按 Fail 处理：默认策略必须保证安全。
	var f v1.FailurePolicyType
	if v.failPolicy == nil {
		f = v1.Fail
	} else {
		f = *v.failPolicy
	}
	// 编译失败直接短路，避免对不可用的表达式做任何求值。
	if v.compileError != nil {
		return ValidateResult{
			Decisions: []PolicyDecision{
				{
					Action:     policyDecisionActionForError(f),
					Evaluation: EvalError,
					Message:    v.compileError.Error(),
				},
			},
		}
	}
	// 先评估 matchConditions；不匹配则整个策略不产生任何决策。
	if v.celMatcher != nil {
		matchResults := v.celMatcher.Match(ctx, versionedAttr, versionedParams, authz)
		if matchResults.Error != nil {
			return ValidateResult{
				Decisions: []PolicyDecision{
					{
						Action:     policyDecisionActionForError(f),
						Evaluation: EvalError,
						Message:    matchResults.Error.Error(),
					},
				},
			}
		}

		// 若前置条件（preconditions）不满足，则不返回任何 validation 结果
		if !matchResults.Matches {
			return ValidateResult{}
		}
	}

	// 绑定 versionedParams 与 authorizer，使表达式可访问 params 和授权检查；
	// messageExpression 不允许调用 authorizer，故使用不含 Authorizer 的绑定。
	optionalVars := cel.OptionalVariableBindings{VersionedParams: versionedParams, Authorizer: authz}
	expressionOptionalVars := cel.OptionalVariableBindings{VersionedParams: versionedParams}
	admissionRequest := cel.CreateAdmissionRequest(versionedAttr.Attributes, metav1.GroupVersionResource(matchedResource), metav1.GroupVersionKind(versionedAttr.VersionedKind))
	// namespace 对象可能为 nil（集群级资源），由 CreateNamespaceObject 处理。
	ns := cel.CreateNamespaceObject(namespace)
	evalResults, remainingBudget, err := v.validationFilter.ForInput(ctx, versionedAttr, admissionRequest, optionalVars, ns, runtimeCELCostBudget)
	if err != nil {
		return ValidateResult{
			Decisions: []PolicyDecision{
				{
					Action:     policyDecisionActionForError(f),
					Evaluation: EvalError,
					Message:    err.Error(),
				},
			},
		}
	}
	decisions := make([]PolicyDecision, len(evalResults))
	// message 表达式使用剩余预算，避免 validation 已耗尽预算后仍可无限求值。
	messageResults, _, err := v.messageFilter.ForInput(ctx, versionedAttr, admissionRequest, expressionOptionalVars, ns, remainingBudget)
	for i, evalResult := range evalResults {
		var decision = &decisions[i]
		decision.Elapsed = evalResult.Elapsed
		// TODO: 把这段逻辑上移到泛型层
		// 求值结果需还原为 ValidationCondition 才能拿到 reason/message 等元数据；
		// 类型不符属于内部错误，按失败策略处理。
		validation, ok := evalResult.ExpressionAccessor.(*ValidationCondition)
		if !ok {
			klog.Error("Invalid type conversion to ValidationCondition")
			decision.Action = policyDecisionActionForError(f)
			decision.Evaluation = EvalError
			decision.Message = "Invalid type sent to validator, expected ValidationCondition"
			continue
		}

		var messageResult *cel.EvaluationResult
		if len(messageResults) > i {
			messageResult = &messageResults[i]
		}
		if evalResult.Error != nil {
			decision.Action = policyDecisionActionForError(f)
			decision.Evaluation = EvalError
			decision.Message = evalResult.Error.Error()
		} else if errors.Is(err, apiservercel.ErrInternal) || errors.Is(err, apiservercel.ErrOutOfBudget) {
			// message 表达式整体失败（内部错误或预算耗尽）同样按失败策略处理。
			decision.Action = policyDecisionActionForError(f)
			decision.Evaluation = EvalError
			decision.Message = fmt.Sprintf("failed messageExpression: %s", err)
		} else if evalResult.EvalResult != celtypes.True {
			// 表达式返回非 true 即拒绝；Reason 缺省为 Invalid。
			decision.Action = ActionDeny
			decision.Evaluation = EvalDeny
			if validation.Reason == nil {
				decision.Reason = metav1.StatusReasonInvalid
			} else {
				decision.Reason = *validation.Reason
			}
			// 决定最终使用的失败消息
			var message string
			// 尝试以 messageExpression 的结果作为消息
			if messageResult != nil && messageResult.Error == nil && messageResult.EvalResult != nil {
				// 求值结果非字符串（包括 null）或去空白后为空时同样回退。
				if message, ok = messageResult.EvalResult.Value().(string); ok {
					message = strings.TrimSpace(message)
					// 限制长度与换行，防止用户通过策略注入超大或伪造多行的状态消息。
					if len(message) > celconfig.MaxEvaluatedMessageExpressionSizeBytes {
						klog.V(2).InfoS("excessively long message denied", "message", message)
						message = ""
					}
					// 拒绝包含换行的消息
					if strings.ContainsAny(message, "\n") {
						klog.V(2).InfoS("multi-line message denied", "message", message)
						message = ""
					}
				}
			}
			if messageResult != nil && messageResult.Error != nil {
				// 记录 messageExpression 的求值错误
				klog.V(2).ErrorS(messageResult.Error, "error while evaluating messageExpression")
			}
			// 回退：使用策略自定义的 message
			if message == "" && len(validation.Message) > 0 {
				message = strings.TrimSpace(validation.Message)
			}
			// 再回退：用表达式本身拼一条消息
			if message == "" {
				message = fmt.Sprintf("failed expression: %v", strings.TrimSpace(validation.Expression))
			}
			decision.Message = message
		} else {
			decision.Action = ActionAdmit
			decision.Evaluation = EvalAdmit
		}
	}

	// 审计注解表达式即使 validation 已拒绝仍要求值：拒绝事件同样需要携带注解。
	options := cel.OptionalVariableBindings{VersionedParams: versionedParams}
	auditAnnotationEvalResults, _, err := v.auditAnnotationFilter.ForInput(ctx, versionedAttr, admissionRequest, options, namespace, runtimeCELCostBudget)
	if err != nil {
		return ValidateResult{
			Decisions: []PolicyDecision{
				{
					Action:     policyDecisionActionForError(f),
					Evaluation: EvalError,
					Message:    err.Error(),
				},
			},
		}
	}

	auditAnnotationResults := make([]PolicyAuditAnnotation, len(auditAnnotationEvalResults))
	for i, evalResult := range auditAnnotationEvalResults {
		// 未配置 valueExpression 的注解不产生结果。
		if evalResult.ExpressionAccessor == nil {
			continue
		}
		var auditAnnotationResult = &auditAnnotationResults[i]
		auditAnnotationResult.Elapsed = evalResult.Elapsed
		// TODO: 把这段逻辑上移到泛型层
		validation, ok := evalResult.ExpressionAccessor.(*AuditAnnotationCondition)
		if !ok {
			klog.Error("Invalid type conversion to AuditAnnotationCondition")
			auditAnnotationResult.Action = auditAnnotationEvaluationForError(f)
			auditAnnotationResult.Error = fmt.Sprintf("Invalid type sent to validator, expected AuditAnnotationCondition but got %T", evalResult.ExpressionAccessor)
			continue
		}
		auditAnnotationResult.Key = validation.Key

		if evalResult.Error != nil {
			auditAnnotationResult.Action = auditAnnotationEvaluationForError(f)
			auditAnnotationResult.Error = evalResult.Error.Error()
		} else {
			// 只接受 string 或 null：空字符串/null 表示不发布，其他类型视为错误，
			// 防止把复杂对象混入审计注解。
			switch evalResult.EvalResult.Type() {
			case celtypes.StringType:
				value := strings.TrimSpace(evalResult.EvalResult.Value().(string))
				if len(value) == 0 {
					auditAnnotationResult.Action = AuditAnnotationActionExclude
				} else {
					auditAnnotationResult.Action = AuditAnnotationActionPublish
					auditAnnotationResult.Value = value
				}
			case celtypes.NullType:
				auditAnnotationResult.Action = AuditAnnotationActionExclude
			default:
				auditAnnotationResult.Action = AuditAnnotationActionError
				auditAnnotationResult.Error = fmt.Sprintf("valueExpression '%v' resulted in unsupported return type: %v. "+
					"Return type must be either string or null.", validation.ValueExpression, evalResult.EvalResult.Type())
			}
		}
	}
	return ValidateResult{Decisions: decisions, AuditAnnotations: auditAnnotationResults}
}
