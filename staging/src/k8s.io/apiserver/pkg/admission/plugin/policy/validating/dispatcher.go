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

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apiserver/pkg/admission"
	admissionauthorizer "k8s.io/apiserver/pkg/admission/plugin/authorizer"
	"k8s.io/apiserver/pkg/admission/plugin/policy/generic"
	celmetrics "k8s.io/apiserver/pkg/admission/plugin/policy/validating/metrics"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/klog/v2"
)

// dispatcher 对一个请求执行一组已编译的 ValidatingAdmissionPolicy 钩子，
// 把 CEL 决策转换为准入结果（拒绝、告警、审计注解）。
type dispatcher struct {
	matcher generic.PolicyMatcher
	authz   authorizer.UnconditionalAuthorizer
}

var _ generic.Dispatcher[PolicyHook] = &dispatcher{}

// NewDispatcher 构造校验策略分发器。
// authorizer 用于在 CEL 中求值 authorizer 变量；matcher 负责选择器匹配。
func NewDispatcher(
	authorizer authorizer.UnconditionalAuthorizer,
	matcher generic.PolicyMatcher,
) generic.Dispatcher[PolicyHook] {
	return &dispatcher{
		matcher: matcher,
		authz:   authorizer,
	}
}

// policyDecisionWithMetadata 把 CEL 决策与其来源策略、绑定关联起来，
// 以便最终错误消息和指标能指出是哪个策略拒绝了请求。
type policyDecisionWithMetadata struct {
	PolicyDecision
	Definition *admissionregistrationv1.ValidatingAdmissionPolicy
	Binding    *admissionregistrationv1.ValidatingAdmissionPolicyBinding
}

// Start 无后台任务，满足 generic.Dispatcher 的生命周期接口。
func (c *dispatcher) Start(ctx context.Context) error {
	return nil
}

// Dispatch 实现 generic.Dispatcher 接口。
// Dispatch 遍历命中请求的所有策略与绑定并执行 CEL 校验。
// 决策按顺序累积，最终仅以第一个拒绝决策构造错误（其余仍会写入指标/注解）。
// 注意：任何非预期的内部状态（无法识别的 action 等）都会以 error 返回，
// 从而让调用方按失败策略处理，而不是静默放行。
func (c *dispatcher) Dispatch(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces, hooks []PolicyHook) error {
	var deniedDecisions []policyDecisionWithMetadata
	var validationFailures []ValidationFailureValue

	// addConfigError 按策略的 failurePolicy 处理配置类错误：
	// Ignore 时放行，Fail（默认）时拒绝。绑定为 nil 表示错误来自策略本身。
	addConfigError := func(err error, definition *admissionregistrationv1.ValidatingAdmissionPolicy, binding *admissionregistrationv1.ValidatingAdmissionPolicyBinding) {
		// FailurePolicy 未设置时总是会被默认化，并在 API 层完成校验
		var policy admissionregistrationv1.FailurePolicyType
		if definition.Spec.FailurePolicy == nil {
			policy = admissionregistrationv1.Fail
		} else {
			policy = *definition.Spec.FailurePolicy
		}

		// 应用 ValidatingAdmissionPolicy 中指定的 FailurePolicy，默认为 Fail
		switch policy {
		case admissionregistrationv1.Ignore:
			// TODO: 在这里为被忽略的错误补充指标
			return
		case admissionregistrationv1.Fail:
			var message string
			if binding == nil {
				message = fmt.Errorf("failed to configure policy: %w", err).Error()
			} else {
				message = fmt.Errorf("failed to configure binding: %w", err).Error()
			}
			deniedDecisions = append(deniedDecisions, policyDecisionWithMetadata{
				PolicyDecision: PolicyDecision{
					Action:  ActionDeny,
					Message: message,
				},
				Definition: definition,
				Binding:    binding,
			})
		default:
			// 未识别的 failurePolicy 视为失败，宁可拒绝也不误放行。
			deniedDecisions = append(deniedDecisions, policyDecisionWithMetadata{
				PolicyDecision: PolicyDecision{
					Action:  ActionDeny,
					Message: fmt.Errorf("unrecognized failure policy: '%v'", policy).Error(),
				},
				Definition: definition,
				Binding:    binding,
			})
		}
	}

	// 授权结果按请求缓存：同一请求内多个策略重复查询同一 (user, attr) 时
	// 只调用一次底层 authorizer。
	authz := admissionauthorizer.NewCachingAuthorizer(c.authz)
	// 按匹配到的 GVK 缓存 VersionedAttributes，避免同一请求为每个绑定重复转换。
	versionedAttrs := map[schema.GroupVersionKind]*admission.VersionedAttributes{}

	for _, hook := range hooks {
		definition := hook.Policy
		matches, matchResource, matchKind, err := c.matcher.DefinitionMatches(a, o, NewValidatingAdmissionPolicyAccessor(definition))
		if err != nil {
			// 配置错误。
			addConfigError(err, definition, nil)
			continue
		}
		if !matches {
			// 策略定义与请求不匹配
			continue
		} else if hook.ConfigurationError != nil {
			// 配置错误。
			addConfigError(hook.ConfigurationError, definition, nil)
			continue
		}

		auditAnnotationCollector := newAuditAnnotationCollector()
		for _, binding := range hook.Bindings {
			// 若键存在于 dependentBindings 中，则必然有其对应的 bindingInfo
			matches, err := c.matcher.BindingMatches(a, o, NewValidatingAdmissionPolicyBindingAccessor(binding))
			if err != nil {
				// 配置错误。
				addConfigError(err, definition, binding)
				continue
			}
			if !matches {
				continue
			}

			// 收集绑定引用的参数对象；参数可能是集群级或命名空间级，
			// 缺失/类型不符都会作为配置错误按 failurePolicy 处理。
			params, err := generic.CollectParams(
				hook.Policy.Spec.ParamKind,
				hook.ParamInformer,
				hook.ParamScope,
				binding.Spec.ParamRef,
				a.GetNamespace(),
				hook.DynamicClient,
				hook.RESTMapper,
			)

			if err != nil {
				addConfigError(err, definition, binding)
				continue
			}

			versionedAttr := versionedAttrs[matchKind]
			if versionedAttr == nil && len(params) > 0 {
				// 只有带参数时才需要对象的不同版本；延迟转换可省掉无参数策略的开销。
				va, err := admission.NewVersionedAttributes(a, matchKind, o)
				if err != nil {
					wrappedErr := fmt.Errorf("failed to convert object version: %w", err)
					addConfigError(wrappedErr, definition, binding)
					continue
				}
				versionedAttr = va
				versionedAttrs[matchKind] = va
			}

			var validationResults []ValidateResult
			var namespace *v1.Namespace
			namespaceName := a.GetNamespace()

			// Namespace 对象自身的 namespace 字段会等于它自己的名字，
			// 若不置空，CEL 的 namespaceObject 会错误地指向自身。
			if gvk := a.GetKind(); gvk.Kind == "Namespace" && gvk.Version == "v1" && gvk.Group == "" {
				namespaceName = ""
			}

			// 集群级资源没有 namespaceObject；命名空间级资源取不到 Namespace
			// 时直接返回错误，因为策略可能依赖其标签做匹配。
			if namespaceName != "" {
				namespace, err = c.matcher.GetNamespace(ctx, namespaceName)
				if err != nil {
					return err
				}
			}

			for _, param := range params {
				var p runtime.Object = param
				if p != nil && p.GetObjectKind().GroupVersionKind().Empty() {
					// 内部类型不带 TypeMeta，但 CEL 参数对象需要 apiVersion/kind；
					// 用包装器在 ToUnstructured 时补上，避免深拷贝。
					p = &wrappedParam{
						TypeMeta: metav1.TypeMeta{
							APIVersion: definition.Spec.ParamKind.APIVersion,
							Kind:       definition.Spec.ParamKind.Kind,
						},
						nested: param,
					}
				}

				validationResults = append(validationResults,
					hook.Evaluator.Validate(
						ctx,
						matchResource,
						versionedAttr,
						p,
						namespace,
						celconfig.RuntimeCELCostBudget,
						authz,
					),
				)
			}

			for _, validationResult := range validationResults {
				for i, decision := range validationResult.Decisions {
					switch decision.Action {
					case ActionAdmit:
						// 放行仍可能是求值错误（表达式出错但未产生 deny），
						// 因此仅在 EvalError 时记录指标。
						if decision.Evaluation == EvalError {
							celmetrics.Metrics.ObserveAdmission(ctx, decision.Elapsed, definition.Name, binding.Name, ErrorType(&decision))
						}
					case ActionDeny:
						// 绑定的 validationActions 决定拒绝的呈现方式，可同时生效。
						for _, action := range binding.Spec.ValidationActions {
							switch action {
							case admissionregistrationv1.Deny:
								deniedDecisions = append(deniedDecisions, policyDecisionWithMetadata{
									Definition:     definition,
									Binding:        binding,
									PolicyDecision: decision,
								})
								celmetrics.Metrics.ObserveRejection(ctx, decision.Elapsed, definition.Name, binding.Name, ErrorType(&decision))
							case admissionregistrationv1.Audit:
								validationFailures = append(validationFailures, ValidationFailureValue{
									ExpressionIndex:   i,
									Message:           decision.Message,
									ValidationActions: binding.Spec.ValidationActions,
									Binding:           binding.Name,
									Policy:            binding.Spec.PolicyName,
								})
								celmetrics.Metrics.ObserveAudit(ctx, decision.Elapsed, definition.Name, binding.Name, ErrorType(&decision))
							case admissionregistrationv1.Warn:
								// 告警返回给客户端但不阻断请求。
								warning.AddWarning(ctx, "", fmt.Sprintf("Validation failed for ValidatingAdmissionPolicy '%s' with binding '%s': %s", definition.Name, binding.Name, decision.Message))
								celmetrics.Metrics.ObserveWarn(ctx, decision.Elapsed, definition.Name, binding.Name, ErrorType(&decision))
							}
						}
					default:
						return fmt.Errorf("unrecognized evaluation decision '%s' for ValidatingAdmissionPolicyBinding '%s' with ValidatingAdmissionPolicy '%s'",
							decision.Action, binding.Name, definition.Name)
					}
				}

				for _, auditAnnotation := range validationResult.AuditAnnotations {
					switch auditAnnotation.Action {
					case AuditAnnotationActionPublish:
						value := auditAnnotation.Value
						// 截断超长值，防止审计日志被单个注解撑爆。
						if len(auditAnnotation.Value) > maxAuditAnnotationValueLength {
							value = value[:maxAuditAnnotationValueLength]
						}
						auditAnnotationCollector.add(auditAnnotation.Key, value)
					case AuditAnnotationActionError:
						// 注解表达式求值失败视为策略配置错误：产生带错误原因的拒绝决策。
						d := policyDecisionWithMetadata{
							Definition: definition,
							Binding:    binding,
							PolicyDecision: PolicyDecision{
								Action:     ActionDeny,
								Evaluation: EvalError,
								Message:    auditAnnotation.Error,
								Elapsed:    auditAnnotation.Elapsed,
							},
						}
						deniedDecisions = append(deniedDecisions, d)
						celmetrics.Metrics.ObserveRejection(ctx, auditAnnotation.Elapsed, definition.Name, binding.Name, ErrorType(&d.PolicyDecision))
					case AuditAnnotationActionExclude: // 直接跳过
					default:
						return fmt.Errorf("unsupported AuditAnnotation Action: %s", auditAnnotation.Action)
					}
				}
			}
		}
		auditAnnotationCollector.publish(definition.Name, a)
	}

	if len(validationFailures) > 0 {
		publishValidationFailureAnnotations(a, validationFailures)
	}

	if len(deniedDecisions) > 0 {
		// TODO: 重构 admission.NewForbidden，使名称提取可复用，同时 code/reason 可自定义
		// 仅以第一个拒绝决策构造错误消息；其余拒绝仍已记录指标，避免消息膨胀。
		var message string
		deniedDecision := deniedDecisions[0]
		if deniedDecision.Binding != nil {
			message = fmt.Sprintf("ValidatingAdmissionPolicy '%s' with binding '%s' denied request: %s", deniedDecision.Definition.Name, deniedDecision.Binding.Name, deniedDecision.Message)
		} else {
			message = fmt.Sprintf("ValidatingAdmissionPolicy '%s' denied request: %s", deniedDecision.Definition.Name, deniedDecision.Message)
		}
		err := admission.NewForbidden(a, errors.New(message)).(*k8serrors.StatusError)
		// 未显式给出 reason 时按 Invalid 处理，保持与校验失败语义一致。
		reason := deniedDecision.Reason
		if len(reason) == 0 {
			reason = metav1.StatusReasonInvalid
		}
		err.ErrStatus.Reason = reason
		err.ErrStatus.Code = reasonToCode(reason)
		err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes, metav1.StatusCause{Message: message})
		return err
	}
	return nil
}

// maxValidationFailures 是写入审计注解的 validation failure 条数上限，
// 用于防止审计日志资源耗尽。
// 50 条 validation failure 的预期大小约为 10KB。
const maxValidationFailures = 50

// publishValidationFailureAnnotations 把审计型校验失败写入对象注解。
// 超过上限时截断并记录告警，限制审计日志体积。
func publishValidationFailureAnnotations(attributes admission.Attributes, failures []ValidationFailureValue) {
	if len(failures) == 0 {
		return
	}
	if len(failures) > maxValidationFailures {
		klog.Warningf("Validation failures count %d for resource %s exceeds limit of %d; truncating audit annotation", len(failures), attributes.GetResource().String(), maxValidationFailures)
		failures = failures[:maxValidationFailures]
	}

	key := "validation.policy.admission.k8s.io/validation_failure"
	valueJSON, err := utiljson.Marshal(failures)
	if err != nil {
		klog.Warningf("Failed to marshal admission audit validation failures for key %s for ValidatingAdmissionPolicies and ValidatingAdmissionPolicyBindings %s: %v", key, formatPoliciesBindings(failures), err)
		return
	}
	value := string(valueJSON)
	if err := attributes.AddAnnotation(key, value); err != nil {
		klog.Warningf("Failed to set admission audit annotation %s to %s for ValidatingAdmissionPolicies and ValidatingAdmissionPolicyBindings %s: %v", key, value, formatPoliciesBindings(failures), err)
	}
}

// formatPoliciesBindings 把失败项格式化为 "policy/binding, ..." 供日志使用。
func formatPoliciesBindings(failures []ValidationFailureValue) string {
	var policiesBindings []string
	for _, failure := range failures {
		policiesBindings = append(policiesBindings, fmt.Sprintf("%s/%s", failure.Policy, failure.Binding))
	}
	return strings.Join(policiesBindings, ", ")
}

// maxAuditAnnotationValueLength 是单个审计注解值的最大长度（10KB）。
const maxAuditAnnotationValueLength = 10 * 1024

// ValidationFailureValue 是写入审计注解的 JSON 结构。
// 字段名属于对外可见的审计格式，修改需考虑兼容性。
type ValidationFailureValue struct {
	Message           string                                     `json:"message"`
	Policy            string                                     `json:"policy"`
	Binding           string                                     `json:"binding"`
	ExpressionIndex   int                                        `json:"expressionIndex"`
	ValidationActions []admissionregistrationv1.ValidationAction `json:"validationActions"`
}

// auditAnnotationCollector 累积同一策略下各绑定产生的审计注解，
// 在策略处理结束后一次性发布。
type auditAnnotationCollector struct {
	annotations map[string][]string
}

// newAuditAnnotationCollector 创建空的注解收集器。
func newAuditAnnotationCollector() auditAnnotationCollector {
	return auditAnnotationCollector{annotations: map[string][]string{}}
}

// add 追加一个注解值。
func (a auditAnnotationCollector) add(key, value string) {
	// 若多个绑定为审计注解产生了完全相同的键和值，
	// 忽略重复项。
	for _, v := range a.annotations[key] {
		if v == value {
			return
		}
	}
	a.annotations[key] = append(a.annotations[key], value)
}

// publish 把收集到的注解以 "policyName/key" 为键写入对象。
// 同名多值时用逗号拼接，因为注解值只能是字符串。
func (a auditAnnotationCollector) publish(policyName string, attributes admission.Attributes) {
	for key, bindingAnnotations := range a.annotations {
		var value string
		if len(bindingAnnotations) == 1 {
			value = bindingAnnotations[0]
		} else {
			// 当审计注解的 valueExpression 引用了绑定参数时，可能出现多个不同的值，
			// 此时将这些值拼接为逗号分隔的列表。
			value = strings.Join(bindingAnnotations, ", ")
		}
		if err := attributes.AddAnnotation(policyName+"/"+key, value); err != nil {
			klog.Warningf("Failed to set admission audit annotation %s to %s for ValidatingAdmissionPolicy %s: %v", key, value, policyName, err)
		}
	}
}

// wrappedParam 在不复制底层对象的前提下为 CEL 参数补充 TypeMeta。
// 仅在 ToUnstructured 时合并 TypeMeta，其余方法委托或直接报错。
type wrappedParam struct {
	metav1.TypeMeta
	nested runtime.Object
}

// MarshalJSON 未实现：参数只应以非结构化形式暴露给 CEL。
func (w *wrappedParam) MarshalJSON() ([]byte, error) {
	return nil, errors.New("MarshalJSON unimplemented for wrappedParam")
}

// UnmarshalJSON 未实现：wrappedParam 仅用于单向暴露给 CEL。
func (w *wrappedParam) UnmarshalJSON(data []byte) error {
	return errors.New("UnmarshalJSON unimplemented for wrappedParam")
}

// ToUnstructured 把内部对象转换为非结构化表示并合并 TypeMeta，
// 供 CEL 通过 apiVersion/kind 访问。
func (w *wrappedParam) ToUnstructured() interface{} {
	res, err := runtime.DefaultUnstructuredConverter.ToUnstructured(w.nested)

	if err != nil {
		return nil
	}

	metaRes, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&w.TypeMeta)
	if err != nil {
		return nil
	}

	for k, v := range metaRes {
		res[k] = v
	}

	return res
}

// DeepCopyObject 深拷贝包装器及其内部对象。
func (w *wrappedParam) DeepCopyObject() runtime.Object {
	return &wrappedParam{
		TypeMeta: w.TypeMeta,
		nested:   w.nested.DeepCopyObject(),
	}
}

// GetObjectKind 返回自身，使 TypeMeta 字段生效。
func (w *wrappedParam) GetObjectKind() schema.ObjectKind {
	return w
}
