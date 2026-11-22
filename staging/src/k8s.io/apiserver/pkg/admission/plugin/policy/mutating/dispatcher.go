/*
Copyright 2024 The Kubernetes Authors.

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

package mutating

import (
	"context"
	"errors"
	"fmt"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/admission"
	admissionauthorizer "k8s.io/apiserver/pkg/admission/plugin/authorizer"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/admission/plugin/policy/generic"
	"k8s.io/apiserver/pkg/admission/plugin/policy/matching"
	celmetrics "k8s.io/apiserver/pkg/admission/plugin/policy/mutating/metrics"
	"k8s.io/apiserver/pkg/admission/plugin/policy/mutating/patch"
	webhookgeneric "k8s.io/apiserver/pkg/admission/plugin/webhook/generic"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// NewDispatcher 构造 MutatingAdmissionPolicy 的分发器。
// 中文说明：外层 dispatcher 负责持有 matcher/authz/类型转换器，内层通用 Dispatcher
// 负责策略的注册、缓存与失效；把 dispatchInvocations 作为回调注入，
// 使通用部分无需了解 mutation 的补丁应用细节。
func NewDispatcher(a authorizer.UnconditionalAuthorizer, m *matching.Matcher, tcm patch.TypeConverterManager) generic.Dispatcher[PolicyHook] {
	res := &dispatcher{
		matcher:              m,
		authz:                a,
		typeConverterManager: tcm,
	}
	res.Dispatcher = generic.NewPolicyDispatcher[*Policy, *PolicyBinding, PolicyEvaluator](
		NewMutatingAdmissionPolicyAccessor,
		NewMutatingAdmissionPolicyBindingAccessor,
		m,
		res.dispatchInvocations,
	)
	return res
}

// dispatcher 组合策略分发与 mutation 执行所需的依赖。
type dispatcher struct {
	matcher              *matching.Matcher
	authz                authorizer.UnconditionalAuthorizer
	typeConverterManager patch.TypeConverterManager
	generic.Dispatcher[PolicyHook]
}

// Start 先启动类型转换器的后台轮询，再启动策略分发。
// 中文说明：顺序重要——若先分发再轮询，早期到达的 CRD 请求可能因 schema 尚未缓存
// 而返回 503；虽然只是短暂窗口，但无谓的错误会污染客户端重试。
func (d *dispatcher) Start(ctx context.Context) error {
	go d.typeConverterManager.Run(ctx)
	return d.Dispatcher.Start(ctx)
}

// dispatchInvocations 按顺序执行匹配到的策略，并处理 reinvocation 语义。
// 中文说明：核心不变量——
//  1. 同一策略内的多个 mutation 必须按声明顺序串行执行（后一个可能依赖前一个的结果）；
//  2. 只要对象被修改，就需要让其他 mutating 插件以及本插件中 IfNeeded 的策略重新评估；
//  3. 最后一次修改若发生在版本化对象上，必须写回原始对象，否则变更不会持久化。
func (d *dispatcher) dispatchInvocations(
	ctx context.Context,
	a admission.Attributes,
	o admission.ObjectInterfaces,
	versionedAttributes webhookgeneric.VersionedAttributeAccessor,
	invocations []generic.PolicyInvocation[*Policy, *PolicyBinding, PolicyEvaluator],
) ([]generic.PolicyError, *k8serrors.StatusError) {
	var lastVersionedAttr *admission.VersionedAttributes

	// reinvocation 状态挂在 admission 框架的上下文上，跨插件共享；
	// 首次进入时创建并注册，后续 reinvoke 时复用。
	reinvokeCtx := a.GetReinvocationContext()
	var policyReinvokeCtx *policyReinvokeContext
	if v := reinvokeCtx.Value(PluginName); v != nil {
		policyReinvokeCtx = v.(*policyReinvokeContext)
	} else {
		policyReinvokeCtx = &policyReinvokeContext{}
		reinvokeCtx.SetValue(PluginName, policyReinvokeCtx)
	}

	if reinvokeCtx.IsReinvoke() && policyReinvokeCtx.IsOutputChangedSinceLastPolicyInvocation(a.GetObject()) {
		// If the object has changed, we know the in-tree plugin re-invocations have mutated the object,
		// and we need to reinvoke all eligible policies.
		//
		// 中文说明：内建插件（如 ServiceAccount、DefaultStorageClass）在 reinvoke 阶段
		// 也可能修改对象；此时之前的策略结论已失效，必须全部重跑。
		policyReinvokeCtx.RequireReinvokingPreviouslyInvokedPlugins()
	}
	defer func() {
		// 记录本轮结束时的对象快照，供下一轮判断「是否又被改动」。
		policyReinvokeCtx.SetLastPolicyInvocationOutput(a.GetObject())
	}()

	var policyErrors []generic.PolicyError
	addConfigError := func(err error, invocation generic.PolicyInvocation[*Policy, *PolicyBinding, PolicyEvaluator], reason metav1.StatusReason) {
		policyErrors = append(policyErrors, generic.PolicyError{
			Message: err,
			Policy:  NewMutatingAdmissionPolicyAccessor(invocation.Policy),
			Binding: NewMutatingAdmissionPolicyBindingAccessor(invocation.Binding),
			Reason:  reason,
		})
	}

	// There is at least one invocation to invoke. Make sure we have a namespace
	// object if the incoming object is not cluster scoped to pass into the evaluator.
	var namespace *v1.Namespace
	var err error
	namespaceName := a.GetNamespace()

	// Special case, the namespace object has the namespace of itself (maybe a bug).
	// unset it if the incoming object is a namespace
	//
	// 中文说明：创建 Namespace 时请求路径带 namespace 段，但该值就是对象自身，
	// 若不清空会去查找「自己所在命名空间」，导致创建期间查询失败。
	if gvk := a.GetKind(); gvk.Kind == "Namespace" && gvk.Version == "v1" && gvk.Group == "" {
		namespaceName = ""
	}

	// if it is cluster scoped, namespaceName will be empty
	// Otherwise, get the Namespace resource.
	if namespaceName != "" {
		namespace, err = d.matcher.GetNamespace(ctx, namespaceName)
		if err != nil {
			var statusError *k8serrors.StatusError
			if errors.As(err, &statusError) {
				return nil, statusError
			}
			return nil, k8serrors.NewNotFound(schema.GroupResource{Group: "", Resource: "namespaces"}, namespaceName)
		}
	}

	// 授权结果在一次准入内可复用（同一用户、同一策略），缓存避免对每个 mutation 重复授权。
	authz := admissionauthorizer.NewCachingAuthorizer(d.authz)

	// Should loop through invocations, handling possible error and invoking
	// evaluator to apply patch, also should handle re-invocations
	for _, invocation := range invocations {
		if invocation.Evaluator.CompositedCompiler != nil {
			ctx = invocation.Evaluator.CompositedCompiler.CreateContext(ctx)
		}
		if len(invocation.Evaluator.Mutators) != len(invocation.Policy.Spec.Mutations) {
			// This would be a bug. The compiler should always return exactly as
			// many evaluators as there are mutations
			return nil, k8serrors.NewInternalError(fmt.Errorf("expected %v compiled evaluators for policy %v, got %v",
				len(invocation.Policy.Spec.Mutations), invocation.Policy.Name, len(invocation.Evaluator.Mutators)))
		}

		versionedAttr, err := versionedAttributes.VersionedAttribute(invocation.Kind)
		if err != nil {
			// This should never happen, we pre-warm versoined attribute
			// accessors before starting the dispatcher
			return nil, k8serrors.NewInternalError(err)
		}

		if invocation.Evaluator.Matcher != nil {
			matchResults := invocation.Evaluator.Matcher.Match(ctx, versionedAttr, invocation.Param, authz)
			if matchResults.Error != nil {
				addConfigError(matchResults.Error, invocation, metav1.StatusReasonInvalid)
				continue
			}

			// if preconditions are not met, then skip mutations
			if !matchResults.Matches {
				continue
			}
		}

		// key 唯一标识「策略+绑定+参数」这一组合，reinvocation 用它判断该策略是否需要重跑。
		invocationKey, invocationKeyErr := keyFor(invocation)
		if invocationKeyErr != nil {
			// This should never happen. It occurs if there is a programming
			// error causing the Param not to be a valid object.
			return nil, k8serrors.NewInternalError(invocationKeyErr)
		}
		if reinvokeCtx.IsReinvoke() && !policyReinvokeCtx.ShouldReinvoke(invocationKey) {
			continue
		}

		objectBeforeMutations := versionedAttr.VersionedObject.Object()
		// Mutations for a single invocation of a MutatingAdmissionPolicy are evaluated
		// in order.
		for mutationIndex := range invocation.Policy.Spec.Mutations {
			lastVersionedAttr = versionedAttr
			if versionedAttr.VersionedObject.Object() == nil { // Do not call patchers if there is no object to patch.
				continue
			}

			patcher := invocation.Evaluator.Mutators[mutationIndex]
			optionalVariables := cel.OptionalVariableBindings{VersionedParams: invocation.Param, Authorizer: authz}
			startTime := time.Now()
			err = d.dispatchOne(ctx, patcher, o, versionedAttr, namespace, invocation.Resource, optionalVariables)
			elapsed := time.Since(startTime)

			if err != nil {
				celmetrics.Metrics.ObserveRejection(ctx, elapsed, invocation.Policy.Name, invocation.Binding.Name, ErrorType(err))
				var statusError *k8serrors.StatusError
				if errors.As(err, &statusError) {
					// 已经是结构化错误（如配额/禁止），直接上抛，不再包装成配置错误。
					return nil, statusError
				}

				addConfigError(err, invocation, metav1.StatusReasonInvalid)
				continue
			} else {
				celmetrics.Metrics.ObserveAdmission(ctx, elapsed, invocation.Policy.Name, invocation.Binding.Name, celmetrics.MutationNoError)
			}
		}
		if !apiequality.Semantic.DeepEqual(objectBeforeMutations, versionedAttr.VersionedObject.Object()) {
			// The mutation has changed the object. Prepare to reinvoke all previous mutations that are eligible for re-invocation.
			//
			// 中文说明：使用语义相等而非字节比较，忽略默认值等无关差异，
			// 避免把「无实质变化」误判为需要重跑。
			policyReinvokeCtx.RequireReinvokingPreviouslyInvokedPlugins()
			reinvokeCtx.SetShouldReinvoke()
		}
		if invocation.Policy.Spec.ReinvocationPolicy == admissionregistrationv1.IfNeededReinvocationPolicy {
			policyReinvokeCtx.AddReinvocablePolicyToPreviouslyInvoked(invocationKey)
		}
	}

	// 最后一处修改可能发生在版本化对象（非原始对象）上，必须转换回原始对象的版本，
	// 否则后续插件与持久化看到的仍是旧值。
	if lastVersionedAttr != nil && lastVersionedAttr.VersionedObject.Object() != nil && lastVersionedAttr.Dirty {
		policyReinvokeCtx.RequireReinvokingPreviouslyInvokedPlugins()
		reinvokeCtx.SetShouldReinvoke()
		if err := o.GetObjectConvertor().Convert(lastVersionedAttr.VersionedObject.Object(), lastVersionedAttr.Attributes.GetObject(), nil); err != nil {
			return nil, k8serrors.NewInternalError(fmt.Errorf("failed to convert object: %w", err))
		}
	}

	return policyErrors, nil
}

// dispatchOne 对单个 mutation 求值并应用补丁，然后把结果写回 versionedAttributes。
// 中文说明：类型转换器缺失时返回 503 而非错误——CRD 刚创建时 schema 尚未被
// 类型转换管理器拉取，这属于暂时性状态，客户端应重试。
func (d *dispatcher) dispatchOne(
	ctx context.Context,
	patcher patch.Patcher,
	o admission.ObjectInterfaces,
	versionedAttributes *admission.VersionedAttributes,
	namespace *v1.Namespace,
	resource schema.GroupVersionResource,
	optionalVariables cel.OptionalVariableBindings,
) (err error) {
	if patcher == nil {
		// internal error. this should not happen
		return k8serrors.NewInternalError(fmt.Errorf("policy evaluator is nil"))
	}

	// Find type converter for the invoked Group-Version.
	typeConverter := d.typeConverterManager.GetTypeConverter(versionedAttributes.VersionedKind)
	if typeConverter == nil {
		// This can happen if the request is for a resource whose schema
		// has not been registered with the type converter manager.
		return k8serrors.NewServiceUnavailable(fmt.Sprintf("Resource kind %s not found. There can be a delay between when CustomResourceDefinitions are created and when they are available.", versionedAttributes.VersionedKind))
	}

	patchRequest := patch.Request{
		MatchedResource:     resource,
		VersionedAttributes: versionedAttributes,
		ObjectInterfaces:    o,
		OptionalVariables:   optionalVariables,
		Namespace:           namespace,
		TypeConverter:       typeConverter,
	}
	// 预算限制防止恶意 CEL 表达式耗尽 CPU；超出预算会返回错误而非无限计算。
	newVersionedObject, err := patcher.Patch(ctx, patchRequest, celconfig.RuntimeCELCostBudget)
	if err != nil {
		return err
	}

	switch versionedAttributes.VersionedObject.Object().(type) {
	case *unstructured.Unstructured:
		// No conversion needed before defaulting for the patch object if the admitted object is unstructured.
	default:
		// Before defaulting, if the admitted object is a typed object, convert unstructured patch result back to a typed object.
		//
		// 中文说明：补丁结果总是 unstructured；若原对象是类型化对象，必须先转回类型化
		// 再执行 defaulting，否则默认值函数无法作用于 unstructured。
		newVersionedObject, err = o.GetObjectConvertor().ConvertToVersion(newVersionedObject, versionedAttributes.GetKind().GroupVersion())
		if err != nil {
			return err
		}
	}
	o.GetObjectDefaulter().Default(newVersionedObject)
	versionedAttributes.UpdateObject(newVersionedObject)
	return nil
}

// keyFor 构造策略调用的稳定标识，用于 reinvocation 去重与判断。
// 中文说明：Param 可为 nil（策略无参数），此时 ParamUID 为零值；
// 三个命名空间/名称共同唯一确定一次调用。
func keyFor(invocation generic.PolicyInvocation[*Policy, *PolicyBinding, PolicyEvaluator]) (key, error) {
	var paramUID types.NamespacedName
	if invocation.Param != nil {
		paramAccessor, err := meta.Accessor(invocation.Param)
		if err != nil {
			// This should never happen, as the param should have been validated
			// before being passed to the plugin.
			return key{}, err
		}
		paramUID = types.NamespacedName{
			Name:      paramAccessor.GetName(),
			Namespace: paramAccessor.GetNamespace(),
		}
	}

	return key{
		PolicyUID: types.NamespacedName{
			Name:      invocation.Policy.GetName(),
			Namespace: invocation.Policy.GetNamespace(),
		},
		BindingUID: types.NamespacedName{
			Name:      invocation.Binding.GetName(),
			Namespace: invocation.Binding.GetNamespace(),
		},
		ParamUID: paramUID,
	}, nil
}
