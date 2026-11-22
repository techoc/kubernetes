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

package generic

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/policy/matching"
	webhookgeneric "k8s.io/apiserver/pkg/admission/plugin/webhook/generic"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
)

// PolicyInvocation is a single policy-binding-param tuple from a Policy Hook
// in the context of a specific request. The params have already been resolved
// and any error in configuration or setting up the invocation is stored in
// the Error field.
//
// 一个绑定若通过 selector 匹配到多个 param，会展开成多条 invocation，
// 因此下游 delegate 必须能处理"同一绑定多次求值"的情况。
type PolicyInvocation[P runtime.Object, B runtime.Object, E Evaluator] struct {
	// Relevant policy for this hook.
	// This field is always populated
	Policy P

	// Matched Kind for the request given the policy's matchconstraints
	// May be empty if there was an error matching the resource
	Kind schema.GroupVersionKind

	// Matched Resource for the request given the policy's matchconstraints
	// May be empty if there was an error matching the resource
	Resource schema.GroupVersionResource

	// Relevant binding for this hook.
	// May be empty if there was an error with the policy's configuration itself
	Binding B

	// Compiled policy evaluator
	Evaluator E

	// Params fetched by the binding to use to evaluate the policy
	Param runtime.Object
}

// dispatcherDelegate is called during a request with a pre-filtered list
// of (Policy, Binding, Param) tuples that are active and match the request.
// The dispatcher delegate is responsible for updating the object on the
// admission attributes in the case of mutation, or returning a status error in
// the case of validation.
//
// The delegate provides the "validation" or "mutation" aspect of dispatcher functionality
// (in contrast to generic.PolicyDispatcher which only selects active policies and params)
type dispatcherDelegate[P, B runtime.Object, E Evaluator] func(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces, versionedAttributes webhookgeneric.VersionedAttributeAccessor, invocations []PolicyInvocation[P, B, E]) ([]PolicyError, *apierrors.StatusError)

// policyDispatcher 负责从所有 hook 中筛出与当前请求匹配的 (policy, binding, param)，
// 再交给 delegate 执行具体的变异或校验。泛型参数让 webhook 与 VAP/MAP 复用同一套调度。
type policyDispatcher[P runtime.Object, B runtime.Object, E Evaluator] struct {
	newPolicyAccessor  func(P) PolicyAccessor
	newBindingAccessor func(B) BindingAccessor
	matcher            PolicyMatcher
	delegate           dispatcherDelegate[P, B, E]
}

// NewPolicyDispatcher 组装调度器；matcher 与 delegate 由具体插件提供。
func NewPolicyDispatcher[P runtime.Object, B runtime.Object, E Evaluator](
	newPolicyAccessor func(P) PolicyAccessor,
	newBindingAccessor func(B) BindingAccessor,
	matcher *matching.Matcher,
	delegate dispatcherDelegate[P, B, E],
) Dispatcher[PolicyHook[P, B, E]] {
	return &policyDispatcher[P, B, E]{
		newPolicyAccessor:  newPolicyAccessor,
		newBindingAccessor: newBindingAccessor,
		matcher:            NewPolicyMatcher(matcher),
		delegate:           delegate,
	}
}

// Dispatch implements generic.Dispatcher. It loops through all active hooks
// (policy x binding pairs) and selects those which are active for the current
// request. It then resolves all params and creates an Invocation for each
// matching policy-binding-param tuple. The delegate is then called with the
// list of tuples.
//
// Start 无后台工作，满足 Dispatcher 接口即可。
func (d *policyDispatcher[P, B, E]) Start(ctx context.Context) error {
	return nil
}

// Note: MatchConditions expressions are not evaluated here. The dispatcher delegate
// is expected to ignore the result of any policies whose match conditions dont pass.
// This may be possible to refactor so matchconditions are checked here instead.
func (d *policyDispatcher[P, B, E]) Dispatch(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces, hooks []PolicyHook[P, B, E]) error {
	var relevantHooks []PolicyInvocation[P, B, E]
	// Construct all the versions we need to call our webhooks
	versionedAttrAccessor := &versionedAttributeAccessor{
		versionedAttrs:   map[schema.GroupVersionKind]*admission.VersionedAttributes{},
		attr:             a,
		objectInterfaces: o,
	}

	var policyErrors []PolicyError
	// 配置错误先收集而不是立即返回：单个策略配置错误不应阻止其它策略生效，
	// 最终是否拒绝由各策略的 FailurePolicy 决定。
	addConfigError := func(err error, definition PolicyAccessor, binding BindingAccessor) {
		var message error
		if binding == nil {
			message = fmt.Errorf("failed to configure policy: %w", err)
		} else {
			message = fmt.Errorf("failed to configure binding: %w", err)
		}

		policyErrors = append(policyErrors, PolicyError{
			Policy:  definition,
			Binding: binding,
			Message: message,
		})
	}

	for _, hook := range hooks {
		policyAccessor := d.newPolicyAccessor(hook.Policy)
		matches, matchGVR, matchGVK, err := d.matcher.DefinitionMatches(a, o, policyAccessor)
		if err != nil {
			// There was an error evaluating if this policy matches anything.
			addConfigError(err, policyAccessor, nil)
			continue
		} else if !matches {
			continue
		} else if hook.ConfigurationError != nil {
			// 策略本身已匹配，但编译期就配置有误（如 CEL 无法编译）。
			addConfigError(hook.ConfigurationError, policyAccessor, nil)
			continue
		}

		for _, binding := range hook.Bindings {
			bindingAccessor := d.newBindingAccessor(binding)
			matches, err = d.matcher.BindingMatches(a, o, bindingAccessor)
			if err != nil {
				// There was an error evaluating if this binding matches anything.
				addConfigError(err, policyAccessor, bindingAccessor)
				continue
			} else if !matches {
				continue
			}

			// here the binding matches.
			// VersionedAttr result will be cached and reused later during parallel
			// hook calls.
			if _, err = versionedAttrAccessor.VersionedAttribute(matchGVK); err != nil {
				// VersionedAttr result will be cached and reused later during parallel
				// hook calls.
				addConfigError(err, policyAccessor, nil)
				continue
			}

			// Collect params for this binding
			params, err := CollectParams(
				policyAccessor.GetParamKind(),
				hook.ParamInformer,
				hook.ParamScope,
				bindingAccessor.GetParamRef(),
				a.GetNamespace(),
				hook.DynamicClient,
				hook.RESTMapper,
			)
			if err != nil {
				// There was an error collecting params for this binding.
				addConfigError(err, policyAccessor, bindingAccessor)
				continue
			}

			// If params is empty and there was no error, that means that
			// ParamNotFoundAction is ignore, so it shouldnt be added to list
			for _, param := range params {
				relevantHooks = append(relevantHooks, PolicyInvocation[P, B, E]{
					Policy:    hook.Policy,
					Binding:   binding,
					Kind:      matchGVK,
					Resource:  matchGVR,
					Param:     param,
					Evaluator: hook.Evaluator,
				})
			}
		}
	}

	if len(relevantHooks) > 0 {
		extraPolicyErrors, statusError := d.delegate(ctx, a, o, versionedAttrAccessor, relevantHooks)
		// delegate 返回的 StatusError 是硬性失败（例如无法构造请求对象），直接终止。
		if statusError != nil {
			return statusError
		}
		policyErrors = append(policyErrors, extraPolicyErrors...)
	}

	var filteredErrors []PolicyError
	for _, e := range policyErrors {
		// we always default the FailurePolicy if it is unset and validate it in API level
		var policy v1.FailurePolicyType
		if fp := e.Policy.GetFailurePolicy(); fp == nil {
			policy = v1.Fail
		} else {
			policy = *fp
		}

		switch policy {
		case v1.Ignore:
			// TODO: add metrics for ignored error here
			continue
		case v1.Fail:
			filteredErrors = append(filteredErrors, e)
		default:
			// 未知取值按 Fail 处理，避免配置异常导致策略被静默跳过。
			filteredErrors = append(filteredErrors, e)
		}
	}

	if len(filteredErrors) > 0 {

		forbiddenErr := admission.NewForbidden(a, fmt.Errorf("admission request denied by policy"))

		// The forbiddenErr is always a StatusError.
		var err *apierrors.StatusError
		if !errors.As(forbiddenErr, &err) {
			// Should never happen.
			return apierrors.NewInternalError(fmt.Errorf("failed to create status error"))
		}
		// 先清空占位消息，下面用第一条真实的拒绝原因填充。
		err.ErrStatus.Message = ""

		for _, policyError := range filteredErrors {
			message := policyError.Error()

			// If this is the first denied decision, use its message and reason
			// for the status error message.
			if err.ErrStatus.Message == "" {
				err.ErrStatus.Message = message
				if policyError.Reason != "" {
					err.ErrStatus.Reason = policyError.Reason
				}
			}

			// Add the denied decision's message to the status error's details
			err.ErrStatus.Details.Causes = append(
				err.ErrStatus.Details.Causes,
				metav1.StatusCause{Message: message})
		}

		return err
	}

	return nil
}

// Returns params to use to evaluate a policy-binding with given param
// configuration. If the policy-binding has no param configuration, it
// returns a single-element list with a nil param.
//
// The dynamicClient and restMapper parameters enable direct API fallback when
// the informer cache hasn't received the watch event for a newly created param yet.
//
// 参数解析失败会被上抛为配置错误，最终由绑定的 FailurePolicy 决定是否拒绝请求；
// 因此这里的错误信息要足够具体，便于用户定位是 paramKind、namespace 还是 selector 的问题。
func CollectParams(
	paramKind *v1.ParamKind,
	paramInformer informers.GenericInformer,
	paramScope meta.RESTScope,
	paramRef *v1.ParamRef,
	namespace string,
	dynamicClient dynamic.Interface,
	restMapper meta.RESTMapper,
) ([]runtime.Object, error) {
	// If definition has paramKind, paramRef is required in binding.
	// If definition has no paramKind, paramRef set in binding will be ignored.
	var params []runtime.Object
	var paramStore cache.GenericNamespaceLister

	// Make sure the param kind is ready to use
	if paramKind != nil && paramRef != nil {
		if paramInformer == nil {
			return nil, fmt.Errorf("paramKind kind `%v` not known",
				paramKind.String())
		}

		// Set up cluster-scoped, or namespaced access to the params
		// "default" if not provided, and paramKind is namespaced
		paramStore = paramInformer.Lister()
		if paramScope.Name() == meta.RESTScopeNameNamespace {
			paramsNamespace := namespace
			if len(paramRef.Namespace) > 0 {
				paramsNamespace = paramRef.Namespace
			} else if len(paramsNamespace) == 0 {
				// You must supply namespace if your matcher can possibly
				// match a cluster-scoped resource
				return nil, fmt.Errorf("cannot use namespaced paramRef in policy binding that matches cluster-scoped resources")
			}

			paramStore = paramInformer.Lister().ByNamespace(paramsNamespace)
		}

		// If the param informer for this admission policy has not yet
		// had time to perform an initial listing, don't attempt to use
		// it.
		// 限时 1 秒等待首次同步：准入路径不能无限阻塞，超时按配置错误处理。
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		if !cache.WaitForCacheSync(timeoutCtx.Done(), paramInformer.Informer().HasSynced) {
			return nil, fmt.Errorf("paramKind kind `%v` not yet synced to use for admission",
				paramKind.String())
		}
	}

	// Find params to use with policy
	switch {
	case paramKind == nil:
		// ParamKind is unset. Ignore any globalParamRef or namespaceParamRef
		// setting.
		return []runtime.Object{nil}, nil
	case paramRef == nil:
		// Policy ParamKind is set, but binding does not use it.
		// Validate with nil params
		return []runtime.Object{nil}, nil
	case len(paramRef.Namespace) > 0 && paramScope.Name() == meta.RESTScopeRoot.Name():
		// Not allowed to set namespace for cluster-scoped param
		return nil, fmt.Errorf("paramRef.namespace must not be provided for a cluster-scoped `paramKind`")

	case len(paramRef.Name) > 0:
		if paramRef.Selector != nil {
			// This should be validated, but just in case.
			return nil, fmt.Errorf("paramRef.name and paramRef.selector are mutually exclusive")
		}

		// First attempt: try to get the param from the informer cache (fast path)
		param, err := paramStore.Get(paramRef.Name)

		// If cache returns NotFound and we have a client, try direct API call as fallback.
		// This handles the race condition where resources (ConfigMap, Policy, Binding, and Param)
		// are created in quick succession. The informer cache may have completed its initial
		// sync (checked by WaitForCacheSync above), but newly created params might not have
		// propagated to the cache yet. The direct API call ensures we get the correct answer
		// without timing-dependent retry logic.
		if apierrors.IsNotFound(err) && dynamicClient != nil && restMapper != nil && paramKind != nil {
			param, err = getParamDirectly(dynamicClient, restMapper, paramKind, paramRef, namespace, paramScope)
		}

		switch {
		case err == nil:
			params = []runtime.Object{param}
		case apierrors.IsNotFound(err):
			// Param not yet available. User may need to wait a bit
			// before being able to use it for validation.
			//
			// Set params to nil to prepare for not found action
			params = nil
		case apierrors.IsInvalid(err):
			// Param mis-configured
			// require to set namespace for namespaced resource
			// and unset namespace for cluster scoped resource
			return nil, err
		default:
			// Internal error
			utilruntime.HandleError(err)
			return nil, err
		}
	case paramRef.Selector != nil:
		// Select everything by default if empty name and selector
		selector, err := metav1.LabelSelectorAsSelector(paramRef.Selector)
		if err != nil {
			// Cannot parse label selector: configuration error
			return nil, err

		}

		paramList, err := paramStore.List(selector)
		if err != nil {
			// There was a bad internal error
			utilruntime.HandleError(err)
			return nil, err
		}

		// Successfully grabbed params
		params = paramList
	default:
		// Should be unreachable due to validation
		return nil, fmt.Errorf("one of name or selector must be provided")
	}

	// Apply fail action for params not found case
	// Deny 动作下"没找到参数"本身就要拒绝请求，否则绑定会静默失效。
	if len(params) == 0 && paramRef.ParameterNotFoundAction != nil && *paramRef.ParameterNotFoundAction == v1.DenyAction {
		return nil, errors.New("no params found for policy binding with `Deny` parameterNotFoundAction")
	}

	return params, nil
}

// getParamDirectly performs a direct API call to retrieve a param when the informer
// cache doesn't have it yet. This handles the race condition where a param resource
// is created but the watch event hasn't propagated to the informer cache.
//
// 直接读 API 比"重试等 watch"更可预测：一次同步 GET 要么拿到对象，
// 要么明确 NotFound，避免在准入路径上引入不确定的等待时间。
func getParamDirectly(
	dynamicClient dynamic.Interface,
	restMapper meta.RESTMapper,
	paramKind *v1.ParamKind,
	paramRef *v1.ParamRef,
	namespace string,
	paramScope meta.RESTScope,
) (runtime.Object, error) {
	// Convert GVK to GVR using the RESTMapper
	gv, err := schema.ParseGroupVersion(paramKind.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid paramKind APIVersion %q: %w", paramKind.APIVersion, err)
	}
	gvk := gv.WithKind(paramKind.Kind)

	mapping, err := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		// An unknown paramKind (CRD not registered or deleted) is reported as
		// NotFound so the caller routes it through ParameterNotFoundAction
		// instead of failing the admission request as an internal error.
		if meta.IsNoMatchError(err) {
			return nil, apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, paramRef.Name)
		}
		return nil, fmt.Errorf("failed to find REST mapping for %v: %w", gvk, err)
	}

	// Determine the namespace for the Get request
	var targetNamespace string
	if paramScope.Name() == meta.RESTScopeNameNamespace {
		if len(paramRef.Namespace) > 0 {
			targetNamespace = paramRef.Namespace
		} else {
			targetNamespace = namespace
		}
	}

	// Perform the direct API call with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var resourceClient dynamic.ResourceInterface
	if targetNamespace != "" {
		resourceClient = dynamicClient.Resource(mapping.Resource).Namespace(targetNamespace)
	} else {
		resourceClient = dynamicClient.Resource(mapping.Resource)
	}

	param, err := resourceClient.Get(ctx, paramRef.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return convertParamToInformerRepresentation(gvk, param)
}

// convertParamToInformerRepresentation makes a get response object look like an informer object by
// representing it as unstructured and without TypeMeta. This ensures that CEL expressions
// receive param objects identically regardless of if the object was received via the informer
// or if an informer cache miss resulted in a get request to fetch the object.
//
// 关键一致性要求：同一 param 无论来自缓存还是直接 GET，CEL 看到的对象形态必须相同，
// 否则同一表达式会在两条路径上给出不同结果。
func convertParamToInformerRepresentation(gvk schema.GroupVersionKind, param *unstructured.Unstructured) (runtime.Object, error) {
	typed, err := clientgoscheme.Scheme.New(gvk)
	if err != nil {
		// The kind has no typed representation, so its informer also serves unstructured objects.
		return param, nil
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(param.UnstructuredContent(), typed); err != nil {
		return nil, fmt.Errorf("failed to convert param %v to typed object: %w", gvk, err)
	}
	// informer 中的对象不带 TypeMeta，这里清空以保持两条路径一致。
	typed.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
	return typed, nil
}

var _ webhookgeneric.VersionedAttributeAccessor = &versionedAttributeAccessor{}

// versionedAttributeAccessor 按 GVK 缓存转换结果。
// 同一次请求内多个策略可能请求同一 GVK，缓存避免重复的版本转换开销。
// 非并发安全：仅在 Dispatch 的单线程筛选阶段使用。
type versionedAttributeAccessor struct {
	versionedAttrs   map[schema.GroupVersionKind]*admission.VersionedAttributes
	attr             admission.Attributes
	objectInterfaces admission.ObjectInterfaces
}

// VersionedAttribute 返回指定 GVK 的属性视图，首次调用时创建并缓存。
func (v *versionedAttributeAccessor) VersionedAttribute(gvk schema.GroupVersionKind) (*admission.VersionedAttributes, error) {
	if val, ok := v.versionedAttrs[gvk]; ok {
		return val, nil
	}
	versionedAttr, err := admission.NewVersionedAttributes(v.attr, gvk, v.objectInterfaces)
	if err != nil {
		return nil, err
	}
	v.versionedAttrs[gvk] = versionedAttr
	return versionedAttr, nil
}

// PolicyError 描述某条策略/绑定在本次请求中的失败。
// 保留 Policy 与 Binding 引用，便于按 FailurePolicy 决策并生成可定位的错误信息。
type PolicyError struct {
	Policy  PolicyAccessor
	Binding BindingAccessor
	Message error
	Reason  metav1.StatusReason
}

// Error 生成用户可读的拒绝原因；有绑定时同时指出策略与绑定名称。
func (c PolicyError) Error() string {
	if c.Binding != nil {
		return fmt.Sprintf("policy '%s' with binding '%s' denied request: %s", c.Policy.GetName(), c.Binding.GetName(), c.Message.Error())
	}

	return fmt.Sprintf("policy %q denied request: %s", c.Policy.GetName(), c.Message.Error())
}
