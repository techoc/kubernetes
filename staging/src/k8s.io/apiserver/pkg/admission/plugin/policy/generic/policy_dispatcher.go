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

// PolicyInvocation 表示某个请求上下文中来自一条 Policy Hook 的
// policy-binding-param 三元组。params 已解析完毕，配置或构造 invocation
// 过程中的错误存放在 Error 字段。
// 一个绑定若通过 selector 匹配到多个 param，会展开成多条 invocation，
// 因此下游 delegate 必须能处理“同一绑定多次求值”的情况。
type PolicyInvocation[P runtime.Object, B runtime.Object, E Evaluator] struct {
	// 与本次 hook 相关的策略；该字段总是有值。
	Policy P

	// 根据策略的 matchConstraints 匹配出的请求 Kind；
	Kind schema.GroupVersionKind

	// 根据策略的 matchConstraints 匹配出的请求 Resource；
	Resource schema.GroupVersionResource

	// 与本次 hook 相关的绑定；
	// 策略自身配置有误时可能为空。
	Binding B

	// 编译后的策略评估器。
	Evaluator E

	// 绑定获取的、用于评估策略的参数。
	Param runtime.Object
}

// dispatcherDelegate 在请求期间被调用，接收已预先过滤好的、处于激活状态且与请求
// 匹配的 (Policy, Binding, Param) 三元组列表。delegate 负责在变异场景下更新
// admission attributes 上的对象，或在校验场景下返回状态错误。
// delegate 提供调度功能中“校验”或“变异”的部分
// （与之相对，generic.PolicyDispatcher 只负责筛选激活的策略与参数）。
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

// Dispatch 实现 generic.Dispatcher：遍历所有激活的 hook（policy×binding 组合），
// 筛出与当前请求匹配的项，解析全部 params，为每个匹配的
// policy-binding-param 三元组构造 Invocation，再携带该列表调用 delegate。
// Start 无后台工作，满足 Dispatcher 接口即可。
func (d *policyDispatcher[P, B, E]) Start(ctx context.Context) error {
	return nil
}

// 注意：MatchConditions 表达式不在这里求值。delegate 需要自行忽略
// match conditions 未通过的策略结果。将来或许可以重构，把
// matchconditions 的检查挪到这里。
func (d *policyDispatcher[P, B, E]) Dispatch(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces, hooks []PolicyHook[P, B, E]) error {
	var relevantHooks []PolicyInvocation[P, B, E]
	// 构造调用 webhook 所需的全部版本化属性。
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
			// 评估策略是否匹配时出错。
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
				// 评估绑定是否匹配时出错。
				addConfigError(err, policyAccessor, bindingAccessor)
				continue
			} else if !matches {
				continue
			}

			// 到这里绑定已匹配。
			// VersionedAttr 的结果会被缓存，供后续并行调用 hook 时复用。
			if _, err = versionedAttrAccessor.VersionedAttribute(matchGVK); err != nil {
				addConfigError(err, policyAccessor, nil)
				continue
			}

			// 收集该绑定的 params。
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
				// 收集该绑定的 params 时出错。
				addConfigError(err, policyAccessor, bindingAccessor)
				continue
			}

			// params 为空且没有出错，说明 ParamNotFoundAction 是 Ignore，
			// 因此不需要加入列表。
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
		// FailurePolicy 未设置时一律默认为 Fail，且已在 API 层做过校验。
		var policy v1.FailurePolicyType
		if fp := e.Policy.GetFailurePolicy(); fp == nil {
			policy = v1.Fail
		} else {
			policy = *fp
		}

		switch policy {
		case v1.Ignore:
			// TODO: 在此为被忽略的错误添加指标。
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

		// forbiddenErr 一定是 StatusError。
		var err *apierrors.StatusError
		if !errors.As(forbiddenErr, &err) {
			// 理论上不会发生。
			return apierrors.NewInternalError(fmt.Errorf("failed to create status error"))
		}
		// 先清空占位消息，下面用第一条真实的拒绝原因填充。
		err.ErrStatus.Message = ""

		for _, policyError := range filteredErrors {
			message := policyError.Error()

			// 如果是第一条被拒绝的决定，用它的 message 和 reason
			// 作为状态错误的消息。
			if err.ErrStatus.Message == "" {
				err.ErrStatus.Message = message
				if policyError.Reason != "" {
					err.ErrStatus.Reason = policyError.Reason
				}
			}

			// 把被拒绝决定的消息加入状态错误的 details。
			err.ErrStatus.Details.Causes = append(
				err.ErrStatus.Details.Causes,
				metav1.StatusCause{Message: message})
		}

		return err
	}

	return nil
}

// 返回用于配合给定 param 配置评估某条 policy-binding 的 params；
// 若该 policy-binding 没有 param 配置，返回只含一个 nil param 的列表。
//
// dynamicClient 与 restMapper 参数支持在 informer 缓存尚未收到
// 新建 param 的 watch 事件时，直接回退到 API 读取。
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
	// 定义带有 paramKind 时，绑定必须提供 paramRef；
	// 定义没有 paramKind 时，绑定中设置的 paramRef 将被忽略。
	var params []runtime.Object
	var paramStore cache.GenericNamespaceLister

	// 确保 param kind 可用。
	if paramKind != nil && paramRef != nil {
		if paramInformer == nil {
			return nil, fmt.Errorf("paramKind kind `%v` not known",
				paramKind.String())
		}

		// 建立集群级或命名空间级的 param 访问；
		// paramKind 为命名空间级且未提供 namespace 时回退到 default。
		paramStore = paramInformer.Lister()
		if paramScope.Name() == meta.RESTScopeNameNamespace {
			paramsNamespace := namespace
			if len(paramRef.Namespace) > 0 {
				paramsNamespace = paramRef.Namespace
			} else if len(paramsNamespace) == 0 {
				// 如果 matcher 可能匹配集群级资源，
				// 则必须提供 namespace。
				return nil, fmt.Errorf("cannot use namespaced paramRef in policy binding that matches cluster-scoped resources")
			}

			paramStore = paramInformer.Lister().ByNamespace(paramsNamespace)
		}

		// 若该准入策略的 param informer 尚未完成首次列表，就不要尝试使用它。
		// 限时 1 秒等待首次同步：准入路径不能无限阻塞，超时按配置错误处理。
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		if !cache.WaitForCacheSync(timeoutCtx.Done(), paramInformer.Informer().HasSynced) {
			return nil, fmt.Errorf("paramKind kind `%v` not yet synced to use for admission",
				paramKind.String())
		}
	}

	// 查找与策略配合使用的 params。
	switch {
	case paramKind == nil:
		// ParamKind 未设置，忽略 globalParamRef 或 namespaceParamRef 的配置。
		return []runtime.Object{nil}, nil
	case paramRef == nil:
		// 策略的 ParamKind 已设置，但绑定没有使用它；
		// 以 nil param 校验。
		return []runtime.Object{nil}, nil
	case len(paramRef.Namespace) > 0 && paramScope.Name() == meta.RESTScopeRoot.Name():
		// 集群级 param 不允许设置 namespace。
		return nil, fmt.Errorf("paramRef.namespace must not be provided for a cluster-scoped `paramKind`")

	case len(paramRef.Name) > 0:
		if paramRef.Selector != nil {
			// 本应由校验保证，这里只是兜底。
			return nil, fmt.Errorf("paramRef.name and paramRef.selector are mutually exclusive")
		}

		// 第一步：尝试从 informer 缓存获取 param（快路径）。
		param, err := paramStore.Get(paramRef.Name)

		// 缓存返回 NotFound 且有 client 时，尝试直接调用 API 作为回退。
		// 这处理了 ConfigMap、Policy、Binding、Param 被连续快速创建时的竞态：
		// informer 缓存可能已完成首次同步（上面的 WaitForCacheSync 已检查），
		// 但新建的 param 尚未传播到缓存。直接 API 调用保证拿到正确结果，
		// 无需依赖时序的重试逻辑。
		if apierrors.IsNotFound(err) && dynamicClient != nil && restMapper != nil && paramKind != nil {
			param, err = getParamDirectly(dynamicClient, restMapper, paramKind, paramRef, namespace, paramScope)
		}

		switch {
		case err == nil:
			params = []runtime.Object{param}
		case apierrors.IsNotFound(err):
			// param 尚不可用，用户可能需要稍等才能用它做校验。
			//
			// 将 params 置为 nil，为 not found 动作做准备。
			params = nil
		case apierrors.IsInvalid(err):
			// param 配置有误：命名空间级资源必须设置 namespace，
			// 集群级资源必须不设 namespace。
			return nil, err
		default:
			// 内部错误。
			utilruntime.HandleError(err)
			return nil, err
		}
	case paramRef.Selector != nil:
		// name 与 selector 均为空时默认全选。
		selector, err := metav1.LabelSelectorAsSelector(paramRef.Selector)
		if err != nil {
			// 无法解析 label selector：属于配置错误。
			return nil, err

		}

		paramList, err := paramStore.List(selector)
		if err != nil {
			// 严重的内部错误。
			utilruntime.HandleError(err)
			return nil, err
		}

		// 成功获取到 params。
		params = paramList
	default:
		// 有 API 校验保证，理论上不可达。
		return nil, fmt.Errorf("one of name or selector must be provided")
	}

	// Deny 动作下"没找到参数"本身就要拒绝请求，否则绑定会静默失效。
	if len(params) == 0 && paramRef.ParameterNotFoundAction != nil && *paramRef.ParameterNotFoundAction == v1.DenyAction {
		return nil, errors.New("no params found for policy binding with `Deny` parameterNotFoundAction")
	}

	return params, nil
}

// getParamDirectly 在 informer 缓存还没有该 param 时直接调用 API 获取，
// 用于处理 param 资源刚创建、watch 事件尚未传播到缓存的竞态。
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
	// 使用 RESTMapper 将 GVK 转换为 GVR。
	gv, err := schema.ParseGroupVersion(paramKind.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid paramKind APIVersion %q: %w", paramKind.APIVersion, err)
	}
	gvk := gv.WithKind(paramKind.Kind)

	mapping, err := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		// 未知的 paramKind（CRD 未注册或已删除）报告为 NotFound，让调用方走
		// ParameterNotFoundAction 处理，
		// 而不是作为内部错误拒绝准入请求。
		if meta.IsNoMatchError(err) {
			return nil, apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, paramRef.Name)
		}
		return nil, fmt.Errorf("failed to find REST mapping for %v: %w", gvk, err)
	}

	// 确定 Get 请求的命名空间。
	var targetNamespace string
	if paramScope.Name() == meta.RESTScopeNameNamespace {
		if len(paramRef.Namespace) > 0 {
			targetNamespace = paramRef.Namespace
		} else {
			targetNamespace = namespace
		}
	}

	// 带超时执行直接的 API 调用。
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

// convertParamToInformerRepresentation 把 GET 响应对象转换成 informer 对象的形态：
// 以 unstructured 表示、不带 TypeMeta。这样无论对象来自 informer 还是缓存未命中后
// 的 GET，CEL 表达式收到的 param 对象都保持一致。
//
// 关键一致性要求：同一 param 无论来自缓存还是直接 GET，CEL 看到的对象形态必须相同，
// 否则同一表达式会在两条路径上给出不同结果。
func convertParamToInformerRepresentation(gvk schema.GroupVersionKind, param *unstructured.Unstructured) (runtime.Object, error) {
	typed, err := clientgoscheme.Scheme.New(gvk)
	if err != nil {
		// 该 kind 没有类型化表示，其 informer 同样只提供 unstructured 对象。
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
