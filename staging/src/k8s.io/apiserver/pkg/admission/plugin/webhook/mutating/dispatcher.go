/*
Copyright 2018 The Kubernetes Authors.

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

// Package mutating 把准入检查委托给动态配置的变更型 webhook：负责调用远端
// webhook、应用 JSONPatch、记录审计注解，并在对象被修改后触发必要的
// 重新调用（reinvocation）。
package mutating

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"

	admissionv1 "k8s.io/api/admission/v1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/admission"
	admissionmetrics "k8s.io/apiserver/pkg/admission/metrics"
	"k8s.io/apiserver/pkg/admission/plugin/webhook"
	webhookerrors "k8s.io/apiserver/pkg/admission/plugin/webhook/errors"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/generic"
	webhookrequest "k8s.io/apiserver/pkg/admission/plugin/webhook/request"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	endpointsrequest "k8s.io/apiserver/pkg/endpoints/request"
	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
)

const (

	// PatchAuditAnnotationPrefix 是审计注解中保存 webhook 补丁的键前缀。
	// 补丁可能包含敏感内容，因此这类注解只在审计级别不低于 Request 时才写出。
	PatchAuditAnnotationPrefix = "patch.webhook.admission.k8s.io/"
	// MutationAuditAnnotationPrefix 标记某次 webhook 调用是否真的改动了对象，
	// 只记录布尔结果，不含对象内容，因此比补丁注解更安全。
	MutationAuditAnnotationPrefix = "mutation.webhook.admission.k8s.io/"
	// 带此前缀的注解表示 webhook 在连接失败或后端 5xx 时按 failurePolicy=Ignore
	// 放行（fail open）。留下注解是为了让安全审计能发现"本应检查却未检查"。
	MutationAuditAnnotationFailedOpenKeyPrefix string = "failed-open." + MutationAuditAnnotationPrefix
)

// mutatingDispatcher 持有共享的 REST 客户端管理器与插件引用，本身无可变
// 状态，因此可被所有请求并发复用。ClientManager 负责证书轮换与连接复用。
type mutatingDispatcher struct {
	cm     *webhookutil.ClientManager
	plugin *Plugin
}

// newMutatingDispatcher 返回一个工厂：通用 webhook 框架在初始化时传入
// ClientManager，延迟到此刻才构造 dispatcher，保证客户端管理器已完成配置。
func newMutatingDispatcher(p *Plugin) func(cm *webhookutil.ClientManager) generic.Dispatcher {
	return func(cm *webhookutil.ClientManager) generic.Dispatcher {
		return &mutatingDispatcher{cm, p}
	}
}

// 编译期断言：versionedAttributeAccessor 必须实现通用层的版本化属性访问器。
var _ generic.VersionedAttributeAccessor = &versionedAttributeAccessor{}

// versionedAttributeAccessor 惰性地在内部版本与 webhook 要求的版本之间
// 转换属性。多个 webhook 可能要求不同版本；这里缓存最近一次的转换结果，
// 只有版本变化时才重新转换，避免重复编码。
type versionedAttributeAccessor struct {
	versionedAttr    *admission.VersionedAttributes
	attr             admission.Attributes
	objectInterfaces admission.ObjectInterfaces
}

// VersionedAttribute 返回按 gvk 转换后的属性。首次调用创建，后续调用在
// 版本不同时做增量转换；转换失败包装为 InternalError，因为对请求处理
// 链路而言这属于服务端配置/编解码问题，而非用户输入错误。
func (v *versionedAttributeAccessor) VersionedAttribute(gvk schema.GroupVersionKind) (*admission.VersionedAttributes, error) {
	if v.versionedAttr == nil {
		// 首次调用，创建版本化属性
		var err error
		if v.versionedAttr, err = admission.NewVersionedAttributes(v.attr, gvk, v.objectInterfaces); err != nil {
			return nil, apierrors.NewInternalError(err)
		}
	} else {
		// 后续调用，把已有的版本化属性转换为目标版本
		if err := admission.ConvertVersionedAttributes(v.versionedAttr, gvk, v.objectInterfaces); err != nil {
			return nil, apierrors.NewInternalError(err)
		}
	}
	return v.versionedAttr, nil
}

var _ generic.Dispatcher = &mutatingDispatcher{}

// Dispatch 依次调用所有匹配的变更型 webhook，并把最终的变更结果写回
// attr。变更型 hook 的语义是 fail open/closed 取决于 failurePolicy；
// 任何未忽略的调用错误都会导致请求被拒绝。
func (a *mutatingDispatcher) Dispatch(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces, hooks []webhook.WebhookAccessor) error {
	// reinvocationContext 在整条准入链内共享，用于判断 in-tree 插件是否
	// 改动了对象，从而决定是否需要重新调用 webhook。
	reinvokeCtx := attr.GetReinvocationContext()
	var webhookReinvokeCtx *webhookReinvokeContext
	if v := reinvokeCtx.Value(PluginName); v != nil {
		webhookReinvokeCtx = v.(*webhookReinvokeContext)
	} else {
		// 第一次进入本插件，初始化并存入共享上下文。
		webhookReinvokeCtx = &webhookReinvokeContext{}
		reinvokeCtx.SetValue(PluginName, webhookReinvokeCtx)
	}

	if reinvokeCtx.IsReinvoke() && webhookReinvokeCtx.IsOutputChangedSinceLastWebhookInvocation(attr.GetObject()) {
		// 对象在两次调用之间被修改，说明此前的 mutation 可能基于过期输入，
		// 于是要求重新调用所有声明了 IfNeeded 的 webhook。
		webhookReinvokeCtx.RequireReinvokingPreviouslyInvokedPlugins()
	}
	// 记录本轮结束时的对象快照，供下一轮判断是否发生变化。
	defer func() {
		webhookReinvokeCtx.SetLastWebhookInvocationOutput(attr.GetObject())
	}()
	v := &versionedAttributeAccessor{
		attr:             attr,
		objectInterfaces: o,
	}
	for i, hook := range hooks {
		// 若之前的 hook 已产生版本化对象，则用其做匹配检查，确保匹配
		// 看到的是最新变更后的内容。
		attrForCheck := attr
		if v.versionedAttr != nil {
			attrForCheck = v.versionedAttr
		}

		invocation, statusErr := a.plugin.ShouldCallHook(ctx, hook, attrForCheck, o, v)
		if statusErr != nil {
			return statusErr
		}
		// nil 表示规则、对象选择器或命名空间选择器不匹配，跳过该 hook。
		if invocation == nil {
			continue
		}

		hook, ok := invocation.Webhook.GetMutatingWebhook()
		if !ok {
			return fmt.Errorf("mutating webhook dispatch requires v1.MutatingWebhook, but got %T", hook)
		}

		// 重新调用阶段只重放此前确已调用过、且声明 IfNeeded 的 webhook；
		// 第一轮被跳过的 hook 不会在重放时"补跑"，否则会改变既定语义。
		if reinvokeCtx.IsReinvoke() && !webhookReinvokeCtx.ShouldReinvokeWebhook(invocation.Webhook.GetUID()) {
			continue
		}

		versionedAttr, err := v.VersionedAttribute(invocation.Kind)
		if err != nil {
			return apierrors.NewInternalError(err)
		}

		t := time.Now()
		// round 用于审计注解键，区分首次调用（0）与重新调用（1），
		// 便于排查同一 webhook 的多轮变更。
		round := 0
		if reinvokeCtx.IsReinvoke() {
			round = 1
		}

		annotator := newWebhookAnnotator(versionedAttr, round, i, hook.Name, invocation.Webhook.GetConfigurationName())
		changed, err := a.callAttrMutatingHook(ctx, hook, invocation, versionedAttr, annotator, o, round, i)
		ignoreClientCallFailures := hook.FailurePolicy != nil && *hook.FailurePolicy == admissionregistrationv1.Ignore
		rejected := false
		if err != nil {
			// 按错误类型分别记指标：调用失败、webhook 主动拒绝、服务端内部错误
			// 在排障时含义完全不同，不能混为一谈。
			switch err := err.(type) {
			case *webhookutil.ErrCallingWebhook:
				if !ignoreClientCallFailures {
					rejected = true
					// 上下文已取消时，不计入 webhook 拒绝指标
					if !errors.Is(err.Reason, context.Canceled) {
						admissionmetrics.Metrics.ObserveWebhookRejection(ctx, hook.Name, "admit", string(versionedAttr.Attributes.GetOperation()), admissionmetrics.WebhookRejectionCallingWebhookError, int(err.Status.ErrStatus.Code))
					}
				}
				admissionmetrics.Metrics.ObserveWebhook(ctx, hook.Name, time.Since(t), rejected, versionedAttr.Attributes, "admit", int(err.Status.ErrStatus.Code))
			case *webhookutil.ErrWebhookRejection:
				rejected = true
				admissionmetrics.Metrics.ObserveWebhookRejection(ctx, hook.Name, "admit", string(versionedAttr.Attributes.GetOperation()), admissionmetrics.WebhookRejectionNoError, int(err.Status.ErrStatus.Code))
				admissionmetrics.Metrics.ObserveWebhook(ctx, hook.Name, time.Since(t), rejected, versionedAttr.Attributes, "admit", int(err.Status.ErrStatus.Code))
			default:
				rejected = true
				admissionmetrics.Metrics.ObserveWebhookRejection(ctx, hook.Name, "admit", string(versionedAttr.Attributes.GetOperation()), admissionmetrics.WebhookRejectionAPIServerInternalError, 0)
				admissionmetrics.Metrics.ObserveWebhook(ctx, hook.Name, time.Since(t), rejected, versionedAttr.Attributes, "admit", 0)
			}
		} else {
			admissionmetrics.Metrics.ObserveWebhook(ctx, hook.Name, time.Since(t), rejected, versionedAttr.Attributes, "admit", 200)
		}
		if changed {
			// 对象已改变，后续 webhook 看到的输入也变了，因此标记需要
			// 重放此前已调用过的 hook。
			webhookReinvokeCtx.RequireReinvokingPreviouslyInvokedPlugins()
			reinvokeCtx.SetShouldReinvoke()
		}
		// 只有显式声明 IfNeededReinvocationPolicy 的 hook 才会被加入重放名单。
		if hook.ReinvocationPolicy != nil && *hook.ReinvocationPolicy == admissionregistrationv1.IfNeededReinvocationPolicy {
			webhookReinvokeCtx.AddReinvocableWebhookToPreviouslyInvoked(invocation.Webhook.GetUID())
		}
		if err == nil {
			continue
		}

		if callErr, ok := err.(*webhookutil.ErrCallingWebhook); ok {
			if ignoreClientCallFailures {
				// 上下文已取消时，不计入 webhook 拒绝指标
				if errors.Is(callErr.Reason, context.Canceled) {
					klog.Warningf("Context canceled when calling webhook %v", hook.Name)
				} else {
					// failurePolicy=Ignore 时按 fail open 处理，同时留下审计
					// 注解，便于事后发现未执行的检查。
					klog.Warningf("Failed calling webhook, failing open %v: %v", hook.Name, callErr)
					admissionmetrics.Metrics.ObserveWebhookFailOpen(ctx, hook.Name, "admit")
					annotator.addFailedOpenAnnotation()
				}
				utilruntime.HandleError(callErr)

				select {
				case <-ctx.Done():
					// 整个请求的上下文已结束，继续调用其余 webhook 没有意义。
					return apierrors.NewTimeoutError("request did not complete within requested timeout", 0)
				default:
					// 仅该 webhook 超时，请求整体仍可继续。
					continue
				}
			}
			// failurePolicy=Fail（默认）时按 fail closed 处理。
			klog.Warningf("Failed calling webhook, failing closed %v: %v", hook.Name, err)
			return apierrors.NewInternalError(err)
		}
		if rejectionErr, ok := err.(*webhookutil.ErrWebhookRejection); ok {
			return rejectionErr.Status
		}
		return err
	}

	// 所有 webhook 处理完后，把版本化对象转换回内部版本，后续插件
	// 才能看到变更后的对象。Dirty 表示确实发生过改动，避免无谓转换。
	if v.versionedAttr != nil && v.versionedAttr.VersionedObject.Object() != nil && v.versionedAttr.Dirty {
		return o.GetObjectConvertor().Convert(v.versionedAttr.VersionedObject.Object(), v.versionedAttr.Attributes.GetObject(), nil)
	}

	return nil
}

// 注意：callAttrMutatingHook 会更新 attr

// callAttrMutatingHook 调用单个变更型 webhook，解析补丁并应用到 attr。
// 返回值 changed 表示对象是否真的发生变化（用于决定是否重放），
// annotator 会在退出时（defer）记录 mutation 审计注解，即使中途出错。
func (a *mutatingDispatcher) callAttrMutatingHook(ctx context.Context, h *admissionregistrationv1.MutatingWebhook, invocation *generic.WebhookInvocation, attr *admission.VersionedAttributes, annotator *webhookAnnotator, o admission.ObjectInterfaces, round, idx int) (bool, error) {
	configurationName := invocation.Webhook.GetConfigurationName()
	changed := false
	// 无论成功失败都记录 mutation 注解，审计才能区分"未改动"与"未调用"。
	defer func() { annotator.addMutationAnnotation(changed) }()
	// dry-run 请求不得产生副作用；SideEffects 未声明时无法保证这一点，
	// 因此直接拒绝，而不是猜测。
	if attr.Attributes.IsDryRun() {
		if h.SideEffects == nil {
			return false, &webhookutil.ErrCallingWebhook{WebhookName: h.Name, Reason: fmt.Errorf("Webhook SideEffects is nil"), Status: apierrors.NewBadRequest("Webhook SideEffects is nil")}
		}
		if !(*h.SideEffects == admissionregistrationv1.SideEffectClassNone || *h.SideEffects == admissionregistrationv1.SideEffectClassNoneOnDryRun) {
			return false, webhookerrors.NewDryRunUnsupportedErr(h.Name)
		}
	}

	uid, request, response, err := webhookrequest.CreateAdmissionObjects(attr, invocation)
	if err != nil {
		return false, &webhookutil.ErrCallingWebhook{WebhookName: h.Name, Reason: fmt.Errorf("could not create admission objects: %w", err), Status: apierrors.NewBadRequest("error creating admission objects")}
	}
	// 构造 webhook 请求
	client, err := invocation.Webhook.GetRESTClient(a.cm)
	if err != nil {
		return false, &webhookutil.ErrCallingWebhook{WebhookName: h.Name, Reason: fmt.Errorf("could not get REST client: %w", err), Status: apierrors.NewInternalError(err)}
	}
	// 追踪 span 携带配置名、webhook 名与请求 UID，超时结束时间限制为
	// 500ms，避免卡住的 span 长时间占用追踪后端。
	ctx, span := tracing.Start(ctx, "Call mutating webhook",
		attribute.String("configuration", configurationName),
		attribute.String("webhook", h.Name),
		attribute.Stringer("resource", attr.GetResource()),
		attribute.String("subresource", attr.GetSubresource()),
		attribute.String("operation", string(attr.GetOperation())),
		attribute.String("UID", string(uid)))
	defer span.End(500 * time.Millisecond)

	// 若 webhook 配置了专属超时，包装 context 使其生效
	if h.TimeoutSeconds != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*h.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	r := client.Post().Body(request)

	// 把剩余超时传给后端（向上取整到秒），让 webhook 侧也能提前放弃，
	// 而不是在服务端已经超时后仍继续处理。
	if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
		// 计算超时
		if timeout := time.Until(deadline); timeout > 0 {
			// 若不是整秒，向上取整到最近的整秒
			if truncated := timeout.Truncate(time.Second); truncated != timeout {
				timeout = truncated + time.Second
			}
			// 设置超时
			r.Timeout(timeout)
		}
	}

	do := func() { err = r.Do(ctx).Into(response) }
	// 若启用了延迟追踪，则把这次调用计入 MutatingWebhookTracker，
	// 用于统计 webhook 呼叫在请求延迟中的占比。
	if wd, ok := endpointsrequest.LatencyTrackersFrom(ctx); ok {
		tmp := do
		do = func() { wd.MutatingWebhookTracker.Track(tmp) }
	}
	do()
	if err != nil {
		var status *apierrors.StatusError
		if se, ok := err.(*apierrors.StatusError); ok {
			status = se
		} else {
			// 非 Status 错误（网络、编解码等）对用户统一视为 503，
			// 避免泄露后端细节。
			status = apierrors.NewServiceUnavailable("error calling webhook")
		}
		return false, &webhookutil.ErrCallingWebhook{WebhookName: h.Name, Reason: fmt.Errorf("failed to call webhook: %w", err), Status: status}
	}
	span.AddEvent("Request completed")

	result, err := webhookrequest.VerifyAdmissionResponse(uid, true, response)
	if err != nil {
		return false, &webhookutil.ErrCallingWebhook{WebhookName: h.Name, Reason: fmt.Errorf("received invalid webhook response: %w", err), Status: apierrors.NewServiceUnavailable("error validating webhook response")}
	}

	// webhook 返回的审计注解统一加上 webhook 名前缀，防止不同 webhook
	// 之间互相覆盖键。
	for k, v := range result.AuditAnnotations {
		key := h.Name + "/" + k
		if err := attr.Attributes.AddAnnotation(key, v); err != nil {
			klog.Warningf("Failed to set admission audit annotation %s to %s for mutating webhook %s: %v", key, v, h.Name, err)
		}
	}
	// 警告写入请求上下文，最终随响应头返回给客户端。
	for _, w := range result.Warnings {
		warning.AddWarning(ctx, "", w)
	}

	if !result.Allowed {
		return false, &webhookutil.ErrWebhookRejection{Status: webhookerrors.ToStatusErr(h.Name, result.Result)}
	}

	// 空补丁视为无变更，直接返回，省去后续编解码开销。
	if len(result.Patch) == 0 {
		return false, nil
	}
	patchObj, err := jsonpatch.DecodePatch(result.Patch)
	if err != nil {
		return false, &webhookutil.ErrCallingWebhook{WebhookName: h.Name, Reason: fmt.Errorf("received undecodable patch in webhook response: %w", err), Status: apierrors.NewServiceUnavailable("error decoding patch in webhook response")}
	}

	if len(patchObj) == 0 {
		return false, nil
	}

	// DELETE 等操作没有可接受补丁的对象，必须报错而不是静默丢弃补丁。
	if attr.VersionedObject.Object() == nil {
		return false, apierrors.NewInternalError(fmt.Errorf("admission webhook %q attempted to modify the object, which is not supported for this operation", h.Name))
	}

	var patchedJS []byte
	jsonSerializer := json.NewSerializerWithOptions(json.DefaultMetaFactory, o.GetObjectCreater(), o.GetObjectTyper(), json.SerializerOptions{})
	switch result.PatchType {
	// 响应中的 PatchType 已被规范化为 v1 类型，因此这里只处理 JSONPatch。
	case admissionv1.PatchTypeJSONPatch:
		objJS, err := runtime.Encode(jsonSerializer, attr.VersionedObject.Object())
		if err != nil {
			return false, apierrors.NewInternalError(err)
		}
		patchedJS, err = patchObj.Apply(objJS)
		if err != nil {
			return false, apierrors.NewInternalError(err)
		}
	default:
		return false, &webhookutil.ErrCallingWebhook{WebhookName: h.Name, Reason: fmt.Errorf("unsupported patch type %q", result.PatchType), Status: webhookerrors.ToStatusErr(h.Name, result.Result)}
	}

	var newVersionedObject runtime.Object
	if _, ok := attr.VersionedObject.Object().(*unstructured.Unstructured); ok {
		// CRD 没有静态 Go 类型，只能用 Unstructured 承载补丁结果。
		newVersionedObject = &unstructured.Unstructured{}
	} else {
		newVersionedObject, err = o.GetObjectCreater().New(attr.VersionedKind)
		if err != nil {
			return false, apierrors.NewInternalError(err)
		}
	}

	// TODO: 如果有多个变更型 webhook，可以记住 JSON，
	// 而不是每个 webhook 都做一次编解码。
	if newVersionedObject, _, err = jsonSerializer.Decode(patchedJS, nil, newVersionedObject); err != nil {
		return false, apierrors.NewInternalError(err)
	}

	// 语义比较会忽略无意义差异（如 nil 与空集合），避免把"格式等价"
	// 误判为实际变更从而触发不必要的重放。
	changed = !apiequality.Semantic.DeepEqual(attr.VersionedObject.Object(), newVersionedObject)
	span.AddEvent("Patch applied")
	annotator.addPatchAnnotation(patchObj, result.PatchType)
	attr.UpdateObject(newVersionedObject)
	// 补丁可能删除了必需字段的默认值，重新执行默认化以恢复 API 不变量。
	o.GetObjectDefaulter().Default(attr.VersionedObject.Object())
	return changed, nil
}

// webhookAnnotator 负责把本次 webhook 调用的结果（是否变更、原始补丁、
// 是否 fail open）写入请求的审计注解。注解键在构造时一次性生成，包含
// 轮次与 hook 序号，保证同一请求中多次调用不会互相覆盖。
type webhookAnnotator struct {
	attr                    *admission.VersionedAttributes
	failedOpenAnnotationKey string
	patchAnnotationKey      string
	mutationAnnotationKey   string
	webhook                 string
	configuration           string
}

// newWebhookAnnotator 预计算三个注解键。round/idx 编码进键名，使审计日志
// 能还原调用顺序以及是首次调用还是重放。
func newWebhookAnnotator(attr *admission.VersionedAttributes, round, idx int, webhook, configuration string) *webhookAnnotator {
	return &webhookAnnotator{
		attr:                    attr,
		failedOpenAnnotationKey: fmt.Sprintf("%sround_%d_index_%d", MutationAuditAnnotationFailedOpenKeyPrefix, round, idx),
		patchAnnotationKey:      fmt.Sprintf("%sround_%d_index_%d", PatchAuditAnnotationPrefix, round, idx),
		mutationAnnotationKey:   fmt.Sprintf("%sround_%d_index_%d", MutationAuditAnnotationPrefix, round, idx),
		webhook:                 webhook,
		configuration:           configuration,
	}
}

// addFailedOpenAnnotation 记录 fail open 事实。写注解失败只告警不中断，
// 因为它属于尽力而为的审计增强，不能反过来导致请求失败。
func (w *webhookAnnotator) addFailedOpenAnnotation() {
	if w.attr == nil || w.attr.Attributes == nil {
		return
	}
	value := w.webhook
	if err := w.attr.Attributes.AddAnnotation(w.failedOpenAnnotationKey, value); err != nil {
		klog.Warningf("failed to set failed open annotation for mutating webhook key %s to %s: %v", w.failedOpenAnnotationKey, value, err)
	}
}

// addMutationAnnotation 记录本次调用是否改变了对象。序列化失败同样只告警，
// 避免审计辅助逻辑影响准入主路径。
func (w *webhookAnnotator) addMutationAnnotation(mutated bool) {
	if w.attr == nil || w.attr.Attributes == nil {
		return
	}
	value, err := mutationAnnotationValue(w.configuration, w.webhook, mutated)
	if err != nil {
		klog.Warningf("unexpected error composing mutating webhook annotation: %v", err)
		return
	}
	if err := w.attr.Attributes.AddAnnotation(w.mutationAnnotationKey, value); err != nil {
		klog.Warningf("failed to set mutation annotation for mutating webhook key %s to %s: %v", w.mutationAnnotationKey, value, err)
	}
}

// addPatchAnnotation 记录 webhook 返回的原始补丁。补丁按 Request 级别写入，
// 因为其可能包含敏感字段内容；不支持的补丁类型只告警，不阻断请求。
func (w *webhookAnnotator) addPatchAnnotation(patch interface{}, patchType admissionv1.PatchType) {
	if w.attr == nil || w.attr.Attributes == nil {
		return
	}
	var value string
	var err error
	switch patchType {
	case admissionv1.PatchTypeJSONPatch:
		value, err = jsonPatchAnnotationValue(w.configuration, w.webhook, patch)
		if err != nil {
			klog.Warningf("unexpected error composing mutating webhook JSON patch annotation: %v", err)
			return
		}
	default:
		klog.Warningf("unsupported patch type for mutating webhook annotation: %v", patchType)
		return
	}
	if err := w.attr.Attributes.AddAnnotationWithLevel(w.patchAnnotationKey, value, auditinternal.LevelRequest); err != nil {
		// NOTE: 不在 kube-apiserver 日志中记录实际补丁内容，以避免潜在的信息泄露。
		klog.Warningf("failed to set patch annotation for mutating webhook key %s; confugiration name: %s, webhook name: %s", w.patchAnnotationKey, w.configuration, w.webhook)
	}
}

// MutationAuditAnnotation 是 mutation 注解的 JSON 载荷。字段标签必须保持
// 稳定，因为审计日志消费者依赖这些键名。
type MutationAuditAnnotation struct {
	Configuration string `json:"configuration"`
	Webhook       string `json:"webhook"`
	Mutated       bool   `json:"mutated"`
}

// PatchAuditAnnotation 是 patch 注解的 JSON 载荷，Patch 使用 interface{}
// 以便按 jsonpatch.DecodePatch 的结果原样记录。
type PatchAuditAnnotation struct {
	Configuration string      `json:"configuration"`
	Webhook       string      `json:"webhook"`
	Patch         interface{} `json:"patch,omitempty"`
	PatchType     string      `json:"patchType,omitempty"`
}

// mutationAnnotationValue 把 mutation 信息序列化为注解字符串。
func mutationAnnotationValue(configuration, webhook string, mutated bool) (string, error) {
	m := MutationAuditAnnotation{
		Configuration: configuration,
		Webhook:       webhook,
		Mutated:       mutated,
	}
	bytes, err := utiljson.Marshal(m)
	return string(bytes), err
}

// jsonPatchAnnotationValue 把补丁及其类型序列化为注解字符串。
func jsonPatchAnnotationValue(configuration, webhook string, patch interface{}) (string, error) {
	p := PatchAuditAnnotation{
		Configuration: configuration,
		Webhook:       webhook,
		Patch:         patch,
		PatchType:     string(admissionv1.PatchTypeJSONPatch),
	}
	bytes, err := utiljson.Marshal(p)
	return string(bytes), err
}
