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

package validating

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"

	v1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/admission"
	admissionmetrics "k8s.io/apiserver/pkg/admission/metrics"
	"k8s.io/apiserver/pkg/admission/plugin/webhook"
	webhookerrors "k8s.io/apiserver/pkg/admission/plugin/webhook/errors"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/generic"
	webhookrequest "k8s.io/apiserver/pkg/admission/plugin/webhook/request"
	endpointsrequest "k8s.io/apiserver/pkg/endpoints/request"
	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
)

const (
	// ValidatingAuditAnnotationPrefix is a prefix for keeping noteworthy
	// validating audit annotations.
	ValidatingAuditAnnotationPrefix = "validating.webhook.admission.k8s.io/"
	// ValidatingAuditAnnotationFailedOpenKeyPrefix in an annotation indicates
	// the validating webhook failed open when the webhook backend connection
	// failed or returned an internal server error.
	//
	// fail-open 的结论不是“无意见”，而是“有风险地放行”，因此必须在审计
	// 注解中留痕，供事后追查。
	ValidatingAuditAnnotationFailedOpenKeyPrefix = "failed-open." + ValidatingAuditAnnotationPrefix
)

// validatingDispatcher 负责把一次准入请求并发地分发给所有匹配的
// validating webhook，并汇总拒绝原因。
type validatingDispatcher struct {
	cm     *webhookutil.ClientManager
	plugin *Plugin
}

// newValidatingDispatcher 返回一个工厂：ClientManager 在插件初始化时才可用，
// 因此先捕获插件、待注入 client manager 后再构造 dispatcher。
func newValidatingDispatcher(p *Plugin) func(cm *webhookutil.ClientManager) generic.Dispatcher {
	return func(cm *webhookutil.ClientManager) generic.Dispatcher {
		return &validatingDispatcher{cm, p}
	}
}

var _ generic.VersionedAttributeAccessor = &versionedAttributeAccessor{}

// versionedAttributeAccessor 按 GVK 缓存版本化属性，避免多个 webhook
// 请求同一版本时重复做转换。缓存生命周期仅限于一次 Dispatch。
type versionedAttributeAccessor struct {
	versionedAttrs   map[schema.GroupVersionKind]*admission.VersionedAttributes
	attr             admission.Attributes
	objectInterfaces admission.ObjectInterfaces
}

// VersionedAttribute 返回（必要时构造并缓存）指定 GVK 的属性视图。
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

var _ generic.Dispatcher = &validatingDispatcher{}

// Dispatch 实现 generic.Dispatcher。
// 语义：只要任一 webhook 拒绝或调用失败（fail-closed），整个请求即被拒绝；
// 所有 webhook 并发调用，以请求总延迟而非累加延迟为准。
func (d *validatingDispatcher) Dispatch(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces, hooks []webhook.WebhookAccessor) error {
	var relevantHooks []*generic.WebhookInvocation
	// Construct all the versions we need to call our webhooks
	versionedAttrAccessor := &versionedAttributeAccessor{
		versionedAttrs:   map[schema.GroupVersionKind]*admission.VersionedAttributes{},
		attr:             attr,
		objectInterfaces: o,
	}
	for _, hook := range hooks {
		invocation, statusError := d.plugin.ShouldCallHook(ctx, hook, attr, o, versionedAttrAccessor)
		if statusError != nil {
			// 匹配阶段的错误直接拒绝，不进入并发调用。
			return statusError
		}
		if invocation == nil {
			continue
		}

		relevantHooks = append(relevantHooks, invocation)
		// VersionedAttr result will be cached and reused later during parallel webhook calls
		// 提前构造并缓存，把转换错误暴露在并发之前，且避免并发读写 map。
		_, err := versionedAttrAccessor.VersionedAttribute(invocation.Kind)
		if err != nil {
			return apierrors.NewInternalError(err)
		}
	}

	if len(relevantHooks) == 0 {
		// no matching hooks
		return nil
	}

	// Check if the request has already timed out before spawning remote calls
	// 先检查 ctx，避免为已超时的请求无谓地发起远程调用。
	select {
	case <-ctx.Done():
		// parent context is canceled or timed out, no point in continuing
		return apierrors.NewTimeoutError("request did not complete within requested timeout", 0)
	default:
	}

	wg := sync.WaitGroup{}
	errCh := make(chan error, 2*len(relevantHooks)) // double the length to handle extra errors for panics in the gofunc
	wg.Add(len(relevantHooks))
	for i := range relevantHooks {
		go func(invocation *generic.WebhookInvocation, idx int) {
			ignoreClientCallFailures := false
			hookName := "unknown"
			versionedAttr := versionedAttrAccessor.versionedAttrs[invocation.Kind]
			// The ordering of these two defers is critical. The wg.Done will release the parent go func to close the errCh
			// that is used by the second defer to report errors. The recovery and error reporting must be done first.
			defer wg.Done()
			defer func() {
				// HandleCrash has already called the crash handlers and it has been configured to utilruntime.ReallyCrash
				// This block prevents the second panic from failing our process.
				// This failure mode for the handler functions properly using the channel below.
				recover()
			}()
			defer utilruntime.HandleCrash(
				func(r interface{}) {
					if r == nil {
						return
					}
					if ignoreClientCallFailures {
						// if failures are supposed to ignored, ignore it
						// 配置为 Ignore 时 panic 属 fail-open：记录、计指标、写审计注解。
						klog.Warningf("Panic calling webhook, failing open %v: %v", hookName, r)
						admissionmetrics.Metrics.ObserveWebhookFailOpen(ctx, hookName, "validating")
						key := fmt.Sprintf("%sround_0_index_%d", ValidatingAuditAnnotationFailedOpenKeyPrefix, idx)
						value := hookName
						if err := versionedAttr.Attributes.AddAnnotation(key, value); err != nil {
							klog.Warningf("Failed to set admission audit annotation %s to %s for validating webhook %s: %v", key, value, hookName, err)
						}
						return
					}
					// this ensures that the admission request fails and a message is provided.
					errCh <- apierrors.NewInternalError(fmt.Errorf("ValidatingAdmissionWebhook/%v has panicked: %v", hookName, r))
				},
			)

			hook, ok := invocation.Webhook.GetValidatingWebhook()
			if !ok {
				utilruntime.HandleError(fmt.Errorf("validating webhook dispatch requires v1.ValidatingWebhook, but got %T", hook))
				return
			}
			hookName = hook.Name
			ignoreClientCallFailures = hook.FailurePolicy != nil && *hook.FailurePolicy == v1.Ignore
			t := time.Now()
			err := d.callHook(ctx, hook, invocation, versionedAttr)
			rejected := false
			if err != nil {
				switch err := err.(type) {
				case *webhookutil.ErrCallingWebhook:
					if !ignoreClientCallFailures {
						rejected = true
						// Ignore context cancelled from webhook metrics
						if !errors.Is(err.Reason, context.Canceled) {
							admissionmetrics.Metrics.ObserveWebhookRejection(ctx, hook.Name, "validating", string(versionedAttr.Attributes.GetOperation()), admissionmetrics.WebhookRejectionCallingWebhookError, int(err.Status.ErrStatus.Code))
						}
					}
					admissionmetrics.Metrics.ObserveWebhook(ctx, hook.Name, time.Since(t), rejected, versionedAttr.Attributes, "validating", int(err.Status.ErrStatus.Code))
				case *webhookutil.ErrWebhookRejection:
					rejected = true
					admissionmetrics.Metrics.ObserveWebhookRejection(ctx, hook.Name, "validating", string(versionedAttr.Attributes.GetOperation()), admissionmetrics.WebhookRejectionNoError, int(err.Status.ErrStatus.Code))
					admissionmetrics.Metrics.ObserveWebhook(ctx, hook.Name, time.Since(t), rejected, versionedAttr.Attributes, "validating", int(err.Status.ErrStatus.Code))
				default:
					rejected = true
					admissionmetrics.Metrics.ObserveWebhookRejection(ctx, hook.Name, "validating", string(versionedAttr.Attributes.GetOperation()), admissionmetrics.WebhookRejectionAPIServerInternalError, 0)
					admissionmetrics.Metrics.ObserveWebhook(ctx, hook.Name, time.Since(t), rejected, versionedAttr.Attributes, "validating", 0)
				}
			} else {
				admissionmetrics.Metrics.ObserveWebhook(ctx, hook.Name, time.Since(t), rejected, versionedAttr.Attributes, "validating", 200)
				return
			}

			if callErr, ok := err.(*webhookutil.ErrCallingWebhook); ok {
				if ignoreClientCallFailures {
					// Ignore context cancelled from webhook metrics
					if errors.Is(callErr.Reason, context.Canceled) {
						klog.Warningf("Context canceled when calling webhook %v", hook.Name)
					} else {
						// fail-open：调用失败但策略允许忽略，需留下审计痕迹。
						klog.Warningf("Failed calling webhook, failing open %v: %v", hook.Name, callErr)
						admissionmetrics.Metrics.ObserveWebhookFailOpen(ctx, hook.Name, "validating")
						key := fmt.Sprintf("%sround_0_index_%d", ValidatingAuditAnnotationFailedOpenKeyPrefix, idx)
						value := hook.Name
						if err := versionedAttr.Attributes.AddAnnotation(key, value); err != nil {
							klog.Warningf("Failed to set admission audit annotation %s to %s for validating webhook %s: %v", key, value, hook.Name, err)
						}
					}
					utilruntime.HandleError(callErr)
					return
				}

				// 默认 Fail 策略：调用失败即拒绝，防止 webhook 不可用时绕过校验。
				klog.Warningf("Failed calling webhook, failing closed %v: %v", hook.Name, err)
				errCh <- apierrors.NewInternalError(err)
				return
			}

			// 其余情况归为 webhook 明确拒绝：把 Status 原样上抛。
			if rejectionErr, ok := err.(*webhookutil.ErrWebhookRejection); ok {
				err = rejectionErr.Status
			}
			klog.Warningf("rejected by webhook %q: %#v", hook.Name, err)
			errCh <- err
		}(relevantHooks[i], i)
	}
	wg.Wait()
	close(errCh)

	var errs []error
	for e := range errCh {
		errs = append(errs, e)
	}
	if len(errs) == 0 {
		return nil
	}
	if len(errs) > 1 {
		// 多个拒绝时只返回第一个，其余记录以便诊断；合并 Status 语义复杂，
		// 且对客户端而言第一个错误已足以说明请求被拒。
		for i := 1; i < len(errs); i++ {
			// TODO: merge status errors; until then, just return the first one.
			utilruntime.HandleError(errs[i])
		}
	}
	return errs[0]
}

// callHook 构造并发送单个 AdmissionReview 请求，返回 nil 表示放行。
func (d *validatingDispatcher) callHook(ctx context.Context, h *v1.ValidatingWebhook, invocation *generic.WebhookInvocation, attr *admission.VersionedAttributes) error {
	// dry-run 语义上不应产生副作用：webhook 未声明 sideEffects 或声明了
	// 有副作用时，必须拒绝而不是悄悄执行。
	if attr.Attributes.IsDryRun() {
		if h.SideEffects == nil {
			return &webhookutil.ErrCallingWebhook{WebhookName: h.Name, Reason: fmt.Errorf("Webhook SideEffects is nil"), Status: apierrors.NewBadRequest("Webhook SideEffects is nil")}
		}
		if !(*h.SideEffects == v1.SideEffectClassNone || *h.SideEffects == v1.SideEffectClassNoneOnDryRun) {
			return webhookerrors.NewDryRunUnsupportedErr(h.Name)
		}
	}

	uid, request, response, err := webhookrequest.CreateAdmissionObjects(attr, invocation)
	if err != nil {
		return &webhookutil.ErrCallingWebhook{WebhookName: h.Name, Reason: fmt.Errorf("could not create admission objects: %w", err), Status: apierrors.NewBadRequest("error creating admission objects")}
	}
	// Make the webhook request
	client, err := invocation.Webhook.GetRESTClient(d.cm)
	if err != nil {
		return &webhookutil.ErrCallingWebhook{WebhookName: h.Name, Reason: fmt.Errorf("could not get REST client: %w", err), Status: apierrors.NewInternalError(err)}
	}
	// span 属性包含配置名、webhook 名与操作类型，但不包含对象内容，
	// 避免把用户数据写入追踪系统。
	ctx, span := tracing.Start(ctx, "Call validating webhook",
		attribute.String("configuration", invocation.Webhook.GetConfigurationName()),
		attribute.String("webhook", h.Name),
		attribute.Stringer("resource", attr.GetResource()),
		attribute.String("subresource", attr.GetSubresource()),
		attribute.String("operation", string(attr.GetOperation())),
		attribute.String("UID", string(uid)))
	defer span.End(500 * time.Millisecond)

	// if the webhook has a specific timeout, wrap the context to apply it
	if h.TimeoutSeconds != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*h.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	r := client.Post().Body(request)

	// if the context has a deadline, set it as a parameter to inform the backend
	// 同时把超时告诉 webhook，便于对方自行控制处理时长。
	if deadline, hasDeadline := ctx.Deadline(); hasDeadline {
		// compute the timeout
		if timeout := time.Until(deadline); timeout > 0 {
			// if it's not an even number of seconds, round up to the nearest second
			if truncated := timeout.Truncate(time.Second); truncated != timeout {
				timeout = truncated + time.Second
			}
			// set the timeout
			r.Timeout(timeout)
		}
	}

	do := func() { err = r.Do(ctx).Into(response) }
	// 若请求带有延迟追踪器，把 webhook 调用耗时计入，便于分析请求各阶段开销。
	if wd, ok := endpointsrequest.LatencyTrackersFrom(ctx); ok {
		tmp := do
		do = func() { wd.ValidatingWebhookTracker.Track(tmp) }
	}
	do()
	if err != nil {
		// 传输层错误统一包装成 503，避免把内部网络细节暴露给客户端。
		var status *apierrors.StatusError
		if se, ok := err.(*apierrors.StatusError); ok {
			status = se
		} else {
			status = apierrors.NewServiceUnavailable("error calling webhook")
		}
		return &webhookutil.ErrCallingWebhook{WebhookName: h.Name, Reason: fmt.Errorf("failed to call webhook: %w", err), Status: status}
	}
	span.AddEvent("Request completed")

	result, err := webhookrequest.VerifyAdmissionResponse(uid, false, response)
	if err != nil {
		return &webhookutil.ErrCallingWebhook{WebhookName: h.Name, Reason: fmt.Errorf("received invalid webhook response: %w", err), Status: apierrors.NewServiceUnavailable("error validating webhook response")}
	}

	// webhook 返回的审计注解加上 webhook 名前缀，避免多个 webhook 互相覆盖。
	for k, v := range result.AuditAnnotations {
		key := h.Name + "/" + k
		if err := attr.Attributes.AddAnnotation(key, v); err != nil {
			klog.Warningf("Failed to set admission audit annotation %s to %s for validating webhook %s: %v", key, v, h.Name, err)
		}
	}
	// warning 透传给客户端，属于非致命提示。
	for _, w := range result.Warnings {
		warning.AddWarning(ctx, "", w)
	}
	if result.Allowed {
		return nil
	}
	return &webhookutil.ErrWebhookRejection{Status: webhookerrors.ToStatusErr(h.Name, result.Result)}
}
