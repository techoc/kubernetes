/*
Copyright 2017 The Kubernetes Authors.

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

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metainternalversionvalidation "k8s.io/apimachinery/pkg/apis/meta/internalversion/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/handlers/finisher"
	requestmetrics "k8s.io/apiserver/pkg/endpoints/handlers/metrics"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/util/apihelpers"
	"k8s.io/apiserver/pkg/util/dryrun"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/tracing"

	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// DeleteResource 返回处理资源删除的函数。
// TODO: 这里的准入最终应只保留校验准入。
//
// 删除流程刻意分成"静态属性准入"与"存储层删除"两段：前半段只依据请求本身
// （命名空间、名称、DeleteOptions）构造 admission 属性，使准入插件在读取对象前就能
// 拒绝明显非法的请求；真正的对象级校验在存储层拿到对象后再执行。
func DeleteResource(r rest.GracefulDeleter, allowsOptions bool, scope *RequestScope, admit admission.Interface) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		// 用于性能追踪。
		ctx, span := tracing.Start(ctx, "Delete", traceFields(req)...)
		req = req.WithContext(ctx)
		defer span.End(500 * time.Millisecond)

		namespace, name, err := scope.Namer.Name(req)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		// 强制超时不超过 requestTimeoutUpperBound（34s）；若父上下文中的用户超时更短则沿用。
		// 上限与客户端超时取较小者：既防止请求无限挂起，也尊重调用方更严格的时限。
		ctx, cancel := context.WithTimeout(ctx, requestTimeoutUpperBound)
		defer cancel()

		ctx = request.WithNamespace(ctx, namespace)
		admit = admission.WithAudit(admit)

		outputMediaType, _, err := negotiation.NegotiateOutputMediaType(req, scope.Serializer, scope)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		// DeleteOptions 既可来自请求体也可来自查询参数；前者优先，后者用于兼容
		options := &metav1.DeleteOptions{}
		if allowsOptions {
			body, err := limitedReadBodyWithRecordMetric(ctx, req, scope.MaxRequestBodyBytes, scope.Resource.GroupResource(), requestmetrics.Delete)
			if err != nil {
				span.AddEvent("limitedReadBody failed", attribute.Int("len", len(body)), attribute.String("err", err.Error()))
				scope.err(err, w, req)
				return
			}
			span.AddEvent("limitedReadBody succeeded", attribute.Int("len", len(body)))
			if len(body) > 0 {
				s, err := negotiation.NegotiateInputSerializer(req, false, apihelpers.GetMetaInternalVersionCodecs())
				if err != nil {
					scope.err(err, w, req)
					return
				}
				// 为向后兼容，需要允许既有客户端提交按 group 定义的 DeleteOptions，
				// 同时也允许提交 meta.k8s.io/v1.DeleteOptions 的请求体。
				defaultGVK := scope.MetaGroupVersion.WithKind("DeleteOptions")
				obj, gvk, err := apihelpers.GetMetaInternalVersionCodecs().DecoderToVersion(s.Serializer, defaultGVK.GroupVersion()).Decode(body, &defaultGVK, options)
				if err != nil {
					err = errors.NewBadRequest(err.Error())
					scope.err(err, w, req)
					return
				}
				// 解码必须落回同一个 options 指针，否则说明 body 是别的类型，属于客户端错误。
				if obj != options {
					err = errors.NewBadRequest("decoded object cannot be converted to DeleteOptions")
					scope.err(err, w, req)
					return
				}
				span.AddEvent("Decoded delete options")

				objGV := gvk.GroupVersion()
				audit.LogRequestObject(req.Context(), obj, objGV, scope.Resource, scope.Subresource, apihelpers.GetMetaInternalVersionCodecs())
				span.AddEvent("Recorded the audit event")
			} else {
				if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), scope.MetaGroupVersion, options); err != nil {
					err = errors.NewBadRequest(err.Error())
					scope.err(err, w, req)
					return
				}
			}
		}
		// 特性关闭时必须剥离该字段，防止旧客户端提供的危险选项被静默接受。
		if !utilfeature.DefaultFeatureGate.Enabled(features.AllowUnsafeMalformedObjectDeletion) && options != nil {
			options.IgnoreStoreReadErrorWithClusterBreakingPotential = nil
		}
		if errs := validation.ValidateDeleteOptions(options); len(errs) > 0 {
			err := errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "DeleteOptions"}, "", errs)
			scope.err(err, w, req)
			return
		}
		options.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("DeleteOptions"))

		userInfo, _ := request.UserFrom(ctx)
		staticAdmissionAttrs := admission.NewAttributesRecord(nil, nil, scope.Kind, namespace, name, scope.Resource, scope.Subresource, admission.Delete, options, dryrun.IsDryRun(options.DryRun), userInfo)

		// AllowUnsafeMalformedObjectDeletion 允许在对象已损坏、无法正常读取时强行删除；
		// 这是绕过一致性保护的高危操作，因此要求额外的细粒度权限并留下审计注解。
		if utilfeature.DefaultFeatureGate.Enabled(features.AllowUnsafeMalformedObjectDeletion) {
			if options != nil && ptr.Deref(options.IgnoreStoreReadErrorWithClusterBreakingPotential, false) {
				// 确保审计记录能反映这次删除请求启用了
				// ignoreStoreReadErrorWithClusterBreakingPotential。
				audit.AddAuditAnnotation(ctx, "apiserver.k8s.io/unsafe-delete-ignore-read-error", "")

				p, ok := r.(rest.CorruptObjectDeleterProvider)
				if !ok || p.GetCorruptObjDeleter() == nil {
					// 这是开发者的编码错误
					scope.err(errors.NewInternalError(fmt.Errorf("no unsafe deleter provided, can not honor ignoreStoreReadErrorWithClusterBreakingPotential")), w, req)
					return
				}
				if scope.Authorizer == nil {
					scope.err(errors.NewInternalError(fmt.Errorf("no authorizer provided, unable to authorize unsafe delete")), w, req)
					return
				}
				if err := authorizeUnsafeDelete(ctx, staticAdmissionAttrs, scope.Authorizer); err != nil {
					scope.err(err, w, req)
					return
				}

				r = p.GetCorruptObjDeleter()
			}
		}

		span.AddEvent("About to delete object from database")
		wasDeleted := true
		// FinishRequest 防止请求因客户端断开而被中途取消：删除一旦开始，最好让它完成，
		// 否则会留下"部分删除"的不一致状态。
		result, err := finisher.FinishRequest(ctx, func() (runtime.Object, error) {
			obj, deleted, err := r.Delete(ctx, name, rest.AdmissionToValidateObjectDeleteFunc(admit, staticAdmissionAttrs, scope), options)
			wasDeleted = deleted
			return obj, err
		})
		if err != nil {
			scope.err(err, w, req)
			return
		}
		span.AddEvent("Object deleted from database")

		status := http.StatusOK
		// 若资源未被立即删除且用户通过设置 OrphanDependents=false 请求级联删除，则返回 http.StatusAccepted。
		// 注意：我们希望对所有“未立即删除”的情况都这么做，但那会破坏既有客户端。
		// 其他不会立即删除资源的场景有：命名空间删除与 pod 优雅删除。
		//
		// 202 是历史行为：只有"显式要求级联删除且未立即完成"才返回，
		// 是为了在不破坏既有客户端的前提下表达"已受理、稍后完成"。
		//nolint:staticcheck // SA1019 backwards compatibility
		//nolint: staticcheck
		if !wasDeleted && options.OrphanDependents != nil && !*options.OrphanDependents {
			status = http.StatusAccepted
		}
		// 若 rest.Deleter 返回 nil 对象，则补上一个 status；调用方也可能返回有效对象。
		if result == nil {
			result = &metav1.Status{
				Status: metav1.StatusSuccess,
				Code:   int32(status),
				Details: &metav1.StatusDetails{
					Name: name,
					Kind: scope.Kind.Kind,
				},
			}
		}

		span.AddEvent("About to write a response")
		defer span.AddEvent("Writing http response done")
		transformResponseObject(ctx, scope, req, w, status, outputMediaType, result)
	}
}

// DeleteCollection 返回处理集合删除的函数。
//
// 与单对象删除的关键差异：不施加 34 秒上限，因为集合删除可能涉及大量对象；
// 也不支持 ignoreStoreReadErrorWithClusterBreakingPotential（下面显式拒绝）。
func DeleteCollection(r rest.CollectionDeleter, checkBody bool, scope *RequestScope, admit admission.Interface) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		ctx, span := tracing.Start(ctx, "Delete", traceFields(req)...)
		req = req.WithContext(ctx)
		defer span.End(500 * time.Millisecond)

		namespace, err := scope.Namer.Namespace(req)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		// DELETECOLLECTION 可能是耗时较长的操作，
		// 这里不应施加 34 秒超时。
		// 注意：这与 LIST 类似，LIST 也不强制 34s 超时。
		ctx = request.WithNamespace(ctx, namespace)

		outputMediaType, _, err := negotiation.NegotiateOutputMediaType(req, scope.Serializer, scope)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		// 集合删除用 ListOptions 选择目标对象；selector 与分页参数都需要校验，
		// 否则可能被用来构造超大范围的删除。
		listOptions := metainternalversion.ListOptions{}
		if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), scope.MetaGroupVersion, &listOptions); err != nil {
			err = errors.NewBadRequest(err.Error())
			scope.err(err, w, req)
			return
		}

		metainternalversion.SetListOptionsDefaults(&listOptions, utilfeature.DefaultFeatureGate.Enabled(features.WatchList))
		if errs := metainternalversionvalidation.ValidateListOptions(&listOptions, utilfeature.DefaultFeatureGate.Enabled(features.WatchList)); len(errs) > 0 {
			err := errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "ListOptions"}, "", errs)
			scope.err(err, w, req)
			return
		}

		// 转换字段。
		// TODO: 这件事本该由 DecodeParametersInto 来完成。
		// 字段选择器需按资源类型转换标签，使 --field-selector metadata.name=x 之类
		// 的通用写法能匹配到内部字段。
		if listOptions.FieldSelector != nil {
			fn := func(label, value string) (newLabel, newValue string, err error) {
				return scope.Convertor.ConvertFieldLabel(scope.Kind, label, value)
			}
			if listOptions.FieldSelector, err = listOptions.FieldSelector.Transform(fn); err != nil {
				// TODO: 让 bad request 能够根据查询参数给出具体原因。
				err = errors.NewBadRequest(err.Error())
				scope.err(err, w, req)
				return
			}
		}

		options := &metav1.DeleteOptions{}
		if checkBody {
			body, err := limitedReadBodyWithRecordMetric(ctx, req, scope.MaxRequestBodyBytes, scope.Resource.GroupResource(), requestmetrics.DeleteCollection)
			if err != nil {
				span.AddEvent("limitedReadBody failed", attribute.Int("len", len(body)), attribute.String("err", err.Error()))
				scope.err(err, w, req)
				return
			}
			span.AddEvent("limitedReadBody succeeded", attribute.Int("len", len(body)))
			if len(body) > 0 {
				s, err := negotiation.NegotiateInputSerializer(req, false, apihelpers.GetMetaInternalVersionCodecs())
				if err != nil {
					scope.err(err, w, req)
					return
				}
				// 为向后兼容，需要允许既有客户端提交按 group 定义的 DeleteOptions，
				// 同时也允许提交 meta.k8s.io/v1.DeleteOptions 的请求体。
				defaultGVK := scope.MetaGroupVersion.WithKind("DeleteOptions")
				obj, gvk, err := apihelpers.GetMetaInternalVersionCodecs().DecoderToVersion(s.Serializer, defaultGVK.GroupVersion()).Decode(body, &defaultGVK, options)
				if err != nil {
					err = errors.NewBadRequest(err.Error())
					scope.err(err, w, req)
					return
				}
				if obj != options {
					err = errors.NewBadRequest("decoded object cannot be converted to DeleteOptions")
					scope.err(err, w, req)
					return
				}

				objGV := gvk.GroupVersion()
				audit.LogRequestObject(req.Context(), obj, objGV, scope.Resource, scope.Subresource, apihelpers.GetMetaInternalVersionCodecs())
			} else {
				if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), scope.MetaGroupVersion, options); err != nil {
					err = errors.NewBadRequest(err.Error())
					scope.err(err, w, req)
					return
				}
			}
		}
		if !utilfeature.DefaultFeatureGate.Enabled(features.AllowUnsafeMalformedObjectDeletion) && options != nil {
			options.IgnoreStoreReadErrorWithClusterBreakingPotential = nil
		}
		if errs := validation.ValidateDeleteOptions(options); len(errs) > 0 {
			err := errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "DeleteOptions"}, "", errs)
			scope.err(err, w, req)
			return
		}

		// 集合删除同样拒绝该危险选项：逐个忽略读取错误会让批量删除的语义变得不可预测。
		if utilfeature.DefaultFeatureGate.Enabled(features.AllowUnsafeMalformedObjectDeletion) {
			if options != nil && ptr.Deref(options.IgnoreStoreReadErrorWithClusterBreakingPotential, false) {
				fieldErrList := field.ErrorList{
					field.Invalid(field.NewPath("ignoreStoreReadErrorWithClusterBreakingPotential"), true, "is not allowed with DELETECOLLECTION, try again after removing the option"),
				}
				err := errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "DeleteOptions"}, "", fieldErrList)
				scope.err(err, w, req)
				return
			}
		}

		options.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("DeleteOptions"))

		admit = admission.WithAudit(admit)
		userInfo, _ := request.UserFrom(ctx)
		staticAdmissionAttrs := admission.NewAttributesRecord(nil, nil, scope.Kind, namespace, "", scope.Resource, scope.Subresource, admission.Delete, options, dryrun.IsDryRun(options.DryRun), userInfo)
		result, err := finisher.FinishRequest(ctx, func() (runtime.Object, error) {
			return r.DeleteCollection(ctx, rest.AdmissionToValidateObjectDeleteFunc(admit, staticAdmissionAttrs, scope), options, &listOptions)
		})
		if err != nil {
			scope.err(err, w, req)
			return
		}

		// 若 rest.Deleter 返回 nil 对象，则补上一个 status；调用方也可能返回有效对象。
		if result == nil {
			result = &metav1.Status{
				Status: metav1.StatusSuccess,
				Code:   http.StatusOK,
				Details: &metav1.StatusDetails{
					Kind: scope.Kind.Kind,
				},
			}
		}

		span.AddEvent("About to write a response")
		defer span.AddEvent("Writing http response done")
		transformResponseObject(ctx, scope, req, w, http.StatusOK, outputMediaType, result)
	}
}

// authorizeUnsafeDelete 在启用 ignoreStoreReadErrorWithClusterBreakingPotential 时，
// 确认用户对被删除的资源拥有 'unsafe-delete-ignore-read-errors' 权限。
//
// 使用独立的动词而非普通 delete 权限：绕过存储读取错误可能破坏一致性，
// 必须由集群管理员显式授予，不能由普通的资源删除权限隐含。
func authorizeUnsafeDelete(ctx context.Context, attr admission.Attributes, authz authorizer.UnconditionalAuthorizer) (err error) {
	// 只对 Delete 且带选项的请求生效；其他情况无需额外授权。
	if attr.GetOperation() != admission.Delete || attr.GetOperationOptions() == nil {
		return nil
	}
	options, ok := attr.GetOperationOptions().(*metav1.DeleteOptions)
	if !ok {
		return errors.NewInternalError(fmt.Errorf("expected an option of type: %T, but got: %T", &metav1.DeleteOptions{}, attr.GetOperationOptions()))
	}
	// 未开启危险选项时无需求额外权限。
	if !ptr.Deref(options.IgnoreStoreReadErrorWithClusterBreakingPotential, false) {
		return nil
	}

	requestInfo, found := request.RequestInfoFrom(ctx)
	if !found {
		return admission.NewForbidden(attr, fmt.Errorf("no RequestInfo found in the context"))
	}
	// 禁止在子资源或非资源请求上使用该选项：这些路径的语义不明确，容易误用。
	if !requestInfo.IsResourceRequest || len(attr.GetSubresource()) > 0 {
		return admission.NewForbidden(attr, fmt.Errorf("ignoreStoreReadErrorWithClusterBreakingPotential delete option is not allowed on a subresource or non-resource request"))
	}

	// 走到这里说明删除选项中 IgnoreStoreReadErrorWithClusterBreakingPotential
	// 已被置为 true，用户必须对给定资源拥有 'unsafe-delete-ignore-read-errors' 权限。
	record := authorizer.AttributesRecord{
		User:            attr.GetUserInfo(),
		Verb:            "unsafe-delete-ignore-read-errors",
		Namespace:       attr.GetNamespace(),
		Name:            attr.GetName(),
		APIGroup:        attr.GetResource().Group,
		APIVersion:      attr.GetResource().Version,
		Resource:        attr.GetResource().Resource,
		ResourceRequest: true,
	}
	// TODO: 无法使用 k8s.io/kubernetes/pkg/registry/authorization/util 中的 ResourceAttributesFrom，
	// 以避免 staging 反向依赖 k8s.io/kubernetes 的问题。
	//
	// 把选择器一并带入授权属性，使基于 selector 的 RBAC 规则（如只能删除特定字段值的对象）
	// 在批量删除场景下同样生效；解析失败也记录下来，交由授权器决定如何处理。
	if len(requestInfo.FieldSelector) > 0 {
		fieldSelector, err := fields.ParseSelector(requestInfo.FieldSelector)
		if err != nil {
			record.FieldSelectorRequirements, record.FieldSelectorParsingErr = nil, err
		} else {
			if requirements := fieldSelector.Requirements(); len(requirements) > 0 {
				record.FieldSelectorRequirements, record.FieldSelectorParsingErr = fieldSelector.Requirements(), nil
			}
		}
	}
	if len(requestInfo.LabelSelector) > 0 {
		labelSelector, err := labels.Parse(requestInfo.LabelSelector)
		if err != nil {
			record.LabelSelectorRequirements, record.LabelSelectorParsingErr = nil, err
		} else {
			if requirements, _ /*selectable*/ := labelSelector.Requirements(); len(requirements) > 0 {
				record.LabelSelectorRequirements, record.LabelSelectorParsingErr = requirements, nil
			}
		}
	}

	decision, reason, err := authz.Authorize(ctx, record)
	if err != nil {
		// 授权器出错按拒绝处理（fail closed），不能默认放行高危操作。
		err = fmt.Errorf("error while checking permission for %q, %w", record.Verb, err)
		klog.FromContext(ctx).V(1).Info("failed to authorize", "err", err)
		return admission.NewForbidden(attr, err)
	}
	if decision == authorizer.DecisionAllow {
		return nil
	}

	return admission.NewForbidden(attr, fmt.Errorf("not permitted to do %q, reason: %s", record.Verb, reason))
}
