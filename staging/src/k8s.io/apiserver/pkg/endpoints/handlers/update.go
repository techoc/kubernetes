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
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	"k8s.io/apiserver/pkg/endpoints/handlers/finisher"
	requestmetrics "k8s.io/apiserver/pkg/endpoints/handlers/metrics"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/util/dryrun"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
)

// UpdateResource returns a function that will handle a resource update
//
// PUT 可能变成 create（AllowCreateOnUpdate 的资源），因此这里同时准备
// create 与 update 两套准入属性与授权属性，由存储层在事务内决定实际走哪条路径。
func UpdateResource(r rest.Updater, scope *RequestScope, admit admission.Interface) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		// For performance tracking purposes.
		ctx, span := tracing.Start(ctx, "Update", traceFields(req)...)
		req = req.WithContext(ctx)
		defer span.End(500 * time.Millisecond)

		namespace, name, err := scope.Namer.Name(req)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		// enforce a timeout of at most requestTimeoutUpperBound (34s) or less if the user-provided
		// timeout inside the parent context is lower than requestTimeoutUpperBound.
		// 34s 来自 etcd 默认请求超时的安全余量，保证客户端断开前服务端能先收尾。
		ctx, cancel := context.WithTimeout(ctx, requestTimeoutUpperBound)
		defer cancel()

		ctx = request.WithNamespace(ctx, namespace)

		outputMediaType, _, err := negotiation.NegotiateOutputMediaType(req, scope.Serializer, scope)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		body, err := limitedReadBodyWithRecordMetric(ctx, req, scope.MaxRequestBodyBytes, scope.Resource.GroupResource(), requestmetrics.Update)
		if err != nil {
			span.AddEvent("limitedReadBody failed", attribute.Int("len", len(body)), attribute.String("err", err.Error()))
			scope.err(err, w, req)
			return
		}
		span.AddEvent("limitedReadBody succeeded", attribute.Int("len", len(body)))

		// 先解析并校验查询参数，再读请求体：非法参数不应消耗带宽去读取大对象。
		options := &metav1.UpdateOptions{}
		if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), scope.MetaGroupVersion, options); err != nil {
			err = errors.NewBadRequest(err.Error())
			scope.err(err, w, req)
			return
		}
		if errs := validation.ValidateUpdateOptions(options); len(errs) > 0 {
			err := errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "UpdateOptions"}, "", errs)
			scope.err(err, w, req)
			return
		}
		options.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("UpdateOptions"))

		s, err := negotiation.NegotiateInputSerializer(req, false, scope.Serializer)
		if err != nil {
			scope.err(err, w, req)
			return
		}
		defaultGVK := scope.Kind
		original := r.New()

		// 严格解码只在 Warn/Strict 模式下启用；Ignore 必须使用宽松解码，
		// 否则未知字段会直接导致请求失败，违背该模式的兼容性承诺。
		validationDirective := fieldValidation(options.FieldValidation)
		decodeSerializer := s.Serializer
		if validationDirective == metav1.FieldValidationWarn || validationDirective == metav1.FieldValidationStrict {
			decodeSerializer = s.StrictSerializer
		}

		decoder := scope.Serializer.DecoderToVersion(decodeSerializer, scope.HubGroupVersion)
		span.AddEvent("About to convert to expected version")
		obj, gvk, err := decoder.Decode(body, &defaultGVK, original)
		if err != nil {
			strictError, isStrictError := runtime.AsStrictDecodingError(err)
			switch {
			case isStrictError && obj != nil && validationDirective == metav1.FieldValidationWarn:
				addStrictDecodingWarnings(req.Context(), strictError.Errors())
			case isStrictError && validationDirective == metav1.FieldValidationIgnore:
				klog.Warningf("unexpected strict error when field validation is set to ignore")
				fallthrough
			default:
				err = transformDecodeError(scope.Typer, err, original, gvk, body)
				scope.err(err, w, req)
				return
			}
		}

		// 请求体中的 apiVersion 必须与路由期望一致，否则拒绝：
		// 否则客户端可能用 A 版本的 schema 提交 B 版本的语义。
		objGV := gvk.GroupVersion()
		if !scope.AcceptsGroupVersion(objGV) {
			err = errors.NewBadRequest(fmt.Sprintf("the API version in the data (%s) does not match the expected API version (%s)", objGV, defaultGVK.GroupVersion()))
			scope.err(err, w, req)
			return
		}
		span.AddEvent("Conversion done")

		// 在准入之前记录审计请求体，保证即使准入拒绝也能留下原始输入。
		audit.LogRequestObject(req.Context(), obj, objGV, scope.Resource, scope.Subresource, scope.Serializer)
		admit = admission.WithAudit(admit)

		// if this object supports namespace info
		// 命名空间必须与 URL 一致，防止越权写到其他命名空间。
		if objectMeta, err := meta.Accessor(obj); err == nil {
			// ensure namespace on the object is correct, or error if a conflicting namespace was set in the object
			if err := rest.EnsureObjectNamespaceMatchesRequestNamespace(rest.ExpectedNamespaceForResource(namespace, scope.Resource), objectMeta); err != nil {
				scope.err(err, w, req)
				return
			}
		}

		if err := checkName(obj, name, namespace, scope.Namer); err != nil {
			scope.err(err, w, req)
			return
		}

		userInfo, _ := request.UserFrom(ctx)
		transformers := []rest.TransformFunc{}

		// allows skipping managedFields update if the resulting object is too big
		// managedFields 可能非常大；若因此超出 etcd 限制，会退化为不维护它并重试。
		shouldUpdateManagedFields := true
		admit = fieldmanager.NewManagedFieldsValidatingAdmissionController(admit)
		transformers = append(transformers, func(_ context.Context, newObj, liveObj runtime.Object) (runtime.Object, error) {
			if shouldUpdateManagedFields {
				return scope.FieldManager.UpdateNoErrors(liveObj, newObj, managerOrUserAgent(options.FieldManager, req.UserAgent())), nil
			}
			return newObj, nil
		})

		// 变更类准入作为 transformer 注入，在存储层“读-改-写”事务内执行，
		// 从而保证准入看到的是最新的 liveObj。
		if mutatingAdmission, ok := admit.(admission.MutationInterface); ok {
			transformers = append(transformers, func(ctx context.Context, newObj, oldObj runtime.Object) (runtime.Object, error) {
				isNotZeroObject, err := hasUID(oldObj)
				if err != nil {
					return nil, fmt.Errorf("unexpected error when extracting UID from oldObj: %v", err.Error())
				} else if !isNotZeroObject {
					if mutatingAdmission.Handles(admission.Create) {
						return newObj, mutatingAdmission.Admit(ctx, admission.NewAttributesRecord(newObj, nil, scope.Kind, namespace, name, scope.Resource, scope.Subresource, admission.Create, updateToCreateOptions(options), dryrun.IsDryRun(options.DryRun), userInfo), scope)
					}
				} else {
					if mutatingAdmission.Handles(admission.Update) {
						return newObj, mutatingAdmission.Admit(ctx, admission.NewAttributesRecord(newObj, oldObj, scope.Kind, namespace, name, scope.Resource, scope.Subresource, admission.Update, options, dryrun.IsDryRun(options.DryRun), userInfo), scope)
					}
				}
				return newObj, nil
			})
			transformers = append(transformers, func(ctx context.Context, newObj, oldObj runtime.Object) (runtime.Object, error) {
				// Dedup owner references again after mutating admission happens
				// 准入可能新增 ownerReferences，必须在写库前再次去重。
				dedupOwnerReferencesAndAddWarning(newObj, req.Context(), true)
				return newObj, nil
			})
		}

		// PUT 触发创建时需要的授权属性：verb 固定为 create，与 update 权限分离，
		// 防止仅有 update 权限的用户凭空创建对象。
		createAuthorizerAttributes := authorizer.AttributesRecord{
			User:            userInfo,
			ResourceRequest: true,
			Path:            req.URL.Path,
			Verb:            "create",
			APIGroup:        scope.Resource.Group,
			APIVersion:      scope.Resource.Version,
			Resource:        scope.Resource.Resource,
			Subresource:     scope.Subresource,
			Namespace:       namespace,
			Name:            name,
		}

		span.AddEvent("About to store object in database")
		wasCreated := false
		requestFunc := func() (runtime.Object, error) {
			obj, created, err := r.Update(
				ctx,
				name,
				rest.DefaultUpdatedObjectInfo(obj, transformers...),
				withAuthorization(rest.AdmissionToValidateObjectFunc(
					admit,
					admission.NewAttributesRecord(nil, nil, scope.Kind, namespace, name, scope.Resource, scope.Subresource, admission.Create, updateToCreateOptions(options), dryrun.IsDryRun(options.DryRun), userInfo), scope),
					scope.Authorizer, createAuthorizerAttributes),
				rest.AdmissionToValidateObjectUpdateFunc(
					admit,
					admission.NewAttributesRecord(nil, nil, scope.Kind, namespace, name, scope.Resource, scope.Subresource, admission.Update, options, dryrun.IsDryRun(options.DryRun), userInfo), scope),
				false,
				options,
			)
			wasCreated = created
			return obj, err
		}
		// Dedup owner references before updating managed fields
		// 入库前先去重，避免重复引用被计入 managedFields 造成后续冲突。
		dedupOwnerReferencesAndAddWarning(obj, req.Context(), false)
		result, err := finisher.FinishRequest(ctx, func() (runtime.Object, error) {
			result, err := requestFunc()
			// If the object wasn't committed to storage because it's serialized size was too large,
			// it is safe to remove managedFields (which can be large) and try again.
			// 仅对“对象过大”重试一次，且必须在确认未写入后进行，避免重复应用变更。
			if isTooLargeError(err) {
				if accessor, accessorErr := meta.Accessor(obj); accessorErr == nil {
					accessor.SetManagedFields(nil)
					shouldUpdateManagedFields = false
					result, err = requestFunc()
				}
			}
			return result, err
		})
		if err != nil {
			span.AddEvent("Write to database call failed", attribute.Int("len", len(body)), attribute.String("err", err.Error()))
			scope.err(err, w, req)
			return
		}
		span.AddEvent("Write to database call succeeded", attribute.Int("len", len(body)))

		// 语义上 PUT 触发了创建时返回 201，其余情况返回 200。
		status := http.StatusOK
		if wasCreated {
			status = http.StatusCreated
		}

		span.AddEvent("About to write a response")
		defer span.AddEvent("Writing http response done")
		transformResponseObject(ctx, scope, req, w, status, outputMediaType, result)
	}
}

// withAuthorization 在 create-on-update 路径上补做 create 授权，再执行校验准入。
//
// 使用 sync.Once 缓存授权结果：存储层可能因冲突重试而多次调用校验函数，
// 授权判断不必也不应在每次重试时重复执行。
func withAuthorization(validate rest.ValidateObjectFunc, a authorizer.UnconditionalAuthorizer, attributes authorizer.Attributes) rest.ValidateObjectFunc {
	var once sync.Once
	var authorizerDecision authorizer.Decision
	var authorizerReason string
	var authorizerErr error
	return func(ctx context.Context, obj runtime.Object) error {
		if a == nil {
			return errors.NewInternalError(fmt.Errorf("no authorizer provided, unable to authorize a create on update"))
		}
		once.Do(func() {
			authorizerDecision, authorizerReason, authorizerErr = a.Authorize(ctx, attributes)
		})
		// an authorizer like RBAC could encounter evaluation errors and still allow the request, so authorizer decision is checked before error here.
		// 顺序不可颠倒：Allow 优先于 error，否则 RBAC 的部分失败会被误报为 500。
		if authorizerDecision == authorizer.DecisionAllow {
			// Continue to validating admission
			return validate(ctx, obj)
		}
		if authorizerErr != nil {
			return errors.NewInternalError(authorizerErr)
		}

		// The user is not authorized to perform this action, so we need to build the error response
		return responsewriters.ForbiddenStatusError(attributes, authorizerReason)
	}
}

// updateToCreateOptions creates a CreateOptions with the same field values as the provided UpdateOptions.
//
// 仅复制 dryRun/fieldManager/fieldValidation 三个语义等价的字段，其余字段
// 在 create 与 update 之间并不通用。
func updateToCreateOptions(uo *metav1.UpdateOptions) *metav1.CreateOptions {
	if uo == nil {
		return nil
	}
	co := &metav1.CreateOptions{
		DryRun:          uo.DryRun,
		FieldManager:    uo.FieldManager,
		FieldValidation: uo.FieldValidation,
	}
	co.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("CreateOptions"))
	return co
}
