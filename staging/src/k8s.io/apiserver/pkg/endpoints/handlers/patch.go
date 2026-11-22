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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	jsonpatch "gopkg.in/evanphx/json-patch.v4"
	kjson "sigs.k8s.io/json"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/mergepatch"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	"k8s.io/apiserver/pkg/endpoints/handlers/finisher"
	requestmetrics "k8s.io/apiserver/pkg/endpoints/handlers/metrics"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/util/dryrun"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/tracing"
)

const (
	// maximum number of operations a single json patch may contain.
	//
	// 限制操作数是为了给 JSON Patch 的应用成本设上界，防止单个请求消耗过多 CPU。
	maxJSONPatchOperations = 10000
)

// PatchResource returns a function that will handle a resource patch.
//
// 处理流程：解析 Content-Type 与请求参数 → 读取并校验 patch → 记录审计 →
// 组装 patcher → 通过 GuaranteedUpdate 在存储层重试地应用 patch 与准入 → 写响应。
func PatchResource(r rest.Patcher, scope *RequestScope, admit admission.Interface, patchTypes []string) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		// For performance tracking purposes.
		ctx, span := tracing.Start(ctx, "Patch", traceFields(req)...)
		req = req.WithContext(ctx)
		defer span.End(500 * time.Millisecond)

		// Do this first, otherwise name extraction can fail for unrecognized content types
		// TODO: handle this in negotiation
		//
		// 必须先剥离 "; charset=..." 再交给 Namer，否则部分 Namer 无法识别媒体类型。
		contentType := req.Header.Get("Content-Type")
		// Remove "; charset=" if included in header.
		if idx := strings.Index(contentType, ";"); idx > 0 {
			contentType = contentType[:idx]
		}
		patchType := types.PatchType(contentType)

		// Ensure the patchType is one we support
		if !sets.NewString(patchTypes...).Has(contentType) {
			scope.err(negotiation.NewUnsupportedMediaTypeError(patchTypes), w, req)
			return
		}

		namespace, name, err := scope.Namer.Name(req)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		// enforce a timeout of at most requestTimeoutUpperBound (34s) or less if the user-provided
		// timeout inside the parent context is lower than requestTimeoutUpperBound.
		//
		// 这里只设上界；父上下文中更短的超时仍会先生效（context 取最早截止时间）。
		ctx, cancel := context.WithTimeout(ctx, requestTimeoutUpperBound)
		defer cancel()

		ctx = request.WithNamespace(ctx, namespace)

		outputMediaType, _, err := negotiation.NegotiateOutputMediaType(req, scope.Serializer, scope)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		patchBytes, err := limitedReadBodyWithRecordMetric(ctx, req, scope.MaxRequestBodyBytes, scope.Resource.GroupResource(), requestmetrics.Patch)
		if err != nil {
			span.AddEvent("limitedReadBody failed", attribute.Int("len", len(patchBytes)), attribute.String("err", err.Error()))
			scope.err(err, w, req)
			return
		}
		span.AddEvent("limitedReadBody succeeded", attribute.Int("len", len(patchBytes)))

		options := &metav1.PatchOptions{}
		if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), scope.MetaGroupVersion, options); err != nil {
			err = errors.NewBadRequest(err.Error())
			scope.err(err, w, req)
			return
		}
		if errs := validation.ValidatePatchOptions(options, patchType); len(errs) > 0 {
			err := errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "PatchOptions"}, "", errs)
			scope.err(err, w, req)
			return
		}
		options.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("PatchOptions"))

		admit = admission.WithAudit(admit)

		// Decode and convert the patch payload to JSON for storage into audit.
		// the decision to reject the request for failure to decode before we record the request for audit.
		//
		// This guarantees that the audit event contains valid JSON, or the request is rejected.
		patchForAudit, err := validateAndTranscodePatch(patchBytes, patchType)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		audit.LogRequestPatch(req.Context(), patchForAudit)
		span.AddEvent("Recorded the audit event")

		// apply 类 patch 的载荷是 YAML/CBOR 而非 JSON，需要选择对应的基础序列化器。
		var baseContentType string
		switch patchType {
		case types.ApplyYAMLPatchType:
			baseContentType = runtime.ContentTypeYAML
		case types.ApplyCBORPatchType:
			if !utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
				// This request should have already been rejected by the
				// Content-Type allowlist check. Return 500 because assumptions are
				// already broken and the feature is not GA.
				utilruntime.HandleErrorWithContext(req.Context(), nil, "The patch content-type allowlist check should have made this unreachable.")
				scope.err(errors.NewInternalError(errors.NewInternalError(fmt.Errorf("unexpected patch type: %v", patchType))), w, req)
				return
			}

			baseContentType = runtime.ContentTypeCBOR
		default:
			baseContentType = runtime.ContentTypeJSON
		}

		s, ok := runtime.SerializerInfoForMediaType(scope.Serializer.SupportedMediaTypes(), baseContentType)
		if !ok {
			scope.err(fmt.Errorf("no serializer defined for %v", baseContentType), w, req)
			return
		}
		gv := scope.Kind.GroupVersion()

		// 严格/警告模式要求解码时报告未知字段，因此换成 StrictSerializer。
		validationDirective := fieldValidation(options.FieldValidation)
		decodeSerializer := s.Serializer
		if validationDirective == metav1.FieldValidationWarn || validationDirective == metav1.FieldValidationStrict {
			decodeSerializer = s.StrictSerializer
		}

		codec := runtime.NewCodec(
			scope.Serializer.EncoderForVersion(s.Serializer, gv),
			scope.Serializer.DecoderToVersion(decodeSerializer, scope.HubGroupVersion),
		)

		// 预构造 Create/Update 两种准入属性，因为 patch 既可能创建对象也可能更新对象。
		userInfo, _ := request.UserFrom(ctx)
		staticCreateAttributes := admission.NewAttributesRecord(
			nil,
			nil,
			scope.Kind,
			namespace,
			name,
			scope.Resource,
			scope.Subresource,
			admission.Create,
			patchToCreateOptions(options),
			dryrun.IsDryRun(options.DryRun),
			userInfo)
		staticUpdateAttributes := admission.NewAttributesRecord(
			nil,
			nil,
			scope.Kind,
			namespace,
			name,
			scope.Resource,
			scope.Subresource,
			admission.Update,
			patchToUpdateOptions(options),
			dryrun.IsDryRun(options.DryRun),
			userInfo,
		)

		// 包一层 managedFields 校验：在准入链中拦截对 managedFields 的非法修改。
		admit = fieldmanager.NewManagedFieldsValidatingAdmissionController(admit)

		mutatingAdmission, _ := admit.(admission.MutationInterface)
		// create 的授权属性需要单独构造：patch 触发的创建同样要经过 create 权限检查。
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

		p := patcher{
			namer:               scope.Namer,
			creater:             scope.Creater,
			defaulter:           scope.Defaulter,
			typer:               scope.Typer,
			unsafeConvertor:     scope.UnsafeConvertor,
			kind:                scope.Kind,
			resource:            scope.Resource,
			subresource:         scope.Subresource,
			dryRun:              dryrun.IsDryRun(options.DryRun),
			validationDirective: validationDirective,

			objectInterfaces: scope,

			hubGroupVersion: scope.HubGroupVersion,

			// create 校验额外套一层授权：patch 可能创建对象，必须检查 create 权限。
			createValidation: withAuthorization(rest.AdmissionToValidateObjectFunc(admit, staticCreateAttributes, scope), scope.Authorizer, createAuthorizerAttributes),
			updateValidation: rest.AdmissionToValidateObjectUpdateFunc(admit, staticUpdateAttributes, scope),
			admissionCheck:   mutatingAdmission,

			codec: codec,

			options: options,

			restPatcher: r,
			name:        name,
			patchType:   patchType,
			patchBytes:  patchBytes,
			userAgent:   req.UserAgent(),
		}

		result, wasCreated, err := p.patchResource(ctx, scope)
		if err != nil {
			scope.err(err, w, req)
			return
		}
		span.AddEvent("Object stored in database")

		status := http.StatusOK
		// 服务器端 apply 可能创建对象，此时必须返回 201 而非 200。
		if wasCreated {
			status = http.StatusCreated
		}

		span.AddEvent("About to write a response")
		defer span.AddEvent("Writing http response done")
		transformResponseObject(ctx, scope, req, w, status, outputMediaType, result)
	}
}

// mutateObjectUpdateFunc 是“对 (obj, old) 执行变更”的函数签名。
type mutateObjectUpdateFunc func(ctx context.Context, obj, old runtime.Object) error

// patcher breaks the process of patch application and retries into smaller
// pieces of functionality.
// TODO: Use builder pattern to construct this object?
// TODO: As part of that effort, some aspects of PatchResource above could be
// moved into this type.
type patcher struct {
	// Pieces of RequestScope
	namer               ScopeNamer
	creater             runtime.ObjectCreater
	defaulter           runtime.ObjectDefaulter
	typer               runtime.ObjectTyper
	unsafeConvertor     runtime.ObjectConvertor
	resource            schema.GroupVersionResource
	kind                schema.GroupVersionKind
	subresource         string
	dryRun              bool
	validationDirective string

	objectInterfaces admission.ObjectInterfaces

	hubGroupVersion schema.GroupVersion

	// Validation functions
	createValidation rest.ValidateObjectFunc
	updateValidation rest.ValidateObjectUpdateFunc
	admissionCheck   admission.MutationInterface

	codec runtime.Codec

	options *metav1.PatchOptions

	// Operation information
	restPatcher rest.Patcher
	name        string
	patchType   types.PatchType
	patchBytes  []byte
	userAgent   string

	// Set at invocation-time (by applyPatch) and immutable thereafter
	//
	// 这些字段在 patchResource 确定 patch 机制后写入，之后只读；
	// 因为 GuaranteedUpdate 可能重试多次，每次重试共享同一份配置。
	namespace         string
	updatedObjectInfo rest.UpdatedObjectInfo
	mechanism         patchMechanism
	forceAllowCreate  bool
}

// patchMechanism 抽象不同 patch 类型的“应用到现有对象”与“从零创建对象”两种操作。
// 分开两种操作是为了让 patch 在对象不存在时也能走 create 语义（apply 场景）。
type patchMechanism interface {
	applyPatchToCurrentObject(requextContext context.Context, currentObject runtime.Object) (runtime.Object, error)
	createNewObject(requestContext context.Context) (runtime.Object, error)
}

// jsonPatcher 处理 JSON Patch 与 Merge Patch。
type jsonPatcher struct {
	*patcher

	fieldManager *managedfields.FieldManager
}

// applyPatchToCurrentObject 把 JSON 类 patch 应用到当前对象上。
// 先编码为外部版本 JSON（patch 是针对外部版本构造的），再应用 patch 并解码回类型化对象。
func (p *jsonPatcher) applyPatchToCurrentObject(requestContext context.Context, currentObject runtime.Object) (runtime.Object, error) {
	// Encode will convert & return a versioned object in JSON.
	currentObjJS, err := runtime.Encode(p.codec, currentObject)
	if err != nil {
		return nil, err
	}

	// Apply the patch.
	patchedObjJS, appliedStrictErrs, err := p.applyJSPatch(currentObjJS)
	if err != nil {
		return nil, err
	}

	// Construct the resulting typed, unversioned object.
	objToUpdate := p.restPatcher.New()
	if err := runtime.DecodeInto(p.codec, patchedObjJS, objToUpdate); err != nil {
		strictError, isStrictError := runtime.AsStrictDecodingError(err)
		switch {
		case !isStrictError:
			// disregard any appliedStrictErrs, because it's an incomplete
			// list of strict errors given that we don't know what fields were
			// unknown because DecodeInto failed. Non-strict errors trump in this case.
			return nil, errors.NewInvalid(schema.GroupKind{}, "", field.ErrorList{
				field.Invalid(field.NewPath("patch"), string(patchedObjJS), err.Error()),
			})
		case p.validationDirective == metav1.FieldValidationWarn:
			addStrictDecodingWarnings(requestContext, append(appliedStrictErrs, strictError.Errors()...))
		default:
			strictDecodingError := runtime.NewStrictDecodingError(append(appliedStrictErrs, strictError.Errors()...))
			return nil, errors.NewInvalid(schema.GroupKind{}, "", field.ErrorList{
				field.Invalid(field.NewPath("patch"), string(patchedObjJS), strictDecodingError.Error()),
			})
		}
	} else if len(appliedStrictErrs) > 0 {
		switch {
		case p.validationDirective == metav1.FieldValidationWarn:
			addStrictDecodingWarnings(requestContext, appliedStrictErrs)
		default:
			return nil, errors.NewInvalid(schema.GroupKind{}, "", field.ErrorList{
				field.Invalid(field.NewPath("patch"), string(patchedObjJS), runtime.NewStrictDecodingError(appliedStrictErrs).Error()),
			})
		}
	}

	if p.options == nil {
		// Provide a more informative error for the crash that would
		// happen on the next line
		panic("PatchOptions required but not provided")
	}
	// UpdateNoErrors 只更新 managedFields 所有权，校验留给后续的准入链，
	// 避免把字段所有权冲突错误地当作请求错误返回。
	objToUpdate = p.fieldManager.UpdateNoErrors(currentObject, objToUpdate, managerOrUserAgent(p.options.FieldManager, p.userAgent))
	return objToUpdate, nil
}

// createNewObject 对 JSON/Merge patch 返回 NotFound：这类 patch 不能凭空创建对象。
func (p *jsonPatcher) createNewObject(_ context.Context) (runtime.Object, error) {
	return nil, errors.NewNotFound(p.resource.GroupResource(), p.name)
}

// jsonPatchOp 是 JSON Patch 单个操作的 JSON 表示，仅用于严格解码检查未知字段。
type jsonPatchOp struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	From  string      `json:"from"`
	Value interface{} `json:"value"`
}

// applyJSPatch applies the patch. Input and output objects must both have
// the external version, since that is what the patch must have been constructed against.
func (p *jsonPatcher) applyJSPatch(versionedJS []byte) (patchedJS []byte, strictErrors []error, retErr error) {
	switch p.patchType {
	case types.JSONPatchType:
		// 严格/警告模式下先按操作数组解码，以发现未知字段；不影响实际 patch 的应用。
		if p.validationDirective == metav1.FieldValidationStrict || p.validationDirective == metav1.FieldValidationWarn {
			var v []jsonPatchOp
			var err error
			if strictErrors, err = kjson.UnmarshalStrict(p.patchBytes, &v); err != nil {
				return nil, nil, errors.NewBadRequest(fmt.Sprintf("error decoding patch: %v", err))
			}
			for i, e := range strictErrors {
				strictErrors[i] = fmt.Errorf("json patch %v", e)
			}
		}

		patchObj, err := jsonpatch.DecodePatch(p.patchBytes)
		if err != nil {
			return nil, nil, errors.NewBadRequest(err.Error())
		}
		// 操作数超限返回 413，提示客户端拆分为多次请求。
		if len(patchObj) > maxJSONPatchOperations {
			return nil, nil, errors.NewRequestEntityTooLargeError(
				fmt.Sprintf("The allowed maximum operations in a JSON patch is %d, got %d",
					maxJSONPatchOperations, len(patchObj)))
		}
		patchedJS, err := patchObj.Apply(versionedJS)
		if err != nil {
			return nil, nil, errors.NewGenericServerResponse(http.StatusUnprocessableEntity, "", schema.GroupResource{}, "", err.Error(), 0, false)
		}
		return patchedJS, strictErrors, nil
	case types.MergePatchType:
		if p.validationDirective == metav1.FieldValidationStrict || p.validationDirective == metav1.FieldValidationWarn {
			v := map[string]any{}
			var err error
			strictErrors, err = kjson.UnmarshalStrict(p.patchBytes, &v)
			if err != nil {
				return nil, nil, errors.NewBadRequest(fmt.Sprintf("error decoding patch: %v", err))
			}
		}

		patchedJS, retErr = jsonpatch.MergePatch(versionedJS, p.patchBytes)
		if retErr == jsonpatch.ErrBadJSONPatch {
			return nil, nil, errors.NewBadRequest(retErr.Error())
		}
		return patchedJS, strictErrors, retErr
	default:
		// only here as a safety net - go-restful filters content-type
		return nil, nil, fmt.Errorf("unknown Content-Type header for patch: %v", p.patchType)
	}
}

// smpPatcher 处理 Strategic Merge Patch，需要 schema 参考对象来计算合并策略。
type smpPatcher struct {
	*patcher

	// Schema
	schemaReferenceObj runtime.Object
	fieldManager       *managedfields.FieldManager
}

// applyPatchToCurrentObject 先把对象转到外部版本（patch 针对外部版本），
// 应用策略合并后再转回 hub 版本，最后更新 managedFields 所有权。
func (p *smpPatcher) applyPatchToCurrentObject(requestContext context.Context, currentObject runtime.Object) (runtime.Object, error) {
	// Since the patch is applied on versioned objects, we need to convert the
	// current object to versioned representation first.
	currentVersionedObject, err := p.unsafeConvertor.ConvertToVersion(currentObject, p.kind.GroupVersion())
	if err != nil {
		return nil, err
	}
	versionedObjToUpdate, err := p.creater.New(p.kind)
	if err != nil {
		return nil, err
	}
	if err := strategicPatchObject(requestContext, p.defaulter, currentVersionedObject, p.patchBytes, versionedObjToUpdate, p.schemaReferenceObj, p.validationDirective); err != nil {
		return nil, err
	}
	// Convert the object back to the hub version
	newObj, err := p.unsafeConvertor.ConvertToVersion(versionedObjToUpdate, p.hubGroupVersion)
	if err != nil {
		return nil, err
	}

	newObj = p.fieldManager.UpdateNoErrors(currentObject, newObj, managerOrUserAgent(p.options.FieldManager, p.userAgent))
	return newObj, nil
}

// createNewObject 对策略合并 patch 同样返回 NotFound。
func (p *smpPatcher) createNewObject(_ context.Context) (runtime.Object, error) {
	return nil, errors.NewNotFound(p.resource.GroupResource(), p.name)
}

// newApplyPatcher 构造 apply patcher；解码函数按 YAML/CBOR 注入，避免在结构体内分支。
func newApplyPatcher(p *patcher, fieldManager *managedfields.FieldManager, unmarshalFn, unmarshalStrictFn func([]byte, interface{}) error) *applyPatcher {
	return &applyPatcher{
		fieldManager:        fieldManager,
		patch:               p.patchBytes,
		options:             p.options,
		creater:             p.creater,
		kind:                p.kind,
		userAgent:           p.userAgent,
		validationDirective: p.validationDirective,
		unmarshalFn:         unmarshalFn,
		unmarshalStrictFn:   unmarshalStrictFn,
	}
}

// applyPatcher 处理服务器端 apply（YAML/CBOR），语义是字段所有权合并。
type applyPatcher struct {
	patch               []byte
	options             *metav1.PatchOptions
	creater             runtime.ObjectCreater
	kind                schema.GroupVersionKind
	fieldManager        *managedfields.FieldManager
	userAgent           string
	validationDirective string
	unmarshalFn         func(data []byte, v any) error
	unmarshalStrictFn   func(data []byte, v any) error
}

// applyPatchToCurrentObject 解析 apply configuration 并交给字段管理器合并。
// force 来自 query 参数，用于强制接管他人拥有的字段；这是有意的破坏性操作，需显式请求。
func (p *applyPatcher) applyPatchToCurrentObject(requestContext context.Context, obj runtime.Object) (runtime.Object, error) {
	force := false
	if p.options.Force != nil {
		force = *p.options.Force
	}
	if p.fieldManager == nil {
		panic("FieldManager must be installed to run apply")
	}

	patchObj := &unstructured.Unstructured{Object: map[string]any{}}
	if err := p.unmarshalFn(p.patch, &patchObj.Object); err != nil {
		return nil, errors.NewBadRequest(fmt.Sprintf("error decoding patch: %v", err))
	}

	obj, err := p.fieldManager.Apply(obj, patchObj, p.options.FieldManager, force)
	if err != nil {
		return obj, err
	}

	// TODO: spawn something to track deciding whether a fieldValidation=Strict
	// fatal error should return before an error from the apply operation
	//
	// 严格解码在 apply 之后单独做一次：apply configuration 目标类型是 unstructured，
	// 普通解码不会暴露未知字段问题。
	if p.validationDirective == metav1.FieldValidationStrict || p.validationDirective == metav1.FieldValidationWarn {
		if err := p.unmarshalStrictFn(p.patch, &map[string]any{}); err != nil {
			if p.validationDirective == metav1.FieldValidationStrict {
				return nil, errors.NewBadRequest(fmt.Sprintf("error strict decoding patch: %v", err))
			}
			addStrictDecodingWarnings(requestContext, []error{err})
		}
	}
	return obj, nil
}

// createNewObject 新建空对象后立即 apply，实现“不存在则创建”的 apply 语义。
func (p *applyPatcher) createNewObject(requestContext context.Context) (runtime.Object, error) {
	obj, err := p.creater.New(p.kind)
	if err != nil {
		return nil, fmt.Errorf("failed to create new object: %v", err)
	}
	return p.applyPatchToCurrentObject(requestContext, obj)
}

// strategicPatchObject applies a strategic merge patch of `patchBytes` to
// `originalObject` and stores the result in `objToUpdate`.
// It additionally returns the map[string]interface{} representation of the
// `originalObject` and `patchBytes`.
//
// NOTE: Both `originalObject` and `objToUpdate` are supposed to be versioned.
// 先把对象转成 map 再做策略合并：strategicpatch 需要 map 形态才能按 schema 处理列表合并键。
func strategicPatchObject(
	requestContext context.Context,
	defaulter runtime.ObjectDefaulter,
	originalObject runtime.Object,
	patchBytes []byte,
	objToUpdate runtime.Object,
	schemaReferenceObj runtime.Object,
	validationDirective string,
) error {
	originalObjMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(originalObject)
	if err != nil {
		return err
	}

	patchMap := make(map[string]interface{})
	var strictErrs []error
	if validationDirective == metav1.FieldValidationWarn || validationDirective == metav1.FieldValidationStrict {
		strictErrs, err = kjson.UnmarshalStrict(patchBytes, &patchMap)
		if err != nil {
			return errors.NewBadRequest(err.Error())
		}
	} else {
		if err = kjson.UnmarshalCaseSensitivePreserveInts(patchBytes, &patchMap); err != nil {
			return errors.NewBadRequest(err.Error())
		}
	}

	if err := applyPatchToObject(requestContext, defaulter, originalObjMap, patchMap, objToUpdate, schemaReferenceObj, strictErrs, validationDirective); err != nil {
		return err
	}
	return nil
}

// applyPatch is called every time GuaranteedUpdate asks for the updated object,
// and is given the currently persisted object as input.
// TODO: rename this function because the name implies it is related to applyPatcher
//
// 该函数会被 GuaranteedUpdate 在每次乐观并发冲突后重复调用，因此必须无副作用、
// 只基于传入的 currentObject 计算新对象。
func (p *patcher) applyPatch(ctx context.Context, _, currentObject runtime.Object) (objToUpdate runtime.Object, patchErr error) {
	// Make sure we actually have a persisted currentObject
	tracing.SpanFromContext(ctx).AddEvent("About to apply patch")
	currentObjectHasUID, err := hasUID(currentObject)
	if err != nil {
		return nil, err
	} else if !currentObjectHasUID {
		// 没有 UID 说明对象不存在（或未持久化），交给机制决定能否创建。
		objToUpdate, patchErr = p.mechanism.createNewObject(ctx)
	} else {
		objToUpdate, patchErr = p.mechanism.applyPatchToCurrentObject(ctx, currentObject)
	}

	if patchErr != nil {
		return nil, patchErr
	}

	objToUpdateHasUID, err := hasUID(objToUpdate)
	if err != nil {
		return nil, err
	}
	// 对象原本不存在，但 patch 却指定了 UID：说明客户端在试图“复活”一个已知对象，必须拒绝。
	if objToUpdateHasUID && !currentObjectHasUID {
		accessor, err := meta.Accessor(objToUpdate)
		if err != nil {
			return nil, err
		}
		return nil, errors.NewConflict(p.resource.GroupResource(), p.name, fmt.Errorf("uid mismatch: the provided object specified uid %s, and no existing object was found", accessor.GetUID()))
	}

	// if this object supports namespace info
	if objectMeta, err := meta.Accessor(objToUpdate); err == nil {
		// ensure namespace on the object is correct, or error if a conflicting namespace was set in the object
		if err := rest.EnsureObjectNamespaceMatchesRequestNamespace(rest.ExpectedNamespaceForResource(p.namespace, p.resource), objectMeta); err != nil {
			return nil, err
		}
	}

	// patch 不允许修改对象名；名字与 URL 不一致会破坏资源寻址。
	if err := checkName(objToUpdate, p.name, p.namespace, p.namer); err != nil {
		return nil, err
	}
	return objToUpdate, nil
}

// admissionAttributes 构造本次 patch 的准入属性；create 与 update 的 oldObject 不同。
func (p *patcher) admissionAttributes(ctx context.Context, updatedObject runtime.Object, currentObject runtime.Object, operation admission.Operation, operationOptions runtime.Object) admission.Attributes {
	userInfo, _ := request.UserFrom(ctx)
	return admission.NewAttributesRecord(updatedObject, currentObject, p.kind, p.namespace, p.name, p.resource, p.subresource, operation, operationOptions, p.dryRun, userInfo)
}

// applyAdmission is called every time GuaranteedUpdate asks for the updated object,
// and is given the currently persisted object and the patched object as input.
// TODO: rename this function because the name implies it is related to applyPatcher
//
// 根据对象是否已存在选择 Create/Update 准入语义，保证 patch 触发的创建也经过 create 准入。
func (p *patcher) applyAdmission(ctx context.Context, patchedObject runtime.Object, currentObject runtime.Object) (runtime.Object, error) {
	tracing.SpanFromContext(ctx).AddEvent("About to check admission control")
	var operation admission.Operation
	var options runtime.Object
	if hasUID, err := hasUID(currentObject); err != nil {
		return nil, err
	} else if !hasUID {
		operation = admission.Create
		currentObject = nil
		options = patchToCreateOptions(p.options)
	} else {
		operation = admission.Update
		options = patchToUpdateOptions(p.options)
	}
	if p.admissionCheck != nil && p.admissionCheck.Handles(operation) {
		attributes := p.admissionAttributes(ctx, patchedObject, currentObject, operation, options)
		return patchedObject, p.admissionCheck.Admit(ctx, attributes, p.objectInterfaces)
	}
	return patchedObject, nil
}

// patchResource divides PatchResource for easier unit testing
//
// 核心流程：按 patch 类型选定 mechanism，组装 transformer 链（应用 patch → 准入 → 去重 ownerReferences），
// 然后交给 restPatcher.Update 在存储层带重试地执行。
func (p *patcher) patchResource(ctx context.Context, scope *RequestScope) (runtime.Object, bool, error) {
	p.namespace = request.NamespaceValue(ctx)
	switch p.patchType {
	case types.JSONPatchType, types.MergePatchType:
		p.mechanism = &jsonPatcher{
			patcher:      p,
			fieldManager: scope.FieldManager,
		}
	case types.StrategicMergePatchType:
		// 策略合并需要一份同类型的“空对象”作为 schema 参考。
		schemaReferenceObj, err := p.unsafeConvertor.ConvertToVersion(p.restPatcher.New(), p.kind.GroupVersion())
		if err != nil {
			return nil, false, err
		}
		p.mechanism = &smpPatcher{
			patcher:            p,
			schemaReferenceObj: schemaReferenceObj,
			fieldManager:       scope.FieldManager,
		}
	// this case is unreachable if ServerSideApply is not enabled because we will have already rejected the content type
	case types.ApplyYAMLPatchType:
		p.mechanism = newApplyPatcher(p, scope.FieldManager, yaml.Unmarshal, yaml.UnmarshalStrict)
		p.forceAllowCreate = true
	case types.ApplyCBORPatchType:
		if !utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
			utilruntime.HandleErrorWithContext(context.TODO(), nil, "CBOR apply requests should be rejected before reaching this point unless the feature gate is enabled.")
			return nil, false, fmt.Errorf("%v: unimplemented patch type", p.patchType)
		}

		// The strict and non-strict funcs are the same here because any CBOR map with
		// duplicate keys is invalid and always rejected outright regardless of strictness
		// mode, and unknown field errors can't occur in practice because the type of the
		// destination value for unmarshaling an apply configuration is always
		// "unstructured".
		p.mechanism = newApplyPatcher(p, scope.FieldManager, cbor.Unmarshal, cbor.Unmarshal)
		p.forceAllowCreate = true
	default:
		return nil, false, fmt.Errorf("%v: unimplemented patch type", p.patchType)
	}
	// ownerReferences 去重放在准入之后：准入可能注入重复引用，只有最终对象才值得清理。
	dedupOwnerReferencesTransformer := func(_ context.Context, obj, _ runtime.Object) (runtime.Object, error) {
		// Dedup owner references after mutating admission happens
		dedupOwnerReferencesAndAddWarning(obj, ctx, true)
		return obj, nil
	}

	transformers := []rest.TransformFunc{p.applyPatch, p.applyAdmission, dedupOwnerReferencesTransformer}

	wasCreated := false
	p.updatedObjectInfo = rest.DefaultUpdatedObjectInfo(nil, transformers...)
	requestFunc := func() (runtime.Object, error) {
		// Pass in UpdateOptions to override UpdateStrategy.AllowUpdateOnCreate
		options := patchToUpdateOptions(p.options)
		updateObject, created, updateErr := p.restPatcher.Update(ctx, p.name, p.updatedObjectInfo, p.createValidation, p.updateValidation, p.forceAllowCreate, options)
		wasCreated = created
		return updateObject, updateErr
	}
	result, err := finisher.FinishRequest(ctx, func() (runtime.Object, error) {

		result, err := requestFunc()
		// If the object wasn't committed to storage because it's serialized size was too large,
		// it is safe to remove managedFields (which can be large) and try again.
		//
		// 仅对非 apply patch 重试：apply 的 managedFields 是所有权契约的一部分，不能随意丢弃。
		if isTooLargeError(err) && p.patchType != types.ApplyYAMLPatchType && p.patchType != types.ApplyCBORPatchType {
			if _, accessorErr := meta.Accessor(p.restPatcher.New()); accessorErr == nil {
				p.updatedObjectInfo = rest.DefaultUpdatedObjectInfo(nil,
					p.applyPatch,
					p.applyAdmission,
					dedupOwnerReferencesTransformer,
					func(_ context.Context, obj, _ runtime.Object) (runtime.Object, error) {
						accessor, _ := meta.Accessor(obj)
						accessor.SetManagedFields(nil)
						return obj, nil
					})
				result, err = requestFunc()
			}
		}
		return result, err
	})

	// In case of a timeout error, the goroutine handling the request is still running.
	// https://github.com/kubernetes/kubernetes/blob/d2c12afa4593e50a187075157d38748292b02733/staging/src/k8s.io/apiserver/pkg/endpoints/handlers/finisher/finisher.go#L127-L146
	// We cannot reliably read the variable (data race!) and have to assume that
	// the object was not created.
	//
	// 超时后写协程仍在运行，读取 wasCreated 会有数据竞争，因此保守地返回“未创建”。
	if errors.IsTimeout(err) {
		return result, false, err
	}
	return result, wasCreated, err
}

// applyPatchToObject applies a strategic merge patch of <patchMap> to
// <originalMap> and stores the result in <objToUpdate>.
// NOTE: <objToUpdate> must be a versioned object.
//
// 直接做 map→object 的转换而非 JSON 往返，减少一次序列化；未知字段是否返回取决于校验指令。
func applyPatchToObject(
	requestContext context.Context,
	defaulter runtime.ObjectDefaulter,
	originalMap map[string]interface{},
	patchMap map[string]interface{},
	objToUpdate runtime.Object,
	schemaReferenceObj runtime.Object,
	strictErrs []error,
	validationDirective string,
) error {
	patchedObjMap, err := strategicpatch.StrategicMergeMapPatch(originalMap, patchMap, schemaReferenceObj)
	if err != nil {
		return interpretStrategicMergePatchError(err)
	}

	// Rather than serialize the patched map to JSON, then decode it to an object, we go directly from a map to an object
	converter := runtime.DefaultUnstructuredConverter
	returnUnknownFields := validationDirective == metav1.FieldValidationWarn || validationDirective == metav1.FieldValidationStrict
	if err := converter.FromUnstructuredWithValidation(patchedObjMap, objToUpdate, returnUnknownFields); err != nil {
		strictError, isStrictError := runtime.AsStrictDecodingError(err)
		switch {
		case !isStrictError:
			// disregard any sttrictErrs, because it's an incomplete
			// list of strict errors given that we don't know what fields were
			// unknown because StrategicMergeMapPatch failed.
			// Non-strict errors trump in this case.
			return errors.NewInvalid(schema.GroupKind{}, "", field.ErrorList{
				field.Invalid(field.NewPath("patch"), fmt.Sprintf("%+v", patchMap), err.Error()),
			})
		case validationDirective == metav1.FieldValidationWarn:
			addStrictDecodingWarnings(requestContext, append(strictErrs, strictError.Errors()...))
		default:
			strictDecodingError := runtime.NewStrictDecodingError(append(strictErrs, strictError.Errors()...))
			return errors.NewInvalid(schema.GroupKind{}, "", field.ErrorList{
				field.Invalid(field.NewPath("patch"), fmt.Sprintf("%+v", patchMap), strictDecodingError.Error()),
			})
		}
	} else if len(strictErrs) > 0 {
		switch {
		case validationDirective == metav1.FieldValidationWarn:
			addStrictDecodingWarnings(requestContext, strictErrs)
		default:
			return errors.NewInvalid(schema.GroupKind{}, "", field.ErrorList{
				field.Invalid(field.NewPath("patch"), fmt.Sprintf("%+v", patchMap), runtime.NewStrictDecodingError(strictErrs).Error()),
			})
		}
	}

	// Decoding from JSON to a versioned object would apply defaults, so we do the same here
	defaulter.Default(objToUpdate)

	return nil
}

// interpretStrategicMergePatchError interprets the error type and returns an error with appropriate HTTP code.
//
// 区分“客户端 patch 格式错误”（400）与“patch 与对象语义冲突”（422），
// 便于客户端决定是修正请求还是重试。
func interpretStrategicMergePatchError(err error) error {
	switch err {
	case mergepatch.ErrBadJSONDoc, mergepatch.ErrBadPatchFormatForPrimitiveList, mergepatch.ErrBadPatchFormatForRetainKeys, mergepatch.ErrBadPatchFormatForSetElementOrderList, mergepatch.ErrUnsupportedStrategicMergePatchFormat:
		return errors.NewBadRequest(err.Error())
	case mergepatch.ErrNoListOfLists, mergepatch.ErrPatchContentNotMatchRetainKeys:
		return errors.NewGenericServerResponse(http.StatusUnprocessableEntity, "", schema.GroupResource{}, "", err.Error(), 0, false)
	default:
		return err
	}
}

// patchToUpdateOptions creates an UpdateOptions with the same field values as the provided PatchOptions.
//
// patch 触发的更新需要把 dryRun/fieldManager/fieldValidation 原样传给存储与准入层。
func patchToUpdateOptions(po *metav1.PatchOptions) *metav1.UpdateOptions {
	if po == nil {
		return nil
	}
	uo := &metav1.UpdateOptions{
		DryRun:          po.DryRun,
		FieldManager:    po.FieldManager,
		FieldValidation: po.FieldValidation,
	}
	uo.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("UpdateOptions"))
	return uo
}

// patchToCreateOptions creates an CreateOptions with the same field values as the provided PatchOptions.
func patchToCreateOptions(po *metav1.PatchOptions) *metav1.CreateOptions {
	if po == nil {
		return nil
	}
	co := &metav1.CreateOptions{
		DryRun:          po.DryRun,
		FieldManager:    po.FieldManager,
		FieldValidation: po.FieldValidation,
	}
	co.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("CreateOptions"))
	return co
}

// validateAndTranscodePatch 校验 patch 载荷并转换为 JSON，用于审计。
// 在记录审计之前完成校验：保证审计里只会出现合法 JSON，非法载荷直接拒绝请求。
func validateAndTranscodePatch(patchBytes []byte, patchType types.PatchType) ([]byte, error) {
	switch patchType {
	case types.ApplyCBORPatchType:
		var obj any
		if err := cbor.Unmarshal(patchBytes, &obj); err != nil {
			return nil, errors.NewBadRequest(fmt.Sprintf("error decoding patch: %v", err))
		}
		jsonBytes, err := json.Marshal(obj)
		if err != nil {
			return nil, errors.NewBadRequest(fmt.Sprintf("error encoding patch: %v", err))
		}
		return jsonBytes, nil
	case types.ApplyYAMLPatchType:
		jsonBytes, err := yaml.ToJSON(patchBytes)
		if err != nil {
			return nil, errors.NewBadRequest(fmt.Sprintf("error encoding patch: %v", err))
		}
		return jsonBytes, nil
	default:
		if !json.Valid(patchBytes) {
			return nil, errors.NewBadRequest("invalid JSON patch")
		}
		return patchBytes, nil
	}
}
