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
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

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
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	"k8s.io/apiserver/pkg/endpoints/handlers/finisher"
	requestmetrics "k8s.io/apiserver/pkg/endpoints/handlers/metrics"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/util/dryrun"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
)

var namespaceGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}

// createHandler 构造处理 POST（创建）请求的 HTTP handler。
//
// 中文说明：includeName 区分 CreateNamedResource（URL 中带名字）与普通创建；
// 两者的差异仅在名字缺失时是报错还是回退到命名空间解析。
func createHandler(r rest.NamedCreater, scope *RequestScope, admit admission.Interface, includeName bool) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		// For performance tracking purposes.
		ctx, span := tracing.Start(ctx, "Create", traceFields(req)...)
		req = req.WithContext(ctx)
		// 500ms 的 span 截止时间只为限制追踪开销，不表示请求超时。
		defer span.End(500 * time.Millisecond)

		namespace, name, err := scope.Namer.Name(req)
		if err != nil {
			if includeName {
				// name was required, return
				scope.err(err, w, req)
				return
			}

			// otherwise attempt to look up the namespace
			// 中文说明：普通创建请求的 URL 只含命名空间，名字来自请求体。
			namespace, err = scope.Namer.Namespace(req)
			if err != nil {
				scope.err(err, w, req)
				return
			}
		}

		// enforce a timeout of at most requestTimeoutUpperBound (34s) or less if the user-provided
		// timeout inside the parent context is lower than requestTimeoutUpperBound.
		// 中文说明：父上下文若已更短，WithTimeout 不会延长它，因此这里只是设上限。
		ctx, cancel := context.WithTimeout(ctx, requestTimeoutUpperBound)
		defer cancel()
		outputMediaType, _, err := negotiation.NegotiateOutputMediaType(req, scope.Serializer, scope)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		gv := scope.Kind.GroupVersion()
		// 输入序列化器先协商，才能按客户端声明的 Content-Type 解码请求体。
		s, err := negotiation.NegotiateInputSerializer(req, false, scope.Serializer)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		// 读取请求体并限制大小，避免超大请求耗尽内存；读取量同时计入指标。
		body, err := limitedReadBodyWithRecordMetric(ctx, req, scope.MaxRequestBodyBytes, scope.Resource.GroupResource(), requestmetrics.Create)
		if err != nil {
			span.AddEvent("limitedReadBody failed", attribute.Int("len", len(body)), attribute.String("err", err.Error()))
			scope.err(err, w, req)
			return
		}
		span.AddEvent("limitedReadBody succeeded", attribute.Int("len", len(body)))

		options := &metav1.CreateOptions{}
		values := req.URL.Query()
		// CreateOptions 来自查询参数（如 dryRun、fieldValidation），而非请求体。
		if err := metainternalversionscheme.ParameterCodec.DecodeParameters(values, scope.MetaGroupVersion, options); err != nil {
			err = errors.NewBadRequest(err.Error())
			scope.err(err, w, req)
			return
		}
		if errs := validation.ValidateCreateOptions(options); len(errs) > 0 {
			err := errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "CreateOptions"}, "", errs)
			scope.err(err, w, req)
			return
		}
		options.TypeMeta.SetGroupVersionKind(metav1.SchemeGroupVersion.WithKind("CreateOptions"))

		defaultGVK := scope.Kind
		original := r.New()

		validationDirective := fieldValidation(options.FieldValidation)
		decodeSerializer := s.Serializer
		// strict 模式用严格解码器，未知字段直接失败；warn 模式同样检测未知字段，
		// 但把错误降级为告警。
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
				// warn 模式下对象仍可用，把未知字段写进 Warning 响应头。
				addStrictDecodingWarnings(req.Context(), strictError.Errors())
			case isStrictError && validationDirective == metav1.FieldValidationIgnore:
				// ignore 模式本不应产生严格错误，出现即说明内部状态不一致，记录后按普通错误处理。
				klog.Warningf("unexpected strict error when field validation is set to ignore")
				fallthrough
			default:
				err = transformDecodeError(scope.Typer, err, original, gvk, body)
				scope.err(err, w, req)
				return
			}
		}

		objGV := gvk.GroupVersion()
		// 拒绝请求体与 URL 版本不一致的对象，避免跨版本歧义。
		if !scope.AcceptsGroupVersion(objGV) {
			err = errors.NewBadRequest(fmt.Sprintf("the API version in the data (%s) does not match the expected API version (%v)", objGV.String(), gv.String()))
			scope.err(err, w, req)
			return
		}
		span.AddEvent("Conversion done")

		// On create, get name from new object if unset
		if len(name) == 0 {
			_, name, _ = scope.Namer.ObjectName(obj)
		}
		// namespace 资源自身的 namespace 等于其名字，这是 kube-apiserver 的特殊约定。
		if len(namespace) == 0 && scope.Resource == namespaceGVR {
			namespace = name
		}
		ctx = request.WithNamespace(ctx, namespace)

		// 包装审计是为了在准入链内部也能记录审计注解。
		admit = admission.WithAudit(admit)
		audit.LogRequestObject(req.Context(), obj, objGV, scope.Resource, scope.Subresource, scope.Serializer)

		userInfo, _ := request.UserFrom(ctx)

		if objectMeta, err := meta.Accessor(obj); err == nil {
			preserveObjectMetaSystemFields := false
			// 子资源创建（如 pods/eviction）可能需要保留请求中的 metadata，由资源自行决定。
			if c, ok := r.(rest.SubresourceObjectMetaPreserver); ok && len(scope.Subresource) > 0 {
				preserveObjectMetaSystemFields = c.PreserveRequestObjectMetaSystemFieldsOnSubresourceCreate()
			}
			if !preserveObjectMetaSystemFields {
				// 清除客户端伪造的 UID、resourceVersion、creationTimestamp 等系统字段。
				rest.WipeObjectMetaSystemFields(objectMeta)
			}

			// ensure namespace on the object is correct, or error if a conflicting namespace was set in the object
			if err := rest.EnsureObjectNamespaceMatchesRequestNamespace(rest.ExpectedNamespaceForResource(namespace, scope.Resource), objectMeta); err != nil {
				scope.err(err, w, req)
				return
			}
		}

		span.AddEvent("About to store object in database")
		admissionAttributes := admission.NewAttributesRecord(obj, nil, scope.Kind, namespace, name, scope.Resource, scope.Subresource, admission.Create, options, dryrun.IsDryRun(options.DryRun), userInfo)
		// 把存储调用封装为闭包，因为体积过大时需要用去掉 managedFields 的对象重试。
		requestFunc := func() (runtime.Object, error) {
			return r.Create(
				ctx,
				name,
				obj,
				rest.AdmissionToValidateObjectFunc(admit, admissionAttributes, scope),
				options,
			)
		}
		// Dedup owner references before updating managed fields
		// 中文说明：先清理一次，避免重复 ownerReference 影响 field manager 计算；
		// 变更准入后再清理一次，因为插件可能重新引入重复项。
		dedupOwnerReferencesAndAddWarning(obj, req.Context(), false)
		result, err := finisher.FinishRequest(ctx, func() (runtime.Object, error) {
			liveObj, err := scope.Creater.New(scope.Kind)
			if err != nil {
				return nil, fmt.Errorf("failed to create new object (Create for %v): %v", scope.Kind, err)
			}
			// 计算服务端字段管理（managedFields），并包装准入以校验 managedFields 合法性。
			obj = scope.FieldManager.UpdateNoErrors(liveObj, obj, managerOrUserAgent(options.FieldManager, req.UserAgent()))
			admit = fieldmanager.NewManagedFieldsValidatingAdmissionController(admit)

			if mutatingAdmission, ok := admit.(admission.MutationInterface); ok && mutatingAdmission.Handles(admission.Create) {
				if err := mutatingAdmission.Admit(ctx, admissionAttributes, scope); err != nil {
					return nil, err
				}
			}
			// Dedup owner references again after mutating admission happens
			dedupOwnerReferencesAndAddWarning(obj, req.Context(), true)
			result, err := requestFunc()
			// If the object wasn't committed to storage because it's serialized size was too large,
			// it is safe to remove managedFields (which can be large) and try again.
			// 中文说明：仅在超限错误时重试一次；managedFields 是可有可无的元数据，
			// 丢弃它比直接拒绝用户请求更友好。
			if isTooLargeError(err) {
				if accessor, accessorErr := meta.Accessor(obj); accessorErr == nil {
					accessor.SetManagedFields(nil)
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

		code := http.StatusCreated
		status, ok := result.(*metav1.Status)
		if ok && status.Code == 0 {
			// 存储层返回的 Status 未带状态码时补 201，保证响应符合创建语义。
			status.Code = int32(code)
		}

		span.AddEvent("About to write a response")
		defer span.AddEvent("Writing http response done")
		transformResponseObject(ctx, scope, req, w, code, outputMediaType, result)
	}
}

// CreateNamedResource returns a function that will handle a resource creation with name.
//
// 中文说明：用于名字出现在 URL 中的创建接口（如子资源或部分非标准资源）。
func CreateNamedResource(r rest.NamedCreater, scope *RequestScope, admission admission.Interface) http.HandlerFunc {
	return createHandler(r, scope, admission, true)
}

// CreateResource returns a function that will handle a resource creation.
//
// 中文说明：标准集合创建；通过适配器把 Creater 提升为 NamedCreater，以复用同一 handler。
func CreateResource(r rest.Creater, scope *RequestScope, admission admission.Interface) http.HandlerFunc {
	return createHandler(&namedCreaterAdapter{r}, scope, admission, false)
}

// namedCreaterAdapter 把只接受对象名的 Creater 适配为 NamedCreater。
//
// 中文说明：普通创建的路径中名字来自对象本身，适配器直接忽略传入的 name 参数。
type namedCreaterAdapter struct {
	rest.Creater
}

// Create 丢弃 name，委托给内嵌的 Creater。
func (c *namedCreaterAdapter) Create(ctx context.Context, name string, obj runtime.Object, createValidatingAdmission rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	return c.Creater.Create(ctx, obj, createValidatingAdmission, options)
}

// manager is assumed to be already a valid value, we need to make
// userAgent into a valid value too.
//
// 中文说明：显式 FieldManager 优先；否则从 User-Agent 派生，保证 managedFields
// 的 manager 字段总是合法值。
func managerOrUserAgent(manager, userAgent string) string {
	if manager != "" {
		return manager
	}
	return prefixFromUserAgent(userAgent)
}

// prefixFromUserAgent takes the characters preceding the first /, quote
// unprintable character and then trim what's beyond the
// FieldManagerMaxLength limit.
//
// 中文说明：截断按 rune 边界进行，避免把多字节字符切成非法 UTF-8；
// 同时过滤不可打印字符，防止污染 managedFields。
func prefixFromUserAgent(u string) string {
	m := strings.Split(u, "/")[0]
	buf := bytes.NewBuffer(nil)
	for _, r := range m {
		// Ignore non-printable characters
		if !unicode.IsPrint(r) {
			continue
		}
		// Only append if we have room for it
		// 中文说明：按编码后字节数判断，确保结果满足长度上限。
		if buf.Len()+utf8.RuneLen(r) > validation.FieldManagerMaxLength {
			break
		}
		buf.WriteRune(r)
	}
	return buf.String()
}
