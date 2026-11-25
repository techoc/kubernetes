/*
Copyright 2014 The Kubernetes Authors.

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
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	requestmetrics "k8s.io/apiserver/pkg/endpoints/handlers/metrics"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/warning"
)

const (
	// 选 34 是因为它接近 30 又足够特别，下次看到超时时能让我一眼认出来。
	// 大家都选 30。
	// 请求超时上界：略大于常见的 30s 客户端超时，留出处理与写回余量。
	requestTimeoutUpperBound = 34 * time.Second
	// DuplicateOwnerReferencesWarningFormat 是当 create/update 请求包含重复的 owner reference
	// 条目时客户端收到的警告。
	DuplicateOwnerReferencesWarningFormat = ".metadata.ownerReferences contains duplicate entries; API server dedups owner references in 1.20+, and may reject such requests as early as 1.24; please fix your requests; duplicate UID(s) observed: %v"
	// DuplicateOwnerReferencesAfterMutatingAdmissionWarningFormat 表明重复是在变更准入之后被发现的。
	// 注意：对 CREATE 和 UPDATE 请求，API Server 会在变更准入前后各去重一次；
	// 对 PATCH 请求只在变更准入之后去重。
	DuplicateOwnerReferencesAfterMutatingAdmissionWarningFormat = ".metadata.ownerReferences contains duplicate entries after mutating admission happens; API server dedups owner references in 1.20+, and may reject such requests as early as 1.24; please fix your requests; duplicate UID(s) observed: %v"
	// shortPrefix 是 yaml 严格解码错误的一种可能前缀。
	shortPrefix = "yaml: unmarshal errors:\n"
	// longPrefix 是 yaml 严格解码错误的另一种可能前缀。
	longPrefix = "error converting YAML to JSON: yaml: unmarshal errors:\n"
)

// RequestScope 汇总所有 RESTful handler 方法共用的字段。
// 它是各 verb 处理器的共享配置：编解码、授权、字段管理、请求体上限等。
// 构造后视为只读，可被所有请求处理协程并发使用。
type RequestScope struct {
	Namer ScopeNamer

	Serializer runtime.NegotiatedSerializer
	runtime.ParameterCodec

	// 若设置了 StandardSerializers，则在不对输出做转换（转成 Table 或 PartialObjectMetadata）时，
	// 用它限制可用的序列化器。目前仅由尚不支持 Protobuf 的 CRD 使用。
	StandardSerializers []runtime.SerializerInfo

	Creater         runtime.ObjectCreater
	Convertor       runtime.ObjectConvertor
	Defaulter       runtime.ObjectDefaulter
	Typer           runtime.ObjectTyper
	UnsafeConvertor runtime.ObjectConvertor
	Authorizer      authorizer.UnconditionalAuthorizer

	EquivalentResourceMapper runtime.EquivalentResourceMapper

	TableConvertor rest.TableConvertor
	FieldManager   *managedfields.FieldManager

	Resource schema.GroupVersionResource
	Kind     schema.GroupVersionKind

	// AcceptsGroupVersionDelegate 是一个可选的委托对象，可询问某个 GVK 是否能被
	// create 或 update 请求接受。为 nil 时只接受 scope.Kind。
	// 注意：这不会让同一端点支持读取多个版本。
	AcceptsGroupVersionDelegate rest.GroupVersionAcceptor

	Subresource string

	MetaGroupVersion schema.GroupVersion

	// HubGroupVersion 指明从 etcd 读出的对象或传入请求的对象在内存中处理时应转换到的版本。
	HubGroupVersion schema.GroupVersion

	MaxRequestBodyBytes int64
}

// err 以请求的协商格式写出错误响应。
func (scope *RequestScope) err(err error, w http.ResponseWriter, req *http.Request) {
	responsewriters.ErrorNegotiated(err, scope.Serializer, scope.Kind.GroupVersion(), w, req)
}

// AcceptsGroupVersion 返回指定的 GroupVersion 是否被 create 和 update 请求允许。
func (scope *RequestScope) AcceptsGroupVersion(gv schema.GroupVersion) bool {
	// 若存在自定义 acceptor，则委托给它，这种情况极为罕见。
	if scope.AcceptsGroupVersionDelegate != nil {
		return scope.AcceptsGroupVersionDelegate.AcceptsGroupVersion(gv)
	}
	// 回退为只允许那一个 Kind，这是最常见的行为。
	// 默认单版本：只接受端点自身注册的版本，避免隐式跨版本写入。
	return gv == scope.Kind.GroupVersion()
}

// AllowsMediaTypeTransform 判断是否允许把对象转换为 Table / PartialObjectMetadata 等元数据格式。
func (scope *RequestScope) AllowsMediaTypeTransform(mimeType, mimeSubType string, gvk *schema.GroupVersionKind) bool {
	// 某些 handler（如 CRD）无法提供 PartialObjectMetadata 或 Table 支持的全部 MIME 类型：
	// gvk 为 nil 表示不做转换，此时由 StandardSerializers 进一步限制可用的 MIME 集合。
	if gvk == nil {
		if len(scope.StandardSerializers) == 0 {
			return true
		}
		for _, info := range scope.StandardSerializers {
			if info.MediaTypeType == mimeType && info.MediaTypeSubType == mimeSubType {
				return true
			}
		}
		return false
	}

	// TODO: 这是临时实现，将来应替换为在端点安装期计算好的抽象。
	// 仅允许 meta.k8s.io 的两个版本下的元数据类转换，其他 GVK 一律拒绝。
	if gvk.GroupVersion() == metav1beta1.SchemeGroupVersion || gvk.GroupVersion() == metav1.SchemeGroupVersion {
		switch gvk.Kind {
		case "Table":
			// Table 需要注册了转换器，且只支持 JSON/YAML。
			return scope.TableConvertor != nil &&
				mimeType == "application" &&
				(mimeSubType == "json" || mimeSubType == "yaml")
		case "PartialObjectMetadata", "PartialObjectMetadataList":
			// TODO: 应当区分 list 端点与非 list 端点
			return true
		default:
			return false
		}
	}
	return false
}

// AllowsServerVersion 校验请求的 Accept 中 server 版本是否为端点的 meta 版本。
func (scope *RequestScope) AllowsServerVersion(version string) bool {
	return version == scope.MetaGroupVersion.Version
}

// AllowsStreamSchema 目前只支持 watch 一种流式 schema。
func (scope *RequestScope) AllowsStreamSchema(s string) bool {
	return s == "watch"
}

// 编译期断言：RequestScope 可作为准入插件所需的对象接口集合。
var _ admission.ObjectInterfaces = &RequestScope{}

// 分别暴露 scope 配置的构造器、类型解析器、默认值填充器与版本转换器；GetEquivalentResourceMapper
// 分别暴露 scope 配置的构造器、类型解析器、默认值填充器与版本转换器；
// GetEquivalentResourceMapper 返回等价资源映射器，供准入插件定位同义资源。
func (r *RequestScope) GetObjectCreater() runtime.ObjectCreater     { return r.Creater }
func (r *RequestScope) GetObjectTyper() runtime.ObjectTyper         { return r.Typer }
func (r *RequestScope) GetObjectDefaulter() runtime.ObjectDefaulter { return r.Defaulter }
func (r *RequestScope) GetObjectConvertor() runtime.ObjectConvertor { return r.Convertor }
func (r *RequestScope) GetEquivalentResourceMapper() runtime.EquivalentResourceMapper {
	return r.EquivalentResourceMapper
}

// ConnectResource 返回处理 rest.Storage 对象上 connect 请求的函数。
func ConnectResource(connecter rest.Connecter, scope *RequestScope, admit admission.Interface, restPath string, isSubresource bool) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		// CONNECT 语义（流式、双向）与 dry-run 不兼容，直接拒绝。
		if isDryRun(req.URL) {
			scope.err(errors.NewBadRequest("dryRun is not supported"), w, req)
			return
		}

		namespace, name, err := scope.Namer.Name(req)
		if err != nil {
			scope.err(err, w, req)
			return
		}
		ctx := req.Context()
		ctx = request.WithNamespace(ctx, namespace)
		admit = admission.WithAudit(admit)

		opts, subpath, subpathKey := connecter.NewConnectOptions()
		// 选项从查询参数解码；解码失败一律按客户端错误处理。
		if err := getRequestOptions(req, scope, opts, subpath, subpathKey, isSubresource); err != nil {
			err = errors.NewBadRequest(err.Error())
			scope.err(err, w, req)
			return
		}
		if admit != nil && admit.Handles(admission.Connect) {
			userInfo, _ := request.UserFrom(ctx)
			// TODO: 等所有处理 CONNECT 的插件迁移完成后，移除这里的变更准入。
			// 变更准入可能修改连接选项（如 exec 命令），因此必须在校验准入之前执行。
			if mutatingAdmission, ok := admit.(admission.MutationInterface); ok {
				err = mutatingAdmission.Admit(ctx, admission.NewAttributesRecord(opts, nil, scope.Kind, namespace, name, scope.Resource, scope.Subresource, admission.Connect, nil, false, userInfo), scope)
				if err != nil {
					scope.err(err, w, req)
					return
				}
			}
			if validatingAdmission, ok := admit.(admission.ValidationInterface); ok {
				err = validatingAdmission.Validate(ctx, admission.NewAttributesRecord(opts, nil, scope.Kind, namespace, name, scope.Resource, scope.Subresource, admission.Connect, nil, false, userInfo), scope)
				if err != nil {
					scope.err(err, w, req)
					return
				}
			}
		}
		requestInfo, _ := request.RequestInfoFrom(ctx)
		// CONNECT 是长连接，指标需单独记录持续时长而非普通请求延迟。
		metrics.RecordLongRunning(req, requestInfo, metrics.APIServerComponent, func() {
			handler, err := connecter.Connect(ctx, name, opts, &responder{scope: scope, req: req, w: w})
			if err != nil {
				scope.err(err, w, req)
				return
			}
			handler.ServeHTTP(w, req)
		})
	}
}

// responder 实现 rest.Responder，协助 connector 写出对象或错误。
type responder struct {
	scope *RequestScope
	req   *http.Request
	w     http.ResponseWriter
}

// Object 按协商格式写回连接建立结果。
func (r *responder) Object(statusCode int, obj runtime.Object) {
	responsewriters.WriteObjectNegotiated(r.scope.Serializer, r.scope, r.scope.Kind.GroupVersion(), r.w, r.req, statusCode, obj, false)
}

// Error 写回连接建立失败。
func (r *responder) Error(err error) {
	r.scope.err(err, r.w, r.req)
}

// transformDecodeError 在解码失败时，往 bad-request 的 API 错误中补充额外信息。
// 把底层解码错误包装为对客户端可理解的 BadRequest，并在正文非 JSON 时用十六进制摘要。
func transformDecodeError(typer runtime.ObjectTyper, baseErr error, into runtime.Object, gvk *schema.GroupVersionKind, body []byte) error {
	objGVKs, _, err := typer.ObjectKinds(into)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	objGVK := objGVKs[0]
	if gvk != nil && len(gvk.Kind) > 0 {
		// 请求声明的 GVK 与端点期望不一致，提示版本层面的原因。
		return errors.NewBadRequest(fmt.Sprintf("%s in version %q cannot be handled as a %s: %v", gvk.Kind, gvk.Version, objGVK.Kind, baseErr))
	}
	summary := summarizeData(body, 30)
	return errors.NewBadRequest(fmt.Sprintf("the object provided is unrecognized (must be of type %s): %v (%s)", objGVK.Kind, baseErr, summary))
}

// hasUID 判断对象是否已带 UID；用于区分创建与更新语义。
func hasUID(obj runtime.Object) (bool, error) {
	if obj == nil {
		return false, nil
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return false, errors.NewInternalError(err)
	}
	if len(accessor.GetUID()) == 0 {
		return false, nil
	}
	return true, nil
}

// checkName 校验请求体中提供的 name 是否与请求 URL 一致。
// 防止请求体中的 metadata.name/namespace 与 URL 不一致，避免越权写入其他对象。
func checkName(obj runtime.Object, name, namespace string, namer ScopeNamer) error {
	objNamespace, objName, err := namer.ObjectName(obj)
	if err != nil {
		return errors.NewBadRequest(fmt.Sprintf(
			"the name of the object (%s based on URL) was undeterminable: %v", name, err))
	}
	if objName != name {
		return errors.NewBadRequest(fmt.Sprintf(
			"the name of the object (%s) does not match the name on the URL (%s)", objName, name))
	}
	if len(namespace) > 0 {
		// 允许请求体不带 namespace（由 URL 补全），但不允许与 URL 冲突。
		if len(objNamespace) > 0 && objNamespace != namespace {
			return errors.NewBadRequest(fmt.Sprintf(
				"the namespace of the object (%s) does not match the namespace on the request (%s)", objNamespace, namespace))
		}
	}

	return nil
}

// dedupOwnerReferences 对整个条目列表做 owner references 去重。
// 注意：我们对“UID 相同但字段不同”的既有情况了解得还不够，
// 也不清楚改动会破坏什么，因此将来才可能对相同 UID 直接去重/拒绝。
//
// 仅去除“完全语义相同”的重复项；同 UID 但字段不同的条目暂时保留，以免破坏既有工作负载。
func dedupOwnerReferences(refs []metav1.OwnerReference) ([]metav1.OwnerReference, []string) {
	var result []metav1.OwnerReference
	var duplicates []string
	seen := make(map[types.UID]struct{})
	for _, ref := range refs {
		_, ok := seen[ref.UID]
		// 若该 UID 首次出现则直接短路；否则与已有的整个列表逐一比较。
		// 常见路径 O(1)；只有 UID 重复时才做 O(n) 语义比较。
		if !ok || !hasOwnerReference(result, ref) {
			seen[ref.UID] = struct{}{}
			result = append(result, ref)
		} else {
			duplicates = append(duplicates, string(ref.UID))
		}
	}
	return result, duplicates
}

// hasOwnerReference 判断 refs 中是否存在与 ref 相等的条目。该函数关注语义相等
// 而非内存相等，以便捕获指针地址不同的重复项。它使用 apiequality.Semantic
// 而不是自行实现比较逻辑，从而能容忍 metav1.OwnerReference 的 API 变化。
// 注意：这个比较开销较大，但我们接受它，因为它只在包含重复 UID 的 owner references
// 上才会执行，而且通常列表中的条目数量很少。
func hasOwnerReference(refs []metav1.OwnerReference, ref metav1.OwnerReference) bool {
	for _, r := range refs {
		if apiequality.Semantic.DeepEqual(r, ref) {
			return true
		}
	}
	return false
}

// dedupOwnerReferencesAndAddWarning 对对象 metadata 中的 owner references 去重。
// 若发现重复项，函数会向给定的 context 记录一条警告。
func dedupOwnerReferencesAndAddWarning(obj runtime.Object, requestContext context.Context, afterMutatingAdmission bool) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		// 对象没有 metadata，无需处理。
		return
	}
	refs := accessor.GetOwnerReferences()
	deduped, duplicates := dedupOwnerReferences(refs)
	if len(duplicates) > 0 {
		// 注意：对 CREATE 和 UPDATE 请求，API Server 会在变更准入前后各去重一次。
		// 对 PATCH 请求则只在变更准入之后去重。
		// 根据阶段选择不同警告文案，便于客户端定位是请求本身还是变更准入引入的重复。
		format := DuplicateOwnerReferencesWarningFormat
		if afterMutatingAdmission {
			format = DuplicateOwnerReferencesAfterMutatingAdmissionWarningFormat
		}
		warning.AddWarning(requestContext, "", fmt.Sprintf(format,
			strings.Join(duplicates, ", ")))
		accessor.SetOwnerReferences(deduped)
	}
}

// summarizeData 为错误信息生成请求体摘要：JSON 直接截断，二进制用十六进制，
// 避免把不可打印内容或过长的正文塞进错误响应。
func summarizeData(data []byte, maxLength int) string {
	switch {
	case len(data) == 0:
		return "<empty>"
	case data[0] == '{':
		if len(data) > maxLength {
			return string(data[:maxLength]) + " ..."
		}
		return string(data)
	default:
		if len(data) > maxLength {
			return hex.EncodeToString(data[:maxLength]) + " ..."
		}
		return hex.EncodeToString(data)
	}
}

// limitedReadBody 读取请求体并强制大小上限；limit<=0 表示不限制。
// 多读 1 字节以区分“恰好等于上限”和“超出上限”。
func limitedReadBody(req *http.Request, limit int64) ([]byte, error) {
	defer req.Body.Close()
	if limit <= 0 {
		return io.ReadAll(req.Body)
	}
	lr := &io.LimitedReader{
		R: req.Body,
		N: limit + 1,
	}
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if lr.N <= 0 {
		return nil, errors.NewRequestEntityTooLargeError(fmt.Sprintf("limit is %d", limit))
	}
	return data, nil
}

// limitedReadBodyWithRecordMetric 在读取成功时记录请求体大小指标；
// 失败时记录无意义，因此只在 err == nil 时上报。
func limitedReadBodyWithRecordMetric(ctx context.Context, req *http.Request, limit int64, groupResource schema.GroupResource, verb requestmetrics.RequestBodyVerb) ([]byte, error) {
	readBody, err := limitedReadBody(req, limit)
	if err == nil {
		// 只在读取成功时记录。
		requestmetrics.RecordRequestBodySize(ctx, groupResource, verb, len(readBody))
	}
	return readBody, err
}

// isDryRun 只要出现 dryRun 查询参数即视为 dry-run（含 dryRun=false 的历史语义）。
func isDryRun(url *url.URL) bool {
	return len(url.Query()["dryRun"]) != 0
}

// fieldValidation 在字段校验特性开启时，返回下列合法指令之一：
// - Ignore
// - Warn（默认）
// - Strict
func fieldValidation(directive string) string {
	if directive == "" {
		return metav1.FieldValidationWarn
	}
	return directive
}

// parseYAMLWarnings 从 yaml 解码器输出的严格解码错误中解析出一条条警告；
// 若不像 yaml 严格解码错误，则原样保留该警告。
func parseYAMLWarnings(errString string) []string {
	var trimmedString string
	// 两种前缀对应不同解码路径；用长度变化判断 TrimPrefix 是否命中。
	if trimmedShortString := strings.TrimPrefix(errString, shortPrefix); len(trimmedShortString) < len(errString) {
		trimmedString = trimmedShortString
	} else if trimmedLongString := strings.TrimPrefix(errString, longPrefix); len(trimmedLongString) < len(errString) {
		trimmedString = trimmedLongString
	} else {
		// 不是 yaml 错误，原样返回。
		return []string{errString}
	}

	// 每个违规项单独成行，去掉缩进后作为独立 warning 返回。
	splitStrings := strings.Split(trimmedString, "\n")
	for i, s := range splitStrings {
		splitStrings[i] = strings.TrimSpace(s)
	}
	return splitStrings
}

// addStrictDecodingWarnings 确认错误是否为严格解码（strict decoding）错误，
// 若是则把每条严格解码违规各自作为一条警告加入上下文。
func addStrictDecodingWarnings(requestContext context.Context, errs []error) {
	for _, e := range errs {
		yamlWarnings := parseYAMLWarnings(e.Error())
		for _, w := range yamlWarnings {
			warning.AddWarning(requestContext, "", w)
		}
	}
}

// etcdError 抽象 etcd 客户端错误，仅暴露判断所需的方法。
type etcdError interface {
	Code() grpccodes.Code
	Error() string
}

// grpcError 抽象 gRPC 状态错误。
type grpcError interface {
	GRPCStatus() *grpcstatus.Status
}

// isTooLargeError 识别“对象超出存储大小限制”的错误，
// 以便上层转换为 413 而不是笼统的 500。
func isTooLargeError(err error) bool {
	if err != nil {
		if etcdErr, ok := err.(etcdError); ok {
			if etcdErr.Code() == grpccodes.InvalidArgument && etcdErr.Error() == "etcdserver: request is too large" {
				return true
			}
		}
		if grpcErr, ok := err.(grpcError); ok {
			if grpcErr.GRPCStatus().Code() == grpccodes.ResourceExhausted && strings.Contains(grpcErr.GRPCStatus().Message(), "trying to send message larger than max") {
				return true
			}
		}
	}
	return false
}
