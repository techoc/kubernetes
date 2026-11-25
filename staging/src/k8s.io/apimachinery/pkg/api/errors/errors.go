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

package errors

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// StatusError 是面向 REST API 服务端消费的错误类型，也能由客户端从 REST 响应中重建。
// 它刻意导出，方便调用方用类型断言或 errors.As 做类型分支。
type StatusError struct {
	ErrStatus metav1.Status
}

// APIStatus 由能够转换为 api.Status 对象的错误实现，
// 用于向调用方暴露更细粒度的状态细节，而无需依赖具体错误类型。
type APIStatus interface {
	Status() metav1.Status
}

// 编译期断言：*StatusError 一定实现 error 接口。
var _ error = &StatusError{}

// knownReasons 记录本包明确定义过的 StatusReason 集合。
// 各 Is* 函数在 reason 无法识别时才退回到按 HTTP 状态码判断；
// 加入这里的 reason 意味着"该语义已由 reason 唯一确定"，不应再仅凭状态码误判。
var knownReasons = map[metav1.StatusReason]struct{}{
	// StatusReasonUnknown 不列入已知原因：它代表"未能识别原因"，需要后续按状态码兜底判断。
	metav1.StatusReasonUnauthorized:          {},
	metav1.StatusReasonForbidden:             {},
	metav1.StatusReasonNotFound:              {},
	metav1.StatusReasonAlreadyExists:         {},
	metav1.StatusReasonConflict:              {},
	metav1.StatusReasonGone:                  {},
	metav1.StatusReasonInvalid:               {},
	metav1.StatusReasonServerTimeout:         {},
	metav1.StatusReasonStoreReadError:        {},
	metav1.StatusReasonTimeout:               {},
	metav1.StatusReasonTooManyRequests:       {},
	metav1.StatusReasonBadRequest:            {},
	metav1.StatusReasonMethodNotAllowed:      {},
	metav1.StatusReasonNotAcceptable:         {},
	metav1.StatusReasonRequestEntityTooLarge: {},
	metav1.StatusReasonUnsupportedMediaType:  {},
	metav1.StatusReasonInternalError:         {},
	metav1.StatusReasonExpired:               {},
	metav1.StatusReasonServiceUnavailable:    {},
}

// Error 实现 error 接口，返回 Status 中的 Message 字段。
func (e *StatusError) Error() string {
	return e.ErrStatus.Message
}

// Status 直接暴露底层 Status，调用方无需了解 StatusError 的内部结构即可读取细节。
func (e *StatusError) Status() metav1.Status {
	return e.ErrStatus
}

// DebugError 返回用于调试的扩展信息：优先使用缩进后的 JSON，
// JSON 序列化失败时退回 %#v 的形式，保证总有可读输出。
func (e *StatusError) DebugError() (string, []interface{}) {
	if out, err := json.MarshalIndent(e.ErrStatus, "", "  "); err == nil {
		return "server response object: %s", []interface{}{string(out)}
	}
	return "server response object: %#v", []interface{}{e.ErrStatus}
}

// HasStatusCause 判断给定错误是否带有指定类型的 details cause。
// 它支持包装错误（errors.As），并在 err 为 nil 时返回 false。
func HasStatusCause(err error, name metav1.CauseType) bool {
	_, ok := StatusCause(err, name)
	return ok
}

// StatusCause 在提供的错误中查找指定类型的 cause；前提是该错误能解包为 APIStatus
// 且带有非空的 Details。找不到时返回零值和 false。
func StatusCause(err error, name metav1.CauseType) (metav1.StatusCause, bool) {
	status, ok := err.(APIStatus)
	// 先尝试直接断言，再退回 errors.As 以支持被包装的错误。
	if (ok || errors.As(err, &status)) && status.Status().Details != nil {
		for _, cause := range status.Status().Details.Causes {
			if cause.Type == name {
				return cause, true
			}
		}
	}
	return metav1.StatusCause{}, false
}

// UnexpectedObjectError 在 FromObject 收到非 Status 对象时返回。
type UnexpectedObjectError struct {
	Object runtime.Object
}

// Error 返回描述 *u 的错误信息。
func (u *UnexpectedObjectError) Error() string {
	return fmt.Sprintf("unexpected object: %v", u.Object)
}

// FromObject 在 obj 为 metav1.Status 时将其转换为 StatusError；
// 也支持把以 Unstructured 形式表示的 Status 还原为 StatusError，否则返回 UnexpectedObjectError。
func FromObject(obj runtime.Object) error {
	switch t := obj.(type) {
	case *metav1.Status:
		return &StatusError{ErrStatus: *t}
	case runtime.Unstructured:
		var status metav1.Status
		obj := t.UnstructuredContent()
		// 只接受 kind 精确为 "Status" 的非结构化对象，避免把普通资源误判为状态对象。
		if !reflect.DeepEqual(obj["kind"], "Status") {
			break
		}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(t.UnstructuredContent(), &status); err != nil {
			return err
		}
		// 只接受核心组 v1 或 meta.k8s.io/v1 的 Status；其他版本说明来源不可信，退回通用错误。
		if status.APIVersion != "v1" && status.APIVersion != "meta.k8s.io/v1" {
			break
		}
		return &StatusError{ErrStatus: status}
	}
	return &UnexpectedObjectError{obj}
}

// NewNotFound 返回表示指定类型、名称的资源不存在的错误。
func NewNotFound(qualifiedResource schema.GroupResource, name string) *StatusError {
	return &StatusError{metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusNotFound,
		Reason: metav1.StatusReasonNotFound,
		Details: &metav1.StatusDetails{
			Group: qualifiedResource.Group,
			Kind:  qualifiedResource.Resource,
			Name:  name,
		},
		Message: fmt.Sprintf("%s %q not found", qualifiedResource.String(), name),
	}}
}

// NewAlreadyExists 返回表示该标识对应的对象已存在的错误。
func NewAlreadyExists(qualifiedResource schema.GroupResource, name string) *StatusError {
	return &StatusError{metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusConflict,
		Reason: metav1.StatusReasonAlreadyExists,
		Details: &metav1.StatusDetails{
			Group: qualifiedResource.Group,
			Kind:  qualifiedResource.Resource,
			Name:  name,
		},
		Message: fmt.Sprintf("%s %q already exists", qualifiedResource.String(), name),
	}}
}

// NewGenerateNameConflict 返回表示服务端无法为资源生成合法且唯一名称的错误。
// 其 reason 仍为 AlreadyExists，并通过 RetryAfterSeconds 提示客户端稍后重试。
func NewGenerateNameConflict(qualifiedResource schema.GroupResource, name string, retryAfterSeconds int) *StatusError {
	return &StatusError{metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusConflict,
		Reason: metav1.StatusReasonAlreadyExists,
		Details: &metav1.StatusDetails{
			Group:             qualifiedResource.Group,
			Kind:              qualifiedResource.Resource,
			Name:              name,
			RetryAfterSeconds: int32(retryAfterSeconds),
		},
		Message: fmt.Sprintf(
			"%s %q already exists, the server was not able to generate a unique name for the object",
			qualifiedResource.String(), name),
	}}
}

// NewUnauthorized 返回表示客户端未通过身份认证、无权执行请求操作的错误。
func NewUnauthorized(reason string) *StatusError {
	message := reason
	// reason 为空时给出默认文案，避免返回一条没有说明的空消息。
	if len(message) == 0 {
		message = "not authorized"
	}
	return &StatusError{metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusUnauthorized,
		Reason:  metav1.StatusReasonUnauthorized,
		Message: message,
	}}
}

// NewForbidden 返回表示请求的操作被禁止的错误。
func NewForbidden(qualifiedResource schema.GroupResource, name string, err error) *StatusError {
	var message string
	// 按信息丰富程度分三档拼装消息：资源为空时只报原因，有资源无名称时报资源，
	// 两者都具备时精确到具体对象，便于调用方定位问题。
	if qualifiedResource.Empty() {
		message = fmt.Sprintf("forbidden: %v", err)
	} else if name == "" {
		message = fmt.Sprintf("%s is forbidden: %v", qualifiedResource.String(), err)
	} else {
		message = fmt.Sprintf("%s %q is forbidden: %v", qualifiedResource.String(), name, err)
	}
	return &StatusError{metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusForbidden,
		Reason: metav1.StatusReasonForbidden,
		Details: &metav1.StatusDetails{
			Group: qualifiedResource.Group,
			Kind:  qualifiedResource.Resource,
			Name:  name,
		},
		Message: message,
	}}
}

// NewConflict 返回表示对象因并发更新等原因无法按给定内容更新的错误。
func NewConflict(qualifiedResource schema.GroupResource, name string, err error) *StatusError {
	return &StatusError{metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusConflict,
		Reason: metav1.StatusReasonConflict,
		Details: &metav1.StatusDetails{
			Group: qualifiedResource.Group,
			Kind:  qualifiedResource.Resource,
			Name:  name,
		},
		Message: fmt.Sprintf("Operation cannot be fulfilled on %s %q: %v", qualifiedResource.String(), name, err),
	}}
}

// NewApplyConflict 返回包含服务端 apply 冲突细节的错误。
func NewApplyConflict(causes []metav1.StatusCause, message string) *StatusError {
	return &StatusError{ErrStatus: metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusConflict,
		Reason: metav1.StatusReasonConflict,
		Details: &metav1.StatusDetails{
			// 此处暂不填充对象级细节，只把冲突原因交给调用方。
			Causes: causes,
		},
		Message: message,
	}}
}

// NewGone 返回表示对象在服务端已不存在、且没有已知转发地址的错误。
//
// Deprecated: 请改用 NewResourceExpired。
func NewGone(message string) *StatusError {
	return &StatusError{metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusGone,
		Reason:  metav1.StatusReasonGone,
		Message: message,
	}}
}

// NewResourceExpired 创建一个错误，表示请求的资源内容已从服务端过期
// （通常是因为使用的 resourceVersion 过旧）。
func NewResourceExpired(message string) *StatusError {
	return &StatusError{metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusGone,
		Reason:  metav1.StatusReasonExpired,
		Message: message,
	}}
}

// NewInvalid 返回表示对象校验失败、无法处理的错误。
// 每条 field.Error 会被转换为一个 StatusCause，保留字段路径与错误原因，方便客户端按字段处理。
func NewInvalid(qualifiedKind schema.GroupKind, name string, errs field.ErrorList) *StatusError {
	causes := make([]metav1.StatusCause, 0, len(errs))
	for i := range errs {
		err := errs[i]
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseType(err.Type),
			Message: err.ErrorBody(),
			Field:   err.Field,
		})
	}
	err := &StatusError{metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusUnprocessableEntity,
		Reason: metav1.StatusReasonInvalid,
		Details: &metav1.StatusDetails{
			Group:  qualifiedKind.Group,
			Kind:   qualifiedKind.Kind,
			Name:   name,
			Causes: causes,
		},
	}}
	// 把所有校验错误聚合成一条可读消息；没有任何错误时保持简洁的 "is invalid" 文案。
	aggregatedErrs := errs.ToAggregate()
	if aggregatedErrs == nil {
		err.ErrStatus.Message = fmt.Sprintf("%s %q is invalid", qualifiedKind.String(), name)
	} else {
		err.ErrStatus.Message = fmt.Sprintf("%s %q is invalid: %v", qualifiedKind.String(), name, aggregatedErrs)
	}
	return err
}

// NewBadRequest 创建一个错误，表示请求本身不合法、无法处理。
func NewBadRequest(reason string) *StatusError {
	return &StatusError{metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusBadRequest,
		Reason:  metav1.StatusReasonBadRequest,
		Message: reason,
	}}
}

// NewTooManyRequests 创建一个错误，表示客户端之后必须重试，因为该端点当前不接受请求。
// 如果客户端需要知道被限流的具体原因，应通过消息或 Details 提供更具体的信息。
func NewTooManyRequests(message string, retryAfterSeconds int) *StatusError {
	return &StatusError{metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusTooManyRequests,
		Reason:  metav1.StatusReasonTooManyRequests,
		Message: message,
		Details: &metav1.StatusDetails{
			RetryAfterSeconds: int32(retryAfterSeconds),
		},
	}}
}

// NewServiceUnavailable 创建一个错误，表示请求的服务当前不可用。
func NewServiceUnavailable(reason string) *StatusError {
	return &StatusError{metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusServiceUnavailable,
		Reason:  metav1.StatusReasonServiceUnavailable,
		Message: reason,
	}}
}

// NewMethodNotSupported 返回表示该资源类型不支持所请求操作（如对某类资源执行 POST）的错误。
func NewMethodNotSupported(qualifiedResource schema.GroupResource, action string) *StatusError {
	return &StatusError{metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusMethodNotAllowed,
		Reason: metav1.StatusReasonMethodNotAllowed,
		Details: &metav1.StatusDetails{
			Group: qualifiedResource.Group,
			Kind:  qualifiedResource.Resource,
		},
		Message: fmt.Sprintf("%s is not supported on resources of kind %q", action, qualifiedResource.String()),
	}}
}

// NewServerTimeout 返回表示请求因瞬时错误无法完成、客户端应重试的错误。
func NewServerTimeout(qualifiedResource schema.GroupResource, operation string, retryAfterSeconds int) *StatusError {
	return &StatusError{metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusInternalServerError,
		Reason: metav1.StatusReasonServerTimeout,
		Details: &metav1.StatusDetails{
			Group:             qualifiedResource.Group,
			Kind:              qualifiedResource.Resource,
			Name:              operation,
			RetryAfterSeconds: int32(retryAfterSeconds),
		},
		Message: fmt.Sprintf("The %s operation against %s could not be completed at this time, please try again.", operation, qualifiedResource.String()),
	}}
}

// NewServerTimeoutForKind 本不应存在：服务端超时发生在访问资源时，Kind 只是请求失败时恰好查看的类型。
// 这里委托给 NewServerTimeout 以免调用方逻辑分裂，但长期目标是移除该函数。
// 它把 GroupKind 当作 GroupResource 传入，是一种有损的兼容处理。
func NewServerTimeoutForKind(qualifiedKind schema.GroupKind, operation string, retryAfterSeconds int) *StatusError {
	return NewServerTimeout(schema.GroupResource{Group: qualifiedKind.Group, Resource: qualifiedKind.Kind}, operation, retryAfterSeconds)
}

// NewInternalError 返回表示服务端内部错误的错误；原始错误文本会同时写入 message 和 cause。
func NewInternalError(err error) *StatusError {
	return &StatusError{metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusInternalServerError,
		Reason: metav1.StatusReasonInternalError,
		Details: &metav1.StatusDetails{
			Causes: []metav1.StatusCause{{Message: err.Error()}},
		},
		Message: fmt.Sprintf("Internal error occurred: %v", err),
	}}
}

// NewTimeoutError 返回表示请求完成前发生超时的错误。客户端可以重试，
// 但原操作仍可能已经成功，因此重试需考虑幂等性。
func NewTimeoutError(message string, retryAfterSeconds int) *StatusError {
	return &StatusError{metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusGatewayTimeout,
		Reason:  metav1.StatusReasonTimeout,
		Message: fmt.Sprintf("Timeout: %s", message),
		Details: &metav1.StatusDetails{
			RetryAfterSeconds: int32(retryAfterSeconds),
		},
	}}
}

// NewTooManyRequestsError 返回表示服务端收到过多请求而拒绝本次请求的错误。
// 客户端应等待后重试；若请求本身具有时效性（易过期），则不应重试。
func NewTooManyRequestsError(message string) *StatusError {
	return &StatusError{metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusTooManyRequests,
		Reason:  metav1.StatusReasonTooManyRequests,
		Message: fmt.Sprintf("Too many requests: %s", message),
	}}
}

// NewRequestEntityTooLargeError 返回表示请求体过大的错误。
func NewRequestEntityTooLargeError(message string) *StatusError {
	return &StatusError{metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusRequestEntityTooLarge,
		Reason:  metav1.StatusReasonRequestEntityTooLarge,
		Message: fmt.Sprintf("Request entity too large: %s", message),
	}}
}

// NewGenericServerResponse 为无法识别为结构化 Status 的服务端响应构造错误。
// 它根据 HTTP 状态码推断 Reason 与默认文案；当 isUnexpectedResponse 为 true 时，
// 会把原始服务端消息记录为一个 UnexpectedServerResponse cause，便于排查协议不匹配问题。
func NewGenericServerResponse(code int, verb string, qualifiedResource schema.GroupResource, name, serverMessage string, retryAfterSeconds int, isUnexpectedResponse bool) *StatusError {
	reason := metav1.StatusReasonUnknown
	message := fmt.Sprintf("the server responded with the status code %d but did not return more information", code)
	switch code {
	case http.StatusConflict:
		// POST 上的 409 通常意味着对象已存在（create 冲突），其他动词则视为更新冲突。
		if verb == http.MethodPost {
			reason = metav1.StatusReasonAlreadyExists
		} else {
			reason = metav1.StatusReasonConflict
		}
		message = "the server reported a conflict"
	case http.StatusNotFound:
		reason = metav1.StatusReasonNotFound
		message = "the server could not find the requested resource"
	case http.StatusBadRequest:
		reason = metav1.StatusReasonBadRequest
		message = "the server rejected our request for an unknown reason"
	case http.StatusUnauthorized:
		reason = metav1.StatusReasonUnauthorized
		message = "the server has asked for the client to provide credentials"
	case http.StatusForbidden:
		reason = metav1.StatusReasonForbidden
		// 服务端消息包含"谁在做什么"的细节，直接保留，信息量高于通用文案。
		message = serverMessage
	case http.StatusNotAcceptable:
		reason = metav1.StatusReasonNotAcceptable
		// 服务端消息包含可接受的类型信息；没有有效信息时使用通用说明。
		if len(serverMessage) == 0 || serverMessage == "unknown" {
			message = "the server was unable to respond with a content type that the client supports"
		} else {
			message = serverMessage
		}
	case http.StatusUnsupportedMediaType:
		reason = metav1.StatusReasonUnsupportedMediaType
		// 服务端消息包含可接受的类型信息，直接使用。
		message = serverMessage
	case http.StatusMethodNotAllowed:
		reason = metav1.StatusReasonMethodNotAllowed
		message = "the server does not allow this method on the requested resource"
	case http.StatusUnprocessableEntity:
		reason = metav1.StatusReasonInvalid
		message = "the server rejected our request due to an error in our request"
	case http.StatusServiceUnavailable:
		reason = metav1.StatusReasonServiceUnavailable
		message = "the server is currently unable to handle the request"
	case http.StatusGatewayTimeout:
		reason = metav1.StatusReasonTimeout
		message = "the server was unable to return a response in the time allotted, but may still be processing the request"
	case http.StatusTooManyRequests:
		reason = metav1.StatusReasonTooManyRequests
		message = "the server has received too many requests and has asked us to try again later"
	default:
		// 未明确列出的 5xx 一律视为服务端内部错误，并附带原始消息辅助定位。
		if code >= 500 {
			reason = metav1.StatusReasonInternalError
			message = fmt.Sprintf("an error on the server (%q) has prevented the request from succeeding", serverMessage)
		}
	}
	// 在消息末尾追加操作与对象信息，让调用方无需再拼接上下文。
	switch {
	case !qualifiedResource.Empty() && len(name) > 0:
		message = fmt.Sprintf("%s (%s %s %s)", message, strings.ToLower(verb), qualifiedResource.String(), name)
	case !qualifiedResource.Empty():
		message = fmt.Sprintf("%s (%s %s)", message, strings.ToLower(verb), qualifiedResource.String())
	}
	var causes []metav1.StatusCause
	// 仅当响应格式确实无法识别时才附加 cause；正常结构化响应不引入额外噪声。
	if isUnexpectedResponse {
		causes = []metav1.StatusCause{
			{
				Type:    metav1.CauseTypeUnexpectedServerResponse,
				Message: serverMessage,
			},
		}
	} else {
		causes = nil
	}
	return &StatusError{metav1.Status{
		Status: metav1.StatusFailure,
		Code:   int32(code),
		Reason: reason,
		Details: &metav1.StatusDetails{
			Group: qualifiedResource.Group,
			Kind:  qualifiedResource.Resource,
			Name:  name,

			Causes:            causes,
			RetryAfterSeconds: int32(retryAfterSeconds),
		},
		Message: message,
	}}
}

// IsNotFound 判断指定错误是否由 NewNotFound 创建。
// 支持包装错误；err 为 nil 时返回 false。
// 当 reason 未在 knownReasons 中登记时，退回按 HTTP 状态码 404 判断，
// 以兼容那些只带状态码、reason 不可识别的旧服务端响应。
func IsNotFound(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonNotFound {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusNotFound {
		return true
	}
	return false
}

// IsAlreadyExists 判断 err 是否表示指定资源已存在。支持包装错误；err 为 nil 时返回 false。
func IsAlreadyExists(err error) bool {
	return ReasonForError(err) == metav1.StatusReasonAlreadyExists
}

// IsConflict 判断 err 是否表示本次更新发生冲突。支持包装错误；err 为 nil 时返回 false。
func IsConflict(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonConflict {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusConflict {
		return true
	}
	return false
}

// IsInvalid 判断 err 是否表示资源内容不合法。支持包装错误；err 为 nil 时返回 false。
func IsInvalid(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonInvalid {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusUnprocessableEntity {
		return true
	}
	return false
}

// IsGone 判断错误是否表示所请求的资源已不再可用。支持包装错误；err 为 nil 时返回 false。
func IsGone(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonGone {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusGone {
		return true
	}
	return false
}

// IsResourceExpired 判断错误是否表示资源已过期、当前操作无法继续。
// 支持包装错误；err 为 nil 时返回 false。
func IsResourceExpired(err error) bool {
	return ReasonForError(err) == metav1.StatusReasonExpired
}

// IsNotAcceptable 判断 err 是否表示请求因 Accept 头不合法而失败。支持包装错误；err 为 nil 时返回 false。
func IsNotAcceptable(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonNotAcceptable {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusNotAcceptable {
		return true
	}
	return false
}

// IsUnsupportedMediaType 判断 err 是否表示请求因 Content-Type 头不合法而失败。支持包装错误；err 为 nil 时返回 false。
func IsUnsupportedMediaType(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonUnsupportedMediaType {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusUnsupportedMediaType {
		return true
	}
	return false
}

// IsMethodNotSupported 判断 err 是否表示服务端不支持所请求的操作。支持包装错误；err 为 nil 时返回 false。
func IsMethodNotSupported(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonMethodNotAllowed {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusMethodNotAllowed {
		return true
	}
	return false
}

// IsServiceUnavailable 判断错误是否表示底层服务已不可用。支持包装错误；err 为 nil 时返回 false。
func IsServiceUnavailable(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonServiceUnavailable {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusServiceUnavailable {
		return true
	}
	return false
}

// IsBadRequest 判断 err 是否表示请求不合法。支持包装错误；err 为 nil 时返回 false。
func IsBadRequest(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonBadRequest {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusBadRequest {
		return true
	}
	return false
}

// IsUnauthorized 判断 err 是否表示请求未获授权、需要用户完成认证。
// 支持包装错误；err 为 nil 时返回 false。
func IsUnauthorized(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonUnauthorized {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusUnauthorized {
		return true
	}
	return false
}

// IsForbidden 判断 err 是否表示请求被禁止、无法按请求完成。支持包装错误；err 为 nil 时返回 false。
func IsForbidden(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonForbidden {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusForbidden {
		return true
	}
	return false
}

// IsTimeout 判断 err 是否表示请求因处理耗时过长而超时。支持包装错误；err 为 nil 时返回 false。
func IsTimeout(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonTimeout {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusGatewayTimeout {
		return true
	}
	return false
}

// IsServerTimeout 判断 err 是否表示客户端需要重试请求。支持包装错误；err 为 nil 时返回 false。
func IsServerTimeout(err error) bool {
	// 不检查状态码：HTTP 状态码无法专门表示可重试的超时。
	return ReasonForError(err) == metav1.StatusReasonServerTimeout
}

// IsInternalError 判断 err 是否表示服务端内部错误。支持包装错误；err 为 nil 时返回 false。
func IsInternalError(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonInternalError {
		return true
	}
	if _, ok := knownReasons[reason]; !ok && code == http.StatusInternalServerError {
		return true
	}
	return false
}

// IsTooManyRequests 判断 err 是否表示请求过多、服务端无法处理。支持包装错误；err 为 nil 时返回 false。
func IsTooManyRequests(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonTooManyRequests {
		return true
	}

	// IsTooManyRequests 检查状态码早于其他 Is* 函数；为保持向后兼容，
	// 即使 reason 未知，也仍按状态码判断。
	//
	if code == http.StatusTooManyRequests {
		return true
	}
	return false
}

// IsRequestEntityTooLargeError 判断 err 是否表示请求体过大。支持包装错误；err 为 nil 时返回 false。
func IsRequestEntityTooLargeError(err error) bool {
	reason, code := reasonAndCodeForError(err)
	if reason == metav1.StatusReasonRequestEntityTooLarge {
		return true
	}

	// IsRequestEntityTooLargeError 检查状态码早于其他 Is* 函数；
	// 为保持向后兼容，
	// 即使 reason 未知，也仍按状态码判断。
	if code == http.StatusRequestEntityTooLarge {
		return true
	}
	return false
}

// IsUnexpectedServerError 判断服务端响应是否不符合预期的 API 格式，
// 这可能来自链路上其他 HTTP 中间组件。支持包装错误；err 为 nil 时返回 false。
func IsUnexpectedServerError(err error) bool {
	status, ok := err.(APIStatus)
	if (ok || errors.As(err, &status)) && status.Status().Details != nil {
		for _, cause := range status.Status().Details.Causes {
			if cause.Type == metav1.CauseTypeUnexpectedServerResponse {
				return true
			}
		}
	}
	return false
}

// IsUnexpectedObjectError 判断 err 是否由 master 返回了非预期对象所致。
// 支持包装错误；err 为 nil 时返回 false。
func IsUnexpectedObjectError(err error) bool {
	uoe, ok := err.(*UnexpectedObjectError)
	return err != nil && (ok || errors.As(err, &uoe))
}

// IsStoreReadError 判断 err 是否由存储数据转换失败或对象解码失败所致。
func IsStoreReadError(err error) bool {
	return ReasonForError(err) == metav1.StatusReasonStoreReadError
}

// SuggestsClientDelay 在错误建议客户端延迟时返回建议等待的秒数以及 true，
// 否则返回 0 和 false。它不判断错误是否*应当*重试——某些错误（如 3xx）
// 可能要求延迟但并不要求重试。支持包装错误；err 为 nil 时返回 false。
func SuggestsClientDelay(err error) (int, bool) {
	t, ok := err.(APIStatus)
	if (ok || errors.As(err, &t)) && t.Status().Details != nil {
		switch t.Status().Reason {
		// 此 StatusReason 明确要求调用方延迟操作。
		case metav1.StatusReasonServerTimeout:
			return int(t.Status().Details.RetryAfterSeconds), true
		}
		// 如果状态详情明确给出了重试等待秒数，则将该值返回给调用方。
		if t.Status().Details.RetryAfterSeconds > 0 {
			return int(t.Status().Details.RetryAfterSeconds), true
		}
	}
	return 0, false
}

// ReasonForError 返回特定错误的 HTTP 状态原因（注意：返回的是 StatusReason，而非状态码）。
// 支持包装错误；err 为 nil 或不带状态时返回 StatusReasonUnknown。
func ReasonForError(err error) metav1.StatusReason {
	if status, ok := err.(APIStatus); ok || errors.As(err, &status) {
		return status.Status().Reason
	}
	return metav1.StatusReasonUnknown
}

// reasonAndCodeForError 提取错误的 reason 与 HTTP 状态码，供 Is* 系列函数做双重判断。
// 未实现 APIStatus 的错误返回 Unknown 和 0，使各 Is* 函数的兜底分支自然不命中。
func reasonAndCodeForError(err error) (metav1.StatusReason, int32) {
	if status, ok := err.(APIStatus); ok || errors.As(err, &status) {
		return status.Status().Reason, status.Status().Code
	}
	return metav1.StatusReasonUnknown, 0
}

// ErrorReporter 把通用 error 转换为 runtime.Object 形式的错误，
// 使调用方无需依赖 meta/v1（Status 所在包）。这样做可避免 core watch 代码中出现循环依赖。
type ErrorReporter struct {
	code   int
	verb   string
	reason string
}

// NewClientErrorReporter 返回一个 ErrorReporter，用于生成能报告"非预期服务端响应"的合法 v1.Status。
// 主要供 watch 在解码服务端响应失败（格式不符合预期）时使用。由于 watch 是 core api 的依赖，
// 该包不能直接返回 meta/v1.Status，只能注入此接口把通用错误转换为合适形式。
// reason 会作为该 status 上唯一的 cause 类型；未提供时使用通用的 "ClientError"。
func NewClientErrorReporter(code int, verb string, reason string) *ErrorReporter {
	return &ErrorReporter{
		code:   code,
		verb:   verb,
		reason: reason,
	}
}

// AsObject 使用 reporter 中的状态码与动词，为给定 error 生成合法的 runtime.Object（v1.Status）。
// 生成的错误被标记为非预期的服务端响应。
func (r *ErrorReporter) AsObject(err error) runtime.Object {
	status := NewGenericServerResponse(r.code, r.verb, schema.GroupResource{}, "", err.Error(), 0, true)
	// NewGenericServerResponse 在非预期响应路径上通常会带 Details，但这里仍做防御性初始化，
	// 避免后续追加 cause 时出现空指针。
	if status.ErrStatus.Details == nil {
		status.ErrStatus.Details = &metav1.StatusDetails{}
	}
	reason := r.reason
	// 未显式指定原因时使用通用 "ClientError"，保证 cause 类型始终可读。
	if len(reason) == 0 {
		reason = "ClientError"
	}
	status.ErrStatus.Details.Causes = append(status.ErrStatus.Details.Causes, metav1.StatusCause{
		Type:    metav1.CauseType(reason),
		Message: err.Error(),
	})
	return &status.ErrStatus
}
