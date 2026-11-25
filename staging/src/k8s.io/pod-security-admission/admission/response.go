/*
Copyright 2021 The Kubernetes Authors.

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

package admission

import (
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/pod-security-admission/api"
)

// 放行响应不含动态内容，复用同一实例可避免每次准入请求都分配一个新对象；
// 各豁免/特权分支的审计注解在 init 中一次性写入，调用方只读，不会互相污染。
var (
	sharedAllowedResponse                        = allowedResponse()
	sharedAllowedPrivilegedResponse              = allowedResponse()
	sharedAllowedByUserExemptionResponse         = allowedResponse()
	sharedAllowedByNamespaceExemptionResponse    = allowedResponse()
	sharedAllowedByRuntimeClassExemptionResponse = allowedResponse()
)

// 在 init 中给各共享响应一次性写入审计注解：
// 审计日志据此区分“普通放行”与“因豁免/特权而放行”，便于事后追溯策略为何未生效。
func init() {
	sharedAllowedPrivilegedResponse.AuditAnnotations = map[string]string{
		api.EnforcedPolicyAnnotationKey: api.LevelVersion{Level: api.LevelPrivileged, Version: api.LatestVersion()}.String(),
	}
	sharedAllowedByUserExemptionResponse.AuditAnnotations = map[string]string{api.ExemptionReasonAnnotationKey: "user"}
	sharedAllowedByNamespaceExemptionResponse.AuditAnnotations = map[string]string{api.ExemptionReasonAnnotationKey: "namespace"}
	sharedAllowedByRuntimeClassExemptionResponse.AuditAnnotations = map[string]string{api.ExemptionReasonAnnotationKey: "runtimeClass"}
}

// allowedResponse 构造放行响应，只置 Allowed=true，不附加审计注解；
// 需要记录放行原因的分支使用 init 中准备好的共享实例。
func allowedResponse() *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{Allowed: true}
}

// forbiddenResponse 为策略违规构造拒绝响应。
// 用 Forbidden 而非普通错误构造 Result，让客户端收到语义明确的 403，
// 错误信息中带出具体违规理由，用户无需翻看服务端日志即可知道如何整改。
func forbiddenResponse(attrs api.Attributes, err error) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result:  &apierrors.NewForbidden(attrs.GetResource().GroupResource(), attrs.GetName(), err).ErrStatus,
	}
}

// invalidResponse 为命名空间标签非法构造响应。
// 标签写错属于请求格式问题而非策略违规，用 Invalid（422）返回才能与策略拒绝区分开。
func invalidResponse(attrs api.Attributes, fieldErrors field.ErrorList) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result:  &apierrors.NewInvalid(attrs.GetKind().GroupKind(), attrs.GetName(), fieldErrors).ErrStatus,
	}
}

// errorResponse 为通用错误构造失败响应。
// 内部错误细节只写入审计注解，不追加到 Result 消息，避免向请求方暴露敏感信息。
func errorResponse(err error, status *metav1.Status) *admissionv1.AdmissionResponse {
	var errDetail string
	if err != nil {
		errDetail = fmt.Sprintf("%s: %v", status.Message, err)
	} else {
		errDetail = status.Message
	}
	return &admissionv1.AdmissionResponse{
		Allowed:          false,
		Result:           status,
		AuditAnnotations: map[string]string{"error": errDetail},
	}
}
