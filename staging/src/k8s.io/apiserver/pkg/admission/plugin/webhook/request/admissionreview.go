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

package request

import (
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	admissionv1beta1 "k8s.io/api/admission/v1beta1"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/generic"
)

// AdmissionResponse 包含从 AdmissionReview 响应中提取出的字段。
//
// 只保留准入逻辑关心的字段，隐藏 AdmissionReview 的版本差异；
// 这样调用方无需区分 v1/v1beta1，也便于日后新增版本。
type AdmissionResponse struct {
	AuditAnnotations map[string]string
	Allowed          bool
	Patch            []byte
	PatchType        admissionv1.PatchType
	Result           *metav1.Status
	Warnings         []string
}

// VerifyAdmissionResponse 检查所提供的 admission review 对象的有效性，并返回
// 审计注解、该响应是否允许请求、所提供的 patch/patchType/status，
// 或在所提供的 admission review 无效时返回错误。
//
// 这是信任外部 webhook 输出的第一道校验。UID 必须与请求一致，
// 否则响应可能属于另一个并发请求（重放/串扰）；GVK 必须匹配，防止服务端返回错误版本。
// 对 mutating webhook，patch 与 patchType 必须成对出现——只给 patch 无法安全应用；
// 对 validating webhook，出现 patch 一律拒绝，因为它无权修改对象。
// v1beta1 分支不校验 GVK/UID：该版本协议本身未要求，保持向后兼容。
func VerifyAdmissionResponse(uid types.UID, mutating bool, review runtime.Object) (*AdmissionResponse, error) {
	switch r := review.(type) {
	case *admissionv1.AdmissionReview:
		if r.Response == nil {
			return nil, fmt.Errorf("webhook response was absent")
		}

		// 校验 UID 是否匹配
		if r.Response.UID != uid {
			return nil, fmt.Errorf("expected response.uid=%q, got %q", uid, r.Response.UID)
		}

		// 校验 GVK
		v1GVK := admissionv1.SchemeGroupVersion.WithKind("AdmissionReview")
		if r.GroupVersionKind() != v1GVK {
			return nil, fmt.Errorf("expected webhook response of %v, got %v", v1GVK.String(), r.GroupVersionKind().String())
		}

		patch := []byte(nil)
		patchType := admissionv1.PatchType("")

		if mutating {
			// 确保 mutating webhook 同时提供 patch 与 patchType
			if len(r.Response.Patch) > 0 && r.Response.PatchType == nil {
				return nil, fmt.Errorf("webhook returned response.patch but not response.patchType")
			}
			if len(r.Response.Patch) == 0 && r.Response.PatchType != nil {
				return nil, fmt.Errorf("webhook returned response.patchType but not response.patch")
			}
			patch = r.Response.Patch
			if r.Response.PatchType != nil {
				patchType = *r.Response.PatchType
				// 空字符串虽非 nil，但同样无法应用，必须拒绝。
				if len(patchType) == 0 {
					return nil, fmt.Errorf("webhook returned invalid response.patchType of %q", patchType)
				}
			}
		} else {
			// 确保 validating webhook 不返回 patch 或 patchType
			if len(r.Response.Patch) > 0 {
				return nil, fmt.Errorf("validating webhook may not return response.patch")
			}
			if r.Response.PatchType != nil {
				return nil, fmt.Errorf("validating webhook may not return response.patchType")
			}
		}

		return &AdmissionResponse{
			AuditAnnotations: r.Response.AuditAnnotations,
			Allowed:          r.Response.Allowed,
			Patch:            patch,
			PatchType:        patchType,
			Result:           r.Response.Result,
			Warnings:         r.Response.Warnings,
		}, nil

	case *admissionv1beta1.AdmissionReview:
		if r.Response == nil {
			return nil, fmt.Errorf("webhook response was absent")
		}

		// v1beta1 的处理流程未校验响应 GVK 与 response.uid，此处允许任意值

		patch := []byte(nil)
		patchType := admissionv1.PatchType("")
		if mutating {
			patch = r.Response.Patch
			if len(r.Response.Patch) > 0 {
				// v1beta1 admissionreview 处理流程未校验 patch type。若提供了 patch，则固定为唯一受支持的版本。
				//
				// v1beta1 的 PatchType 语义模糊，这里统一假定为 JSONPatch，
				// 这是该版本事实上的唯一用法，避免引入未经验证的补丁类型。
				patchType = admissionv1.PatchTypeJSONPatch
			}
		}

		return &AdmissionResponse{
			AuditAnnotations: r.Response.AuditAnnotations,
			Allowed:          r.Response.Allowed,
			Patch:            patch,
			PatchType:        patchType,
			Result:           r.Response.Result,
			Warnings:         r.Response.Warnings,
		}, nil

	default:
		return nil, fmt.Errorf("unexpected response type %T", review)
	}
}

// CreateAdmissionObjects 返回唯一的请求 uid、要发送给 webhook 的 AdmissionReview 对象和用于解码响应的对象，
// 若 webhook 不支持任何我们已知可发送的 admission review 版本，则返回错误。
//
// 按 webhook 声明的版本偏好顺序选择第一个受支持的版本；
// request 与 response 必须是同一版本的对象，否则解码会失败。
// UID 在每次调用时新生成，用于把并发请求的响应严格对应起来。
func CreateAdmissionObjects(versionedAttributes *admission.VersionedAttributes, invocation *generic.WebhookInvocation) (uid types.UID, request, response runtime.Object, err error) {
	for _, version := range invocation.Webhook.GetAdmissionReviewVersions() {
		switch version {
		case admissionv1.SchemeGroupVersion.Version:
			uid := types.UID(uuid.NewUUID())
			request := CreateV1AdmissionReview(uid, versionedAttributes, invocation)
			response := &admissionv1.AdmissionReview{}
			return uid, request, response, nil

		case admissionv1beta1.SchemeGroupVersion.Version:
			uid := types.UID(uuid.NewUUID())
			request := CreateV1beta1AdmissionReview(uid, versionedAttributes, invocation)
			response := &admissionv1beta1.AdmissionReview{}
			return uid, request, response, nil

		}
	}
	return "", nil, nil, fmt.Errorf("webhook does not accept known AdmissionReview versions (v1, v1beta1)")
}

// CreateV1AdmissionReview 为给定的 admission.Attributes 创建 AdmissionReview。
//
// Kind/Resource 取自 webhook 配置的匹配目标，RequestKind/RequestResource
// 取自实际请求——两者可能不同（例如通过转换版本访问），webhook 需要能区分，
// 否则无法正确判断自己是否应处理该请求。
// UserInfo 的 Extra 需逐项复制为 authenticationv1.ExtraValue，直接赋值类型不兼容。
func CreateV1AdmissionReview(uid types.UID, versionedAttributes *admission.VersionedAttributes, invocation *generic.WebhookInvocation) *admissionv1.AdmissionReview {
	attr := versionedAttributes.Attributes
	gvk := invocation.Kind
	gvr := invocation.Resource
	subresource := invocation.Subresource
	requestGVK := attr.GetKind()
	requestGVR := attr.GetResource()
	requestSubResource := attr.GetSubresource()
	aUserInfo := attr.GetUserInfo()
	userInfo := authenticationv1.UserInfo{
		Extra:    make(map[string]authenticationv1.ExtraValue),
		Groups:   aUserInfo.GetGroups(),
		UID:      aUserInfo.GetUID(),
		Username: aUserInfo.GetName(),
	}
	dryRun := attr.IsDryRun()

	// 转换 user 对象中的 extra 信息
	for key, val := range aUserInfo.GetExtra() {
		userInfo.Extra[key] = authenticationv1.ExtraValue(val)
	}

	return &admissionv1.AdmissionReview{
		Request: &admissionv1.AdmissionRequest{
			UID: uid,
			Kind: metav1.GroupVersionKind{
				Group:   gvk.Group,
				Kind:    gvk.Kind,
				Version: gvk.Version,
			},
			Resource: metav1.GroupVersionResource{
				Group:    gvr.Group,
				Resource: gvr.Resource,
				Version:  gvr.Version,
			},
			SubResource: subresource,
			RequestKind: &metav1.GroupVersionKind{
				Group:   requestGVK.Group,
				Kind:    requestGVK.Kind,
				Version: requestGVK.Version,
			},
			RequestResource: &metav1.GroupVersionResource{
				Group:    requestGVR.Group,
				Resource: requestGVR.Resource,
				Version:  requestGVR.Version,
			},
			RequestSubResource: requestSubResource,
			Name:               attr.GetName(),
			Namespace:          attr.GetNamespace(),
			Operation:          admissionv1.Operation(attr.GetOperation()),
			UserInfo:           userInfo,
			Object: runtime.RawExtension{
				Object: versionedAttributes.VersionedObject.Object(),
			},
			OldObject: runtime.RawExtension{
				Object: versionedAttributes.VersionedOldObject.Object(),
			},
			DryRun: &dryRun,
			Options: runtime.RawExtension{
				Object: attr.GetOperationOptions(),
			},
		},
	}
}

// CreateV1beta1AdmissionReview 为给定的 admission.Attributes 创建 v1beta1 AdmissionReview。
//
// 字段布局与 v1 完全一致，仅类型不同；保留独立实现是为了让 v1beta1
// 的兼容行为（如不校验 GVK）可以单独演进，而不影响 v1 路径。
func CreateV1beta1AdmissionReview(uid types.UID, versionedAttributes *admission.VersionedAttributes, invocation *generic.WebhookInvocation) *admissionv1beta1.AdmissionReview {
	attr := versionedAttributes.Attributes
	gvk := invocation.Kind
	gvr := invocation.Resource
	subresource := invocation.Subresource
	requestGVK := attr.GetKind()
	requestGVR := attr.GetResource()
	requestSubResource := attr.GetSubresource()
	aUserInfo := attr.GetUserInfo()
	userInfo := authenticationv1.UserInfo{
		Extra:    make(map[string]authenticationv1.ExtraValue),
		Groups:   aUserInfo.GetGroups(),
		UID:      aUserInfo.GetUID(),
		Username: aUserInfo.GetName(),
	}
	dryRun := attr.IsDryRun()

	// 转换 user 对象中的 extra 信息
	for key, val := range aUserInfo.GetExtra() {
		userInfo.Extra[key] = authenticationv1.ExtraValue(val)
	}

	return &admissionv1beta1.AdmissionReview{
		Request: &admissionv1beta1.AdmissionRequest{
			UID: uid,
			Kind: metav1.GroupVersionKind{
				Group:   gvk.Group,
				Kind:    gvk.Kind,
				Version: gvk.Version,
			},
			Resource: metav1.GroupVersionResource{
				Group:    gvr.Group,
				Resource: gvr.Resource,
				Version:  gvr.Version,
			},
			SubResource: subresource,
			RequestKind: &metav1.GroupVersionKind{
				Group:   requestGVK.Group,
				Kind:    requestGVK.Kind,
				Version: requestGVK.Version,
			},
			RequestResource: &metav1.GroupVersionResource{
				Group:    requestGVR.Group,
				Resource: requestGVR.Resource,
				Version:  requestGVR.Version,
			},
			RequestSubResource: requestSubResource,
			Name:               attr.GetName(),
			Namespace:          attr.GetNamespace(),
			Operation:          admissionv1beta1.Operation(attr.GetOperation()),
			UserInfo:           userInfo,
			Object: runtime.RawExtension{
				Object: versionedAttributes.VersionedObject.Object(),
			},
			OldObject: runtime.RawExtension{
				Object: versionedAttributes.VersionedOldObject.Object(),
			},
			DryRun: &dryRun,
			Options: runtime.RawExtension{
				Object: attr.GetOperationOptions(),
			},
		},
	}
}
