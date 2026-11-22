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

package v1beta1

import (
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// 此 API 从不由服务端提供，仅用于 apiserver 发出的请求。这可确保它不会被意外提供。
// 让生成器作用于该 group，也能保护将来可能对外提供的 API。
// +k8s:prerelease-lifecycle-gen:replacement=admission.k8s.io,v1,AdmissionReview

// AdmissionReview 描述一次 admission review 的请求/响应。
type AdmissionReview struct {
	metav1.TypeMeta `json:""`
	// request 描述 admission 请求的属性。
	// +optional
	Request *AdmissionRequest `json:"request,omitempty" protobuf:"bytes,1,opt,name=request"`
	// response 描述 admission 响应的属性。
	// +optional
	Response *AdmissionResponse `json:"response,omitempty" protobuf:"bytes,2,opt,name=response"`
}

// AdmissionRequest 描述 admission 请求的 admission.Attributes。
type AdmissionRequest struct {
	// uid 是单次请求/响应的标识符，用于区分那些内容相同
	// （并行请求、此前请求未做修改的请求等）的请求实例。
	// UID 用于跟踪 KAS 与 WebHook 之间的一次往返（request/response），而非用户发起的请求。
	// 适合用于在 webhook 与 apiserver 之间关联日志，以进行审计或调试。
	// +optional
	UID types.UID `json:"uid" protobuf:"bytes,1,opt,name=uid"`
	// kind 是所提交对象的完整限定类型（例如 v1.Pod 或 autoscaling.v1.Scale）。
	// +optional
	Kind metav1.GroupVersionKind `json:"kind" protobuf:"bytes,2,opt,name=kind"`
	// resource 是所请求资源的完整限定名（例如 v1.pods）。
	// +optional
	Resource metav1.GroupVersionResource `json:"resource" protobuf:"bytes,3,opt,name=resource"`
	// subResource 是所请求的子资源（如有）（例如 "status" 或 "scale"）。
	// +optional
	SubResource string `json:"subResource,omitempty" protobuf:"bytes,4,opt,name=subResource"`

	// requestKind 是原始 API 请求的完整限定类型（例如 v1.Pod 或 autoscaling.v1.Scale）。
	// 若该字段有值且与 "kind" 的值不同，则说明执行了等价匹配与转换。
	//
	// 例如，若可通过 apps/v1 和 apps/v1beta1 修改 deployments，且某个 webhook 注册了规则
	// `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]` 与 `matchPolicy: Equivalent`，
	// 那么针对 apps/v1beta1 deployments 的 API 请求将被转换后发送给该 webhook，
	// 其中的 `kind: {group:"apps", version:"v1", kind:"Deployment"}`（与该 webhook 注册的规则匹配），
	// 以及 `requestKind: {group:"apps", version:"v1beta1", kind:"Deployment"}`（表示原始 API 请求的 kind）。
	//
	// 更多细节见 webhook configuration 类型中 "matchPolicy" 字段的文档。
	// +optional
	RequestKind *metav1.GroupVersionKind `json:"requestKind,omitempty" protobuf:"bytes,13,opt,name=requestKind"`
	// requestResource 是原始 API 请求的完整限定资源（例如 v1.pods）。
	// 若该字段有值且与 "resource" 的值不同，则说明执行了等价匹配与转换。
	//
	// 例如，若可通过 apps/v1 和 apps/v1beta1 修改 deployments，且某个 webhook 注册了规则
	// `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]` 与 `matchPolicy: Equivalent`，
	// 那么针对 apps/v1beta1 deployments 的 API 请求将被转换后发送给该 webhook，
	// 其中的 `resource: {group:"apps", version:"v1", resource:"deployments"}`（与该 webhook 注册的资源匹配），
	// 以及 `requestResource: {group:"apps", version:"v1beta1", resource:"deployments"}`（表示原始 API 请求的资源）。
	//
	// 见 webhook configuration 类型中 "matchPolicy" 字段的文档。
	// +optional
	RequestResource *metav1.GroupVersionResource `json:"requestResource,omitempty" protobuf:"bytes,14,opt,name=requestResource"`
	// requestSubResource 是原始 API 请求中子资源的名称（如有）（例如 "status" 或 "scale"）。
	// 若该字段有值且与 "subResource" 的值不同，则说明执行了等价匹配与转换。
	// 见 webhook configuration 类型中 "matchPolicy" 字段的文档。
	// +optional
	RequestSubResource string `json:"requestSubResource,omitempty" protobuf:"bytes,15,opt,name=requestSubResource"`

	// name 是请求中所呈现的对象名称。在 CREATE 操作中，客户端可省略 name，
	// 由服务端生成名称；此时该字段为空字符串。
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,5,opt,name=name"`
	// namespace 是与该请求关联的 namespace（如有）。
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,6,opt,name=namespace"`
	// operation 是实际执行的操作，可能与请求的操作不同。
	// 例如，patch 可能导致 CREATE 或 UPDATE 操作。
	// +optional
	Operation Operation `json:"operation" protobuf:"bytes,7,opt,name=operation"`
	// userInfo 是发起请求的用户的信息。
	// +optional
	UserInfo authenticationv1.UserInfo `json:"userInfo" protobuf:"bytes,8,opt,name=userInfo"`
	// object 是传入请求中的对象。
	// +optional
	Object runtime.RawExtension `json:"object,omitempty" protobuf:"bytes,9,opt,name=object"`
	// oldObject 是已存在的对象，仅对 DELETE 和 UPDATE 请求填充。
	// +optional
	OldObject runtime.RawExtension `json:"oldObject,omitempty" protobuf:"bytes,10,opt,name=oldObject"`
	// dryRun 表示对该请求的修改一定不会被持久化。
	// 默认为 false。
	// +optional
	DryRun *bool `json:"dryRun,omitempty" protobuf:"varint,11,opt,name=dryRun"`
	// options 是实际执行操作的操作选项结构，
	// 例如 `meta.k8s.io/v1.DeleteOptions` 或 `meta.k8s.io/v1.CreateOptions`。它可能与调用方提供的选项不同。
	// 例如，对于 patch 请求，实际执行的操作可能是 CREATE，此时 Options 为
	// `meta.k8s.io/v1.CreateOptions`，尽管调用方提供的是 `meta.k8s.io/v1.PatchOptions`。
	// +optional
	Options runtime.RawExtension `json:"options,omitempty" protobuf:"bytes,12,opt,name=options"`
}

// AdmissionResponse 描述一次 admission 响应。
type AdmissionResponse struct {
	// uid 是单次请求/响应的标识符。
	// 应从对应的 AdmissionRequest 原样复制过来。
	// +optional
	UID types.UID `json:"uid" protobuf:"bytes,1,opt,name=uid"`

	// allowed 表示 admission 请求是否被允许。
	// +optional
	Allowed bool `json:"allowed" protobuf:"varint,2,opt,name=allowed"`

	// status 是结果，包含 admission 请求被拒绝原因的额外细节。
	// 若 "Allowed" 为 "true"，该字段不会被任何方式参考。
	// +optional
	Result *metav1.Status `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`

	// patch 是 patch 的内容。目前仅支持实现 RFC 6902 的 "JSONPatch"。
	// +optional
	Patch []byte `json:"patch,omitempty" protobuf:"bytes,4,opt,name=patch"`

	// patchType 是 Patch 的类型。目前仅允许 "JSONPatch"。
	// +optional
	PatchType *PatchType `json:"patchType,omitempty" protobuf:"bytes,5,opt,name=patchType"`

	// auditAnnotations 是由远端 admission controller 设置的非结构化键值映射（例如 error=image-blacklisted）。
	// MutatingAdmissionWebhook 与 ValidatingAdmissionWebhook admission controller 会为键加上
	// admission webhook 名称前缀（例如 imagepolicy.example.com/error=image-blacklisted）。
	// AuditAnnotations 由 admission webhook 提供，用于为该请求的审计日志补充上下文。
	// +optional
	AuditAnnotations map[string]string `json:"auditAnnotations,omitempty" protobuf:"bytes,6,opt,name=auditAnnotations"`

	// warnings 是要返回给发起请求的 API 客户端的警告消息列表。
	// 警告消息描述发起 API 请求的客户端应当修正或注意的问题。
	// 警告尽量限制在 120 个字符以内。
	// 超过 256 个字符的警告以及数量过多的警告可能会被截断。
	// +optional
	// +listType=atomic
	Warnings []string `json:"warnings,omitempty" protobuf:"bytes,7,rep,name=warnings"`
}

// PatchType 是用于表示被修改对象的 patch 类型。
type PatchType string

// PatchType 常量。
const (
	// PatchTypeJSONPatch 表示遵循 RFC 6902 的 JSONPatch 类型。
	PatchTypeJSONPatch PatchType = "JSONPatch"
)

// Operation 是接受 admission control 检查的资源操作类型。
type Operation string

// Operation 常量。
const (
	// Create 表示创建资源的 CREATE 操作。
	Create Operation = "CREATE"
	// Update 表示更新资源的 UPDATE 操作。
	Update Operation = "UPDATE"
	// Delete 表示删除资源的 DELETE 操作。
	Delete Operation = "DELETE"
	// Connect 表示连接资源的 CONNECT 操作。
	Connect Operation = "CONNECT"
)
