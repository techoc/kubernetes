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

package admission

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/apis/authentication"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AdmissionReview 描述一次准入审查的请求/响应。
type AdmissionReview struct {
	metav1.TypeMeta

	// Request 描述准入请求的属性。
	// +optional
	Request *AdmissionRequest

	// Response 描述准入响应的属性。
	// +optional
	Response *AdmissionResponse
}

// AdmissionRequest 描述准入请求的 admission.Attributes。
type AdmissionRequest struct {
	// UID 是每个请求/响应的标识符，用于区分原本相同的请求实例，
	// 例如并发请求，或先前请求未修改对象的场景。
	// 该 UID 用于跟踪 KAS 与 WebHook 之间的一次往返（请求/响应），而不是用户请求本身。
	// 它适合用于关联 webhook 与 apiserver 两侧的日志，便于审计或调试。
	UID types.UID
	// Kind 是被提交对象的完整限定类型（例如 v1.Pod 或 autoscaling.v1.Scale）。
	Kind metav1.GroupVersionKind
	// Resource 是被请求资源的完整限定名称（例如 v1.pods）。
	Resource metav1.GroupVersionResource
	// SubResource 是被请求的子资源（如有），例如 "status" 或 "scale"。
	// +optional
	SubResource string

	// RequestKind 是原始 API 请求的完整限定类型（例如 v1.Pod 或 autoscaling.v1.Scale）。
	// 若该字段存在且与 "kind" 不同，说明执行了等价匹配与转换。
	//
	// 例如，若 deployment 可通过 apps/v1 和 apps/v1beta1 修改，而 webhook 注册了如下规则：
	// `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]` 且 `matchPolicy: Equivalent`，
	// 则针对 apps/v1beta1 deployment 的 API 请求会被转换后发送给该 webhook。
	// 其中 `kind: {group:"apps", version:"v1", kind:"Deployment"}` 匹配 webhook 注册的规则，
	// 而 `requestKind: {group:"apps", version:"v1beta1", kind:"Deployment"}` 表示原始 API 请求的类型。
	//
	// 更多细节参见 webhook 配置类型中 "matchPolicy" 字段的文档。
	// +optional
	RequestKind *metav1.GroupVersionKind
	// RequestResource 是原始 API 请求的完整限定资源（例如 v1.pods）。
	// If this is specified and differs from the value in "resource", an equivalent match and conversion was performed.
	//
	// 例如，若 deployment 可通过 apps/v1 和 apps/v1beta1 修改，而 webhook 注册了如下规则：
	// `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]` 且 `matchPolicy: Equivalent`，
	// 则针对 apps/v1beta1 deployment 的 API 请求会被转换后发送给该 webhook。
	// 其中 `resource: {group:"apps", version:"v1", resource:"deployments"}` 匹配 webhook 注册的资源，
	// 而 `requestResource: {group:"apps", version:"v1beta1", resource:"deployments"}` 表示原始 API 请求的资源。
	//
	// 参见 webhook 配置类型中 "matchPolicy" 字段的文档。
	// +optional
	RequestResource *metav1.GroupVersionResource
	// RequestSubResource 是原始 API 请求中的子资源名称（如有），例如 "status" 或 "scale"。
	// If this is specified and differs from the value in "subResource", an equivalent match and conversion was performed.
	// 参见 webhook 配置类型中 "matchPolicy" 字段的文档。
	// +optional
	RequestSubResource string

	// Name 是请求中给出的对象名称。CREATE 操作时客户端可以省略 name，
	// 由服务端生成。此时该方法返回空字符串。
	// +optional
	Name string
	// Namespace 是请求关联的命名空间（如有）。
	// +optional
	Namespace string
	// Operation 是实际执行的操作，可能与请求中的操作不同。
	// 例如 patch 最终可能转化为 CREATE 或 UPDATE。
	Operation Operation
	// UserInfo 是发起请求的用户信息。
	UserInfo authentication.UserInfo
	// Object 来自传入的请求。
	// +optional
	Object runtime.Object
	// OldObject 是已存在的对象，仅在 DELETE 和 UPDATE 请求中填充。
	// +optional
	OldObject runtime.Object
	// DryRun 表示该请求的修改一定不会被持久化。
	// DryRun 为 true 时，对 webhook 的调用必须没有副作用。
	// 默认为 false。
	// +optional
	DryRun *bool
	// Options 是实际操作对应的操作选项结构。
	// 例如 `meta.k8s.io/v1.DeleteOptions` 或 `meta.k8s.io/v1.CreateOptions`。它可能
	// 与调用方提供的选项不同。例如 patch 请求最终执行的是
	// CREATE 操作，此时 Options 会是
	// `meta.k8s.io/v1.CreateOptions`，即使调用方提供的是 `meta.k8s.io/v1.PatchOptions`。
	// +optional
	Options runtime.Object
}

// AdmissionResponse describes an admission response.
type AdmissionResponse struct {
	// UID is an identifier for the individual request/response.
	// 应从对应的 AdmissionRequest 中原样复制。
	UID types.UID
	// Allowed 表示准入请求是否被允许。
	Allowed bool
	// Result 包含准入请求被拒绝的详细原因。
	// 当 "Allowed" 为 "true" 时，不会以任何方式读取该字段。
	// +optional
	Result *metav1.Status
	// Patch 包含实际的补丁。目前仅支持 RFC 6902 的 JSONPatch 形式。
	// +optional
	Patch []byte
	// PatchType 表示 Patch 的形式。目前仅支持 "JSONPatch"。
	// +optional
	PatchType *PatchType
	// AuditAnnotations 是由远程准入控制器设置的非结构化键值映射（例如 error=image-blacklisted）。
	// MutatingAdmissionWebhook 和 ValidatingAdmissionWebhook 准入控制器会给 key 加上
	// webhook 名称前缀（例如 imagepolicy.example.com/error=image-blacklisted）。AuditAnnotations 由
	// 准入 webhook 提供，用于向该请求的审计日志补充上下文。
	// +optional
	AuditAnnotations map[string]string
	// warnings 是返回给发起请求的 API 客户端的警告消息列表。
	// 警告消息描述客户端应注意或修正的问题。
	// 警告尽量控制在 120 个字符以内。
	// 超过 256 个字符的警告或数量过多的警告可能被截断。
	// +optional
	Warnings []string
}

// PatchType 是表示变更对象所使用的补丁类型。
type PatchType string

// PatchType 常量。
const (
	PatchTypeJSONPatch PatchType = "JSONPatch"
)

// Operation 是需要做准入检查的资源操作类型。
type Operation string

// Operation 常量。
const (
	Create  Operation = "CREATE"
	Update  Operation = "UPDATE"
	Delete  Operation = "DELETE"
	Connect Operation = "CONNECT"
)
