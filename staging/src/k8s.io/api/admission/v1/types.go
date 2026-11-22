/*
Copyright 2019 The Kubernetes Authors.

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

package v1

import (
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// AdmissionReview 描述一次准入审查的请求/响应
type AdmissionReview struct {
	metav1.TypeMeta `json:""`
	// request 描述准入请求的属性
	// +optional
	Request *AdmissionRequest `json:"request,omitempty" protobuf:"bytes,1,opt,name=request"`
	// response 描述准入响应的属性
	// +optional
	Response *AdmissionResponse `json:"response,omitempty" protobuf:"bytes,2,opt,name=response"`
}

// AdmissionRequest 描述准入请求的 admission.Attributes
type AdmissionRequest struct {
	// uid 是单次请求/响应的标识符，用于区分那些本身完全相同（并发请求、先前请求未产生修改等）的请求实例
	//
	// 该 UID 用于跟踪 KAS 与 Webhook 之间的一次往返（请求/响应），而非用户请求本身
	// 它适合用于关联 webhook 与 apiserver 两端的日志，便于审计或调试
	// +optional
	UID types.UID `json:"uid" protobuf:"bytes,1,opt,name=uid"`
	// kind 是提交对象的完整限定类型（例如 v1.Pod 或 autoscaling.v1.Scale）
	// +optional
	Kind metav1.GroupVersionKind `json:"kind" protobuf:"bytes,2,opt,name=kind"`
	// resource 是被请求资源的完整限定名称（例如 v1.pods）
	// +optional
	Resource metav1.GroupVersionResource `json:"resource" protobuf:"bytes,3,opt,name=resource"`
	// subResource 是被请求的子资源（如有），例如 "status" 或 "scale"
	// +optional
	SubResource string `json:"subResource,omitempty" protobuf:"bytes,4,opt,name=subResource"`

	// requestKind 是原始 API 请求中对象的完整限定类型（例如 v1.Pod 或 autoscaling.v1.Scale）
	// 若填写此值且与 "kind" 不同，说明曾执行等价匹配与转换
	//
	// 例如，若 deployment 可通过 apps/v1 和 apps/v1beta1 修改，而 webhook 注册了如下规则
	// `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]` and `matchPolicy: Equivalent`,
	// 对 apps/v1beta1 deployment 的 API 请求会被转换后发送给 webhook，携带
	// `kind: {group:"apps", version:"v1", kind:"Deployment"}`（匹配 webhook 注册的规则），
	// 以及 `requestKind: {group:"apps", version:"v1beta1", kind:"Deployment"}`（表示原始 API 请求中的类型）
	//
	// 更多细节参见 webhook 配置类型中 "matchPolicy" 字段的文档
	// +optional
	RequestKind *metav1.GroupVersionKind `json:"requestKind,omitempty" protobuf:"bytes,13,opt,name=requestKind"`
	// requestResource 是原始 API 请求中资源的完整限定名称（例如 v1.pods）
	// 若填写此值且与 "resource" 不同，说明曾执行等价匹配与转换
	//
	// 例如，若 deployment 可通过 apps/v1 和 apps/v1beta1 修改，而 webhook 注册了如下规则
	// `apiGroups:["apps"], apiVersions:["v1"], resources: ["deployments"]` and `matchPolicy: Equivalent`,
	// 对 apps/v1beta1 deployment 的 API 请求会被转换后发送给 webhook，携带
	// `resource: {group:"apps", version:"v1", resource:"deployments"}`（匹配 webhook 注册的资源），
	// 以及 `requestResource: {group:"apps", version:"v1beta1", resource:"deployments"}`（表示原始 API 请求中的资源）
	//
	// 参见 webhook 配置类型中 "matchPolicy" 字段的文档
	// +optional
	RequestResource *metav1.GroupVersionResource `json:"requestResource,omitempty" protobuf:"bytes,14,opt,name=requestResource"`
	// requestSubResource 是原始 API 请求中子资源的名称（如有），例如 "status" 或 "scale"
	// 若填写此值且与 "subResource" 不同，说明曾执行等价匹配与转换
	// 参见 webhook 配置类型中 "matchPolicy" 字段的文档
	// +optional
	RequestSubResource string `json:"requestSubResource,omitempty" protobuf:"bytes,15,opt,name=requestSubResource"`

	// name 是请求中给出的对象名称。CREATE 操作时客户端可以省略名称，
	// 由服务端生成；此时本字段为空字符串
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,5,opt,name=name"`
	// namespace 是请求关联的命名空间（如有）
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,6,opt,name=namespace"`
	// operation 是实际执行的操作，可能与请求的操作不同。
	// 例如 patch 可能导致 CREATE 或 UPDATE 操作
	// +optional
	Operation Operation `json:"operation" protobuf:"bytes,7,opt,name=operation"`
	// userInfo 是发起请求的用户信息
	// +optional
	UserInfo authenticationv1.UserInfo `json:"userInfo" protobuf:"bytes,8,opt,name=userInfo"`
	// object 是传入请求中的对象
	// +optional
	Object runtime.RawExtension `json:"object,omitempty" protobuf:"bytes,9,opt,name=object"`
	// oldObject 是现存对象，仅 DELETE 和 UPDATE 请求会填充
	// +optional
	OldObject runtime.RawExtension `json:"oldObject,omitempty" protobuf:"bytes,10,opt,name=oldObject"`
	// dryRun 表示本请求的修改一定不会被持久化
	// 默认为 false
	// +optional
	DryRun *bool `json:"dryRun,omitempty" protobuf:"varint,11,opt,name=dryRun"`
	// options 是实际执行操作所对应的选项结构
	// 例如 `meta.k8s.io/v1.DeleteOptions` 或 `meta.k8s.io/v1.CreateOptions`。它可能
	// 与调用方提供的选项不同。例如 patch 请求实际执行的
	// 操作可能是 CREATE，此时 Options 为
	// `meta.k8s.io/v1.CreateOptions`，即使调用方提供的是 `meta.k8s.io/v1.PatchOptions`
	// +optional
	Options runtime.RawExtension `json:"options,omitempty" protobuf:"bytes,12,opt,name=options"`
}

// AdmissionResponse 描述准入响应
type AdmissionResponse struct {
	// uid 是单次请求/响应的标识符
	// 必须原样复制自相应的 AdmissionRequest
	// +optional
	UID types.UID `json:"uid" protobuf:"bytes,1,opt,name=uid"`

	// allowed 表示准入请求是否被允许
	// +optional
	Allowed bool `json:"allowed" protobuf:"varint,2,opt,name=allowed"`

	// status 包含准入请求被拒绝原因的额外详情
	// 当 "Allowed" 为 "true" 时，完全不会读取本字段
	// +optional
	Result *metav1.Status `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`

	// patch 是补丁内容。目前只支持实现 RFC 6902 的 "JSONPatch"
	// +optional
	Patch []byte `json:"patch,omitempty" protobuf:"bytes,4,opt,name=patch"`

	// patchType 是补丁类型，目前只允许 "JSONPatch"
	// +optional
	PatchType *PatchType `json:"patchType,omitempty" protobuf:"bytes,5,opt,name=patchType"`

	// auditAnnotations 是远程准入控制器设置的非结构化键值映射（如 error=image-blacklisted）
	// MutatingAdmissionWebhook 和 ValidatingAdmissionWebhook 准入控制器会为键加上 webhook 名称前缀
	// （如 imagepolicy.example.com/error=image-blacklisted）。webhook 提供 AuditAnnotations，
	// 用于在该请求的审计日志中补充上下文
	// +optional
	AuditAnnotations map[string]string `json:"auditAnnotations,omitempty" protobuf:"bytes,6,opt,name=auditAnnotations"`

	// warnings 是返回给 API 客户端的警告消息列表
	// 警告消息描述发起 API 请求的客户端应当修正或注意的问题
	// 警告消息尽量不超过 120 个字符
	// 超过 256 个字符的警告，或警告数量过多时，可能被截断
	// +optional
	// +listType=atomic
	Warnings []string `json:"warnings,omitempty" protobuf:"bytes,7,rep,name=warnings"`
}

// PatchType 表示变更对象所用补丁的类型
type PatchType string

// PatchType 常量
const (
	// PatchTypeJSONPatch 表示 RFC 6902 定义的 JSONPatch 补丁
	PatchTypeJSONPatch PatchType = "JSONPatch"
)

// Operation 是准入控制所检查的资源操作类型
type Operation string

// Operation 常量
const (
	// Create 表示创建资源的准入操作
	Create Operation = "CREATE"
	// Update 表示更新资源的准入操作
	Update Operation = "UPDATE"
	// Delete 表示删除资源的准入操作
	Delete Operation = "DELETE"
	// Connect 表示连接资源的准入操作
	Connect Operation = "CONNECT"
)
