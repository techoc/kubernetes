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

package v1

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// ImpersonateUserHeader 用于在 API 服务器请求期间模拟指定用户。
	ImpersonateUserHeader = "Impersonate-User"

	// ImpersonateGroupHeader 用于在 API 服务器请求期间模拟指定用户组。
	// 多个组可重复多次该请求头。
	ImpersonateGroupHeader = "Impersonate-Group"

	// ImpersonateUIDHeader 用于在 API 服务器请求期间模拟指定 UID。
	ImpersonateUIDHeader = "Impersonate-Uid"

	// ImpersonateUserExtraHeaderPrefix 是用于模拟 user.Info 中 extra map[string][]string
	// 条目的请求头前缀，前缀之后的全部内容即为键名。
	// 多个 map 键可重复多次该请求头；同一个键也可重复多次，以为该键下的切片提供多个元素。
	ImpersonateUserExtraHeaderPrefix = "Impersonate-Extra-"

	// AttestationAdmissionReviewAPIGroups 是 admissionReviewAPIGroups 声明的 map 键。
	// 它表示令牌授权其持有者查询准入 webhook 的 APIGroup。该键对应的值必须是长度为 1 的切片，
	// 且这唯一的元素必须与向 webhook 发出的 AdmissionReview 请求的 APIGroup 相符。
	// 值切片中唯一元素不能为空字符串。特殊值 "*" 表示"所有 API 组"，并要求具备相应权限（见下文）。
	//
	// 该声明要被视为有效，TokenRequest 必须满足两个条件。第一，BoundObjectRef 必须是
	// ValidatingWebhookConfiguration 或 MutatingWebhookConfiguration 之一；对应的 Webhook
	// Configuration 必须有一条 Rule 管辖该键值所指 APIGroup 下的某个资源。第二，请求的 audience
	// 必须匹配以下模式之一：
	//   1. webhook 配置了 URL 时，audience 必须与 URL 字段完全匹配。
	//   2. webhook 配置了 service 时，audience 必须匹配 `https://$name.$namespace.svc:$port[/$path]` 模式，
	//      其中 `/$path` 可选。
	//
	// 发起 TokenRequest 的 ServiceAccount 必须对组 "authentication.k8s.io"、资源
	// "admissionReviewAPIGroups" 拥有 "attest" 权限，且资源名恰好为 "*" 或该键值所指的 API 组。
	AttestationAdmissionReviewAPIGroups = "admissionReviewAPIGroups"
)

// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6

// TokenReview 尝试将令牌认证为已知用户。
// 注意：kube-apiserver 中的 webhook 令牌认证插件可能会缓存 TokenReview 请求。
// +k8s:supportsSubresource="/status"
type TokenReview struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 保存待评估请求的相关信息
	// +required
	Spec TokenReviewSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 由服务器填写，指示该请求能否通过认证。
	// +optional
	Status TokenReviewStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// TokenReviewSpec 描述令牌认证请求。
type TokenReviewSpec struct {
	// token 是不透明的持有者令牌。
	// +required
	Token string `json:"token,omitempty" protobuf:"bytes,1,opt,name=token"`
	// audiences 是资源服务器随令牌提供的标识符列表。支持 audience 的令牌认证器会验证
	// 令牌的预期 audience 是否至少与列表中的一个匹配。若未提供 audience，则默认为
	// Kubernetes apiserver 的 audience。
	// +optional
	// +listType=atomic
	Audiences []string `json:"audiences,omitempty" protobuf:"bytes,2,rep,name=audiences"`
}

// TokenReviewStatus 是令牌认证请求的结果。
type TokenReviewStatus struct {
	// authenticated 表示令牌与某个已知用户相关联。
	// +optional
	Authenticated bool `json:"authenticated,omitempty" protobuf:"varint,1,opt,name=authenticated"`
	// user 是与所提供令牌关联的 UserInfo。
	// +optional
	User UserInfo `json:"user,omitempty" protobuf:"bytes,2,opt,name=user"`
	// audiences 是认证器选出的、同时与 TokenReview 和令牌兼容的 audience 标识符。
	// 标识符取自 TokenReviewSpec 的 audiences 与令牌 audiences 的交集。
	// 设置了 spec.audiences 的 TokenReview API 客户端应校验 status.audiences 中返回了
	// 兼容的 audience 标识符，以确认 TokenReview 服务器支持 audience 感知。
	// 若 TokenReview 在 status.authenticated 为 "true" 时返回空的 status.audience 字段，
	// 则说明该令牌对 Kubernetes API 服务器的 audience 有效。
	// +optional
	// +listType=atomic
	Audiences []string `json:"audiences,omitempty" protobuf:"bytes,4,rep,name=audiences"`
	// error 表示无法校验令牌
	// +optional
	Error string `json:"error,omitempty" protobuf:"bytes,3,opt,name=error"`
}

// UserInfo 保存实现 user.Info 接口所需的用户信息。
type UserInfo struct {
	// username 是在所有活跃用户中唯一标识该用户的名称。
	// +optional
	Username string `json:"username,omitempty" protobuf:"bytes,1,opt,name=username"`
	// uid 是跨时间唯一标识该用户的值。若该用户被删除后又添加了同名用户，
	// 两者的 UID 会不同。
	// +optional
	UID string `json:"uid,omitempty" protobuf:"bytes,2,opt,name=uid"`
	// groups 是该用户所属组的名称列表。
	// +optional
	// +listType=atomic
	Groups []string `json:"groups,omitempty" protobuf:"bytes,3,rep,name=groups"`
	// extra 是认证器提供的任何附加信息。
	// +optional
	Extra map[string]ExtraValue `json:"extra,omitempty" protobuf:"bytes,4,rep,name=extra"`
}

// ExtraValue 对值做包装，以便 protobuf 生成代码。
// +protobuf.nullable=true
// +protobuf.options.(gogoproto.goproto_stringer)=false
type ExtraValue []string

// String 返回 ExtraValue 的切片格式化字符串。
func (t ExtraValue) String() string {
	return fmt.Sprintf("%v", []string(t))
}

// AttestationValue 对值做包装，以便 protobuf 生成代码。
// +protobuf.nullable=true
// +protobuf.options.(gogoproto.goproto_stringer)=false
type AttestationValue []string

// String 返回 AttestationValue 的切片格式化字符串。
func (a AttestationValue) String() string {
	return fmt.Sprintf("%v", []string(a))
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.10

// TokenRequest 为指定的 ServiceAccount 请求令牌。
type TokenRequest struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 保存待评估请求的相关信息
	// +optional
	Spec TokenRequestSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 由服务器填写，指示该令牌能否通过认证。
	// +optional
	Status TokenRequestStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// TokenRequestSpec 包含客户端提供的令牌请求参数。
type TokenRequestSpec struct {
	// audiences 是令牌的预期 audience。令牌接收方必须用令牌 audience 列表中的某个标识符
	// 表明自己的身份，否则应拒绝该令牌。为多个 audience 签发的令牌可用于向其中任意一个
	// audience 认证，但这意味着目标 audience 之间存在高度信任。
	// +optional
	// +listType=atomic
	Audiences []string `json:"audiences" protobuf:"bytes,1,rep,name=audiences"`

	// expirationSeconds 是请求的有效期时长。令牌签发方可能返回有效期不同的令牌，
	// 因此客户端需要检查响应中的 'expiration' 字段。
	// +optional
	ExpirationSeconds *int64 `json:"expirationSeconds" protobuf:"varint,4,opt,name=expirationSeconds"`

	// boundObjectRef 指向令牌将要绑定的对象。令牌仅在绑定对象存在期间有效。
	// 注意：API 服务器的 TokenReview 端点会校验 BoundObjectRef，但其他 audience 未必会。
	// 若希望及时吊销令牌，请将 ExpirationSeconds 设小一些。
	// +optional
	BoundObjectRef *BoundObjectReference `json:"boundObjectRef" protobuf:"bytes,3,opt,name=boundObjectRef"`

	// attestations 是已知键到字符串切片值的映射。
	// 每个键的值都有特定语义，文档见对应键的定义。令牌请求者可请求 Kubernetes API 服务器
	// 对某些声明作出证明。API 服务器可能会根据该 map 的键执行授权检查。
	// +optional
	Attestations map[string]AttestationValue `json:"attestations,omitempty" protobuf:"bytes,5,rep,name=attestations"`
}

// TokenRequestStatus 是令牌请求的结果。
type TokenRequestStatus struct {
	// token 是不透明的持有者令牌。
	// +optional
	Token string `json:"token" protobuf:"bytes,1,opt,name=token"`
	// expirationTimestamp 是所返回令牌的过期时间。
	// +optional
	ExpirationTimestamp metav1.Time `json:"expirationTimestamp" protobuf:"bytes,2,opt,name=expirationTimestamp"`
}

// BoundObjectReference 指向令牌所绑定的对象。
type BoundObjectReference struct {
	// 被引用对象的 kind。有效值包括 'Pod'、'Secret'、'Node'、
	// 'ValidatingWebhookConfiguration' 和 'MutatingWebhookConfiguration'。
	// +optional
	Kind string `json:"kind,omitempty" protobuf:"bytes,1,opt,name=kind"`
	// apiVersion 是被引用对象的 API 版本。
	// +optional
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,2,opt,name=apiVersion"`

	// 被引用对象的 name。
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,3,opt,name=name"`
	// 被引用对象的 uid。
	// +optional
	UID types.UID `json:"uid,omitempty" protobuf:"bytes,4,opt,name=uID,casttype=k8s.io/apimachinery/pkg/types.UID"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.28

// SelfSubjectReview 包含 kube-apiserver 关于发起请求用户的用户信息。
// 使用模拟时，用户会收到被模拟用户的用户信息。若使用模拟或请求头认证，
// 所有 extra 键将忽略大小写并以小写形式返回。
// +k8s:supportsSubresource="/status"
type SelfSubjectReview struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// status 由服务器填写，包含用户属性。
	// +optional
	Status SelfSubjectReviewStatus `json:"status,omitempty" protobuf:"bytes,2,opt,name=status"`
}

// SelfSubjectReviewStatus 由 kube-apiserver 填写并返回给用户。
type SelfSubjectReviewStatus struct {
	// userInfo 是发起请求用户的一组属性。
	// +optional
	UserInfo UserInfo `json:"userInfo,omitempty" protobuf:"bytes,1,opt,name=userInfo"`
}
