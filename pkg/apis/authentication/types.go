/*
Copyright 2016 The Kubernetes Authors.

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

package authentication

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// ImpersonateUserHeader 用于在 API 服务器请求期间模拟指定用户。
	ImpersonateUserHeader = "Impersonate-User"

	// ImpersonateUIDHeader 用于在 API 服务器请求期间模拟指定 UID。
	ImpersonateUIDHeader = "Impersonate-Uid"

	// ImpersonateGroupHeader 用于在 API 服务器请求期间模拟指定用户组。
	// 多个组可重复多次该请求头。
	ImpersonateGroupHeader = "Impersonate-Group"

	// ImpersonateUserExtraHeaderPrefix 是用于模拟 user.Info 中 extra map[string][]string
	// 条目的请求头前缀，前缀之后的全部内容即为键名。
	// 多个 map 键可重复多次该请求头；同一个键也可重复多次，以为该键下的切片提供多个元素。
	ImpersonateUserExtraHeaderPrefix = "Impersonate-Extra-"

	// AttestationAdmissionReviewAPIGroups 是 admissionReviewAPIGroups 声明的 map 键。
	// 它表示该令牌授权其持有者可以就哪些 APIGroup 查询准入 webhook。与该键对应的值
	// 必须是长度为 1 的切片，并且该切片的第一个也是唯一一个元素必须与向该
	// webhook 发起的 AdmissionReview 请求的 APIGroup 匹配。值切片的第一个也是唯一一个
	// 元素不能为空字符串。特殊值 "*" 表示"所有 api group"，
	// 并要求具备匹配的权限（见下文描述）。
	//
	// 要使该声明被视为合法，TokenRequest 必须满足两个条件。
	// 第一，BoundObjectRef 必须是 ValidatingWebhookConfiguration 或
	// MutatingWebhookConfiguration 之一；相应的 Webhook Configuration 必须有一条
	// Rule 管辖该键值中所指 APIGroup 下的资源。第二，请求的 audience 必须匹配
	// 以下模式之一：
	//   1. 当 webhook 以 URL 配置时，audience 必须与 URL 字段完全一致。
	//   2. 当 webhook 以 service 配置时，audience 必须匹配模式
	//      `https://$name.$namespace.svc:$port[/$path]`，其中 `/$path` 是可选的。
	//
	// 发起 TokenRequest 的 service account 必须在组 "authentication.k8s.io"、
	// 资源 "admissionReviewAPIGroups 上拥有 "attest" 权限，并且资源名称必须与
	// "*" 或该键值中所指的 api group 完全一致。
	AttestationAdmissionReviewAPIGroups = "admissionReviewAPIGroups"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TokenReview 尝试将某个令牌认证为已知用户。
type TokenReview struct {
	metav1.TypeMeta
	// ObjectMeta 实现 metav1.ObjectMetaAccessor 接口，以便标准
	// REST handler 路径能够正常工作。
	metav1.ObjectMeta

	// spec 包含待评估请求的信息。
	Spec TokenReviewSpec

	// status 由服务器填写，指示该请求能否通过认证。
	Status TokenReviewStatus
}

// TokenReviewSpec 描述令牌认证请求。
type TokenReviewSpec struct {
	// Token 是不透明的 bearer token。
	Token string `datapolicy:"token"`
	// audiences 是资源服务器随令牌提供的标识符列表。支持 audience 的令牌认证器会验证
	// 令牌的预期 audience 是否至少与列表中的一个匹配。若未提供 audience，则默认为
	// Kubernetes apiserver 的 audience。
	Audiences []string
}

// TokenReviewStatus 是令牌认证请求的结果。该类型与 authentication.Token 接口相对应。
type TokenReviewStatus struct {
	// authenticated 表示令牌与某个已知用户相关联。
	Authenticated bool
	// user 是与所提供令牌关联的 UserInfo。
	User UserInfo
	// audiences 是认证器选出的、同时与 TokenReview 和令牌兼容的 audience 标识符。
	// 标识符取自 TokenReviewSpec 的 audiences 与令牌 audiences 的交集。
	// 设置了 spec.audiences 的 TokenReview API 客户端应校验 status.audiences 中返回了
	// 兼容的 audience 标识符，以确认 TokenReview 服务器支持 audience 感知。
	// 若 TokenReview 在 status.authenticated 为 "true" 时返回空的 status.audience 字段，
	// 则说明该令牌对 Kubernetes API 服务器的 audience 有效。
	Audiences []string
	// error 表示无法校验该 token。
	Error string
}

// UserInfo 保存实现 user.Info 接口所需的用户信息。
type UserInfo struct {
	// username 是在所有活跃用户中唯一标识该用户的名称。
	Username string
	// UID 是在时间维度上唯一标识该用户的值。若该用户被删除后又添加了
	// 同名用户，两者的 UID 将不同。
	UID string
	// groups 是该用户所属组的名称。
	Groups []string
	// extra 是认证器提供的任何附加信息。
	Extra map[string]ExtraValue
}

// ExtraValue 对值做掩码，使 protobuf 能够生成
type ExtraValue []string

// AttestationValue 对值做包装，以便 protobuf 生成代码。
type AttestationValue []string

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TokenRequest 为指定的 ServiceAccount 请求令牌。
type TokenRequest struct {
	metav1.TypeMeta
	// ObjectMeta 实现 metav1.ObjectMetaAccessor 接口，以便标准
	// REST handler 路径能够正常工作。
	metav1.ObjectMeta

	Spec   TokenRequestSpec
	Status TokenRequestStatus
}

// TokenRequestSpec 包含客户端提供的令牌请求参数。
type TokenRequestSpec struct {
	// audiences 是令牌的预期 audience。令牌接收方必须用令牌 audience 列表中的某个标识符
	// 表明自己的身份，否则应拒绝该令牌。为多个 audience 签发的令牌可用于向其中任意一个
	// audience 认证，但这意味着目标 audience 之间存在高度信任。
	Audiences []string

	// ExpirationSeconds 是请求的令牌有效期时长。令牌签发方可能返回有效期不同的令牌，
	// 因此客户端需要检查响应中的 'expiration' 字段。
	ExpirationSeconds int64

	// BoundObjectRef 是对令牌所绑定对象的引用。只有被绑定对象存在期间令牌才有效。
	// NOTE: API 服务器的 TokenReview 端点会校验 BoundObjectRef，
	// 但其他 audience 可能不会。若希望快速吊销，请将 ExpirationSeconds
	// 保持在较小值。
	BoundObjectRef *BoundObjectReference

	// attestations 是已知键到字符串切片值的映射。
	// 每个键的值都有特定语义，文档见对应键的定义。令牌请求者可请求 Kubernetes API 服务器
	// 对某些声明作出证明。API 服务器可能会根据该 map 的键执行授权检查。
	Attestations map[string]AttestationValue
}

// TokenRequestStatus 是令牌请求的结果。
type TokenRequestStatus struct {
	// Token 是不透明的 bearer token。
	Token string `datapolicy:"token"`
	// ExpirationTimestamp 是返回令牌的过期时间。
	ExpirationTimestamp metav1.Time
}

// BoundObjectReference 指向令牌所绑定的对象。
type BoundObjectReference struct {
	// Kind 是被引用对象的类别。合法取值为 'Pod' 和 'Secret'。
	Kind string
	// APIVersion 是被引用对象的 API 版本。
	APIVersion string

	// 被引用对象的 name。
	Name string
	// UID 是被引用对象的 UID。
	UID types.UID
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SelfSubjectReview 包含 kube-apiserver 所掌握的关于发起请求的用户的信息。
// 使用身份冒充时，用户将收到被冒充用户的信息。如果使用身份冒充或请求头认证，
// 任何额外键都会忽略大小写并以小写形式返回。
type SelfSubjectReview struct {
	metav1.TypeMeta
	// ObjectMeta 实现 metav1.ObjectMetaAccessor 接口，以便标准
	// REST handler 路径能够正常工作。
	metav1.ObjectMeta
	// status 由服务端填入用户属性。
	Status SelfSubjectReviewStatus
}

// SelfSubjectReviewStatus 由 kube-apiserver 填入并返回给用户。
type SelfSubjectReviewStatus struct {
	// UserInfo 是发起该请求的用户的属性。
	UserInfo UserInfo
}
