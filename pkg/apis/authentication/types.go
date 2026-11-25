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

	// AttestationAdmissionReviewAPIGroups is the map key for the
	// admissionReviewAPIGroups claim. It represents the APIGroup that a token
	// authorizes its bearer to query admission webhooks about. The value
	// corresponding to this key must be a slice of length 1, and the first and
	// only element of this slice must match the APIGroup of the AdmissionReview
	// request being made of the webhook. The empty string is invalid as the
	// first and only element of the value slice. The special value "*" means
	// "all api groups", and requires matching permissions (described below).
	//
	// For this claim to be considered valid, the TokenRequest must meet two
	// conditions. First, the BoundObjectRef must be one of
	// ValidatingWebhookConfiguration or MutatingWebhookConfiguration; the
	// Webhook Configuration in question must have a Rule governing a resource
	// under the APIGroup named in the value to this key. Second, the requested
	// audience must match one of the following patterns:
	//   1. When the webhook is configured with a URL, the audience must match
	//      the URL field exactly.
	//   2. When the webhook is configured with a service, the audience must
	//      match the pattern `https://$name.$namespace.svc:$port[/$path]`, where
	//      `/$path` is optional.
	//
	// The service account for which the TokenRequest is being made must have
	// "attest" permissions on group "authentication.k8s.io", resource
	// "admissionReviewAPIGroups, and a resource name matching exactly either
	// "*", or the api group named in the value to this key.
	AttestationAdmissionReviewAPIGroups = "admissionReviewAPIGroups"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TokenReview attempts to authenticate a token to a known user.
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

// TokenReviewStatus 是令牌认证请求的结果。
// This type mirrors the authentication.Token interface
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

// UserInfo holds the information about the user needed to implement the
// user.Info interface.
type UserInfo struct {
	// username 是在所有活跃用户中唯一标识该用户的名称。
	Username string
	// A unique value that identifies this user across time. If this user is
	// deleted and another user by the same name is added, they will have
	// different UIDs.
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

	// ExpirationSeconds is the requested duration of validity of the request. The
	// token issuer may return a token with a different validity duration so a
	// client needs to check the 'expiration' field in a response.
	ExpirationSeconds int64

	// BoundObjectRef is a reference to an object that the token will be bound to.
	// The token will only be valid for as long as the bound object exists.
	// NOTE: The API server's TokenReview endpoint will validate the
	// BoundObjectRef, but other audiences may not. Keep ExpirationSeconds
	// small if you want prompt revocation.
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
	// ExpirationTimestamp is the time of expiration of the returned token.
	ExpirationTimestamp metav1.Time
}

// BoundObjectReference 指向令牌所绑定的对象。
type BoundObjectReference struct {
	// Kind of the referent. Valid kinds are 'Pod' and 'Secret'.
	Kind string
	// API version of the referent.
	APIVersion string

	// 被引用对象的 name。
	Name string
	// UID of the referent.
	UID types.UID
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SelfSubjectReview 包含 kube-apiserver 所掌握的关于发起请求的用户的信息。
// 使用身份冒充时，用户将收到被冒充用户的信息。如果使用身份冒充或请求头认证，
// 任何额外键都会忽略大小写并以小写形式返回。
type SelfSubjectReview struct {
	metav1.TypeMeta
	// ObjectMeta fulfills the metav1.ObjectMetaAccessor interface so that the stock.
	// REST handler paths work.
	metav1.ObjectMeta
	// status 由服务端填入用户属性。
	Status SelfSubjectReviewStatus
}

// SelfSubjectReviewStatus 由 kube-apiserver 填入并返回给用户。
type SelfSubjectReviewStatus struct {
	// User attributes of the user making this request.
	UserInfo UserInfo
}
