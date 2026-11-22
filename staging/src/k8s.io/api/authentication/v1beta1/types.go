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

package v1beta1

import (
	"fmt"

	v1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.4
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=authentication.k8s.io,v1,TokenReview

// TokenReview 尝试将 token 认证为已知用户。
// 注意：kube-apiserver 中的 webhook token 认证插件可能会缓存 TokenReview 请求。
// +k8s:supportsSubresource="/status"
type TokenReview struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含待评估请求的信息。
	// +required
	Spec TokenReviewSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 由服务端填入，指示该 token 是否可以通过认证。
	// +optional
	Status TokenReviewStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// TokenReviewSpec 描述 token 认证请求。
type TokenReviewSpec struct {
	// token 是不透明的 bearer token。
	// +required
	Token string `json:"token,omitempty" protobuf:"bytes,1,opt,name=token"`
	// audiences 是资源服务器随 token 提供的、用于表明自身身份的标识符列表。
	// 支持 audience 的认证器会验证 token 是否至少面向列表中的一个 audience。
	// 若未提供 audience，则默认为 Kubernetes apiserver 的 audience。
	// +optional
	// +listType=atomic
	Audiences []string `json:"audiences,omitempty" protobuf:"bytes,2,rep,name=audiences"`
}

// TokenReviewStatus 是 token 认证请求的结果。
type TokenReviewStatus struct {
	// authenticated 表示该 token 与已知用户关联。
	// +optional
	Authenticated bool `json:"authenticated,omitempty" protobuf:"varint,1,opt,name=authenticated"`
	// user 是与所提供 token 关联的 UserInfo。
	// +optional
	User UserInfo `json:"user,omitempty" protobuf:"bytes,2,opt,name=user"`
	// audiences 是认证器选择的、同时兼容 TokenReview 和 token 的 audience 标识符。
	// 标识符可以是 TokenReviewSpec.audiences 与 token audience 交集中的任意标识符。
	// 设置了 spec.audiences 字段的 TokenReview API 客户端应校验 status.audiences
	// 中返回了兼容的 audience 标识符，以确认 TokenReview 服务端支持 audience。
	// 若 authenticated 为 "true" 但 status.audience 为空，则该 token 对
	// Kubernetes API 服务端的 audience 有效。
	// +optional
	// +listType=atomic
	Audiences []string `json:"audiences,omitempty" protobuf:"bytes,4,rep,name=audiences"`
	// error 表示无法校验该 token。
	// +optional
	Error string `json:"error,omitempty" protobuf:"bytes,3,opt,name=error"`
}

// UserInfo 包含实现 user.Info 接口所需的用户信息。
type UserInfo struct {
	// username 是在所有活跃用户中唯一标识该用户的名称。
	// +optional
	Username string `json:"username,omitempty" protobuf:"bytes,1,opt,name=username"`
	// uid 是跨时间唯一标识该用户的值。若该用户被删除后又添加了同名用户，
	// 二者将具有不同的 UID。
	// +optional
	UID string `json:"uid,omitempty" protobuf:"bytes,2,opt,name=uid"`
	// groups 是该用户所属组的名称。
	// +optional
	// +listType=atomic
	Groups []string `json:"groups,omitempty" protobuf:"bytes,3,rep,name=groups"`
	// extra 是认证器提供的任何附加信息。
	// +optional
	Extra map[string]ExtraValue `json:"extra,omitempty" protobuf:"bytes,4,rep,name=extra"`
}

// ExtraValue 对值进行包装，以便 protobuf 生成。
// +protobuf.nullable=true
// +protobuf.options.(gogoproto.goproto_stringer)=false
type ExtraValue []string

// String 返回 ExtraValue 的字符串表示。
func (t ExtraValue) String() string {
	return fmt.Sprintf("%v", []string(t))
}

// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.27

// SelfSubjectReview 包含 kube-apiserver 所掌握的关于发起请求的用户的信息。
// 使用身份冒充时，用户将收到被冒充用户的信息。如果使用身份冒充或请求头认证，
// 任何额外键都会忽略大小写并以小写形式返回。
// +k8s:supportsSubresource="/status"
type SelfSubjectReview struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// status 由服务端填入用户属性。
	// +optional
	Status SelfSubjectReviewStatus `json:"status,omitempty" protobuf:"bytes,2,opt,name=status"`
}

// SelfSubjectReviewStatus 由 kube-apiserver 填入并返回给用户。
type SelfSubjectReviewStatus struct {
	// userInfo 是发起请求的用户的属性集合。
	// +optional
	UserInfo v1.UserInfo `json:"userInfo,omitempty" protobuf:"bytes,1,opt,name=userInfo"`
}
