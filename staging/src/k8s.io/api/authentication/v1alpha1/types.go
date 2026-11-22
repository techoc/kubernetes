/*
Copyright 2022 The Kubernetes Authors.

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

package v1alpha1

import (
	v1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +genclient:onlyVerbs=create
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.26

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
