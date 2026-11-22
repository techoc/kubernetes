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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +genclient:noVerbs
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ImageReview 检查 Pod 中的镜像集合是否被允许使用。
// +k8s:supportsSubresource="/status"
type ImageReview struct {
	metav1.TypeMeta `json:""`
	// metadata 为标准对象元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含待评估 Pod 的信息。
	// +optional
	Spec ImageReviewSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 由后端填充，指示是否允许该 Pod 运行。
	// +optional
	Status ImageReviewStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ImageReviewSpec 描述 Pod 创建请求。
type ImageReviewSpec struct {
	// containers 为待创建 Pod 中各容器信息的子集列表。
	// +optional
	// +listType=atomic
	Containers []ImageReviewContainerSpec `json:"containers,omitempty" protobuf:"bytes,1,rep,name=containers"`
	// annotations 为从 Pod 注解中提取的键值对列表。
	// 仅包含匹配 `*.image-policy.k8s.io/*` 模式的键。
	// 是否解释及如何解释这些注解由各 webhook 后端自行决定。
	// +optional
	Annotations map[string]string `json:"annotations,omitempty" protobuf:"bytes,2,rep,name=annotations"`
	// namespace 为 Pod 将要创建于其中的命名空间。
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,3,opt,name=namespace"`
}

// ImageReviewContainerSpec 描述 Pod 创建请求中的一个容器。
type ImageReviewContainerSpec struct {
	// image 可采用 image:tag 或 image@SHA:012345679abcdef 形式。
	// +optional
	Image string `json:"image,omitempty" protobuf:"bytes,1,opt,name=image"`
	// 未来可能加入命令行覆盖、exec 健康检查命令等内容。
}

// ImageReviewStatus 为 Pod 创建请求的审查结果。
type ImageReviewStatus struct {
	// allowed 表示所有镜像均被允许运行。
	// +optional
	Allowed bool `json:"allowed" protobuf:"varint,1,opt,name=allowed"`
	// reason 通常为空，除非 Allowed 为 false，此时可简要说明问题所在。
	// Kubernetes 向用户展示时可能截断过长的错误信息。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,2,opt,name=reason"`
	// auditAnnotations 将通过 'AddAnnotation' 添加到准入控制器请求的属性对象中。
	// 键不应带前缀（准入控制器会添加适当的前缀）。
	// +optional
	AuditAnnotations map[string]string `json:"auditAnnotations,omitempty" protobuf:"bytes,3,rep,name=auditAnnotations"`
}
