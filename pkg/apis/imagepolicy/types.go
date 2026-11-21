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

package imagepolicy

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ImageReview 用于检查 Pod 中的一组镜像是否被允许运行。
type ImageReview struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	// Spec 保存待检查 Pod 的相关信息。
	Spec ImageReviewSpec

	// Status 由后端（webhook）填写，指明该 Pod 是否应被允许创建。
	Status ImageReviewStatus
}

// ImageReviewSpec 描述 Pod 创建请求中与镜像审查相关的内容。
type ImageReviewSpec struct {
	// Containers 是被创建 Pod 中每个容器信息的子集列表。
	Containers []ImageReviewContainerSpec
	// Annotations 是从 Pod 注解中提取出的键值对集合。
	// 仅包含匹配 `*.image-policy.k8s.io/*` 模式的键。
	// 是否以及如何解释这些注解由各 webhook 后端自行决定。
	Annotations map[string]string
	// Namespace 是 Pod 将要创建所在的命名空间。
	Namespace string
}

// ImageReviewContainerSpec 描述 Pod 创建请求中的单个容器。
type ImageReviewContainerSpec struct {
	// 镜像可以是 image:tag 或 image@SHA:012345679abcdef 的形式。
	Image string
	// 未来可能加入命令行覆盖、exec 健康检查命令等信息。
}

// ImageReviewStatus 是镜像审查对 Pod 创建请求给出的结果。
type ImageReviewStatus struct {
	// Allowed 表示所有镜像都被允许运行。
	Allowed bool
	// Reason 通常应为空，仅当 Allowed 为 false 时才可能有值。
	// 此时可包含一段简短的问题描述。
	// 向用户展示时，Kubernetes 可能会截断过长的错误信息。
	Reason string
	// AuditAnnotations 会通过 AddAnnotation 添加到准入控制器请求的属性对象中。
	// 其中的键不应带前缀。
	// （准入控制器会自动补上适当的前缀。）
	AuditAnnotations map[string]string
}
