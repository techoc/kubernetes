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

package scheme

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 本文件定义自有的内部 Scale 版本用于转换，因为无法使用 Kubernetes 主仓库的内部版本。
// 这里自定义内部 Scale 类型，是因为 client-go 不能依赖 k8s.io/kubernetes 的内部
// API 包；转换器以本包类型作为中间表示。

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Scale 表示对资源的伸缩请求。
type Scale struct {
	metav1.TypeMeta
	// 标准对象元数据；详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata。
	// +optional
	metav1.ObjectMeta

	// 定义 Scale 的行为；详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status。
	// +optional
	Spec ScaleSpec

	// Scale 的当前状态，只读；详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status。
	// +optional
	Status ScaleStatus
}

// ScaleSpec 描述 scale 子资源的属性。
type ScaleSpec struct {
	// 被伸缩对象期望的实例数。
	// +optional
	Replicas int32
}

// ScaleStatus 表示 scale 子资源的当前状态。
type ScaleStatus struct {
	// 实际观测到的被伸缩对象实例数。
	Replicas int32

	// 用于筛选应计入副本数的 Pod 的标签查询。
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// 内部表示统一使用结构化 LabelSelector，旧版本的字符串 TargetSelector 在转换时解析。
	// +optional
	Selector *metav1.LabelSelector
}
