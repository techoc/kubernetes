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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Foo 描述一个由控制器管理的 Deployment 及其期望状态。
// 一个 Foo 对应一个由控制器管理的 Deployment；期望状态放 Spec，观测结果放 Status。
// 指令 +genclient 让生成器产出该资源的客户端，指令 +k8s:deepcopy-gen:interfaces 让它实现 runtime.Object，从而能装进 informer。
// 内嵌 TypeMeta 让 Foo 自带 API 版本信息；内嵌 ObjectMeta 让 Foo 拥有名字、命名空间等元数据。
// Spec 是用户声明的期望状态，控制器只读不写；Status 是控制器写入的观测结果。
type Foo struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec 是用户声明的期望状态，控制器只读不写。
	Spec FooSpec `json:"spec"`
	// Status 是控制器写入的观测结果。
	Status FooStatus `json:"status"`
}

// FooSpec 定义 Foo 的期望状态。
// Replicas 用指针而非 int32，是为了区分“用户没填”和“显式填 0”：
// 前者沿用 Deployment 的默认值，后者才真正要求缩容到零。
type FooSpec struct {
	// DeploymentName 是目标 Deployment 的名字，必填；空值会让控制器跳过该 Foo。
	DeploymentName string `json:"deploymentName"`
	// Replicas 是期望副本数；nil 表示由 Deployment 侧决定。
	Replicas *int32 `json:"replicas"`
}

// FooStatus 保存 Foo 的观测状态。
// 只暴露一个观测值，避免状态结构随业务膨胀；需要更多指标时再按版本演进添加。
type FooStatus struct {
	// AvailableReplicas 直接复用 Deployment 的可用副本数，不自行统计 Pod。
	AvailableReplicas int32 `json:"availableReplicas"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// FooList 是 Foo 资源的列表。
// List 类型主要服务于 client-go 的 list/watch 机制，生成器依赖它实现 Foo 的列表操作。
type FooList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Foo `json:"items"`
}
