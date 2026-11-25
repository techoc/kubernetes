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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0
// +k8s:prerelease-lifecycle-gen:removed=1.10

// FlunderList 是 Flunder 对象的列表。
// 列表对象需要自带 TypeMeta，客户端才能按版本解码 whole-list 响应。
type FlunderList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []Flunder `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ReferenceType 决定 Reference 指向 Flunder 还是 Fischer，取空值时由默认化逻辑补齐。
type ReferenceType string

const (
	// FlunderReferenceType 表示引用指向另一个 Flunder，客户端可直接比较字符串值。
	FlunderReferenceType = ReferenceType("Flunder")
	// FischerReferenceType 表示引用指向 Fischer，后者是集群级资源。
	FischerReferenceType = ReferenceType("Fischer")
)

// FlunderSpec 演示同一字段按类型引用两种资源，以及指针字段承载默认值语义。
type FlunderSpec struct {
	// Reference 保存目标 Flunder 或 Fischer 的名字，具体类型由 ReferenceType 决定。
	// 这里只存名字，不存对象引用，避免跨版本序列化耦合。
	Reference string `json:"reference,omitempty" protobuf:"bytes,1,opt,name=reference"`
	// ReferenceType 表示引用目标的类型；设置 Reference 时默认值为 Flunder。
	// 用指针区分“未设置”和“显式设为空”，默认化时才能只在缺省情况下填入 Flunder。
	ReferenceType *ReferenceType `json:"referenceType,omitempty" protobuf:"bytes,2,opt,name=referenceType"`
}

// FlunderStatus 目前为空，占位以便后续加入状态子资源而不改对象结构。
type FlunderStatus struct {
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0
// +k8s:prerelease-lifecycle-gen:removed=1.10

// Flunder 是示例里的带命名空间资源，用 +genclient 生成常规客户端。
type Flunder struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 保存用户声明的期望状态。
	Spec FlunderSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// Status 预留给控制器回写观测状态。
	Status FlunderStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0
// +k8s:prerelease-lifecycle-gen:removed=1.10

// Fischer 是集群级资源，banflunder 准入插件从它的名单里读取被禁止的 Flunder 名字。
type Fischer struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// DisallowedFlunders 保存被禁止使用的 Flunder 名字。
	// 指令 +listType=atomic 表示整段列表作为一个整体更新，避免并发写时按元素合并出半新半旧的名单。
	// +listType=atomic
	DisallowedFlunders []string `json:"disallowedFlunders,omitempty" protobuf:"bytes,2,rep,name=disallowedFlunders"`
}

// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0
// +k8s:prerelease-lifecycle-gen:removed=1.10

// FischerList 是 Fischer 对象的列表。
// 集群级资源的列表同样需要 TypeMeta，便于不带 context 的 list 调用解码。
type FischerList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []Fischer `json:"items" protobuf:"bytes,2,rep,name=items"`
}
