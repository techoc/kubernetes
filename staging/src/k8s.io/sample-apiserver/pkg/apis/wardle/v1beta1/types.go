/*
Copyright 2018 The Kubernetes Authors.

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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// FlunderList 是 Flunder 对象的列表。
// 列表自身的 TypeMeta 让客户端能按版本解码整批结果。
type FlunderList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []Flunder `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ReferenceType 表示对象引用的目标类型。
// 与 v1alpha1 用指针字段不同，这里是非指针字段，依赖 defaulting 在转换时补齐。
type ReferenceType string

const (
	// FlunderReferenceType 表示引用指向 Flunder。
	// 常量值即序列化到 API 的字符串，改动会破坏已有对象。
	FlunderReferenceType = ReferenceType("Flunder")
	// FischerReferenceType 表示引用指向 Fischer，后者是集群级资源。
	FischerReferenceType = ReferenceType("Fischer")
)

// FlunderSpec 定义 Flunder 的期望状态。
// 两个引用字段分开表示，便于从 v1alpha1 的单一 Reference 字段做显式转换。
type FlunderSpec struct {
	// FlunderReference 是另一个 Flunder 的名字，与 FischerReference 互斥。
	// 互斥由校验层保证，结构体本身不编码约束。
	FlunderReference string `json:"flunderReference,omitempty" protobuf:"bytes,1,opt,name=flunderReference"`
	// FischerReference 是一个 Fischer 的名字，与 FlunderReference 互斥。
	FischerReference string `json:"fischerReference,omitempty" protobuf:"bytes,2,opt,name=fischerReference"`
	// ReferenceType 表示引用目标的类型。
	// 需与上面两个字段保持一致，空值由转换逻辑从参考字段推导。
	ReferenceType ReferenceType `json:"referenceType,omitempty" protobuf:"bytes,3,opt,name=referenceType"`
}

// FlunderStatus 保存 Flunder 的观测状态。
// 当前为空，保留结构以便日后添加状态字段而不改变对象布局。
type FlunderStatus struct {
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Flunder 是包含 Spec 和 Status 的示例资源。
// Spec 由用户提交，Status 留给控制器回写。
type Flunder struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 保存用户声明的期望状态。
	Spec FlunderSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// Status 预留给控制器回写观测状态。
	Status FlunderStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}
