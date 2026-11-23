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

// FlunderList is a list of Flunder objects.
// 列表自身的 TypeMeta 让客户端能按版本解码整批结果。
type FlunderList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []Flunder `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ReferenceType defines the type of an object reference.
// 与 v1alpha1 用指针字段不同，这里是非指针字段，依赖 defaulting 在转换时补齐。
type ReferenceType string

const (
	// FlunderReferenceType is used for Flunder references.
	// 常量值即序列化到 API 的字符串，改动会破坏已有对象。
	FlunderReferenceType = ReferenceType("Flunder")
	// FischerReferenceType is used for Fischer references.
	FischerReferenceType = ReferenceType("Fischer")
)

// FlunderSpec is the specification of a Flunder.
// 两个引用字段分开表示，便于从 v1alpha1 的单一 Reference 字段做显式转换。
type FlunderSpec struct {
	// A name of another flunder, mutually exclusive to the FischerReference.
	// 互斥由校验层保证，结构体本身不编码约束。
	FlunderReference string `json:"flunderReference,omitempty" protobuf:"bytes,1,opt,name=flunderReference"`
	// A name of a fischer, mutually exclusive to the FlunderReference.
	FischerReference string `json:"fischerReference,omitempty" protobuf:"bytes,2,opt,name=fischerReference"`
	// The reference type.
	// 需与上面两个字段保持一致，空值由转换逻辑从参考字段推导。
	ReferenceType ReferenceType `json:"referenceType,omitempty" protobuf:"bytes,3,opt,name=referenceType"`
}

// FlunderStatus is the status of a Flunder.
// 当前为空，保留结构以便日后添加状态字段而不改变对象布局。
type FlunderStatus struct {
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Flunder is an example type with a spec and a status.
type Flunder struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   FlunderSpec   `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	Status FlunderStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}
