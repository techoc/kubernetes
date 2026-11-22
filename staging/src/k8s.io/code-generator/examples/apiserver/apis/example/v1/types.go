/*
Copyright 2015 The Kubernetes Authors.

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

package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestType is a top-level type. A client is created for it.
type TestType struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +optional
	Status TestTypeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestTypeList is a top-level list type. The client methods for lists are automatically created.
// You are not supposed to create a separated client for this one.
type TestTypeList struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []TestType `json:"items"`
}

// TestTypeStatus 是 TestType 的状态子结构。
type TestTypeStatus struct {
	Blah string `json:"blah"`
}

// Conversion 验证 conversion-gen 对“内存表示一致/不一致”两类结构体的处理。
type Conversion struct {
	Identical MemoryIdentical `json:"identical"`
	Different MemoryDifferent `json:"different"`
}

// MemoryIdentical 在 internal 与 external 中表示完全一致，
// 转换函数可以直接赋值，无需逐字段处理。
type MemoryIdentical struct {
	Items *MemoryIdentical `json:"items,omitempty"`

	Properties map[string]MemoryIdentical `json:"properties,omitempty"`

	// +listType=atomic
	AllOf []MemoryIdentical `json:"allOf,omitempty"`

	Bool bool `json:"bool"`
}

// MemoryDifferent 的 Bool 在 external 中是 *bool，内存布局不同，
// 因此必须生成显式转换代码。
type MemoryDifferent struct {
	Items *MemoryDifferent `json:"items,omitempty"`

	Properties map[string]MemoryDifferent `json:"properties,omitempty"`

	// +listType=atomic
	AllOf []MemoryDifferent `json:"allOf,omitempty"`

	Bool *bool `json:"bool"` // differs from internal representation
}

// ConversionPrivate 验证未导出字段不会参与转换。
type ConversionPrivate struct {
	PublicField  string `json:"publicField"`
	privateField string `json:"privateField"`
}

// ConversionCustomContainer 覆盖 custom conversion 在 slice/map/struct
// 及指针变体上的分派。
type ConversionCustomContainer struct {
	// +listType=atomic
	Slice []ConversionCustom `json:"slice"`
	// +listType=atomic
	SliceP  []*ConversionCustom          `json:"sliceP"`
	Map     map[string]ConversionCustom  `json:"map"`
	MapP    map[string]*ConversionCustom `json:"mapP"`
	Struct  ConversionCustom             `json:"struct"`
	StructP *ConversionCustom            `json:"structP"`
}

// ConversionCustom 有手写转换函数，用以验证 custom conversion 的优先级。
type ConversionCustom struct {
	PublicField  string `json:"publicField"`
	privateField string `json:"privateField"`
}
