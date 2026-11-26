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

package example

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestType 是顶层类型，生成器会为它创建 client。
// TestType 是 internal 版本的顶层类型。
type TestType struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta
	// +optional
	Status TestTypeStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestTypeList 是顶层列表类型，列表的 client 方法会自动生成，不应为它单独创建 client。
// TestTypeList 是 internal 版本的列表类型。
type TestTypeList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []TestType
}

// TestTypeStatus 是 TestType 的状态部分。
type TestTypeStatus struct {
	Blah string
}

// Conversion 汇总内存布局相同与不同的两个类型，用于测试 conversion-gen
// 的 unsafe 快速路径与逐字段转换路径。
type Conversion struct {
	Identical MemoryIdentical
	Different MemoryDifferent
}

// MemoryIdentical 在 internal 与 external 表示完全一致，可走内存级转换。
type MemoryIdentical struct {
	Items      *MemoryIdentical
	Properties map[string]MemoryIdentical
	AllOf      []MemoryIdentical
	Bool       bool
}

// MemoryDifferent 与外部表示不同（Bool 字段的语义/类型不一致），
// 必须生成逐字段转换函数。
type MemoryDifferent struct {
	Items      *MemoryDifferent
	Properties map[string]MemoryDifferent
	AllOf      []MemoryDifferent
	Bool       bool // differs from external representation
}

// ConversionPrivate 含未导出字段，验证生成器如何处理私有字段。
type ConversionPrivate struct {
	PublicField  string
	privateField string
}

// ConversionCustomContainer 用多种容器形态引用 ConversionCustom，
// 验证自定义转换函数在 slice/map/指针等位置都被调用。
type ConversionCustomContainer struct {
	Slice   []ConversionCustom
	SliceP  []*ConversionCustom
	Map     map[string]ConversionCustom
	MapP    map[string]*ConversionCustom
	Struct  ConversionCustom
	StructP *ConversionCustom
}

// ConversionCustom 需要手写转换函数（含私有字段）。
type ConversionCustom struct {
	PublicField  string
	privateField string
}
