/*
Copyright 2024 The Kubernetes Authors.

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

package core

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestType 是顶层类型，生成器会为它创建 client。
//
// 内嵌 TypeMeta/ObjectMeta 是 API 类型的标准布局：前者提供 kind/apiVersion，
// 后者提供对象元数据；client-gen 依赖 ObjectMeta 判断是否命名空间化。
type TestType struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta
	// +optional
	Status TestTypeStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestTypeList 是顶层列表类型，列表的 client 方法会自动生成，不应为它单独创建 client。
//
// 列表类型与单对象类型成对出现；生成器按 <Type>List 命名约定推导 List 方法。
type TestTypeList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []TestType
}

// TestTypeStatus 是 TestType 的状态子结构；示例中保持最小化。
type TestTypeStatus struct {
	Blah string
}
