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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	gwv1alpha2 "k8s.io/code-generator/examples/crd/apis/gateway-api/v1alpha2"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestType 是顶层类型，生成器会为它创建 client。
//
// 内嵌 TestEmbeddedType 用于制造字段名冲突，验证生成器对 embedded 字段
// 与 TypeMeta/ObjectMeta 的优先级处理。
type TestType struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +optional
	Status TestTypeStatus `json:"status,omitempty"`

	TestEmbeddedType `json:"embedded"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestTypeList 是顶层列表类型，列表的 client 方法会自动生成，不应为它单独创建 client。
type TestTypeList struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []TestType `json:"items"`
}

// TestTypeStatus 内嵌来自 gateway-api 的 PolicyStatus，
// 验证跨包嵌入类型的字段合并。
type TestTypeStatus struct {
	Blah                    string `json:"blah"`
	gwv1alpha2.PolicyStatus `json:""`
}

// TestTypeEmbeddedStatus 自引用 slice，用于验证递归类型的处理。
type TestTypeEmbeddedStatus struct {
	Statuses []TestTypeEmbeddedStatus `json:"statuses"`
}

// TestEmbeddedType 用于制造与其他内嵌结构体的字段冲突：Kind 与内联的 TypeMeta 冲突，
// Namespace 与内嵌的 ObjectMeta 冲突。
type TestEmbeddedType struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
}
