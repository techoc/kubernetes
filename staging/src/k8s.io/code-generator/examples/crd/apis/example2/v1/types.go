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

// TestType 是顶层类型，生成器会为它创建 client。
//
// 生成指令说明：+genclient 触发 client-gen 为它生成 typed client；
// deepcopy 接口标记让它实现 runtime.Object。
type TestType struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +optional
	Status TestTypeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestTypeList 是顶层列表类型，列表的 client 方法会自动生成，不应为它单独创建 client。
//
// List 类型无需单独的 client，client-gen 会为其生成 list 方法。
type TestTypeList struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []TestType `json:"items"`
}

// TestTypeStatus 是 TestType 的 status 子结构。
type TestTypeStatus struct {
	Blah string `json:"blah"`
}
