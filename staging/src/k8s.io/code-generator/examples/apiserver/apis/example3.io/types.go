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

package example3

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestType 是顶层类型，会为其生成 client。
// 该类型是生成器的输入样本：+genclient 触发客户端生成，
// deepcopy-gen:interfaces 让它实现 runtime.Object，从而可被 scheme 注册。
type TestType struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta
	// +optional
	Status TestTypeStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestTypeList 是顶层列表类型，其 client 方法自动生成。
// 不应为这个类型单独生成 client。
// 列表类型不单独生成客户端；List verb 复用 TestType 的 typed client。
type TestTypeList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []TestType
}

// TestTypeStatus 是 TestType 的 status 子结构，用于验证状态子资源的生成路径。
type TestTypeStatus struct {
	Blah string
}
