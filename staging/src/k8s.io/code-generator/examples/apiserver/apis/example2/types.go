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

package example2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient:nonNamespaced
//
// TestType 是顶层类型，会为其生成 client。
// TestType 是 example2 组的顶层类型；nonNamespaced 使其成为集群级资源。
type TestType struct {
	metav1.TypeMeta
	metav1.ObjectMeta
	Status TestTypeStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestTypeList 是顶层列表类型，其 client 方法自动生成。
// 不应为这个类型单独生成 client。
// TestTypeList 是 example2 组的列表类型。
type TestTypeList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []TestType
}

// TestTypeStatus 是 TestType 的状态部分。
type TestTypeStatus struct {
	Blah string
}
